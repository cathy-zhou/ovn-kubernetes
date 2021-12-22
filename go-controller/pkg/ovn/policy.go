package ovn

import (
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	libovsdbclient "github.com/ovn-org/libovsdb/client"
	ovsdb "github.com/ovn-org/libovsdb/ovsdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdbops"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/metrics"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	kapi "k8s.io/api/core/v1"
	knet "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	utilnet "k8s.io/utils/net"
)

const (
	// defaultDenyPolicyTypeACLExtIdKey external ID key for default deny policy type
	defaultDenyPolicyTypeACLExtIdKey = "default-deny-policy-type"
	// l4MatchACLExtIdKey external ID key for L4 Match on 'gress policy ACLs
	l4MatchACLExtIdKey = "l4Match"
	// ipBlockCIDRACLExtIdKey external ID key for IP block CIDR on 'gress policy ACLs
	ipBlockCIDRACLExtIdKey = "ipblock_cidr"
	// namespaceACLExtIdKey external ID key for namespace on 'gress policy ACLs
	namespaceACLExtIdKey = "namespace"
	// policyACLExtIdKey external ID key for policy name on 'gress policy ACLs
	policyACLExtIdKey = "policy"
	// policyACLExtKey external ID key for policy type on 'gress policy ACLs
	policyTypeACLExtIdKey = "policy_type"
	// policyTypeNumACLExtIdKey external ID key for policy index by type on 'gress policy ACLs
	policyTypeNumACLExtIdKey = "%s_num"
	// sharePortGroupExtIdKey external ID indicate the port group is for shared port group
	sharePortGroupExtIdKey = "shared_port_group"
	// ingressDefaultDenySuffix is the suffix used when creating the ingress port group for a namespace
	ingressDefaultDenySuffix = "ingressDefaultDeny"
	// egressDefaultDenySuffix is the suffix used when creating the ingress port group for a namespace
	egressDefaultDenySuffix = "egressDefaultDeny"
	// arpAllowPolicySuffix is the suffix used when creating default ACLs for a namespace
	arpAllowPolicySuffix = "ARPallowPolicy"
)

var NetworkPolicyNotCreated error

type networkPolicy struct {
	// RWMutex synchronizes operations on the policy.
	// Operations that change local and peer pods take a RLock,
	// whereas operations that affect the policy take a Lock.
	sync.RWMutex
	name            string
	namespace       string
	policy          *knet.NetworkPolicy
	ingressPolicies []*gressPolicy
	egressPolicies  []*gressPolicy
	podHandlerList  []*factory.Handler
	svcHandlerList  []*factory.Handler
	nsHandlerList   []*factory.Handler

	// portGroupName is the name of the shared port group this network policy belongs to
	portGroupName string
	deleted       bool //deleted policy
	created       bool

	// indicate if the policy needs ingress/egress default deny rule
	ingressDefDeny bool
	egressDefDeny  bool

	// sharePortGroupName is in the form of <policy_namespace>_shared_port_group_<localPodSelector.String>
	// it is used as the key in each per-namespace spgInfoMap (oc.spgInfoMap[namespace]), and it is also used as
	// the external-ids:name field of the associated shared port group
	sharePortGroupName string
}

// now we only support portGroup sharing if the policy's local pod selector is empty. This can be changed later.
func getSharedPortGroupName(policy *knet.NetworkPolicy) string {
	sel, _ := metav1.LabelSelectorAsSelector(&policy.Spec.PodSelector)
	return policy.Namespace + "_" + "shared_port_group" + "_" + sel.String()
}

func NewNetworkPolicy(policy *knet.NetworkPolicy) *networkPolicy {
	// Default deny rule.
	// 1. Any pod that matches a network policy should get a default
	// ingress deny rule.  This is irrespective of whether there
	// is a ingress section in the network policy. But, if
	// PolicyTypes in the policy has only "egress" in it, then
	// it is a 'egress' only network policy and we should not
	// add any default deny rule for ingress.
	// 2. If there is any "egress" section in the policy or
	// the PolicyTypes has 'egress' in it, we add a default
	// egress deny rule.
	//
	// check if the given policy requires ingressDefaultDeny/egressDefaultDeny rules.

	// condition 1 above.
	ingressDefDeny := !(len(policy.Spec.PolicyTypes) == 1 && policy.Spec.PolicyTypes[0] == knet.PolicyTypeEgress)
	// condition 2 above.
	egressDefDeny := (len(policy.Spec.PolicyTypes) == 1 && policy.Spec.PolicyTypes[0] == knet.PolicyTypeEgress) ||
		len(policy.Spec.Egress) > 0 || len(policy.Spec.PolicyTypes) == 2

	np := &networkPolicy{
		name:            policy.Name,
		namespace:       policy.Namespace,
		policy:          policy,
		ingressPolicies: make([]*gressPolicy, 0),
		egressPolicies:  make([]*gressPolicy, 0),
		podHandlerList:  make([]*factory.Handler, 0),
		svcHandlerList:  make([]*factory.Handler, 0),
		nsHandlerList:   make([]*factory.Handler, 0),
		ingressDefDeny:  ingressDefDeny,
		egressDefDeny:   egressDefDeny,
	}
	return np
}

const (
	noneMatch = "None"
	// IPv6 multicast traffic destined to dynamic groups must have the "T" bit
	// set to 1: https://tools.ietf.org/html/rfc3307#section-4.3
	ipv6DynamicMulticastMatch = "(ip6.dst[120..127] == 0xff && ip6.dst[116] == 1)"
	// Legacy multicastDefaultDeny port group removed by commit 40a90f0
	legacyMulticastDefaultDenyPortGroup = "mcastPortGroupDeny"
)

// hash the provided input to make it a valid portGroup name.
func hashedPortGroup(s string) string {
	return util.HashForOVN(s)
}

func (oc *Controller) syncNetworkPolicies(networkPolicies []interface{}) error {
	// expected shared port groups for all existing network policies
	type expectedPortGroupInfo struct {
		selector         metav1.LabelSelector // localPodSelecor
		lspIngressRefCnt int                  // ingress default deny policy reference count for this shared port group
		lspEgressRefCnt  int                  // egress default deny policy reference count for this shared port group
	}

	// existing shared port groups in the ovn-nb DB
	type existingPortGroupInfo struct {
		pgName   string               // port group name
		selector metav1.LabelSelector // localPodSelector, derived from external-ids:name
		aclUUIDs map[string]bool      // map of acls in the port group
	}

	stalePGs := sets.NewString()

	// find all existing shared port groups in the ovn-nb DB
	existingSharedPortGroupsInfoMap := make(map[string]map[string]existingPortGroupInfo)
	pPortGroup := func(item *nbdb.PortGroup) bool {
		if _, ok := item.ExternalIDs[sharePortGroupExtIdKey]; ok {
			return true
		}
		return false
	}
	portGroups, err := libovsdbops.FindPortGroupsWithPredicate(oc.nbClient, pPortGroup)
	if err != nil {
		return fmt.Errorf("failed to find shared port groups: %v", err)
	}
	for _, portGroup := range portGroups {
		ns, ok := portGroup.ExternalIDs[namespaceACLExtIdKey]
		if !ok {
			klog.Warningf("Invalid shared port group %s: no %s external-id", portGroup.Name, namespaceACLExtIdKey)
			stalePGs.Insert(portGroup.Name)
			continue
		}
		name, ok := portGroup.ExternalIDs["name"]
		if !ok {
			klog.Warningf("Invalid shared port group %s: no %s external-id", portGroup.Name, "name")
			stalePGs.Insert(portGroup.Name)
			continue
		}

		prefix := ns + "_" + "shared_port_group" + "_"
		if !strings.HasPrefix(name, prefix) {
			klog.Warningf("Invalid shared port group %s: external-id:name %s, expect to be prefixed with %s",
				portGroup.Name, name, prefix)
			stalePGs.Insert(portGroup.Name)
			continue
		}
		selString := strings.TrimPrefix(name, prefix)
		sel, err := metav1.ParseToLabelSelector(selString)
		if err != nil {
			klog.Warningf("Invalid shared port group %s: invalid external-id:name %s, failed to parse to selector",
				portGroup.Name, name)
			stalePGs.Insert(portGroup.Name)
			continue
		}
		pgInfo := existingPortGroupInfo{
			pgName:   portGroup.Name,
			selector: *sel,
			aclUUIDs: map[string]bool{},
		}
		for _, uuid := range portGroup.ACLs {
			pgInfo.aclUUIDs[uuid] = true
		}
		if sharedPortGroups, ok := existingSharedPortGroupsInfoMap[ns]; ok {
			sharedPortGroups[portGroup.Name] = pgInfo
		} else {
			existingSharedPortGroupsInfoMap[ns] = map[string]existingPortGroupInfo{portGroup.Name: pgInfo}
		}
	}
	klog.V(5).Infof("Found all existing SharedPortGroups %v", existingSharedPortGroupsInfoMap)

	// based on all existing localPod policy acls and default deny acls in the ovn-nb DB, found all possible stale
	// per-policy port groups and default deny port groups that were created before shared-port-group support.
	pAcl := func(item *nbdb.ACL) bool {
		_, ok1 := item.ExternalIDs[namespaceACLExtIdKey]
		_, ok2 := item.ExternalIDs[policyACLExtIdKey]
		if ok1 && ok2 {
			return true
		}
		_, ok := item.ExternalIDs[defaultDenyPolicyTypeACLExtIdKey]
		if ok {
			if strings.Contains(item.Match, ingressDefaultDenySuffix) || strings.Contains(item.Match, egressDefaultDenySuffix) {
				return true
			}
		}
		return false
	}
	acls, err := libovsdbops.FindACLsWithPredicate(oc.nbClient, pAcl)
	if err != nil {
		return err
	}
	for _, acl := range acls {
		if _, ok := acl.ExternalIDs[defaultDenyPolicyTypeACLExtIdKey]; ok {
			if acl.Name != nil {
				// if this acl is before shared port group support, it's name is prefixed with namespace, otherwise,
				// it is prefixed with shared port group name which should be in existingSharedPortGroupsInfoMap
				found := false
				namespaceOrSharedPortGroupName := strings.Split(*acl.Name, "_")[0]
				for _, existingSharedPortGroupsInfo := range existingSharedPortGroupsInfoMap {
					if _, ok = existingSharedPortGroupsInfo[namespaceOrSharedPortGroupName]; ok {
						found = true
					}
				}
				if !found {
					// this acl is created before shared port group support, the port group name can be determined
					stalePGs.Insert(defaultDenyPortGroup(namespaceOrSharedPortGroupName, ingressDefaultDenySuffix))
					stalePGs.Insert(defaultDenyPortGroup(namespaceOrSharedPortGroupName, egressDefaultDenySuffix))
				}
			}
		} else {
			stalePGs.Insert(hashedPortGroup(fmt.Sprintf("%s_%s", acl.ExternalIDs[namespaceACLExtIdKey],
				acl.ExternalIDs[policyACLExtIdKey])))
		}
	}

	// now delete all stale port groups pre shared-port-group support
	ops := []ovsdb.Operation{}
	klog.V(5).Infof("Delete stale port groups %v", stalePGs)
	for stalePG := range stalePGs {
		ops, err = libovsdbops.DeletePortGroupsOps(oc.nbClient, ops, stalePG)
		if err != nil {
			return fmt.Errorf("error getting ops of removing stale port group %v: %v", stalePG, err)
		}
	}
	_, err = libovsdbops.TransactAndCheck(oc.nbClient, ops)
	if err != nil {
		return err
	}

	// all ports groups that are created before shared-port-group support are deleted, now focus on
	// stale logical entities created after shared-port-group support.
	//
	// get all existing network policies and expected shared port groups
	expectedPolicies := make(map[string]map[string]bool)
	expectedPgInfosMap := make(map[string][]*expectedPortGroupInfo)
	for _, npInterface := range networkPolicies {
		policy, ok := npInterface.(*knet.NetworkPolicy)
		if !ok {
			return fmt.Errorf("spurious object in syncNetworkPolicies: %v", npInterface)
		}

		np := NewNetworkPolicy(policy)

		if nsMap, ok := expectedPolicies[policy.Namespace]; ok {
			nsMap[policy.Name] = true
		} else {
			expectedPolicies[policy.Namespace] = map[string]bool{
				policy.Name: true,
			}
		}

		found := false
		var pgInfo *expectedPortGroupInfo
		pgInfos, ok := expectedPgInfosMap[np.namespace]
		if ok {
			for _, pgInfo = range pgInfos {
				if reflect.DeepEqual(pgInfo.selector, policy.Spec.PodSelector) {
					found = true
					break
				}
			}
		}
		if !found {
			pgInfo = &expectedPortGroupInfo{selector: policy.Spec.PodSelector}
			if !ok {
				expectedPgInfosMap[np.namespace] = []*expectedPortGroupInfo{pgInfo}
			} else {
				expectedPgInfosMap[np.namespace] = append(expectedPgInfosMap[np.namespace], pgInfo)
			}
		}
		if np.egressDefDeny {
			pgInfo.lspEgressRefCnt++
		}
		if np.ingressDefDeny {
			pgInfo.lspIngressRefCnt++
		}
	}
	klog.V(5).Infof("Expect shared port groups %v for all existing network policies %v", expectedPgInfosMap, expectedPolicies)

	err = oc.addressSetFactory.ProcessEachAddressSet(func(addrSetName, namespaceName, policyName string) error {
		if policyName == "" {
			return nil
		}
		if _, ok := expectedPolicies[namespaceName][policyName]; !ok {
			// policy doesn't exist on k8s.

			// delete the address sets for this old policy from OVN
			if err := oc.addressSetFactory.DestroyAddressSetInBackingStore(addrSetName); err != nil {
				klog.Errorf(err.Error())
				return err
			}
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("error in syncing network policies: %v", err)
	}

	// get all per-policy acls and default deny acls.
	pAcl = func(item *nbdb.ACL) bool {
		_, ok1 := item.ExternalIDs[namespaceACLExtIdKey]
		_, ok2 := item.ExternalIDs[policyACLExtIdKey]
		if ok1 && ok2 {
			return true
		}
		_, ok := item.ExternalIDs[defaultDenyPolicyTypeACLExtIdKey]
		if ok {
			if strings.Contains(item.Match, ingressDefaultDenySuffix) || strings.Contains(item.Match, egressDefaultDenySuffix) {
				return true
			}
		}
		return false
	}
	acls, err = libovsdbops.FindACLsWithPredicate(oc.nbClient, pAcl)
	if err != nil {
		return err
	}

	// found all stale localPod acls of policies deleted during ovnkube-master shutdown;
	// also collect all default deny acls in the allDefDenyAclsMap, this will be used when
	// deleting stale default deny acls are needed.
	staleAcls := make([]*nbdb.ACL, 0, len(acls))
	allDefDenyAclsMap := make(map[string]*nbdb.ACL)
	for index := range acls {
		acl := acls[index]
		if _, ok := acl.ExternalIDs[defaultDenyPolicyTypeACLExtIdKey]; ok && acl.Name != nil {
			// default deny acls, note that there could be duplicate acl.Name for ingress/egress,
			// so ingress/egress as part of map key
			allDefDenyAclsMap[*acl.Name+"_"+acl.ExternalIDs[defaultDenyPolicyTypeACLExtIdKey]] = acl
		} else {
			if _, ok := expectedPolicies[acl.ExternalIDs[namespaceACLExtIdKey]][acl.ExternalIDs[policyACLExtIdKey]]; !ok {
				staleAcls = append(staleAcls, acl)
			}
		}
	}
	klog.V(5).Infof("Found stale per-policy acls %v; and all existing default deny acls %v", staleAcls, allDefDenyAclsMap)

	// find shared port group the stale acl references
	ops = []ovsdb.Operation{}
	for _, staleAcl := range staleAcls {
		ns := staleAcl.ExternalIDs[namespaceACLExtIdKey]

		portGroupNames := []string{}
		portGroupInfos, ok := existingSharedPortGroupsInfoMap[ns]
		if ok {
		out:
			for pgName := range portGroupInfos {
				portGroupInfo := portGroupInfos[pgName]
				if _, ok := portGroupInfo.aclUUIDs[staleAcl.UUID]; ok {
					portGroupNames = []string{portGroupInfo.pgName}
					break out
				}
			}
		}
		klog.V(5).Infof("Delete stale per-policy acl %v from port group %v", staleAcl, portGroupNames)
		if ops, err = libovsdbops.DeleteACLsOps(oc.nbClient, ops, portGroupNames, nil, staleAcl); err != nil {
			return fmt.Errorf("failed to get ops to delete ACLs for stale policy %s/%s: %v", ns,
				staleAcl.ExternalIDs[policyACLExtIdKey], err)
		}
	}

	// if the egress/ingress default deny reference count drops to 0, delete the default deny acls if they exist.
	for ns, pgInfos := range existingSharedPortGroupsInfoMap {
		for pgName, pgInfo := range pgInfos {
			pgNeeded := false
			staleDefDenyAclKeys := []string{}
			// if the no acls left, delete the port group as well
			if expectedPgInfos, ok := expectedPgInfosMap[ns]; ok {
				for _, expectedPgInfo := range expectedPgInfos {
					if reflect.DeepEqual(pgInfo.selector, expectedPgInfo.selector) {
						pgNeeded = true
						// it is possible egress/ingress default deny reference count has changed, we'd need to delete
						// default deny acls if they are no longer needed.
						if expectedPgInfo.lspEgressRefCnt == 0 {
							klog.V(5).Infof("Egress default deny reference count drops to 0 for port group %s", pgName)
							aclName := namespacePortGroupACLName("", pgName, egressDefaultDenySuffix)
							staleDefDenyAclKeys = append(staleDefDenyAclKeys, aclName+"_"+string(knet.PolicyTypeEgress))
							aclName = namespacePortGroupACLName("", pgName, arpAllowPolicySuffix)
							staleDefDenyAclKeys = append(staleDefDenyAclKeys, aclName+"_"+string(knet.PolicyTypeEgress))
						}
						if expectedPgInfo.lspIngressRefCnt == 0 {
							klog.V(5).Infof("Ingress default deny reference count drops to 0 for port group %s", pgName)
							aclName := namespacePortGroupACLName("", pgName, ingressDefaultDenySuffix)
							staleDefDenyAclKeys = append(staleDefDenyAclKeys, aclName+"_"+string(knet.PolicyTypeIngress))
							aclName = namespacePortGroupACLName("", pgName, arpAllowPolicySuffix)
							staleDefDenyAclKeys = append(staleDefDenyAclKeys, aclName+"_"+string(knet.PolicyTypeIngress))
						}
					}
				}
			}

			for _, staleDefDenyAclKey := range staleDefDenyAclKeys {
				if staleDefDenyAcl, ok := allDefDenyAclsMap[staleDefDenyAclKey]; ok {
					klog.V(5).Infof("Delete stale default deny acl %s from port group %s", staleDefDenyAclKey, pgName)
					if ops, err = libovsdbops.DeleteACLsOps(oc.nbClient, ops, []string{pgName}, nil, staleDefDenyAcl); err != nil {
						return fmt.Errorf("failed to get ops to delete default deny ACLs from shared port group %s: %v", pgName, err)
					}
				}
			}

			if !pgNeeded {
				klog.Infof("Delete stale shared port group %s", pgName)
				ops, err = libovsdbops.DeletePortGroupsOps(oc.nbClient, ops, pgName)
				if err != nil {
					return fmt.Errorf("error getting ops of removing stale port groups %v: %v", pgName, err)
				}
			}
		}
	}

	// Update existing egress network policies to use the updated ACLs
	// Note that the default multicast egress acls were created with the correct direction, but
	// we'd still need to update its apply-after-lb=true option, so that the ACL priorities can apply properly;
	// If acl's option["apply-after-lb"] is already set to true, then its direction should be also correct.
	p := func(item *nbdb.ACL) bool {
		return (item.ExternalIDs[policyTypeACLExtIdKey] == string(knet.PolicyTypeEgress) ||
			item.ExternalIDs[defaultDenyPolicyTypeACLExtIdKey] == string(knet.PolicyTypeEgress)) &&
			item.Options["apply-after-lb"] != "true"
	}
	egressACLs, err := libovsdbops.FindACLsWithPredicate(oc.nbClient, p)
	if err != nil {
		return fmt.Errorf("cannot find NetworkPolicy Egress ACLs: %v", err)
	}

	if len(egressACLs) > 0 {
		for _, acl := range egressACLs {
			acl.Direction = nbdb.ACLDirectionFromLport
			if acl.Options == nil {
				acl.Options = map[string]string{"apply-after-lb": "true"}
			} else {
				acl.Options["apply-after-lb"] = "true"
			}
		}
		ops, err := libovsdbops.CreateOrUpdateACLsOps(oc.nbClient, ops, egressACLs...)
		if err != nil {
			return fmt.Errorf("cannot create ops to update old Egress NetworkPolicy ACLs: %v", err)
		}
		_, err = libovsdbops.TransactAndCheck(oc.nbClient, ops)
		if err != nil {
			return fmt.Errorf("cannot update old Egress NetworkPolicy ACLs: %v", err)
		}
	}

	return nil
}

func addAllowACLFromNode(nodeName string, mgmtPortIP net.IP, nbClient libovsdbclient.Client) error {
	ipFamily := "ip4"
	if utilnet.IsIPv6(mgmtPortIP) {
		ipFamily = "ip6"
	}
	match := fmt.Sprintf("%s.src==%s", ipFamily, mgmtPortIP.String())

	nodeACL := BuildACL("", nbdb.ACLDirectionToLport, types.DefaultAllowPriority, match, "allow-related", nil, nil, nil)

	ops, err := libovsdbops.CreateOrUpdateACLsOps(nbClient, nil, nodeACL)
	if err != nil {
		return fmt.Errorf("failed to create or update ACL %v: %v", nodeACL, err)
	}

	ops, err = libovsdbops.AddACLsToLogicalSwitchOps(nbClient, ops, nodeName, nodeACL)
	if err != nil {
		return fmt.Errorf("failed to add ACL %v to switch %s: %v", nodeACL, nodeName, err)
	}

	_, err = libovsdbops.TransactAndCheck(nbClient, ops)
	if err != nil {
		return err
	}

	return nil
}

func getACLMatch(portGroupName, match string, policyType knet.PolicyType) string {
	var aclMatch string
	if policyType == knet.PolicyTypeIngress {
		aclMatch = "outport == @" + portGroupName
	} else {
		aclMatch = "inport == @" + portGroupName
	}

	if match != "" {
		aclMatch += " && " + match
	}

	return aclMatch
}

func namespacePortGroupACLName(namespace, portGroup, name string) string {
	policyNamespace := namespace
	if policyNamespace == "" {
		policyNamespace = portGroup
	}
	if name == "" {
		return policyNamespace

	}
	return fmt.Sprintf("%s_%s", policyNamespace, name)
}

func buildACL(namespace, portGroup, name, direction string, priority int, match, action string,
	logLevels *ACLLoggingLevels, policyType knet.PolicyType) *nbdb.ACL {
	var options map[string]string
	aclName := namespacePortGroupACLName(namespace, portGroup, name)
	var externalIds map[string]string
	if policyType != "" {
		externalIds = map[string]string{
			defaultDenyPolicyTypeACLExtIdKey: string(policyType),
		}
	}
	if policyType == knet.PolicyTypeEgress {
		options = map[string]string{
			"apply-after-lb": "true",
		}
	}

	return BuildACL(aclName, direction, priority, match, action, logLevels, externalIds, options)
}

func defaultDenyPortGroup(namespace, gressSuffix string) string {
	return hashedPortGroup(namespace) + "_" + gressSuffix
}

func buildDenyACLs(namespace, pg string, aclLogging *ACLLoggingLevels, policyType knet.PolicyType) (denyACL, allowACL *nbdb.ACL) {
	denyMatch := getACLMatch(pg, "", policyType)
	allowMatch := getACLMatch(pg, "(arp || nd)", policyType)
	if policyType == knet.PolicyTypeIngress {
		denyACL = buildACL(namespace, pg, ingressDefaultDenySuffix, nbdb.ACLDirectionToLport,
			types.DefaultDenyPriority, denyMatch, nbdb.ACLActionDrop, aclLogging, policyType)
		allowACL = buildACL(namespace, pg, arpAllowPolicySuffix, nbdb.ACLDirectionToLport,
			types.DefaultAllowPriority, allowMatch, nbdb.ACLActionAllow, nil, policyType)
	} else {
		denyACL = buildACL(namespace, pg, egressDefaultDenySuffix, nbdb.ACLDirectionFromLport,
			types.DefaultDenyPriority, denyMatch, nbdb.ACLActionDrop, aclLogging, policyType)
		allowACL = buildACL(namespace, pg, arpAllowPolicySuffix, nbdb.ACLDirectionFromLport,
			types.DefaultAllowPriority, allowMatch, nbdb.ACLActionAllow, nil, policyType)
	}
	return
}

func (oc *Controller) updateACLLoggingForPolicy(np *networkPolicy, aclLogging *ACLLoggingLevels) error {
	np.Lock()
	defer np.Unlock()

	if np.deleted {
		return nil
	}

	if !np.created {
		return NetworkPolicyNotCreated
	}
	// Predicate for given network policy ACLs
	p := func(item *nbdb.ACL) bool {
		return item.ExternalIDs[namespaceACLExtIdKey] == np.namespace && item.ExternalIDs[policyACLExtIdKey] == np.name
	}
	return UpdateACLLoggingWithPredicate(oc.nbClient, p, aclLogging)
}

// update default deny ACLs for shared port groups, if the given networkPolicy is null, update all shared port group
// in this namespace
func (oc *Controller) updateACLLoggingForSharedGroupAcls(nsInfo *namespaceInfo, np *networkPolicy) error {
	npMap := nsInfo.networkPolicies
	if np != nil {
		npMap = map[string]*networkPolicy{np.name: np}
	}

	acls := []*nbdb.ACL{}
	spgNames := map[string]bool{}
	for _, np = range npMap {
		if _, ok := spgNames[np.sharePortGroupName]; ok {
			// already handled
			continue
		}
		oc.spgInfoMutex.Lock()
		spgInfoMap, ok := oc.spgInfoMap[np.namespace]
		if !ok {
			oc.spgInfoMutex.Unlock()
			continue
		}
		spgInfo, ok := spgInfoMap[np.sharePortGroupName]
		if !ok {
			oc.spgInfoMutex.Unlock()
			continue
		}
		spgInfo.Lock()
		oc.spgInfoMutex.Unlock()
		spgNames[np.sharePortGroupName] = true
		// nsInfo lock is held at this time which prevent shared group policy deletion from proceeding
		if spgInfo.lspIngressRefCnt > 0 {
			acl, _ := buildDenyACLs("", hashedPortGroup(np.sharePortGroupName),
				&nsInfo.aclLogging, knet.PolicyTypeIngress)
			acls = append(acls, acl)
		}
		if spgInfo.lspEgressRefCnt > 0 {
			acl, _ := buildDenyACLs("", hashedPortGroup(np.sharePortGroupName),
				&nsInfo.aclLogging, knet.PolicyTypeEgress)
			acls = append(acls, acl)
		}
		spgInfo.Unlock()
	}

	if err := UpdateACLLogging(oc.nbClient, acls, &nsInfo.aclLogging); err != nil {
		return fmt.Errorf("unable to update ACL logging for shared port groups: %w", err)
	}
	return nil
}

func (oc *Controller) setNetworkPolicyACLLoggingForNamespace(ns string, nsInfo *namespaceInfo) error {
	if err := oc.updateACLLoggingForSharedGroupAcls(nsInfo, nil); err != nil {
		return err
	}

	// now update network policy specific ACLs
	klog.V(5).Infof("Setting network policy ACLs for ns: %s", ns)
	for name, policy := range nsInfo.networkPolicies {
		// REMOVEME(trozet): once we can hold the np lock for the duration of the np create
		// there is no reason to do this loop
		// 24ms is chosen because gomega.Eventually default timeout is 50ms
		// libovsdb transactions take less than 50ms usually as well so pod create
		// should be done within a couple iterations
		retryErr := wait.PollImmediate(24*time.Millisecond, 1*time.Second, func() (bool, error) {
			if err := oc.updateACLLoggingForPolicy(policy, &nsInfo.aclLogging); err == nil {
				return true, nil
			} else if errors.Is(err, NetworkPolicyNotCreated) {
				return false, nil
			} else {
				return false, fmt.Errorf("unable to update ACL for network policy: %v", err)
			}
		})
		if retryErr != nil {
			return retryErr
		}
		klog.Infof("ACL for network policy: %s, updated to new log level: %s", name, nsInfo.aclLogging.Allow)
	}
	return nil
}

func getACLMatchAF(ipv4Match, ipv6Match string) string {
	if config.IPv4Mode && config.IPv6Mode {
		return "(" + ipv4Match + " || " + ipv6Match + ")"
	} else if config.IPv4Mode {
		return ipv4Match
	} else {
		return ipv6Match
	}
}

// Creates the match string used for ACLs matching on multicast traffic.
func getMulticastACLMatch() string {
	return "(ip4.mcast || mldv1 || mldv2 || " + ipv6DynamicMulticastMatch + ")"
}

// Allow IGMP traffic (e.g., IGMP queries) and namespace multicast traffic
// towards pods.
func getMulticastACLIgrMatchV4(addrSetName string) string {
	return "(igmp || (ip4.src == $" + addrSetName + " && ip4.mcast))"
}

// Allow MLD traffic (e.g., MLD queries) and namespace multicast traffic
// towards pods.
func getMulticastACLIgrMatchV6(addrSetName string) string {
	return "(mldv1 || mldv2 || (ip6.src == $" + addrSetName + " && " + ipv6DynamicMulticastMatch + "))"
}

// Creates the match string used for ACLs allowing incoming multicast into a
// namespace, that is, from IPs that are in the namespace's address set.
func getMulticastACLIgrMatch(nsInfo *namespaceInfo) string {
	var ipv4Match, ipv6Match string
	addrSetNameV4, addrSetNameV6 := nsInfo.addressSet.GetASHashNames()
	if config.IPv4Mode {
		ipv4Match = getMulticastACLIgrMatchV4(addrSetNameV4)
	}
	if config.IPv6Mode {
		ipv6Match = getMulticastACLIgrMatchV6(addrSetNameV6)
	}
	return getACLMatchAF(ipv4Match, ipv6Match)
}

// Creates the match string used for ACLs allowing outgoing multicast from a
// namespace.
func getMulticastACLEgrMatch() string {
	var ipv4Match, ipv6Match string
	if config.IPv4Mode {
		ipv4Match = "ip4.mcast"
	}
	if config.IPv6Mode {
		ipv6Match = "(mldv1 || mldv2 || " + ipv6DynamicMulticastMatch + ")"
	}
	return getACLMatchAF(ipv4Match, ipv6Match)
}

// Creates a policy to allow multicast traffic within 'ns':
// - a port group containing all logical ports associated with 'ns'
// - one "from-lport" ACL allowing egress multicast traffic from the pods
//   in 'ns'
// - one "to-lport" ACL allowing ingress multicast traffic to pods in 'ns'.
//   This matches only traffic originated by pods in 'ns' (based on the
//   namespace address set).
func (oc *Controller) createMulticastAllowPolicy(ns string, nsInfo *namespaceInfo) error {
	portGroupName := hashedPortGroup(ns)

	egressMatch := getACLMatch(portGroupName, getMulticastACLEgrMatch(), knet.PolicyTypeEgress)
	egressACL := buildACL(ns, portGroupName, "MulticastAllowEgress", nbdb.ACLDirectionFromLport,
		types.DefaultMcastAllowPriority, egressMatch, nbdb.ACLActionAllow, nil, knet.PolicyTypeEgress)
	ingressMatch := getACLMatch(portGroupName, getMulticastACLIgrMatch(nsInfo), knet.PolicyTypeIngress)
	ingressACL := buildACL(ns, portGroupName, "MulticastAllowIngress", nbdb.ACLDirectionToLport,
		types.DefaultMcastAllowPriority, ingressMatch, nbdb.ACLActionAllow, nil, knet.PolicyTypeIngress)
	acls := []*nbdb.ACL{egressACL, ingressACL}
	ops, err := libovsdbops.CreateOrUpdateACLsOps(oc.nbClient, nil, acls...)
	if err != nil {
		return err
	}

	// Add all ports from this namespace to the multicast allow group.
	ports := []*nbdb.LogicalSwitchPort{}
	pods, err := oc.watchFactory.GetPods(ns)
	if err != nil {
		klog.Warningf("Failed to get pods for namespace %q: %v", ns, err)
	}
	for _, pod := range pods {
		if util.PodCompleted(pod) {
			continue
		}
		portName := util.GetLogicalPortName(pod.Namespace, pod.Name)
		if portInfo, err := oc.logicalPortCache.get(portName); err != nil {
			klog.Errorf(err.Error())
		} else {
			ports = append(ports, &nbdb.LogicalSwitchPort{UUID: portInfo.uuid})
		}
	}

	pg := libovsdbops.BuildPortGroup(portGroupName, ns, ports, acls)
	ops, err = libovsdbops.CreateOrUpdatePortGroupsOps(oc.nbClient, ops, pg)
	if err != nil {
		return err
	}

	_, err = libovsdbops.TransactAndCheck(oc.nbClient, ops)
	if err != nil {
		return err
	}

	return nil
}

func deleteMulticastAllowPolicy(nbClient libovsdbclient.Client, ns string) error {
	portGroupName := hashedPortGroup(ns)
	// ACLs referenced by the port group wil be deleted by db if there are no other references
	err := libovsdbops.DeletePortGroups(nbClient, portGroupName)
	if err != nil {
		return fmt.Errorf("failed deleting port group %s: %v", portGroupName, err)
	}

	return nil
}

// Creates a global default deny multicast policy:
// - one ACL dropping egress multicast traffic from all pods: this is to
//   protect OVN controller from processing IP multicast reports from nodes
//   that are not allowed to receive multicast traffic.
// - one ACL dropping ingress multicast traffic to all pods.
// Caller must hold the namespace's namespaceInfo object lock.
func (oc *Controller) createDefaultDenyMulticastPolicy() error {
	match := getMulticastACLMatch()

	// By default deny any egress multicast traffic from any pod. This drops
	// IP multicast membership reports therefore denying any multicast traffic
	// to be forwarded to pods.
	egressACL := buildACL("", types.ClusterPortGroupName, "DefaultDenyMulticastEgress",
		nbdb.ACLDirectionFromLport, types.DefaultMcastDenyPriority, match, nbdb.ACLActionDrop, nil,
		knet.PolicyTypeEgress)

	// By default deny any ingress multicast traffic to any pod.
	ingressACL := buildACL("", types.ClusterPortGroupName, "DefaultDenyMulticastIngress",
		nbdb.ACLDirectionToLport, types.DefaultMcastDenyPriority, match, nbdb.ACLActionDrop, nil,
		knet.PolicyTypeIngress)

	ops, err := libovsdbops.CreateOrUpdateACLsOps(oc.nbClient, nil, egressACL, ingressACL)
	if err != nil {
		return err
	}

	ops, err = libovsdbops.AddACLsToPortGroupOps(oc.nbClient, ops, types.ClusterPortGroupName, egressACL, ingressACL)
	if err != nil {
		return err
	}

	// Remove old multicastDefaultDeny port group now that all ports
	// have been added to the clusterPortGroup by WatchPods()
	ops, err = libovsdbops.DeletePortGroupsOps(oc.nbClient, ops, legacyMulticastDefaultDenyPortGroup)
	if err != nil {
		return err
	}

	_, err = libovsdbops.TransactAndCheck(oc.nbClient, ops)
	if err != nil {
		return err
	}

	return nil
}

// Creates a global default allow multicast policy:
// - one ACL allowing multicast traffic from cluster router ports
// - one ACL allowing multicast traffic to cluster router ports.
// Caller must hold the namespace's namespaceInfo object lock.
func (oc *Controller) createDefaultAllowMulticastPolicy() error {
	mcastMatch := getMulticastACLMatch()

	egressMatch := getACLMatch(types.ClusterRtrPortGroupName, mcastMatch, knet.PolicyTypeEgress)
	egressACL := buildACL("", types.ClusterRtrPortGroupName, "DefaultAllowMulticastEgress",
		nbdb.ACLDirectionFromLport, types.DefaultMcastAllowPriority, egressMatch, nbdb.ACLActionAllow, nil,
		knet.PolicyTypeEgress)

	ingressMatch := getACLMatch(types.ClusterRtrPortGroupName, mcastMatch, knet.PolicyTypeIngress)
	ingressACL := buildACL("", types.ClusterRtrPortGroupName, "DefaultAllowMulticastIngress",
		nbdb.ACLDirectionToLport, types.DefaultMcastAllowPriority, ingressMatch, nbdb.ACLActionAllow, nil,
		knet.PolicyTypeIngress)

	ops, err := libovsdbops.CreateOrUpdateACLsOps(oc.nbClient, nil, egressACL, ingressACL)
	if err != nil {
		return err
	}

	ops, err = libovsdbops.AddACLsToPortGroupOps(oc.nbClient, ops, types.ClusterRtrPortGroupName, egressACL, ingressACL)
	if err != nil {
		return err
	}

	_, err = libovsdbops.TransactAndCheck(oc.nbClient, ops)
	if err != nil {
		return err
	}

	return nil
}

// podAddAllowMulticastPolicy adds the pod's logical switch port to the namespace's
// multicast port group. Caller must hold the namespace's namespaceInfo object
// lock.
func podAddAllowMulticastPolicy(nbClient libovsdbclient.Client, ns string, portInfo *lpInfo) error {
	return libovsdbops.AddPortsToPortGroup(nbClient, hashedPortGroup(ns), portInfo.uuid)
}

// podDeleteAllowMulticastPolicy removes the pod's logical switch port from the
// namespace's multicast port group. Caller must hold the namespace's
// namespaceInfo object lock.
func podDeleteAllowMulticastPolicy(nbClient libovsdbclient.Client, ns string, portUUID string) error {
	return libovsdbops.DeletePortsFromPortGroup(nbClient, hashedPortGroup(ns), portUUID)
}

func (oc *Controller) processLocalPodSelectorSetPods(spgInfo *sharedPortGroupInfo,
	objs ...interface{}) (policyPorts []string) {
	klog.Infof("Processing shared networkGroup %s to have %d local pods...", spgInfo.pgName, len(objs))

	// get list of pods and their logical ports to add
	// theoretically this should never filter any pods but it's always good to be
	// paranoid.
	policyPorts = make([]string, 0, len(objs))

	// thread safe helper vars used by the `getPortInfo` go-routine
	getPortsInfoMap := sync.Map{}
	getPolicyPortsWg := &sync.WaitGroup{}

	getPortInfo := func(pod *kapi.Pod) {
		defer getPolicyPortsWg.Done()

		if pod.Spec.NodeName == "" {
			return
		}

		// Get the logical port info
		logicalPort := util.GetLogicalPortName(pod.Namespace, pod.Name)
		var portInfo *lpInfo

		// Get the logical port info from the cache, if that fails, retry.
		// If the gotten LSP is scheduled for removal, retry (stateful-sets).
		//
		// 24ms is chosen because gomega.Eventually default timeout is 50ms
		// libovsdb transactions take less than 50ms usually as well so pod create
		// should be done within a couple iterations
		retryErr := wait.PollImmediate(24*time.Millisecond, 1*time.Second, func() (bool, error) {
			var err error

			// Retry if getting pod LSP from the cache fails
			portInfo, err = oc.logicalPortCache.get(logicalPort)
			if err != nil {
				klog.Warningf("Failed to get LSP for pod %s/%s for shared networkGroup %s refetching err: %v", pod.Namespace, pod.Name, spgInfo.pgName, err)
				return false, nil
			}

			// Retry if LSP is scheduled for deletion
			if !portInfo.expires.IsZero() {
				klog.Warningf("Stale LSP %s for network shared networkGroup %s found in cache refetching", portInfo.name, spgInfo.pgName)
				return false, nil
			}

			// LSP get succeeded and LSP is up to fresh, exit and continue
			klog.V(5).Infof("Fresh LSP %s for shared networkGroup %s found in cache", portInfo.name, spgInfo.pgName)
			return true, nil
		})
		if retryErr != nil {
			// Failed to get an up to date version of the LSP from the cache
			klog.Warningf("Failed to get LSP after multiple retries for pod %s/%s for shared networkGroup %s err: %v",
				pod.Namespace, pod.Name, spgInfo.pgName, retryErr)
			return
		}

		// if this pod is somehow already added to this policy, then skip
		if _, ok := spgInfo.localPods.LoadOrStore(portInfo.name, portInfo); ok {
			return
		}

		getPortsInfoMap.Store(portInfo.uuid, portInfo)
	}
	for _, obj := range objs {
		pod := obj.(*kapi.Pod)

		if util.PodCompleted(pod) {
			// if pod is completed, do not add it to NP port group
			continue
		}

		getPolicyPortsWg.Add(1)
		go getPortInfo(pod)
	}
	getPolicyPortsWg.Wait()

	getPortsInfoMap.Range(func(key interface{}, value interface{}) bool {
		policyPorts = append(policyPorts, key.(string))
		return true
	})

	return
}

func (oc *Controller) processLocalPodSelectorDelPods(spgInfo *sharedPortGroupInfo,
	objs ...interface{}) (policyPorts []string) {
	klog.Infof("Processing shared networkGroup %s  to delete %d local pods...", spgInfo.pgName, len(objs))
	policyPorts = make([]string, 0, len(objs))
	for _, obj := range objs {
		pod := obj.(*kapi.Pod)

		if pod.Spec.NodeName == "" {
			continue
		}

		// Get the logical port info
		logicalPort := util.GetLogicalPortName(pod.Namespace, pod.Name)
		portInfo, err := oc.logicalPortCache.get(logicalPort)
		// pod is not yet handled
		// no big deal, we'll get the update when it is.
		if err != nil {
			klog.Errorf(err.Error())
			return
		}

		// If we never saw this pod, short-circuit
		if _, ok := spgInfo.localPods.LoadAndDelete(logicalPort); !ok {
			continue
		}

		policyPorts = append(policyPorts, portInfo.uuid)
	}

	return
}

// handleLocalPodSelectorAddFunc adds new pods to an existing shared group
func (oc *Controller) handleLocalPodSelectorAddFunc(spgInfo *sharedPortGroupInfo, obj interface{}) error {
	policyPorts := oc.processLocalPodSelectorSetPods(spgInfo, obj)

	klog.V(5).Infof("Add ports %v to portGroup %s", policyPorts, spgInfo.pgName)
	err := libovsdbops.AddPortsToPortGroup(oc.nbClient, spgInfo.pgName, policyPorts...)
	if err != nil {
		return fmt.Errorf("failed to add ports %v to portgroup %s: %v", policyPorts, spgInfo.pgName, err)
	}
	return nil
}

// handleLocalPodSelectorDelFunc delete a removed pod to an existing NetworkPolicy
func (oc *Controller) handleLocalPodSelectorDelFunc(spgInfo *sharedPortGroupInfo, obj interface{}) error {
	policyPorts := oc.processLocalPodSelectorDelPods(spgInfo, obj)
	err := libovsdbops.DeletePortsFromPortGroup(oc.nbClient, spgInfo.pgName, policyPorts...)
	if err != nil {
		return fmt.Errorf("failed to delete ports %v from portgroup %s: %v", policyPorts, spgInfo.pgName, err)
	}
	return nil
}

func (oc *Controller) createSharedPortGroup(policy *knet.NetworkPolicy, np *networkPolicy) (*sharedPortGroupInfo, error) {
	klog.Infof("Create shared portGroup %s", np.sharePortGroupName)

	// create shared portGroup
	pgName := np.sharePortGroupName
	pgHashName := hashedPortGroup(pgName)
	sharedPG := libovsdbops.BuildPortGroup(pgHashName, pgName, nil, nil)
	sharedPG.ExternalIDs[sharePortGroupExtIdKey] = "true"
	sharedPG.ExternalIDs[namespaceACLExtIdKey] = policy.Namespace

	spgInfo := &sharedPortGroupInfo{
		pgName:           pgHashName,
		lspIngressRefCnt: 0,
		lspEgressRefCnt:  0,
		podHandler:       nil,
		localPods:        sync.Map{},
		policies:         map[string]bool{},
		localPodSelector: policy.Spec.PodSelector,
	}
	var selectedPods []interface{}
	handleSharedPortGroupInitialSelectedPods := func(objs []interface{}) error {
		selectedPods = objs
		policyPorts := oc.processLocalPodSelectorSetPods(spgInfo, selectedPods...)
		sharedPG.Ports = policyPorts
		err := libovsdbops.CreateOrUpdatePortGroups(oc.nbClient, sharedPG)
		if err != nil {
			return fmt.Errorf("failed to create port group %s: %v", sharedPG.Name, err)
		}
		return nil
	}
	// NetworkPolicy is validated by the apiserver
	sel, _ := metav1.LabelSelectorAsSelector(&policy.Spec.PodSelector)
	retryLocalPods := NewRetryObjs(
		factory.LocalPodSelectorType,
		policy.Namespace,
		sel,
		handleSharedPortGroupInitialSelectedPods,
		spgInfo,
	)

	podHandler, err := oc.WatchResource(retryLocalPods)
	if err != nil {
		klog.Errorf("Failed WatchResource for handleLocalPodSelector: %v", err)
		return nil, err
	}

	spgInfo.podHandler = podHandler
	return spgInfo, nil
}

func (oc *Controller) deleteSharedPortGroup(np *networkPolicy, isLastPolicyInSharedGroup bool, ops []ovsdb.Operation) error {
	var err error

	// delete shared port group if this is the last policy in the port group
	if isLastPolicyInSharedGroup {
		klog.V(5).Infof("Delete shared port group %s", np.portGroupName)
		ops, err = libovsdbops.DeletePortGroupsOps(oc.nbClient, ops, np.portGroupName)
		if err != nil {
			return fmt.Errorf("failed to delete shared portgroup ops for %s, error: %v", np.portGroupName, err)
		}
	}

	recordOps, txOkCallBack, _, err := metrics.GetConfigDurationRecorder().AddOVN(oc.nbClient, "networkpolicy",
		np.policy.Namespace, np.policy.Name)
	if err != nil {
		klog.Errorf("Failed to record config duration: %v", err)
	}
	ops = append(ops, recordOps...)

	_, err = libovsdbops.TransactAndCheck(oc.nbClient, ops)
	if err != nil {
		return fmt.Errorf("failed to execute ovsdb txn to delete network policy: %s/%s, error: %v",
			np.namespace, np.name, err)
	}
	txOkCallBack()
	return nil
}

func (oc *Controller) createSharedPolicy(np *networkPolicy, policy *knet.NetworkPolicy, aclLogging *ACLLoggingLevels) (err error) {
	var sharedPortGroupName string
	var spgInfo *sharedPortGroupInfo
	klog.Infof("Policy %s/%s to be added to shared portGroup", np.namespace, np.name)
	// create shared portGroup
	found := false
	oc.spgInfoMutex.Lock()
	spgInfoMap, ok := oc.spgInfoMap[np.namespace]
	if ok {
		for sharedPortGroupName, spgInfo = range spgInfoMap {
			if reflect.DeepEqual(spgInfo.localPodSelector, policy.Spec.PodSelector) {
				found = true
				break
			}
		}
	}

	if !found {
		np.sharePortGroupName = getSharedPortGroupName(policy)
		spgInfo, err = oc.createSharedPortGroup(policy, np)
		if err != nil {
			oc.spgInfoMutex.Unlock()
			return err
		}
		if _, ok := oc.spgInfoMap[np.namespace]; !ok {
			oc.spgInfoMap[np.namespace] = map[string]*sharedPortGroupInfo{np.sharePortGroupName: spgInfo}
		} else {
			oc.spgInfoMap[np.namespace][np.sharePortGroupName] = spgInfo
		}
	} else {
		np.sharePortGroupName = sharedPortGroupName
	}
	np.portGroupName = spgInfo.pgName
	klog.Infof("Policy %s/%s to be added to shared portGroup %s hash %s",
		np.namespace, np.name, np.sharePortGroupName, np.portGroupName)
	spgInfo.Lock()
	oc.spgInfoMutex.Unlock()

	defer spgInfo.Unlock()
	_, ok = spgInfo.policies[getPolicyNamespacedName(policy)]
	if ok {
		return nil
	}

	acls := []*nbdb.ACL{}
	if np.ingressDefDeny {
		if spgInfo.lspIngressRefCnt == 0 {
			ingressDenyACL, ingressAllowACL := buildDenyACLs("", np.portGroupName, aclLogging, knet.PolicyTypeIngress)
			acls = append(acls, ingressDenyACL)
			acls = append(acls, ingressAllowACL)
		}
	}
	if np.egressDefDeny {
		if spgInfo.lspEgressRefCnt == 0 {
			egressDenyACL, egressAllowACL := buildDenyACLs("", np.portGroupName, aclLogging, knet.PolicyTypeEgress)
			acls = append(acls, egressDenyACL)
			acls = append(acls, egressAllowACL)
		}
	}

	defer func() {
		if err == nil {
			if np.ingressDefDeny {
				spgInfo.lspIngressRefCnt++
			}
			if np.egressDefDeny {
				spgInfo.lspEgressRefCnt++
			}
			spgInfo.policies[getPolicyNamespacedName(policy)] = true
		}
	}()

	if len(acls) == 0 {
		return nil
	}

	klog.Infof("Creating default deny acls %+v for network policy %s/%s shared portGroup",
		acls, np.namespace, np.name)
	ops, err := libovsdbops.CreateOrUpdateACLsOps(oc.nbClient, nil, acls...)
	if err != nil {
		return err
	}

	ops, err = libovsdbops.AddACLsToPortGroupOps(oc.nbClient, ops, np.portGroupName, acls...)
	if err != nil {
		return err
	}
	_, err = libovsdbops.TransactAndCheck(oc.nbClient, ops)
	if err != nil {
		return err
	}
	return nil
}

func (oc *Controller) deleteSharedPolicy(np *networkPolicy) error {
	klog.V(5).Infof("Policy %s/%s to be removed from shared portGroup %s", np.namespace, np.name, np.sharePortGroupName)

	// if sharePortGroupName has not been assigned, nothing to do
	if np.sharePortGroupName == "" {
		return nil
	}

	// delete shared portGroup
	oc.spgInfoMutex.Lock()
	defer oc.spgInfoMutex.Unlock()
	spgInfoMap, ok := oc.spgInfoMap[np.namespace]
	if !ok {
		return nil
	}
	spgInfo, ok := spgInfoMap[np.sharePortGroupName]
	if !ok {
		return nil
	}
	spgInfo.Lock()
	defer spgInfo.Unlock()

	ops := []ovsdb.Operation{}
	// for delete operation, give any aclLogging
	aclLogging := ACLLoggingLevels{nbdb.ACLSeverityInfo, nbdb.ACLSeverityInfo}
	// check if this is the last policy sharing this port group
	expectedLastPolicyNum := 0
	_, ok = spgInfo.policies[getNPNamespacedName(np)]
	if ok {
		expectedLastPolicyNum = 1
	}
	isLastPolicyInSharedGroup := len(spgInfo.policies) == expectedLastPolicyNum
	if ok {
		// if this policy is in spgInfo.policies, its default deny acls have been processed
		acls := []*nbdb.ACL{}
		// delete shared portGroup if this is the last policy sharing the perNamespace portGroup
		if np.ingressDefDeny {
			if spgInfo.lspIngressRefCnt == 1 {
				ingressDenyACL, ingressAllowACL := buildDenyACLs("", np.portGroupName, &aclLogging, knet.PolicyTypeIngress)
				acls = append(acls, ingressDenyACL)
				acls = append(acls, ingressAllowACL)
			} else if spgInfo.lspIngressRefCnt <= 0 {
				return fmt.Errorf("unexpected ingress refCount number sharing portGroup %s", np.sharePortGroupName)
			}
		}
		if np.egressDefDeny {
			if spgInfo.lspEgressRefCnt == 1 {
				egressDenyACL, egressAllowACL := buildDenyACLs("", np.portGroupName, &aclLogging, knet.PolicyTypeEgress)
				acls = append(acls, egressDenyACL)
				acls = append(acls, egressAllowACL)
			} else if spgInfo.lspEgressRefCnt <= 0 {
				klog.Errorf("Unexpected egress refCount number sharing portGroup %s", np.sharePortGroupName)
			}
		}

		if np.created {
			localAcls := oc.buildNetworkPolicyACLs(np, &aclLogging)
			acls = append(acls, localAcls...)
		}
		klog.V(5).Infof("Destroying acls %+v for network policy %s/%s sharing portGroup %s",
			acls, np.namespace, np.name, np.sharePortGroupName)

		matchAcls, err := libovsdbops.FindACLsWithUUID(oc.nbClient, acls)
		if err != nil {
			return fmt.Errorf("failed to find acls %+v for network policy %s/%s sharing portGroup %s: %v",
				acls, np.namespace, np.name, np.sharePortGroupName, err.Error())
		}
		ops, err = libovsdbops.DeleteACLsOps(oc.nbClient, ops, []string{spgInfo.pgName}, nil, matchAcls...)
		if err != nil {
			return fmt.Errorf("failed to make ops for remove acls %+v: %v", matchAcls, err.Error())
		}

		err = oc.deleteSharedPortGroup(np, isLastPolicyInSharedGroup, ops)
		if err != nil {
			return err
		}

		if np.ingressDefDeny {
			spgInfo.lspIngressRefCnt--
		}
		if np.egressDefDeny {
			spgInfo.lspEgressRefCnt--
		}
		delete(spgInfo.policies, getNPNamespacedName(np))
	} else {
		err := oc.deleteSharedPortGroup(np, isLastPolicyInSharedGroup, ops)
		if err != nil {
			return err
		}
	}
	if isLastPolicyInSharedGroup {
		if spgInfo.podHandler != nil {
			oc.watchFactory.RemovePodHandler(spgInfo.podHandler)
			spgInfo.podHandler = nil
		}

		delete(spgInfoMap, np.sharePortGroupName)
		if len(spgInfoMap) == 0 {
			delete(oc.spgInfoMap, np.namespace)
		}
	}

	return nil
}

// we only need to create an address set if there is a podSelector or namespaceSelector
func hasAnyLabelSelector(peers []knet.NetworkPolicyPeer) bool {
	for _, peer := range peers {
		if peer.PodSelector != nil || peer.NamespaceSelector != nil {
			return true
		}
	}
	return false
}

// createNetworkPolicy creates a network policy
func (oc *Controller) createNetworkPolicy(np *networkPolicy, policy *knet.NetworkPolicy, aclLogging *ACLLoggingLevels) error {

	np.Lock()

	if aclLogging.Deny != "" || aclLogging.Allow != "" {
		klog.Infof("ACL logging for network policy %s in namespace %s set to deny=%s, allow=%s",
			policy.Name, policy.Namespace, aclLogging.Deny, aclLogging.Allow)
	}

	type policyHandler struct {
		gress             *gressPolicy
		namespaceSelector *metav1.LabelSelector
		podSelector       *metav1.LabelSelector
	}
	var policyHandlers []policyHandler
	// Go through each ingress rule.  For each ingress rule, create an
	// addressSet for the peer pods.
	for i, ingressJSON := range policy.Spec.Ingress {
		klog.V(5).Infof("Network policy ingress is %+v", ingressJSON)

		ingress := newGressPolicy(knet.PolicyTypeIngress, i, policy.Namespace, policy.Name)

		// Each ingress rule can have multiple ports to which we allow traffic.
		for _, portJSON := range ingressJSON.Ports {
			ingress.addPortPolicy(&portJSON)
		}

		if hasAnyLabelSelector(ingressJSON.From) {
			klog.V(5).Infof("Network policy %s with ingress rule %s has a selector", policy.Name, ingress.policyName)
			if err := ingress.ensurePeerAddressSet(oc.addressSetFactory); err != nil {
				np.Unlock()
				return err
			}
			// Start service handlers ONLY if there's an ingress Address Set
			if err := oc.handlePeerService(policy, ingress, np); err != nil {
				np.Unlock()
				return err
			}
		}

		for _, fromJSON := range ingressJSON.From {
			// Add IPBlock to ingress network policy
			if fromJSON.IPBlock != nil {
				ingress.addIPBlock(fromJSON.IPBlock)
			}

			policyHandlers = append(policyHandlers, policyHandler{
				gress:             ingress,
				namespaceSelector: fromJSON.NamespaceSelector,
				podSelector:       fromJSON.PodSelector,
			})
		}
		np.ingressPolicies = append(np.ingressPolicies, ingress)
	}

	// Go through each egress rule.  For each egress rule, create an
	// addressSet for the peer pods.
	for i, egressJSON := range policy.Spec.Egress {
		klog.V(5).Infof("Network policy egress is %+v", egressJSON)

		egress := newGressPolicy(knet.PolicyTypeEgress, i, policy.Namespace, policy.Name)

		// Each egress rule can have multiple ports to which we allow traffic.
		for _, portJSON := range egressJSON.Ports {
			egress.addPortPolicy(&portJSON)
		}

		if hasAnyLabelSelector(egressJSON.To) {
			klog.V(5).Infof("Network policy %s with egress rule %s has a selector", policy.Name, egress.policyName)
			if err := egress.ensurePeerAddressSet(oc.addressSetFactory); err != nil {
				np.Unlock()
				return err
			}
		}

		for _, toJSON := range egressJSON.To {
			// Add IPBlock to egress network policy
			if toJSON.IPBlock != nil {
				egress.addIPBlock(toJSON.IPBlock)
			}

			policyHandlers = append(policyHandlers, policyHandler{
				gress:             egress,
				namespaceSelector: toJSON.NamespaceSelector,
				podSelector:       toJSON.PodSelector,
			})
		}
		np.egressPolicies = append(np.egressPolicies, egress)
	}
	np.Unlock()

	for _, handler := range policyHandlers {
		var err error
		if handler.namespaceSelector != nil && handler.podSelector != nil {
			// For each rule that contains both peer namespace selector and
			// peer pod selector, we create a watcher for each matching namespace
			// that populates the addressSet
			err = oc.handlePeerNamespaceAndPodSelector(handler.namespaceSelector, handler.podSelector, handler.gress, np)
		} else if handler.namespaceSelector != nil {
			// For each peer namespace selector, we create a watcher that
			// populates ingress.peerAddressSets
			err = oc.handlePeerNamespaceSelector(handler.namespaceSelector, handler.gress, np)
		} else if handler.podSelector != nil {
			// For each peer pod selector, we create a watcher that
			// populates the addressSet
			err = oc.handlePeerPodSelector(policy, handler.podSelector,
				handler.gress, np)
		}
		if err != nil {
			return fmt.Errorf("failed to handle policy handler selector: %v", err)
		}
	}

	err := oc.createSharedPolicy(np, policy, aclLogging)
	if err != nil {
		return fmt.Errorf("failed to create shared portGroup for policy %s/%s: %v", np.namespace, np.name, err)
	}

	np.Lock()
	defer np.Unlock()
	if np.deleted {
		return nil
	}
	acls := oc.buildNetworkPolicyACLs(np, aclLogging)
	klog.Infof("Creating local policy acls %+v for network policy %s/%s sharing portGroup %s",
		acls, np.namespace, np.name, np.sharePortGroupName)
	ops, err := libovsdbops.CreateOrUpdateACLsOps(oc.nbClient, nil, acls...)
	if err != nil {
		return fmt.Errorf("failed to create local acls ops for network policy %s/%s: %v", np.namespace, np.name, err)
	}

	ops, err = libovsdbops.AddACLsToPortGroupOps(oc.nbClient, ops, np.portGroupName, acls...)
	if err != nil {
		return fmt.Errorf("failed to add local acls ops to portGroup %s for network policy %s/%s: %v", np.portGroupName, np.namespace, np.name, err)
	}
	_, err = libovsdbops.TransactAndCheck(oc.nbClient, ops)
	if err != nil {
		return fmt.Errorf("failed to add local acls to portGroup %s for network policy %s/%s: %v", np.portGroupName, np.namespace, np.name, err)
	}
	np.created = true
	return nil
}

// addNetworkPolicy creates and applies OVN ACLs to pod logical switch
// ports from Kubernetes NetworkPolicy objects using OVN Port Groups
func (oc *Controller) addNetworkPolicy(policy *knet.NetworkPolicy) error {
	klog.Infof("Adding network policy %s in namespace %s", policy.Name,
		policy.Namespace)

	nsInfo, nsUnlock, err := oc.ensureNamespaceLocked(policy.Namespace, false, nil)
	if err != nil {
		return fmt.Errorf("unable to ensure namespace for network policy: %s, namespace: %s, error: %v",
			policy.Name, policy.Namespace, err)
	}
	_, alreadyExists := nsInfo.networkPolicies[policy.Name]
	if alreadyExists {
		nsUnlock()
		// If this scenario happens something is wrong in our code, however if we return error here
		// the NP will be retried infinitely to be added. Another option would be to fatal out here
		// but that seems too aggressive
		klog.Errorf("During add network policy, policy already found for %s/%s, this should not happen!",
			policy.Namespace, policy.Name)
		return nil
	}
	np := NewNetworkPolicy(policy)

	aclLogging := nsInfo.aclLogging
	nsUnlock()
	if err := oc.createNetworkPolicy(np, policy, &aclLogging); err != nil {
		if err := oc.deleteNetworkPolicy(policy, np); err != nil {
			// rollback failed, add to retry to cleanup
			key := getPolicyNamespacedName(policy)
			oc.retryNetworkPolicies.DoWithLock(key, func(key string) {
				oc.retryNetworkPolicies.initRetryObjWithDelete(policy, key, np, false)
			})
		}
		return fmt.Errorf("failed to create Network Policy: %s/%s, error: %v",
			policy.Namespace, policy.Name, err)
	}

	// Now do nsinfo operations to set the policy
	nsInfo, nsUnlock, err = oc.ensureNamespaceLocked(policy.Namespace, false, nil)
	if err != nil {
		// rollback network policy
		if err := oc.deleteNetworkPolicy(policy, np); err != nil {
			// rollback failed, add to retry to cleanup
			key := getPolicyNamespacedName(policy)
			oc.retryNetworkPolicies.DoWithLock(key, func(key string) {
				oc.retryNetworkPolicies.initRetryObjWithDelete(policy, key, np, false)
			})
		}
		return fmt.Errorf("unable to ensure namespace for network policy: %s, namespace: %s, error: %v",
			policy.Name, policy.Namespace, err)
	}
	defer nsUnlock()
	// Update default ACL log level, since namespace update will only affect namespace-wide ACLs if
	// len(nsInfo.networkPolicies) > 0, which is not the case at this point
	if nsInfo.aclLogging.Deny != aclLogging.Deny && len(nsInfo.networkPolicies) == 0 {
		if err := oc.updateACLLoggingForSharedGroupAcls(nsInfo, np); err != nil {
			klog.Warningf(err.Error())
		} else {
			klog.Infof("Policy %s: ACL logging setting of shared port group updated to deny=%s allow=%s",
				getPolicyNamespacedName(policy), nsInfo.aclLogging.Deny, nsInfo.aclLogging.Allow)
		}
	}
	// The allow logging level was updated while we were creating the policy if
	// the current allow logging level is different than the one we have from
	// the first time we locked the namespace.
	// namespace update handler couldn't update this netpol, since it's not in the nsInfo.networkPolicies map yet.
	if nsInfo.aclLogging.Allow != aclLogging.Allow {
		if err := oc.updateACLLoggingForPolicy(np, &nsInfo.aclLogging); err != nil {
			klog.Warningf(err.Error())
		} else {
			klog.Infof("Policy %s: ACL logging setting updated to deny=%s allow=%s",
				getPolicyNamespacedName(policy), nsInfo.aclLogging.Deny, nsInfo.aclLogging.Allow)
		}
	}
	nsInfo.networkPolicies[policy.Name] = np
	return nil
}

// buildNetworkPolicyACLs builds the ACLS associated with the 'gress policies
// of the provided network policy.
func (oc *Controller) buildNetworkPolicyACLs(np *networkPolicy, aclLogging *ACLLoggingLevels) []*nbdb.ACL {
	acls := []*nbdb.ACL{}
	for _, gp := range np.ingressPolicies {
		acl := gp.buildLocalPodACLs(np.portGroupName, aclLogging)
		acls = append(acls, acl...)
	}
	for _, gp := range np.egressPolicies {
		acl := gp.buildLocalPodACLs(np.portGroupName, aclLogging)
		acls = append(acls, acl...)
	}

	return acls
}

// deleteNetworkPolicy removes a network policy
// If np is provided, then deletion may still occur without a lock on nsInfo
func (oc *Controller) deleteNetworkPolicy(policy *knet.NetworkPolicy, np *networkPolicy) error {
	klog.Infof("Deleting network policy %s in namespace %s, np is nil: %v",
		policy.Name, policy.Namespace, np == nil)

	nsInfo, nsUnlock := oc.getNamespaceLocked(policy.Namespace, false)
	if nsInfo == nil {
		// if we didn't get nsInfo and np is nil, we cannot proceed
		if np == nil {
			klog.Warningf("Failed to get namespace lock when deleting policy %s in namespace %s",
				policy.Name, policy.Namespace)
			return nil
		}

		if err := oc.destroyNetworkPolicy(np); err != nil {
			return fmt.Errorf("failed to destroy network policy: %s/%s: %v", policy.Namespace, policy.Name, err)
		}
		return nil
	}

	defer nsUnlock()

	// try to use the more official np found in nsInfo
	// also, if this is called during the process of the policy creation, the current network policy
	// may not be added to nsInfo.networkPolicies yet.
	foundNp, ok := nsInfo.networkPolicies[policy.Name]
	if ok {
		np = foundNp
	}
	if np == nil {
		klog.Warningf("Unable to delete network policy: %s/%s since its not found in cache", policy.Namespace, policy.Name)
		return nil
	}
	if err := oc.destroyNetworkPolicy(np); err != nil {
		return fmt.Errorf("failed to destroy network policy: %s/%s: %v", policy.Namespace, policy.Name, err)
	}

	delete(nsInfo.networkPolicies, policy.Name)
	return nil
}

// destroys a particular network policy
// if nsInfo is provided, the entire port group will be deleted for ingress/egress directions
// lastPolicy indicates if no other policies are using the respective portgroup anymore
func (oc *Controller) destroyNetworkPolicy(np *networkPolicy) error {
	np.Lock()
	defer np.Unlock()
	np.deleted = true
	oc.shutdownHandlers(np)

	var err error
	err = oc.deleteSharedPolicy(np)
	if err != nil {
		return err
	}

	// Delete ingress/egress address sets
	for _, policy := range np.ingressPolicies {
		err = policy.destroy()
		if err != nil {
			return fmt.Errorf("failed to delete network policy ingress address sets, policy: %s/%s, error: %v",
				np.namespace, np.name, err)
		}
	}
	for _, policy := range np.egressPolicies {
		err = policy.destroy()
		if err != nil {
			return fmt.Errorf("failed to delete network policy egress address sets, policy: %s/%s, error: %v",
				np.namespace, np.name, err)
		}
	}
	return nil
}

// handlePeerPodSelectorAddUpdate adds the IP address of a pod that has been
// selected as a peer by a NetworkPolicy's ingress/egress section to that
// ingress/egress address set
func (oc *Controller) handlePeerPodSelectorAddUpdate(gp *gressPolicy, objs ...interface{}) error {
	pods := make([]*kapi.Pod, 0, len(objs))
	for _, obj := range objs {
		pod := obj.(*kapi.Pod)
		if pod.Spec.NodeName == "" {
			continue
		}
		pods = append(pods, pod)
	}
	// If no IP is found, the pod handler may not have added it by the time the network policy handler
	// processed this pod event. It will grab it during the pod update event to add the annotation,
	// so don't log an error here.
	if err := gp.addPeerPods(pods...); err != nil && !errors.Is(err, util.ErrNoPodIPFound) {
		return err
	}
	return nil
}

// handlePeerPodSelectorDelete removes the IP address of a pod that no longer
// matches a NetworkPolicy ingress/egress section's selectors from that
// ingress/egress address set
func (oc *Controller) handlePeerPodSelectorDelete(gp *gressPolicy, obj interface{}) error {
	pod := obj.(*kapi.Pod)
	if pod.Spec.NodeName == "" {
		klog.Infof("Pod %s/%s not scheduled on any node, skipping it", pod.Namespace, pod.Name)
		return nil
	}
	if err := gp.deletePeerPod(pod); err != nil {
		return err
	}
	return nil
}

// handlePeerServiceSelectorAddUpdate adds the VIP of a service that selects
// pods that are selected by the Network Policy
func (oc *Controller) handlePeerServiceAdd(gp *gressPolicy, service *kapi.Service) error {
	klog.V(5).Infof("A Service: %s matches the namespace as the gress policy: %s", service.Name, gp.policyName)
	return gp.addPeerSvcVip(oc.nbClient, service)
}

// handlePeerServiceDelete removes the VIP of a service that selects
// pods that are selected by the Network Policy
func (oc *Controller) handlePeerServiceDelete(gp *gressPolicy, service *kapi.Service) error {
	return gp.deletePeerSvcVip(oc.nbClient, service)
}

type NetworkPolicyExtraParameters struct {
	np          *networkPolicy
	gp          *gressPolicy
	podSelector labels.Selector
}

// Watch services that are in the same Namespace as the NP
// To account for hairpined traffic
func (oc *Controller) handlePeerService(
	policy *knet.NetworkPolicy, gp *gressPolicy, np *networkPolicy) error {
	// start watching services in the same namespace as the network policy
	retryPeerServices := NewRetryObjs(
		factory.PeerServiceType,
		policy.Namespace,
		nil, nil,
		&NetworkPolicyExtraParameters{gp: gp})

	serviceHandler, err := oc.WatchResource(retryPeerServices)
	if err != nil {
		klog.Errorf("Failed WatchResource for handlePeerService: %v", err)
		return err
	}

	np.svcHandlerList = append(np.svcHandlerList, serviceHandler)
	return nil
}

func (oc *Controller) handlePeerPodSelector(
	policy *knet.NetworkPolicy, podSelector *metav1.LabelSelector,
	gp *gressPolicy, np *networkPolicy) error {

	// NetworkPolicy is validated by the apiserver; this can't fail.
	sel, _ := metav1.LabelSelectorAsSelector(podSelector)

	// start watching pods in the same namespace as the network policy and selected by the
	// label selector
	retryPeerPods := NewRetryObjs(
		factory.PeerPodSelectorType,
		policy.Namespace,
		sel, nil,
		&NetworkPolicyExtraParameters{gp: gp})

	podHandler, err := oc.WatchResource(retryPeerPods)
	if err != nil {
		klog.Errorf("Failed WatchResource for handlePeerPodSelector: %v", err)
		return err
	}

	np.podHandlerList = append(np.podHandlerList, podHandler)
	return nil
}

func (oc *Controller) handlePeerNamespaceAndPodSelector(
	namespaceSelector *metav1.LabelSelector,
	podSelector *metav1.LabelSelector,
	gp *gressPolicy,
	np *networkPolicy) error {

	// NetworkPolicy is validated by the apiserver; this can't fail.
	nsSel, _ := metav1.LabelSelectorAsSelector(namespaceSelector)
	podSel, _ := metav1.LabelSelectorAsSelector(podSelector)

	// start watching namespaces selected by the namespace selector nsSel;
	// upon namespace add event, start watching pods in that namespace selected
	// by the label selector podSel
	retryPeerNamespaces := NewRetryObjs(
		factory.PeerNamespaceAndPodSelectorType,
		"", nsSel, nil,
		&NetworkPolicyExtraParameters{
			gp:          gp,
			np:          np,
			podSelector: podSel}, // will be used in the addFunc to create a pod handler
	)

	namespaceHandler, err := oc.WatchResource(retryPeerNamespaces)
	if err != nil {
		klog.Errorf("Failed WatchResource for handlePeerNamespaceAndPodSelector: %v", err)
		return err
	}

	np.nsHandlerList = append(np.nsHandlerList, namespaceHandler)
	return nil
}

func (oc *Controller) handlePeerNamespaceSelectorOnUpdate(np *networkPolicy, gp *gressPolicy, doUpdate func() bool) error {
	aclLoggingLevels := oc.GetNamespaceACLLogging(np.namespace)
	np.Lock()
	defer np.Unlock()
	// This needs to be a write lock because there's no locking around 'gress policies
	if !np.deleted && doUpdate() {
		acls := gp.buildLocalPodACLs(np.portGroupName, aclLoggingLevels)
		ops, err := libovsdbops.CreateOrUpdateACLsOps(oc.nbClient, nil, acls...)
		if err != nil {
			return err
		}
		ops, err = libovsdbops.AddACLsToPortGroupOps(oc.nbClient, ops, np.portGroupName, acls...)
		if err != nil {
			return err
		}
		_, err = libovsdbops.TransactAndCheck(oc.nbClient, ops)
		if err != nil {
			return err
		}
	}
	return nil
}

func (oc *Controller) handlePeerNamespaceSelector(
	namespaceSelector *metav1.LabelSelector,
	gress *gressPolicy, np *networkPolicy) error {

	// NetworkPolicy is validated by the apiserver; this can't fail.
	sel, _ := metav1.LabelSelectorAsSelector(namespaceSelector)

	// start watching namespaces selected by the namespace selector
	retryPeerNamespaces := NewRetryObjs(
		factory.PeerNamespaceSelectorType,
		"", sel, nil,
		&NetworkPolicyExtraParameters{gp: gress, np: np},
	)

	namespaceHandler, err := oc.WatchResource(retryPeerNamespaces)
	if err != nil {
		klog.Errorf("Failed WatchResource for handlePeerNamespaceSelector: %v", err)
		return err
	}

	np.nsHandlerList = append(np.nsHandlerList, namespaceHandler)
	return nil
}

func (oc *Controller) shutdownHandlers(np *networkPolicy) {
	for _, handler := range np.podHandlerList {
		oc.watchFactory.RemovePodHandler(handler)
	}
	for _, handler := range np.nsHandlerList {
		oc.watchFactory.RemoveNamespaceHandler(handler)
	}
	for _, handler := range np.svcHandlerList {
		oc.watchFactory.RemoveServiceHandler(handler)
	}
}

func getPolicyNamespacedName(policy *knet.NetworkPolicy) string {
	return fmt.Sprintf("%v/%v", policy.Namespace, policy.Name)
}

func getNPNamespacedName(np *networkPolicy) string {
	return fmt.Sprintf("%v/%v", np.namespace, np.name)
}
