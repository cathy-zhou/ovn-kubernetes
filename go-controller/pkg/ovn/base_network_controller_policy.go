package ovn

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	libovsdbclient "github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdbops"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/metrics"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	kapi "k8s.io/api/core/v1"
	knet "k8s.io/api/networking/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kerrorsutil "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	utilnet "k8s.io/utils/net"
)

type netpolDefaultDenyACLType string

const (
	// netpolDefaultDenyACLType is used to distinguish default deny and arp allow acls create for the same port group
	defaultDenyACL netpolDefaultDenyACLType = "defaultDeny"
	arpAllowACL    netpolDefaultDenyACLType = "arpAllow"
	// port groups suffixes
	// ingressDefaultDenySuffix is the suffix used when creating the ingress port group for a namespace
	ingressDefaultDenySuffix = "ingressDefaultDeny"
	// egressDefaultDenySuffix is the suffix used when creating the ingress port group for a namespace
	egressDefaultDenySuffix = "egressDefaultDeny"
	// arpAllowPolicyMatch is the match used when creating default allow ARP ACLs for a namespace
	arpAllowPolicyMatch   = "(arp || nd)"
	allowHairpinningACLID = "allow-hairpinning"
	// ovnStatelessNetPolAnnotationName is an annotation on K8s Network Policy resource to specify that all
	// the resulting OVN ACLs must be created as stateless
	ovnStatelessNetPolAnnotationName = "k8s.ovn.org/acl-stateless"
)

type networkPolicy struct {
	// For now networkPolicy has
	// 3 types of global events (those use bnc.networkPolicies to get networkPolicy object)
	// 1. Create network policy - create networkPolicy resources,
	// enable local events, and Update namespace loglevel event
	// 2. Update namespace loglevel - update ACLs for defaultDenyPortGroups and portGroup
	// 3. Delete network policy - disable local events, and Update namespace loglevel event,
	// send deletion signal to already running event handlers, delete resources
	//
	// local events (those use the same networkPolicy object there were created for):
	// 1. peerNamespace events - add/delete gressPolicy address set, update ACLs for portGroup
	//
	// Delete network policy conflict with all other handlers, therefore we need to make sure it only runs
	// when no other handlers are executing, and that no other handlers will try to work with networkPolicy after
	// Delete network policy was called. This can be done with RWLock, if Delete network policy takes Write lock
	// and sets deleted field to true, and all other handlers take RLock and return immediately if deleted is true.
	// Create network Policy can also take Write lock while it is creating required resources.
	//
	// The only other conflict between handlers here is Update namespace loglevel and peerNamespace, since they both update
	// portGroup ACLs, but this conflict is handled with namespace lock, because both these functions need to lock
	// namespace to create/update ACLs with correct loglevel.
	//
	// We also need to make sure handlers of the same type can be executed in parallel, if this is not true, every
	// event handler can have it own additional lock to sync handlers of the same type.
	//
	// Allowed order of locking is namespace Lock -> bnc.networkPolicies key Lock -> networkPolicy.Lock
	// Don't take namespace Lock while holding networkPolicy key lock to avoid deadlock.
	// Don't take RLock from the same goroutine twice, it can lead to deadlock.
	sync.RWMutex

	name            string
	namespace       string
	ingressPolicies []*gressPolicy
	egressPolicies  []*gressPolicy
	isIngress       bool
	isEgress        bool

	// network policy owns only 1 local pod handler
	localPodHandler *factory.Handler
	// peer namespace handlers
	nsHandlerList []*factory.Handler
	// peerAddressSets stores PodSelectorAddressSet keys for peers that this network policy was successfully added to.
	// Required for cleanup.
	peerAddressSets []string

	// protected by networkPolicy.RWMutex
	portGroupName string
	// this is a signal for related event handlers that they are/should be stopped.
	// it will be set to true before any networkPolicy infrastructure is deleted,
	// therefore every handler can either do its work and be sure all required resources are there,
	// or this value will be set to true and handler can't proceed.
	// Use networkPolicy.RLock to read this field and hold it for the whole event handling.
	deleted bool
}

func NewNetworkPolicy(policy *knet.NetworkPolicy) *networkPolicy {
	policyTypeIngress, policyTypeEgress := getPolicyType(policy)
	np := &networkPolicy{
		name:            policy.Name,
		namespace:       policy.Namespace,
		ingressPolicies: make([]*gressPolicy, 0),
		egressPolicies:  make([]*gressPolicy, 0),
		isIngress:       policyTypeIngress,
		isEgress:        policyTypeEgress,
		nsHandlerList:   make([]*factory.Handler, 0),
	}
	return np
}

// syncNetworkPoliciesCommon syncs logical entities associated with existing network policies.
// It serves both networkpolicies (for default network) and multi-networkpolicies (for secondary networks)
func (bnc *BaseNetworkController) syncNetworkPoliciesCommon(expectedPolicies map[string]map[string]bool) error {
	// find network policies that don't exist in k8s anymore, but still present in the dbs, and cleanup.
	// Peer address sets and network policy's port groups (together with acls) will be cleaned up.
	// Delete port groups with acls first, since address sets may be referenced in these acls, and
	// cause SyntaxError in ovn-controller, if address sets deleted first, but acls still reference them.

	// cleanup port groups based on acl search
	// netpol-owned port groups first
	predicateIDs := libovsdbops.NewDbObjectIDs(libovsdbops.ACLNetworkPolicy, bnc.controllerName, nil)
	p := libovsdbops.GetPredicate[*nbdb.ACL](predicateIDs, nil)
	netpolACLs, err := libovsdbops.FindACLsWithPredicate(bnc.nbClient, p)
	if err != nil {
		return fmt.Errorf("cannot find NetworkPolicy ACLs: %v", err)
	}
	stalePGs := sets.Set[string]{}
	for _, netpolACL := range netpolACLs {
		// policy-owned acl
		namespace, policyName, err := parseACLPolicyKey(netpolACL.ExternalIDs[libovsdbops.ObjectNameKey.String()])
		if err != nil {
			return fmt.Errorf("failed to sync stale network policies: acl IDs parsing failed: %w", err)
		}
		if !expectedPolicies[namespace][policyName] {
			// policy doesn't exist on k8s, cleanup
			portGroupName, _ := bnc.getNetworkPolicyPGName(namespace, policyName)
			stalePGs.Insert(portGroupName)
		}
	}
	// default deny port groups
	predicateIDs = libovsdbops.NewDbObjectIDs(libovsdbops.ACLNetpolNamespace, bnc.controllerName, nil)
	p = libovsdbops.GetPredicate[*nbdb.ACL](predicateIDs, nil)
	netpolACLs, err = libovsdbops.FindACLsWithPredicate(bnc.nbClient, p)
	if err != nil {
		return fmt.Errorf("cannot find default deny NetworkPolicy ACLs: %v", err)
	}
	for _, netpolACL := range netpolACLs {
		// default deny acl
		namespace := netpolACL.ExternalIDs[libovsdbops.ObjectNameKey.String()]
		if _, ok := expectedPolicies[namespace]; !ok {
			// no policies in that namespace are found, delete default deny port group
			stalePGs.Insert(bnc.defaultDenyPortGroupName(namespace, ingressDefaultDenySuffix))
			stalePGs.Insert(bnc.defaultDenyPortGroupName(namespace, egressDefaultDenySuffix))
		}
	}
	if len(stalePGs) > 0 {
		sets.List[string](stalePGs)
		err = libovsdbops.DeletePortGroups(bnc.nbClient, sets.List[string](stalePGs)...)
		if err != nil {
			return fmt.Errorf("error removing stale port groups %v: %v", stalePGs, err)
		}
		klog.Infof("Network policy sync cleaned up %d stale port groups", len(stalePGs))
	}

	return nil
}

func getAllowFromNodeACLDbIDs(nodeName, mgmtPortIP, controller string) *libovsdbops.DbObjectIDs {
	return libovsdbops.NewDbObjectIDs(libovsdbops.ACLNetpolNode, controller,
		map[libovsdbops.ExternalIDKey]string{
			libovsdbops.ObjectNameKey: nodeName,
			libovsdbops.IpKey:         mgmtPortIP,
		})
}

// There is no delete function for this ACL type, because the ACL is applied on a node switch.
// When the node is deleted, switch will be deleted by the node sync, and the dependent ACLs will be
// garbage-collected.
func (bnc *BaseNetworkController) addAllowACLFromNode(nodeName string, mgmtPortIP net.IP) error {
	ipFamily := "ip4"
	if utilnet.IsIPv6(mgmtPortIP) {
		ipFamily = "ip6"
	}
	match := fmt.Sprintf("%s.src==%s", ipFamily, mgmtPortIP.String())
	dbIDs := getAllowFromNodeACLDbIDs(nodeName, mgmtPortIP.String(), bnc.controllerName)
	nodeACL := BuildACL(dbIDs, types.DefaultAllowPriority, match,
		nbdb.ACLActionAllowRelated, nil, lportIngress)

	ops, err := libovsdbops.CreateOrUpdateACLsOps(bnc.nbClient, nil, nodeACL)
	if err != nil {
		return fmt.Errorf("failed to create or update ACL %v: %v", nodeACL, err)
	}

	ops, err = libovsdbops.AddACLsToLogicalSwitchOps(bnc.nbClient, ops, nodeName, nodeACL)
	if err != nil {
		return fmt.Errorf("failed to add ACL %v to switch %s: %v", nodeACL, nodeName, err)
	}

	_, err = libovsdbops.TransactAndCheck(bnc.nbClient, ops)
	if err != nil {
		return err
	}

	return nil
}

func (bnc *BaseNetworkController) getDefaultDenyPolicyACLIDs(pgName string, aclDir aclDirection,
	defaultACLType netpolDefaultDenyACLType) *libovsdbops.DbObjectIDs {
	return libovsdbops.NewDbObjectIDs(libovsdbops.ACLNetpolSharedPortGroup, bnc.controllerName,
		map[libovsdbops.ExternalIDKey]string{
			libovsdbops.ObjectNameKey: pgName,
			// in the same namespace there can be 2 default deny port groups, egress and ingress,
			// every port group has default deny and arp allow acl.
			libovsdbops.PolicyDirectionKey: string(aclDir),
			libovsdbops.TypeKey:            string(defaultACLType),
		})
}

func (bnc *BaseNetworkController) defaultDenyPortGroupName(namespace, gressSuffix string) string {
	return hashedPortGroup(bnc.GetNetworkScopedName(namespace)) + "_" + gressSuffix
}

func (bnc *BaseNetworkController) buildDenyACLs(pg string, aclLogging *ACLLoggingLevels,
	aclDir aclDirection) (denyACL, allowACL *nbdb.ACL) {
	denyMatch := getACLMatch(pg, "", aclDir)
	allowMatch := getACLMatch(pg, arpAllowPolicyMatch, aclDir)
	aclPipeline := aclDirectionToACLPipeline(aclDir)

	denyACL = BuildACL(bnc.getDefaultDenyPolicyACLIDs(pg, aclDir, defaultDenyACL),
		types.DefaultDenyPriority, denyMatch, nbdb.ACLActionDrop, aclLogging, aclPipeline)
	allowACL = BuildACL(bnc.getDefaultDenyPolicyACLIDs(pg, aclDir, arpAllowACL),
		types.DefaultAllowPriority, allowMatch, nbdb.ACLActionAllow, nil, aclPipeline)
	return
}

// must be called with namespace lock
func (bnc *BaseNetworkController) updateACLLoggingForPolicy(np *networkPolicy, aclLogging *ACLLoggingLevels, updateAllow, updateDeny bool) error {
	np.RLock()
	defer np.RUnlock()
	if np.deleted || np.portGroupName == "" {
		return nil
	}

	if updateAllow {
		// Predicate for given network policy ACLs
		predicateIDs := libovsdbops.NewDbObjectIDs(libovsdbops.ACLNetworkPolicy, bnc.controllerName, map[libovsdbops.ExternalIDKey]string{
			libovsdbops.ObjectNameKey: getACLPolicyKey(np.namespace, np.name),
		})
		p := libovsdbops.GetPredicate[*nbdb.ACL](predicateIDs, nil)
		if err := UpdateACLLoggingWithPredicate(bnc.nbClient, p, aclLogging); err != nil {
			return err
		}
	}

	if updateDeny {
		predicateIDs := libovsdbops.NewDbObjectIDs(libovsdbops.ACLNetpolSharedPortGroup, bnc.controllerName, map[libovsdbops.ExternalIDKey]string{
			libovsdbops.ObjectNameKey: np.portGroupName,
			libovsdbops.TypeKey:       string(defaultDenyACL),
		})
		p := libovsdbops.GetPredicate[*nbdb.ACL](predicateIDs, nil)
		if err := UpdateACLLoggingWithPredicate(bnc.nbClient, p, aclLogging); err != nil {
			return err
		}
	}
	return nil
}

// handleNetPolNamespaceUpdate should update all network policies related to given namespace.
// Must be called with namespace Lock, should be retriable
func (bnc *BaseNetworkController) handleNetPolNamespaceUpdate(namespace string, nsInfo *namespaceInfo) error {
	// now update network policy specific ACLs
	klog.V(5).Infof("Setting network policy ACLs for ns: %s", namespace)
	for npKey := range nsInfo.relatedNetworkPolicies {
		err := bnc.networkPolicies.DoWithLock(npKey, func(key string) error {
			np, found := bnc.networkPolicies.Load(npKey)
			if !found {
				klog.Errorf("Netpol was deleted from cache, but not from namespace related objects")
				return nil
			}
			return bnc.updateACLLoggingForPolicy(np, &nsInfo.aclLogging, true, true)
		})
		if err != nil {
			return fmt.Errorf("unable to update ACL for network policy %s: %v", npKey, err)
		}
		klog.Infof("ACL for network policy: %s, updated to new log level: %s", npKey, nsInfo.aclLogging.Allow)
	}
	return nil
}

// getPolicyType returns whether the policy is of type ingress and/or egress
func getPolicyType(policy *knet.NetworkPolicy) (bool, bool) {
	var policyTypeIngress bool
	var policyTypeEgress bool

	for _, policyType := range policy.Spec.PolicyTypes {
		if policyType == knet.PolicyTypeIngress {
			policyTypeIngress = true
		} else if policyType == knet.PolicyTypeEgress {
			policyTypeEgress = true
		}
	}

	return policyTypeIngress, policyTypeEgress
}

// getNewLocalPolicyPorts will find and return port info for every given pod obj, that is not found in
// spgInfo.localPods.
// if there are problems with fetching port info from logicalPortCache, error will be added to returned error array.
func (bnc *BaseNetworkController) getNewLocalPolicyPorts(spgInfo *sharedPortGroupInfo,
	objs ...interface{}) (policyPortsToUUIDs map[string]string, policyPortUUIDs []string, errs []error) {

	policyPortUUIDs = []string{}
	policyPortsToUUIDs = map[string]string{}
	errs = []error{}

	for _, obj := range objs {
		pod := obj.(*kapi.Pod)
		if pod.Spec.NodeName == "" {
			// pod is not yet scheduled, will receive update event for it
			continue
		}

		// Skip pods that will never be present in logicalPortCache,
		// e.g. hostNetwork pods, overlay node pods, or completed pods
		if !bnc.podExpectedInLogicalCache(pod) {
			continue
		}

		nadNames := bnc.getPodNADNames(pod)
		for _, nadName := range nadNames {
			logicalPortName := bnc.GetLogicalPortName(pod, nadName)
			if _, ok := spgInfo.localPods.Load(logicalPortName); ok {
				// port is already added for this policy
				continue
			}

			// Return error for retry if
			// 1. getting pod LSP from the cache fails,
			// 2. the gotten LSP is scheduled for removal (stateful-sets).
			portInfo, err := bnc.logicalPortCache.get(pod, nadName)
			if err != nil {
				klog.Warningf("Failed to get get LSP for pod %s/%s NAD %s for shared port group %s, err: %v",
					pod.Namespace, pod.Name, nadName, spgInfo.pgHashName, err)
				errs = append(errs, fmt.Errorf("unable to get port info for pod %s/%s NAD %s", pod.Namespace, pod.Name, nadName))
				continue
			}

			// Add pod to errObjs if LSP is scheduled for deletion
			if !portInfo.expires.IsZero() {
				klog.Warningf("Stale LSP %s for shared port group %s found in cache",
					portInfo.name, spgInfo.pgHashName)
				errs = append(errs, fmt.Errorf("unable to get port info for pod %s/%s NAD %s", pod.Namespace, pod.Name, nadName))
				continue
			}

			// LSP get succeeded and LSP is up to fresh
			klog.V(5).Infof("Fresh LSP %s for shared port group %s found in cache",
				portInfo.name, spgInfo.pgHashName)

			policyPortUUIDs = append(policyPortUUIDs, portInfo.uuid)
			policyPortsToUUIDs[portInfo.name] = portInfo.uuid
		}
	}

	return
}

// getExistingLocalPolicyPorts will find and return port info for every given pod obj, that is present in np.localPods.
func (bnc *BaseNetworkController) getExistingLocalPolicyPorts(spgInfo *sharedPortGroupInfo,
	objs ...interface{}) (policyPortsToUUIDs map[string]string, policyPortUUIDs []string) {
	klog.Infof("Processing shared port group %s to delete %d local pods...", spgInfo.pgHashName, len(objs))

	policyPortUUIDs = []string{}
	policyPortsToUUIDs = map[string]string{}
	for _, obj := range objs {
		pod := obj.(*kapi.Pod)

		nadNames := bnc.getPodNADNames(pod)
		for _, nadName := range nadNames {
			logicalPortName := bnc.GetLogicalPortName(pod, nadName)
			loadedPortUUID, ok := spgInfo.localPods.Load(logicalPortName)
			if !ok {
				// port is already deleted for this policy
				continue
			}
			portUUID := loadedPortUUID.(string)

			policyPortsToUUIDs[logicalPortName] = portUUID
			policyPortUUIDs = append(policyPortUUIDs, portUUID)
		}
	}
	return
}

func (bnc *BaseNetworkController) getNetworkPolicyPGName(namespace, name string) (pgName, readablePGName string) {
	readableGroupName := fmt.Sprintf("%s_%s", namespace, name)
	return hashedPortGroup(bnc.GetNetworkScopedName(readableGroupName)), readableGroupName
}

type policyHandler struct {
	gress             *gressPolicy
	namespaceSelector *metav1.LabelSelector
}

// createNetworkPolicy creates a network policy, should be retriable.
// If network policy with given key exists, it will try to clean it up first, and return an error if it fails.
// No need to log network policy key here, because caller of createNetworkPolicy should prepend error message with
// that information.
func (bnc *BaseNetworkController) createNetworkPolicy(policy *knet.NetworkPolicy, aclLogging *ACLLoggingLevels) (*networkPolicy, error) {
	// To avoid existing connections disruption, make sure to apply allow ACLs before applying deny ACLs.
	// This requires to start peer handlers before local pod handlers.
	// 1. Cleanup old policy if it failed to be created
	// 2. Build gress policies, create addressSets for peers
	// 3. Add policy to default deny port group.
	// 4. Build policy ACLs and port group. All the local pods that this policy
	// selects will be eventually added to this port group.
	// Pods are not added to default deny port groups yet, this is just a preparation step
	// 5. Unlock networkPolicy before starting pod handlers to avoid deadlock
	// since pod handlers take np.RLock
	// 6. Start peer handlers to update all allow rules first
	// 7. Start local pod handlers, that will update networkPolicy and default deny port groups with selected pods.

	npKey := getPolicyKey(policy)
	var np *networkPolicy
	var policyHandlers []*policyHandler

	// network policy will be annotated with this
	// annotation -- [ "k8s.ovn.org/acl-stateless": "true"] for the ingress/egress
	// policies to be added as stateless OVN ACL's.
	// if the above annotation is not present or set to false in network policy,
	// then corresponding egress/ingress policies will be added as stateful OVN ACL's.
	var statelessNetPol bool
	if config.OVNKubernetesFeature.EnableStatelessNetPol {
		// look for stateless annotation if the statlessNetPol feature flag is enabled
		val, ok := policy.Annotations[ovnStatelessNetPolAnnotationName]
		if ok && val == "true" {
			statelessNetPol = true
		}
	}

	err := bnc.networkPolicies.DoWithLock(npKey, func(npKey string) error {
		oldNP, found := bnc.networkPolicies.Load(npKey)
		if found {
			// 1. Cleanup old policy if it failed to be created
			if cleanupErr := bnc.cleanupNetworkPolicy(oldNP); cleanupErr != nil {
				return fmt.Errorf("cleanup for retrying network policy create failed: %v", cleanupErr)
			}
		}
		np, found = bnc.networkPolicies.LoadOrStore(npKey, NewNetworkPolicy(policy))
		if found {
			// that should never happen, because successful cleanup will delete np from bnc.networkPolicies
			return fmt.Errorf("network policy is found in the system, "+
				"while it should've been cleaned up, obj: %+v", np)
		}
		np.Lock()
		npLocked := true
		// we unlock np in the middle of this function, use npLocked to track if it was already unlocked explicitly
		defer func() {
			if npLocked {
				np.Unlock()
			}
		}()
		// no need to check np.deleted, since the object has just been created
		// now we have a new np stored in bnc.networkPolicies
		var err error

		if aclLogging.Deny != "" || aclLogging.Allow != "" {
			klog.Infof("ACL logging for network policy %s in namespace %s set to deny=%s, allow=%s",
				policy.Name, policy.Namespace, aclLogging.Deny, aclLogging.Allow)
		}

		// 2. Build gress policies, create addressSets for peers

		// Consider both ingress and egress rules of the policy regardless of this
		// policy type. A pod is isolated as long as as it is selected by any
		// namespace policy. Since we don't process all namespace policies on a
		// given policy update that might change the isolation status of a selected
		// pod, we have created the allow ACLs derived from the policy rules in case
		// the selected pods become isolated in the future even if that is not their
		// current status.

		// Go through each ingress rule.  For each ingress rule, create an
		// addressSet for the peer pods.
		for i, ingressJSON := range policy.Spec.Ingress {
			klog.V(5).Infof("Network policy ingress is %+v", ingressJSON)

			ingress := newGressPolicy(knet.PolicyTypeIngress, i, policy.Namespace, policy.Name, bnc.controllerName, statelessNetPol, bnc.NetConfInfo)
			// append ingress policy to be able to cleanup created address set
			// see cleanupNetworkPolicy for details
			np.ingressPolicies = append(np.ingressPolicies, ingress)

			// Each ingress rule can have multiple ports to which we allow traffic.
			for _, portJSON := range ingressJSON.Ports {
				ingress.addPortPolicy(&portJSON)
			}

			for _, fromJSON := range ingressJSON.From {
				handler, err := bnc.setupGressPolicy(np, ingress, fromJSON)
				if err != nil {
					return err
				}
				if handler != nil {
					policyHandlers = append(policyHandlers, handler)
				}
			}
		}

		// Go through each egress rule.  For each egress rule, create an
		// addressSet for the peer pods.
		for i, egressJSON := range policy.Spec.Egress {
			klog.V(5).Infof("Network policy egress is %+v", egressJSON)

			egress := newGressPolicy(knet.PolicyTypeEgress, i, policy.Namespace, policy.Name, bnc.controllerName, statelessNetPol, bnc.NetConfInfo)
			// append ingress policy to be able to cleanup created address set
			// see cleanupNetworkPolicy for details
			np.egressPolicies = append(np.egressPolicies, egress)

			// Each egress rule can have multiple ports to which we allow traffic.
			for _, portJSON := range egressJSON.Ports {
				egress.addPortPolicy(&portJSON)
			}

			for _, toJSON := range egressJSON.To {
				handler, err := bnc.setupGressPolicy(np, egress, toJSON)
				if err != nil {
					return err
				}
				if handler != nil {
					policyHandlers = append(policyHandlers, handler)
				}
			}
		}
		klog.Infof("Policy %s added to peer address sets %v", npKey, np.peerAddressSets)

		//// 3. Add policy to default deny port group
		//// Pods are not added to default deny port groups yet, this is just a preparation step
		//err = bnc.addPolicyToDefaultPortGroups(np, aclLogging)
		//if err != nil {
		//	return err
		//}
		// 3. Build local pod policies, create shared port group if needed
		err = bnc.createNetworkLocalPolicy(np, policy, aclLogging)
		if err != nil {
			return fmt.Errorf("failed to create network local policies: %v", err)
		}

		// 5. Unlock network policy before starting pod handlers to avoid deadlock,
		// since pod handlers take np.RLock
		np.Unlock()
		npLocked = false

		// 6. Start peer handlers to update all allow rules first
		for _, handler := range policyHandlers {
			// For each peer namespace selector, we create a watcher that
			// populates ingress.peerAddressSets
			err = bnc.addPeerNamespaceHandler(handler.namespaceSelector, handler.gress, np)
			if err != nil {
				return fmt.Errorf("failed to start peer handler: %v", err)
			}
		}

		return nil
	})
	return np, err
}

func (bnc *BaseNetworkController) setupGressPolicy(np *networkPolicy, gp *gressPolicy,
	peer knet.NetworkPolicyPeer) (*policyHandler, error) {
	// Add IPBlock to ingress network policy
	if peer.IPBlock != nil {
		gp.addIPBlock(peer.IPBlock)
		return nil, nil
	}
	if peer.PodSelector == nil && peer.NamespaceSelector == nil {
		// undefined behaviour
		klog.Errorf("setupGressPolicy failed: all fields unset")
		return nil, nil
	}
	gp.hasPeerSelector = true

	podSelector := peer.PodSelector
	if podSelector == nil {
		// nil pod selector is equivalent to empty pod selector, which selects all
		podSelector = &metav1.LabelSelector{}
	}
	podSel, _ := metav1.LabelSelectorAsSelector(podSelector)
	nsSel, _ := metav1.LabelSelectorAsSelector(peer.NamespaceSelector)

	if podSel.Empty() && (peer.NamespaceSelector == nil || !nsSel.Empty()) {
		// namespace-based filtering
		if peer.NamespaceSelector == nil {
			// nil namespace selector means same namespace
			_, err := gp.addNamespaceAddressSet(np.namespace, bnc.addressSetFactory)
			if err != nil {
				return nil, fmt.Errorf("failed to add namespace address set for gress policy: %w", err)
			}
		} else if !nsSel.Empty() {
			// namespace selector, use namespace address sets
			handler := &policyHandler{
				gress:             gp,
				namespaceSelector: peer.NamespaceSelector,
			}
			return handler, nil
		}
	} else {
		// use podSelector address set
		// np.namespace will be used when fromJSON.NamespaceSelector = nil
		asKey, ipv4as, ipv6as, err := bnc.EnsurePodSelectorAddressSet(
			podSelector, peer.NamespaceSelector, np.namespace, np.getKeyWithKind())
		// even if GetPodSelectorAddressSet failed, add key for future cleanup or retry.
		np.peerAddressSets = append(np.peerAddressSets, asKey)
		if err != nil {
			return nil, fmt.Errorf("failed to ensure pod selector address set %s: %v", asKey, err)
		}
		gp.addPeerAddressSets(ipv4as, ipv6as)
	}
	return nil, nil
}

// addNetworkPolicy creates and applies OVN ACLs to pod logical switch
// ports from Kubernetes NetworkPolicy objects using OVN Port Groups
// if addNetworkPolicy fails, create or delete operation can be retried
func (bnc *BaseNetworkController) addNetworkPolicy(policy *knet.NetworkPolicy) error {
	klog.Infof("Adding network policy %s for network %s", getPolicyKey(policy), bnc.GetNetworkName())
	if !bnc.IsSecondary() && config.Metrics.EnableScaleMetrics {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			metrics.RecordNetpolEvent("add", duration)
		}()
	}

	// To not hold nsLock for the whole process on network policy creation, we do the following:
	// 1. save required namespace information to use for netpol create
	// 2. create network policy without ns Lock
	// 3. lock namespace
	// 4. check if namespace information related to network policy has changed, run the same function as on namespace update
	// 5. subscribe to namespace update events
	// 6. unlock namespace

	// 1. save required namespace information to use for netpol create,
	npKey := getPolicyKey(policy)
	nsInfo, nsUnlock := bnc.getNamespaceLocked(policy.Namespace, true)
	if nsInfo == nil {
		return fmt.Errorf("unable to get namespace for network policy %s: namespace doesn't exist", npKey)
	}
	aclLogging := nsInfo.aclLogging
	nsUnlock()

	// 2. create network policy without ns Lock, cleanup on failure
	var np *networkPolicy
	var err error

	np, err = bnc.createNetworkPolicy(policy, &aclLogging)
	defer func() {
		if err != nil {
			klog.Infof("Create network policy %s failed, try to cleanup", npKey)
			// try to cleanup network policy straight away
			// it will be retried later with add/delete network policy handlers if it fails
			cleanupErr := bnc.networkPolicies.DoWithLock(npKey, func(npKey string) error {
				np, ok := bnc.networkPolicies.Load(npKey)
				if !ok {
					klog.Infof("Deleting policy %s that is already deleted", npKey)
					return nil
				}
				return bnc.cleanupNetworkPolicy(np)
			})
			if cleanupErr != nil {
				klog.Infof("Cleanup for failed create network policy %s returned an error: %v",
					npKey, cleanupErr)
			}
		}
	}()
	if err != nil {
		return fmt.Errorf("failed to create Network Policy %s: %v", npKey, err)
	}
	klog.Infof("Create network policy %s resources completed, update namespace loglevel", npKey)

	// 3. lock namespace
	nsInfo, nsUnlock = bnc.getNamespaceLocked(policy.Namespace, false)
	if nsInfo == nil {
		// namespace was deleted while we were adding network policy,
		// try to cleanup network policy
		// expect retry to be handled by delete event that should come
		err = fmt.Errorf("unable to get namespace at the end of network policy %s creation: %v", npKey, err)
		return err
	}
	// 6. defer unlock namespace
	defer nsUnlock()

	// 4. check if namespace information related to network policy has changed,
	// network policy only reacts to namespace update ACL log level.
	// Run handleNetPolNamespaceUpdate sequence, but only for 1 newly added policy.
	if err = bnc.updateACLLoggingForPolicy(np, &nsInfo.aclLogging, nsInfo.aclLogging.Allow != aclLogging.Allow,
		nsInfo.aclLogging.Deny != aclLogging.Deny); err != nil {
		return fmt.Errorf("network policy %s failed to be created: update policy ACLs failed: %v", npKey, err)
	} else {
		klog.Infof("Policy %s: ACL logging setting updated to deny=%s allow=%s",
			npKey, nsInfo.aclLogging.Deny, nsInfo.aclLogging.Allow)
	}

	// 5. subscribe to namespace update events
	nsInfo.relatedNetworkPolicies[npKey] = true
	return nil
}

// buildNetworkPolicyACLs builds the ACLS associated with the 'gress policies
// of the provided network policy.
func (bnc *BaseNetworkController) buildNetworkPolicyACLs(pgHashName string, np *networkPolicy, aclLogging *ACLLoggingLevels) []*nbdb.ACL {
	acls := []*nbdb.ACL{}
	for _, gp := range np.ingressPolicies {
		acl, _ := gp.buildLocalPodACLs(pgHashName, aclLogging)
		acls = append(acls, acl...)
	}
	for _, gp := range np.egressPolicies {
		acl, _ := gp.buildLocalPodACLs(pgHashName, aclLogging)
		acls = append(acls, acl...)
	}

	return acls
}

// deleteNetworkPolicy removes a network policy
// It only uses Namespace and Name from given network policy
func (bnc *BaseNetworkController) deleteNetworkPolicy(policy *knet.NetworkPolicy) error {
	npKey := getPolicyKey(policy)
	klog.Infof("Deleting network policy %s", npKey)
	if config.Metrics.EnableScaleMetrics {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			metrics.RecordNetpolEvent("delete", duration)
		}()
	}
	// First lock and update namespace
	nsInfo, nsUnlock := bnc.getNamespaceLocked(policy.Namespace, false)
	if nsInfo != nil {
		// unsubscribe from namespace events
		delete(nsInfo.relatedNetworkPolicies, npKey)
		nsUnlock()
	}
	// Next cleanup network policy
	err := bnc.networkPolicies.DoWithLock(npKey, func(npKey string) error {
		np, ok := bnc.networkPolicies.Load(npKey)
		if !ok {
			klog.Infof("Deleting policy %s that is already deleted", npKey)
			return nil
		}
		if err := bnc.cleanupNetworkPolicy(np); err != nil {
			return fmt.Errorf("deleting policy %s failed: %v", npKey, err)
		}
		return nil
	})
	return err
}

// cleanupNetworkPolicy should be retriable
// It takes and releases networkPolicy lock.
// It updates bnc.networkPolicies on success, should be called with bnc.networkPolicies key locked.
// No need to log network policy key here, because caller of cleanupNetworkPolicy should prepend error message with
// that information.
func (bnc *BaseNetworkController) cleanupNetworkPolicy(np *networkPolicy) error {
	npKey := np.getKey()
	klog.Infof("Cleaning up network policy %s", npKey)
	np.Lock()
	defer np.Unlock()

	// signal to local pod/peer handlers to ignore new events
	np.deleted = true

	// stop handlers, retriable
	bnc.shutdownHandlers(np)

	err := bnc.deleteNetworkLocalPolicy(np)
	if err != nil {
		return err
	}

	// delete from peer address set
	for i, asKey := range np.peerAddressSets {
		if err := bnc.DeletePodSelectorAddressSet(asKey, np.getKeyWithKind()); err != nil {
			// remove deleted address sets from the list
			np.peerAddressSets = np.peerAddressSets[i:]
			return fmt.Errorf("failed to delete network policy from peer address set %s: %v", asKey, err)
		}
	}
	np.peerAddressSets = nil

	// finally, delete netpol from existing networkPolicies
	// this is the signal that cleanup was successful
	bnc.networkPolicies.Delete(npKey)
	return nil
}

type NetworkPolicyExtraParameters struct {
	np *networkPolicy
	gp *gressPolicy
	// shared port group info, used by LocalPodSelectorType
	spgInfo *sharedPortGroupInfo
}

func (bnc *BaseNetworkController) handlePeerNamespaceSelectorAdd(np *networkPolicy, gp *gressPolicy, objs ...interface{}) error {
	if !bnc.IsSecondary() && config.Metrics.EnableScaleMetrics {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			metrics.RecordNetpolPeerNamespaceEvent("add", duration)
		}()
	}
	np.RLock()
	if np.deleted {
		np.RUnlock()
		return nil
	}
	updated := false
	var errors []error
	for _, obj := range objs {
		namespace := obj.(*kapi.Namespace)
		// addNamespaceAddressSet is safe for concurrent use, doesn't require additional synchronization
		nsUpdated, err := gp.addNamespaceAddressSet(namespace.Name, bnc.addressSetFactory)
		if err != nil {
			errors = append(errors, err)
		} else if nsUpdated {
			updated = true
		}
	}
	np.RUnlock()
	// unlock networkPolicy, before calling peerNamespaceUpdate
	if updated {
		err := bnc.peerNamespaceUpdate(np, gp)
		if err != nil {
			errors = append(errors, err)
		}
	}
	return kerrorsutil.NewAggregate(errors)

}

func (bnc *BaseNetworkController) handlePeerNamespaceSelectorDel(np *networkPolicy, gp *gressPolicy, objs ...interface{}) error {
	if !bnc.IsSecondary() && config.Metrics.EnableScaleMetrics {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			metrics.RecordNetpolPeerNamespaceEvent("delete", duration)
		}()
	}
	np.RLock()
	if np.deleted {
		np.RUnlock()
		return nil
	}
	updated := false
	for _, obj := range objs {
		namespace := obj.(*kapi.Namespace)
		// delNamespaceAddressSet is safe for concurrent use, doesn't require additional synchronization
		if gp.delNamespaceAddressSet(namespace.Name) {
			updated = true
		}
	}
	np.RUnlock()
	// unlock networkPolicy, before calling peerNamespaceUpdate
	if updated {
		return bnc.peerNamespaceUpdate(np, gp)
	}
	return nil
}

// peerNamespaceUpdate updates gress ACLs, for this purpose it need to take nsInfo lock and np.RLock
// make sure to pass unlocked networkPolicy
func (bnc *BaseNetworkController) peerNamespaceUpdate(np *networkPolicy, gp *gressPolicy) error {
	// Lock namespace before locking np
	// this is to make sure we don't miss update acl loglevel event for namespace.
	// The order of locking is strict: namespace first, then network policy, otherwise deadlock may happen
	nsInfo, nsUnlock := bnc.getNamespaceLocked(np.namespace, true)
	var aclLogging *ACLLoggingLevels
	if nsInfo == nil {
		aclLogging = &ACLLoggingLevels{
			Allow: "",
			Deny:  "",
		}
	} else {
		defer nsUnlock()
		aclLogging = &nsInfo.aclLogging
	}
	np.RLock()
	defer np.RUnlock()
	if np.deleted {
		return nil
	}
	// buildLocalPodACLs is safe for concurrent use, see function comment for details
	acls, deletedACLs := gp.buildLocalPodACLs(np.portGroupName, aclLogging)
	ops, err := libovsdbops.CreateOrUpdateACLsOps(bnc.nbClient, nil, acls...)
	if err != nil {
		return err
	}
	ops, err = libovsdbops.AddACLsToPortGroupOps(bnc.nbClient, ops, np.portGroupName, acls...)
	if err != nil {
		return err
	}
	if len(deletedACLs) > 0 {
		deletedACLsWithUUID, err := libovsdbops.FindACLs(bnc.nbClient, deletedACLs)
		if err != nil {
			return fmt.Errorf("failed to find deleted acls: %w", err)
		}

		ops, err = libovsdbops.DeleteACLsFromPortGroupOps(bnc.nbClient, ops, np.portGroupName, deletedACLsWithUUID...)
		if err != nil {
			return err
		}
	}
	_, err = libovsdbops.TransactAndCheck(bnc.nbClient, ops)
	return err
}

// addPeerNamespaceHandler starts a watcher for PeerNamespaceSelectorType.
// Sync function and Add event for every existing namespace will be executed sequentially first, and an error will be
// returned if something fails.
// PeerNamespaceSelectorType uses handlePeerNamespaceSelectorAdd on Add,
// and handlePeerNamespaceSelectorDel on Delete.
func (bnc *BaseNetworkController) addPeerNamespaceHandler(
	namespaceSelector *metav1.LabelSelector,
	gress *gressPolicy, np *networkPolicy) error {

	// NetworkPolicy is validated by the apiserver; this can't fail.
	sel, _ := metav1.LabelSelectorAsSelector(namespaceSelector)

	// start watching namespaces selected by the namespace selector
	syncFunc := func(objs []interface{}) error {
		// ignore returned error, since any namespace that wasn't properly handled will be retried individually.
		_ = bnc.handlePeerNamespaceSelectorAdd(np, gress, objs...)
		return nil
	}
	retryPeerNamespaces := bnc.newNetpolRetryFramework(
		factory.PeerNamespaceSelectorType,
		syncFunc,
		&NetworkPolicyExtraParameters{gp: gress, np: np},
	)

	namespaceHandler, err := retryPeerNamespaces.WatchResourceFiltered("", sel)
	if err != nil {
		klog.Errorf("WatchResource failed for addPeerNamespaceHandler: %v", err)
		return err
	}

	np.nsHandlerList = append(np.nsHandlerList, namespaceHandler)
	return nil
}

func (bnc *BaseNetworkController) shutdownHandlers(np *networkPolicy) {
	if np.localPodHandler != nil {
		bnc.watchFactory.RemovePodHandler(np.localPodHandler)
		np.localPodHandler = nil
	}
	for _, handler := range np.nsHandlerList {
		bnc.watchFactory.RemoveNamespaceHandler(handler)
	}
	np.nsHandlerList = make([]*factory.Handler, 0)
}

// The following 2 functions should return the same key for network policy based on k8s on internal networkPolicy object
func getPolicyKey(policy *knet.NetworkPolicy) string {
	return fmt.Sprintf("%v/%v", policy.Namespace, policy.Name)
}

func (np *networkPolicy) getKey() string {
	return fmt.Sprintf("%v/%v", np.namespace, np.name)
}

func (np *networkPolicy) getKeyWithKind() string {
	return fmt.Sprintf("%v/%v/%v", "NetworkPolicy", np.namespace, np.name)
}

// PortGroupHasPorts returns true if a port group contains all given ports
func PortGroupHasPorts(nbClient libovsdbclient.Client, pgName string, portUUIDs []string) bool {
	pg := &nbdb.PortGroup{
		Name: pgName,
	}
	pg, err := libovsdbops.GetPortGroup(nbClient, pg)
	if err != nil {
		return false
	}

	return sets.NewString(pg.Ports...).HasAll(portUUIDs...)
}

// getStaleNetpolAddrSetDbIDs returns the ids for address sets that were owned by network policy before we
// switched to shared address sets with PodSelectorAddressSet. Should only be used for sync and testing.
func getStaleNetpolAddrSetDbIDs(policyNamespace, policyName, policyType, idx, controller string) *libovsdbops.DbObjectIDs {
	return libovsdbops.NewDbObjectIDs(libovsdbops.AddressSetNetworkPolicy, controller, map[libovsdbops.ExternalIDKey]string{
		libovsdbops.ObjectNameKey: policyNamespace + "_" + policyName,
		// direction and idx uniquely identify address set (= gress policy rule)
		libovsdbops.PolicyDirectionKey: strings.ToLower(policyType),
		libovsdbops.GressIdxKey:        idx,
	})
}

func (bnc *BaseNetworkController) getNetpolDefaultACLDbIDs(direction string) *libovsdbops.DbObjectIDs {
	return libovsdbops.NewDbObjectIDs(libovsdbops.ACLNetpolDefault, bnc.controllerName,
		map[libovsdbops.ExternalIDKey]string{
			libovsdbops.ObjectNameKey:      allowHairpinningACLID,
			libovsdbops.PolicyDirectionKey: direction,
		})
}

type gressType struct {
	isIngress bool
	isEgress  bool
}

type sharedPortGroupInfo struct {
	pgName           string               // readable name of this shared port group
	pgHashName       string               // shared port group name
	policies         map[string]gressType // policies sharing this port group
	lspIngressRefCnt int                  // requires ingress default deny acl in this port group is it is greater than 0
	lspEgressRefCnt  int                  // requires egress default deny acl in this port group is it is greater than 0
	podHandler       *factory.Handler

	// localPods is a map of pods affected by this shared port group.
	// It is used to update shared port group port counters, when deleting network policy.
	// Port should only be added here if it was successfully added to shared deny port group,
	// and local port group in db.
	// localPods may be updated by multiple pod handlers at the same time,
	// therefore it uses a sync map to handle simultaneous access.
	// map of portName(string): portUUID(string)
	localPods sync.Map
}

func (bnc *BaseNetworkController) buildLocalNetworkPolicyAcls(spgInfo *sharedPortGroupInfo, np *networkPolicy, aclLogging *ACLLoggingLevels) []*nbdb.ACL {
	acls := []*nbdb.ACL{}
	if np.isIngress {
		if spgInfo.lspIngressRefCnt == 0 {
			ingressDenyACL, ingressAllowACL := bnc.buildDenyACLs(spgInfo.pgHashName, aclLogging, aclIngress)
			acls = append(acls, ingressDenyACL)
			acls = append(acls, ingressAllowACL)
		}
	}
	if np.isEgress {
		if spgInfo.lspEgressRefCnt == 0 {
			egressDenyACL, egressAllowACL := bnc.buildDenyACLs(spgInfo.pgHashName, aclLogging, aclEgress)
			acls = append(acls, egressDenyACL)
			acls = append(acls, egressAllowACL)
		}
	}

	policyAcls := bnc.buildNetworkPolicyACLs(spgInfo.pgHashName, np, aclLogging)
	klog.Infof("Build local policy acls %+v for network policy %s/%s sharing port group %s",
		policyAcls, np.namespace, np.name, spgInfo.pgHashName)
	acls = append(acls, policyAcls...)
	return acls
}

func (bnc *BaseNetworkController) handleInitialSelectedLocalPods(spgInfo *sharedPortGroupInfo, np *networkPolicy, aclLogging *ACLLoggingLevels, objs ...interface{}) error {
	acls := bnc.buildLocalNetworkPolicyAcls(spgInfo, np, aclLogging)
	ops, err := libovsdbops.CreateOrUpdateACLsOps(bnc.nbClient, nil, acls...)
	if err != nil {
		return fmt.Errorf("failed to create acls ops for network policy %s/%s: %v", np.namespace, np.name, err)
	}

	pg := bnc.buildPortGroup(spgInfo.pgHashName, spgInfo.pgName, nil, acls)
	ops, err = libovsdbops.CreateOrUpdatePortGroupsOps(bnc.nbClient, ops, pg)
	if err != nil {
		return err
	}

	recordOps, txOkCallBack, _, err := metrics.GetConfigDurationRecorder().AddOVN(bnc.nbClient, "networkpolicy",
		np.namespace, np.name)
	if err != nil {
		klog.Warningf("Failed to record config duration: %v", err)
	}
	ops = append(ops, recordOps...)

	txnDone, err := bnc.handleLocalPodSelectorAddFunc(spgInfo, ops, txOkCallBack, objs...)
	if err != nil {
		// ignore returned handleLocalPodSelectorAddFunc error, since any pod that wasn't properly handled will be retried individually.
		klog.Warningf(err.Error())

		// create shared port group oly if handleLocalPodSelectorAddFunc() failed to do so
		if !txnDone {
			_, err = libovsdbops.TransactAndCheck(bnc.nbClient, ops)
			if err != nil {
				return fmt.Errorf("failed to run ovsdb txn to create shared port group %s: %v", spgInfo.pgHashName, err)
			}
			txOkCallBack()
		}
	}
	return nil
}

// handleLocalPodSelectorAddFunc adds a new pod to an existing NetworkPolicy, should be retriable.
func (bnc *BaseNetworkController) handleLocalPodSelectorAddFunc(spgInfo *sharedPortGroupInfo, ops []ovsdb.Operation, txOkCallBack func(), objs ...interface{}) (txnDone bool, err error) {
	if !bnc.IsSecondary() && config.Metrics.EnableScaleMetrics {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			metrics.RecordNetpolLocalPodEvent("add", duration)
		}()
	}
	// get info for new pods that are not listed in spgInfo.localPods
	portNamesToUUIDs, policyPortUUIDs, errs := bnc.getNewLocalPolicyPorts(spgInfo, objs...)
	// for multiple objects, try to update the ones that were fetched successfully
	// return error for errPods in the end
	if len(portNamesToUUIDs) > 0 {
		// add pods to policy port group
		if !PortGroupHasPorts(bnc.nbClient, spgInfo.pgHashName, policyPortUUIDs) {
			ops, err = libovsdbops.AddPortsToPortGroupOps(bnc.nbClient, ops, spgInfo.pgHashName, policyPortUUIDs...)
			if err != nil {
				err = fmt.Errorf("unable to get ops to add new pod to policy port group %s: %v", spgInfo.pgHashName, err)
				return
			}
		}
	}

	if _, err = libovsdbops.TransactAndCheck(bnc.nbClient, ops); err != nil {
		err = fmt.Errorf("failed to run ovsdb txn to add ports to shared port group %s: %v", spgInfo.pgHashName, err)
		return
	}
	if txOkCallBack != nil {
		txOkCallBack()
	}

	// all operations were successful, update spgInfo.localPods
	for portName, portUUID := range portNamesToUUIDs {
		spgInfo.localPods.Store(portName, portUUID)
	}

	txnDone = true
	if len(errs) != 0 {
		err = kerrorsutil.NewAggregate(errs)
	}
	return
}

// handleLocalPodSelectorDelFunc handles delete event for local pod, should be retriable
func (bnc *BaseNetworkController) handleLocalPodSelectorDelFunc(spgInfo *sharedPortGroupInfo, objs ...interface{}) error {
	if !bnc.IsSecondary() && config.Metrics.EnableScaleMetrics {
		start := time.Now()
		defer func() {
			duration := time.Since(start)
			metrics.RecordNetpolLocalPodEvent("delete", duration)
		}()
	}

	portNamesToUUIDs, policyPortUUIDs := bnc.getExistingLocalPolicyPorts(spgInfo, objs...)

	if len(portNamesToUUIDs) > 0 {
		// del pods from policy port group
		err := libovsdbops.DeletePortsFromPortGroup(bnc.nbClient, spgInfo.pgHashName, policyPortUUIDs...)
		if err != nil {
			return fmt.Errorf("unable to get ops to add new pod to policy port group: %v", err)
		}

		// all operations were successful, update np.localPods
		for portName := range portNamesToUUIDs {
			spgInfo.localPods.Delete(portName)
		}
	}

	return nil
}

// addNetworkPolicyToSharedPortGroup create shared port group if required, then add policy acls to the shared port group
func (bnc *BaseNetworkController) addNetworkPolicyToSharedPortGroup(spgInfo *sharedPortGroupInfo, aclLogging *ACLLoggingLevels, np *networkPolicy, policy *knet.NetworkPolicy) error {
	klog.Infof("Add network policy %s to shared portGroup readable name %s hashName %s", np.getKey(), spgInfo.pgName, spgInfo.pgHashName)

	_, ok := spgInfo.policies[np.getKey()]
	if ok {
		return nil
	}

	// create shared portGroup if this is the first policy in this shared port group
	if len(spgInfo.policies) == 0 {
		// NetworkPolicy is validated by the apiserver
		sel, err := metav1.LabelSelectorAsSelector(&policy.Spec.PodSelector)
		if err != nil {
			klog.Errorf("Could not set up watcher for local pods: %v", err)
			return err
		}

		// Add all local pods in a syncFunction to minimize db ops.
		syncFunc := func(objs []interface{}) error {
			return bnc.handleInitialSelectedLocalPods(spgInfo, np, aclLogging, objs...)
		}

		// it is ok for LocalPodSelectorType event handler to directly access spgInfo without locking,
		// as spgInfo will only be deleted after the LocalPodSelectorType handler is removed.
		retryLocalPods := bnc.newNetpolRetryFramework(
			factory.LocalPodSelectorType,
			syncFunc,
			&NetworkPolicyExtraParameters{
				spgInfo: spgInfo,
			})

		podHandler, err := retryLocalPods.WatchResourceFiltered(np.namespace, sel)
		if err != nil {
			klog.Errorf("WatchResource failed for addNetworkPolicyToSharedPortGroup: %v", err)
			return err
		}

		spgInfo.podHandler = podHandler
	} else {
		acls := bnc.buildLocalNetworkPolicyAcls(spgInfo, np, aclLogging)
		ops, err := libovsdbops.CreateOrUpdateACLsOps(bnc.nbClient, nil, acls...)
		if err != nil {
			return fmt.Errorf("failed to create acls ops for network policy %s/%s: %v", np.namespace, np.name, err)
		}

		ops, err = libovsdbops.AddACLsToPortGroupOps(bnc.nbClient, ops, spgInfo.pgHashName, acls...)
		if err != nil {
			return fmt.Errorf("failed to create ops to add acls of network policy %s/%s to shared port group %s: %v", np.namespace, np.name, spgInfo.pgName, err)
		}
		_, err = libovsdbops.TransactAndCheck(bnc.nbClient, ops)
		if err != nil {
			return fmt.Errorf("failed to run ovsdb txn to add acls of network policy %s/%s to shared port group %s: %v", np.namespace, np.name, spgInfo.pgName, err)
		}
	}
	if np.isIngress {
		spgInfo.lspIngressRefCnt++
	}
	if np.isEgress {
		spgInfo.lspEgressRefCnt++
	}
	spgInfo.policies[np.getKey()] = gressType{np.isIngress, np.isEgress}
	np.portGroupName = spgInfo.pgHashName
	return nil
}

// deleteNetworkPolicyFromSharedPortGroup remove policy acls from the shared port group and delete shared port group if this is the last policy in the portgroup
func (bnc *BaseNetworkController) deleteNetworkPolicyFromSharedPortGroup(spgInfo *sharedPortGroupInfo, np *networkPolicy) error {
	klog.Infof("Remove network policy %s from shared portGroup readable name %s hashName %s", np.getKey(), spgInfo.pgName, spgInfo.pgHashName)
	var err error

	err = nil
	_, ok := spgInfo.policies[np.getKey()]
	if !ok {
		return err
	}

	if np.isIngress {
		spgInfo.lspIngressRefCnt--
		defer func() {
			if err != nil {
				spgInfo.lspIngressRefCnt++
			}
		}()
	}
	if np.isEgress {
		spgInfo.lspEgressRefCnt--
		defer func() {
			if err != nil {
				spgInfo.lspEgressRefCnt++
			}
		}()
	}

	acls := bnc.buildLocalNetworkPolicyAcls(spgInfo, np, nil)
	deletedACLsWithUUID, err := libovsdbops.FindACLs(bnc.nbClient, acls)
	if err != nil {
		return fmt.Errorf("failed to find acls %+v for network policy %s/%s sharing portGroup %s: %v",
			acls, np.namespace, np.name, spgInfo.pgHashName, err)
	}

	var ops []ovsdb.Operation
	ops, err = libovsdbops.DeleteACLsFromPortGroupOps(bnc.nbClient, nil, spgInfo.pgHashName, deletedACLsWithUUID...)
	if err != nil {
		return fmt.Errorf("failed to create ops for remove acls %+v from shared port group %s: %v", deletedACLsWithUUID, spgInfo.pgHashName, err)
	}

	// create shared portGroup if this is the first policy in this shared port group
	if len(spgInfo.policies) == 1 {
		if spgInfo.podHandler != nil {
			bnc.watchFactory.RemovePodHandler(spgInfo.podHandler)
		}
		spgInfo.podHandler = nil
		ops, err = libovsdbops.DeletePortGroupsOps(bnc.nbClient, ops, spgInfo.pgHashName)
		if err != nil {
			return fmt.Errorf("failed to create ops to delete shared port group %s: %v", spgInfo.pgName, err)
		}
	}

	recordOps, txOkCallBack, _, err1 := metrics.GetConfigDurationRecorder().AddOVN(bnc.nbClient, "networkpolicy",
		np.namespace, np.name)
	if err1 != nil {
		klog.Errorf("Failed to record config duration: %v", err1)
	}
	ops = append(ops, recordOps...)

	_, err = libovsdbops.TransactAndCheck(bnc.nbClient, ops)
	if err != nil {
		return fmt.Errorf("failed to run ovsdb txn to delete network policy %s/%s from shared port group %s: %v", np.namespace, np.name, spgInfo.pgName, err)
	}
	txOkCallBack()
	delete(spgInfo.policies, np.getKey())
	np.portGroupName = ""
	return nil
}

func (bnc *BaseNetworkController) getNetworkPolicySharedPortGroupName(namespace, podSelectorString string) (pgName, readablePGName string) {
	readableGroupName := fmt.Sprintf("%s_%s", namespace, podSelectorString)
	return hashedPortGroup(bnc.GetNetworkScopedName(readableGroupName)), readableGroupName
}

func (bnc *BaseNetworkController) createNetworkLocalPolicy(np *networkPolicy, policy *knet.NetworkPolicy, aclLogging *ACLLoggingLevels) (err error) {
	klog.Infof("Policy %s/%s to be added to shared portGroup", np.namespace, np.name)
	sharedGroupName, readableGroupName := bnc.getNetworkPolicySharedPortGroupName(np.namespace, shortLabelSelectorString(&policy.Spec.PodSelector))
	err = bnc.sharedPortGroupInfos.DoWithLock(sharedGroupName, func(groupName string) error {
		spgInfo, loaded := bnc.sharedPortGroupInfos.LoadOrStore(
			groupName,
			&sharedPortGroupInfo{
				pgName:           readableGroupName,
				pgHashName:       groupName,
				policies:         map[string]gressType{},
				lspIngressRefCnt: 0,
				lspEgressRefCnt:  0,
				podHandler:       nil,
				localPods:        sync.Map{},
			})
		err = bnc.addNetworkPolicyToSharedPortGroup(spgInfo, aclLogging, np, policy)
		if err != nil && !loaded {
			bnc.sharedPortGroupInfos.Delete(sharedGroupName)
		}
		return err
	})
	return err
}

func (bnc *BaseNetworkController) deleteNetworkLocalPolicy(np *networkPolicy) (err error) {
	if np.portGroupName == "" {
		return nil
	}

	klog.Infof("Policy %s/%s to be deleted from shared portGroup %s", np.namespace, np.name, np.portGroupName)
	err = bnc.sharedPortGroupInfos.DoWithLock(np.portGroupName, func(pgHashName string) error {
		spgInfo, loaded := bnc.sharedPortGroupInfos.Load(pgHashName)
		if !loaded {
			return nil
		}
		err = bnc.deleteNetworkPolicyFromSharedPortGroup(spgInfo, np)
		if err != nil {
			return err
		}
		if len(spgInfo.policies) == 0 {
			bnc.sharedPortGroupInfos.Delete(pgHashName)
		}
		return nil
	})
	return err
}
