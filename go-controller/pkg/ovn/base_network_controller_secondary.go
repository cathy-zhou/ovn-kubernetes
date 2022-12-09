package ovn

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"time"

	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdbops"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/metrics"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	kapi "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"
)

func (bnc *BaseNetworkController) getPortInfo4SecondaryNetwork(pod *kapi.Pod) *lpInfo {
	if !util.PodWantsNetwork(pod) {
		return nil
	} else {
		on, network, err := util.IsNetworkOnPod(pod, bnc.NetInfo)
		if err == nil && on {
			nadName := util.GetNadName(network.Namespace, network.Name)
			key := util.GetSecondaryNetworkLogicalPortName(pod.Namespace, pod.Name, nadName)
			portInfo, _ := bnc.logicalPortCache.get(key)
			return portInfo
		}
	}
	return nil

}

// GetInternalCacheEntry4SecondaryNetwork returns the internal cache entry for this object, given an object and its type.
// This is now used only for pods, which will get their the logical port cache entry.
func (bnc *BaseNetworkController) GetInternalCacheEntry4SecondaryNetwork(objType reflect.Type, obj interface{}) interface{} {
	switch objType {
	case factory.PodType:
		pod := obj.(*kapi.Pod)
		return bnc.getPortInfo4SecondaryNetwork(pod)
	default:
		return nil
	}
}

func (bnc *BaseNetworkController) AddSecondaryNetworkResourceCommon(objType reflect.Type, obj interface{}) error {
	switch objType {
	case factory.PodType:
		pod, ok := obj.(*kapi.Pod)
		if !ok {
			return fmt.Errorf("could not cast %T object to *knet.Pod", obj)
		}
		return bnc.ensurePod4SecondaryNetwork(pod, true)

	case factory.NamespaceType:
		ns, ok := obj.(*kapi.Namespace)
		if !ok {
			return fmt.Errorf("could not cast %T object to *kapi.Namespace", obj)
		}
		return bnc.AddNamespace4SecondaryNetwork(ns)

	default:
		return fmt.Errorf("object type %s not supported", objType)
	}
}

func (bnc *BaseNetworkController) UpdateSecondaryNetworkResourceCommon(objType reflect.Type, oldObj, newObj interface{}, inRetryCache bool) error {
	switch objType {
	case factory.PodType:
		oldPod := oldObj.(*kapi.Pod)
		newPod := newObj.(*kapi.Pod)

		return bnc.ensurePod4SecondaryNetwork(newPod, inRetryCache || util.PodScheduled(oldPod) != util.PodScheduled(newPod))

	case factory.NamespaceType:
		oldNs, newNs := oldObj.(*kapi.Namespace), newObj.(*kapi.Namespace)
		return bnc.updateNamespace4SecondaryNetwork(oldNs, newNs)

	default:
		return fmt.Errorf("object type %s not supported", objType)
	}
}

func (bnc *BaseNetworkController) DeleteSecondaryNetworkResourceCommon(objType reflect.Type, obj, cachedObj interface{}) error {
	switch objType {
	case factory.PodType:
		var portInfo *lpInfo
		pod := obj.(*kapi.Pod)

		if cachedObj != nil {
			portInfo = cachedObj.(*lpInfo)
		}
		return bnc.removePod4SecondaryNetwork(pod, portInfo)

	case factory.NamespaceType:
		ns := obj.(*kapi.Namespace)
		return bnc.deleteNamespace4SecondaryNetwork(ns)

	default:
		return fmt.Errorf("object type %s not supported", objType)
	}
}

// ensurePod4SecondaryNetwork tries to set up a pod for secondary network. It returns nil on success and error
// on failure; failure indicates the pod set up should be retried later.
func (bnc *BaseNetworkController) ensurePod4SecondaryNetwork(pod *kapi.Pod, addPort bool) error {

	// Try unscheduled pods later
	if !util.PodScheduled(pod) {
		return nil
	}

	if !util.PodWantsNetwork(pod) && !addPort {
		return nil
	}

	// If a node does node have an assigned hostsubnet don't wait for the logical switch to appear
	switchName, err := bnc.getExpectedSwitchName(pod)
	if err != nil {
		return err
	}

	if bnc.lsManager.IsNonHostSubnetSwitch(switchName) {
		return nil
	}

	on, network, err := util.IsNetworkOnPod(pod, bnc.NetInfo)
	if err != nil {
		// configuration error, no need to retry, do not return error
		klog.Errorf("Error getting network-attachment for pod %s/%s network %s: %v",
			pod.Namespace, pod.Name, bnc.GetNetworkName(), err)
		return nil
	}

	if !on {
		// the pod is not attached to this specific network
		klog.V(5).Infof("Pod %s/%s is not attached on this network controller %s error (%v) ",
			pod.Namespace, pod.Name, bnc.GetNetworkName(), err)
		return nil
	}

	nadName := util.GetNadName(network.Namespace, network.Name)

	var libovsdbExecuteTime time.Duration

	// Keep track of how long syncs take.
	start := time.Now()
	defer func() {
		klog.Infof("[%s/%s] addLogicalPort for nad %s took %v, libovsdb time %v",
			pod.Namespace, pod.Name, nadName, time.Since(start), libovsdbExecuteTime)
	}()

	ops, lsp, podAnnotation, newlyCreated, err := bnc.addLogicalPortToNetwork(pod, nadName, network)
	if err != nil {
		return err
	}

	addOps, err := bnc.addPodToNamespace4SecondaryNetwork(pod.Namespace, podAnnotation.IPs)
	if err != nil {
		return err
	}
	ops = append(ops, addOps...)

	recordOps, txOkCallBack, _, err := metrics.GetConfigDurationRecorder().AddOVN(bnc.nbClient, "pod", pod.Namespace,
		pod.Name, bnc.NetInfo)
	if err != nil {
		klog.Errorf("Config duration recorder: %v", err)
	}
	ops = append(ops, recordOps...)

	transactStart := time.Now()
	_, err = libovsdbops.TransactAndCheckAndSetUUIDs(bnc.nbClient, lsp, ops)
	libovsdbExecuteTime = time.Since(transactStart)
	if err != nil {
		return fmt.Errorf("error transacting operations %+v: %v", ops, err)
	}
	txOkCallBack()
	bnc.podRecorder.AddLSP(pod.UID, bnc.NetInfo)

	// if somehow lspUUID is empty, there is a bug here with interpreting OVSDB results
	if len(lsp.UUID) == 0 {
		return fmt.Errorf("UUID is empty from LSP: %+v", *lsp)
	}

	// Add the pod's logical switch port to the port cache
	_ = bnc.logicalPortCache.add(switchName, lsp.Name, lsp.UUID, podAnnotation.MAC, podAnnotation.IPs)

	if newlyCreated {
		metrics.RecordPodCreated(pod, bnc.NetInfo)
	}
	return nil
}

// removePod tried to tear down a pod. It returns nil on success and error on failure;
// failure indicates the pod tear down should be retried later.
func (bnc *BaseNetworkController) removePod4SecondaryNetwork(pod *kapi.Pod, portInfo *lpInfo) error {
	var nadName, logicalPortName string

	if !util.PodWantsNetwork(pod) {
		return nil
	}
	podDesc := pod.Namespace + "/" + pod.Name
	klog.Infof("Deleting pod: %s for network %s", podDesc, bnc.GetNetworkName())

	if !util.PodScheduled(pod) {
		return nil
	}

	if portInfo != nil {
		logicalPortName = portInfo.name
		bnc.logicalPortCache.remove(logicalPortName)
	}
	on, network, err := util.IsNetworkOnPod(pod, bnc.NetInfo)
	if err != nil || !on {
		// the pod is not attached to this specific network
		return nil
	}
	nadName = util.GetNadName(network.Namespace, network.Name)
	logicalPortName = util.GetSecondaryNetworkLogicalPortName(pod.Namespace, pod.Name, nadName)
	if portInfo == nil {
		bnc.logicalPortCache.remove(logicalPortName)
	}

	// TBD namespaceInfo needed when multi-network policy support is added
	pInfo, err := bnc.deletePodLogicalPort(pod, portInfo, nadName)
	if err != nil {
		return err
	}

	// do not release IP address unless we have validated no other pod is using it
	if pInfo == nil {
		return nil
	}

	// Releasing IPs needs to happen last so that we can deterministically know that if delete failed that
	// the IP of the pod needs to be released. Otherwise we could have a completed pod failed to be removed
	// and we dont know if the IP was released or not, and subsequently could accidentally release the IP
	// while it is now on another pod
	klog.Infof("Attempting to release IPs for pod: %s/%s, ips: %s network %s", pod.Namespace, pod.Name,
		util.JoinIPNetIPs(pInfo.ips, " "), bnc.GetNetworkName())
	return bnc.releasePodIPs(pInfo)
}

func (bnc *BaseNetworkController) syncPods4SecondaryNetwork(pods []interface{}) error {
	// get the list of logical switch ports (equivalent to pods). Reserve all existing Pod IPs to
	// avoid subsequent new Pods getting the same duplicate Pod IP.
	expectedLogicalPorts := make(map[string]bool)
	for _, podInterface := range pods {
		pod, ok := podInterface.(*kapi.Pod)
		if !ok {
			return fmt.Errorf("spurious object in syncPods: %v", podInterface)
		}
		on, network, err := util.IsNetworkOnPod(pod, bnc.NetInfo)
		if err != nil || !on {
			continue
		}
		nadName := util.GetNadName(network.Namespace, network.Name)
		annotations, err := util.UnmarshalPodAnnotation(pod.Annotations, nadName)
		if err != nil {
			continue
		}
		expectedLogicalPortName, err := bnc.allocatePodIPs(pod, annotations, nadName)
		if err != nil {
			return err
		}
		if expectedLogicalPortName != "" {
			expectedLogicalPorts[expectedLogicalPortName] = true
		}
	}
	return bnc.deleteStaleLogicalSwitchPorts(expectedLogicalPorts)
}

// AddNamespace creates corresponding addressset in ovn db
func (bnc *BaseNetworkController) AddNamespace4SecondaryNetwork(ns *kapi.Namespace) error {
	klog.Infof("[%s] adding namespace for network %s", ns.Name, bnc.GetNetworkName())
	// Keep track of how long syncs take.
	start := time.Now()
	defer func() {
		klog.Infof("[%s] adding namespace took %v for network %s", ns.Name, time.Since(start), bnc.GetNetworkName())
	}()

	_, nsUnlock, err := bnc.ensureNamespaceLocked4SecondaryNetwork(ns.Name, false, ns)
	if err != nil {
		return fmt.Errorf("failed to ensure namespace locked: %v", err)
	}
	defer nsUnlock()
	return nil
}

// ensureNamespaceLocked locks namespacesMutex, gets/creates an entry for ns, configures OVN nsInfo, and returns it
// with its mutex locked.
// ns is the name of the namespace, while namespace is the optional k8s namespace object
// if no k8s namespace object is provided, this function will attempt to find it via informer cache
func (bnc *BaseNetworkController) ensureNamespaceLocked4SecondaryNetwork(ns string, readOnly bool, namespace *kapi.Namespace) (*namespaceInfo, func(), error) {
	ips := bnc.getAllNamespacePodAddresses(ns)

	bnc.namespacesMutex.Lock()
	nsInfo := bnc.namespaces[ns]
	nsInfoExisted := false
	if nsInfo == nil {
		nsInfo = &namespaceInfo{
			relatedNetworkPolicies: map[string]bool{},
			multicastEnabled:       false,
		}
		// we are creating nsInfo and going to set it in namespaces map
		// so safe to hold the lock while we create and add it
		defer bnc.namespacesMutex.Unlock()
		// create the adddress set for the new namespace
		var err error
		nsInfo.addressSet, err = bnc.createNamespaceAddrSetAllPods(ns, ips)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create address set for namespace: %s, error: %v", ns, err)
		}
		bnc.namespaces[ns] = nsInfo
	} else {
		nsInfoExisted = true
		// if we found an existing nsInfo, do not hold the namespaces lock
		// while waiting for nsInfo to Lock
		bnc.namespacesMutex.Unlock()
	}

	var unlockFunc func()
	if readOnly {
		unlockFunc = func() { nsInfo.RUnlock() }
		nsInfo.RLock()
	} else {
		unlockFunc = func() { nsInfo.Unlock() }
		nsInfo.Lock()
	}

	if nsInfoExisted {
		// Check that the namespace wasn't deleted while we were waiting for the lock
		bnc.namespacesMutex.Lock()
		defer bnc.namespacesMutex.Unlock()
		if nsInfo != bnc.namespaces[ns] {
			unlockFunc()
			return nil, nil, fmt.Errorf("namespace %s, was removed during ensure", ns)
		}
	}

	// nsInfo and namespace didn't exist, get it from lister
	if namespace == nil {
		var err error
		namespace, err = bnc.watchFactory.GetNamespace(ns)
		if err != nil {
			namespace, err = bnc.client.CoreV1().Namespaces().Get(context.TODO(), ns, metav1.GetOptions{})
			if err != nil {
				klog.Warningf("Unable to find namespace during ensure in informer cache or kube api server. " +
					"Will defer configuring namespace.")
			}
		}
	}

	if namespace != nil {
		// if we have the namespace, attempt to configure nsInfo with it
		if err := bnc.configureNamespaceCommon(nsInfo, namespace); err != nil {
			unlockFunc()
			return nil, nil, fmt.Errorf("failed to configure namespace %s: %v", ns, err)
		}
	}

	return nsInfo, unlockFunc, nil
}

func (bnc *BaseNetworkController) updateNamespace4SecondaryNetwork(old, newer *kapi.Namespace) error {
	var errors []error
	klog.Infof("[%s] updating namespace for network %s", old.Name, bnc.GetNetworkName())

	nsInfo, nsUnlock := bnc.getNamespaceLocked(old.Name, false)
	if nsInfo == nil {
		klog.Warningf("Update event for unknown namespace %q", old.Name)
		return nil
	}
	defer nsUnlock()

	aclAnnotation := newer.Annotations[util.AclLoggingAnnotation]
	oldACLAnnotation := old.Annotations[util.AclLoggingAnnotation]
	// support for ACL logging update, if new annotation is empty, make sure we propagate new setting
	if aclAnnotation != oldACLAnnotation {
		if err := bnc.updateNamespaceAclLogging(old.Name, aclAnnotation, nsInfo); err != nil {
			errors = append(errors, err)
		}
	}

	return kerrors.NewAggregate(errors)
}

func (bnc *BaseNetworkController) deleteNamespace4SecondaryNetwork(ns *kapi.Namespace) error {
	klog.Infof("[%s] deleting namespace for network %s", ns.Name, bnc.GetNetworkName())

	nsInfo := bnc.deleteNamespaceLocked(ns.Name)
	if nsInfo == nil {
		return nil
	}
	defer nsInfo.Unlock()
	return nil
}

func (bnc *BaseNetworkController) addPodToNamespace4SecondaryNetwork(ns string, ips []*net.IPNet) ([]ovsdb.Operation, error) {
	var ops []ovsdb.Operation
	var err error
	nsInfo, nsUnlock, err := bnc.ensureNamespaceLocked4SecondaryNetwork(ns, true, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to ensure namespace locked: %v", err)
	}

	defer nsUnlock()

	if ops, err = nsInfo.addressSet.AddIPsReturnOps(createIPAddressSlice(ips)); err != nil {
		return nil, err
	}

	return ops, nil
}

func (bnc *BaseNetworkController) CleanupNetworkPolicyLogicalEntities(netName string) ([]ovsdb.Operation, error) {
	asPred := func(item *nbdb.AddressSet) bool {
		return item.ExternalIDs[types.NetworkNameExternalID] == netName
	}
	ops, err := libovsdbops.DeleteAddressSetsWithPredicateOps(bnc.nbClient, nil, asPred)
	if err != nil {
		return ops, fmt.Errorf("failed to get ops of remove address sets of network %s: %v", netName, err)
	}
	pgPred := func(item *nbdb.PortGroup) bool {
		return item.ExternalIDs[types.NetworkNameExternalID] == netName
	}
	ops, err = libovsdbops.DeletePortGroupsWithPredicateOps(bnc.nbClient, ops, pgPred)
	if err != nil {
		return ops, fmt.Errorf("failed to get ops of remove port groups of network %s: %v", netName, err)
	}
	return ops, nil
}
