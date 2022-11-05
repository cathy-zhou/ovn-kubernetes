package ovn

import (
	"context"
	"fmt"
	ovntypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	networkattachmentdefinitionapi "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	libovsdbops "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdbops"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
	lsm "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/logical_switch_manager"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/subnetallocator"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/retry"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	kapi "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
)

// SecondaryL3Controller is created for logical network infrastructure and policy
// for a secondary l3 network
type SecondaryL3Controller struct {
	NetworkControllerInfo

	wg       *sync.WaitGroup
	stopChan chan struct{}

	nodeHandler *factory.Handler
	podHandler  *factory.Handler

	// configured cluster subnets
	clusterSubnets []config.CIDRNetworkEntry

	// FIXME DUAL-STACK -  Make IP Allocators more dual-stack friendly
	masterSubnetAllocator *subnetallocator.HostSubnetAllocator

	// A cache of all logical switches seen by the watcher and their subnets
	lsManager *lsm.LogicalSwitchManager

	// A cache of all logical ports known to the controller
	logicalPortCache *portCache

	// retry framework for pods
	retryPods *retry.RetryFramework

	// retry framework for nodes
	retryNodes *retry.RetryFramework
	// Node-specific syncMaps used by node event handler
	addNodeFailed               sync.Map
	nodeClusterRouterPortFailed sync.Map

	//podRecorder metrics.PodRecorder
}

// NewSecondaryL3Controller create a new OVN controller for the given secondary l3 nad
func NewSecondaryL3Controller(bnc *BaseNetworkController, netInfo *util.NetInfo,
	netConfInfo util.NetConfInfo) (*SecondaryL3Controller, error) {
	var oc *SecondaryL3Controller

	l3NetConfInfo := netConfInfo.(*util.L3NetConfInfo)
	clusterSubnets, err := config.ParseClusterSubnetEntries(l3NetConfInfo.NetCidr)
	if err != nil {
		return nil, fmt.Errorf("cluster subnet %s for network %s is invalid: %v",
			l3NetConfInfo.NetCidr, netInfo.NetName, err)
	}

	stopChan := make(chan struct{})
	oc = &SecondaryL3Controller{
		NetworkControllerInfo: NetworkControllerInfo{
			BaseNetworkController: *bnc,
			NetInfo:               *netInfo,
			NetConfInfo:           netConfInfo,
		},
		stopChan:              stopChan,
		wg:                    &sync.WaitGroup{},
		clusterSubnets:        clusterSubnets,
		masterSubnetAllocator: subnetallocator.NewHostSubnetAllocator(),
		lsManager:             lsm.NewLogicalSwitchManager(),
		logicalPortCache:      newPortCache(stopChan),
	}

	oc.initRetryFrameworkForMaster()
	return oc, nil
}

func (oc *SecondaryL3Controller) initRetryFrameworkForMaster() {
	// Init the retry framework for pods, namespaces, nodes, network policies, egress firewalls,
	// egress IP (and dependent namespaces, pods, nodes), cloud private ip config.
	oc.retryPods = newRetryFrameworkMaster(oc, oc.watchFactory, factory.PodType)
	oc.retryNodes = newRetryFrameworkMaster(oc, oc.watchFactory, factory.NodeType)
}

func (oc *SecondaryL3Controller) CompareNetConf(netConfInfo util.NetConfInfo) bool {
	return oc.Compare(netConfInfo)
}

func (oc *SecondaryL3Controller) RecordAddEvent(eventObjType reflect.Type, obj interface{}) {
	// TBD
	//switch eventObjType {
	//case factory.PodType:
	//	pod := obj.(*kapi.Pod)
	//	klog.V(5).Infof("Recording add event on pod %s/%s", pod.Namespace, pod.Name)
	//	oc.podRecorder.AddPod(pod.UID)
	//	metrics.GetConfigDurationRecorder().Start("pod", pod.Namespace, pod.Name)
	//}
}

func (oc *SecondaryL3Controller) RecordUpdateEvent(eventObjType reflect.Type, obj interface{}) {
	// TBD
	//switch eventObjType {
	//case factory.PodType:
	//	pod := obj.(*kapi.Pod)
	//	klog.V(5).Infof("Recording update event on pod %s/%s", pod.Namespace, pod.Name)
	//	metrics.GetConfigDurationRecorder().Start("pod", pod.Namespace, pod.Name)
	//}
}

func (oc *SecondaryL3Controller) RecordDeleteEvent(eventObjType reflect.Type, obj interface{}) {
	// TBD
	//switch eventObjType {
	//case factory.PodType:
	//	pod := obj.(*kapi.Pod)
	//	klog.V(5).Infof("Recording delete event on pod %s/%s", pod.Namespace, pod.Name)
	//	oc.podRecorder.CleanPod(pod.UID)
	//	metrics.GetConfigDurationRecorder().Start("pod", pod.Namespace, pod.Name)
	//}
}

func (oc *SecondaryL3Controller) RecordSuccessEvent(eventObjType reflect.Type, obj interface{}) {
	// TBD
}

func (oc *SecondaryL3Controller) RecordErrorEvent(eventObjType reflect.Type, eventErr error, reason string, obj interface{}) {
	// TBD
}

func (oc *SecondaryL3Controller) Start(ctx context.Context) error {
	if err := oc.StartClusterMaster(); err != nil {
		return err
	}

	return oc.Run()
}

func (oc *SecondaryL3Controller) Stop(deleteLogicalEntities bool) error {
	oc.wg.Wait()
	close(oc.stopChan)

	if oc.podHandler != nil {
		oc.watchFactory.RemovePodHandler(oc.podHandler)
	}

	if oc.nodeHandler != nil {
		oc.watchFactory.RemoveNodeHandler(oc.nodeHandler)
	}

	if !deleteLogicalEntities {
		return nil
	}
	// cleanup related OVN logical entities
	var ops []ovsdb.Operation
	var err error

	// first delete node logical switches
	ops, err = libovsdbops.DeleteLogicalSwitchesWithPredicateOps(oc.nbClient, ops,
		func(item *nbdb.LogicalSwitch) bool { return item.ExternalIDs["network_name"] == oc.NetName })
	if err != nil {
		return fmt.Errorf("failed to get ops for deleting switches of network %s", oc.NetName)
	}

	// now delete cluster router
	ops, err = libovsdbops.DeleteLogicalRoutersWithPredicateOps(oc.nbClient, ops,
		func(item *nbdb.LogicalRouter) bool { return item.ExternalIDs["network_name"] == oc.NetName })
	if err != nil {
		return fmt.Errorf("failed to get ops for deleting routers of network %s", oc.NetName)
	}

	_, err = libovsdbops.TransactAndCheck(oc.nbClient, ops)
	if err != nil {
		return fmt.Errorf("failed to deleting routers/switches of network %s", oc.NetName)
	}

	// cleanup related OVN logical entities
	existingNodes, err := oc.watchFactory.GetNodes()
	if err != nil {
		klog.Errorf("Error in initializing/fetching subnets: %v", err)
	} else {
		// remove hostsubnet annoation for this network
		for _, node := range existingNodes {
			oc.lsManager.DeleteSwitch(oc.Prefix + node.Name)
			if noHostSubnet(node) {
				continue
			}
			hostSubnetsMap := map[string][]*net.IPNet{oc.NetName: nil}
			err = oc.UpdateNodeAnnotationWithRetry(node.Name, hostSubnetsMap, nil)
			if err != nil {
				return fmt.Errorf("failed to clear node %q subnet annotation for network %s",
					node.Name, oc.NetName)
			}
		}
	}
	return nil
}

func (oc *SecondaryL3Controller) Run() error {
	klog.Infof("Starting all the Watchers...")
	start := time.Now()

	if err := oc.WatchNodes(); err != nil {
		return err
	}

	if err := oc.WatchPods(); err != nil {
		return err
	}

	klog.Infof("Completing all the Watchers took %v", time.Since(start))

	// controller is fully running and resource handlers have synced, update Topology version in OVN
	if err := oc.updateL3TopologyVersion(); err != nil {
		klog.Errorf("Failed to update topology version: %v", err)
		return err
	}

	return nil
}

// WatchNodes starts the watching of node resource and calls
// back the appropriate handler logic
func (oc *SecondaryL3Controller) WatchNodes() error {
	handler, err := oc.retryNodes.WatchResource()
	if err == nil {
		oc.nodeHandler = handler
	}
	return err
}

// WatchPods starts the watching of the Pod resource and calls back the appropriate handler logic
func (oc *SecondaryL3Controller) WatchPods() error {
	handler, err := oc.retryPods.WatchResource()
	if err == nil {
		oc.podHandler = handler
	}
	return err
}

func (oc *SecondaryL3Controller) AddResource(eventObjType reflect.Type, obj interface{}, fromRetryLoop bool, extraParameters interface{}) error {
	var err error

	switch eventObjType {
	case factory.PodType:
		pod, ok := obj.(*kapi.Pod)
		if !ok {
			return fmt.Errorf("could not cast %T object to *knet.Pod", obj)
		}
		return oc.ensurePod(nil, pod, true)

	case factory.NodeType:
		node, ok := obj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast %T object to *kapi.Node", obj)
		}
		var nodeParams *nodeSyncs
		if fromRetryLoop {
			_, nodeSync := oc.addNodeFailed.Load(node.Name)
			_, clusterRtrSync := oc.nodeClusterRouterPortFailed.Load(node.Name)
			nodeParams = &nodeSyncs{syncNode: nodeSync, syncClusterRouterPort: clusterRtrSync}
		} else {
			nodeParams = &nodeSyncs{syncNode: true, syncClusterRouterPort: true}
		}

		if err = oc.addUpdateNodeEvent(node, nodeParams); err != nil {
			klog.Infof("Node add failed for %s network %s, will try again later: %v",
				node.Name, oc.NetName, err)
			return err
		}

	default:
		return fmt.Errorf("no add function for object type %s", eventObjType)
	}

	return nil
}

func (oc *SecondaryL3Controller) UpdateResource(eventObjType reflect.Type, oldObj interface{}, newObj interface{}, inRetryCache bool, extraParameters interface{}) error {
	switch eventObjType {
	case factory.PodType:
		oldPod := oldObj.(*kapi.Pod)
		newPod := newObj.(*kapi.Pod)

		return oc.ensurePod(oldPod, newPod, inRetryCache || util.PodScheduled(oldPod) != util.PodScheduled(newPod))

	case factory.NodeType:
		newNode, ok := newObj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast newObj of type %T to *kapi.Node", newObj)
		}
		oldNode, ok := oldObj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast oldObj of type %T to *kapi.Node", oldObj)
		}
		// determine what actually changed in this update
		_, nodeSync := oc.addNodeFailed.Load(newNode.Name)
		_, failed := oc.nodeClusterRouterPortFailed.Load(newNode.Name)
		clusterRtrSync := failed || nodeChassisChanged(oldNode, newNode) || nodeSubnetChanged(oldNode, newNode)

		return oc.addUpdateNodeEvent(newNode, &nodeSyncs{syncNode: nodeSync, syncClusterRouterPort: clusterRtrSync})
	}
	return fmt.Errorf("no update function for object type %s", eventObjType)
}

func (oc *SecondaryL3Controller) DeleteResource(eventObjType reflect.Type, obj, cachedObj, extraParameters interface{}) error {
	switch eventObjType {
	case factory.PodType:
		var portInfo *lpInfo
		pod := obj.(*kapi.Pod)

		if cachedObj != nil {
			portInfo = cachedObj.(*lpInfo)
		}
		oc.logicalPortCache.remove(util.GetLogicalPortName(pod.Namespace, pod.Name))
		return oc.removePod(pod, portInfo)

	case factory.NodeType:
		node, ok := obj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast obj of type %T to *knet.Node", obj)
		}
		return oc.deleteNodeEvent(node)

	default:
		return fmt.Errorf("object type %s not supported", eventObjType)
	}
}

func (oc *SecondaryL3Controller) GetSyncFunc(objType reflect.Type) (syncFunc func([]interface{}) error, err error) {
	switch objType {
	case factory.PodType:
		syncFunc = oc.syncPods

	case factory.NodeType:
		syncFunc = oc.syncNodes

	default:
		return nil, fmt.Errorf("no sync function for object type %s", objType)
	}

	return syncFunc, nil
}

func (oc *SecondaryL3Controller) GetInternalCacheEntry(eventObjType reflect.Type, obj interface{}) interface{} {
	switch eventObjType {
	case factory.PodType:
		pod := obj.(*kapi.Pod)
		key := util.GetLogicalPortName(pod.Namespace, pod.Name)
		portInfo, _ := oc.logicalPortCache.get(key)
		return portInfo
	default:
		return nil
	}
}

func (oc *SecondaryL3Controller) StartClusterMaster() error {
	//klog.Infof("Starting cluster master for network %s", oc.nadInfo.NetName)

	klog.Infof("Allocating subnets")
	if err := oc.masterSubnetAllocator.InitRanges(oc.clusterSubnets); err != nil {
		klog.Errorf("Failed to initialize host subnet allocator ranges: %v", err)
		return err
	}

	_, err := oc.createOvnClusterRouter(false)
	return err
}

// ensurePod tries to set up a pod. It returns nil on success and error on failure; failure
// indicates the pod set up should be retried later.
func (oc *SecondaryL3Controller) ensurePod(oldPod, pod *kapi.Pod, addPort bool) error {
	// Try unscheduled pods later
	if !util.PodScheduled(pod) {
		return nil
	}

	if !util.PodWantsNetwork(pod) && !addPort {
		return nil
	}

	// If a node does node have an assigned hostsubnet don't wait for the logical switch to appear
	switchName := oc.Prefix + pod.Spec.NodeName
	if oc.lsManager.IsNonHostSubnetSwitch(switchName) {
		return nil
	}

	on, network, err := util.IsNetworkOnPod(pod, &oc.NetInfo)
	if err != nil || !on {
		// the pod is not attached to this specific network
		klog.V(5).Infof("Pod %s/%s is not attached on this network controller %s error (%v) ",
			pod.Namespace, pod.Name, oc.NetName, err)
		return nil
	}

	nadKeyName := util.GetNadKeyName(network.Namespace, network.Name)
	err = oc.addLogicalPort(pod, nadKeyName, network)
	if err != nil {
		return fmt.Errorf("failed to add port of nad %s for pod %s/%s", nadKeyName, pod.Namespace, pod.Name)
	}
	return nil
}

func (oc *SecondaryL3Controller) addLogicalPort(pod *kapi.Pod, nadKeyName string,
	network *networkattachmentdefinitionapi.NetworkSelectionElement) error {
	var libovsdbExecuteTime time.Duration
	var lsp *nbdb.LogicalSwitchPort
	var ops []ovsdb.Operation
	var podAnnotation *util.PodAnnotation
	var err error

	switchName := oc.Prefix + pod.Spec.NodeName
	// Keep track of how long syncs take.
	start := time.Now()
	defer func() {
		klog.Infof("[%s/%s] addLogicalPort for nad %s took %v, libovsdb time %v",
			pod.Namespace, pod.Name, nadKeyName, time.Since(start), libovsdbExecuteTime)
	}()

	ops, lsp, podAnnotation, _, err = oc.addPodLogicalPort(pod,
		oc.lsManager, oc.clusterSubnets, nadKeyName, network)
	if err != nil {
		return err
	}

	// TBD, need to add PodIP to namespace when multi-network policy support is added

	// TBD what to do with secondary network?
	//recordOps, txOkCallBack, _, err := metrics.GetConfigDurationRecorder().AddOVN(oc.nbClient, "pod", pod.Namespace,
	//	pod.Name)
	//if err != nil {
	//	klog.Errorf("Config duration recorder: %v", err)
	//}
	//ops = append(ops, recordOps...)

	transactStart := time.Now()
	_, err = libovsdbops.TransactAndCheckAndSetUUIDs(oc.nbClient, lsp, ops)
	libovsdbExecuteTime = time.Since(transactStart)
	if err != nil {
		return fmt.Errorf("error transacting operations %+v: %v", ops, err)
	}
	//txOkCallBack()
	//oc.podRecorder.AddLSP(pod.UID)

	// if somehow lspUUID is empty, there is a bug here with interpreting OVSDB results
	if len(lsp.UUID) == 0 {
		return fmt.Errorf("UUID is empty from LSP: %+v", *lsp)
	}

	// Add the pod's logical switch port to the port cache
	_ = oc.logicalPortCache.add(switchName, lsp.Name, lsp.UUID, podAnnotation.MAC, podAnnotation.IPs)

	// TBD, observe the pod creation latency metric.
	//if newlyCreated {
	//	metrics.RecordPodCreated(pod)
	//}
	return nil
}

// removePod tried to tear down a pod. It returns nil on success and error on failure;
// failure indicates the pod tear down should be retried later.
func (oc *SecondaryL3Controller) removePod(pod *kapi.Pod, portInfo *lpInfo) error {
	if !util.PodWantsNetwork(pod) {
		return nil
	}
	podDesc := pod.Namespace + "/" + pod.Name
	klog.Infof("Deleting pod: %s", podDesc)

	if !util.PodScheduled(pod) {
		return nil
	}

	on, network, err := util.IsNetworkOnPod(pod, &oc.NetInfo)
	if err != nil || !on {
		// the pod is not attached to this specific network
		return nil
	}

	nadKeyName := util.GetNadKeyName(network.Namespace, network.Name)
	// TBD namespaceInfo needed when multi-network policy support is added
	pInfo, err := oc.deletePodLogicalPort(pod, portInfo, nadKeyName, nil, oc.lsManager, false)
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
	klog.Infof("Attempting to release IPs for pod: %s/%s, ips: %s", pod.Namespace, pod.Name,
		util.JoinIPNetIPs(pInfo.ips, " "))
	if err := oc.lsManager.ReleaseIPs(pInfo.logicalSwitch, pInfo.ips); err != nil {
		return fmt.Errorf("cannot release IPs for pod %s: %w", podDesc, err)
	}

	return nil
}

func (oc *SecondaryL3Controller) addUpdateNodeEvent(node *kapi.Node, nSyncs *nodeSyncs) error {
	var hostSubnets []*net.IPNet
	var errs []error
	var err error

	if noHostSubnet := noHostSubnet(node); noHostSubnet {
		err := oc.lsManager.AddNoHostSubnetSwitch(oc.Prefix + node.Name)
		if err != nil {
			return fmt.Errorf("nodeAdd: error adding noHost subnet for node %s: %w", node.Name, err)
		}
		return nil
	}

	klog.Infof("Adding or Updating Node %q network %s", node.Name, oc.NetName)
	if nSyncs.syncNode {
		if hostSubnets, err = oc.addNode(node); err != nil {
			oc.addNodeFailed.Store(node.Name, true)
			oc.nodeClusterRouterPortFailed.Store(node.Name, true)
			// TBD
			// err = fmt.Errorf("nodeAdd: error adding node %q: %w", node.Name, err)
			// oc.recordNodeErrorEvent(node, err)
			return err
		}
		oc.addNodeFailed.Delete(node.Name)
	}

	if nSyncs.syncClusterRouterPort {
		if err = oc.syncNodeClusterRouterPort(node, hostSubnets); err != nil {
			errs = append(errs, err)
			oc.nodeClusterRouterPortFailed.Store(node.Name, true)
		} else {
			oc.nodeClusterRouterPortFailed.Delete(node.Name)
		}
	}

	// ensure pods that already exist on this node have their logical ports created
	if nSyncs.syncNode { // do this only if it is a new node add
		errors := oc.requestAddPodOnNode(node.Name, oc.retryPods)
		errs = append(errs, errors...)
	}

	err = kerrors.NewAggregate(errs)
	// TBD
	//if err != nil {
	//	oc.recordNodeErrorEvent(node, err)
	//}
	return err
}

func (oc *SecondaryL3Controller) addNode(node *kapi.Node) ([]*net.IPNet, error) {
	hostSubnets, err := oc.createNodeSubnetAnnotation(node, oc.masterSubnetAllocator)
	if err != nil {
		return nil, err
	}

	hostSubnetsMap := map[string][]*net.IPNet{oc.NetName: hostSubnets}
	err = oc.UpdateNodeAnnotationWithRetry(node.Name, hostSubnetsMap, nil)
	if err != nil {
		return nil, err
	}

	logicalSwitch, err := oc.createNodeLogicalSwitch(node.Name, hostSubnets, "", false)
	if err != nil {
		return nil, err
	}

	// Add the switch to the logical switch cache
	err = oc.lsManager.AddSwitch(logicalSwitch.Name, logicalSwitch.UUID, hostSubnets)
	if err != nil {
		return nil, err
	}
	return hostSubnets, nil
}

func (oc *SecondaryL3Controller) deleteNodeEvent(node *kapi.Node) error {
	klog.V(5).Infof("Deleting Node %q. Removing the node from "+
		"various caches", node.Name)

	if err := oc.deleteNode(node.Name); err != nil {
		return err
	}
	oc.lsManager.DeleteSwitch(node.Name)
	oc.addNodeFailed.Delete(node.Name)
	oc.nodeClusterRouterPortFailed.Delete(node.Name)
	return nil
}

func (oc *SecondaryL3Controller) deleteNode(nodeName string) error {
	oc.masterSubnetAllocator.ReleaseAllNodeSubnets(nodeName)

	if err := oc.deleteNodeLogicalNetwork(nodeName); err != nil {
		return fmt.Errorf("error deleting node %s logical network: %v", nodeName, err)
	}

	return nil
}

func (oc *SecondaryL3Controller) syncPods(pods []interface{}) error {
	// get the list of logical switch ports (equivalent to pods). Reserve all existing Pod IPs to
	// avoid subsequent new Pods getting the same duplicate Pod IP.
	expectedLogicalPorts := make(map[string]bool)
	for _, podInterface := range pods {
		pod, ok := podInterface.(*kapi.Pod)
		if !ok {
			return fmt.Errorf("spurious object in syncPods: %v", podInterface)
		}
		on, network, err := util.IsNetworkOnPod(pod, &oc.NetInfo)
		if err != nil || !on {
			continue
		}
		nadKeyName := util.GetNadKeyName(network.Namespace, network.Name)
		annotations, err := util.UnmarshalPodAnnotation(pod.Annotations, nadKeyName)
		if err != nil {
			continue
		}
		err = oc.updateExpectedLogicalPorts(pod, oc.lsManager, annotations, nadKeyName, expectedLogicalPorts)
		if err != nil {
			return err
		}
	}
	return oc.deleteStaleLogicalSwitchPorts(oc.lsManager, expectedLogicalPorts)
}

// We only deal with cleaning up nodes that shouldn't exist here, since
// watchNodes() will be called for all existing nodes at startup anyway.
// Note that this list will include the 'join' cluster switch, which we
// do not want to delete.
func (oc *SecondaryL3Controller) syncNodes(nodes []interface{}) error {
	foundNodes := sets.NewString()
	for _, tmp := range nodes {
		node, ok := tmp.(*kapi.Node)
		if !ok {
			return fmt.Errorf("spurious object in syncNodes: %v", tmp)
		}
		_ = oc.updateFoundNodes(node, oc.masterSubnetAllocator, foundNodes)
	}

	p := func(item *nbdb.LogicalSwitch) bool {
		return len(item.OtherConfig) > 0 && item.ExternalIDs[ovntypes.NetworkNameExternalID] == oc.NetName
	}
	nodeSwitches, err := libovsdbops.FindLogicalSwitchesWithPredicate(oc.nbClient, p)
	if err != nil {
		return fmt.Errorf("failed to get node logical switches which have other-config set: %v", err)
	}
	for _, nodeSwitch := range nodeSwitches {
		if !strings.HasPrefix(nodeSwitch.Name, oc.Prefix) {
			klog.Errorf("Node switch name %s unexpected, expect prefix %s", nodeSwitch.Name, oc.Prefix)
			continue
		}
		nodeName := strings.Trim(nodeSwitch.Name, oc.Prefix)
		if !foundNodes.Has(nodeName) {
			if err := oc.deleteNode(nodeName); err != nil {
				return fmt.Errorf("failed to delete node:%s, err:%v", nodeName, err)
			}
		}
	}
	return nil
}
