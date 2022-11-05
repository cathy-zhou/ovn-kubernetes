package ovn

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	networkattachmentdefinitionapi "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	libovsdbops "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdbops"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
	lsm "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/logical_switch_manager"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/subnetallocator"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/retry"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	kapi "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
)

type secondaryLayer3NetworkControllerEventHandler struct {
	baseHandler     baseNetworkControllerEventHandler
	watchFactory    *factory.WatchFactory
	objType         reflect.Type
	oc              *SecondaryLayer3NetworkController
	extraParameters interface{}
	syncFunc        func([]interface{}) error
}

// AreResourcesEqual returns true if, given two objects of a known resource type, the update logic for this resource
// type considers them equal and therefore no update is needed. It returns false when the two objects are not considered
// equal and an update needs be executed. This is regardless of how the update is carried out (whether with a dedicated update
// function or with a delete on the old obj followed by an add on the new obj).
func (h *secondaryLayer3NetworkControllerEventHandler) AreResourcesEqual(obj1, obj2 interface{}) (bool, error) {
	return h.baseHandler.areResourcesEqual(h.objType, obj1, obj2)
}

// GetInternalCacheEntry returns the internal cache entry for this object, given an object and its type.
// This is now used only for pods, which will get their the logical port cache entry.
func (h *secondaryLayer3NetworkControllerEventHandler) GetInternalCacheEntry(obj interface{}) interface{} {
	switch h.objType {
	case factory.PodType:
		pod := obj.(*kapi.Pod)
		return h.oc.getPortInfo(pod)
	default:
		return nil
	}
}

// GetResourceFromInformerCache returns the latest state of the object, given an object key and its type.
// from the informers cache.
func (h *secondaryLayer3NetworkControllerEventHandler) GetResourceFromInformerCache(key string) (interface{}, error) {
	return h.baseHandler.getResourceFromInformerCache(h.objType, h.watchFactory, key)
}

// RecordAddEvent records the add event on this given object.
func (h *secondaryLayer3NetworkControllerEventHandler) RecordAddEvent(obj interface{}) {
}

// RecordUpdateEvent records the udpate event on this given object.
func (h *secondaryLayer3NetworkControllerEventHandler) RecordUpdateEvent(obj interface{}) {
}

// RecordDeleteEvent records the delete event on this given object.
func (h *secondaryLayer3NetworkControllerEventHandler) RecordDeleteEvent(obj interface{}) {
}

// RecordSuccessEvent records the success event on this given object.
func (h *secondaryLayer3NetworkControllerEventHandler) RecordSuccessEvent(obj interface{}) {
}

// RecordErrorEvent records the error event on this given object.
func (h *secondaryLayer3NetworkControllerEventHandler) RecordErrorEvent(obj interface{}, reason string, err error) {
}

// IsResourceScheduled returns true if the given object has been scheduled.
// Only applied to pods for now. Returns true for all other types.
func (h *secondaryLayer3NetworkControllerEventHandler) IsResourceScheduled(obj interface{}) bool {
	return h.baseHandler.isResourceScheduled(h.objType, obj)
}

// AddResource adds the specified object to the cluster according to its type and returns the error,
// if any, yielded during object creation.
// Given an object to add and a boolean specifying if the function was executed from iterateRetryResources
func (h *secondaryLayer3NetworkControllerEventHandler) AddResource(obj interface{}, fromRetryLoop bool) error {
	var err error

	switch h.objType {
	case factory.PodType:
		pod, ok := obj.(*kapi.Pod)
		if !ok {
			return fmt.Errorf("could not cast %T object to *knet.Pod", obj)
		}
		return h.oc.ensurePod(nil, pod, true)

	case factory.NodeType:
		node, ok := obj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast %T object to *kapi.Node", obj)
		}
		var nodeParams *nodeSyncs
		if fromRetryLoop {
			_, nodeSync := h.oc.addNodeFailed.Load(node.Name)
			_, clusterRtrSync := h.oc.nodeClusterRouterPortFailed.Load(node.Name)
			nodeParams = &nodeSyncs{syncNode: nodeSync, syncClusterRouterPort: clusterRtrSync}
		} else {
			nodeParams = &nodeSyncs{syncNode: true, syncClusterRouterPort: true}
		}

		if err = h.oc.addUpdateNodeEvent(node, nodeParams); err != nil {
			klog.Infof("Node add failed for %s, will try again later: %v",
				node.Name, err)
			return err
		}

	default:
		return fmt.Errorf("no add function for object type %s", h.objType)
	}
	return nil
}

// UpdateResource updates the specified object in the cluster to its version in newObj according to its
// type and returns the error, if any, yielded during the object update.
// Given an old and a new object; The inRetryCache boolean argument is to indicate if the given resource
// is in the retryCache or not.
func (h *secondaryLayer3NetworkControllerEventHandler) UpdateResource(oldObj, newObj interface{}, inRetryCache bool) error {
	switch h.objType {
	case factory.PodType:
		oldPod := oldObj.(*kapi.Pod)
		newPod := newObj.(*kapi.Pod)

		return h.oc.ensurePod(oldPod, newPod, inRetryCache || util.PodScheduled(oldPod) != util.PodScheduled(newPod))

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
		_, nodeSync := h.oc.addNodeFailed.Load(newNode.Name)
		_, failed := h.oc.nodeClusterRouterPortFailed.Load(newNode.Name)
		clusterRtrSync := failed || nodeChassisChanged(oldNode, newNode) || nodeSubnetChanged(oldNode, newNode)

		return h.oc.addUpdateNodeEvent(newNode, &nodeSyncs{syncNode: nodeSync, syncClusterRouterPort: clusterRtrSync})
	}
	return fmt.Errorf("no update function for object type %s", h.objType)
}

// DeleteResource deletes the object from the cluster according to the delete logic of its resource type.
// Given an object and optionally a cachedObj; cachedObj is the internal cache entry for this object,
// used for now for pods and network policies.
func (h *secondaryLayer3NetworkControllerEventHandler) DeleteResource(obj, cachedObj interface{}) error {
	switch h.objType {
	case factory.PodType:
		var portInfo *lpInfo
		pod := obj.(*kapi.Pod)

		if cachedObj != nil {
			portInfo = cachedObj.(*lpInfo)
		}
		h.oc.logicalPortCache.remove(util.GetLogicalPortName(pod.Namespace, pod.Name))
		return h.oc.removePod(pod, portInfo)

	case factory.NodeType:
		node, ok := obj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast obj of type %T to *knet.Node", obj)
		}
		return h.oc.deleteNodeEvent(node)

	default:
		return fmt.Errorf("object type %s not supported", h.objType)
	}
}

func (h *secondaryLayer3NetworkControllerEventHandler) SyncFunc(objs []interface{}) error {
	var syncFunc func([]interface{}) error

	if h.syncFunc != nil {
		// syncFunc was provided explicitly
		syncFunc = h.syncFunc
	} else {
		switch h.objType {
		case factory.PodType:
			syncFunc = h.oc.syncPods

		case factory.NodeType:
			syncFunc = h.oc.syncNodes

		default:
			return fmt.Errorf("no sync function for object type %s", h.objType)
		}
	}
	if syncFunc == nil {
		return nil
	}
	return syncFunc(objs)
}

// IsObjectInTerminalState returns true if the given object is a in terminal state.
// This is used now for pods that are either in a PodSucceeded or in a PodFailed state.
func (h *secondaryLayer3NetworkControllerEventHandler) IsObjectInTerminalState(obj interface{}) bool {
	return h.baseHandler.isObjectInTerminalState(h.objType, obj)
}

// SecondaryLayer3NetworkController is created for logical network infrastructure and policy
// for a secondary l3 network
type SecondaryLayer3NetworkController struct {
	NetworkControllerInfo

	wg       *sync.WaitGroup
	stopChan chan struct{}

	nodeHandler *factory.Handler
	podHandler  *factory.Handler

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
}

// NewSecondaryLayer3Controller create a new OVN controller for the given secondary l3 nad
func NewSecondaryLayer3Controller(bnc *BaseNetworkController, nInfo util.NetInfo,
	netconfInfo *util.Layer3NetConfInfo) (*SecondaryLayer3NetworkController, error) {
	stopChan := make(chan struct{})
	oc := &SecondaryLayer3NetworkController{
		NetworkControllerInfo: NetworkControllerInfo{
			BaseNetworkController: *bnc,
			NetInfo:               nInfo,
			NetConfInfo:           netconfInfo,
		},
		stopChan:              stopChan,
		wg:                    &sync.WaitGroup{},
		masterSubnetAllocator: subnetallocator.NewHostSubnetAllocator(),
		lsManager:             lsm.NewLogicalSwitchManager(),
		logicalPortCache:      newPortCache(stopChan),
	}

	oc.initRetryFramework()
	return oc, nil
}

func (oc *SecondaryLayer3NetworkController) initRetryFramework() {
	// Init the retry framework for pods, namespaces, nodes, network policies, egress firewalls,
	// egress IP (and dependent namespaces, pods, nodes), cloud private ip config.
	oc.retryPods = oc.newRetryFrameworkWithParameters(factory.PodType, nil, nil)
	oc.retryNodes = oc.newRetryFrameworkWithParameters(factory.NodeType, nil, nil)
}

// newRetryFrameworkMasterWithParameters builds and returns a retry framework for the input resource
// type and assigns all ovnk-master-specific function attributes in the returned struct;
func (oc *SecondaryLayer3NetworkController) newRetryFrameworkWithParameters(
	objectType reflect.Type,
	syncFunc func([]interface{}) error,
	extraParameters interface{}) *retry.RetryFramework {
	eventHandler := &secondaryLayer3NetworkControllerEventHandler{
		baseHandler:     baseNetworkControllerEventHandler{},
		objType:         objectType,
		watchFactory:    oc.watchFactory,
		oc:              oc,
		extraParameters: extraParameters, // in use by network policy dynamic watchers
		syncFunc:        syncFunc,
	}
	resourceHandler := &retry.ResourceHandler{
		HasUpdateFunc:          hasResourceAnUpdateFunc(objectType),
		NeedsUpdateDuringRetry: needsUpdateDuringRetry(objectType),
		ObjType:                objectType,
		EventHandler:           eventHandler,
	}
	r := retry.NewRetryFramework(
		oc.watchFactory,
		resourceHandler,
	)
	return r
}

func (oc *SecondaryLayer3NetworkController) Start(ctx context.Context) error {
	if err := oc.Init(); err != nil {
		return err
	}

	return oc.Run()
}

func (oc *SecondaryLayer3NetworkController) Stop(deleteLogicalEntities bool) error {
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
		func(item *nbdb.LogicalSwitch) bool {
			return item.ExternalIDs[types.NetworkNameExternalID] == oc.GetNetworkName()
		})
	if err != nil {
		return fmt.Errorf("failed to get ops for deleting switches of network %s", oc.GetNetworkName())
	}

	// now delete cluster router
	ops, err = libovsdbops.DeleteLogicalRoutersWithPredicateOps(oc.nbClient, ops,
		func(item *nbdb.LogicalRouter) bool {
			return item.ExternalIDs[types.NetworkNameExternalID] == oc.GetNetworkName()
		})
	if err != nil {
		return fmt.Errorf("failed to get ops for deleting routers of network %s", oc.GetNetworkName())
	}

	_, err = libovsdbops.TransactAndCheck(oc.nbClient, ops)
	if err != nil {
		return fmt.Errorf("failed to deleting routers/switches of network %s", oc.GetNetworkName())
	}

	// cleanup related OVN logical entities
	existingNodes, err := oc.watchFactory.GetNodes()
	if err != nil {
		klog.Errorf("Error in initializing/fetching subnets: %v", err)
	} else {
		// remove hostsubnet annoation for this network
		for _, node := range existingNodes {
			oc.lsManager.DeleteSwitch(oc.GetPrefix() + node.Name)
			if noHostSubnet(node) {
				continue
			}
			hostSubnetsMap := map[string][]*net.IPNet{oc.GetNetworkName(): nil}
			err = oc.UpdateNodeAnnotationWithRetry(node.Name, hostSubnetsMap, nil)
			if err != nil {
				return fmt.Errorf("failed to clear node %q subnet annotation for network %s",
					node.Name, oc.GetNetworkName())
			}
		}
	}
	return nil
}

func (oc *SecondaryLayer3NetworkController) Run() error {
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
func (oc *SecondaryLayer3NetworkController) WatchNodes() error {
	handler, err := oc.retryNodes.WatchResource()
	if err == nil {
		oc.nodeHandler = handler
	}
	return err
}

// WatchPods starts the watching of the Pod resource and calls back the appropriate handler logic
func (oc *SecondaryLayer3NetworkController) WatchPods() error {
	handler, err := oc.retryPods.WatchResource()
	if err == nil {
		oc.podHandler = handler
	}
	return err
}

func (oc *SecondaryLayer3NetworkController) getPortInfo(pod *kapi.Pod) *lpInfo {
	if !util.PodWantsNetwork(pod) {
		return nil
	} else {
		on, network, err := util.IsNetworkOnPod(pod, oc.NetInfo)
		if err == nil && on {
			nadName := util.GetNadName(network.Namespace, network.Name)
			key := util.GetSecondaryNetworkLogicalPortName(pod.Namespace, pod.Name, nadName)
			portInfo, _ := oc.logicalPortCache.get(key)
			return portInfo
		}
	}
	return nil

}

func (oc *SecondaryLayer3NetworkController) Init() error {
	klog.Infof("Allocating subnets")
	l3NetConfInfo := oc.NetConfInfo.(*util.Layer3NetConfInfo)
	if err := oc.masterSubnetAllocator.InitRanges(l3NetConfInfo.ClusterSubnets); err != nil {
		klog.Errorf("Failed to initialize host subnet allocator ranges: %v", err)
		return err
	}

	_, err := oc.createOvnClusterRouter(false)
	return err
}

// ensurePod tries to set up a pod. It returns nil on success and error on failure; failure
// indicates the pod set up should be retried later.
func (oc *SecondaryLayer3NetworkController) ensurePod(oldPod, pod *kapi.Pod, addPort bool) error {
	// Try unscheduled pods later
	if !util.PodScheduled(pod) {
		return nil
	}

	if !util.PodWantsNetwork(pod) && !addPort {
		return nil
	}

	// If a node does node have an assigned hostsubnet don't wait for the logical switch to appear
	switchName := oc.GetPrefix() + pod.Spec.NodeName
	if oc.lsManager.IsNonHostSubnetSwitch(switchName) {
		return nil
	}

	on, network, err := util.IsNetworkOnPod(pod, oc.NetInfo)
	if err != nil || !on {
		// the pod is not attached to this specific network
		klog.V(5).Infof("Pod %s/%s is not attached on this network controller %s error (%v) ",
			pod.Namespace, pod.Name, oc.GetNetworkName(), err)
		return nil
	}

	nadName := util.GetNadName(network.Namespace, network.Name)
	err = oc.addLogicalPort(pod, nadName, network)
	if err != nil {
		return fmt.Errorf("failed to add port of nad %s for pod %s/%s", nadName, pod.Namespace, pod.Name)
	}
	return nil
}

func (oc *SecondaryLayer3NetworkController) addLogicalPort(pod *kapi.Pod, nadName string,
	network *networkattachmentdefinitionapi.NetworkSelectionElement) error {
	var libovsdbExecuteTime time.Duration
	var lsp *nbdb.LogicalSwitchPort
	var ops []ovsdb.Operation
	var podAnnotation *util.PodAnnotation
	var err error

	switchName := oc.GetPrefix() + pod.Spec.NodeName
	// Keep track of how long syncs take.
	start := time.Now()
	defer func() {
		klog.Infof("[%s/%s] addLogicalPort for nad %s took %v, libovsdb time %v",
			pod.Namespace, pod.Name, nadName, time.Since(start), libovsdbExecuteTime)
	}()

	ops, lsp, podAnnotation, _, err = oc.addPodLogicalPort(pod, oc.lsManager, nadName, network)
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
func (oc *SecondaryLayer3NetworkController) removePod(pod *kapi.Pod, portInfo *lpInfo) error {
	if !util.PodWantsNetwork(pod) {
		return nil
	}
	podDesc := pod.Namespace + "/" + pod.Name
	klog.Infof("Deleting pod: %s", podDesc)

	if !util.PodScheduled(pod) {
		return nil
	}

	on, network, err := util.IsNetworkOnPod(pod, oc.NetInfo)
	if err != nil || !on {
		// the pod is not attached to this specific network
		return nil
	}

	nadName := util.GetNadName(network.Namespace, network.Name)
	// TBD namespaceInfo needed when multi-network policy support is added
	pInfo, err := oc.deletePodLogicalPort(pod, portInfo, nadName, nil, oc.lsManager, false)
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

func (oc *SecondaryLayer3NetworkController) addUpdateNodeEvent(node *kapi.Node, nSyncs *nodeSyncs) error {
	var hostSubnets []*net.IPNet
	var errs []error
	var err error

	if noHostSubnet := noHostSubnet(node); noHostSubnet {
		err := oc.lsManager.AddNoHostSubnetSwitch(oc.GetPrefix() + node.Name)
		if err != nil {
			return fmt.Errorf("nodeAdd: error adding noHost subnet for node %s: %w", node.Name, err)
		}
		return nil
	}

	klog.Infof("Adding or Updating Node %q network %s", node.Name, oc.GetNetworkName())
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

func (oc *SecondaryLayer3NetworkController) addNode(node *kapi.Node) ([]*net.IPNet, error) {
	hostSubnets, err := oc.createNodeSubnetAnnotation(node, oc.masterSubnetAllocator)
	if err != nil {
		return nil, err
	}

	hostSubnetsMap := map[string][]*net.IPNet{oc.GetNetworkName(): hostSubnets}
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

func (oc *SecondaryLayer3NetworkController) deleteNodeEvent(node *kapi.Node) error {
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

func (oc *SecondaryLayer3NetworkController) deleteNode(nodeName string) error {
	oc.masterSubnetAllocator.ReleaseAllNodeSubnets(nodeName)

	if err := oc.deleteNodeLogicalNetwork(nodeName); err != nil {
		return fmt.Errorf("error deleting node %s logical network: %v", nodeName, err)
	}

	return nil
}

func (oc *SecondaryLayer3NetworkController) syncPods(pods []interface{}) error {
	// get the list of logical switch ports (equivalent to pods). Reserve all existing Pod IPs to
	// avoid subsequent new Pods getting the same duplicate Pod IP.
	expectedLogicalPorts := make(map[string]bool)
	for _, podInterface := range pods {
		pod, ok := podInterface.(*kapi.Pod)
		if !ok {
			return fmt.Errorf("spurious object in syncPods: %v", podInterface)
		}
		on, network, err := util.IsNetworkOnPod(pod, oc.NetInfo)
		if err != nil || !on {
			continue
		}
		nadName := util.GetNadName(network.Namespace, network.Name)
		annotations, err := util.UnmarshalPodAnnotation(pod.Annotations, nadName)
		if err != nil {
			continue
		}
		err = oc.updateExpectedLogicalPorts(pod, oc.lsManager, annotations, nadName, expectedLogicalPorts)
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
func (oc *SecondaryLayer3NetworkController) syncNodes(nodes []interface{}) error {
	foundNodes := sets.NewString()
	for _, tmp := range nodes {
		node, ok := tmp.(*kapi.Node)
		if !ok {
			return fmt.Errorf("spurious object in syncNodes: %v", tmp)
		}
		_ = oc.updateFoundNodes(node, oc.masterSubnetAllocator, foundNodes)
	}

	p := func(item *nbdb.LogicalSwitch) bool {
		return len(item.OtherConfig) > 0 && item.ExternalIDs[types.NetworkNameExternalID] == oc.GetNetworkName()
	}
	nodeSwitches, err := libovsdbops.FindLogicalSwitchesWithPredicate(oc.nbClient, p)
	if err != nil {
		return fmt.Errorf("failed to get node logical switches which have other-config set: %v", err)
	}
	for _, nodeSwitch := range nodeSwitches {
		if !strings.HasPrefix(nodeSwitch.Name, oc.GetPrefix()) {
			klog.Errorf("Node switch name %s unexpected, expect prefix %s", nodeSwitch.Name, oc.GetPrefix())
			continue
		}
		nodeName := strings.Trim(nodeSwitch.Name, oc.GetPrefix())
		if !foundNodes.Has(nodeName) {
			if err := oc.deleteNode(nodeName); err != nil {
				return fmt.Errorf("failed to delete node:%s, err:%v", nodeName, err)
			}
		}
	}
	return nil
}
