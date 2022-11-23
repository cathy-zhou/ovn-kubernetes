package ovn

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"

	networkattachmentdefinitionapi "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	libovsdbops "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdbops"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/metrics"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
	addressset "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/address_set"
	lsm "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/logical_switch_manager"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/retry"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/syncmap"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	kapi "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
	utilnet "k8s.io/utils/net"
)

type secondaryLayer2NetworkControllerEventHandler struct {
	baseHandler     baseNetworkControllerEventHandler
	watchFactory    *factory.WatchFactory
	objType         reflect.Type
	oc              *SecondaryLayer2NetworkController
	extraParameters interface{}
	syncFunc        func([]interface{}) error
}

// AreResourcesEqual returns true if, given two objects of a known resource type, the update logic for this resource
// type considers them equal and therefore no update is needed. It returns false when the two objects are not considered
// equal and an update needs be executed. This is regardless of how the update is carried out (whether with a dedicated update
// function or with a delete on the old obj followed by an add on the new obj).
func (h *secondaryLayer2NetworkControllerEventHandler) AreResourcesEqual(obj1, obj2 interface{}) (bool, error) {
	return h.baseHandler.areResourcesEqual(h.objType, obj1, obj2)
}

// GetInternalCacheEntry returns the internal cache entry for this object, given an object and its type.
// This is now used only for pods, which will get their the logical port cache entry.
func (h *secondaryLayer2NetworkControllerEventHandler) GetInternalCacheEntry(obj interface{}) interface{} {
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
func (h *secondaryLayer2NetworkControllerEventHandler) GetResourceFromInformerCache(key string) (interface{}, error) {
	return h.baseHandler.getResourceFromInformerCache(h.objType, h.watchFactory, key)
}

// RecordAddEvent records the add event on this given object.
func (h *secondaryLayer2NetworkControllerEventHandler) RecordAddEvent(obj interface{}) {
}

// RecordUpdateEvent records the udpate event on this given object.
func (h *secondaryLayer2NetworkControllerEventHandler) RecordUpdateEvent(obj interface{}) {
}

// RecordDeleteEvent records the delete event on this given object.
func (h *secondaryLayer2NetworkControllerEventHandler) RecordDeleteEvent(obj interface{}) {
}

// RecordSuccessEvent records the success event on this given object.
func (h *secondaryLayer2NetworkControllerEventHandler) RecordSuccessEvent(obj interface{}) {
}

// RecordErrorEvent records the error event on this given object.
func (h *secondaryLayer2NetworkControllerEventHandler) RecordErrorEvent(obj interface{}, reason string, err error) {
}

// IsResourceScheduled returns true if the given object has been scheduled.
// Only applied to pods for now. Returns true for all other types.
func (h *secondaryLayer2NetworkControllerEventHandler) IsResourceScheduled(obj interface{}) bool {
	return h.baseHandler.isResourceScheduled(h.objType, obj)
}

// AddResource adds the specified object to the cluster according to its type and returns the error,
// if any, yielded during object creation.
// Given an object to add and a boolean specifying if the function was executed from iterateRetryResources
func (h *secondaryLayer2NetworkControllerEventHandler) AddResource(obj interface{}, fromRetryLoop bool) error {
	switch h.objType {
	case factory.PodType:
		pod, ok := obj.(*kapi.Pod)
		if !ok {
			return fmt.Errorf("could not cast %T object to *knet.Pod", obj)
		}
		return h.oc.ensurePod(nil, pod, true)

	default:
		return fmt.Errorf("no add function for object type %s", h.objType)
	}
}

// UpdateResource updates the specified object in the cluster to its version in newObj according to its
// type and returns the error, if any, yielded during the object update.
// Given an old and a new object; The inRetryCache boolean argument is to indicate if the given resource
// is in the retryCache or not.
func (h *secondaryLayer2NetworkControllerEventHandler) UpdateResource(oldObj, newObj interface{}, inRetryCache bool) error {
	switch h.objType {
	case factory.PodType:
		oldPod := oldObj.(*kapi.Pod)
		newPod := newObj.(*kapi.Pod)

		return h.oc.ensurePod(oldPod, newPod, inRetryCache || util.PodScheduled(oldPod) != util.PodScheduled(newPod))
	}
	return fmt.Errorf("no update function for object type %s", h.objType)
}

// DeleteResource deletes the object from the cluster according to the delete logic of its resource type.
// Given an object and optionally a cachedObj; cachedObj is the internal cache entry for this object,
// used for now for pods and network policies.
func (h *secondaryLayer2NetworkControllerEventHandler) DeleteResource(obj, cachedObj interface{}) error {
	switch h.objType {
	case factory.PodType:
		var portInfo *lpInfo
		pod := obj.(*kapi.Pod)

		if cachedObj != nil {
			portInfo = cachedObj.(*lpInfo)
		}
		h.oc.logicalPortCache.remove(util.GetLogicalPortName(pod.Namespace, pod.Name))
		return h.oc.removePod(pod, portInfo)

	default:
		return fmt.Errorf("object type %s not supported", h.objType)
	}
}

func (h *secondaryLayer2NetworkControllerEventHandler) SyncFunc(objs []interface{}) error {
	var syncFunc func([]interface{}) error

	if h.syncFunc != nil {
		// syncFunc was provided explicitly
		syncFunc = h.syncFunc
	} else {
		switch h.objType {
		case factory.PodType:
			syncFunc = h.oc.syncPods

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
func (h *secondaryLayer2NetworkControllerEventHandler) IsObjectInTerminalState(obj interface{}) bool {
	return h.baseHandler.isObjectInTerminalState(h.objType, obj)
}

// SecondaryLayer2NetworkController is created for logical network infrastructure and policy
// for a secondary layer2 network
type SecondaryLayer2NetworkController struct {
	NetworkControllerInfo

	wg         *sync.WaitGroup
	podHandler *factory.Handler

	// retry framework for pods
	retryPods *retry.RetryFramework
}

// NewSecondaryLayer2NetworkController create a new OVN controller for the given secondary layer2 nad
func NewSecondaryLayer2NetworkController(bnc *BaseNetworkController, nInfo util.NetInfo,
	netconfInfo util.NetConfInfo) *SecondaryLayer2NetworkController {
	stopChan := make(chan struct{})

	oc := &SecondaryLayer2NetworkController{
		NetworkControllerInfo: NetworkControllerInfo{
			BaseNetworkController:  *bnc,
			NetInfo:                nInfo,
			NetConfInfo:            netconfInfo,
			lsManager:              lsm.NewL2SwitchManager(),
			logicalPortCache:       newPortCache(stopChan),
			addressSetFactory:      addressset.NewOvnAddressSetFactory(bnc.nbClient, nInfo),
			networkPolicies:        syncmap.NewSyncMap[*networkPolicy](),
			sharedNetpolPortGroups: syncmap.NewSyncMap[*defaultDenyPortGroups](),
			namespaceManager: namespaceManager{
				namespaces:      make(map[string]*namespaceInfo),
				namespacesMutex: sync.Mutex{},
			},
			stopChan: stopChan,
		},
		wg: &sync.WaitGroup{},
	}
	// disable multicast support for secondary networks for now
	oc.multicastSupport = false
	oc.initRetryFramework()
	return oc
}

func (oc *SecondaryLayer2NetworkController) initRetryFramework() {
	// Init the retry framework for pods
	oc.retryPods = oc.newRetryFrameworkWithParameters(factory.PodType, nil, nil)
}

// newRetryFrameworkMasterWithParameters builds and returns a retry framework for the input resource
// type and assigns all ovnk-master-specific function attributes in the returned struct;
func (oc *SecondaryLayer2NetworkController) newRetryFrameworkWithParameters(
	objectType reflect.Type,
	syncFunc func([]interface{}) error,
	extraParameters interface{}) *retry.RetryFramework {
	eventHandler := &secondaryLayer2NetworkControllerEventHandler{
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

// Start starts the secondary layer2 controller, handles all events and creates all needed logical entities
func (oc *SecondaryLayer2NetworkController) Start(ctx context.Context) error {
	klog.Infof("Start secondary %s network controller of network %s", oc.GetTopologyType(), oc.GetNetworkName())
	if err := oc.Init(); err != nil {
		return err
	}

	return oc.Run()
}

// DeleteLogicalEntities delete logical entities for this network
func (oc *SecondaryLayer2NetworkController) DeleteLogicalEntities(netName string) error {
	klog.Infof("Delete OVN logical entities for %s network controller of network %s", oc.GetTopologyType(), oc.GetNetworkName())

	// delete layer 2 logical switches
	ops, err := libovsdbops.DeleteLogicalSwitchesWithPredicateOps(oc.nbClient, nil,
		func(item *nbdb.LogicalSwitch) bool {
			return item.ExternalIDs[types.NetworkNameExternalID] == netName
		})
	if err != nil {
		return fmt.Errorf("failed to get ops for deleting switches of network %s", netName)
	}

	_, err = libovsdbops.TransactAndCheck(oc.nbClient, ops)
	if err != nil {
		return fmt.Errorf("failed to deleting switches of network %s", netName)
	}

	return nil
}

// Stop gracefully stops the controller, and delete all logical entities for this network if requested
func (oc *SecondaryLayer2NetworkController) Stop(deleteLogicalEntities bool) error {
	klog.Infof("Stop secondary %s network controller of network %s", oc.GetTopologyType(), oc.GetNetworkName())
	close(oc.stopChan)
	oc.wg.Wait()

	if oc.podHandler != nil {
		oc.watchFactory.RemovePodHandler(oc.podHandler)
	}

	if !deleteLogicalEntities {
		return nil
	}

	// cleanup related OVN logical entities
	return oc.DeleteLogicalEntities(oc.GetNetworkName())
}

func (oc *SecondaryLayer2NetworkController) Run() error {
	klog.Infof("Starting all the Watchers for network %s ...", oc.GetNetworkName())
	start := time.Now()

	if err := oc.WatchPods(); err != nil {
		return err
	}

	klog.Infof("Completing all the Watchers for network %s took %v", oc.GetNetworkName(), time.Since(start))

	// controller is fully running and resource handlers have synced, update Topology version in OVN
	if err := oc.updateL2TopologyVersion(); err != nil {
		return fmt.Errorf("failed to update topology version for network %s: %v", oc.GetNetworkName(), err)
	}

	return nil
}

// WatchPods starts the watching of the Pod resource and calls back the appropriate handler logic
func (oc *SecondaryLayer2NetworkController) WatchPods() error {
	if oc.podHandler != nil {
		return nil
	}
	handler, err := oc.retryPods.WatchResource()
	if err == nil {
		oc.podHandler = handler
	}
	return err
}

func (oc *SecondaryLayer2NetworkController) getPortInfo(pod *kapi.Pod) *lpInfo {
	return oc.NetworkControllerInfo.getPortInfo(pod, oc.logicalPortCache)
}

func (oc *SecondaryLayer2NetworkController) Init() error {
	switchName := oc.GetPrefix() + types.OvnLayer2Switch
	logicalSwitch := nbdb.LogicalSwitch{
		Name:        switchName,
		ExternalIDs: map[string]string{},
	}
	if oc.IsSecondary() {
		logicalSwitch.ExternalIDs[types.NetworkNameExternalID] = oc.GetNetworkName()
		logicalSwitch.ExternalIDs[types.TopoTypeExternalID] = oc.GetTopologyType()
	}

	layer2NetConfInfo := oc.NetConfInfo.(*util.Layer2NetConfInfo)

	hostSubnets := make([]*net.IPNet, 0, len(layer2NetConfInfo.ClusterSubnets))
	for _, subnet := range layer2NetConfInfo.ClusterSubnets {
		hostSubnet := subnet.CIDR
		hostSubnets = append(hostSubnets, hostSubnet)
		if utilnet.IsIPv6CIDR(hostSubnet) {
			logicalSwitch.OtherConfig = map[string]string{"ipv6_prefix": hostSubnet.IP.String()}
		} else {
			logicalSwitch.OtherConfig = map[string]string{"subnet": hostSubnet.String()}
		}
	}

	err := libovsdbops.CreateOrUpdateLogicalSwitch(oc.nbClient, &logicalSwitch, &logicalSwitch.OtherConfig)
	if err != nil {
		return fmt.Errorf("failed to create logical switch %+v: %v", logicalSwitch, err)
	}

	err = oc.lsManager.AddSwitch(switchName, logicalSwitch.UUID, hostSubnets)
	if err != nil {
		return err
	}

	for _, excludeIP := range layer2NetConfInfo.ExcludeIPs {
		var ipMask net.IPMask
		if excludeIP.To4() != nil {
			ipMask = net.CIDRMask(32, 32)
		} else {
			ipMask = net.CIDRMask(128, 128)
		}

		_ = oc.lsManager.AllocateIPs(switchName, []*net.IPNet{{IP: excludeIP, Mask: ipMask}})
	}

	return nil
}

// ensurePod tries to set up a pod. It returns nil on success and error on failure; failure
// indicates the pod set up should be retried later.
func (oc *SecondaryLayer2NetworkController) ensurePod(oldPod, pod *kapi.Pod, addPort bool) error {
	// Try unscheduled pods later
	if !util.PodScheduled(pod) {
		return nil
	}

	if !util.PodWantsNetwork(pod) && !addPort {
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

func (oc *SecondaryLayer2NetworkController) addLogicalPort(pod *kapi.Pod, nadName string,
	network *networkattachmentdefinitionapi.NetworkSelectionElement) error {
	var libovsdbExecuteTime time.Duration

	// Keep track of how long syncs take.
	start := time.Now()
	defer func() {
		klog.Infof("[%s/%s] addLogicalPort for nad %s took %v, libovsdb time %v",
			pod.Namespace, pod.Name, nadName, time.Since(start), libovsdbExecuteTime)
	}()

	ops, lsp, podAnnotation, newlyCreated, err := oc.addPodLogicalPort(pod, nadName, network)
	if err != nil {
		return err
	}

	recordOps, txOkCallBack, _, err := metrics.GetConfigDurationRecorder().AddOVN(oc.nbClient, "pod", pod.Namespace,
		pod.Name, oc.NetInfo)
	if err != nil {
		klog.Errorf("Config duration recorder: %v", err)
	}
	ops = append(ops, recordOps...)

	transactStart := time.Now()
	_, err = libovsdbops.TransactAndCheckAndSetUUIDs(oc.nbClient, lsp, ops)
	libovsdbExecuteTime = time.Since(transactStart)
	if err != nil {
		return fmt.Errorf("error transacting operations %+v: %v", ops, err)
	}
	txOkCallBack()
	oc.podRecorder.AddLSP(pod.UID, oc.NetInfo)

	// if somehow lspUUID is empty, there is a bug here with interpreting OVSDB results
	if len(lsp.UUID) == 0 {
		return fmt.Errorf("UUID is empty from LSP: %+v", *lsp)
	}

	// Add the pod's logical switch port to the port cache
	switchName := oc.GetPrefix() + types.OvnLayer2Switch
	_ = oc.logicalPortCache.add(switchName, lsp.Name, lsp.UUID, podAnnotation.MAC, podAnnotation.IPs)

	if newlyCreated {
		metrics.RecordPodCreated(pod, oc.NetInfo)
	}
	return nil
}

// removePod tried to tear down a pod. It returns nil on success and error on failure;
// failure indicates the pod tear down should be retried later.
func (oc *SecondaryLayer2NetworkController) removePod(pod *kapi.Pod, portInfo *lpInfo) error {
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
	pInfo, err := oc.deletePodLogicalPort(pod, portInfo, nadName, nil)
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

func (oc *SecondaryLayer2NetworkController) syncPods(pods []interface{}) error {
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
		err = oc.updateExpectedLogicalPorts(pod, annotations, nadName, expectedLogicalPorts)
		if err != nil {
			return err
		}
	}
	switchName := oc.GetPrefix() + types.OvnLayer2Switch
	return oc.deleteStaleLogicalSwitchPorts([]string{switchName}, expectedLogicalPorts)
}
