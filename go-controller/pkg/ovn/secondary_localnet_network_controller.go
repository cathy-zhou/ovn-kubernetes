package ovn

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	libovsdbops "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdbops"
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

type secondaryLocalnetNetworkControllerEventHandler struct {
	baseHandler     baseNetworkControllerEventHandler
	watchFactory    *factory.WatchFactory
	objType         reflect.Type
	oc              *SecondaryLocalnetNetworkController
	extraParameters interface{}
	syncFunc        func([]interface{}) error
}

// AreResourcesEqual returns true if, given two objects of a known resource type, the update logic for this resource
// type considers them equal and therefore no update is needed. It returns false when the two objects are not considered
// equal and an update needs be executed. This is regardless of how the update is carried out (whether with a dedicated update
// function or with a delete on the old obj followed by an add on the new obj).
func (h *secondaryLocalnetNetworkControllerEventHandler) AreResourcesEqual(obj1, obj2 interface{}) (bool, error) {
	return h.baseHandler.areResourcesEqual(h.objType, obj1, obj2)
}

// GetInternalCacheEntry returns the internal cache entry for this object, given an object and its type.
// This is now used only for pods, which will get their the logical port cache entry.
func (h *secondaryLocalnetNetworkControllerEventHandler) GetInternalCacheEntry(obj interface{}) interface{} {
	switch h.objType {
	case factory.PodType:
		pod := obj.(*kapi.Pod)
		return h.oc.getPortInfo4SecondaryNetwork(pod)
	default:
		return nil
	}
}

// GetResourceFromInformerCache returns the latest state of the object, given an object key and its type.
// from the informers cache.
func (h *secondaryLocalnetNetworkControllerEventHandler) GetResourceFromInformerCache(key string) (interface{}, error) {
	return h.baseHandler.getResourceFromInformerCache(h.objType, h.watchFactory, key)
}

// RecordAddEvent records the add event on this given object.
func (h *secondaryLocalnetNetworkControllerEventHandler) RecordAddEvent(obj interface{}) {
}

// RecordUpdateEvent records the udpate event on this given object.
func (h *secondaryLocalnetNetworkControllerEventHandler) RecordUpdateEvent(obj interface{}) {
}

// RecordDeleteEvent records the delete event on this given object.
func (h *secondaryLocalnetNetworkControllerEventHandler) RecordDeleteEvent(obj interface{}) {
}

// RecordSuccessEvent records the success event on this given object.
func (h *secondaryLocalnetNetworkControllerEventHandler) RecordSuccessEvent(obj interface{}) {
}

// RecordErrorEvent records the error event on this given object.
func (h *secondaryLocalnetNetworkControllerEventHandler) RecordErrorEvent(obj interface{}, reason string, err error) {
}

// IsResourceScheduled returns true if the given object has been scheduled.
// Only applied to pods for now. Returns true for all other types.
func (h *secondaryLocalnetNetworkControllerEventHandler) IsResourceScheduled(obj interface{}) bool {
	return h.baseHandler.isResourceScheduled(h.objType, obj)
}

// AddResource adds the specified object to the cluster according to its type and returns the error,
// if any, yielded during object creation.
// Given an object to add and a boolean specifying if the function was executed from iterateRetryResources
func (h *secondaryLocalnetNetworkControllerEventHandler) AddResource(obj interface{}, fromRetryLoop bool) error {
	switch h.objType {
	case factory.PodType:
		pod, ok := obj.(*kapi.Pod)
		if !ok {
			return fmt.Errorf("could not cast %T object to *knet.Pod", obj)
		}
		return h.oc.ensurePod4SecondaryNetworkCommon(pod, true)

	case factory.NamespaceType:
		ns, ok := obj.(*kapi.Namespace)
		if !ok {
			return fmt.Errorf("could not cast %T object to *kapi.Namespace", obj)
		}
		return h.oc.AddNamespace4SecondaryNetwork(ns)

	default:
		return fmt.Errorf("no add function for object type %s", h.objType)
	}
}

// UpdateResource updates the specified object in the cluster to its version in newObj according to its
// type and returns the error, if any, yielded during the object update.
// Given an old and a new object; The inRetryCache boolean argument is to indicate if the given resource
// is in the retryCache or not.
func (h *secondaryLocalnetNetworkControllerEventHandler) UpdateResource(oldObj, newObj interface{}, inRetryCache bool) error {
	switch h.objType {
	case factory.PodType:
		oldPod := oldObj.(*kapi.Pod)
		newPod := newObj.(*kapi.Pod)

		return h.oc.ensurePod4SecondaryNetworkCommon(newPod, inRetryCache || util.PodScheduled(oldPod) != util.PodScheduled(newPod))
	case factory.NamespaceType:
		oldNs, newNs := oldObj.(*kapi.Namespace), newObj.(*kapi.Namespace)
		return h.oc.updateNamespace4SecondaryNetwork(oldNs, newNs)
	}

	return fmt.Errorf("no update function for object type %s", h.objType)
}

// DeleteResource deletes the object from the cluster according to the delete logic of its resource type.
// Given an object and optionally a cachedObj; cachedObj is the internal cache entry for this object,
// used for now for pods and network policies.
func (h *secondaryLocalnetNetworkControllerEventHandler) DeleteResource(obj, cachedObj interface{}) error {
	return h.oc.DeleteSecondaryNetworkResourceCommon(h.objType, obj, cachedObj)
}

func (h *secondaryLocalnetNetworkControllerEventHandler) SyncFunc(objs []interface{}) error {
	var syncFunc func([]interface{}) error

	if h.syncFunc != nil {
		// syncFunc was provided explicitly
		syncFunc = h.syncFunc
	} else {
		switch h.objType {
		case factory.PodType:
			syncFunc = h.oc.syncPods4SecondaryNetwork

		case factory.NamespaceType:
			syncFunc = h.oc.syncNamespaces

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
func (h *secondaryLocalnetNetworkControllerEventHandler) IsObjectInTerminalState(obj interface{}) bool {
	return h.baseHandler.isObjectInTerminalState(h.objType, obj)
}

// SecondaryLocalnetNetworkController is created for logical network infrastructure and policy
// for a secondary layer2 network
type SecondaryLocalnetNetworkController struct {
	NetworkControllerInfo
	wg *sync.WaitGroup
}

// NewSecondaryLocalnetNetworkController create a new OVN controller for the given secondary layer2 nad
func NewSecondaryLocalnetNetworkController(bnc *BaseNetworkController, nInfo util.NetInfo,
	netconfInfo util.NetConfInfo) *SecondaryLocalnetNetworkController {
	stopChan := make(chan struct{})

	oc := &SecondaryLocalnetNetworkController{
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

func (oc *SecondaryLocalnetNetworkController) initRetryFramework() {
	// Init the retry framework for pods
	oc.retryPods = oc.newRetryFramework(factory.PodType)
	oc.retryNamespaces = oc.newRetryFramework(factory.NamespaceType)
	oc.NetworkControllerInfo.initRetryFramework()
}

// newRetryFrameworkMasterWithParameters builds and returns a retry framework for the input resource
// type and assigns all ovnk-master-specific function attributes in the returned struct;
func (oc *SecondaryLocalnetNetworkController) newRetryFramework(
	objectType reflect.Type) *retry.RetryFramework {
	eventHandler := &secondaryLocalnetNetworkControllerEventHandler{
		baseHandler:     baseNetworkControllerEventHandler{},
		objType:         objectType,
		watchFactory:    oc.watchFactory,
		oc:              oc,
		extraParameters: nil, // in use by network policy dynamic watchers
		syncFunc:        nil,
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
func (oc *SecondaryLocalnetNetworkController) Start(ctx context.Context) error {
	klog.Infof("Start secondary %s network controller of network %s", oc.GetTopologyType(), oc.GetNetworkName())
	if err := oc.Init(); err != nil {
		return err
	}

	return oc.Run()
}

// DeleteLogicalEntities delete logical entities for this network
func (oc *SecondaryLocalnetNetworkController) DeleteLogicalEntities(netName string) error {
	klog.Infof("Delete OVN logical entities for %s network controller of network %s", oc.GetTopologyType(), oc.GetNetworkName())
	// delete localnet logical switches
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
func (oc *SecondaryLocalnetNetworkController) Stop(deleteLogicalEntities bool) error {
	klog.Infof("Stop secondary %s network controller of network %s", oc.GetTopologyType(), oc.GetNetworkName())
	close(oc.stopChan)
	oc.wg.Wait()

	if oc.policyHandler != nil {
		oc.watchFactory.RemoveMultiNetworkPolicyHandler(oc.policyHandler)
	}
	if oc.podHandler != nil {
		oc.watchFactory.RemovePodHandler(oc.podHandler)
	}
	if oc.namespaceHandler != nil {
		oc.watchFactory.RemoveNamespaceHandler(oc.namespaceHandler)
	}

	if !deleteLogicalEntities {
		return nil
	}

	// cleanup related OVN logical entities
	return oc.DeleteLogicalEntities(oc.GetNetworkName())
}

func (oc *SecondaryLocalnetNetworkController) Run() error {
	klog.Infof("Starting all the Watchers for network %s ...", oc.GetNetworkName())
	start := time.Now()
	// WatchNamespaces() should be started first because it has no other dependencies
	if err := oc.WatchNamespaces(); err != nil {
		return err
	}

	if err := oc.WatchPods(); err != nil {
		return err
	}

	// WatchNetworkPolicy depends on WatchPods and WatchNamespaces
	if err := oc.WatchNetworkPolicy(); err != nil {
		return err
	}

	klog.Infof("Completing all the Watchers for network %s took %v", oc.GetNetworkName(), time.Since(start))

	// controller is fully running and resource handlers have synced, update Topology version in OVN
	if err := oc.updateL2TopologyVersion(); err != nil {
		return fmt.Errorf("failed to update topology version for network %s: %v", oc.GetNetworkName(), err)
	}

	return nil
}

func (oc *SecondaryLocalnetNetworkController) Init() error {
	switchName := oc.GetPrefix() + types.OVNLocalnetSwitch

	// Add external interface as a logical port to external_switch.
	// This is a learning switch port with "unknown" address. The external
	// world is accessed via this port.
	logicalSwitchPort := nbdb.LogicalSwitchPort{
		Addresses: []string{"unknown"},
		Type:      "localnet",
		Options: map[string]string{
			"network_name": oc.GetPrefix() + types.LocalNetBridgeName,
		},
		Name: oc.GetPrefix() + types.OVNLocalnetPort,
	}
	localnetNetConfInfo := oc.NetConfInfo.(*util.LocalnetNetConfInfo)
	if localnetNetConfInfo.VlanId != 0 {
		intVlanID := int(localnetNetConfInfo.VlanId)
		logicalSwitchPort.TagRequest = &intVlanID
	}

	logicalSwitch := nbdb.LogicalSwitch{
		Name:        switchName,
		ExternalIDs: map[string]string{},
	}
	if oc.IsSecondary() {
		logicalSwitch.ExternalIDs[types.NetworkNameExternalID] = oc.GetNetworkName()
		logicalSwitch.ExternalIDs[types.TopoTypeExternalID] = oc.GetTopologyType()
	}

	hostSubnets := make([]*net.IPNet, 0, len(localnetNetConfInfo.ClusterSubnets))
	for _, subnet := range localnetNetConfInfo.ClusterSubnets {
		hostSubnet := subnet.CIDR
		hostSubnets = append(hostSubnets, hostSubnet)
		if utilnet.IsIPv6CIDR(hostSubnet) {
			logicalSwitch.OtherConfig = map[string]string{"ipv6_prefix": hostSubnet.IP.String()}
		} else {
			logicalSwitch.OtherConfig = map[string]string{"subnet": hostSubnet.String()}
		}
	}

	err := libovsdbops.CreateOrUpdateLogicalSwitchPortsAndSwitch(oc.nbClient, &logicalSwitch, &logicalSwitchPort)
	if err != nil {
		return fmt.Errorf("failed to create logical switch %+v and port %v: %v", logicalSwitch, logicalSwitchPort, err)
	}

	err = oc.lsManager.AddSwitch(switchName, logicalSwitch.UUID, hostSubnets)
	if err != nil {
		return err
	}

	for _, excludeIP := range localnetNetConfInfo.ExcludeIPs {
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
