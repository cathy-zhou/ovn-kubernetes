package ovn

import (
	"context"
	"fmt"
	"reflect"
	"sync"

	ocpcloudnetworkapi "github.com/openshift/api/cloudnetwork/v1"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	egressfirewall "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/egressfirewall/v1"
	egressipv1 "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/egressip/v1"
	egressqoslisters "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/egressqos/v1/apis/listers/egressqos/v1"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/metrics"
	addressset "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/address_set"
	egresssvc "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/controller/egress_services"
	svccontroller "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/controller/services"
	lsm "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/logical_switch_manager"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/subnetallocator"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/retry"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/syncmap"
	ovntypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	kapi "k8s.io/api/core/v1"
	knet "k8s.io/api/networking/v1"
	ktypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
)

type defaultNetworkControllerEventHandler struct {
	masterEventHandler masterEventHandler
	watchFactory       *factory.WatchFactory
	objType            reflect.Type
	oc                 *DefaultNetworkController
	extraParameters    interface{}
	syncFunc           func([]interface{}) error
}

// DefaultNetworkController structure is the object which holds the controls for starting
// and reacting upon the watched resources (e.g. pods, endpoints) for default l3 network
type DefaultNetworkController struct {
	NetworkControllerInfo

	// wg and stopChan per-Controller
	wg       *sync.WaitGroup
	stopChan chan struct{}

	// FIXME DUAL-STACK -  Make IP Allocators more dual-stack friendly
	masterSubnetAllocator        *subnetallocator.HostSubnetAllocator
	hybridOverlaySubnetAllocator *subnetallocator.HostSubnetAllocator

	// For TCP, UDP, and SCTP type traffic, cache OVN load-balancers used for the
	// cluster's east-west traffic.
	loadbalancerClusterCache map[kapi.Protocol]string

	// A cache of all logical switches seen by the watcher and their subnets
	lsManager *lsm.LogicalSwitchManager

	// A cache of all logical ports known to the controller
	logicalPortCache *portCache

	namespaceManager

	externalGWCache map[ktypes.NamespacedName]*externalRouteInfo
	exGWCacheMutex  sync.RWMutex

	// egressFirewalls is a map of namespaces and the egressFirewall attached to it
	egressFirewalls sync.Map

	// EgressQoS
	egressQoSLister egressqoslisters.EgressQoSLister
	egressQoSSynced cache.InformerSynced
	egressQoSQueue  workqueue.RateLimitingInterface
	egressQoSCache  sync.Map

	egressQoSPodLister corev1listers.PodLister
	egressQoSPodSynced cache.InformerSynced
	egressQoSPodQueue  workqueue.RateLimitingInterface

	egressQoSNodeLister corev1listers.NodeLister
	egressQoSNodeSynced cache.InformerSynced
	egressQoSNodeQueue  workqueue.RateLimitingInterface

	// An address set factory that creates address sets
	addressSetFactory addressset.AddressSetFactory

	// network policies map, key should be retrieved with getPolicyKey(policy *knet.NetworkPolicy).
	// network policies that failed to be created will also be added here, and can be retried or cleaned up later.
	// network policy is only deleted from this map after successful cleanup.
	// Allowed order of locking is namespace Lock -> oc.networkPolicies key Lock -> networkPolicy.Lock
	// Don't take namespace Lock while holding networkPolicy key lock to avoid deadlock.
	networkPolicies *syncmap.SyncMap[*networkPolicy]

	// map of existing shared port groups for network policies
	// port group exists in the db if and only if port group key is present in this map
	// key is namespace
	// allowed locking order is namespace Lock -> networkPolicy.Lock -> sharedNetpolPortGroups key Lock
	// make sure to keep this order to avoid deadlocks
	sharedNetpolPortGroups *syncmap.SyncMap[*defaultDenyPortGroups]

	// Supports multicast?
	multicastSupport bool

	// Cluster wide Load_Balancer_Group UUID.
	loadBalancerGroupUUID string

	// Cluster-wide router default Control Plane Protection (COPP) UUID
	defaultCOPPUUID string

	// Controller used for programming OVN for egress IP
	eIPC egressIPController

	// Controller used to handle services
	svcController *svccontroller.Controller
	// Controller used to handle egress services
	egressSvcController *egresssvc.Controller
	// svcFactory used to handle service related events
	svcFactory informers.SharedInformerFactory

	egressFirewallDNS *EgressDNS

	// Is ACL logging enabled while configuring meters?
	aclLoggingEnabled bool

	joinSwIPManager *lsm.JoinSwitchIPManager

	// retry framework for pods
	retryPods *retry.RetryFramework

	// retry framework for network policies
	retryNetworkPolicies *retry.RetryFramework

	// retry framework for egress firewall
	retryEgressFirewalls *retry.RetryFramework

	// retry framework for egress IP
	retryEgressIPs *retry.RetryFramework
	// retry framework for egress IP Namespaces
	retryEgressIPNamespaces *retry.RetryFramework
	// retry framework for egress IP Pods
	retryEgressIPPods *retry.RetryFramework
	// retry framework for Egress nodes
	retryEgressNodes *retry.RetryFramework
	// EgressIP Node-specific syncMap used by egressip node event handler
	addEgressNodeFailed sync.Map

	// retry framework for nodes
	retryNodes *retry.RetryFramework
	// Node-specific syncMaps used by node event handler
	gatewaysFailed              sync.Map
	mgmtPortFailed              sync.Map
	addNodeFailed               sync.Map
	nodeClusterRouterPortFailed sync.Map
	hybridOverlayFailed         sync.Map

	// retry framework for Cloud private IP config
	retryCloudPrivateIPConfig *retry.RetryFramework

	// retry framework for namespaces
	retryNamespaces *retry.RetryFramework

	// variable to determine if all pods present on the node during startup have been processed
	// updated atomically
	allInitialPodsProcessed uint32
}

// NewDefaultController creates a new OVN controller for creating logical network
// infrastructure and policy for default l3 network
func NewDefaultController(bnc *BaseNetworkController) *DefaultNetworkController {
	stopChan := make(chan struct{})
	wg := &sync.WaitGroup{}
	return newDefaultControllerCommon(bnc, stopChan, wg, nil)
}

func newDefaultControllerCommon(bnc *BaseNetworkController,
	defaultStopChan chan struct{}, defaultWg *sync.WaitGroup,
	addressSetFactory addressset.AddressSetFactory) *DefaultNetworkController {

	if addressSetFactory == nil {
		addressSetFactory = addressset.NewOvnAddressSetFactory(bnc.nbClient)
	}
	svcController, svcFactory := newServiceController(bnc.client, bnc.nbClient, bnc.recorder)
	egressSvcController := newEgressServiceController(bnc.client, bnc.nbClient, svcFactory, defaultStopChan)
	var hybridOverlaySubnetAllocator *subnetallocator.HostSubnetAllocator
	if config.HybridOverlay.Enabled {
		hybridOverlaySubnetAllocator = subnetallocator.NewHostSubnetAllocator()
	}
	oc := &DefaultNetworkController{
		NetworkControllerInfo: NetworkControllerInfo{
			BaseNetworkController: *bnc,
			NetConfInfo:           &util.DefaultNetConfInfo{},
			NetInfo: util.NetInfo{
				NetName:     ovntypes.DefaultNetworkName,
				Prefix:      "",
				IsSecondary: false,
				NadNames:    &sync.Map{},
			},
		},
		stopChan:                     defaultStopChan,
		wg:                           defaultWg,
		masterSubnetAllocator:        subnetallocator.NewHostSubnetAllocator(),
		hybridOverlaySubnetAllocator: hybridOverlaySubnetAllocator,
		lsManager:                    lsm.NewLogicalSwitchManager(),
		logicalPortCache:             newPortCache(defaultStopChan),
		namespaceManager: namespaceManager{
			namespaces:      make(map[string]*namespaceInfo),
			namespacesMutex: sync.Mutex{},
		},
		externalGWCache:        make(map[ktypes.NamespacedName]*externalRouteInfo),
		exGWCacheMutex:         sync.RWMutex{},
		addressSetFactory:      addressSetFactory,
		networkPolicies:        syncmap.NewSyncMap[*networkPolicy](),
		sharedNetpolPortGroups: syncmap.NewSyncMap[*defaultDenyPortGroups](),
		eIPC: egressIPController{
			egressIPAssignmentMutex:           &sync.Mutex{},
			podAssignmentMutex:                &sync.Mutex{},
			podAssignment:                     make(map[string]*podAssignmentState),
			pendingCloudPrivateIPConfigsMutex: &sync.Mutex{},
			pendingCloudPrivateIPConfigsOps:   make(map[string]map[string]*cloudPrivateIPConfigOp),
			allocator:                         allocator{&sync.Mutex{}, make(map[string]*egressNode)},
			nbClient:                          bnc.nbClient,
			watchFactory:                      bnc.watchFactory,
			egressIPTotalTimeout:              config.OVNKubernetesFeature.EgressIPReachabiltyTotalTimeout,
			egressIPNodeHealthCheckPort:       config.OVNKubernetesFeature.EgressIPNodeHealthCheckPort,
		},
		loadbalancerClusterCache: make(map[kapi.Protocol]string),
		multicastSupport:         config.EnableMulticast,
		loadBalancerGroupUUID:    "",
		aclLoggingEnabled:        true,
		joinSwIPManager:          nil,
		svcController:            svcController,
		svcFactory:               svcFactory,
		egressSvcController:      egressSvcController,
	}

	oc.initRetryFrameworkForMaster()
	return oc
}

func (oc *DefaultNetworkController) initRetryFrameworkForMaster() {
	// Init the retry framework for pods, namespaces, nodes, network policies, egress firewalls,
	// egress IP (and dependent namespaces, pods, nodes), cloud private ip config.
	oc.retryPods = oc.newRetryFrameworkMasterWithParameters(factory.PodType, nil, nil)
	oc.retryNetworkPolicies = oc.newRetryFrameworkMasterWithParameters(factory.PolicyType, nil, nil)
	oc.retryNodes = oc.newRetryFrameworkMasterWithParameters(factory.NodeType, nil, nil)
	oc.retryEgressFirewalls = oc.newRetryFrameworkMasterWithParameters(factory.EgressFirewallType, nil, nil)
	oc.retryEgressIPs = oc.newRetryFrameworkMasterWithParameters(factory.EgressIPType, nil, nil)
	oc.retryEgressIPNamespaces = oc.newRetryFrameworkMasterWithParameters(factory.EgressIPNamespaceType, nil, nil)
	oc.retryEgressIPPods = oc.newRetryFrameworkMasterWithParameters(factory.EgressIPPodType, nil, nil)
	oc.retryEgressNodes = oc.newRetryFrameworkMasterWithParameters(factory.EgressNodeType, nil, nil)
	oc.retryCloudPrivateIPConfig = oc.newRetryFrameworkMasterWithParameters(factory.CloudPrivateIPConfigType, nil, nil)
	oc.retryNamespaces = oc.newRetryFrameworkMasterWithParameters(factory.NamespaceType, nil, nil)
}

// newRetryFrameworkMasterWithParameters builds and returns a retry framework for the input resource
// type and assigns all ovnk-master-specific function attributes in the returned struct;
// these functions will then be called by the retry logic in the retry package when
// WatchResource() is called.
// newRetryFrameworkMasterWithParameters takes as input a resource type (required)
// and the following optional parameters: a namespace and a label filter for the
// shared informer, a sync function to process all objects of this type at startup,
// and resource-specific extra parameters (used now for network-policy-dependant types).
// In order to create a retry framework for most resource types, newRetryFrameworkMaster is
// to be preferred, as it calls newRetryFrameworkMasterWithParameters with all optional parameters unset.
// newRetryFrameworkMasterWithParameters is instead called directly by the watchers that are
// dynamically created when a network policy is added: PeerNamespaceAndPodSelectorType,
// PeerPodForNamespaceAndPodSelectorType, PeerNamespaceSelectorType, PeerPodSelectorType.
func (oc *DefaultNetworkController) newRetryFrameworkMasterWithParameters(
	objectType reflect.Type,
	syncFunc func([]interface{}) error,
	extraParameters interface{}) *retry.RetryFramework {
	eventHandler := &defaultNetworkControllerEventHandler{
		masterEventHandler: masterEventHandler{},
		objType:            objectType,
		watchFactory:       oc.watchFactory,
		oc:                 oc,
		extraParameters:    extraParameters, // in use by network policy dynamic watchers
		syncFunc:           syncFunc,
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

func (oc *DefaultNetworkController) CompareNetConf(netConfInfo util.NetConfInfo) bool {
	return oc.Compare(netConfInfo)
}

// Start starts the default controller; handles all events and creates all needed logical entities
func (oc *DefaultNetworkController) Start(ctx context.Context) error {
	if err := oc.StartClusterMaster(); err != nil {
		return err
	}

	return oc.Run(ctx, oc.wg)
}

// Stop gracefully stop the controller
// deleteLogicalEntities will never be true for default network
func (oc *DefaultNetworkController) Stop(deleteLogicalEntities bool) error {
	oc.wg.Wait()
	close(oc.stopChan)
	return nil
}

// AreResourcesEqual returns true if, given two objects of a known resource type, the update logic for this resource
// type considers them equal and therefore no update is needed. It returns false when the two objects are not considered
// equal and an update needs be executed. This is regardless of how the update is carried out (whether with a dedicated update
// function or with a delete on the old obj followed by an add on the new obj).
func (h *defaultNetworkControllerEventHandler) AreResourcesEqual(obj1, obj2 interface{}) (bool, error) {
	return h.masterEventHandler.areResourcesEqual(h.objType, obj1, obj2)
}

// GetInternalCacheEntry returns the internal cache entry for this object, given an object and its type.
// This is now used only for pods, which will get their the logical port cache entry.
func (h *defaultNetworkControllerEventHandler) GetInternalCacheEntry(obj interface{}) interface{} {
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
func (h *defaultNetworkControllerEventHandler) GetResourceFromInformerCache(key string) (interface{}, error) {
	return h.masterEventHandler.getResourceFromInformerCache(h.objType, h.watchFactory, key)
}

// RecordAddEvent records the add event on this given object.
func (h *defaultNetworkControllerEventHandler) RecordAddEvent(obj interface{}) {
	switch h.objType {
	case factory.PodType:
		pod := obj.(*kapi.Pod)
		klog.V(5).Infof("Recording add event on pod %s/%s", pod.Namespace, pod.Name)
		h.oc.podRecorder.AddPod(pod.UID)
		metrics.GetConfigDurationRecorder().Start("pod", pod.Namespace, pod.Name)
	case factory.PolicyType:
		np := obj.(*knet.NetworkPolicy)
		klog.V(5).Infof("Recording add event on network policy %s/%s", np.Namespace, np.Name)
		metrics.GetConfigDurationRecorder().Start("networkpolicy", np.Namespace, np.Name)
	}
}

// RecordUpdateEvent records the update event on this given object.
func (h *defaultNetworkControllerEventHandler) RecordUpdateEvent(obj interface{}) {
	switch h.objType {
	case factory.PodType:
		pod := obj.(*kapi.Pod)
		klog.V(5).Infof("Recording update event on pod %s/%s", pod.Namespace, pod.Name)
		metrics.GetConfigDurationRecorder().Start("pod", pod.Namespace, pod.Name)
	case factory.PolicyType:
		np := obj.(*knet.NetworkPolicy)
		klog.V(5).Infof("Recording update event on network policy %s/%s", np.Namespace, np.Name)
		metrics.GetConfigDurationRecorder().Start("networkpolicy", np.Namespace, np.Name)
	}
}

// RecordDeleteEvent records the delete event on this given object.
func (h *defaultNetworkControllerEventHandler) RecordDeleteEvent(obj interface{}) {
	switch h.objType {
	case factory.PodType:
		pod := obj.(*kapi.Pod)
		klog.V(5).Infof("Recording delete event on pod %s/%s", pod.Namespace, pod.Name)
		h.oc.podRecorder.CleanPod(pod.UID)
		metrics.GetConfigDurationRecorder().Start("pod", pod.Namespace, pod.Name)
	case factory.PolicyType:
		np := obj.(*knet.NetworkPolicy)
		klog.V(5).Infof("Recording delete event on network policy %s/%s", np.Namespace, np.Name)
		metrics.GetConfigDurationRecorder().Start("networkpolicy", np.Namespace, np.Name)
	}
}

// RecordSuccessEvent records the success event on this given object.
func (h *defaultNetworkControllerEventHandler) RecordSuccessEvent(obj interface{}) {
	switch h.objType {
	case factory.PodType:
		pod := obj.(*kapi.Pod)
		klog.V(5).Infof("Recording success event on pod %s/%s", pod.Namespace, pod.Name)
		metrics.GetConfigDurationRecorder().End("pod", pod.Namespace, pod.Name)
	case factory.PolicyType:
		np := obj.(*knet.NetworkPolicy)
		klog.V(5).Infof("Recording success event on network policy %s/%s", np.Namespace, np.Name)
		metrics.GetConfigDurationRecorder().End("networkpolicy", np.Namespace, np.Name)
	}
}

// RecordErrorEvent records an error event on the given object.
// Only used for pods now.
func (h *defaultNetworkControllerEventHandler) RecordErrorEvent(obj interface{}, reason string, err error) {
	switch h.objType {
	case factory.PodType:
		pod := obj.(*kapi.Pod)
		klog.V(5).Infof("Recording error event on pod %s/%s", pod.Namespace, pod.Name)
		h.oc.recordPodEvent(reason, err, pod)
	}
}

// IsResourceScheduled returns true if the given object has been scheduled.
// Only applied to pods for now. Returns true for all other types.
func (h *defaultNetworkControllerEventHandler) IsResourceScheduled(obj interface{}) bool {
	return h.masterEventHandler.isResourceScheduled(h.objType, obj)
}

// Given a *RetryFramework instance, an object to add and a boolean specifying if the function was executed from
// iterateRetryResources, AddResource adds the specified object to the cluster according to its type and
// returns the error, if any, yielded during object creation.
func (h *defaultNetworkControllerEventHandler) AddResource(obj interface{}, fromRetryLoop bool) error {
	var err error

	switch h.objType {
	case factory.PodType:
		pod, ok := obj.(*kapi.Pod)
		if !ok {
			return fmt.Errorf("could not cast %T object to *knet.Pod", obj)
		}
		return h.oc.ensurePod(nil, pod, true)

	case factory.PolicyType:
		np, ok := obj.(*knet.NetworkPolicy)
		if !ok {
			return fmt.Errorf("could not cast %T object to *knet.NetworkPolicy", obj)
		}

		if err = h.oc.addNetworkPolicy(np); err != nil {
			klog.Infof("Network Policy add failed for %s/%s, will try again later: %v",
				np.Namespace, np.Name, err)
			return err
		}

	case factory.NodeType:
		node, ok := obj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast %T object to *kapi.Node", obj)
		}
		var nodeParams *nodeSyncs
		if fromRetryLoop {
			_, nodeSync := h.oc.addNodeFailed.Load(node.Name)
			_, clusterRtrSync := h.oc.nodeClusterRouterPortFailed.Load(node.Name)
			_, mgmtSync := h.oc.mgmtPortFailed.Load(node.Name)
			_, gwSync := h.oc.gatewaysFailed.Load(node.Name)
			_, hoSync := h.oc.hybridOverlayFailed.Load(node.Name)
			nodeParams = &nodeSyncs{
				nodeSync,
				clusterRtrSync,
				mgmtSync,
				gwSync,
				hoSync}
		} else {
			nodeParams = &nodeSyncs{true, true, true, true, config.HybridOverlay.Enabled}
		}

		if err = h.oc.addUpdateNodeEvent(node, nodeParams); err != nil {
			klog.Infof("Node add failed for %s, will try again later: %v",
				node.Name, err)
			return err
		}

	case factory.PeerPodSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.oc.handlePeerPodSelectorAddUpdate(extraParameters.np, extraParameters.gp, obj)

	case factory.PeerNamespaceAndPodSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.oc.handlePeerNamespaceAndPodAdd(extraParameters.np, extraParameters.gp,
			extraParameters.podSelector, obj)

	case factory.PeerPodForNamespaceAndPodSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.oc.handlePeerPodSelectorAddUpdate(extraParameters.np, extraParameters.gp, obj)

	case factory.PeerNamespaceSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.oc.handlePeerNamespaceSelectorAdd(extraParameters.np, extraParameters.gp, obj)

	case factory.LocalPodSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.oc.handleLocalPodSelectorAddFunc(
			extraParameters.np,
			obj)

	case factory.EgressFirewallType:
		var err error
		egressFirewall := obj.(*egressfirewall.EgressFirewall).DeepCopy()
		if err = h.oc.addEgressFirewall(egressFirewall); err != nil {
			egressFirewall.Status.Status = egressFirewallAddError
		} else {
			egressFirewall.Status.Status = egressFirewallAppliedCorrectly
			metrics.UpdateEgressFirewallRuleCount(float64(len(egressFirewall.Spec.Egress)))
			metrics.IncrementEgressFirewallCount()
		}
		if err = h.oc.updateEgressFirewallStatusWithRetry(egressFirewall); err != nil {
			klog.Errorf("Failed to update egress firewall status %s, error: %v",
				getEgressFirewallNamespacedName(egressFirewall), err)
		}
		return err

	case factory.EgressIPType:
		eIP := obj.(*egressipv1.EgressIP)
		return h.oc.reconcileEgressIP(nil, eIP)

	case factory.EgressIPNamespaceType:
		namespace := obj.(*kapi.Namespace)
		return h.oc.reconcileEgressIPNamespace(nil, namespace)

	case factory.EgressIPPodType:
		pod := obj.(*kapi.Pod)
		return h.oc.reconcileEgressIPPod(nil, pod)

	case factory.EgressNodeType:
		node := obj.(*kapi.Node)
		if err := h.oc.setupNodeForEgress(node); err != nil {
			return err
		}
		nodeEgressLabel := util.GetNodeEgressLabel()
		nodeLabels := node.GetLabels()
		_, hasEgressLabel := nodeLabels[nodeEgressLabel]
		if hasEgressLabel {
			h.oc.setNodeEgressAssignable(node.Name, true)
		}
		isReady := h.oc.isEgressNodeReady(node)
		if isReady {
			h.oc.setNodeEgressReady(node.Name, true)
		}
		isReachable := h.oc.isEgressNodeReachable(node)
		if isReachable {
			h.oc.setNodeEgressReachable(node.Name, true)
		}
		if hasEgressLabel && isReachable && isReady {
			if err := h.oc.addEgressNode(node.Name); err != nil {
				return err
			}
		}

	case factory.CloudPrivateIPConfigType:
		cloudPrivateIPConfig := obj.(*ocpcloudnetworkapi.CloudPrivateIPConfig)
		return h.oc.reconcileCloudPrivateIPConfig(nil, cloudPrivateIPConfig)

	case factory.NamespaceType:
		ns, ok := obj.(*kapi.Namespace)
		if !ok {
			return fmt.Errorf("could not cast %T object to *kapi.Namespace", obj)
		}
		return h.oc.AddNamespace(ns)

	default:
		return fmt.Errorf("no add function for object type %s", h.objType)
	}

	return nil
}

// Given a *RetryFramework instance, an old and a new object, UpdateResource updates the specified object in the cluster
// to its version in newObj according to its type and returns the error, if any, yielded during the object update.
// The inRetryCache boolean argument is to indicate if the given resource is in the retryCache or not.
func (h *defaultNetworkControllerEventHandler) UpdateResource(oldObj, newObj interface{}, inRetryCache bool) error {
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
		_, failed = h.oc.mgmtPortFailed.Load(newNode.Name)
		mgmtSync := failed || macAddressChanged(oldNode, newNode) || nodeSubnetChanged(oldNode, newNode)
		_, failed = h.oc.gatewaysFailed.Load(newNode.Name)
		gwSync := (failed || gatewayChanged(oldNode, newNode) ||
			nodeSubnetChanged(oldNode, newNode) || hostAddressesChanged(oldNode, newNode))
		_, hoSync := h.oc.hybridOverlayFailed.Load(newNode.Name)

		return h.oc.addUpdateNodeEvent(newNode, &nodeSyncs{nodeSync, clusterRtrSync, mgmtSync, gwSync, hoSync})

	case factory.PeerPodSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.oc.handlePeerPodSelectorAddUpdate(extraParameters.np, extraParameters.gp, newObj)

	case factory.PeerPodForNamespaceAndPodSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.oc.handlePeerPodSelectorAddUpdate(extraParameters.np, extraParameters.gp, newObj)

	case factory.LocalPodSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.oc.handleLocalPodSelectorAddFunc(
			extraParameters.np,
			newObj)

	case factory.EgressIPType:
		oldEIP := oldObj.(*egressipv1.EgressIP)
		newEIP := newObj.(*egressipv1.EgressIP)
		return h.oc.reconcileEgressIP(oldEIP, newEIP)

	case factory.EgressIPNamespaceType:
		oldNamespace := oldObj.(*kapi.Namespace)
		newNamespace := newObj.(*kapi.Namespace)
		return h.oc.reconcileEgressIPNamespace(oldNamespace, newNamespace)

	case factory.EgressIPPodType:
		oldPod := oldObj.(*kapi.Pod)
		newPod := newObj.(*kapi.Pod)
		return h.oc.reconcileEgressIPPod(oldPod, newPod)

	case factory.EgressNodeType:
		oldNode := oldObj.(*kapi.Node)
		newNode := newObj.(*kapi.Node)
		// Initialize the allocator on every update,
		// ovnkube-node/cloud-network-config-controller will make sure to
		// annotate the node with the egressIPConfig, but that might have
		// happened after we processed the ADD for that object, hence keep
		// retrying for all UPDATEs.
		if err := h.oc.initEgressIPAllocator(newNode); err != nil {
			klog.Warningf("Egress node initialization error: %v", err)
		}
		nodeEgressLabel := util.GetNodeEgressLabel()
		oldLabels := oldNode.GetLabels()
		newLabels := newNode.GetLabels()
		_, oldHadEgressLabel := oldLabels[nodeEgressLabel]
		_, newHasEgressLabel := newLabels[nodeEgressLabel]
		// If the node is not labeled for egress assignment, just return
		// directly, we don't really need to set the ready / reachable
		// status on this node if the user doesn't care about using it.
		if !oldHadEgressLabel && !newHasEgressLabel {
			return nil
		}
		if oldHadEgressLabel && !newHasEgressLabel {
			klog.Infof("Node: %s has been un-labeled, deleting it from egress assignment", newNode.Name)
			h.oc.setNodeEgressAssignable(oldNode.Name, false)
			return h.oc.deleteEgressNode(oldNode.Name)
		}
		isOldReady := h.oc.isEgressNodeReady(oldNode)
		isNewReady := h.oc.isEgressNodeReady(newNode)
		isNewReachable := h.oc.isEgressNodeReachable(newNode)
		h.oc.setNodeEgressReady(newNode.Name, isNewReady)
		h.oc.setNodeEgressReachable(newNode.Name, isNewReachable)
		if !oldHadEgressLabel && newHasEgressLabel {
			klog.Infof("Node: %s has been labeled, adding it for egress assignment", newNode.Name)
			h.oc.setNodeEgressAssignable(newNode.Name, true)
			if isNewReady && isNewReachable {
				if err := h.oc.addEgressNode(newNode.Name); err != nil {
					return err
				}
			} else {
				klog.Warningf("Node: %s has been labeled, but node is not ready"+
					" and reachable, cannot use it for egress assignment", newNode.Name)
			}
			return nil
		}
		if isOldReady == isNewReady {
			return nil
		}
		if !isNewReady {
			klog.Warningf("Node: %s is not ready, deleting it from egress assignment", newNode.Name)
			if err := h.oc.deleteEgressNode(newNode.Name); err != nil {
				return err
			}
		} else if isNewReady && isNewReachable {
			klog.Infof("Node: %s is ready and reachable, adding it for egress assignment", newNode.Name)
			if err := h.oc.addEgressNode(newNode.Name); err != nil {
				return err
			}
		}
		return nil

	case factory.CloudPrivateIPConfigType:
		oldCloudPrivateIPConfig := oldObj.(*ocpcloudnetworkapi.CloudPrivateIPConfig)
		newCloudPrivateIPConfig := newObj.(*ocpcloudnetworkapi.CloudPrivateIPConfig)
		return h.oc.reconcileCloudPrivateIPConfig(oldCloudPrivateIPConfig, newCloudPrivateIPConfig)

	case factory.NamespaceType:
		oldNs, newNs := oldObj.(*kapi.Namespace), newObj.(*kapi.Namespace)
		return h.oc.updateNamespace(oldNs, newNs)
	}
	return fmt.Errorf("no update function for object type %s", h.objType)
}

// Given a *RetryFramework instance, an object and optionally a cachedObj, DeleteResource deletes the object from the cluster
// according to the delete logic of its resource type. cachedObj is the internal cache entry for this object,
// used for now for pods and network policies.
func (h *defaultNetworkControllerEventHandler) DeleteResource(obj, cachedObj interface{}) error {
	switch h.objType {
	case factory.PodType:
		var portInfo *lpInfo
		pod := obj.(*kapi.Pod)

		if cachedObj != nil {
			portInfo = cachedObj.(*lpInfo)
		}
		h.oc.logicalPortCache.remove(util.GetLogicalPortName(pod.Namespace, pod.Name))
		return h.oc.removePod(pod, portInfo)

	case factory.PolicyType:
		knp, ok := obj.(*knet.NetworkPolicy)
		if !ok {
			return fmt.Errorf("could not cast obj of type %T to *knet.NetworkPolicy", obj)
		}
		return h.oc.deleteNetworkPolicy(knp)

	case factory.NodeType:
		node, ok := obj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast obj of type %T to *knet.Node", obj)
		}
		return h.oc.deleteNodeEvent(node)

	case factory.PeerPodSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.oc.handlePeerPodSelectorDelete(extraParameters.np, extraParameters.gp, obj)

	case factory.PeerNamespaceAndPodSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.oc.handlePeerNamespaceAndPodDel(extraParameters.np, extraParameters.gp, obj)

	case factory.PeerPodForNamespaceAndPodSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.oc.handlePeerPodSelectorDelete(extraParameters.np, extraParameters.gp, obj)

	case factory.PeerNamespaceSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.oc.handlePeerNamespaceSelectorDel(extraParameters.np, extraParameters.gp, obj)

	case factory.LocalPodSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.oc.handleLocalPodSelectorDelFunc(
			extraParameters.np,
			obj)

	case factory.EgressFirewallType:
		egressFirewall := obj.(*egressfirewall.EgressFirewall)
		if err := h.oc.deleteEgressFirewall(egressFirewall); err != nil {
			return err
		}
		metrics.UpdateEgressFirewallRuleCount(float64(-len(egressFirewall.Spec.Egress)))
		metrics.DecrementEgressFirewallCount()
		return nil

	case factory.EgressIPType:
		eIP := obj.(*egressipv1.EgressIP)
		return h.oc.reconcileEgressIP(eIP, nil)

	case factory.EgressIPNamespaceType:
		namespace := obj.(*kapi.Namespace)
		return h.oc.reconcileEgressIPNamespace(namespace, nil)

	case factory.EgressIPPodType:
		pod := obj.(*kapi.Pod)
		return h.oc.reconcileEgressIPPod(pod, nil)

	case factory.EgressNodeType:
		node := obj.(*kapi.Node)
		if err := h.oc.deleteNodeForEgress(node); err != nil {
			return err
		}
		nodeEgressLabel := util.GetNodeEgressLabel()
		nodeLabels := node.GetLabels()
		if _, hasEgressLabel := nodeLabels[nodeEgressLabel]; hasEgressLabel {
			if err := h.oc.deleteEgressNode(node.Name); err != nil {
				return err
			}
		}
		return nil

	case factory.CloudPrivateIPConfigType:
		cloudPrivateIPConfig := obj.(*ocpcloudnetworkapi.CloudPrivateIPConfig)
		return h.oc.reconcileCloudPrivateIPConfig(cloudPrivateIPConfig, nil)

	case factory.NamespaceType:
		ns := obj.(*kapi.Namespace)
		return h.oc.deleteNamespace(ns)

	default:
		return fmt.Errorf("object type %s not supported", h.objType)
	}
}

func (h *defaultNetworkControllerEventHandler) SyncFunc(objs []interface{}) error {
	var syncFunc func([]interface{}) error

	if h.syncFunc != nil {
		// syncFunc was provided explicitly
		syncFunc = h.syncFunc
	} else {
		switch h.objType {
		case factory.PodType:
			syncFunc = h.oc.syncPods

		case factory.PolicyType:
			syncFunc = h.oc.syncNetworkPolicies

		case factory.NodeType:
			syncFunc = h.oc.syncNodes

		case factory.LocalPodSelectorType,
			factory.PeerNamespaceAndPodSelectorType,
			factory.PeerPodSelectorType,
			factory.PeerPodForNamespaceAndPodSelectorType,
			factory.PeerNamespaceSelectorType:
			syncFunc = nil

		case factory.EgressFirewallType:
			syncFunc = h.oc.syncEgressFirewall

		case factory.EgressIPNamespaceType:
			syncFunc = h.oc.syncEgressIPs

		case factory.EgressNodeType:
			syncFunc = h.oc.initClusterEgressPolicies

		case factory.EgressIPPodType,
			factory.EgressIPType,
			factory.CloudPrivateIPConfigType:
			syncFunc = nil

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
func (h *defaultNetworkControllerEventHandler) IsObjectInTerminalState(obj interface{}) bool {
	return h.masterEventHandler.isObjectInTerminalState(h.objType, obj)
}
