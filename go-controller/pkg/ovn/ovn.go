package ovn

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"reflect"
	"sync"
	"time"

	nettypes "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	libovsdbclient "github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	egressqoslisters "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/egressqos/v1/apis/listers/egressqos/v1"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
	addressset "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/address_set"
	egresssvc "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/controller/egress_services"
	svccontroller "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/controller/services"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/controller/unidling"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/healthcheck"
	lsm "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/logical_switch_manager"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/subnetallocator"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/syncmap"
	ovntypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	kapi "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	ktypes "k8s.io/apimachinery/pkg/types"
	apierrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	ref "k8s.io/client-go/tools/reference"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"
	utilnet "k8s.io/utils/net"
)

const (
	egressFirewallDNSDefaultDuration time.Duration = 30 * time.Minute
)

// ACL logging severity levels
type ACLLoggingLevels struct {
	Allow string `json:"allow,omitempty"`
	Deny  string `json:"deny,omitempty"`
}

// Controller structure is the object which holds the controls for starting
// and reacting upon the watched resources (e.g. pods, endpoints)
type Controller struct {
	GenericController

	// network configuration information
	nadInfo     *util.NetAttachDefInfo
	isStarted   bool
	startMutex  sync.Mutex
	podHandler  *factory.Handler
	nodeHandler *factory.Handler

	// wg and stopChan is per-Controller
	wg       *sync.WaitGroup
	stopChan chan struct{}

	// configured cluster subnets
	clusterSubnets []config.CIDRNetworkEntry

	// FIXME DUAL-STACK -  Make IP Allocators more dual-stack friendly
	masterSubnetAllocator        *subnetallocator.SubnetAllocator
	hybridOverlaySubnetAllocator *subnetallocator.SubnetAllocator

	// A cache of all logical switches seen by the watcher and their subnets
	lsManager *lsm.LogicalSwitchManager

	// A cache of all logical ports known to the controller
	logicalPortCache *portCache

	// Info about known namespaces. You must use oc.getNamespaceLocked() or
	// oc.waitForNamespaceLocked() to read this map, and oc.createNamespaceLocked()
	// or oc.deleteNamespaceLocked() to modify it. namespacesMutex is only held
	// from inside those functions.
	namespaces      map[string]*namespaceInfo
	namespacesMutex sync.Mutex

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

	// Cluster-wide gateway router default Control Plane Protection (COPP) UUID
	defaultGatewayCOPPUUID string

	// Controller used for programming OVN for egress IP
	eIPC *egressIPController

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

	// v4HostSubnetsUsed keeps track of number of v4 subnets currently assigned to nodes
	// TBD, need to determine if this metrics is per-controller or global
	v4HostSubnetsUsed float64

	// v6HostSubnetsUsed keeps track of number of v6 subnets currently assigned to nodes
	v6HostSubnetsUsed float64

	// RetryObjs needed for both default and secondary networks
	// Objects for pods that need to be retried
	retryPods *RetryObjs
	// Objects for nodes that need to be retried
	retryNodes *RetryObjs

	// RetryObjs needed for default network only
	// Objects for namespaces that need to be retried
	retryNamespaces *RetryObjs
	// Objects for network policies that need to be retried
	retryNetworkPolicies *RetryObjs
	// Objects for egress firewall that need to be retried
	retryEgressFirewalls *RetryObjs
	// Objects for egress IP that need to be retried
	retryEgressIPs *RetryObjs
	// Objects for egress IP Namespaces that need to be retried
	retryEgressIPNamespaces *RetryObjs
	// Objects for egress IP Pods that need to be retried
	retryEgressIPPods *RetryObjs
	// Objects for Egress nodes that need to be retried
	retryEgressNodes *RetryObjs
	// EgressIP Node-specific syncMap used by egressip node event handler
	addEgressNodeFailed sync.Map
	// Objects for Cloud private IP config that need to be retried
	retryCloudPrivateIPConfig *RetryObjs

	// Node-specific syncMap used by node event handler
	gatewaysFailed              sync.Map
	mgmtPortFailed              sync.Map
	addNodeFailed               sync.Map
	nodeClusterRouterPortFailed sync.Map
}

const (
	// TCP is the constant string for the string "TCP"
	TCP = "TCP"

	// UDP is the constant string for the string "UDP"
	UDP = "UDP"

	// SCTP is the constant string for the string "SCTP"
	SCTP = "SCTP"
)

func GetIPFullMask(ip string) string {
	const (
		// IPv4FullMask is the maximum prefix mask for an IPv4 address
		IPv4FullMask = "/32"
		// IPv6FullMask is the maxiumum prefix mask for an IPv6 address
		IPv6FullMask = "/128"
	)

	if utilnet.IsIPv6(net.ParseIP(ip)) {
		return IPv6FullMask
	}
	return IPv4FullMask
}

func (oc *Controller) Start(ctx context.Context) error {
	oc.startMutex.Lock()
	if oc.isStarted {
		oc.startMutex.Unlock()
		return nil
	}
	oc.isStarted = true
	oc.startMutex.Unlock()
	klog.Infof("The first Network Attachment Definition is added to network %s, create associated logical entities", oc.nadInfo.NetName)

	if err := oc.StartClusterMaster(); err != nil {
		return err
	}

	return oc.Run(ctx)
}

func (oc *Controller) Stop() {
	oc.wg.Wait()
	close(oc.stopChan)

	if oc.podHandler != nil {
		oc.watchFactory.RemovePodHandler(oc.podHandler)
	}

	if oc.nodeHandler != nil {
		oc.watchFactory.RemoveNodeHandler(oc.nodeHandler)
	}
}

// Run starts the actual watching.
func (oc *Controller) Run(ctx context.Context) error {
	if !oc.nadInfo.IsSecondary {
		oc.syncPeriodic()
	}
	klog.Infof("Starting all the Watchers for network %s...", oc.nadInfo.NetName)
	start := time.Now()

	if !oc.nadInfo.IsSecondary {
		// Sync external gateway routes. External gateway may be set in namespaces
		// or via pods. So execute an individual sync method at startup
		oc.cleanExGwECMPRoutes()
	}

	// WatchNamespaces() should be started first because it has no other
	// dependencies, and WatchNodes() depends on it
	if !oc.nadInfo.IsSecondary {
		if err := oc.WatchNamespaces(); err != nil {
			return err
		}
	}

	// WatchNodes must be started next because it creates the node switch
	// which most other watches depend on.
	// https://github.com/ovn-org/ovn-kubernetes/pull/859
	if err := oc.WatchNodes(); err != nil {
		return err
	}

	if !oc.nadInfo.IsSecondary {
		// Start service watch factory and sync services
		oc.svcFactory.Start(oc.stopChan)

		// Services should be started after nodes to prevent LB churn
		if err := oc.StartServiceController(oc.wg, true); err != nil {
			return err
		}
	}

	if err := oc.WatchPods(); err != nil {
		return err
	}

	if !oc.nadInfo.IsSecondary {
		// WatchNetworkPolicy depends on WatchPods and WatchNamespaces
		if err := oc.WatchNetworkPolicy(); err != nil {
			return err
		}

		if config.OVNKubernetesFeature.EnableEgressIP {
			// This is probably the best starting order for all egress IP handlers.
			// WatchEgressIPNamespaces and WatchEgressIPPods only use the informer
			// cache to retrieve the egress IPs when determining if namespace/pods
			// match. It is thus better if we initialize them first and allow
			// WatchEgressNodes / WatchEgressIP to initialize after. Those handlers
			// might change the assignments of the existing objects. If we do the
			// inverse and start WatchEgressIPNamespaces / WatchEgressIPPod last, we
			// risk performing a bunch of modifications on the EgressIP objects when
			// we restart and then have these handlers act on stale data when they
			// sync.
			if err := oc.WatchEgressIPNamespaces(); err != nil {
				return err
			}
			if err := oc.WatchEgressIPPods(); err != nil {
				return err
			}
			if err := oc.WatchEgressNodes(); err != nil {
				return err
			}
			if err := oc.WatchEgressIP(); err != nil {
				return err
			}
			if util.PlatformTypeIsEgressIPCloudProvider() {
				if err := oc.WatchCloudPrivateIPConfig(); err != nil {
					return err
				}
			}
			if config.OVNKubernetesFeature.EgressIPReachabiltyTotalTimeout == 0 {
				klog.V(2).Infof("EgressIP node reachability check disabled")
			}
		}
		if config.OVNKubernetesFeature.EgressIPReachabiltyTotalTimeout == 0 {
			klog.V(2).Infof("EgressIP node reachability check disabled")
		} else if config.OVNKubernetesFeature.EgressIPNodeHealthCheckPort != 0 {
			klog.Infof("EgressIP node reachability enabled and using gRPC port %d",
				config.OVNKubernetesFeature.EgressIPNodeHealthCheckPort)
		}

		if config.OVNKubernetesFeature.EnableEgressFirewall {
			var err error
			oc.egressFirewallDNS, err = NewEgressDNS(oc.addressSetFactory, oc.stopChan)
			if err != nil {
				return err
			}
			oc.egressFirewallDNS.Run(egressFirewallDNSDefaultDuration)
			err = oc.WatchEgressFirewall()
			if err != nil {
				return err
			}
		}

		if config.OVNKubernetesFeature.EnableEgressQoS {
			oc.initEgressQoSController(
				oc.watchFactory.EgressQoSInformer(),
				oc.watchFactory.PodCoreInformer(),
				oc.watchFactory.NodeCoreInformer())
			oc.wg.Add(1)
			go func() {
				defer oc.wg.Done()
				oc.runEgressQoSController(1, oc.stopChan)
			}()
		}

		oc.wg.Add(1)
		go func() {
			defer oc.wg.Done()
			oc.egressSvcController.Run(1)
		}()

		klog.Infof("Completing all the Watchers took %v", time.Since(start))

		if config.Kubernetes.OVNEmptyLbEvents {
			klog.Infof("Starting unidling controller")
			unidlingController, err := unidling.NewController(
				oc.recorder,
				oc.watchFactory.ServiceInformer(),
				oc.sbClient,
			)
			if err != nil {
				return err
			}
			oc.wg.Add(1)
			go func() {
				defer oc.wg.Done()
				unidlingController.Run(oc.stopChan)
			}()
		}
	} else {
		klog.Infof("Completing all the Watchers for network %s took %v", oc.nadInfo.NetName, time.Since(start))
	}

	// Final step to cleanup after resource handlers have synced
	err := oc.ovnTopologyCleanup()
	if err != nil {
		klog.Errorf("Failed to cleanup OVN topology to version %d: %v", ovntypes.OvnCurrentTopologyVersion, err)
		return err
	}

	// Master is fully running and resource handlers have synced, update Topology version in OVN and the ConfigMap
	if err := oc.reportTopologyVersion(ctx); err != nil {
		klog.Errorf("Failed to report topology version: %v", err)
		return err
	}

	return nil
}

// syncPeriodic adds a goroutine that periodically does some work
// right now there is only one ticker registered
// for syncNodesPeriodic which deletes chassis records from the sbdb
// every 5 minutes
func (oc *Controller) syncPeriodic() {
	if oc.nadInfo.IsSecondary {
		return
	}

	go func() {
		nodeSyncTicker := time.NewTicker(5 * time.Minute)
		defer nodeSyncTicker.Stop()
		for {
			select {
			case <-nodeSyncTicker.C:
				oc.syncNodesPeriodic()
			case <-oc.stopChan:
				return
			}
		}
	}()
}

func (oc *Controller) recordPodEvent(addErr error, pod *kapi.Pod) {
	podRef, err := ref.GetReference(scheme.Scheme, pod)
	if err != nil {
		klog.Errorf("Couldn't get a reference to pod %s/%s to post an event: '%v'",
			pod.Namespace, pod.Name, err)
	} else {
		klog.V(5).Infof("Posting a %s event for Pod %s/%s on network %s", kapi.EventTypeWarning, pod.Namespace, pod.Name, oc.nadInfo.NetName)
		oc.recorder.Eventf(podRef, kapi.EventTypeWarning, "ErrorAddingLogicalPort", addErr.Error())
	}
}

func exGatewayAnnotationsChanged(oldPod, newPod *kapi.Pod) bool {
	return oldPod.Annotations[util.RoutingNamespaceAnnotation] != newPod.Annotations[util.RoutingNamespaceAnnotation] ||
		oldPod.Annotations[util.RoutingNetworkAnnotation] != newPod.Annotations[util.RoutingNetworkAnnotation] ||
		oldPod.Annotations[util.BfdAnnotation] != newPod.Annotations[util.BfdAnnotation]
}

func networkStatusAnnotationsChanged(oldPod, newPod *kapi.Pod) bool {
	return oldPod.Annotations[nettypes.NetworkStatusAnnot] != newPod.Annotations[nettypes.NetworkStatusAnnot]
}

// ensurePod tries to set up a pod. It returns nil on success and error on failure; failure
// indicates the pod set up should be retried later.
func (oc *Controller) ensurePod(oldPod, pod *kapi.Pod, addPort bool) error {
	// Try unscheduled pods later
	if !util.PodScheduled(pod) {
		return nil
	}

	if !oc.nadInfo.IsSecondary {
		if oldPod != nil && (exGatewayAnnotationsChanged(oldPod, pod) || networkStatusAnnotationsChanged(oldPod, pod)) {
			// No matter if a pod is ovn networked, or host networked, we still need to check for exgw
			// annotations. If the pod is ovn networked and is in update reschedule, addLogicalPort will take
			// care of updating the exgw updates
			if err := oc.deletePodExternalGW(oldPod); err != nil {
				return fmt.Errorf("ensurePod failed %s/%s: %w", pod.Namespace, pod.Name, err)
			}
		}
	}

	if util.PodWantsNetwork(pod) && addPort {
		if err := oc.addLogicalPort(pod); err != nil {
			return fmt.Errorf("addLogicalPort failed for %s/%s network %s: %w", pod.Namespace, pod.Name, oc.nadInfo.NetName, err)
		}
	} else {
		if oc.nadInfo.IsSecondary {
			return nil
		}

		// either pod is host-networked or its an update for a normal pod (addPort=false case)
		if oldPod == nil || exGatewayAnnotationsChanged(oldPod, pod) || networkStatusAnnotationsChanged(oldPod, pod) {
			if err := oc.addPodExternalGW(pod); err != nil {
				return fmt.Errorf("addPodExternalGW failed for %s/%s: %w", pod.Namespace, pod.Name, err)
			}
		}
	}

	return nil
}

// removePod tried to tear down a pod. It returns nil on success and error on failure;
// failure indicates the pod tear down should be retried later.
func (oc *Controller) removePod(pod *kapi.Pod, portInfoMap map[string]*lpInfo) error {
	if !util.PodWantsNetwork(pod) && !oc.nadInfo.IsSecondary {
		if err := oc.deletePodExternalGW(pod); err != nil {
			return fmt.Errorf("unable to delete external gateway routes for pod %s/%s: %w",
				pod.Namespace, pod.Name, err)
		}
		return nil
	}
	if err := oc.deleteLogicalPort(pod, portInfoMap); err != nil {
		return fmt.Errorf("deleteLogicalPort failed for pod %s/%s on network %s: %w",
			pod.Namespace, pod.Name, oc.nadInfo.NetName, err)
	}
	return nil
}

// WatchPods starts the watching of the Pod resource and calls back the appropriate handler logic
func (oc *Controller) WatchPods() error {
	podHandler, err := oc.WatchResource(oc.retryPods)
	if err == nil {
		oc.podHandler = podHandler
	}
	return err
}

// WatchNetworkPolicy starts the watching of the network policy resource and calls
// back the appropriate handler logic
func (oc *Controller) WatchNetworkPolicy() error {
	if oc.nadInfo.IsSecondary {
		klog.Infof("WatchNetworkPolicy for network %s is a no-op", oc.nadInfo.NetName)
		return nil
	}
	_, err := oc.WatchResource(oc.retryNetworkPolicies)
	return err
}

// WatchEgressFirewall starts the watching of egressfirewall resource and calls
// back the appropriate handler logic
func (oc *Controller) WatchEgressFirewall() error {
	if oc.nadInfo.IsSecondary {
		klog.Infof("WatchEgressFirewall for network %s is a no-op", oc.nadInfo.NetName)
		return nil
	}

	_, err := oc.WatchResource(oc.retryEgressFirewalls)
	return err
}

// WatchEgressNodes starts the watching of egress assignable nodes and calls
// back the appropriate handler logic.
func (oc *Controller) WatchEgressNodes() error {
	if oc.nadInfo.IsSecondary {
		klog.Infof("WatchEgressNodes for network %s is a no-op", oc.nadInfo.NetName)
		return nil
	}

	_, err := oc.WatchResource(oc.retryEgressNodes)
	return err
}

// WatchCloudPrivateIPConfig starts the watching of cloudprivateipconfigs
// resource and calls back the appropriate handler logic.
func (oc *Controller) WatchCloudPrivateIPConfig() error {
	if oc.nadInfo.IsSecondary {
		klog.Infof("WatchCloudPrivateIPConfig for network %s is a no-op", oc.nadInfo.NetName)
		return nil
	}

	_, err := oc.WatchResource(oc.retryCloudPrivateIPConfig)
	return err
}

// WatchEgressIP starts the watching of egressip resource and calls back the
// appropriate handler logic. It also initiates the other dedicated resource
// handlers for egress IP setup: namespaces, pods.
func (oc *Controller) WatchEgressIP() error {
	if oc.nadInfo.IsSecondary {
		klog.Infof("WatchEgressIP for network %s is a no-op", oc.nadInfo.NetName)
		return nil
	}

	_, err := oc.WatchResource(oc.retryEgressIPs)
	return err
}

func (oc *Controller) WatchEgressIPNamespaces() error {
	if oc.nadInfo.IsSecondary {
		klog.Infof("WatchEgressIPNamespaces for network %s is a no-op", oc.nadInfo.NetName)
		return nil
	}

	_, err := oc.WatchResource(oc.retryEgressIPNamespaces)
	return err
}

func (oc *Controller) WatchEgressIPPods() error {
	if oc.nadInfo.IsSecondary {
		klog.Infof("WatchEgressIPPods for network %s is a no-op", oc.nadInfo.NetName)
		return nil
	}

	_, err := oc.WatchResource(oc.retryEgressIPPods)
	return err
}

// WatchNamespaces starts the watching of namespace resource and calls
// back the appropriate handler logic
func (oc *Controller) WatchNamespaces() error {
	if oc.nadInfo.IsSecondary {
		klog.Infof("WatchNamespaces for network %s is a no-op", oc.nadInfo.NetName)
		return nil
	}

	_, err := oc.WatchResource(oc.retryNamespaces)
	return err
}

// syncNodeGateway ensures a node's gateway router is configured
func (oc *Controller) syncNodeGateway(node *kapi.Node, hostSubnets []*net.IPNet) error {
	if oc.nadInfo.IsSecondary {
		klog.Infof("WatchNamespaces for network %s is a no-op", oc.nadInfo.NetName)
		return nil
	}

	l3GatewayConfig, err := util.ParseNodeL3GatewayAnnotation(node)
	if err != nil {
		return err
	}

	if hostSubnets == nil {
		hostSubnets, err = util.ParseNodeHostSubnetAnnotation(node, oc.nadInfo.NetName)
		if err != nil {
			return err
		}
	}

	if l3GatewayConfig.Mode == config.GatewayModeDisabled {
		if err := oc.gatewayCleanup(node.Name); err != nil {
			return fmt.Errorf("error cleaning up gateway for node %s: %v", node.Name, err)
		}
		if err := oc.joinSwIPManager.ReleaseJoinLRPIPs(node.Name); err != nil {
			return err
		}
	} else if hostSubnets != nil {
		var hostAddrs sets.String
		if config.Gateway.Mode == config.GatewayModeShared {
			hostAddrs, err = util.ParseNodeHostAddresses(node)
			if err != nil && !util.IsAnnotationNotSetError(err) {
				return fmt.Errorf("failed to get host addresses for node: %s: %v", node.Name, err)
			}
		}
		if err := oc.syncGatewayLogicalNetwork(node, l3GatewayConfig, hostSubnets, hostAddrs); err != nil {
			return fmt.Errorf("error creating gateway for node %s: %v", node.Name, err)
		}
	}
	return nil
}

// WatchNodes starts the watching of node resource and calls
// back the appropriate handler logic
func (oc *Controller) WatchNodes() error {
	nodeHandler, err := oc.WatchResource(oc.retryNodes)
	if err == nil {
		oc.nodeHandler = nodeHandler
	}
	return err
}

// aclLoggingUpdateNsInfo parses the provided annotation values and sets nsInfo.aclLogging.Deny and
// nsInfo.aclLogging.Allow. If errors are encountered parsing the annotation, disable logging completely. If either
// value contains invalid input, disable logging for the respective key. This is needed to ensure idempotency.
// More details:
// *) If the provided annotation cannot be unmarshaled: Disable both Deny and Allow logging. Return an error.
// *) Valid values for "allow" and "deny" are  "alert", "warning", "notice", "info", "debug", "".
// *) Invalid values will return an error, and logging will be disabled for the respective key.
// *) In the following special cases, nsInfo.aclLogging.Deny and nsInfo.aclLogging.Allow. will both be reset to ""
//    without logging an error, meaning that logging will be switched off:
//    i) oc.aclLoggingEnabled == false
//    ii) annotation == ""
//    iii) annotation == "{}"
// *) If one of "allow" or "deny" can be parsed and has a valid value, but the other key is not present in the
//    annotation, then assume that this key should be disabled by setting its nsInfo value to "".
func (oc *Controller) aclLoggingUpdateNsInfo(annotation string, nsInfo *namespaceInfo) error {
	var aclLevels ACLLoggingLevels
	var errors []error

	// If logging is disabled or if the annotation is "" or "{}", use empty strings. Otherwise, parse the annotation.
	if oc.aclLoggingEnabled && annotation != "" && annotation != "{}" {
		err := json.Unmarshal([]byte(annotation), &aclLevels)
		if err != nil {
			// Disable Allow and Deny logging to ensure idempotency.
			nsInfo.aclLogging.Allow = ""
			nsInfo.aclLogging.Deny = ""
			return fmt.Errorf("could not unmarshal namespace ACL annotation '%s', disabling logging, err: %q",
				annotation, err)
		}
	}

	// Valid log levels are the various preestablished levels or the empty string.
	validLogLevels := sets.NewString(nbdb.ACLSeverityAlert, nbdb.ACLSeverityWarning, nbdb.ACLSeverityNotice,
		nbdb.ACLSeverityInfo, nbdb.ACLSeverityDebug, "")

	// Set Deny logging.
	if validLogLevels.Has(aclLevels.Deny) {
		nsInfo.aclLogging.Deny = aclLevels.Deny
	} else {
		errors = append(errors, fmt.Errorf("disabling deny logging due to invalid deny annotation. "+
			"%q is not a valid log severity", aclLevels.Deny))
		nsInfo.aclLogging.Deny = ""
	}

	// Set Allow logging.
	if validLogLevels.Has(aclLevels.Allow) {
		nsInfo.aclLogging.Allow = aclLevels.Allow
	} else {
		errors = append(errors, fmt.Errorf("disabling allow logging due to an invalid allow annotation. "+
			"%q is not a valid log severity", aclLevels.Allow))
		nsInfo.aclLogging.Allow = ""
	}

	return apierrors.NewAggregate(errors)
}

// gatewayChanged() compares old annotations to new and returns true if something has changed.
func gatewayChanged(oldNode, newNode *kapi.Node) bool {
	oldL3GatewayConfig, _ := util.ParseNodeL3GatewayAnnotation(oldNode)
	l3GatewayConfig, _ := util.ParseNodeL3GatewayAnnotation(newNode)
	return !reflect.DeepEqual(oldL3GatewayConfig, l3GatewayConfig)
}

// hostAddressesChanged compares old annotations to new and returns true if the something has changed.
func hostAddressesChanged(oldNode, newNode *kapi.Node) bool {
	oldAddrs, _ := util.ParseNodeHostAddresses(oldNode)
	Addrs, _ := util.ParseNodeHostAddresses(newNode)
	return !oldAddrs.Equal(Addrs)
}

// macAddressChanged() compares old annotations to new and returns true if something has changed.
func macAddressChanged(oldNode, node *kapi.Node) bool {
	oldMacAddress, _ := util.ParseNodeManagementPortMACAddress(oldNode)
	macAddress, _ := util.ParseNodeManagementPortMACAddress(node)
	return !bytes.Equal(oldMacAddress, macAddress)
}

func nodeSubnetChanged(oldNode, node *kapi.Node, netName string) bool {
	oldSubnets, _ := util.ParseNodeHostSubnetAnnotation(oldNode, netName)
	newSubnets, _ := util.ParseNodeHostSubnetAnnotation(node, netName)
	return !reflect.DeepEqual(oldSubnets, newSubnets)
}

func nodeChassisChanged(oldNode, node *kapi.Node) bool {
	oldChassis, _ := util.ParseNodeChassisIDAnnotation(oldNode)
	newChassis, _ := util.ParseNodeChassisIDAnnotation(node)
	return oldChassis != newChassis
}

// noHostSubnet() compares the no-hostsubenet-nodes flag with node labels to see if the node is manageing its
// own network.
func noHostSubnet(node *kapi.Node) bool {
	if config.Kubernetes.NoHostSubnetNodes == nil {
		return false
	}

	nodeSelector, _ := metav1.LabelSelectorAsSelector(config.Kubernetes.NoHostSubnetNodes)
	return nodeSelector.Matches(labels.Set(node.Labels))
}

// shouldUpdate() determines if the ovn-kubernetes plugin should update the state of the node.
// ovn-kube should not perform an update if it does not assign a hostsubnet, or if you want to change
// whether or not ovn-kubernetes assigns a hostsubnet
func shouldUpdate(node, oldNode *kapi.Node) (bool, error) {
	newNoHostSubnet := noHostSubnet(node)
	oldNoHostSubnet := noHostSubnet(oldNode)

	if oldNoHostSubnet && newNoHostSubnet {
		return false, nil
	} else if oldNoHostSubnet && !newNoHostSubnet {
		return false, fmt.Errorf("error updating node %s, cannot remove assigned hostsubnet, please delete node and recreate.", node.Name)
	} else if !oldNoHostSubnet && newNoHostSubnet {
		return false, fmt.Errorf("error updating node %s, cannot assign a hostsubnet to already created node, please delete node and recreate.", node.Name)
	}

	return true, nil
}

func newServiceController(client clientset.Interface, nbClient libovsdbclient.Client, recorder record.EventRecorder) (*svccontroller.Controller, informers.SharedInformerFactory) {
	// Create our own informers to start compartmentalizing the code
	// filter server side the things we don't care about
	noProxyName, err := labels.NewRequirement("service.kubernetes.io/service-proxy-name", selection.DoesNotExist, nil)
	if err != nil {
		panic(err)
	}

	noHeadlessEndpoints, err := labels.NewRequirement(kapi.IsHeadlessService, selection.DoesNotExist, nil)
	if err != nil {
		panic(err)
	}

	labelSelector := labels.NewSelector()
	labelSelector = labelSelector.Add(*noProxyName, *noHeadlessEndpoints)

	svcFactory := informers.NewSharedInformerFactoryWithOptions(client, 0,
		informers.WithTweakListOptions(func(options *metav1.ListOptions) {
			options.LabelSelector = labelSelector.String()
		}))

	controller := svccontroller.NewController(
		client,
		nbClient,
		svcFactory.Core().V1().Services(),
		svcFactory.Discovery().V1().EndpointSlices(),
		svcFactory.Core().V1().Nodes(),
		recorder,
	)

	return controller, svcFactory
}

func (oc *Controller) StartServiceController(wg *sync.WaitGroup, runRepair bool) error {
	if oc.nadInfo.IsSecondary {
		klog.Infof("StartServiceController for network %s is a no-op", oc.nadInfo.NetName)
		return nil
	}

	klog.Infof("Starting OVN Service Controller: Using Endpoint Slices")
	wg.Add(1)
	go func() {
		defer wg.Done()
		useLBGroups := oc.loadBalancerGroupUUID != ""
		// use 5 workers like most of the kubernetes controllers in the
		// kubernetes controller-manager
		err := oc.svcController.Run(5, oc.stopChan, runRepair, useLBGroups)
		if err != nil {
			klog.Errorf("Error running OVN Kubernetes Services controller: %v", err)
		}
	}()
	return nil
}

func newEgressServiceController(client clientset.Interface, nbClient libovsdbclient.Client,
	svcFactory informers.SharedInformerFactory, stopCh <-chan struct{}) *egresssvc.Controller {

	// If the EgressIP controller is enabled it will take care of creating the
	// "no reroute" policies - we can pass "noop" functions to the egress service controller.
	initClusterEgressPolicies := func(libovsdbclient.Client) error { return nil }
	createNodeNoReroutePolicies := func(libovsdbclient.Client, *kapi.Node) error { return nil }
	deleteNodeNoReroutePolicies := func(libovsdbclient.Client, string) error { return nil }

	if !config.OVNKubernetesFeature.EnableEgressIP {
		initClusterEgressPolicies = InitClusterEgressPolicies
		createNodeNoReroutePolicies = CreateDefaultNoRerouteNodePolicies
		deleteNodeNoReroutePolicies = DeleteDefaultNoRerouteNodePolicies
	}

	// TODO: currently an ugly hack to pass the (copied) isReachable func to the egress service controller
	// without touching the egressIP controller code too much before the Controller object is created.
	// This will be removed once we consolidate all of the healthchecks to a different place and have
	// the controllers query a universal cache instead of creating multiple goroutines that do the same thing.
	timeout := config.OVNKubernetesFeature.EgressIPReachabiltyTotalTimeout
	hcPort := config.OVNKubernetesFeature.EgressIPNodeHealthCheckPort
	isReachable := func(nodeName string, mgmtIPs []net.IP, healthClient healthcheck.EgressIPHealthClient) bool {
		// Check if we need to do node reachability check
		if timeout == 0 {
			return true
		}

		if hcPort == 0 {
			return isReachableLegacy(nodeName, mgmtIPs, timeout)
		}

		return isReachableViaGRPC(mgmtIPs, healthClient, hcPort, timeout)
	}

	return egresssvc.NewController(client, nbClient,
		initClusterEgressPolicies, createNodeNoReroutePolicies, deleteNodeNoReroutePolicies, isReachable,
		stopCh, svcFactory.Core().V1().Services(),
		svcFactory.Discovery().V1().EndpointSlices(),
		svcFactory.Core().V1().Nodes())
}
