package ovn

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"reflect"
	"strings"
	"sync"
	"time"

	knetattachment "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	"github.com/openshift/origin/pkg/util/netutils"
	ovntypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/cni/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	util "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	"github.com/sirupsen/logrus"
	kapi "k8s.io/api/core/v1"
	kapisnetworking "k8s.io/api/networking/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	kv1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
)

// ServiceVIPKey is used for looking up service namespace information for a
// particular load balancer
type ServiceVIPKey struct {
	// Load balancer VIP in the form "ip:port"
	vip string
	// Protocol used by the load balancer
	protocol kapi.Protocol
}

type networkAttachmentDefinitionConfig struct {
	isDefault                 bool
	masterSubnetAllocatorList []*netutils.SubnetAllocator
	cidr                      string
	mtu                       int
	enableGateway             bool
	pods                      map[string]bool
}

// Controller structure is the object which holds the controls for starting
// and reacting upon the watched resources (e.g. pods, endpoints)
type Controller struct {
	kube         kube.Interface
	watchFactory *factory.WatchFactory

	netAttchmtDefs map[string]*networkAttachmentDefinitionConfig
	// A mutex for netAttchmtDefs
	netMutex *sync.Mutex

	// key is node name, value is map of subnet whose key is network name
	nodeCache map[string]map[string]string
	// A mutex for nodeCache, held before netMutex
	nodeMutex *sync.Mutex

	// LoadBalance for K8S service access, default netName only
	TCPLoadBalancerUUID string
	UDPLoadBalancerUUID string

	// XXX CATHY gateway IP, key is logical switch name of netPrefix+nodeName
	gatewayCache map[string]string
	// For TCP and UDP type traffic, cache OVN load-balancers used for the
	// cluster's east-west traffic. For K8S service access, default netName
	// only. key is protocol ("TCP" or "UDP")
	loadbalancerClusterCache map[string]string

	// For TCP and UDP type traffice, cache OVN load balancer that exists on the
	// default gateway. For nodePort service access, default netName only
	// key is protocol ("TCP" or "UDP")
	loadbalancerGWCache map[string]string
	defGatewayRouter    string

	// XXX CATHY Existence of node Switch, key is netPrefix+nodeName. A cache of all logical switches seen by the watcher
	logicalSwitchCache map[string]bool

	// XXX CATHY Logical switch of the Pod connect to, key is the logical switch port name of the Pod
	// A cache of all logical ports seen by the watcher and
	// its corresponding logical switch
	logicalPortCache map[string]string

	// XXX CATHY For policy use, UUID of logical port of Pod. default netName only for now.
	// A cache of all logical ports and its corresponding uuids.
	logicalPortUUIDCache map[string]string

	// For each namespace, a map from pod IP address to logical port name
	// for all pods in that namespace.
	namespaceAddressSet map[string]map[string]string

	// For each namespace, a lock to protect critical regions
	namespaceMutex map[string]*sync.Mutex

	// Need to make calls to namespaceMutex also thread-safe
	namespaceMutexMutex sync.Mutex

	// For each namespace, a map of policy name to 'namespacePolicy'.
	namespacePolicies map[string]map[string]*namespacePolicy

	// Port group for ingress deny rule
	portGroupIngressDeny string

	// Port group for egress deny rule
	portGroupEgressDeny string

	// For each logical port, the number of network policies that want
	// to add a ingress deny rule.
	lspIngressDenyCache map[string]int

	// For each logical port, the number of network policies that want
	// to add a egress deny rule.
	lspEgressDenyCache map[string]int

	// A mutex for lspIngressDenyCache and lspEgressDenyCache
	lspMutex *sync.Mutex

	// A mutex for gatewayCache and logicalSwitchCache which holds
	// logicalSwitch information
	lsMutex *sync.Mutex

	// Per namespace multicast enabled?
	multicastEnabled map[string]bool

	// Supports port_group?
	portGroupSupport bool

	// Supports multicast?
	multicastSupport bool

	// Map of load balancers to service namespace
	serviceVIPToName map[ServiceVIPKey]types.NamespacedName

	serviceVIPToNameLock sync.Mutex
}

const (
	// TCP is the constant string for the string "TCP"
	TCP = "TCP"

	// UDP is the constant string for the string "UDP"
	UDP = "UDP"
)

// NewOvnController creates a new OVN controller for creating logical network
// infrastructure and policy
func NewOvnController(kubeClient kubernetes.Interface, wf *factory.WatchFactory) *Controller {
	return &Controller{
		kube:                     &kube.Kube{KClient: kubeClient},
		watchFactory:             wf,
		netAttchmtDefs:           make(map[string]*networkAttachmentDefinitionConfig),
		netMutex:                 &sync.Mutex{},
		logicalSwitchCache:       make(map[string]bool),
		logicalPortCache:         make(map[string]string),
		logicalPortUUIDCache:     make(map[string]string),
		namespaceAddressSet:      make(map[string]map[string]string),
		namespacePolicies:        make(map[string]map[string]*namespacePolicy),
		namespaceMutex:           make(map[string]*sync.Mutex),
		namespaceMutexMutex:      sync.Mutex{},
		lspIngressDenyCache:      make(map[string]int),
		lspEgressDenyCache:       make(map[string]int),
		lspMutex:                 &sync.Mutex{},
		lsMutex:                  &sync.Mutex{},
		gatewayCache:             make(map[string]string),
		loadbalancerClusterCache: make(map[string]string),
		loadbalancerGWCache:      make(map[string]string),
		multicastEnabled:         make(map[string]bool),
		serviceVIPToName:         make(map[ServiceVIPKey]types.NamespacedName),
		serviceVIPToNameLock:     sync.Mutex{},
		nodeMutex:                &sync.Mutex{},
	}
}

// Run starts the actual watching.
func (oc *Controller) Run(stopChan chan struct{}) error {
	startOvnUpdater()

	// watchNetworkAttachmentDefinition needs to be called before WatchNodes()
	// so that the masterSubnetAllocatorList for each netName can correctly
	// take into account all the existing subnet range of the existing Nodes;
	//
	// Then WatchNodes must be started first so that its initial Add will
	// create all node logical switches, which other watches may depend on.
	// https://github.com/ovn-org/ovn-kubernetes/pull/859
	for _, f := range []func() error{oc.watchNetworkAttachmentDefinition, oc.WatchNodes} {
		if err := f(); err != nil {
			return err
		}
	}

	for _, f := range []func() error{oc.WatchPods, oc.WatchServices, oc.WatchEndpoints,
		oc.WatchNamespaces, oc.WatchNetworkPolicy} {
		if err := f(); err != nil {
			return err
		}
	}

	if config.Kubernetes.OVNEmptyLbEvents {
		go oc.ovnControllerEventChecker(stopChan)
	}

	return nil
}

type eventRecord struct {
	Data     [][]interface{} `json:"Data"`
	Headings []string        `json:"Headings"`
}

type emptyLBBackendEvent struct {
	vip      string
	protocol kapi.Protocol
	uuid     string
}

func extractEmptyLBBackendsEvents(out []byte) ([]emptyLBBackendEvent, error) {
	events := make([]emptyLBBackendEvent, 0, 4)

	var f eventRecord
	err := json.Unmarshal(out, &f)
	if err != nil {
		return events, err
	}
	if len(f.Data) == 0 {
		return events, nil
	}

	var eventInfoIndex int
	var eventTypeIndex int
	var uuidIndex int
	for idx, val := range f.Headings {
		switch val {
		case "event_info":
			eventInfoIndex = idx
		case "event_type":
			eventTypeIndex = idx
		case "_uuid":
			uuidIndex = idx
		}
	}

	for _, val := range f.Data {
		if len(val) <= eventTypeIndex {
			return events, errors.New("Mismatched Data and Headings in controller event")
		}
		if val[eventTypeIndex] != "empty_lb_backends" {
			continue
		}

		uuidArray, ok := val[uuidIndex].([]interface{})
		if !ok {
			return events, errors.New("Unexpected '_uuid' data in controller event")
		}
		if len(uuidArray) < 2 {
			return events, errors.New("Malformed UUID presented in controller event")
		}
		uuid, ok := uuidArray[1].(string)
		if !ok {
			return events, errors.New("Failed to parse UUID in controller event")
		}

		// Unpack the data. There's probably a better way to do this.
		info, ok := val[eventInfoIndex].([]interface{})
		if !ok {
			return events, errors.New("Unexpected 'event_info' data in controller event")
		}
		if len(info) < 2 {
			return events, errors.New("Malformed event_info in controller event")
		}
		eventMap, ok := info[1].([]interface{})
		if !ok {
			return events, errors.New("'event_info' data is not the expected type")
		}

		var vip string
		var protocol kapi.Protocol
		for _, x := range eventMap {
			tuple, ok := x.([]interface{})
			if !ok {
				return events, errors.New("event map item failed to parse")
			}
			if len(tuple) < 2 {
				return events, errors.New("event map contains malformed data")
			}
			switch tuple[0] {
			case "vip":
				vip, ok = tuple[1].(string)
				if !ok {
					return events, errors.New("Failed to parse vip in controller event")
				}
			case "protocol":
				prot, ok := tuple[1].(string)
				if !ok {
					return events, errors.New("Failed to parse protocol in controller event")
				}
				if prot == "udp" {
					protocol = kapi.ProtocolUDP
				} else {
					protocol = kapi.ProtocolTCP
				}
			}
		}
		events = append(events, emptyLBBackendEvent{vip, protocol, uuid})
	}

	return events, nil
}

func (oc *Controller) ovnControllerEventChecker(stopChan chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)

	_, _, err := util.RunOVNNbctl("set", "nb_global", ".", "options:controller_event=true")
	if err != nil {
		logrus.Error("Unable to enable controller events. Unidling not possible")
		return
	}

	eventBroadcaster := record.NewBroadcaster()
	eventBroadcaster.StartRecordingToSink(&kv1core.EventSinkImpl{Interface: oc.kube.Events()})
	recorder := eventBroadcaster.NewRecorder(scheme.Scheme, kapi.EventSource{Component: "kube-proxy"})

	for {
		select {
		case <-ticker.C:
			out, _, err := util.RunOVNSbctl("--format=json", "list", "controller_event")
			if err != nil {
				continue
			}

			events, err := extractEmptyLBBackendsEvents([]byte(out))
			if err != nil || len(events) == 0 {
				continue
			}

			for _, event := range events {
				_, _, err := util.RunOVNSbctl("destroy", "controller_event", event.uuid)
				if err != nil {
					// Don't unidle until we are able to remove the controller event
					logrus.Errorf("Unable to remove controller event %s", event.uuid)
					continue
				}
				if serviceName, ok := oc.GetServiceVIPToName(event.vip, event.protocol); ok {
					serviceRef := kapi.ObjectReference{
						Kind:      "Service",
						Namespace: serviceName.Namespace,
						Name:      serviceName.Name,
					}
					logrus.Debugf("Sending a NeedPods event for service %s in namespace %s.", serviceName.Name, serviceName.Namespace)
					recorder.Eventf(&serviceRef, kapi.EventTypeNormal, "NeedPods", "The service %s needs pods", serviceName.Name)
				}
			}
		case <-stopChan:
			return
		}
	}
}

// WatchPods starts the watching of Pod resource and calls back the appropriate handler logic
func (oc *Controller) WatchPods() error {
	_, err := oc.watchFactory.AddPodHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*kapi.Pod)
			if pod.Spec.NodeName != "" {
				logrus.Debugf("CATHY Pod add event for pod %s", pod.Namespace+"/"+pod.Name)
				oc.addPod(pod)
			}
		},
		UpdateFunc: func(old, newer interface{}) {
			podNew := newer.(*kapi.Pod)
			podOld := old.(*kapi.Pod)
			if podOld.Spec.NodeName != podNew.Spec.NodeName {
				logrus.Debugf("CATHY Pod update event for pod %s oldNode %s newNode %s", podNew.Namespace+"/"+podNew.Name,
					podOld.Spec.NodeName, podNew.Spec.NodeName)
				if podOld.Spec.NodeName != "" {
					oc.deletePod(podNew)
				}
				if podNew.Spec.NodeName != "" {
					oc.addPod(podNew)
				}
			}
		},
		DeleteFunc: func(obj interface{}) {
			pod := obj.(*kapi.Pod)
			logrus.Debugf("CATHY Pod delete event for pod %s", pod.Namespace+"/"+pod.Name)
			oc.deletePod(pod)
		},
	}, oc.syncPods)
	return err
}

// WatchServices starts the watching of Service resource and calls back the
// appropriate handler logic
func (oc *Controller) WatchServices() error {
	_, err := oc.watchFactory.AddServiceHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj interface{}) {},
		UpdateFunc: func(old, new interface{}) {},
		DeleteFunc: func(obj interface{}) {
			service := obj.(*kapi.Service)
			oc.deleteService(service)
		},
	}, oc.syncServices)
	return err
}

// WatchEndpoints starts the watching of Endpoint resource and calls back the appropriate handler logic
func (oc *Controller) WatchEndpoints() error {
	_, err := oc.watchFactory.AddEndpointsHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			ep := obj.(*kapi.Endpoints)
			err := oc.AddEndpoints(ep)
			if err != nil {
				logrus.Errorf("Error in adding load balancer: %v", err)
			}
		},
		UpdateFunc: func(old, new interface{}) {
			epNew := new.(*kapi.Endpoints)
			epOld := old.(*kapi.Endpoints)
			if reflect.DeepEqual(epNew.Subsets, epOld.Subsets) {
				return
			}
			if len(epNew.Subsets) == 0 {
				err := oc.deleteEndpoints(epNew)
				if err != nil {
					logrus.Errorf("Error in deleting endpoints - %v", err)
				}
			} else {
				err := oc.AddEndpoints(epNew)
				if err != nil {
					logrus.Errorf("Error in modifying endpoints: %v", err)
				}
			}
		},
		DeleteFunc: func(obj interface{}) {
			ep := obj.(*kapi.Endpoints)
			err := oc.deleteEndpoints(ep)
			if err != nil {
				logrus.Errorf("Error in deleting endpoints - %v", err)
			}
		},
	}, nil)
	return err
}

// WatchNetworkPolicy starts the watching of network policy resource and calls
// back the appropriate handler logic
func (oc *Controller) WatchNetworkPolicy() error {
	_, err := oc.watchFactory.AddPolicyHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			policy := obj.(*kapisnetworking.NetworkPolicy)
			oc.addNetworkPolicy(policy)
			return
		},
		UpdateFunc: func(old, newer interface{}) {
			oldPolicy := old.(*kapisnetworking.NetworkPolicy)
			newPolicy := newer.(*kapisnetworking.NetworkPolicy)
			if !reflect.DeepEqual(oldPolicy, newPolicy) {
				oc.deleteNetworkPolicy(oldPolicy)
				oc.addNetworkPolicy(newPolicy)
			}
			return
		},
		DeleteFunc: func(obj interface{}) {
			policy := obj.(*kapisnetworking.NetworkPolicy)
			oc.deleteNetworkPolicy(policy)
			return
		},
	}, oc.syncNetworkPolicies)
	return err
}

// WatchNamespaces starts the watching of namespace resource and calls
// back the appropriate handler logic
func (oc *Controller) WatchNamespaces() error {
	_, err := oc.watchFactory.AddNamespaceHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			ns := obj.(*kapi.Namespace)
			oc.AddNamespace(ns)
			return
		},
		UpdateFunc: func(old, newer interface{}) {
			oldNs, newNs := old.(*kapi.Namespace), newer.(*kapi.Namespace)
			oc.updateNamespace(oldNs, newNs)
			return
		},
		DeleteFunc: func(obj interface{}) {
			ns := obj.(*kapi.Namespace)
			oc.deleteNamespace(ns)
			return
		},
	}, oc.syncNamespaces)
	return err
}

// WatchNodes starts the watching of node resource and calls
// back the appropriate handler logic
func (oc *Controller) WatchNodes() error {
	gatewaysHandled := make(map[string]bool)
	_, err := oc.watchFactory.AddNodeHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			node := obj.(*kapi.Node)
			logrus.Debugf("Added event for Node %q", node.Name)
			subnets, err := oc.addNode(node)
			if err != nil {
				logrus.Errorf("error creating subnet for node %s: %v", node.Name, err)
			}
			for netName, subnet := range subnets {
				err = oc.syncNodeManagementPort(node, subnet, netName)
				if err != nil {
					logrus.Errorf("error creating Node Management Port for node %s: %v", node.Name, err)
				}
			}
			if !config.Gateway.NodeportEnable {
				return
			}
			gatewaysHandled[node.Name] = oc.handleNodePortLB(node)
		},
		UpdateFunc: func(old, new interface{}) {
			oldNode := old.(*kapi.Node)
			node := new.(*kapi.Node)
			oc.nodeMutex.Lock()
			oc.netMutex.Lock()
			subnets := make(map[string]*net.IPNet)
			for netName := range oc.netAttchmtDefs {
				subnet, err := oc.getHostSubnet(node.Name, netName, false)
				if err != nil {
					logrus.Errorf("failed to get subnet for node %s netName %s: %v", node.Name, netName, err)
					continue
				}
				subnets[netName] = subnet
			}
			oc.netMutex.Unlock()
			oc.nodeMutex.Unlock()

			for netName, subnet := range networks {
				netPrefix := util.GetNetworkPrefix(netName)
				oldMacAddress, _ := oldNode.Annotations[netPrefix+OvnNodeManagementPortMacAddress]
				macAddress, _ := node.Annotations[netPrefix+OvnNodeManagementPortMacAddress]
				if oldMacAddress != macAddress {
					logrus.Debugf("Updated event for Node %q of network %s, old mac %v, new mac %v",
						node.Name, netName, oldMacAddress, macAddress)
					err = oc.syncNodeManagementPort(node, subnet, netName)
					if err != nil {
						logrus.Errorf("error update Node Management Port for node %s: %v", node.Name, err)
					}
				}
			}

			if !reflect.DeepEqual(oldNode.Status.Conditions, node.Status.Conditions) {
				oc.clearInitialNodeNetworkUnavailableCondition(node)
			}

			if !config.Gateway.NodeportEnable {
				return
			}
			if !gatewaysHandled[node.Name] {
				gatewaysHandled[node.Name] = oc.handleNodePortLB(node)
			}
		},
		DeleteFunc: func(obj interface{}) {
			node := obj.(*kapi.Node)
			logrus.Debugf("Delete event for Node %q. Removing the node from "+
				"various caches", node.Name)

			logrus.Debugf("Delete event for Node %q", node.Name)
			err := oc.deleteNode(node.Name)
			if err != nil {
				logrus.Error(err)
			}
			delete(gatewaysHandled, node.Name)
			oc.lsMutex.Lock()
			if oc.defGatewayRouter == "GR_"+node.Name {
				delete(oc.loadbalancerGWCache, TCP)
				delete(oc.loadbalancerGWCache, UDP)
				oc.defGatewayRouter = ""
				oc.handleExternalIPsLB()
			}
			oc.lsMutex.Unlock()
		},
	}, oc.syncNodes)
	return err
}

// AddServiceVIPToName associates a k8s service name with a load balancer VIP
func (oc *Controller) AddServiceVIPToName(vip string, protocol kapi.Protocol, namespace, name string) {
	oc.serviceVIPToNameLock.Lock()
	defer oc.serviceVIPToNameLock.Unlock()
	oc.serviceVIPToName[ServiceVIPKey{vip, protocol}] = types.NamespacedName{namespace, name}
}

// GetServiceVIPToName retrieves the associated k8s service name for a load balancer VIP
func (oc *Controller) GetServiceVIPToName(vip string, protocol kapi.Protocol) (types.NamespacedName, bool) {
	oc.serviceVIPToNameLock.Lock()
	defer oc.serviceVIPToNameLock.Unlock()
	namespace, ok := oc.serviceVIPToName[ServiceVIPKey{vip, protocol}]
	return namespace, ok
}

func (oc *Controller) addNetworkAttachDefinition(netattachdef *knetattachment.NetworkAttachmentDefinition) error {
	var networkAttDef *networkAttachmentDefinitionConfig

	logrus.Debugf("addNetworkAttachDefinition %s", netattachdef.Name)
	netConf := &ovntypes.NetConf{}
	err := json.Unmarshal([]byte(netattachdef.Spec.Config), &netConf)
	if err != nil {
		logrus.Errorf("AddNetworkAttachDefinition: failed to unmarshal Spec.Config of NetworkAttachmentDefinition %s: %v", netattachdef.Name, err)
		return fmt.Errorf("AddNetworkAttachDefinition: failed to unmarshal Spec.Config of NetworkAttachmentDefinition %s: %v", netattachdef.Name, err)
	}
	// Even if this is the NetworkAttachmentDefinition for the default network, add it to the map, so it is easy to
	// look it up when creating a pod
	if !netConf.NotDefault {
		oc.nodeMutex.Lock()
		oc.netMutex.Lock()
		oc.netAttchmtDefs[netattachdef.Name] = &networkAttachmentDefinitionConfig{
			isDefault:                 true,
			cidr:                      null,
			masterSubnetAllocatorList: nil,
			mtu:                       0,
			enableGateway:             false,
			pods:                      nil,
		}
		oc.netMutex.Unlock()
		oc.nodeMutex.Unlock()
		return nil
	}

	// In case name in the json defintion is different from the resource name
	netConf.Name = netattachdef.Name
	networkAttDef, err = oc.SetupMaster(netConf)
	if err != nil {
		logrus.Errorf("AddNetworkAttachDefinition failure: %v", err)
		return err
	}

	subnets := make(map[string]*net.IPNet)
	oc.nodeMutex.Lock()
	oc.netMutex.Lock()
	oc.netAttchmtDefs[netattachdef.Name] = networkAttDef
	oc.initSubnetAllocator(netattachdef.Name)
	for nodeName := range oc.nodeCache {
		subnet, err := oc.getHostSubnet(nodeName, netattachdef.Name, true)
		if err != nil {
			subnets[nodeName] = subnet
		}
	}
	oc.netMutex.Unlock()
	oc.nodeMutex.Unlock()

	for nodeName, subnet := range subnets {
		_ = oc.ensureNodeLogicalNetwork(nodeName, subnet, netattachdef.Name)
	}
	return nil
}

func (oc *Controller) deleteNetworkAttachDefinition(netattachdef *knetattachment.NetworkAttachmentDefinition) {
	logrus.Debugf("deleteNetworkAttachDefinition %s", netattachdef.Name)
	netConf := &ovntypes.NetConf{}
	err := json.Unmarshal([]byte(netattachdef.Spec.Config), &netConf)
	if err != nil {
		logrus.Errorf("deleteNetworkAttachDefinition: failed to unmarshal Spec.Config of NetworkAttachmentDefinition %s: %v", netattachdef.Name, err)
	}

	oc.nodeMutex.Lock()
	oc.netMutex.Lock()
	if len(oc.netAttchmtDefs[netattachdef.Name].pods) != 0 {
		logrus.Errorf("Error: Pods %v still on network %s", oc.netAttchmtDefs[netattachdef.Name].pods, netattachdef.Name)
	}
	delete(oc.netAttchmtDefs, netattachdef.Name)
	nodeNames := oc.nodeCache
	oc.netMutex.Unlock()
	oc.nodeMutex.Unlock()

	// If this is the NetworkAttachmentDefinition for the default network, skip it
	if !netConf.NotDefault {
		return
	}

	oc.deleteMaster(netattachdef.Name)
	for nodeName := range nodeNames {
		if err := oc.deleteNodeLogicalNetwork(nodeName, netattachdef.Name); err != nil {
			logrus.Errorf("Error deleting logical entities for network %s nodeName %s: %v", netattachdef.Name, nodeName, err)
		}

		if err := util.GatewayCleanup(nodeName, netattachdef.Name); err != nil {
			logrus.Errorf("Failed to clean up network %s node %s gateway: (%v)", netattachdef.Name, nodeName, err)
		}
	}
}

// syncNetworkAttachDefinition() delete OVN logical entities of the obsoleted netNames.
func (oc *Controller) syncNetworkAttachDefinition(netattachdefs []interface{}) {

	// Get all the existing non-default netNames
	expectedNetworks := make(map[string]bool)
	for _, netattachdefIntf := range netattachdefs {
		netattachdef, ok := netattachdefIntf.(*knetattachment.NetworkAttachmentDefinition)
		if !ok {
			logrus.Errorf("Spurious object in syncNetworkAttachDefinition: %v", netattachdefIntf)
			continue
		}
		netConf := &ovntypes.NetConf{}
		err := json.Unmarshal([]byte(netattachdef.Spec.Config), &netConf)
		if err != nil {
			logrus.Errorf("Unrecognized Spec.Config of NetworkAttachmentDefinition %s: %v", netattachdef.Name, err)
			continue
		}
		// If this is the NetworkAttachmentDefinition for the default network, skip it
		if !netConf.NotDefault {
			continue
		}
		expectedNetworks[netattachdef.Name] = true
	}

	// Find all the logical node switches for the non-default networks and delete the ones that belong to the
	// obsolete networks
	nodeSwitches, stderr, err := util.RunOVNNbctl("--data=bare", "--no-heading",
		"--columns=name,external_ids", "find", "logical_switch", "external_ids:network_name!=_")
	if err != nil {
		logrus.Errorf("Failed to get logical switches with non-default network: stderr: %q, error: %v", stderr, err)
		return
	}
	for _, result := range strings.Split(nodeSwitches, "\n\n") {
		items := strings.Split(result, "\n")
		if len(items) < 2 || len(items[0]) == 0 {
			continue
		}

		netName := util.GetDbValByKey(items[1], "network_name")
		if _, ok := expectedNetworks[netName]; ok {
			// network still exists, no cleanup to do
			continue
		}

		// items[0] is the switch name, which should be prefixed with netName
		if netName == "" || !strings.HasPrefix(items[0], netName) {
			logrus.Warningf("CATHYZ syncNetworkAttachDefinition Unexpected logical switch %s: (%v)", items[0], items)
			continue
		}

		nodeName := strings.TrimPrefix(items[0], netName+"_")
		if nodeName == OvnJoinSwitch {
			// This is the join switch for this network, skip, it will be deleted later below
			continue
		}

		if err := oc.deleteNodeLogicalNetwork(nodeName, netName); err != nil {
			logrus.Errorf("Error deleting node %s logical network: %v", nodeName, err)
		}

		if err = util.GatewayCleanup(nodeName, netName); err != nil {
			logrus.Errorf("Failed to clean up node %s gateway: (%v)", nodeName, err)
		}
	}
	clusterRouters, stderr, err := util.RunOVNNbctl("--data=bare", "--no-heading",
		"--columns=name,external_ids", "find", "logical_router", "external_ids:network_name!=_")
	if err != nil {
		logrus.Errorf("Failed to get logical routers with non-default network name: stderr: %q, error: %v",
			stderr, err)
		return
	}
	for _, result := range strings.Split(clusterRouters, "\n\n") {
		items := strings.Split(result, "\n")
		if len(items) < 2 || len(items[0]) == 0 {
			continue
		}

		netName := util.GetDbValByKey(items[1], "network_name")
		if _, ok := expectedNetworks[netName]; ok {
			// network still exists, no cleanup to do
			continue
		}

		// items[0] is the router name, which should be prefixed with netName
		if netName == "" || !strings.HasPrefix(items[0], netName) {
			logrus.Warningf("CATHYZ syncNetworkAttachDefinition Unexpected logical router %s: %v", items[0], result)
			continue
		}

		oc.deleteMaster(netName)
	}
}

func (oc *Controller) watchNetworkAttachmentDefinition() error {
	var err error

	_, err = oc.watchFactory.AddNetworkAttachmentDefinitionHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			netattachdef := obj.(*knetattachment.NetworkAttachmentDefinition)
			logrus.Debugf("netattachdef add event for %q spec %q", netattachdef.Name, netattachdef.Spec.Config)
			err = oc.addNetworkAttachDefinition(netattachdef)
			if err != nil {
				logrus.Errorf("error adding new NetworkAttachmentDefintition %s: %v", netattachdef.Name, err)
			}
		},
		UpdateFunc: func(old, new interface{}) {},
		DeleteFunc: func(obj interface{}) {
			netattachdef := obj.(*knetattachment.NetworkAttachmentDefinition)
			logrus.Debugf("netattachdef delete event for for netattachdef %q", netattachdef.Name, netattachdef.Spec.Config)
			oc.deleteNetworkAttachDefinition(netattachdef)
		},
	}, oc.syncNetworkAttachDefinition)
	return err
}
