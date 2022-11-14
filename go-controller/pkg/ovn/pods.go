package ovn

import (
	"fmt"
	"net"
	"sync/atomic"
	"time"

	networkattachmentdefinitionapi "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/metrics"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/ipallocator"
	lsm "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/logical_switch_manager"
	ovntypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	util "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	"github.com/pkg/errors"
	kapi "k8s.io/api/core/v1"
	ktypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
	utilnet "k8s.io/utils/net"

	libovsdbclient "github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/libovsdb/ovsdb"
	libovsdbops "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdbops"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
)

func (nci *NetworkControllerInfo) updateExpectedLogicalPorts(pod *kapi.Pod, lsManager *lsm.LogicalSwitchManager,
	annotations *util.PodAnnotation, nadName string, expectedLogicalPorts map[string]bool) error {
	var err error
	switchName := nci.Prefix + pod.Spec.NodeName
	if util.PodScheduled(pod) && util.PodWantsNetwork(pod) && !util.PodCompleted(pod) {
		// skip nodes that are not running ovnk (inferred from host subnets)
		if lsManager.IsNonHostSubnetSwitch(switchName) {
			return nil
		}
		logicalPort := util.GetLogicalPortName(pod.Namespace, pod.Name)
		if !nci.IsSecondary {
			logicalPort = util.GetSecondaryNetworkLogicalPortName(pod.Namespace, pod.Name, nadName)
		}
		expectedLogicalPorts[logicalPort] = true
		if err := waitForNodeLogicalSwitchInCache(lsManager, switchName); err != nil {
			return fmt.Errorf("failed to wait for switch %s to be added to cache. IP allocation may fail!",
				switchName)
		}
		if err = lsManager.AllocateIPs(switchName, annotations.IPs); err != nil {
			if err == ipallocator.ErrAllocated {
				// already allocated: log an error but not stop syncPod from continuing
				klog.Errorf("Already allocated IPs: %s for pod: %s on switchName: %s",
					util.JoinIPNetIPs(annotations.IPs, " "), logicalPort,
					switchName)
			} else {
				return fmt.Errorf("couldn't allocate IPs: %s for pod: %s on switch: %s"+
					" error: %v", util.JoinIPNetIPs(annotations.IPs, " "), logicalPort,
					switchName, err)
			}
		}
	}
	return nil
}

func (oc *DefaultNetworkController) syncPods(pods []interface{}) error {
	// get the list of logical switch ports (equivalent to pods). Reserve all existing Pod IPs to
	// avoid subsequent new Pods getting the same duplicate Pod IP.
	//
	// TBD: Before this succeeds, add Pod handler should not continue to allocate IPs for the new Pods.
	expectedLogicalPorts := make(map[string]bool)
	for _, podInterface := range pods {
		pod, ok := podInterface.(*kapi.Pod)
		if !ok {
			return fmt.Errorf("spurious object in syncPods: %v", podInterface)
		}
		annotations, err := util.UnmarshalPodAnnotation(pod.Annotations, ovntypes.DefaultNetworkName)
		if err != nil {
			continue
		}
		err = oc.updateExpectedLogicalPorts(pod, oc.lsManager, annotations,
			ovntypes.DefaultNetworkName, expectedLogicalPorts)
		if err != nil {
			return err
		}

		// delete the outdated hybrid overlay subnet route if it exists
		newRoutes := []util.PodRoute{}
		switchName := pod.Spec.NodeName
		for _, subnet := range oc.lsManager.GetSwitchSubnets(switchName) {
			hybridOverlayIFAddr := util.GetNodeHybridOverlayIfAddr(subnet).IP
			for _, route := range annotations.Routes {
				if !route.NextHop.Equal(hybridOverlayIFAddr) {
					newRoutes = append(newRoutes, route)
				}
			}
		}
		// checking the length because cannot compare the slices directly and if routes are removed
		// the length will be different
		if len(annotations.Routes) != len(newRoutes) {
			annotations.Routes = newRoutes
			err = oc.updatePodAnnotationWithRetry(pod, annotations, ovntypes.DefaultNetworkName)
			if err != nil {
				return fmt.Errorf("failed to set annotation on pod %s: %v", pod.Name, err)
			}
		}
	}
	// all pods present before ovn-kube startup have been processed
	atomic.StoreUint32(&oc.allInitialPodsProcessed, 1)

	return oc.deleteStaleLogicalSwitchPorts(oc.lsManager, expectedLogicalPorts)
}

func (nci *NetworkControllerInfo) deleteStaleLogicalSwitchPorts(lsManager *lsm.LogicalSwitchManager,
	expectedLogicalPorts map[string]bool) error {
	// get all the nodes from the watchFactory
	nodes, err := nci.watchFactory.GetNodes()
	if err != nil {
		return fmt.Errorf("failed to get nodes: %v", err)
	}

	var ops []ovsdb.Operation
	for _, n := range nodes {
		// skip nodes that are not running ovnk (inferred from host subnets)
		switchName := nci.Prefix + n.Name
		if lsManager.IsNonHostSubnetSwitch(switchName) {
			continue
		}
		p := func(item *nbdb.LogicalSwitchPort) bool {
			return item.ExternalIDs["pod"] == "true" && !expectedLogicalPorts[item.Name]
		}
		sw := nbdb.LogicalSwitch{
			Name: switchName,
		}
		sw.UUID, _ = lsManager.GetUUID(switchName)

		ops, err = libovsdbops.DeleteLogicalSwitchPortsWithPredicateOps(nci.nbClient, ops, &sw, p)
		if err != nil {
			return fmt.Errorf("could not generate ops to delete stale ports from logical switch %s (%+v)", switchName, err)
		}
	}

	_, err = libovsdbops.TransactAndCheck(nci.nbClient, ops)
	if err != nil {
		return fmt.Errorf("could not remove stale logicalPorts from switches for network %s (%+v)", nci.NetName, err)
	}
	return nil
}

// lookupPortUUIDAndSwitchName will use libovsdb to locate the logical switch port uuid as well as the logical switch
// that owns such port (aka nodeName), based on the logical port name.
func (bnc *BaseNetworkController) lookupPortUUIDAndSwitchName(logicalPort string) (portUUID string, logicalSwitch string, err error) {
	lsp := &nbdb.LogicalSwitchPort{Name: logicalPort}
	lsp, err = libovsdbops.GetLogicalSwitchPort(bnc.nbClient, lsp)
	if err != nil {
		return "", "", err
	}
	p := func(item *nbdb.LogicalSwitch) bool {
		for _, currPortUUID := range item.Ports {
			if currPortUUID == lsp.UUID {
				return true
			}
		}
		return false
	}
	nodeSwitches, err := libovsdbops.FindLogicalSwitchesWithPredicate(bnc.nbClient, p)
	if err != nil {
		return "", "", fmt.Errorf("failed to get node logical switch for logical port %s (%s): %w", logicalPort, lsp.UUID, err)
	}
	if len(nodeSwitches) != 1 {
		return "", "", fmt.Errorf("found %d node logical switch for logical port %s (%s)", len(nodeSwitches), logicalPort, lsp.UUID)
	}
	return lsp.UUID, nodeSwitches[0].Name, nil
}

func (nci *NetworkControllerInfo) deletePodLogicalPort(pod *kapi.Pod, portInfo *lpInfo, nadName string,
	nsm *namespaceManager, lsManager *lsm.LogicalSwitchManager, multicastSupport bool) (*lpInfo, error) {
	var portUUID, switchName string
	var podIfAddrs []*net.IPNet
	var err error

	podDesc := fmt.Sprintf("pod %s/%s/%s", nadName, pod.Namespace, pod.Name)
	logicalPort := util.GetLogicalPortName(pod.Namespace, pod.Name)
	if nci.IsSecondary {
		logicalPort = util.GetSecondaryNetworkLogicalPortName(pod.Namespace, pod.Name, nadName)
	}
	if portInfo == nil {
		// If ovnkube-master restarts, it is also possible the Pod's logical switch port
		// is not re-added into the cache. Delete logical switch port anyway.
		annotation, err := util.UnmarshalPodAnnotation(pod.Annotations, nadName)
		if err != nil {
			if util.IsAnnotationNotSetError(err) {
				// if the annotation doesn’t exist, that’s not an error. It means logical port does not need to be deleted.
				klog.V(5).Infof("No annotations on %s, no need to delete its logical port: %s", podDesc, logicalPort)
				return nil, nil
			}
			return nil, fmt.Errorf("unable to unmarshal pod annotations for %s: %w", podDesc, err)
		}

		// Since portInfo is not available, use ovn to locate the logical switch (named after the node name) for the logical port.
		portUUID, switchName, err = nci.lookupPortUUIDAndSwitchName(logicalPort)
		if err != nil {
			if err != libovsdbclient.ErrNotFound {
				return nil, fmt.Errorf("unable to locate portUUID+switchName for %s: %w", podDesc, err)
			}
			// The logical port no longer exists in OVN. The caller expects this function to be idem-potent,
			// so the proper action to take is to use an empty uuid and extract the node name from the pod spec.
			portUUID = ""
			switchName = nci.Prefix + pod.Spec.NodeName
		}
		podIfAddrs = annotation.IPs

		klog.Warningf("No cached port info for deleting %s. Using logical switch %s port uuid %s and addrs %v",
			podDesc, switchName, portUUID, podIfAddrs)
	} else {
		portUUID = portInfo.uuid
		switchName = portInfo.logicalSwitch
		podIfAddrs = portInfo.ips
	}

	// Sanity check. The logical switch obtained from the port is expected to be derived from the nodeName in the pod spec.
	if switchName != nci.Prefix+pod.Spec.NodeName {
		klog.Errorf("Deleting %s expecting switch name: %s, OVN DB has switch name %s for port uuid %s",
			podDesc, nci.Prefix+pod.Spec.NodeName, switchName, portUUID)
	}

	shouldRelease := true
	// check to make sure no other pods are using this IP before we try to release it if this is a completed pod.
	if util.PodCompleted(pod) {
		if shouldRelease, err = lsManager.ConditionalIPRelease(switchName, podIfAddrs, func() (bool, error) {
			pods, err := nci.watchFactory.GetAllPods()
			if err != nil {
				return false, fmt.Errorf("unable to get pods to determine if completed pod IP is in use by another pod. "+
					"Will not release pod %s/%s IP: %#v from allocator", pod.Namespace, pod.Name, podIfAddrs)
			}
			// iterate through all pods, ignore pods on other switches
			for _, p := range pods {
				if util.PodCompleted(p) || !util.PodWantsNetwork(p) || !util.PodScheduled(p) || nci.Prefix+p.Spec.NodeName != switchName {
					continue
				}
				// check if the pod addresses match in the OVN annotation
				pAddrs, err := util.GetAllPodIPs(p, &nci.NetInfo)
				if err != nil {
					continue
				}

				for _, pAddr := range pAddrs {
					for _, podAddr := range podIfAddrs {
						if pAddr.Equal(podAddr.IP) {
							klog.Infof("Will not release IP address: %s for %s. Detected another pod"+
								" using this IP: %s/%s", pAddr.String(), podDesc, p.Namespace, p.Name)
							return false, nil
						}
					}
				}
			}
			klog.Infof("Releasing IPs for Completed pod: %s/%s, ips: %s", pod.Namespace, pod.Name,
				util.JoinIPNetIPs(podIfAddrs, " "))
			return true, nil
		}); err != nil {
			return nil, fmt.Errorf("cannot determine if IPs are safe to release for completed pod: %s: %w", podDesc, err)
		}
	}

	var allOps, ops []ovsdb.Operation

	// if the ip is in use by another pod we should not try to remove it from the address set
	if shouldRelease {
		if ops, err = nci.deletePodFromNamespace(nsm, pod.Namespace,
			podIfAddrs, portUUID, multicastSupport); err != nil {
			return nil, fmt.Errorf("unable to delete pod %s from namespace: %w", podDesc, err)
		}
		allOps = append(allOps, ops...)
	}
	ops, err = nci.delLSPOps(lsManager, logicalPort, switchName, portUUID)
	// Tolerate cases where logical switch of the logical port no longer exist in OVN.
	if err != nil && !errors.Is(err, libovsdbclient.ErrNotFound) {
		return nil, fmt.Errorf("failed to create delete ops for the lsp: %s: %s", logicalPort, err)
	}
	allOps = append(allOps, ops...)

	// TBD for secondary network
	var txOkCallBack func()
	txOkCallBack = nil
	if !nci.IsSecondary {
		var recordOps []ovsdb.Operation
		recordOps, txOkCallBack, _, err = metrics.GetConfigDurationRecorder().AddOVN(nci.nbClient, "pod", pod.Namespace,
			pod.Name)
		if err != nil {
			klog.Errorf("Failed to record config duration: %v", err)
		}
		allOps = append(allOps, recordOps...)
	}

	_, err = libovsdbops.TransactAndCheck(nci.nbClient, allOps)
	if err != nil {
		return nil, fmt.Errorf("cannot delete logical switch port %s, %v", logicalPort, err)
	}
	if txOkCallBack != nil {
		txOkCallBack()
	}

	// do not remove SNATs/GW routes/IPAM for an IP address unless we have validated no other pod is using it
	if !shouldRelease {
		return nil, nil
	}

	pInfo := lpInfo{
		name:          logicalPort,
		uuid:          portUUID,
		logicalSwitch: switchName,
		ips:           podIfAddrs,
	}
	return &pInfo, nil
}

func (oc *DefaultNetworkController) deleteLogicalPort(pod *kapi.Pod, portInfo *lpInfo) (err error) {
	podDesc := pod.Namespace + "/" + pod.Name
	klog.Infof("Deleting pod: %s", podDesc)

	if err = oc.deletePodExternalGW(pod); err != nil {
		return fmt.Errorf("unable to delete external gateway routes for pod %s: %w", podDesc, err)
	}
	if pod.Spec.HostNetwork {
		return nil
	}
	if !util.PodScheduled(pod) {
		return nil
	}

	pInfo, err := oc.deletePodLogicalPort(pod, portInfo, ovntypes.DefaultNetworkName,
		&oc.namespaceManager, oc.lsManager, oc.multicastSupport)
	if err != nil {
		return err
	}

	// do not remove SNATs/GW routes/IPAM for an IP address unless we have validated no other pod is using it
	if pInfo == nil {
		return nil
	}

	if config.Gateway.DisableSNATMultipleGWs {
		if err := deletePodSNAT(oc.nbClient, pInfo.logicalSwitch, []*net.IPNet{}, pInfo.ips); err != nil {
			return fmt.Errorf("cannot delete GR SNAT for pod %s: %w", podDesc, err)
		}
	}
	podNsName := ktypes.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
	if err := oc.deleteGWRoutesForPod(podNsName, pInfo.ips); err != nil {
		return fmt.Errorf("cannot delete GW Routes for pod %s: %w", podDesc, err)
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

func waitForNodeLogicalSwitch(lsManager *lsm.LogicalSwitchManager, switchName string) (*nbdb.LogicalSwitch, error) {
	// Wait for the node logical switch to be created by the ClusterController and be present
	// in libovsdb's cache. The node switch will be created when the node's logical network infrastructure
	// is created by the node watch
	ls := &nbdb.LogicalSwitch{Name: switchName}
	if err := wait.PollImmediate(30*time.Millisecond, 30*time.Second, func() (bool, error) {
		if lsUUID, ok := lsManager.GetUUID(switchName); !ok {
			return false, fmt.Errorf("error getting logical switch %s: %s", switchName, "switch not in logical switch cache")
		} else {
			ls.UUID = lsUUID
			return true, nil
		}
	}); err != nil {
		return nil, fmt.Errorf("timed out waiting for logical switch in logical switch cache %q subnet: %v", switchName, err)
	}
	return ls, nil
}

func waitForNodeLogicalSwitchInCache(lsManager *lsm.LogicalSwitchManager, switchName string) error {
	// Wait for the node logical switch to be created by the ClusterController.
	// The node switch will be created when the node's logical network infrastructure
	// is created by the node watch.
	var subnets []*net.IPNet
	if err := wait.PollImmediate(30*time.Millisecond, 30*time.Second, func() (bool, error) {
		subnets = lsManager.GetSwitchSubnets(switchName)
		return subnets != nil, nil
	}); err != nil {
		return fmt.Errorf("timed out waiting for logical switch %q subnet: %v", switchName, err)
	}
	return nil
}

func (nci *NetworkControllerInfo) addRoutesGatewayIP(pod *kapi.Pod, network *networkattachmentdefinitionapi.NetworkSelectionElement,
	clusterSubnets []config.CIDRNetworkEntry, podAnnotation *util.PodAnnotation, nodeSubnets []*net.IPNet) error {

	if nci.IsSecondary {
		// non default network, see if its network-attachment's annotation has default-route key.
		// If present, then we need to add default route for it
		podAnnotation.Gateways = append(podAnnotation.Gateways, network.GatewayRequest...)
		for _, podIfAddr := range podAnnotation.IPs {
			isIPv6 := utilnet.IsIPv6CIDR(podIfAddr)
			nodeSubnet, err := util.MatchIPNetFamily(isIPv6, nodeSubnets)
			if err != nil {
				return err
			}
			gatewayIPnet := util.GetNodeGatewayIfAddr(nodeSubnet)
			for _, clusterSubnet := range clusterSubnets {
				if isIPv6 == utilnet.IsIPv6CIDR(clusterSubnet.CIDR) {
					podAnnotation.Routes = append(podAnnotation.Routes, util.PodRoute{
						Dest:    clusterSubnet.CIDR,
						NextHop: gatewayIPnet.IP,
					})
				}
			}
		}
		return nil
	}

	// if there are other network attachments for the pod, then check if those network-attachment's
	// annotation has default-route key. If present, then we need to skip adding default route for
	// OVN interface
	networks, err := util.GetK8sPodAllNetworks(pod)
	if err != nil {
		return fmt.Errorf("error while getting network attachment definition for [%s/%s]: %v",
			pod.Namespace, pod.Name, err)
	}
	otherDefaultRouteV4 := false
	otherDefaultRouteV6 := false
	for _, network := range networks {
		for _, gatewayRequest := range network.GatewayRequest {
			if utilnet.IsIPv6(gatewayRequest) {
				otherDefaultRouteV6 = true
			} else {
				otherDefaultRouteV4 = true
			}
		}
	}

	for _, podIfAddr := range podAnnotation.IPs {
		isIPv6 := utilnet.IsIPv6CIDR(podIfAddr)
		nodeSubnet, err := util.MatchIPNetFamily(isIPv6, nodeSubnets)
		if err != nil {
			return err
		}

		gatewayIPnet := util.GetNodeGatewayIfAddr(nodeSubnet)

		otherDefaultRoute := otherDefaultRouteV4
		if isIPv6 {
			otherDefaultRoute = otherDefaultRouteV6
		}
		var gatewayIP net.IP
		if otherDefaultRoute {
			for _, clusterSubnet := range clusterSubnets {
				if isIPv6 == utilnet.IsIPv6CIDR(clusterSubnet.CIDR) {
					podAnnotation.Routes = append(podAnnotation.Routes, util.PodRoute{
						Dest:    clusterSubnet.CIDR,
						NextHop: gatewayIPnet.IP,
					})
				}
			}
			for _, serviceSubnet := range config.Kubernetes.ServiceCIDRs {
				if isIPv6 == utilnet.IsIPv6CIDR(serviceSubnet) {
					podAnnotation.Routes = append(podAnnotation.Routes, util.PodRoute{
						Dest:    serviceSubnet,
						NextHop: gatewayIPnet.IP,
					})
				}
			}
		} else {
			gatewayIP = gatewayIPnet.IP
		}

		if gatewayIP != nil {
			podAnnotation.Gateways = append(podAnnotation.Gateways, gatewayIP)
		}
	}
	return nil
}

// podExpectedInLogicalCache returns true if pod should be added to oc.logicalPortCache.
// For some pods, like hostNetwork pods, overlay node pods, or completed pods waiting for them to be added
// to oc.logicalPortCache will never succeed.
func (oc *DefaultNetworkController) podExpectedInLogicalCache(pod *kapi.Pod) bool {
	return util.PodWantsNetwork(pod) && !oc.lsManager.IsNonHostSubnetSwitch(pod.Spec.NodeName) && !util.PodCompleted(pod)
}

func (nci *NetworkControllerInfo) addPodLogicalPort(pod *kapi.Pod, lsManager *lsm.LogicalSwitchManager,
	clusterSubnets []config.CIDRNetworkEntry, nadName string,
	network *networkattachmentdefinitionapi.NetworkSelectionElement) (ops []ovsdb.Operation,
	lsp *nbdb.LogicalSwitchPort, podAnnotation *util.PodAnnotation, newlyCreatedPort bool, err error) {
	var ls *nbdb.LogicalSwitch
	podDesc := fmt.Sprintf("%s/%s/%s", nadName, pod.Namespace, pod.Name)
	switchName := nci.Prefix + pod.Spec.NodeName
	ls, err = waitForNodeLogicalSwitch(lsManager, switchName)
	if err != nil {
		return nil, nil, nil, false, err
	}

	portName := util.GetLogicalPortName(pod.Namespace, pod.Name)
	if nci.IsSecondary {
		portName = util.GetSecondaryNetworkLogicalPortName(pod.Namespace, pod.Name, nadName)
	}
	klog.Infof("[%s] creating logical port %s for pod on switch %s", podDesc, portName, switchName)

	var podMac net.HardwareAddr
	var podIfAddrs []*net.IPNet
	var addresses []string
	var releaseIPs bool
	lspExist := false
	needsIP := true

	// Check if the pod's logical switch port already exists. If it
	// does don't re-add the port to OVN as this will change its
	// UUID and and the port cache, address sets, and port groups
	// will still have the old UUID.
	lsp = &nbdb.LogicalSwitchPort{Name: portName}
	existingLSP, err := libovsdbops.GetLogicalSwitchPort(nci.nbClient, lsp)
	if err != nil && err != libovsdbclient.ErrNotFound {
		return nil, nil, nil, false,
			fmt.Errorf("unable to get the lsp %s from the nbdb: %s", portName, err)
	}
	lspExist = err != libovsdbclient.ErrNotFound

	// Sanity check. If port exists, it should be in the logical switch obtained from the pod spec.
	if lspExist {
		portFound := false
		ls, err = libovsdbops.GetLogicalSwitch(nci.nbClient, ls)
		if err != nil {
			return nil, nil, nil, false,
				fmt.Errorf("[%s] unable to find logical switch %s in NBDB", podDesc, switchName)
		}
		for _, currPortUUID := range ls.Ports {
			if currPortUUID == existingLSP.UUID {
				portFound = true
				break
			}
		}
		if !portFound {
			// This should never happen and indicates we failed to clean up an LSP for a pod that was recreated
			return nil, nil, nil, false, fmt.Errorf("[%s] failed to locate existing logical port %s (%s) in logical switch %s",
				podDesc, existingLSP.Name, existingLSP.UUID, switchName)
		}
	}

	lsp.Options = make(map[string]string)
	// Unique identifier to distinguish interfaces for recreated pods, also set by ovnkube-node
	// ovn-controller will claim the OVS interface only if external_ids:iface-id
	// matches with the Port_Binding.logical_port and external_ids:iface-id-ver matches
	// with the Port_Binding.options:iface-id-ver. This is not mandatory.
	// If Port_binding.options:iface-id-ver is not set, then OVS
	// Interface.external_ids:iface-id-ver if set is ignored.
	// Don't set iface-id-ver for already existing LSP if it wasn't set before,
	// because the corresponding OVS port may not have it set
	// (then ovn-controller won't bind the interface).
	// May happen on upgrade, because ovnkube-node doesn't update
	// existing OVS interfaces with new iface-id-ver option.
	if !lspExist || len(existingLSP.Options["iface-id-ver"]) != 0 {
		lsp.Options["iface-id-ver"] = string(pod.UID)
	}
	// Bind the port to the node's chassis; prevents ping-ponging between
	// chassis if ovnkube-node isn't running correctly and hasn't cleared
	// out iface-id for an old instance of this pod, and the pod got
	// rescheduled.
	lsp.Options["requested-chassis"] = pod.Spec.NodeName

	podAnnotation, err = util.UnmarshalPodAnnotation(pod.Annotations, nadName)

	// the IPs we allocate in this function need to be released back to the
	// IPAM pool if there is some error in any step of addLogicalPort past
	// the point the IPs were assigned via the IPAM manager.
	// this needs to be done only when releaseIPs is set to true (the case where
	// we truly have assigned podIPs in this call) AND when there is no error in
	// the rest of the functionality of addLogicalPort. It is important to use a
	// named return variable for defer to work correctly.

	defer func() {
		if releaseIPs && err != nil {
			if relErr := lsManager.ReleaseIPs(switchName, podIfAddrs); relErr != nil {
				klog.Errorf("Error when releasing IPs %s for switch: %s, err: %q",
					util.JoinIPNetIPs(podIfAddrs, " "), switchName, relErr)
			} else {
				klog.Infof("Released IPs: %s for node: %s", util.JoinIPNetIPs(podIfAddrs, " "), switchName)
			}
		}
	}()

	if err == nil {
		podMac = podAnnotation.MAC
		podIfAddrs = podAnnotation.IPs

		// If the pod already has annotations use the existing static
		// IP/MAC from the annotation.
		lsp.DynamicAddresses = nil

		// ensure we have reserved the IPs in the annotation
		if err = lsManager.AllocateIPs(switchName, podIfAddrs); err != nil && err != ipallocator.ErrAllocated {
			return nil, nil, nil, false,
				fmt.Errorf("unable to ensure IPs allocated for already annotated pod: %s, IPs: %s, error: %v",
					podDesc, util.JoinIPNetIPs(podIfAddrs, " "), err)
		} else {
			needsIP = false
		}
	}

	if needsIP {
		if existingLSP != nil {
			// try to get the MAC and IPs from existing OVN port first
			podMac, podIfAddrs, err = getPortAddresses(lsManager, switchName, existingLSP)
			if err != nil {
				return nil, nil, nil, false,
					fmt.Errorf("failed to get pod addresses for pod %s on node: %s, err: %v",
						podDesc, switchName, err)
			}
		}
		needsNewAllocation := false

		// ensure we have reserved the IPs found in OVN
		if len(podIfAddrs) == 0 {
			needsNewAllocation = true
		} else if err = lsManager.AllocateIPs(switchName, podIfAddrs); err != nil && err != ipallocator.ErrAllocated {
			klog.Warningf("Unable to allocate IPs %s found on existing OVN port: %s, for pod %s on switch: %s"+
				" error: %v", util.JoinIPNetIPs(podIfAddrs, " "), portName, podDesc, switchName, err)

			needsNewAllocation = true
		}
		if needsNewAllocation {
			// Previous attempts to use already configured IPs failed, need to assign new
			podMac, podIfAddrs, err = assignPodAddresses(lsManager, switchName)
			if err != nil {
				return nil, nil, nil, false, fmt.Errorf("failed to assign pod addresses for pod %s on switch: %s, err: %v",
					podDesc, switchName, err)
			}
		}

		releaseIPs = true
		// handle error cases separately first to ensure binding to err, otherwise the
		// defer will fail
		if network != nil && network.MacRequest != "" {
			klog.V(5).Infof("Pod %s requested custom MAC: %s", podDesc, network.MacRequest)
			podMac, err = net.ParseMAC(network.MacRequest)
			if err != nil {
				return nil, nil, nil, false, fmt.Errorf("failed to parse mac %s requested in annotation for pod %s: Error %v",
					network.MacRequest, podDesc, err)
			}
		}
		podAnnotation = &util.PodAnnotation{
			IPs: podIfAddrs,
			MAC: podMac,
		}
		var nodeSubnets []*net.IPNet
		if nodeSubnets = lsManager.GetSwitchSubnets(switchName); nodeSubnets == nil {
			return nil, nil, nil, false,
				fmt.Errorf("cannot retrieve subnet for assigning gateway routes for pod %s, switch: %s",
					podDesc, switchName)
		}
		err = nci.addRoutesGatewayIP(pod, network, clusterSubnets, podAnnotation, nodeSubnets)
		if err != nil {
			return nil, nil, nil, false, err
		}

		klog.V(5).Infof("Annotation values: ip=%v ; mac=%s ; gw=%s",
			podIfAddrs, podMac, podAnnotation.Gateways)
		annoStart := time.Now()
		err = nci.updatePodAnnotationWithRetry(pod, podAnnotation, nadName)
		podAnnoTime := time.Since(annoStart)
		klog.Infof("[%s] addLogicalPort annotation time took %v", podDesc, podAnnoTime)
		if err != nil {
			return nil, nil, nil, false, err
		}
		releaseIPs = false
	}

	// set addresses on the port
	// LSP addresses in OVN are a single space-separated value
	addresses = []string{podMac.String()}
	for _, podIfAddr := range podIfAddrs {
		addresses[0] = addresses[0] + " " + podIfAddr.IP.String()
	}

	lsp.Addresses = addresses

	// add external ids
	lsp.ExternalIDs = map[string]string{"namespace": pod.Namespace, "pod": "true"}
	if nci.IsSecondary {
		lsp.ExternalIDs[ovntypes.NetworkNameExternalID] = nci.NetName
		lsp.ExternalIDs[ovntypes.NadNameExternalID] = nadName
	}

	// CNI depends on the flows from port security, delay setting it until end
	lsp.PortSecurity = addresses

	ops, err = libovsdbops.CreateOrUpdateLogicalSwitchPortsOnSwitchOps(nci.nbClient, nil, ls, lsp)
	if err != nil {
		return nil, nil, nil, false,
			fmt.Errorf("error creating logical switch port %+v on switch %+v: %+v", *lsp, *ls, err)
	}

	return ops, lsp, podAnnotation, needsIP && !lspExist, nil
}

func (oc *DefaultNetworkController) addLogicalPort(pod *kapi.Pod) (err error) {
	switchName := pod.Spec.NodeName

	// If a node does node have an assigned hostsubnet don't wait for the logical switch to appear
	if oc.lsManager.IsNonHostSubnetSwitch(switchName) {
		return nil
	}

	_, network, err := util.IsNetworkOnPod(pod, &oc.NetInfo)
	if err != nil {
		return fmt.Errorf("error getting default-network's network-attachment: %v", err)
	}

	var libovsdbExecuteTime time.Duration
	var lsp *nbdb.LogicalSwitchPort
	var ops []ovsdb.Operation
	var podAnnotation *util.PodAnnotation
	var newlyCreatedPort bool
	// Keep track of how long syncs take.
	start := time.Now()
	defer func() {
		klog.Infof("[%s/%s] addLogicalPort took %v, libovsdb time %v: %v",
			pod.Namespace, pod.Name, time.Since(start), libovsdbExecuteTime)
	}()

	ops, lsp, podAnnotation, newlyCreatedPort, err = oc.addPodLogicalPort(pod, oc.lsManager,
		config.Default.ClusterSubnets, ovntypes.DefaultNetworkName, network)
	if err != nil {
		return err
	}

	// Ensure the namespace/nsInfo exists
	routingExternalGWs, routingPodGWs, addOps, err := oc.addPodToNamespace(pod.Namespace, podAnnotation.IPs)
	if err != nil {
		return err
	}
	ops = append(ops, addOps...)

	// if we have any external or pod Gateways, add routes
	gateways := make([]*gatewayInfo, 0, len(routingExternalGWs.gws)+len(routingPodGWs))

	if len(routingExternalGWs.gws) > 0 {
		gateways = append(gateways, routingExternalGWs)
	}
	for key := range routingPodGWs {
		gw := routingPodGWs[key]
		if len(gw.gws) > 0 {
			if err = validateRoutingPodGWs(routingPodGWs); err != nil {
				klog.Error(err)
			}
			gateways = append(gateways, &gw)
		} else {
			klog.Warningf("Found routingPodGW with no gateways ip set for namespace %s", pod.Namespace)
		}
	}

	if len(gateways) > 0 {
		podNsName := ktypes.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
		err = oc.addGWRoutesForPod(gateways, podAnnotation.IPs, podNsName, pod.Spec.NodeName)
		if err != nil {
			return err
		}
	} else if config.Gateway.DisableSNATMultipleGWs {
		// Add NAT rules to pods if disable SNAT is set and does not have
		// namespace annotations to go through external egress router
		if extIPs, err := getExternalIPsGR(oc.watchFactory, pod.Spec.NodeName); err != nil {
			return err
		} else if ops, err = oc.addOrUpdatePodSNATReturnOps(pod.Spec.NodeName, extIPs, podAnnotation.IPs, ops); err != nil {
			return err
		}
	}

	recordOps, txOkCallBack, _, err := metrics.GetConfigDurationRecorder().AddOVN(oc.nbClient, "pod", pod.Namespace,
		pod.Name)
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
	oc.podRecorder.AddLSP(pod.UID)

	// check if this pod is serving as an external GW
	err = oc.addPodExternalGW(pod)
	if err != nil {
		return fmt.Errorf("failed to handle external GW check: %v", err)
	}

	// if somehow lspUUID is empty, there is a bug here with interpreting OVSDB results
	if len(lsp.UUID) == 0 {
		return fmt.Errorf("UUID is empty from LSP: %+v", *lsp)
	}

	// Add the pod's logical switch port to the port cache
	portInfo := oc.logicalPortCache.add(switchName, lsp.Name, lsp.UUID, podAnnotation.MAC, podAnnotation.IPs)

	// If multicast is allowed and enabled for the namespace, add the port to the allow policy.
	// FIXME: there's a race here with the Namespace multicastUpdateNamespace() handler, but
	// it's rare and easily worked around for now.
	ns, err := oc.watchFactory.GetNamespace(pod.Namespace)
	if err != nil {
		return err
	}
	if oc.multicastSupport && isNamespaceMulticastEnabled(ns.Annotations) {
		if err := podAddAllowMulticastPolicy(oc.nbClient, pod.Namespace, portInfo); err != nil {
			return err
		}
	}
	//observe the pod creation latency metric for newly created pods only
	if newlyCreatedPort {
		metrics.RecordPodCreated(pod)
	}
	return nil
}

func (bnc *BaseNetworkController) updatePodAnnotationWithRetry(origPod *kapi.Pod, podInfo *util.PodAnnotation, nadName string) error {
	resultErr := retry.RetryOnConflict(util.OvnConflictBackoff, func() error {
		// Informer cache should not be mutated, so get a copy of the object
		pod, err := bnc.watchFactory.GetPod(origPod.Namespace, origPod.Name)
		if err != nil {
			return err
		}

		cpod := pod.DeepCopy()
		cpod.Annotations, err = util.MarshalPodAnnotation(cpod.Annotations, podInfo, nadName)
		if err != nil {
			return err
		}
		return bnc.kube.UpdatePod(cpod)
	})
	if resultErr != nil {
		return fmt.Errorf("failed to update annotation on pod %s/%s: %v", origPod.Namespace, origPod.Name, resultErr)
	}
	return nil
}

// Given a switch, gets the next set of addresses (from the IPAM) for each of the node's
// subnets to assign to the new pod
func assignPodAddresses(lsManager *lsm.LogicalSwitchManager, switchName string) (net.HardwareAddr, []*net.IPNet, error) {
	var (
		podMAC   net.HardwareAddr
		podCIDRs []*net.IPNet
		err      error
	)
	podCIDRs, err = lsManager.AllocateNextIPs(switchName)
	if err != nil {
		return nil, nil, err
	}
	if len(podCIDRs) > 0 {
		podMAC = util.IPAddrToHWAddr(podCIDRs[0].IP)
	}
	return podMAC, podCIDRs, nil
}

// Given a logical switch port and the switch on which it is scheduled, get all
// addresses currently assigned to it including subnet masks.
func getPortAddresses(lsManager *lsm.LogicalSwitchManager, switchName string, existingLSP *nbdb.LogicalSwitchPort) (net.HardwareAddr, []*net.IPNet, error) {
	podMac, podIPs, err := util.ExtractPortAddresses(existingLSP)
	if err != nil {
		return nil, nil, err
	} else if podMac == nil || len(podIPs) == 0 {
		return nil, nil, nil
	}

	var podIPNets []*net.IPNet

	nodeSubnets := lsManager.GetSwitchSubnets(switchName)

	for _, ip := range podIPs {
		for _, subnet := range nodeSubnets {
			if subnet.Contains(ip) {
				podIPNets = append(podIPNets,
					&net.IPNet{
						IP:   ip,
						Mask: subnet.Mask,
					})
				break
			}
		}
	}
	return podMac, podIPNets, nil
}

// delLSPOps returns the ovsdb operations required to delete the given logical switch port (LSP)
func (bnc *BaseNetworkController) delLSPOps(lsManager *lsm.LogicalSwitchManager, logicalPort, switchName,
	lspUUID string) ([]ovsdb.Operation, error) {
	lsUUID, _ := lsManager.GetUUID(switchName)
	lsw := nbdb.LogicalSwitch{
		UUID: lsUUID,
		Name: switchName,
	}
	lsp := nbdb.LogicalSwitchPort{
		UUID: lspUUID,
		Name: logicalPort,
	}
	ops, err := libovsdbops.DeleteLogicalSwitchPortsOps(bnc.nbClient, nil, &lsw, &lsp)
	if err != nil {
		return nil, fmt.Errorf("error deleting logical switch port %+v from switch %+v: %w", lsp, lsw, err)
	}

	return ops, nil
}
