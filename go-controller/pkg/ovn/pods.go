package ovn

import (
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/cni/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	"github.com/sirupsen/logrus"
	kapi "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
)

// Builds the logical switch port name for a given pod.
func podLogicalPortName(pod *kapi.Pod, netPrefix string) string {
	return netPrefix + pod.Namespace + "_" + pod.Name
}

func (oc *Controller) syncPods(pods []interface{}) {
	logrus.Debugf("CATHY syncPods")
	// get the list of logical switch ports (equivalent to pods)
	expectedLogicalPorts := make(map[string]bool)
	for _, podInterface := range pods {
		pod, ok := podInterface.(*kapi.Pod)
		if !ok {
			logrus.Errorf("Spurious object in syncPods: %v", podInterface)
			continue
		}
		if podScheduled(pod) && podWantsNetwork(pod) {
			logicalPort := podLogicalPortName(pod, "")
			expectedLogicalPorts[logicalPort] = true
		}
	}

	// get the list of logical ports from OVN
	output, stderr, err := util.RunOVNNbctl("--data=bare", "--no-heading",
		"--columns=name,external_ids", "find", "logical_switch_port", "external_ids:pod=true")
	if err != nil {
		logrus.Errorf("Error in obtaining list of logical ports, "+
			"stderr: %q, err: %v",
			stderr, err)
		return
	}
	for _, result := range strings.Split(output, "\n\n") {
		items := strings.Split(result, "\n")
		if len(items) < 2 || len(items[0]) == 0 {
			continue
		}
		existingPort := items[0]
		netName := util.GetDbValByKey(items[1], "network_name")
		if netName != "" {
			if !strings.HasPrefix(existingPort, netName+"_") {
				logrus.Warningf("CATHYZ Unexpected Pod name %s expecting prefixed with %s", existingPort, netName+"_")
				continue
			}
			existingPort = strings.TrimPrefix(existingPort, netName+"_")
		}
		if _, ok := expectedLogicalPorts[existingPort]; !ok {
			// not found, delete this logical port
			logrus.Infof("Stale logical port found: %s. This logical port will be deleted.", items[0])
			out, stderr, err := util.RunOVNNbctl("--if-exists", "lsp-del",
				items[0])
			if err != nil {
				logrus.Errorf("Error in deleting pod's logical port "+
					"stdout: %q, stderr: %q err: %v",
					out, stderr, err)
			}
			if netName == "" && !oc.portGroupSupport {
				oc.deletePodAcls(items[0])
			}
		}
	}
}

func (oc *Controller) deletePodAcls(logicalPort string) {
	// delete the ACL rules on OVN that corresponding pod has been deleted
	uuids, stderr, err := util.RunOVNNbctl("--data=bare", "--no-heading",
		"--columns=_uuid", "find", "ACL",
		fmt.Sprintf("external_ids:logical_port=%s", logicalPort))
	if err != nil {
		logrus.Errorf("Error in getting list of acls "+
			"stdout: %q, stderr: %q, error: %v", uuids, stderr, err)
		return
	}

	if uuids == "" {
		logrus.Debugf("deletePodAcls: returning because find " +
			"returned no ACLs")
		return
	}

	uuidSlice := strings.Fields(uuids)
	for _, uuid := range uuidSlice {
		// Get logical switch
		out, stderr, err := util.RunOVNNbctl("--data=bare",
			"--no-heading", "--columns=_uuid", "find", "logical_switch",
			fmt.Sprintf("acls{>=}%s", uuid))
		if err != nil {
			logrus.Errorf("find failed to get the logical_switch of acl "+
				"uuid=%s, stderr: %q, (%v)", uuid, stderr, err)
			continue
		}

		if out == "" {
			continue
		}
		logicalSwitch := out

		_, stderr, err = util.RunOVNNbctl("--if-exists", "remove",
			"logical_switch", logicalSwitch, "acls", uuid)
		if err != nil {
			logrus.Errorf("failed to delete the allow-from rule %s for"+
				" logical_switch=%s, logical_port=%s, stderr: %q, (%v)",
				uuid, logicalSwitch, logicalPort, stderr, err)
			continue
		}
	}
}

func (oc *Controller) getLogicalPortUUID(logicalPort string) string {
	if oc.logicalPortUUIDCache[logicalPort] != "" {
		return oc.logicalPortUUIDCache[logicalPort]
	}

	out, stderr, err := util.RunOVNNbctl("--if-exists", "get",
		"logical_switch_port", logicalPort, "_uuid")
	if err != nil {
		logrus.Errorf("Error while getting uuid for logical_switch_port "+
			"%s, stderr: %q, err: %v", logicalPort, stderr, err)
		return ""
	}

	if out == "" {
		return out
	}

	oc.logicalPortUUIDCache[logicalPort] = out
	return oc.logicalPortUUIDCache[logicalPort]
}

func (oc *Controller) getGatewayFromSwitch(logicalSwitch string) (*net.IPNet, error) {
	var gatewayIPMaskStr, stderr string
	var ok bool
	var err error

	oc.lsMutex.Lock()
	defer oc.lsMutex.Unlock()
	if gatewayIPMaskStr, ok = oc.gatewayCache[logicalSwitch]; !ok {
		gatewayIPMaskStr, stderr, err = util.RunOVNNbctl("--if-exists",
			"get", "logical_switch", logicalSwitch,
			"external_ids:gateway_ip")
		if err != nil {
			logrus.Errorf("Failed to get gateway IP:  %s, stderr: %q, %v",
				gatewayIPMaskStr, stderr, err)
			return nil, err
		}
		if gatewayIPMaskStr == "" {
			return nil, fmt.Errorf("Empty gateway IP in logical switch %s",
				logicalSwitch)
		}
		oc.gatewayCache[logicalSwitch] = gatewayIPMaskStr
	}
	ip, ipnet, err := net.ParseCIDR(gatewayIPMaskStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse gateway IP %q: %v", gatewayIPMaskStr, err)
	}
	ipnet.IP = ip
	return ipnet, nil
}

func (oc *Controller) getPodNetNames(pod *kapi.Pod) ([]*types.NetworkSelectionElement, error) {
	if pod.Spec.HostNetwork {
		return nil, nil
	}

	defaultNetwork, err := util.GetK8sPodDefaultNetwork(pod)
	if err != nil {
		return nil, fmt.Errorf("failed to get default network for pod %s/%s", pod.Namespace, pod.Name)
	}

	networks, err := util.GetPodNetwork(pod)
	if err != nil {
		return nil, fmt.Errorf("failed to get networks for pod %s/%s", pod.Namespace, pod.Name)
	}

	oc.netMutex.Lock()
	defer oc.netMutex.Unlock()
	for _, network := range networks {
		logrus.Debugf("CATHY getPodNetNames pod %s/%s network %s", pod.Namespace, pod.Name, network.Name)
		if _, ok := oc.netAttchmtDefs[network.Name]; !ok {
			return nil, fmt.Errorf("CATHY network %s does not exist for pod %s/%s", network.Name, pod.Namespace, pod.Name)
		}
		if oc.netAttchmtDefs[network.Name].isDefault {
			return nil, fmt.Errorf("Cathy cluster default network %s cannot be used for non-default interface of Pod %s/%s",
				network.Name, pod.Namespace, pod.Name)
		}
	}
	if defaultNetwork != nil {
		if _, ok := oc.netAttchmtDefs[defaultNetwork.Name]; !ok {
			logrus.Errorf("CATHY network %s does not exist for pod %s", defaultNetwork.Name, pod.Name)
			return nil, fmt.Errorf("CATHY network %s does not exist for pod %s", defaultNetwork.Name, pod.Name)
		}
	}
	if defaultNetwork != nil && !oc.netAttchmtDefs[defaultNetwork.Name].isDefault {
		// default network for this pod is not the cluster default network
		networks = append(networks, defaultNetwork)
	} else {
		// add default network if it is not specified or it is the default crd
		networks = append(networks, &types.NetworkSelectionElement{
			Name:      "",
			Namespace: "",
		})
	}

	for _, network := range networks {
		if network.Name != "" {
			oc.netAttchmtDefs[network.Name].pods[pod.Namespace+"_"+pod.Name] = true
		}
	}

	return networks, nil
}

func (oc *Controller) deletePod(pod *kapi.Pod) {
	logrus.Debugf("delete pod event %s/%s", pod.Namespace, pod.Name)

	// Best effort, as we might not be able to look up the pod's networkattachmentdefinition if it has already been
	// deleted incorrectly.
	// Get the list of logical ports from OVN
	output, stderr, err := util.RunOVNNbctl("--data=bare", "--no-heading",
		"--columns=name,external_ids", "find", "logical_switch_port", "external_ids:pod=true")
	if err != nil {
		logrus.Errorf("Error in obtaining list of logical ports, "+
			"stderr: %q, err: %v",
			stderr, err)
		return
	}
	for _, result := range strings.Split(output, "\n\n") {
		items := strings.Split(result, "\n")
		if len(items) < 2 || len(items[0]) == 0 {
			continue
		}
		// switch port name in the format of netPrefix_PodNamespace_podName
		netName := util.GetDbValByKey(items[1], "network_name")
		if items[0] == util.GetNetworkPrefix(netName)+pod.Namespace+"_"+pod.Name {
			oc.deleteLogicalPort(pod, netName)
		}
	}
	oc.netMutex.Lock()
	defer oc.netMutex.Unlock()
	for _, networkAttDef := range oc.netAttchmtDefs {
		delete(networkAttDef.pods, pod.Namespace+"_"+pod.Name)
	}
}

func (oc *Controller) deleteLogicalPort(pod *kapi.Pod, netName string) {
	podDesc := pod.Namespace + "/" + pod.Name
	logrus.Debugf("CATHY deleteNetworkLogicalPort pod %s network %s", podDesc, netName)
	logrus.Infof("Deleting pod: %s", podDesc)
	netPrefix := util.GetNetworkPrefix(netName)
	logicalPort := podLogicalPortName(pod, netPrefix)
	out, stderr, err := util.RunOVNNbctl("--if-exists", "lsp-del",
		logicalPort)
	if err != nil {
		logrus.Errorf("Error in deleting pod %s logical port "+
			"stdout: %q, stderr: %q, (%v)",
			podDesc, out, stderr, err)
	}

	var podIP net.IP
	podAnnotation, err := util.UnmarshalPodAnnotation(pod.Annotations, netName)
	if err != nil {
		logrus.Debugf("failed to read pod %s annotation when deleting "+
			"logical port; falling back to PodIP %s: %v",
			podDesc, pod.Status.PodIP, err)
		podIP = net.ParseIP(pod.Status.PodIP)
	} else {
		podIP = podAnnotation.IP.IP
	}

	delete(oc.logicalPortCache, logicalPort)

	oc.lspMutex.Lock()
	delete(oc.lspIngressDenyCache, logicalPort)
	delete(oc.lspEgressDenyCache, logicalPort)
	delete(oc.logicalPortUUIDCache, logicalPort)
	oc.lspMutex.Unlock()

	if netName != "" {
		return
	}
	if !oc.portGroupSupport {
		oc.deleteACLDenyOld(pod.Namespace, pod.Spec.NodeName, logicalPort,
			"Ingress")
		oc.deleteACLDenyOld(pod.Namespace, pod.Spec.NodeName, logicalPort,
			"Egress")
	}
	oc.deletePodFromNamespace(pod.Namespace, podIP, logicalPort)
}

func (oc *Controller) waitForNodeLogicalSwitch(nodeName, netName string) error {
	logrus.Debugf("CATHY waitForNodeLogicalSwitch node %s network %s", nodeName, netName)
	netPrefix := util.GetNetworkPrefix(netName)
	oc.lsMutex.Lock()
	ok := oc.logicalSwitchCache[netPrefix+nodeName]
	oc.lsMutex.Unlock()
	// Fast return if we already have the node switch in our cache
	if ok {
		return nil
	}

	// Otherwise wait for the node logical switch to be created by the ClusterController.
	// The node switch will be created very soon after startup so we should
	// only be waiting here once per node at most.
	if err := wait.PollImmediate(500*time.Millisecond, 30*time.Second, func() (bool, error) {
		if _, _, err := util.RunOVNNbctl("get", "logical_switch", netPrefix+nodeName, "other-config"); err != nil {
			return false, nil
		}
		return true, nil
	}); err != nil {
		logrus.Errorf("timed out waiting for node %q logical switch: %v", netPrefix+nodeName, err)
		return err
	}

	oc.lsMutex.Lock()
	defer oc.lsMutex.Unlock()
	if !oc.logicalSwitchCache[netPrefix+nodeName] {
		if netName == "" {
			if err := oc.addAllowACLFromNode(nodeName); err != nil {
				return err
			}
		}
		oc.logicalSwitchCache[netPrefix+nodeName] = true
	}
	return nil
}

func (oc *Controller) addPod(pod *kapi.Pod) error {
	logrus.Debugf("add pod event %s/%s", pod.Namespace, pod.Name)

	// Keep track of how long syncs take.
	start := time.Now()
	defer func() {
		logrus.Infof("[%s/%s] addLogicalPort took %v", pod.Namespace, pod.Name, time.Since(start))
	}()

	annotation, err := util.UnmarshalPodAnnotation(pod.Annotations, "")
	annotationsSet := (err == nil)

	networks, err := oc.getPodNetNames(pod)
	if err != nil {
		return fmt.Errorf("failed to get all networks for pod %s/%s: %v", pod.Namespace, pod.Name, err)
	}

	podInfoMap := make(map[string]*util.PodAnnotation)
	var addedNetworks []string
	for _, network := range networks {
		addedNetworks = append(addedNetworks, network.Name)
		podInfoMap[network], err = oc.addLogicalPort(pod, network.Name)
		if err != nil {
			logrus.Errorf("addNetworkLogicalPort for port %s/%s netName %s failed: %v", pod.Namespace,
				pod.Name, network.Name, err)
			for netName := range podInfoMap {
				oc.deleteLogicalPort(pod, netName)
			}
			oc.netMutex.Lock()
			for _, network := range networks {
				logrus.Debugf("CATHY addLogicalPort pod %s/%s network %s", pod.Namespace, pod.Name, network.Name)
				if _, ok := oc.netAttchmtDefs[network.Name]; ok {
					delete(oc.netAttchmtDefs[network.Name].pods, pod.Namespace+"_"+pod.Name)
				}
			}
			oc.netMutex.Unlock()
			return fmt.Errorf("addNetworkLogicalPort for port %s/%s netName %s failed: %v", pod.Namespace,
				pod.Name, network.Name, err)
		}
	}
	marshalledAnnotation, err := util.MarshalPodAnnotation(podInfoMap)
	if err != nil {
		return fmt.Errorf("failed to creating network annotation for pod %s/%s: %v", pod.Namespace, pod.Name, err)
	}
	err = oc.kube.SetAnnotationsOnPod(pod, marshalledAnnotation)
	if err != nil {
		return fmt.Errorf("failed to set annotation on pod %s/%s - %v", pod.Namespace, pod.Name, err)
	}
	// If we're setting the annotation for the first time, observe the creation
	// latency metric.
	if !annotationsSet {
		recordPodCreated(pod)
	}
	return nil
}

func (oc *Controller) addLogicalPort(pod *kapi.Pod, netName string) (*util.PodAnnotation, error) {
	var out, stderr string
	var err error

	netPrefix := util.GetNetworkPrefix(netName)
	logicalSwitch := netPrefix + pod.Spec.NodeName
	if err = oc.waitForNodeLogicalSwitch(pod.Spec.NodeName, netName); err != nil {
		return nil, fmt.Errorf("Failed to find the logical switch for pod %s/%s netName %s",
			pod.Namespace, pod.Name, netName)
	}

	portName := podLogicalPortName(pod, netPrefix)
	logrus.Debugf("Creating logical port for %s on switch %s", portName, logicalSwitch)

	annotation, err := util.UnmarshalPodAnnotation(pod.Annotations, netName)
	// If pod already has annotations, just add the lsp with static ip/mac.
	// Else, create the lsp with dynamic addresses.
	var cmdArgs []string
	if err == nil {
		cmdArgs = []string{"--may-exist", "lsp-add",
			logicalSwitch, portName, "--", "lsp-set-addresses", portName,
			fmt.Sprintf("%s %s", annotation.MAC, annotation.IP.IP), "--", "set",
			"logical_switch_port", portName,
			"external-ids:namespace=" + pod.Namespace,
			"external-ids:logical_switch=" + logicalSwitch,
			"external-ids:pod=true", "--", "--if-exists",
			"clear", "logical_switch_port", portName, "dynamic_addresses"}
	} else {
		cmdArgs = []string{"--wait=sb", "--",
			"--may-exist", "lsp-add", logicalSwitch, portName,
			"--", "lsp-set-addresses",
			portName, "dynamic", "--", "set",
			"logical_switch_port", portName,
			"external-ids:namespace=" + pod.Namespace,
			"external-ids:logical_switch=" + logicalSwitch,
			"external-ids:pod=true"}
	}
	if netName != "" {
		cmdArgs = append(cmdArgs, "external_ids:network_name="+strings.TrimSuffix(netName, "_"))
	}
	out, stderr, err = util.RunOVNNbctl(cmdArgs...)
	if err != nil {
		return nil, fmt.Errorf("Error while creating logical port %s "+
			"stdout: %q, stderr: %q (%v)",
			portName, out, stderr, err)
	}

	oc.logicalPortCache[portName] = logicalSwitch

	gatewayIP, err := oc.getGatewayFromSwitch(logicalSwitch)
	if err != nil {
		return nil, fmt.Errorf("Error obtaining gateway address for switch %s", logicalSwitch)
	}

	var podMac net.HardwareAddr
	var podIP net.IP
	count := 30
	for count > 0 {
		podMac, podIP, err = util.GetPortAddresses(portName)
		if err == nil && podMac != nil && podIP != nil {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("Error while obtaining addresses for %s - %v", portName, err)
		}
		time.Sleep(time.Second)
		count--
	}
	if count == 0 {
		return nil, fmt.Errorf("Error while obtaining addresses for %s "+
			"stdout: %q, stderr: %q, (%v)", portName, out, stderr, err)
	}

	podCIDR := &net.IPNet{IP: podIP, Mask: gatewayIP.Mask}

	// now set the port security for the logical switch port
	out, stderr, err = util.RunOVNNbctl("lsp-set-port-security", portName,
		fmt.Sprintf("%s %s", podMac, podCIDR))
	if err != nil {
		return nil, fmt.Errorf("error while setting port security for logical port %s "+
			"stdout: %q, stderr: %q (%v)", portName, out, stderr, err)
	}

	if netName == "" {
		oc.addPodToNamespace(pod.Namespace, podIP, portName)
	}

	logrus.Debugf("Annotation values: ip=%s ; mac=%s ; gw=%s", podCIDR, podMac, gatewayIP)
	return &util.PodAnnotation{
		IP:  podCIDR,
		MAC: podMac,
		GW:  gatewayIP.IP,
	}, nil
}
