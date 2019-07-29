package cluster

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/openshift/origin/pkg/util/netutils"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	"github.com/sirupsen/logrus"
	kapi "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"
)

func addService(service *kapi.Service, inport, outport, gwBridge string) {
	if !util.ServiceTypeHasNodePort(service) {
		return
	}

	for _, svcPort := range service.Spec.Ports {
		if svcPort.Protocol != kapi.ProtocolTCP &&
			svcPort.Protocol != kapi.ProtocolUDP {
			continue
		}
		protocol := strings.ToLower(string(svcPort.Protocol))

		_, stderr, err := util.RunOVSOfctl("add-flow", gwBridge,
			fmt.Sprintf("priority=100, in_port=%s, %s, tp_dst=%d, actions=%s",
				inport, protocol, svcPort.NodePort, outport))
		if err != nil {
			logrus.Errorf("Failed to add openflow flow on %s for nodePort "+
				"%d, stderr: %q, error: %v", gwBridge,
				svcPort.NodePort, stderr, err)
		}
	}
}

func deleteService(service *kapi.Service, inport, gwBridge string) {
	if !util.ServiceTypeHasNodePort(service) {
		return
	}

	for _, svcPort := range service.Spec.Ports {
		if svcPort.Protocol != kapi.ProtocolTCP &&
			svcPort.Protocol != kapi.ProtocolUDP {
			continue
		}

		protocol := strings.ToLower(string(svcPort.Protocol))

		_, stderr, err := util.RunOVSOfctl("del-flows", gwBridge,
			fmt.Sprintf("in_port=%s, %s, tp_dst=%d",
				inport, protocol, svcPort.NodePort))
		if err != nil {
			logrus.Errorf("Failed to delete openflow flow on %s for nodePort "+
				"%d, stderr: %q, error: %v", gwBridge,
				svcPort.NodePort, stderr, err)
		}
	}
}

func syncServices(services []interface{}, gwBridge, gwIntf string) {
	// Get ofport of physical interface
	inport, stderr, err := util.RunOVSVsctl("--if-exists", "get",
		"interface", gwIntf, "ofport")
	if err != nil {
		logrus.Errorf("Failed to get ofport of %s, stderr: %q, error: %v",
			gwIntf, stderr, err)
		return
	}

	nodePorts := make(map[string]bool)
	for _, serviceInterface := range services {
		service, ok := serviceInterface.(*kapi.Service)
		if !ok {
			logrus.Errorf("Spurious object in syncServices: %v",
				serviceInterface)
			continue
		}

		if !util.ServiceTypeHasNodePort(service) ||
			len(service.Spec.Ports) == 0 {
			continue
		}

		for _, svcPort := range service.Spec.Ports {
			port := svcPort.NodePort
			if port == 0 {
				continue
			}

			prot := svcPort.Protocol
			if prot != kapi.ProtocolTCP && prot != kapi.ProtocolUDP {
				continue
			}
			protocol := strings.ToLower(string(prot))
			nodePortKey := fmt.Sprintf("%s_%d", protocol, port)
			nodePorts[nodePortKey] = true
		}
	}

	stdout, stderr, err := util.RunOVSOfctl("dump-flows",
		gwBridge)
	if err != nil {
		logrus.Errorf("dump-flows failed: %q (%v)", stderr, err)
		return
	}
	flows := strings.Split(stdout, "\n")

	re, err := regexp.Compile(`tp_dst=(.*?)[, ]`)
	if err != nil {
		logrus.Errorf("regexp compile failed: %v", err)
		return
	}

	for _, flow := range flows {
		group := re.FindStringSubmatch(flow)
		if group == nil {
			continue
		}

		var key string
		if strings.Contains(flow, "tcp") {
			key = fmt.Sprintf("tcp_%s", group[1])
		} else if strings.Contains(flow, "udp") {
			key = fmt.Sprintf("udp_%s", group[1])
		} else {
			continue
		}

		if _, ok := nodePorts[key]; !ok {
			pair := strings.Split(key, "_")
			protocol, port := pair[0], pair[1]

			stdout, _, err := util.RunOVSOfctl(
				"del-flows", gwBridge,
				fmt.Sprintf("in_port=%s, %s, tp_dst=%s",
					inport, protocol, port))
			if err != nil {
				logrus.Errorf("del-flows of %s failed: %q",
					gwBridge, stdout)
			}
		}
	}
}

func nodePortWatcher(nodeName, gwBridge, gwIntf string, wf *factory.WatchFactory) error {
	// the name of the patch port created by ovn-controller is of the form
	// patch-<logical_port_name_of_localnet_port>-to-br-int
	patchPort := "patch-" + gwBridge + "_" + nodeName + "-to-br-int"
	// Get ofport of patchPort
	ofportPatch, stderr, err := util.RunOVSVsctl("--if-exists", "get",
		"interface", patchPort, "ofport")
	if err != nil {
		return fmt.Errorf("Failed to get ofport of %s, stderr: %q, error: %v",
			patchPort, stderr, err)
	}

	// Get ofport of physical interface
	ofportPhys, stderr, err := util.RunOVSVsctl("--if-exists", "get",
		"interface", gwIntf, "ofport")
	if err != nil {
		return fmt.Errorf("Failed to get ofport of %s, stderr: %q, error: %v",
			gwIntf, stderr, err)
	}

	_, err = wf.AddServiceHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			service := obj.(*kapi.Service)
			addService(service, ofportPhys, ofportPatch, gwBridge)
		},
		UpdateFunc: func(old, new interface{}) {
		},
		DeleteFunc: func(obj interface{}) {
			service := obj.(*kapi.Service)
			deleteService(service, ofportPhys, gwBridge)
		},
	}, func(services []interface{}) {
		syncServices(services, gwBridge, gwIntf)
	})

	return err
}

func addDefaultConntrackRules(nodeName, gwBridge, gwIntf string) error {
	// the name of the patch port created by ovn-controller is of the form
	// patch-<logical_port_name_of_localnet_port>-to-br-int
	localnetLpName := gwBridge + "_" + nodeName
	patchPort := "patch-" + localnetLpName + "-to-br-int"
	// Get ofport of patchPort, but before that make sure ovn-controller created
	// one for us (waits for about ovsCommandTimeout seconds)
	ofportPatch, stderr, err := util.RunOVSVsctl("wait-until", "Interface", patchPort, "ofport>0",
		"--", "get", "Interface", patchPort, "ofport")
	if err != nil {
		return fmt.Errorf("Failed while waiting on patch port %q to be created by ovn-controller and "+
			"while getting ofport. stderr: %q, error: %v", patchPort, stderr, err)
	}

	// Get ofport of physical interface
	ofportPhys, stderr, err := util.RunOVSVsctl("--if-exists", "get",
		"interface", gwIntf, "ofport")
	if err != nil {
		return fmt.Errorf("Failed to get ofport of %s, stderr: %q, error: %v",
			gwIntf, stderr, err)
	}

	// table 0, packets coming from pods headed externally. Commit connections
	// so that reverse direction goes back to the pods.
	_, stderr, err = util.RunOVSOfctl("add-flow", gwBridge,
		fmt.Sprintf("priority=100, in_port=%s, ip, "+
			"actions=ct(commit, zone=%d), output:%s",
			ofportPatch, config.Default.ConntrackZone, ofportPhys))
	if err != nil {
		return fmt.Errorf("Failed to add openflow flow to %s, stderr: %q, "+
			"error: %v", gwBridge, stderr, err)
	}

	// table 0, packets coming from external. Send it through conntrack and
	// resubmit to table 1 to know the state of the connection.
	_, stderr, err = util.RunOVSOfctl("add-flow", gwBridge,
		fmt.Sprintf("priority=50, in_port=%s, ip, "+
			"actions=ct(zone=%d, table=1)", ofportPhys, config.Default.ConntrackZone))
	if err != nil {
		return fmt.Errorf("Failed to add openflow flow to %s, stderr: %q, "+
			"error: %v", gwBridge, stderr, err)
	}

	// table 1, established and related connections go to pod
	_, stderr, err = util.RunOVSOfctl("add-flow", gwBridge,
		fmt.Sprintf("priority=100, table=1, ct_state=+trk+est, "+
			"actions=output:%s", ofportPatch))
	if err != nil {
		return fmt.Errorf("Failed to add openflow flow to %s, stderr: %q, "+
			"error: %v", gwBridge, stderr, err)
	}
	_, stderr, err = util.RunOVSOfctl("add-flow", gwBridge,
		fmt.Sprintf("priority=100, table=1, ct_state=+trk+rel, "+
			"actions=output:%s", ofportPatch))
	if err != nil {
		return fmt.Errorf("Failed to add openflow flow to %s, stderr: %q, "+
			"error: %v", gwBridge, stderr, err)
	}

	// table 1, all other connections go to the bridge interface.
	_, stderr, err = util.RunOVSOfctl("add-flow", gwBridge,
		"priority=0, table=1, actions=output:LOCAL")
	if err != nil {
		return fmt.Errorf("Failed to add openflow flow to %s, stderr: %q, "+
			"error: %v", gwBridge, stderr, err)
	}
	return nil
}


// CreateManagementPort creates a management port attached to the node switch
// that lets the node access its pods via their private IP address. This is used
// for health checking and other management tasks.
func createLocalnetPort(nodeName, localSubnet string) error {
	// Make sure br-int is created.
	stdout, stderr, err := util.RunOVSVsctl("--", "--may-exist", "add-br", "br-int")
	if err != nil {
		logrus.Errorf("Failed to create br-int, stdout: %q, stderr: %q, error: %v", stdout, stderr, err)
		return err
	}

	// Create a OVS internal localnet interface.
	portName := "lcl-" + strings.ToLower(nodeName)
	interfaceName := portName[:15]

	stdout, stderr, err = util.RunOVSVsctl("--", "--may-exist", "add-port",
		"br-int", interfaceName, "--", "set", "interface", interfaceName,
		"type=internal", "mtu_request="+fmt.Sprintf("%d", config.Default.MTU),
		"external-ids:iface-id="+portName)
	if err != nil {
		logrus.Errorf("Failed to add port to br-int, stdout: %q, stderr: %q, error: %v", stdout, stderr, err)
		return err
	}
	mac, stderr, err := util.RunOVSVsctl("--if-exists", "get", "interface", interfaceName, "mac_in_use")
	if err != nil {
		logrus.Errorf("Failed to get mac address of %v, stderr: %q, error: %v", interfaceName, stderr, err)
		return err
	}
	if mac == "[]" {
		return fmt.Errorf("Failed to get mac address of %v", interfaceName)
	}

	// Create the gateway switch port in 'join' if it doesn't exist yet
	stdout, stderr, err = util.RunOVNNbctl("--wait=sb",
		"--may-exist", "lsp-add", "join", portName,
		"--", "--if-exists", "clear", "logical_switch_port", portName, "dynamic_addresses",
		"--", "lsp-set-addresses", portName, mac+" "+"dynamic")
	if err != nil {
		return fmt.Errorf("failed to add logical switch "+
			"port %s, stdout: %q, stderr: %q, error: %v",
			portName, stdout, stderr, err)
	}

	// Should have an address already since we waited for the SB above
	_, ip, err := util.GetPortAddresses(portName)
	if err != nil {
		return fmt.Errorf("error while waiting for addresses "+
			"for localnet switch port %q: %v", portName, err)
	}
	if ip == nil {
		return fmt.Errorf("empty addresses for localnet "+
			"switch port %q", portName)
	}

	// Up the interface.
	_, _, err = util.RunIP("link", "set", interfaceName, "up")
	if err != nil {
		return fmt.Errorf("error while bringing up interface %q: %v", interfaceName, err)
	}

	// The interface may already exist, in which case delete the routes and IP.
	_, _, err = util.RunIP("addr", "flush", "dev", interfaceName)
	if err != nil {
		return fmt.Errorf("error while flushing up addresses on interface %q: %v", interfaceName, err)
	}

	// Assign IP address to the internal interface.
	_, _, err = util.RunIP("addr", "add", ip.String()+"/16", "dev", interfaceName)
	if err != nil {
		return fmt.Errorf("error while bringing up address on interface %q: %v", interfaceName, err)
	}

	// Flush the route for the node IP (in case it was added before).
	_, _, err = util.RunIP("route", "flush", localSubnet)
	if err != nil {
		return fmt.Errorf("error while flushing route for local subnet %q: %v", localSubnet, err)
	}

	// Create a route for the local subnet.
	_, _, err = util.RunIP("route", "add", localSubnet, "via", ip.String())
	if err != nil {
		return fmt.Errorf("error while adding route for local subnet %q: %v", localSubnet, err)
	}

	// Create a static arp for the ip.
	_, _, err = util.RunIP("neigh", "add", ip.String(), "lladdr", mac, "dev", interfaceName)
	if err != nil {
		return fmt.Errorf("error while adding static ARP: %v", err)
	}

	// Create the lr route to nodeIP
	nodeIP, err := netutils.GetNodeIP(nodeName)
	stdout, stderr, err = util.RunOVNNbctl("--may-exist", "lr-route-add", "ovn_cluster_router", nodeIP+"/32", ip.String())
	if err != nil {
		return fmt.Errorf("failed to add route for %v on logical router ovn_cluster_route stdout: %q"+
			", stderr: %q, error: %v", nodeName, stdout, stderr, err)
	}

	return nil
}

func initSharedGateway(
	nodeName string, clusterIPSubnet []string, subnet,
	gwNextHop, gwIntf string, wf *factory.WatchFactory) error {
	var bridgeName string

	// Check to see whether the interface is OVS bridge.
	if _, _, err := util.RunOVSVsctl("--", "br-exists", gwIntf); err != nil {
		// This is not a OVS bridge. We need to create a OVS bridge
		// and add cluster.GatewayIntf as a port of that bridge.
		bridgeName, err = util.NicToBridge(gwIntf)
		if err != nil {
			return fmt.Errorf("failed to convert %s to OVS bridge: %v",
				gwIntf, err)
		}
	} else {
		intfName, err := getIntfName(gwIntf)
		if err != nil {
			return err
		}
		bridgeName = gwIntf
		gwIntf = intfName
	}

	// Now, we get IP address from OVS bridge. If IP does not exist,
	// error out.
	ipAddress, err := getIPv4Address(bridgeName)
	if err != nil {
		return fmt.Errorf("Failed to get interface details for %s (%v)",
			bridgeName, err)
	}
	if ipAddress == "" {
		return fmt.Errorf("%s does not have a ipv4 address", bridgeName)
	}

	ifaceID, macAddress, err := bridgedGatewayNodeSetup(nodeName, bridgeName)
	if err != nil {
		return fmt.Errorf("failed to set up shared interface gateway: %v", err)
	}

	var lspArgs []string
	if config.Gateway.VLANID > 0 {
		lspArgs = []string{"--", "set", "logical_switch_port",
			ifaceID, fmt.Sprintf("tag_request=%d", config.Gateway.VLANID)}
	}

	err = createLocalnetPort(nodeName, subnet)
	if err != nil {
		return err
	}

	err = util.GatewayInit(clusterIPSubnet, nodeName, ifaceID, ipAddress,
		macAddress, gwNextHop, subnet, true, true, lspArgs)
	if err != nil {
		return fmt.Errorf("failed to init shared interface gateway: %v", err)
	}

	// Program cluster.GatewayIntf to let non-pod traffic to go to host
	// stack
	if err := addDefaultConntrackRules(nodeName, bridgeName, gwIntf); err != nil {
		return err
	}

	if config.Gateway.NodeportEnable {
		// Program cluster.GatewayIntf to let nodePort traffic to go to pods.
		if err := nodePortWatcher(nodeName, bridgeName, gwIntf, wf); err != nil {
			return err
		}
	}

	return nil
}

func cleanupSharedGateway() error {
	// NicToBridge() may be created before-hand, only delete the patch port here
	stdout, stderr, err := util.RunOVSVsctl("--columns=name", "--no-heading", "find", "port",
		"external_ids:ovn-localnet-port!=_")
	if err != nil {
		return fmt.Errorf("Failed to get ovn-localnet-port port stderr:%s (%v)", stderr, err)
	}
	ports := strings.Fields(strings.Trim(stdout, "\""))
	for _, port := range ports {
		_, stderr, err := util.RunOVSVsctl("--if-exists", "del-port", strings.Trim(port, "\""))
		if err != nil {
			return fmt.Errorf("Failed to delete port %s stderr:%s (%v)", port, stderr, err)
		}
	}
	return nil
}
