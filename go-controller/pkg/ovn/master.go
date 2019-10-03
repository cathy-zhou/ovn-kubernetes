package ovn

import (
	"fmt"
	"net"
	"strconv"
	"strings"

	kapi "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"

	"github.com/containernetworking/cni/pkg/types"
	ovntypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/cni/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/allocator"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	"github.com/sirupsen/logrus"
)

const (
	// OvnHostSubnet is the constant string representing the annotation key
	OvnHostSubnet = "ovn_host_subnet"
	// OvnClusterRouter is the name of the distributed router
	OvnClusterRouter = "ovn_cluster_router"
	// OvnNodeManagementPortMacAddress is the constant string representing the annotation key
	OvnNodeManagementPortMacAddress = "k8s.ovn.org/node-mgmt-port-mac-address"
	// OvnServiceIdledAt is a constant string representing the Service annotation key
	// whose value indicates the time stamp in RFC3339 format when a Service was idled
	OvnServiceIdledAt = "k8s.ovn.org/idled-at"
	// OvnNodeGatewayMode is the mode of the gateway
	OvnNodeGatewayMode = "k8s.ovn.org/node-gateway-mode"
	// OvnNodeGatewayVlanID is the vlanid used by the gateway
	OvnNodeGatewayVlanID = "k8s.ovn.org/node-gateway-vlan-id"
	// OvnNodeGatewayIfaceID is the interfaceID of the gateway
	OvnNodeGatewayIfaceID = "k8s.ovn.org/node-gateway-iface-id"
	// OvnNodeGatewayMacAddress is the MacAddress of the Gateway interface
	OvnNodeGatewayMacAddress = "k8s.ovn.org/node-gateway-mac-address"
	// OvnNodeGatewayIP is the IP address of the Gateway
	OvnNodeGatewayIP = "k8s.ovn.org/node-gateway-ip"
	// OvnNodeGatewayNextHop is the Next Hop
	OvnNodeGatewayNextHop = "k8s.ovn.org/node-gateway-next-hop"
	// OvnJoinSwitch is the name of the join switch that connects all the distribute gateway routers
	OvnJoinSwitch = "join"
)

// protected by both netMutex and nodeMutex
func (oc *Controller) initSubnetAllocator(netName string) error {

	alreadyAllocated := make([]string)
	for _, subnetMap := range oc.nodeCache {
		for name, hostsubnet := range subnetMap {
			if netName == name {
				alreadyAllocated = append(alreadyAllocated, hostsubnet)
			}
		}
	}
	clusterIPNet, err := config.ParseClusterSubnetEntries(oc.netAttchmtDefs[netName].cidr)
	if err != nil {
		logrus.Errorf("cluster subnet %s for netName %s is invalid: %v", oc.netAttchmtDefs[netName].cidr, netName, err)
		return fmt.Errorf("cluster subnet for netName %s is invalid: %v", oc.netAttchmtDefs[netName].cidr, netName, err)
	}

	masterSubnetAllocatorList := make([]*allocator.SubnetAllocator, 0)
	// NewSubnetAllocator is a subnet IPAM, which takes a CIDR (first argument)
	// and gives out subnets of length 'hostSubnetLength' (second argument)
	// but omitting any that exist in 'subrange' (third argument)
	for _, clusterEntry := range clusterIPNet {
		subrange := make([]string, 0)
		for _, allocatedRange := range alreadyAllocated {
			firstAddress, _, err := net.ParseCIDR(allocatedRange)
			if err != nil {
				return err
			}
			if clusterEntry.CIDR.Contains(firstAddress) {
				subrange = append(subrange, allocatedRange)
			}
		}
		subnetAllocator, err := allocator.NewSubnetAllocator(clusterEntry.CIDR.String(), 32-clusterEntry.HostSubnetLength, subrange)
		if err != nil {
			logrus.Errorf("Create cluster subnet %s for netName %s failed: %v", netName, err)
			return err
		}
		masterSubnetAllocatorList = append(masterSubnetAllocatorList, subnetAllocator)
	}
	oc.netAttchmtDefs[netName].masterSubnetAllocatorList = masterSubnetAllocatorList
}

// StartClusterMaster runs a subnet IPAM and a controller that watches arrival/departure
// of nodes in the cluster
// On an addition to the cluster (node create), a new subnet is created for it that will translate
// to creation of a logical switch (done by the node, but could be created here at the master process too)
// Upon deletion of a node, the switch will be deleted
//
// TODO: Verify that the cluster was not already called with a different global subnet
//  If true, then either quit or perform a complete reconfiguration of the cluster (recreate switches/routers with new subnet values)
func (oc *Controller) StartClusterMaster(masterNodeName string) error {
	var networkAttDef *networkAttachmentDefinitionConfig
	var err error

	if _, _, err := util.RunOVNNbctl("--columns=_uuid", "list", "port_group"); err == nil {
		oc.portGroupSupport = true
	}

	// Multicast support requires portGroupSupport
	if oc.portGroupSupport {
		if _, _, err := util.RunOVNSbctl("--columns=_uuid", "list", "IGMP_Group"); err == nil {
			oc.multicastSupport = true
		}
	}

	defaultNetConf := &ovntypes.NetConf{
		types.NetConf{
			Name: "",
		},
		"",
		config.Default.RawClusterSubnets,
		false,
		config.Default.MTU,
		false,
	}
	if networkAttDef, err = oc.SetupMaster(defaultNetConf); err != nil {
		logrus.Errorf("Failed to setup master (%v)", err)
		return err
	}

	oc.netMutex.Lock()
	oc.netAttchmtDefs[""] = networkAttDef
	oc.netMutex.Unlock()
	return nil
}

// SetupMaster creates the central router and load-balancers for the network
// called for non-default network or from NetworkAttachmentDefinition adding handler
func (oc *Controller) SetupMaster(netConf *ovntypes.NetConf) (*networkAttachmentDefinitionConfig, error) {
	var err error

	logrus.Debugf("CATHY SetupMaster: %v", netConf)

	netName := netConf.Name
	netPrefix := util.GetNetworkPrefix(netName)
	if netConf.NetCidr == "" {
		logrus.Errorf("netcidr: %s is not specified for network %s", netConf.NetCidr, netName)
		return nil, fmt.Errorf("netcidr: %s is not specified for network %s", netConf.NetCidr, netName)
	}

	// oc.netAttchmtDefs entries are updated in NetworkAttachmentDefinition events, no need to hold any lock here
	if _, ok := oc.netAttchmtDefs[netName]; ok {
		return nil, fmt.Errorf("Duplicate Network Attachment Defintion %s", netName)
	}

	// Create a single common distributed router for the cluster.
	cmdArgs := []string{"--", "--may-exist", "lr-add", netPrefix + OvnClusterRouter,
		"--", "set", "logical_router", netPrefix + OvnClusterRouter, "external_ids:k8s-cluster-router=yes"}
	if netName != "" {
		cmdArgs = append(cmdArgs, "external_ids:network_name="+netName)
	}
	stdout, stderr, err := util.RunOVNNbctl(cmdArgs...)
	if err != nil {
		logrus.Errorf("Failed to create a single common distributed router for the cluster, "+
			"stdout: %q, stderr: %q, error: %v", stdout, stderr, err)
		return nil, err
	}

	// If supported, enable IGMP relay on the router to forward multicast
	// traffic between nodes.
	if oc.multicastSupport {
		stdout, stderr, err = util.RunOVNNbctl("--", "set", "logical_router",
			OvnClusterRouter, "options:mcast_relay=\"true\"")
		if err != nil {
			logrus.Errorf("Failed to enable IGMP relay on the cluster router, "+
				"stdout: %q, stderr: %q, error: %v", stdout, stderr, err)
			return nil, err
		}

		// Drop IP multicast globally. Multicast is allowed only if explicitly
		// enabled in a namespace.
		err = oc.createDefaultDenyMulticastPolicy()
		if err != nil {
			logrus.Errorf("Failed to create default deny multicast policy, error: %v",
				err)
			return nil, err
		}
	}

	// Create 2 load-balancers for east-west traffic.  One handles UDP and another handles TCP.
	// This is for default network only, as only the default network support k8s service
	if netName == "" {
		oc.TCPLoadBalancerUUID, stderr, err = util.RunOVNNbctl("--data=bare", "--no-heading", "--columns=_uuid", "find", "load_balancer", "external_ids:k8s-cluster-lb-tcp=yes")
		if err != nil {
			logrus.Errorf("Failed to get tcp load-balancer, stderr: %q, error: %v", stderr, err)
			return nil, err
		}

		if oc.TCPLoadBalancerUUID == "" {
			oc.TCPLoadBalancerUUID, stderr, err = util.RunOVNNbctl("--", "create", "load_balancer", "external_ids:k8s-cluster-lb-tcp=yes", "protocol=tcp")
			if err != nil {
				logrus.Errorf("Failed to create tcp load-balancer, stdout: %q, stderr: %q, error: %v", stdout, stderr, err)
				return nil, err
			}
		}

		oc.UDPLoadBalancerUUID, stderr, err = util.RunOVNNbctl("--data=bare", "--no-heading", "--columns=_uuid", "find", "load_balancer", "external_ids:k8s-cluster-lb-udp=yes")
		if err != nil {
			logrus.Errorf("Failed to get udp load-balancer, stderr: %q, error: %v", stderr, err)
			return nil, err
		}
		if oc.UDPLoadBalancerUUID == "" {
			oc.UDPLoadBalancerUUID, stderr, err = util.RunOVNNbctl("--", "create", "load_balancer", "external_ids:k8s-cluster-lb-udp=yes", "protocol=udp")
			if err != nil {
				logrus.Errorf("Failed to create udp load-balancer, stdout: %q, stderr: %q, error: %v", stdout, stderr, err)
				return nil, err
			}
		}
	}

	// Create a logical switch called "join" that will be used to connect gateway routers to the distributed router.
	// The "join" switch will be allocated IP addresses in the range 100.64.0.0/16.
	const joinSubnet string = "100.64.0.1/16"
	joinIP, joinCIDR, _ := net.ParseCIDR(joinSubnet)
	cmdArgs = []string{"--", "--may-exist", "ls-add", netPrefix + OvnJoinSwitch,
		"--", "set", "logical_switch", netPrefix + OvnJoinSwitch, fmt.Sprintf("other-config:subnet=%s", joinCIDR.String()),
		"--", "set", "logical_switch", netPrefix + OvnJoinSwitch, fmt.Sprintf("other-config:exclude_ips=%s", joinIP.String())}
	if netName != "" {
		cmdArgs = append(cmdArgs, "external_ids:network_name="+netName)
	}

	stdout, stderr, err = util.RunOVNNbctl(cmdArgs...)
	if err != nil {
		logrus.Errorf("Failed to create logical switch called \""+netPrefix+OvnJoinSwitch+"\", stdout: %q, stderr: %q, error: %v", stdout, stderr, err)
		return nil, err
	}

	// Connect the distributed router to "join".
	routerMac, stderr, err := util.RunOVNNbctl("--if-exist", "get", "logical_router_port", "rtoj-"+netPrefix+OvnClusterRouter, "mac")
	if err != nil {
		logrus.Errorf("Failed to get logical router port rtoj-%v, stderr: %q, error: %v", netPrefix+OvnClusterRouter, stderr, err)
		return nil, err
	}
	if routerMac == "" {
		routerMac = util.GenerateMac()
		stdout, stderr, err = util.RunOVNNbctl("--", "--may-exist", "lrp-add", netPrefix+OvnClusterRouter,
			"rtoj-"+netPrefix+OvnClusterRouter, routerMac, joinSubnet)
		if err != nil {
			logrus.Errorf("Failed to add logical router port rtoj-%v, stdout: %q, stderr: %q, error: %v",
				netPrefix+OvnClusterRouter, stdout, stderr, err)
			return nil, err
		}
	}

	// Connect the switch "join" to the router.
	stdout, stderr, err = util.RunOVNNbctl("--", "--may-exist", "lsp-add", netPrefix+OvnJoinSwitch, "jtor-"+netPrefix+OvnClusterRouter,
		"--", "set", "logical_switch_port", "jtor-"+netPrefix+OvnClusterRouter, "type=router",
		"options:router-port=rtoj-"+netPrefix+OvnClusterRouter, "addresses="+"\""+routerMac+"\"")
	if err != nil {
		logrus.Errorf("Failed to add router-type logical switch port to "+netPrefix+OvnJoinSwitch+", stdout: %q, stderr: %q, error: %v",
			stdout, stderr, err)
		return nil, err
	}

	return &networkAttachmentDefinitionConfig{
		isDefault:                 false,
		mtu:                       netConf.MTU,
		cidr:                      netConf.NetCidr,
		enableGateway:             !netConf.NoGateway,
		pods:                      make(map[string]bool),
	}, nil
}

func parseNodeManagementPortMacAddr(node *kapi.Node, netPrefix string) (string, error) {
	macAddress, ok := node.Annotations[netPrefix+OvnNodeManagementPortMacAddress]
	if !ok {
		logrus.Errorf("macAddress annotation not found for node %q ", node.Name)
		return "", nil
	}

	_, err := net.ParseMAC(macAddress)
	if err != nil {
		return "", fmt.Errorf("Error %v in parsing node %v macAddress %v", err, node.Name, macAddress)
	}

	return macAddress, nil
}

func (oc *Controller) syncNodeManagementPort(node *kapi.Node, subnet *net.IPNet, netName string) error {

	netPrefix := util.GetNetworkPrefix(netName)
	macAddress, err := parseNodeManagementPortMacAddr(node, netPrefix)
	if err != nil {
		return err
	}

	if macAddress == "" {
		// When macAddress was removed, delete the switch port
		stdout, stderr, err := util.RunOVNNbctl("--", "--if-exists", "lsp-del", netPrefix+"k8s-"+node.Name)
		if err != nil {
			logrus.Errorf("Failed to delete logical port to switch, stdout: %q, stderr: %q, error: %v", stdout, stderr, err)
		}

		return nil
	}

	_, portIP := util.GetNodeWellKnownAddresses(subnet)

	// Create this node's management logical port on the node switch
	stdout, stderr, err := util.RunOVNNbctl(
		"--", "--may-exist", "lsp-add", netPrefix+node.Name, netPrefix+"k8s-"+node.Name,
		"--", "lsp-set-addresses", netPrefix+"k8s-"+node.Name, macAddress+" "+portIP.IP.String(),
		"--", "--if-exists", "remove", "logical_switch", netPrefix+node.Name, "other-config", "exclude_ips")
	if err != nil {
		logrus.Errorf("Failed to add logical port to switch, stdout: %q, stderr: %q, error: %v", stdout, stderr, err)
		return err
	}

	return nil
}

func parseGatewayIfaceID(node *kapi.Node) (string, error) {
	ifaceID, ok := node.Annotations[OvnNodeGatewayIfaceID]
	if !ok || ifaceID == "" {
		return "", fmt.Errorf("%s annotation not found or invalid for node %q ", OvnNodeGatewayIfaceID, node.Name)
	}

	return ifaceID, nil
}

func parseGatewayMacAddress(node *kapi.Node) (string, error) {
	gatewayMacAddress, ok := node.Annotations[OvnNodeGatewayMacAddress]
	if !ok {
		return "", fmt.Errorf("%s annotation not found for node %q ", OvnNodeGatewayMacAddress, node.Name)
	}

	_, err := net.ParseMAC(gatewayMacAddress)
	if err != nil {
		return "", fmt.Errorf("Error %v in parsing node gateway macAddress %v", err, gatewayMacAddress)
	}

	return gatewayMacAddress, nil
}

func parseGatewayLogicalNetwork(node *kapi.Node) (string, string, error) {
	ipAddress, ok := node.Annotations[OvnNodeGatewayIP]
	if !ok {
		return "", "", fmt.Errorf("%s annotation not found for node %q ", OvnNodeGatewayIP, node.Name)
	}

	gwNextHop, ok := node.Annotations[OvnNodeGatewayNextHop]
	if !ok {
		return "", "", fmt.Errorf("%s annotation not found for node %q ", OvnNodeGatewayNextHop, node.Name)
	}

	return ipAddress, gwNextHop, nil
}

func parseGatewayVLANID(node *kapi.Node, ifaceID string) ([]string, error) {

	var lspArgs []string
	vID, ok := node.Annotations[OvnNodeGatewayVlanID]
	if !ok {
		return nil, fmt.Errorf("%s annotation not found for node %q ", OvnNodeGatewayVlanID, node.Name)
	}

	vlanID, errVlan := strconv.Atoi(vID)
	if errVlan != nil {
		return nil, fmt.Errorf("%s annotation has an invalid format for node %q ", OvnNodeGatewayVlanID, node.Name)
	}
	if vlanID > 0 {
		lspArgs = []string{"--", "set", "logical_switch_port",
			ifaceID, fmt.Sprintf("tag_request=%d", vlanID)}
	}

	return lspArgs, nil
}

func (oc *Controller) syncGatewayLogicalNetwork(node *kapi.Node, mode string, subnet string) error {
	var err error
	var clusterSubnets []string
	for _, clusterSubnet := range config.Default.ClusterSubnets {
		clusterSubnets = append(clusterSubnets, clusterSubnet.CIDR.String())
	}

	ifaceID, err := parseGatewayIfaceID(node)
	if err != nil {
		return err
	}

	gwMacAddress, err := parseGatewayMacAddress(node)
	if err != nil {
		return err
	}

	ipAddress, gwNextHop, err := parseGatewayLogicalNetwork(node)
	if err != nil {
		return err
	}

	var lspArgs []string
	var lspErr error
	if mode == string(config.GatewayModeShared) {
		lspArgs, lspErr = parseGatewayVLANID(node, ifaceID)
		if lspErr != nil {
			return lspErr
		}
	}

	err = util.GatewayInit(clusterSubnets, node.Name, ifaceID, ipAddress,
		gwMacAddress, gwNextHop, subnet, true, lspArgs)
	if err != nil {
		return fmt.Errorf("failed to init shared interface gateway: %v", err)
	}

	if mode == string(config.GatewayModeShared) {
		// Add static routes to OVN Cluster Router to enable pods on this Node to
		// reach the host IP
		err = addStaticRouteToHost(node, ipAddress)
		if err != nil {
			return err
		}
	}

	if config.Gateway.NodeportEnable {
		err = oc.handleNodePortLB(node)
	}

	return err
}

func addStaticRouteToHost(node *kapi.Node, nicIP string) error {
	k8sClusterRouter, err := util.GetK8sClusterRouter()
	if err != nil {
		return err
	}

	subnet, err := parseNodeHostSubnet(node)
	if err != nil {
		return fmt.Errorf("failed to get interface IP address for %s (%v)",
			util.GetK8sMgmtIntfName(node.Name), err)
	}
	_, secondIP := util.GetNodeWellKnownAddresses(subnet)
	prefix := strings.Split(nicIP, "/")[0] + "/32"
	nexthop := strings.Split(secondIP.String(), "/")[0]
	_, stderr, err := util.RunOVNNbctl("--may-exist", "lr-route-add", k8sClusterRouter, prefix, nexthop)
	if err != nil {
		return fmt.Errorf("failed to add static route '%s via %s' for host %q on %s "+
			"stderr: %q, error: %v", nicIP, secondIP, node.Name, k8sClusterRouter, stderr, err)
	}

	return nil
}

func parseNodeHostSubnet(node *kapi.Node) (*net.IPNet, error) {
	sub, ok := node.Annotations[OvnHostSubnet]
	if !ok {
		return nil, fmt.Errorf("Error in obtaining host subnet for node %q for deletion", node.Name)
	}
}

// deleteMaster delete the central router and switch for the network
func (oc *Controller) deleteMaster(netName string) {
	logrus.Debugf("CATHY deleteMaster: %v", netName)
	netPrefix := util.GetNetworkPrefix(netName)

	// delete a logical switch called "join" that will be used to connect gateway routers to the distributed router.
	// The "join" switch will be allocated IP addresses in the range 100.64.0.0/16.
	stdout, stderr, err := util.RunOVNNbctl("--if-exist", "ls-del", netPrefix+OvnJoinSwitch)
	if err != nil {
		logrus.Errorf("Failed to delete logical switch called \""+netPrefix+OvnJoinSwitch+"\", stdout: %q, stderr: %q, error: %v", stdout, stderr, err)
	}

	// Create a single common distributed router for the cluster.
	stdout, stderr, err = util.RunOVNNbctl("--if-exist", "lr-del", netPrefix+OvnClusterRouter)
	if err != nil {
		logrus.Errorf("Failed to delete a distributed %s router for the cluster, "+
			"stdout: %q, stderr: %q, error: %v", netPrefix+OvnClusterRouter, stdout, stderr, err)
	}
}

func (oc *Controller) ensureNodeLogicalNetwork(nodeName string, hostsubnet *net.IPNet, netName string) error {

	logrus.Debugf("CATHY ensureNodeLogicalNetwork node %s network: %s", nodeName, netName)
	// Get firstIP for gateway.  Skip the second address of the LogicalSwitch's
	// subnet since we set it aside for the management port on that node.
	firstIP, secondIP := util.GetNodeWellKnownAddresses(hostsubnet)
	netPrefix := util.GetNetworkPrefix(netName)

	nodeLRPMac, stderr, err := util.RunOVNNbctl("--if-exist", "get", "logical_router_port", "rtos-"+netPrefix+nodeName, "mac")
	if err != nil {
		logrus.Errorf("Failed to get logical router port %s, stderr: %q, error: %v", "rtos-"+netPrefix+nodeName, stderr, err)
		return err
	}
	if nodeLRPMac == "" {
		nodeLRPMac = util.GenerateMac()
	}

	// Create a router port and provide it the first address on the node's host subnet
	_, stderr, err = util.RunOVNNbctl("--may-exist", "lrp-add", netPrefix+OvnClusterRouter, "rtos-"+nodeName,
		nodeLRPMac, firstIP.String())
	if err != nil {
		logrus.Errorf("Failed to add logical port %s to router, stderr: %q, error: %v", "rtos-"+netPrefix+nodeName, stderr, err)
		return err
	}

	// Create a logical switch and set its subnet.
	cmdArgs := []string{"--", "--may-exist", "ls-add", netPrefix + nodeName,
		"--", "set", "logical_switch", netPrefix + nodeName, "other-config:subnet=" + hostsubnet.String(),
		"other-config:exclude_ips=" + secondIP.String(),
		"external-ids:gateway_ip=" + firstIP.String()}
	if netName != "" {
		cmdArgs = append(cmdArgs, "external_ids:network_name="+netName)
	}

	// Create a logical switch and set its subnet.
	stdout, stderr, err := util.RunOVNNbctl(cmdArgs...)
	if err != nil {
		logrus.Errorf("Failed to create a logical switch %v, stdout: %q, stderr: %q, error: %v", netPrefix+nodeName, stdout, stderr, err)
		return err
	}

	// If supported, enable IGMP snooping and querier on the node.
	if oc.multicastSupport {
		stdout, stderr, err = util.RunOVNNbctl("set", "logical_switch",
			nodeName, "other-config:mcast_snoop=\"true\"",
			"other-config:mcast_querier=\"true\"",
			"other-config:mcast_eth_src=\""+nodeLRPMac+"\"",
			"other-config:mcast_ip4_src=\""+firstIP.IP.String()+"\"")
		if err != nil {
			logrus.Errorf("Failed to enable IGMP on logical switch %v, stdout: %q, stderr: %q, error: %v",
				nodeName, stdout, stderr, err)
			return err
		}
	}

	// Connect the switch to the router.
	stdout, stderr, err = util.RunOVNNbctl("--", "--may-exist", "lsp-add", netPrefix+nodeName, "stor-"+netPrefix+nodeName,
		"--", "set", "logical_switch_port", "stor-"+netPrefix+nodeName, "type=router", "options:router-port=rtos-"+netPrefix+nodeName, "addresses="+"\""+nodeLRPMac+"\"")
	if err != nil {
		logrus.Errorf("Failed to add logical port %v to switch, stdout: %q, stderr: %q, error: %v", "stor-"+netPrefix+nodeName, stdout, stderr, err)
		return err
	}

	if netName != "" {
		return nil
	}
	// Add our cluster TCP and UDP load balancers to the node switch, default netName only
	if oc.TCPLoadBalancerUUID == "" {
		return fmt.Errorf("TCP cluster load balancer not created")
	}
	stdout, stderr, err = util.RunOVNNbctl("set", "logical_switch", nodeName, "load_balancer="+oc.TCPLoadBalancerUUID)
	if err != nil {
		logrus.Errorf("Failed to set logical switch %v's loadbalancer, stdout: %q, stderr: %q, error: %v", nodeName, stdout, stderr, err)
		return err
	}

	if oc.UDPLoadBalancerUUID == "" {
		return fmt.Errorf("UDP cluster load balancer not created")
	}
	stdout, stderr, err = util.RunOVNNbctl("add", "logical_switch", nodeName, "load_balancer", oc.UDPLoadBalancerUUID)
	if err != nil {
		logrus.Errorf("Failed to add logical switch %v's loadbalancer, stdout: %q, stderr: %q, error: %v", nodeName, stdout, stderr, err)
		return err
	}

	return nil
}

func (oc *Controller) addNode(node *kapi.Node) (map[string]*net.IPNet, error) {
	oc.clearInitialNodeNetworkUnavailableCondition(node)

	logrus.Debug("add Node %s event", node.Name)
	oc.nodeMutex.Lock()
	oc.netMutex.Lock()
	oc.nodeCache[node.Name] = make(map[string]string)
	subnets := make(map[string]*net.IPNet)
	for netName := range oc.netAttchmtDefs {
		logrus.Debugf("CATHY node add event for node %s network %s", node.Name, netName)
		subnet, err := oc.getHostSubnet(node.Name, netName, true)
		if err != nil {
			logrus.Errorf("failed to get subnet for node %s netName %s: %v", node.Name, netName, err)
			continue
		}
		subnets[netName] = subnet
	}
	oc.netMutex.Unlock()
	oc.nodeMutex.Unlock()

	for netName, subnet := range subnets {
		_ = oc.ensureNodeLogicalNetwork(node.Name, subnet, netName)
	}

	return subnets, nil
}

// oc.netMutex is already held
func (oc *Controller) getHostSubnet(nodeName, netName string, create bool) (hostsubnet *net.IPNet, err error) {
	logrus.Debugf("getHostSubnet node %s NetName %s", nodeName, netName)
	if sub, ok := oc.nodeCache[nodeName][netName]; ok {
		_, hostsubnet, err = net.ParseCIDR(sub)
		if err == nil {
			logrus.Debugf("getHostSubnet node %s NetName %s subnet %s", nodeName, netName, hostsubnet.String())
			return hostsubnet, err
		}
	}

	if !create {
		return nil, fmt.Errorf("no network allocated for node %s netName %s: %v", nodeName, netName, err)
	}

	logrus.Debugf("allocate subnet for node %s NetName %s", nodeName, netName)
	// Node doesn't have a subnet assigned; reserve a new one for it
	var subnetAllocator *netutils.SubnetAllocator
	err = netutils.ErrSubnetAllocatorFull
	netattchtDefiniton := oc.netAttchmtDefs[netName]
	for _, subnetAllocator = range netattchtDefiniton.masterSubnetAllocatorList {
		hostsubnet, err = subnetAllocator.GetNetwork()
		if err == allocator.ErrSubnetAllocatorFull {
			// Current subnet exhausted, check next possible subnet
			continue
		} else if err != nil {
			return nil, fmt.Errorf("Error allocating network for node %s: %v", nodeName, err)
		}
		logrus.Infof("Allocated node %s HostSubnet %s", nodeName, hostsubnet.String())
		break
	}
	if err == netutils.ErrSubnetAllocatorFull {
		return nil, fmt.Errorf("Error allocating network for node %s: %v", nodeName, err)
	}

	defer func() {
		// Release the allocation on error
		if err != nil {
			_ = subnetAllocator.ReleaseNetwork(hostsubnet)
		}
	}()

	// Set the HostSubnet annotation on the node object to signal
	// to nodes that their logical infrastructure is set up and they can
	// proceed with their initialization
	netPrefix := util.GetNetworkPrefix(netName)
	err = oc.kube.SetAnnotationOnNode(nodeName, netPrefix+OvnHostSubnet, hostsubnet.String())
	if err != nil {
		logrus.Errorf("Failed to set node %s host subnet annotation to %q: %v",
			nodeName, hostsubnet.String(), err)
		return nil, err
	}

	logrus.Debugf("getHostSubnet successfully allocated subnet %s for node %s NetName %s", hostsubnet.String(), nodeName, netName)
	oc.nodeCache[nodeName][netName] = hostsubnet.String()
	return hostsubnet, nil
}

// oc.netMutex is already held
func (oc *Controller) deleteNodeHostSubnet(nodeName string, subnet *net.IPNet, netName string) error {
	logrus.Debugf("CATHY deleteNodeHostSubnet node %s network: %s", nodeName, netName)
	for _, possibleSubnet := range oc.netAttchmtDefs[netName].masterSubnetAllocatorList {
		if err := possibleSubnet.ReleaseNetwork(subnet); err == nil {
			logrus.Infof("Deleted HostSubnet %v for node %s", subnet, nodeName)
			return nil
		}
	}
	// SubnetAllocator.network is an unexported field so the only way to figure out if a subnet is in a network is to try and delete it
	// if deletion succeeds then stop iterating, if the list is exhausted the node subnet wasn't deleteted return err
	return fmt.Errorf("Error deleting subnet %v for node %q: subnet not found in any CIDR range or already available", subnet, nodeName)
}

func (oc *Controller) deleteNodeLogicalNetwork(nodeName, netName string) error {
	logrus.Debugf("CATHY deleteNodeLogicalNetwork node %s network: %s", nodeName, netName)
	netPrefix := util.GetNetworkPrefix(netName)
	// Remove the logical switch associated with the node
	if _, stderr, err := util.RunOVNNbctl("--if-exist", "ls-del", netPrefix+nodeName); err != nil {
		return fmt.Errorf("Failed to delete logical switch %s, "+
			"stderr: %q, error: %v", netPrefix+nodeName, stderr, err)
	}

	// Remove the patch port that connects distributed router to node's logical switch
	if _, stderr, err := util.RunOVNNbctl("--if-exist", "lrp-del", "rtos-"+netPrefix+nodeName); err != nil {
		return fmt.Errorf("Failed to delete logical router port rtos-%s, "+
			"stderr: %q, error: %v", netPrefix+nodeName, stderr, err)
	}

	return nil
}

func (oc *Controller) deleteNode(nodeName string) error {
	logrus.Debug("delete Node %s event", nodeName)
	deleteNetworks := make(map[string]bool)
	oc.nodeMutex.Lock()
	oc.netMutex.Lock()
	for netName := range oc.netAttchmtDefs {
		logrus.Debugf("CATHY node delete event for node %s network %s", nodeName, netName)
		subnet, err := oc.getHostSubnet(nodeName, netName, false)
		if err == nil {
			if err := oc.deleteNodeHostSubnet(nodeName, subnet, netName); err != nil {
				logrus.Errorf("Error deleting node %s HostSubnet: %v", nodeName, err)
			}
		}
		oc.lsMutex.Lock()
		netPrefix := util.GetNetworkPrefix(netName)
		delete(oc.logicalSwitchCache, netPrefix+nodeName)
		delete(oc.gatewayCache, netPrefix+nodeName)
		oc.lsMutex.Unlock()
		deleteNetworks[netName] = true
	}
	delete(oc.nodeCache, nodeName)
	oc.netMutex.Unlock()
	oc.nodeMutex.Unlock()
	for netName := range deleteNetworks {
		if err := oc.deleteNodeLogicalNetwork(nodeName, netName); err != nil {
			logrus.Errorf("Error deleting node %s logical network: %v", nodeName, err)
		}

	if nodeSubnet != nil {
		if err := util.GatewayCleanup(nodeName, nodeSubnet, netName); err != nil {
			return fmt.Errorf("Failed to clean up node %s gateway: (%v)", nodeName, err)
		}
	}
	return nil
}

// OVN uses an overlay and doesn't need GCE Routes, we need to
// clear the NetworkUnavailable condition that kubelet adds to initial node
// status when using GCE (done here: https://github.com/kubernetes/kubernetes/blob/master/pkg/controller/cloud/node_controller.go#L237).
// See discussion surrounding this here: https://github.com/kubernetes/kubernetes/pull/34398.
// TODO: make upstream kubelet more flexible with overlays and GCE so this
// condition doesn't get added for network plugins that don't want it, and then
// we can remove this function.
func (oc *Controller) clearInitialNodeNetworkUnavailableCondition(origNode *kapi.Node) {
	// Informer cache should not be mutated, so get a copy of the object
	cleared := false
	resultErr := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		var err error

		oldNode, err := oc.kube.GetNode(origNode.Name)
		if err != nil {
			return err
		}

		node := oldNode.DeepCopy()

		for i := range node.Status.Conditions {
			if node.Status.Conditions[i].Type == kapi.NodeNetworkUnavailable {
				condition := &node.Status.Conditions[i]
				if condition.Status != kapi.ConditionFalse && condition.Reason == "NoRouteCreated" {
					condition.Status = kapi.ConditionFalse
					condition.Reason = "RouteCreated"
					condition.Message = "ovn-kube cleared kubelet-set NoRouteCreated"
					condition.LastTransitionTime = metav1.Now()
					if err = oc.kube.UpdateNodeStatus(node); err == nil {
						cleared = true
					}
				}
				break
			}
		}
		return err
	})
	if resultErr != nil {
		logrus.Errorf("status update failed for local node %s: %v", origNode.Name, resultErr)
	} else if cleared {
		logrus.Infof("Cleared node NetworkUnavailable/NoRouteCreated condition for %s", origNode.Name)
	}
}

func (oc *Controller) syncNodes(nodes []interface{}) {
	logrus.Debugf("CATHY syncNodes")
	foundNodes := make(map[string]*kapi.Node)
	for _, tmp := range nodes {
		node, ok := tmp.(*kapi.Node)
		if !ok {
			logrus.Errorf("Spurious object in syncNodes: %v", tmp)
			continue
		}
		foundNodes[node.Name] = node
	}
	oc.nodeMutex.Lock()
	oc.netMutex.Lock()
	// collect all host subnet annotations for different networks, even the network does not exist yet
	for _, node := range foundNodes {
		var netName string
		for key, annotation := range node.Annotations {
			if key == OvnHostSubnet {
				netName = ""
			} else if strings.HasSuffix(key, "_"+OvnHostSubnet) {
				netName = strings.TripSuffix(key, "_"+OvnHostSubnet)
				if netName == "" {
					// unexpected annotation
					continue
				}
			} else {
				continue
			}
			oc.nodeCache[node.Name][netName] = annotation
		}
	}

	logrus.Debugf("Initialize subnet allocator for network %s", netName)
	for netName, _ := range oc.netAttchmtDefs {
		oc.initSubnetAllocator(netName)
	}
	oc.netMutex.Unlock()
	oc.nodeMutex.Unlock()

	// We only deal with cleaning up nodes that shouldn't exist here, since
	// watchNodes() will be called for all existing nodes at startup anyway.
	// Note that this list will include the 'join' cluster switch, which we
	// do not want to delete.
	nodeSwitches, stderr, err := util.RunOVNNbctl("--data=bare", "--no-heading",
		"--columns=name,external_ids", "find", "logical_switch", "other-config:subnet!=_")
	if err != nil {
		logrus.Errorf("Failed to get node logical switches: stderr: %q, error: %v",
			stderr, err)
		return
	}
	for _, result := range strings.Split(nodeSwitches, "\n\n") {
		// Split result into name and other-config
		items := strings.Split(result, "\n")
		if len(items) > 2 || len(items[0]) == 0 {
			continue
		}

		netName := ""
		if len(items) == 2 {
			netName = util.GetDbValByKey(items[1], "network_name")
		}

		// items[0] is the switch name, which should be prefixed with netName
		nodeName := items[0]
		if netName != "" {
			if !strings.HasPrefix(items[0], netName+"_") {
				logrus.Warningf("CATHYZ syncNodes Unexpected logical switch name %s: %v", items[0], result)
				continue
			}
			nodeName = strings.TrimPrefix(nodeName, netName+"_")
		}
		if nodeName == OvnJoinSwitch {
			// Don't delete the cluster switch
			continue
		}
		if _, ok := foundNodes[nodeName]; ok {
			// node still exists, no cleanup to do
			continue
		}

		if err := oc.deleteNode(nodeName); err != nil {
			logrus.Error(err)
		}
	}
}
