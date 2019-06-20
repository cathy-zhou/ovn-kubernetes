package cluster

import (
	"fmt"
	"net"
	"strings"

	kapi "k8s.io/api/core/v1"
	"k8s.io/client-go/tools/cache"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	"github.com/openshift/origin/pkg/util/netutils"
	"github.com/sirupsen/logrus"
)

// RebuildOVNDatabase rebuilds db if HA option is enabled and OVN DB
// doesn't exist. First It updates k8s nodes by creating a logical
// switch for each node. Then it reads all resources from k8s and
// creates needed resources in OVN DB. Last, it updates master node
// ip in default namespace to trigger event on node.
func (cluster *OvnClusterController) RebuildOVNDatabase(
	masterNodeName string, oc *ovn.Controller, networkName string) error {
	logrus.Debugf("Rebuild OVN database for cluster nodes")
	var err error
	ipChange, err := cluster.checkMasterIPChange(masterNodeName)
	if err != nil {
		logrus.Errorf("Error when checking Master Node IP Change: %v", err)
		return err
	}

	// If OvnHA options is enabled, make sure default namespace has the
	// annotation of current cluster master node's overlay IP address
	logrus.Debugf("cluster.OvnHA: %t", cluster.OvnHA)
	if cluster.OvnHA && ipChange {
		logrus.Debugf("HA is enabled and DB doesn't exist!")
		// Rebuild OVN DB for the k8s nodes
		err = cluster.UpdateDBForKubeNodes(masterNodeName, networkName)
		if err != nil {
			return err
		}
		// Rebuild OVN DB for the k8s namespaces and all the resource
		// objects inside the namespace including pods and network
		// policies
		err = cluster.UpdateKubeNsObjects(oc)
		if err != nil {
			return err
		}
		// Update master node IP annotation on default namespace
		err = cluster.UpdateMasterNodeIP(masterNodeName)
		if err != nil {
			return err
		}
	}

	return nil
}

// UpdateDBForKubeNodes rebuilds ovn db for k8s nodes
func (cluster *OvnClusterController) UpdateDBForKubeNodes(
	masterNodeName string, networkName string) error {
	nodes, err := cluster.Kube.GetNodes()
	if err != nil {
		logrus.Errorf("Failed to obtain k8s nodes: %v", err)
		return err
	}
	prefix := util.GetNetworkPrefix(networkName)
	for _, node := range nodes.Items {
		subnetStr, ok := node.Annotations[prefix+OvnHostSubnet]
		if ok {
			ip, subnet, err := net.ParseCIDR(subnetStr)
			if err != nil {
				return fmt.Errorf("Failed to parse subnet %s: %v", subnetStr, err)
			}
			subnet.IP = util.NextIP(ip)
			err = cluster.ensureNodeLogicalNetwork(node.Name, subnet, networkName)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// UpdateKubeNsObjects rebuilds ovn db for k8s namespaces
// and pods/networkpolicies inside the namespaces.
func (cluster *OvnClusterController) UpdateKubeNsObjects(
	oc *ovn.Controller) error {
	namespaces, err := cluster.Kube.GetNamespaces()
	if err != nil {
		logrus.Errorf("Failed to get k8s namespaces: %v", err)
		return err
	}
	for _, ns := range namespaces.Items {
		oc.AddNamespace(&ns)
		pods, err := cluster.Kube.GetPods(ns.Name)
		if err != nil {
			logrus.Errorf("Failed to get k8s pods: %v", err)
			return err
		}
		for _, pod := range pods.Items {
			oc.AddLogicalPortWithIP(&pod)
		}
		endpoints, err := cluster.Kube.GetEndpoints(ns.Name)
		if err != nil {
			logrus.Errorf("Failed to get k8s endpoints: %v", err)
			return err
		}
		for _, ep := range endpoints.Items {
			er := oc.AddEndpoints(&ep)
			if er != nil {
				logrus.Errorf("Error adding endpoints: %v", er)
				return er
			}
		}
		policies, err := cluster.Kube.GetNetworkPolicies(ns.Name)
		if err != nil {
			logrus.Errorf("Failed to get k8s network policies: %v", err)
			return err
		}
		for _, policy := range policies.Items {
			oc.AddNetworkPolicy(&policy)
		}
	}
	return nil
}

// UpdateMasterNodeIP add annotations of master node IP on
// default namespace
func (cluster *OvnClusterController) UpdateMasterNodeIP(
	masterNodeName string) error {
	masterNodeIP, err := netutils.GetNodeIP(masterNodeName)
	if err != nil {
		return fmt.Errorf("Failed to obtain local IP from master node "+
			"%q: %v", masterNodeName, err)
	}

	defaultNs, err := cluster.Kube.GetNamespace(DefaultNamespace)
	if err != nil {
		return fmt.Errorf("Failed to get default namespace: %v", err)
	}

	// Get overlay ip on OVN master node from default namespace. If it
	// doesn't have it or the IP address is different than the current one,
	// set it with current master overlay IP.
	masterIP, ok := defaultNs.Annotations[MasterOverlayIP]
	if !ok || masterIP != masterNodeIP {
		err := cluster.Kube.SetAnnotationOnNamespace(defaultNs, MasterOverlayIP,
			masterNodeIP)
		if err != nil {
			return fmt.Errorf("Failed to set %s=%s on namespace %s: %v",
				MasterOverlayIP, masterNodeIP, defaultNs.Name, err)
		}
	}

	return nil
}

func (cluster *OvnClusterController) checkMasterIPChange(
	masterNodeName string) (bool, error) {
	// Check DB existence by checking if the default namespace annotated
	// IP address is the same as the master node IP. If annotated IP
	// address is different, we assume that the ovn db is crashed on the
	// old node and needs to be rebuilt on the new master node.
	masterNodeIP, err := netutils.GetNodeIP(masterNodeName)
	if err != nil {
		return false, fmt.Errorf("Failed to obtain local IP from master "+
			"node %q: %v", masterNodeName, err)
	}

	defaultNs, err := cluster.Kube.GetNamespace(DefaultNamespace)
	if err != nil {
		return false, fmt.Errorf("Failed to get default namespace: %v", err)
	}

	// Get overlay ip on OVN master node from default namespace. If the IP
	// address is different than master node IP, return true. Else, return
	// false.
	masterIP := defaultNs.Annotations[MasterOverlayIP]
	logrus.Debugf("Master IP: %s, Annotated IP: %s", masterNodeIP, masterIP)
	if masterIP != masterNodeIP {
		logrus.Debugf("Detected Master node IP is different than default " +
			"namespae annotated IP.")
		return true, nil
	}
	return false, nil
}

// StartClusterMaster runs a subnet IPAM and a controller that watches arrival/departure
// of nodes in the cluster
// On an addition to the cluster (node create), a new subnet is created for it that will translate
// to creation of a logical switch (done by the node, but could be created here at the master process too)
// Upon deletion of a node, the switch will be deleted
//
// TODO: Verify that the cluster was not already called with a different global subnet
//  If true, then either quit or perform a complete reconfiguration of the cluster (recreate switches/routers with new subnet values)
func (cluster *OvnClusterController) StartClusterMaster(masterNodeName string) error {

	existingNodes, err := cluster.Kube.GetNodes()
	if err != nil {
		logrus.Errorf("Error in initializing/fetching subnets: %v", err)
		return err
	}
	for _, networkName := range cluster.NetworkNameList {
		prefix := util.GetNetworkPrefix(networkName)

		alreadyAllocated := make([]string, 0)
		for _, node := range existingNodes.Items {
			hostsubnet, ok := node.Annotations[prefix+OvnHostSubnet]
			if ok {
				alreadyAllocated = append(alreadyAllocated, hostsubnet)
			}
		}
		masterSubnetAllocatorList := make([]*netutils.SubnetAllocator, 0)
		// NewSubnetAllocator is a subnet IPAM, which takes a CIDR (first argument)
		// and gives out subnets of length 'hostSubnetLength' (second argument)
		// but omitting any that exist in 'subrange' (third argument)
		for _, clusterEntry := range cluster.ClusterIPNet[networkName] {
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
			subnetAllocator, err := netutils.NewSubnetAllocator(clusterEntry.CIDR.String(), 32-clusterEntry.HostSubnetLength, subrange)
			if err != nil {
				return err
			}
			masterSubnetAllocatorList = append(masterSubnetAllocatorList, subnetAllocator)
		}
		cluster.masterSubnetAllocatorList[networkName] = masterSubnetAllocatorList

		if err := cluster.SetupMaster(masterNodeName, networkName); err != nil {
			logrus.Errorf("Failed to setup master (%v) for network %s", err, networkName)
			return err
		}
	}

	// Watch all node events.  On creation, addNode will be called that will
	// create a subnet for the switch belonging to that node. On a delete
	// call, the subnet will be returned to the allocator as the switch is
	// deleted from ovn
	return cluster.watchNodes()
}

// SetupMaster creates the central router and load-balancers (default network only) for the network
func (cluster *OvnClusterController) SetupMaster(masterNodeName string, networkName string) error {

	if err := setupOVNMaster(masterNodeName); err != nil {
		return err
	}

	prefix := util.GetNetworkPrefix(networkName)

	// Create a single common distributed router for the network.
	stdout, stderr, err := util.RunOVNNbctl("--", "--may-exist", "lr-add", prefix + OvnClusterRouter,
		"--", "set", "logical_router", prefix + OvnClusterRouter, "external_ids:k8s-cluster-router="+prefix+"yes")
	if err != nil {
		logrus.Errorf("Failed to create a single common distributed router for the cluster network %s, "+
			"stdout: %q, stderr: %q, error: %v", networkName, stdout, stderr, err)
		return err
	}

	// Create 2 load-balancers for east-west traffic for the default network.  One handles UDP and another handles TCP.
	if networkName == "default" {
		cluster.TCPLoadBalancerUUID, stderr, err = util.RunOVNNbctl("--data=bare", "--no-heading", "--columns=_uuid", "find", "load_balancer", "external_ids:k8s-cluster-lb-tcp=yes")
		if err != nil {
			logrus.Errorf("Failed to get tcp load-balancer, stderr: %q, error: %v", stderr, err)
			return err
		}

		if cluster.TCPLoadBalancerUUID == "" {
			cluster.TCPLoadBalancerUUID, stderr, err = util.RunOVNNbctl("--", "create", "load_balancer", "external_ids:k8s-cluster-lb-tcp=yes", "protocol=tcp")
			if err != nil {
				logrus.Errorf("Failed to create tcp load-balancer, stdout: %q, stderr: %q, error: %v", stdout, stderr, err)
				return err
			}
		}

		cluster.UDPLoadBalancerUUID, stderr, err = util.RunOVNNbctl("--data=bare", "--no-heading", "--columns=_uuid", "find", "load_balancer", "external_ids:k8s-cluster-lb-udp=yes")
		if err != nil {
			logrus.Errorf("Failed to get udp load-balancer, stderr: %q, error: %v", stderr, err)
			return err
		}
		if cluster.UDPLoadBalancerUUID == "" {
			cluster.UDPLoadBalancerUUID, stderr, err = util.RunOVNNbctl("--", "create", "load_balancer", "external_ids:k8s-cluster-lb-udp=yes", "protocol=udp")
			if err != nil {
				logrus.Errorf("Failed to create udp load-balancer, stdout: %q, stderr: %q, error: %v", stdout, stderr, err)
				return err
			}
		}
	}

	// Create a logical switch called "join" that will be used to connect gateway routers to the distributed router.
	// The "join" switch will be allocated IP addresses in the range 100.64.0.0/16.
	var joinSubnet string
	joinSubnet = "100.64.0.1/16"
	joinIP, joinCIDR, _ := net.ParseCIDR(joinSubnet)
	stdout, stderr, err = util.RunOVNNbctl("--may-exist", "ls-add", prefix + "join",
		"--", "set", "logical_switch", prefix + "join", fmt.Sprintf("other-config:subnet=%s", joinCIDR.String()),
		"--", "set", "logical_switch", prefix + "join", fmt.Sprintf("other-config:exclude_ips=%s", joinIP.String()))
	if err != nil {
		logrus.Errorf("Failed to create logical switch called \"%s\", stdout: %q, stderr: %q, error: %v", prefix + "join", stdout, stderr, err)
		return err
	}

	// Connect the distributed router to "join".
	routerMac, stderr, err := util.RunOVNNbctl("--if-exist", "get", "logical_router_port", "rtoj-"+prefix+OvnClusterRouter, "mac")
	if err != nil {
		logrus.Errorf("Failed to get logical router port rtoj-%s%v, stderr: %q, error: %v", prefix, OvnClusterRouter, stderr, err)
		return err
	}
	if routerMac == "" {
		routerMac = util.GenerateMac()
		stdout, stderr, err = util.RunOVNNbctl("--", "--may-exist", "lrp-add", prefix+OvnClusterRouter,
			"rtoj-"+prefix+OvnClusterRouter, routerMac, joinSubnet)
		if err != nil {
			logrus.Errorf("Failed to add logical router port rtoj-%v%v, stdout: %q, stderr: %q, error: %v",
				prefix, OvnClusterRouter, stdout, stderr, err)
			return err
		}
	}

	// Connect the switch "join" to the router.
	stdout, stderr, err = util.RunOVNNbctl("--", "--may-exist", "lsp-add", prefix+"join", "jtor-"+prefix+OvnClusterRouter,
		"--", "set", "logical_switch_port", "jtor-"+prefix+OvnClusterRouter, "type=router",
		"options:router-port=rtoj-"+prefix+OvnClusterRouter, "addresses="+"\""+routerMac+"\"")
	if err != nil {
		logrus.Errorf("Failed to add router-type logical switch port to %s, stdout: %q, stderr: %q, error: %v",
			prefix+"join", stdout, stderr, err)
		return err
	}

	return nil
}

func parseNodeHostSubnet(node *kapi.Node, networkName string) (*net.IPNet, error) {
	var annotationName string

	prefix := util.GetNetworkPrefix(networkName)
	annotationName = prefix+OvnHostSubnet

	sub, ok := node.Annotations[annotationName]
	if !ok {
		return nil, fmt.Errorf("Error in obtaining host subnet for node %q network %s for deletion", node.Name, networkName)
	}

	_, subnet, err := net.ParseCIDR(sub)
	if err != nil {
		return nil, fmt.Errorf("Error in parsing hostsubnet - %v", err)
	}

	return subnet, nil
}

func (cluster *OvnClusterController) ensureNodeHostSubnet(node *kapi.Node, networkName string) (*net.IPNet, *netutils.SubnetAllocator, error) {
	// Do not create a subnet if the node already has a subnet
	subnet, _ := parseNodeHostSubnet(node, networkName)
	if subnet != nil {
		return subnet, nil, nil
	}

	// Create new subnet
	for _, possibleSubnet := range cluster.masterSubnetAllocatorList[networkName] {
		sn, err := possibleSubnet.GetNetwork()
		if err == netutils.ErrSubnetAllocatorFull {
			// Current subnet exhausted, check next possible subnet
			continue
		} else if err != nil {
			return nil, nil, fmt.Errorf("Error allocating network for node %s network %s: %v", node.Name, networkName, err)
		}

		// Success
		logrus.Infof("Allocated node %s network %s HostSubnet %s", node.Name, networkName, sn.String())
		return sn, possibleSubnet, nil
	}
	return nil, nil, fmt.Errorf("error allocating network for node %s network %s: No more allocatable ranges", node.Name, networkName)
}

func (cluster *OvnClusterController) ensureNodeLogicalNetwork(nodeName string, hostsubnet *net.IPNet, networkName string) error {
	ip := util.NextIP(hostsubnet.IP)
	n, _ := hostsubnet.Mask.Size()
	firstIP := fmt.Sprintf("%s/%d", ip.String(), n)
	prefix := util.GetNetworkPrefix(networkName)

	nodeLRPMac, stderr, err := util.RunOVNNbctl("--if-exist", "get", "logical_router_port", "rtos-"+prefix+nodeName, "mac")
	if err != nil {
		logrus.Errorf("Failed to get logical router port rtos-%v%v,stderr: %q, error: %v", prefix, nodeName, stderr, err)
		return err
	}
	if nodeLRPMac == "" {
		nodeLRPMac = util.GenerateMac()
	}

	// Create a router port and provide it the first address on the node's host subnet
	_, stderr, err = util.RunOVNNbctl("--may-exist", "lrp-add", prefix+OvnClusterRouter, "rtos-"+prefix+nodeName, nodeLRPMac, firstIP)
	if err != nil {
		logrus.Errorf("Failed to add logical port to router, stderr: %q, error: %v", stderr, err)
		return err
	}

	// Skip the second address of the LogicalSwitch's subnet since we set it aside for the
	// management port on that node.
	secondIP := util.NextIP(ip)

	// Create a logical switch and set its subnet.
	stdout, stderr, err := util.RunOVNNbctl("--", "--may-exist", "ls-add", prefix+nodeName,
		"--", "set", "logical_switch", prefix+nodeName, "other-config:subnet="+hostsubnet.String(),
		"other-config:exclude_ips="+secondIP.String(),
		"external-ids:gateway_ip="+firstIP, "external-ids:net_nm="+networkName)
	if err != nil {
		logrus.Errorf("Failed to create a logical switch prefix%v, stdout: %q, stderr: %q, error: %v", prefix, nodeName, stdout, stderr, err)
		return err
	}

	// Connect the switch to the router.
	stdout, stderr, err = util.RunOVNNbctl("--", "--may-exist", "lsp-add", prefix+nodeName, "stor-"+prefix+nodeName,
		"--", "set", "logical_switch_port", "stor-"+prefix+nodeName, "type=router", "options:router-port=rtos-"+prefix+nodeName, "addresses="+"\""+nodeLRPMac+"\"")
	if err != nil {
		logrus.Errorf("Failed to add logical port stro-%v%v to switch, stdout: %q, stderr: %q, error: %v", prefix, nodeName, stdout, stderr, err)
		return err
	}

	// Add our cluster TCP and UDP load balancers to the node switch
	if networkName != "default" {
		return nil
	}

	if cluster.TCPLoadBalancerUUID == "" {
		return fmt.Errorf("TCP cluster load balancer not created")
	}
	stdout, stderr, err = util.RunOVNNbctl("set", "logical_switch", nodeName, "load_balancer="+cluster.TCPLoadBalancerUUID)
	if err != nil {
		logrus.Errorf("Failed to set logical switch %v's loadbalancer, stdout: %q, stderr: %q, error: %v", nodeName, stdout, stderr, err)
		return err
	}

	if cluster.UDPLoadBalancerUUID == "" {
		return fmt.Errorf("UDP cluster load balancer not created")
	}
	stdout, stderr, err = util.RunOVNNbctl("add", "logical_switch", nodeName, "load_balancer", cluster.UDPLoadBalancerUUID)
	if err != nil {
		logrus.Errorf("Failed to add logical switch %v's loadbalancer, stdout: %q, stderr: %q, error: %v", nodeName, stdout, stderr, err)
		return err
	}

	return nil
}

func (cluster *OvnClusterController) addNode(node *kapi.Node) (err error) {
	var hostsubnet *net.IPNet
	var subnetAllocator *netutils.SubnetAllocator
	var annotationName string

	for _, networkName := range cluster.NetworkNameList {
		prefix := util.GetNetworkPrefix(networkName)
		annotationName = prefix + OvnHostSubnet

		hostsubnet, subnetAllocator, err = cluster.ensureNodeHostSubnet(node, networkName)
		if err != nil {
			return err
		}

		defer func() {
			if err != nil && subnetAllocator != nil {
				_ = subnetAllocator.ReleaseNetwork(hostsubnet)
			}
		}()

		// Ensure that the node's logical network has been created
		err = cluster.ensureNodeLogicalNetwork(node.Name, hostsubnet, networkName)
		if err != nil {
			return err
		}

		// Set the HostSubnet annotation on the node object to signal
		// to nodes that their logical infrastructure is set up and they can
		// proceed with their initialization
		err = cluster.Kube.SetAnnotationOnNode(node, annotationName, hostsubnet.String())
		if err != nil {
			logrus.Errorf("Failed to set node %s network %s host subnet annotation: %v", node.Name, networkName, hostsubnet.String())
			return err
		}
	}

	return nil
}

func (cluster *OvnClusterController) deleteNodeHostSubnet(nodeName string, subnet *net.IPNet, networkName string) error {
	for _, possibleSubnet := range cluster.masterSubnetAllocatorList[networkName] {
		if err := possibleSubnet.ReleaseNetwork(subnet); err == nil {
			logrus.Infof("Deleted HostSubnet %v for node %s network %s", subnet, nodeName, networkName)
			return nil
		}
	}
	// SubnetAllocator.network is an unexported field so the only way to figure out if a subnet is in a network is to try and delete it
	// if deletion succeeds then stop iterating, if the list is exhausted the node subnet wasn't deleteted return err
	return fmt.Errorf("Error deleting subnet %v for node %q network %s: subnet not found in any CIDR range or already available", subnet, nodeName, networkName)
}

func (cluster *OvnClusterController) deleteNodeLogicalNetwork(nodeName string, networkName string) error {
	prefix := util.GetNetworkPrefix(networkName)
	// Remove the logical switch associated with the node
	if _, stderr, err := util.RunOVNNbctl("--if-exist", "ls-del", prefix+nodeName); err != nil {
		return fmt.Errorf("Failed to delete logical switch %s%s, "+
			"stderr: %q, error: %v", prefix, nodeName, stderr, err)
	}

	// Remove the patch port that connects distributed router to node's logical switch
	if _, stderr, err := util.RunOVNNbctl("--if-exist", "lrp-del", "rtos-"+prefix+nodeName); err != nil {
		return fmt.Errorf("Failed to delete logical router port rtos-%s%s, "+
			"stderr: %q, error: %v", prefix, nodeName, stderr, err)
	}

	return nil
}

func (cluster *OvnClusterController) deleteNode(nodeName string, nodeSubnet *net.IPNet, networkName string) error {
	// Clean up as much as we can but don't hard error
	if nodeSubnet != nil {
		if err := cluster.deleteNodeHostSubnet(nodeName, nodeSubnet, networkName); err != nil {
			logrus.Errorf("Error deleting node %s HostSubnet: %v", nodeName, err)
		}
	}

	if err := cluster.deleteNodeLogicalNetwork(nodeName, networkName); err != nil {
		logrus.Errorf("Error deleting node %s logical network: %v", nodeName, err)
	}

	if err := util.GatewayCleanup(nodeName, cluster.NodePortEnable[networkName], networkName); err != nil {
		return fmt.Errorf("Failed to clean up node %s gateway: (%v)", nodeName, err)
	}

	return nil
}

func (cluster *OvnClusterController) syncNodes(nodes []interface{}) {
	foundNodes := make(map[string]*kapi.Node)
	for _, tmp := range nodes {
		node, ok := tmp.(*kapi.Node)
		if !ok {
			logrus.Errorf("Spurious object in syncNodes: %v", tmp)
			continue
		}
		foundNodes[node.Name] = node
	}

	// We only deal with cleaning up nodes that shouldn't exist here, since
	// watchNodes() will be called for all existing nodes at startup anyway.
	// Note that this list will include the 'join' cluster switch, which we
	// do not want to delete.
	nodeSwitches, stderr, err := util.RunOVNNbctl("--data=bare", "--no-heading",
		"--columns=name,other-config", "find", "logical_switch", "external-ids:net_nm!=_")
	if err != nil {
		logrus.Errorf("Failed to get node logical switches: stderr: %q, error: %v",
			stderr, err)
		return
	}
	for _, result := range strings.Split(nodeSwitches, "\n\n") {
		// Split result into name and other-config
		items := strings.Split(result, "\n")
		if len(items) != 2 || len(items[0]) == 0 {
			continue
		}
		networkName, stderr, err := util.RunOVNNbctl("get",
			"logical_switch", items[0], "external-ids:net_nm")
		if err != nil {
			logrus.Errorf("Error obtaining network Name for switch %s, stderr: %q (%v)",
				items[0], stderr, err)
			continue
		}
		nodeName := items[0]
		if networkName != "default" {
			prefix := util.GetNetworkPrefix(networkName)
			if strings.HasPrefix(items[0], prefix) {
				logrus.Errorf("Incorrect network Name %s for switch %s", networkName, items[0])
				continue
			}
			nodeName = strings.TrimPrefix(items[0], prefix)
		}
		if _, ok := foundNodes[nodeName]; ok {
			// node still exists, no cleanup to do
			continue
		}
		var subnet *net.IPNet
		if strings.HasPrefix(items[1], "subnet=") {
			subnetStr := strings.TrimPrefix(items[1], "subnet=")
			_, subnet, _ = net.ParseCIDR(subnetStr)
		}

		if err := cluster.deleteNode(nodeName, subnet, networkName); err != nil {
			logrus.Error(err)
		}
	}
}

func (cluster *OvnClusterController) watchNodes() error {
	_, err := cluster.watchFactory.AddNodeHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			node := obj.(*kapi.Node)
			logrus.Debugf("Added event for Node %q", node.Name)
			err := cluster.addNode(node)
			if err != nil {
				logrus.Errorf("error creating subnet for node %s: %v", node.Name, err)
			}
		},
		UpdateFunc: func(old, new interface{}) {},
		DeleteFunc: func(obj interface{}) {
			node := obj.(*kapi.Node)
			logrus.Debugf("Delete event for Node %q", node.Name)
			for _, networkName := range cluster.NetworkNameList {
				nodeSubnet, _ := parseNodeHostSubnet(node, networkName)
				err := cluster.deleteNode(node.Name, nodeSubnet, networkName)
				if err != nil {
					logrus.Error(err)
				}
			}
		},
	}, cluster.syncNodes)
	return err
}
