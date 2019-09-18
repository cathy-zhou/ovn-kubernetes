package util

import (
	"fmt"
	"net"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
)

// GatewayCleanup removes all the NB DB objects created for a node's gateway
func GatewayCleanup(nodeName, nodeSubnet string) error {
	// Get the cluster router
	clusterRouter, err := GetK8sClusterRouter()
	if err != nil {
		return fmt.Errorf("failed to get cluster router")
	}

	gatewayRouter := fmt.Sprintf("GR_%s", nodeName)

	// Get the gateway router port's IP address (connected to join switch)
	var routerIP, mgtPortIP string
	var nextHops []string
	routerIPNetwork, stderr, err := RunOVNNbctl("--if-exist", "get",
		"logical_router_port", "rtoj-"+gatewayRouter, "networks")
	if err != nil {
		return fmt.Errorf("Failed to get logical router port, stderr: %q, "+
			"error: %v", stderr, err)
	}

	if routerIPNetwork != "" {
		routerIPNetwork = strings.Trim(routerIPNetwork, "[]\"")
		if routerIPNetwork != "" {
			routerIP = strings.Split(routerIPNetwork, "/")[0]
		}
	}
	if routerIP != "" {
		nextHops = append(nextHops, routerIP)
	}
	mgtPortIP = getMgtPortIP(nodeSubnet)
	if mgtPortIP != "" {
		nextHops = append(nextHops, mgtPortIP)
	}
	staticRouteCleanup(clusterRouter, nextHops)

	// Remove the patch port that connects join switch to gateway router
	_, stderr, err = RunOVNNbctl("--if-exist", "lsp-del", "jtor-"+gatewayRouter)
	if err != nil {
		return fmt.Errorf("Failed to delete logical switch port jtor-%s, "+
			"stderr: %q, error: %v", gatewayRouter, stderr, err)
	}

	// Remove any gateway routers associated with nodeName
	_, stderr, err = RunOVNNbctl("--if-exist", "lr-del",
		gatewayRouter)
	if err != nil {
		return fmt.Errorf("Failed to delete gateway router %s, stderr: %q, "+
			"error: %v", gatewayRouter, stderr, err)
	}

	// Remove external switch
	externalSwitch := "ext_" + nodeName
	_, stderr, err = RunOVNNbctl("--if-exist", "ls-del",
		externalSwitch)
	if err != nil {
		return fmt.Errorf("Failed to delete external switch %s, stderr: %q, "+
			"error: %v", externalSwitch, stderr, err)
	}

	if config.Gateway.NodeportEnable {
		var k8sNSLbTCP, k8sNSLbUDP string

		//Remove the TCP, UDP load-balancers created for north-south traffic for gateway router.
		k8sNSLbTCP, k8sNSLbUDP, err = getGatewayLoadBalancers(gatewayRouter)
		if err != nil {
			return err
		}
		_, stderr, err = RunOVNNbctl("lb-del", k8sNSLbTCP)
		if err != nil {
			return fmt.Errorf("Failed to delete Gateway router TCP load balancer %s, stderr: %q, "+
				"error: %v", k8sNSLbTCP, stderr, err)
		}
		_, stderr, err = RunOVNNbctl("lb-del", k8sNSLbUDP)
		if err != nil {
			return fmt.Errorf("Failed to delete Gateway router UDP load balancer %s, stderr: %q, "+
				"error: %v", k8sNSLbTCP, stderr, err)
		}
	}

	// We don't know the gateway mode as this is running in the master, try to delete the additional local
	// gateway for the shared gateway mode. it will be no op if this is done for other gateway modes.
	err = localGatewayCleanup(nodeName, clusterRouter)
	if err != nil {
		return err
	}
	return nil
}

// localGatewayCleanup removes all the NB DB objects created for a node's local only gateway
func localGatewayCleanup(nodeName, clusterRouter string) error {
	gatewayRouter := fmt.Sprintf("GR_local_%s", nodeName)

	// Get the gateway router port's IP address (connected to join switch)
	var routerIP string
	routerIPNetwork, stderr, err := RunOVNNbctl("--if-exist", "get",
		"logical_router_port", "rtoj-"+gatewayRouter, "networks")
	if err != nil {
		return fmt.Errorf("Failed to get logical router port, stderr: %q, "+
			"error: %v", stderr, err)
	}

	if routerIPNetwork != "" {
		routerIPNetwork = strings.Trim(routerIPNetwork, "[]\"")
		if routerIPNetwork != "" {
			routerIP = strings.Split(routerIPNetwork, "/")[0]
			if routerIP != "" {
				staticRouteCleanup(clusterRouter, []string{routerIP})
			}
		}
	}

	// Remove the patch port that connects join switch to gateway router
	_, stderr, err = RunOVNNbctl("--if-exist", "lsp-del", "jtor-"+gatewayRouter)
	if err != nil {
		return fmt.Errorf("Failed to delete logical switch port jtor-%s, "+
			"stderr: %q, error: %v", gatewayRouter, stderr, err)
	}

	// Remove any gateway routers associated with nodeName
	_, stderr, err = RunOVNNbctl("--if-exist", "lr-del",
		gatewayRouter)
	if err != nil {
		return fmt.Errorf("Failed to delete gateway router %s, stderr: %q, "+
			"error: %v", gatewayRouter, stderr, err)
	}

	// Remove external switch
	externalSwitch := "ext_local_" + nodeName
	_, stderr, err = RunOVNNbctl("--if-exist", "ls-del",
		externalSwitch)
	if err != nil {
		return fmt.Errorf("Failed to delete external switch %s, stderr: %q, "+
			"error: %v", externalSwitch, stderr, err)
	}
	return nil
}

func staticRouteCleanup(clusterRouter string, nextHops []string) {
	for _, nextHop := range nextHops {
		// Get a list of all the routes in cluster router with the next hop IP.
		var uuids string
		uuids, stderr, err := RunOVNNbctl("--data=bare", "--no-heading",
			"--columns=_uuid", "find", "logical_router_static_route",
			"nexthop="+nextHop)
		if err != nil {
			logrus.Errorf("Failed to fetch all routes with "+
				"IP %s as nexthop, stderr: %q, "+
				"error: %v", nextHop, stderr, err)
			continue
		}

		// Remove all the routes in cluster router with this IP as the nexthop.
		routes := strings.Fields(uuids)
		for _, route := range routes {
			_, stderr, err = RunOVNNbctl("--if-exists", "remove",
				"logical_router", clusterRouter, "static_routes", route)
			if err != nil {
				logrus.Errorf("Failed to delete static route %s"+
					", stderr: %q, err = %v", route, stderr, err)
				continue
			}
		}
	}
}

func getMgtPortIP(nodeSubnet string) string {
	ip, _, err := net.ParseCIDR(nodeSubnet)
	if err != nil {
		logrus.Errorf("Failed to parse local subnet %s: %v", nodeSubnet, err)
		return ""
	}
	//Get the fixed second IP for the management Port
	ip = NextIP(NextIP(ip))
	return ip.String()
}
