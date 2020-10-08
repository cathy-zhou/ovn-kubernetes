package util

import (
	"encoding/json"
	"fmt"
	"net"

	kapi "k8s.io/api/core/v1"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
)

// This handles the annotations related to subnets assigned to a node. The annotations are
// created by the master, and then read by the node. In a single-stack cluster, they look
// like:
//
//   annotations:
//     k8s.ovn.org/node-subnets: |
//       {
//         "default": "10.130.0.0/23"
//       }
//     k8s.ovn.org/node-local-nat-ip: |
//       {
//         "default": ["169.254.16.21", "fd99::10:21"]
//       }
//
// (This allows for specifying multiple network attachments, but currently only "default"
// is used.)
//
// In a dual-stack cluster, the values are lists:
//
//   annotations:
//     k8s.ovn.org/node-subnets: |
//       {
//         "default": ["10.130.0.0/23", "fd01:0:0:2::/64"]
//       }

const (
	// ovnNodeSubnets is the constant string representing the node subnets annotation key
	OvnNodeSubnets = "node-subnets"
	// ovnNodeLocalNatIP is the constant string representing the node management port's NAT IP
	// used in the case of the shared gateway mode
	ovnNodeLocalNatIP = "node-local-nat-ip"
)

func createSubnetAnnotation(annotationName string, defaultSubnets []*net.IPNet) (map[string]interface{}, error) {
	var bytes []byte
	var err error

	if len(defaultSubnets) == 1 {
		bytes, err = json.Marshal(map[string]string{
			"default": defaultSubnets[0].String(),
		})
	} else {
		defaultSubnetStrs := make([]string, len(defaultSubnets))
		for i := range defaultSubnets {
			defaultSubnetStrs[i] = defaultSubnets[i].String()
		}
		bytes, err = json.Marshal(map[string][]string{
			"default": defaultSubnetStrs,
		})
	}
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{
		annotationName: string(bytes),
	}, nil
}

func setSubnetAnnotation(nodeAnnotator kube.Annotator, annotationName string, defaultSubnets []*net.IPNet) error {
	annotation, err := createSubnetAnnotation(annotationName, defaultSubnets)
	if err != nil {
		return err
	}
	return nodeAnnotator.Set(annotationName, annotation[annotationName])
}

func parseSubnetAnnotation(node *kapi.Node, annotationName string) ([]*net.IPNet, error) {
	annotation, ok := node.Annotations[annotationName]
	if !ok {
		return nil, newAnnotationNotSetError("node %q has no %q annotation", node.Name, annotationName)
	}

	var subnets []string
	subnetsDual := make(map[string][]string)
	if err := json.Unmarshal([]byte(annotation), &subnetsDual); err == nil {
		subnets, ok = subnetsDual["default"]
	} else {
		subnetsSingle := make(map[string]string)
		if err := json.Unmarshal([]byte(annotation), &subnetsSingle); err != nil {
			return nil, fmt.Errorf("could not parse %q annotation %q as either single-stack or dual-stack",
				annotationName, annotation)
		}
		subnets = make([]string, 1)
		subnets[0], ok = subnetsSingle["default"]
	}
	if !ok {
		return nil, fmt.Errorf("%q annotation %q has no default network", annotationName, annotation)
	}

	var ipnets []*net.IPNet
	for _, subnet := range subnets {
		_, ipnet, err := net.ParseCIDR(subnet)
		if err != nil {
			return nil, fmt.Errorf("error parsing %q value: %v", annotationName, err)
		}
		ipnets = append(ipnets, ipnet)
	}

	return ipnets, nil
}

// CreateNodeHostSubnetAnnotation creates a "k8s.ovn.org/[netName_]node-subnets" annotation,
// with a single "default" network, suitable for passing to kube.SetAnnotationsOnNode
func CreateNodeHostSubnetAnnotation(defaultSubnets []*net.IPNet, netName string) (map[string]interface{}, error) {
	return createSubnetAnnotation(GetAnnotationName(OvnNodeSubnets, netName), defaultSubnets)
}

// SetNodeHostSubnetAnnotation sets a "k8s.ovn.org/[netName_]node-subnets" annotation
// using a kube.Annotator
func SetNodeHostSubnetAnnotation(nodeAnnotator kube.Annotator, defaultSubnets []*net.IPNet, netName string) error {
	return setSubnetAnnotation(nodeAnnotator, GetAnnotationName(OvnNodeSubnets, netName), defaultSubnets)
}

// DeleteNodeHostSubnetAnnotation removes a "k8s.ovn.org/[netName_]node-subnets" annotation
// using a kube.Annotator
func DeleteNodeHostSubnetAnnotation(nodeAnnotator kube.Annotator, netName string) {
	nodeAnnotator.Delete(GetAnnotationName(OvnNodeSubnets, netName))
}

// ParseNodeHostSubnetAnnotation parses the "k8s.ovn.org/node-subnets" annotation
// on a node and returns the "default" host subnet.
func ParseNodeHostSubnetAnnotation(node *kapi.Node, netName string) ([]*net.IPNet, error) {
	return parseSubnetAnnotation(node, GetAnnotationName(OvnNodeSubnets, netName))
}

// CreateNodeLocalNatAnnotation creates a "k8s.ovn.org/node-local-nat-ip" annotation,
// with a single "default" network, suitable for passing to kube.SetAnnotationsOnNode
func CreateNodeLocalNatAnnotation(nodeLocalNatIPs []net.IP) (map[string]interface{}, error) {
	nodeLocalNatIPStrs := make([]string, len(nodeLocalNatIPs))
	for i := range nodeLocalNatIPs {
		nodeLocalNatIPStrs[i] = nodeLocalNatIPs[i].String()
	}
	bytes, err := json.Marshal(map[string][]string{
		"default": nodeLocalNatIPStrs,
	})
	if err != nil {
		return nil, err
	}
	return map[string]interface{}{
		GetAnnotationName(ovnNodeLocalNatIP, types.DefaultNetworkName): string(bytes),
	}, nil
}

// SetNodeLocalNatAnnotation sets a "node-local-nat-ip" annotation
// using a kube.Annotator
func SetNodeLocalNatAnnotation(nodeAnnotator kube.Annotator, defaultNodeLocalNatIPs []net.IP) error {
	annotation, err := CreateNodeLocalNatAnnotation(defaultNodeLocalNatIPs)
	if err != nil {
		return err
	}
	key := GetAnnotationName(ovnNodeLocalNatIP, types.DefaultNetworkName)
	return nodeAnnotator.Set(key, annotation[key])
}

func ParseNodeLocalNatIPAnnotation(node *kapi.Node) ([]net.IP, error) {
	annotationJson, ok := node.Annotations[GetAnnotationName(ovnNodeLocalNatIP, types.DefaultNetworkName)]
	if !ok {
		return nil, newAnnotationNotSetError("node %q has no %q annotation", node.Name, ovnNodeLocalNatIP)
	}

	annotationMap := make(map[string][]string)
	var ips []string
	if err := json.Unmarshal([]byte(annotationJson), &annotationMap); err == nil {
		ips, ok = annotationMap["default"]
	} else {
		return nil, fmt.Errorf("could not parse %q from annotation %q for node %s",
			ovnNodeLocalNatIP, annotationJson, node.Name)
	}
	if !ok {
		return nil, fmt.Errorf("%q annotation doesn't have default network value for %s",
			annotationJson, ovnNodeLocalNatIP)
	}

	var nodeLocalNatIPs []net.IP
	for _, ip := range ips {
		nodeLocalNatIP := net.ParseIP(ip)
		if nodeLocalNatIP == nil {
			return nil, fmt.Errorf("error parsing %s's value %s: annotation is (%s)",
				ovnNodeLocalNatIP, ip, annotationJson)
		}
		nodeLocalNatIPs = append(nodeLocalNatIPs, nodeLocalNatIP)
	}

	return nodeLocalNatIPs, nil
}
