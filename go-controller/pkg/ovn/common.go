package ovn

import (
	"fmt"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	util "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	"k8s.io/klog/v2"
)

// hash the provided input to make it a valid portGroup name.
func hashedPortGroup(s string) string {
	return util.HashForOVN(s)
}

func createPortGroup(name, hashName, netName string) (string, error) {
	klog.V(5).Infof("createPortGroup with %s for network %s", name, netName)
	hashName = util.GetNetworkPrefix(netName) + hashName
	portGroup, stderr, err := util.RunOVNNbctl("--data=bare",
		"--no-heading", "--columns=_uuid", "find", "port_group",
		fmt.Sprintf("name=%s", hashName))
	if err != nil {
		return "", fmt.Errorf("find failed to get port_group, stderr: %q (%v)",
			stderr, err)
	}

	if portGroup != "" {
		return portGroup, nil
	}

	cmdArgs := []string{"create", "port_group",
		fmt.Sprintf("name=%s", hashName),
		fmt.Sprintf("external-ids:name=%s", name)}
	if netName != types.DefaultNetworkName {
		cmdArgs = append(cmdArgs, fmt.Sprintf("external-ids:network_name=%s", netName))
	}
	portGroup, stderr, err = util.RunOVNNbctl(cmdArgs...)
	if err != nil {
		return "", fmt.Errorf("failed to create port_group %s, "+
			"stderr: %q (%v)", name, stderr, err)
	}

	return portGroup, nil
}

func deletePortGroup(hashName, netName string) {
	klog.V(5).Infof("deletePortGroup %s for network %s", hashName, netName)

	hashName = util.GetNetworkPrefix(netName) + hashName
	portGroup, stderr, err := util.RunOVNNbctl("--data=bare",
		"--no-heading", "--columns=_uuid", "find", "port_group",
		fmt.Sprintf("name=%s", hashName))
	if err != nil {
		klog.Errorf("Find failed to get port_group, stderr: %q (%v)",
			stderr, err)
		return
	}

	if portGroup == "" {
		return
	}

	_, stderr, err = util.RunOVNNbctl("--if-exists", "destroy",
		"port_group", portGroup)
	if err != nil {
		klog.Errorf("Failed to destroy port_group %s, stderr: %q, (%v)",
			hashName, stderr, err)
		return
	}
}

func stringSliceMembership(slice []string, key string) bool {
	for _, val := range slice {
		if val == key {
			return true
		}
	}
	return false
}
