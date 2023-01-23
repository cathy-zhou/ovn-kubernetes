package node

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/node/controllers/upgrade"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	"k8s.io/klog/v2"
)

// updateIsOvnUpEnabled checks if ovnkube-master has been upgraded from OvnPortBindingTopoVersion
// and start to support port binding up field. If so update atomicOvnUpEnabled accordingly.
func (bnnc *BaseNodeNetworkController) updateIsOvnUpEnabled(ctx context.Context) error {
	isOvnUpEnabled := atomic.LoadInt32(&bnnc.atomicOvnUpEnabled) > 0
	if config.OvnKubeNode.Mode != types.NodeModeDPU || isOvnUpEnabled || config.OvnKubeNode.DisableOVNIfaceIdVer {
		return nil
	}
	upgradeController := upgrade.NewController(bnnc.client, bnnc.watchFactory)
	initialTopoVersion, err := upgradeController.GetTopologyVersion(ctx)
	if err != nil {
		return fmt.Errorf("failed to get initial topology version: %w", err)
	}

	klog.Infof("Current control-plane topology version is %d", initialTopoVersion)

	// need to run upgrade controller
	go func() {
		if err := upgradeController.WaitForTopologyVersion(ctx, bnnc.stopChan, types.OvnCurrentTopologyVersion, 30*time.Minute); err != nil {
			klog.Fatalf("Error while waiting for Topology Version to be updated: %v", err)
		}
		// ensure CNI support for port binding built into OVN, as masters have been upgraded
		if initialTopoVersion < types.OvnPortBindingTopoVersion {
			isOvnUpEnabled, err := util.GetOVNIfUpCheckMode()
			if err != nil {
				klog.Errorf("%v", err)
			} else if isOvnUpEnabled {
				klog.Infof("Detected support for port binding with external IDs")
				atomic.StoreInt32(&bnnc.atomicOvnUpEnabled, 1)
			}
		}
	}()
	return nil
}

// checkForStaleOVSRepresentorInterfaces checks for stale OVS ports backed by Repreresentor interfaces,
// derive iface-id from pod name and namespace then remove any interfaces assoicated with a sandbox that are
// not scheduled to the node.
func (bnnc *BaseNodeNetworkController) checkForStaleOVSRepresentorInterfaces() {
	// Get all ovn-kuberntes Pod interfaces. these are OVS interfaces that have their external_ids:sandbox set.
	ovsArgs := []string{"--columns=name,external_ids", "--data=bare", "--no-headings",
		"--format=csv", "find", "Interface", "external_ids:sandbox!=\"\"", "external_ids:vf-netdev-name!=\"\""}
	if bnnc.IsSecondary() {
		ovsArgs = append(ovsArgs, fmt.Sprintf("external_ids:%s=%s", types.NetworkExternalID, bnnc.GetNetworkName()))
	} else {
		ovsArgs = append(ovsArgs, fmt.Sprintf("external_ids:%s{=}[]", types.NetworkExternalID))
	}

	out, stderr, err := util.RunOVSVsctl(ovsArgs...)
	if err != nil {
		klog.Errorf("Failed to list ovn-k8s OVS interfaces:, stderr: %q, error: %v", stderr, err)
		return
	}

	if out == "" {
		return
	}

	// parse this data into local struct
	type interfaceInfo struct {
		Name       string
		Attributes map[string]string
	}

	lines := strings.Split(out, "\n")
	interfaceInfos := make([]*interfaceInfo, 0, len(lines))
	for _, line := range lines {
		cols := strings.Split(line, ",")
		// Note: There are exactly 2 column entries as requested in the ovs query
		// Col 0: interface name
		// Col 1: space separated key=val pairs of external_ids attributes
		if len(cols) < 2 {
			// should never happen
			klog.Errorf("Unexpected output: %s, expect \"<name>,<external_ids\"", line)
			continue
		}
		ifcInfo := interfaceInfo{Name: strings.TrimSpace(cols[0]), Attributes: make(map[string]string)}
		if cols[1] != "" {
			for _, attr := range strings.Split(cols[1], " ") {
				keyVal := strings.SplitN(attr, "=", 2)
				if len(keyVal) != 2 {
					// should never happen
					klog.Errorf("Unexpected output: %s, expect \"<key>=<value>\"", attr)
					continue
				}
				ifcInfo.Attributes[keyVal[0]] = keyVal[1]
			}
		}
		interfaceInfos = append(interfaceInfos, &ifcInfo)
	}

	if len(interfaceInfos) == 0 {
		return
	}

	// list Pods and calculate the expected iface-ids.
	// Note: we do this after scanning ovs interfaces to avoid deleting ports of pods that where just scheduled
	// on the node.
	pods, err := bnnc.watchFactory.GetPods("")
	if err != nil {
		klog.Errorf("Failed to list pods. %v", err)
		return
	}
	expectedIfaceIds := make(map[string]bool)
	for _, pod := range pods {
		// Note: wf (WatchFactory) *usually* returns pods assigned to this node, however we dont rely on it
		// and add this check to filter out pods assigned to other nodes. (e.g when ovnkube master and node
		// share the same process)
		if pod.Spec.NodeName != bnnc.name || util.PodWantsHostNetwork(pod) {
			continue
		}
		if !bnnc.IsSecondary() {
			ifaceID := util.GetIfaceId(pod.Namespace, pod.Name)
			expectedIfaceIds[ifaceID] = true
		} else {
			on, networkMap, err := util.GetPodNADToNetworkMapping(pod, bnnc.NetInfo)
			if err != nil || !on {
				if err != nil {
					klog.Warningf("Error getting network-attachment for pod %s/%s network %s: %v",
						pod.Namespace, pod.Name, bnnc.GetNetworkName(), err)
				}
				continue
			}
			for nadName := range networkMap {
				ifaceID := util.GetSecondaryNetworkIfaceId(pod.Namespace, pod.Name, nadName)
				expectedIfaceIds[ifaceID] = true
			}
		}
	}

	// Remove any stale representor ports
	for _, ifaceInfo := range interfaceInfos {
		ifaceId, ok := ifaceInfo.Attributes["iface-id"]
		if !ok {
			klog.Warningf("iface-id attribute was not found for OVS interface %s. "+
				"skipping cleanup check for interface", ifaceInfo.Name)
			continue
		}
		if _, ok := expectedIfaceIds[ifaceId]; !ok {
			klog.Warningf("Found stale OVS Interface, deleting OVS Port with interface %s", ifaceInfo.Name)
			_, stderr, err := util.RunOVSVsctl("--if-exists", "--with-iface", "del-port", ifaceInfo.Name)
			if err != nil {
				klog.Errorf("Failed to delete interface %q . stderr: %q, error: %v",
					ifaceInfo.Name, stderr, err)
			}
		}
	}
}
