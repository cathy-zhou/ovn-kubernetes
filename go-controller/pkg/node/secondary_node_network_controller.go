package node

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	ovntypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
)

// SecondaryNodeNetworkController structure is the object which holds the controls for starting
// and reacting upon the watched resources (e.g. pods, endpoints) for secondary network
type SecondaryNodeNetworkController struct {
	NodeNetworkControllerInfo
	podHandler *factory.Handler
	bridgeName string
	stopChan   chan struct{}
}

// NewSecondaryNodeNetworkController creates a new OVN controller for creating logical network
// infrastructure and policy for default l3 network
func NewSecondaryNodeNetworkController(bnnc *BaseNodeNetworkController, nInfo util.NetInfo,
	netconfInfo util.NetConfInfo) *SecondaryNodeNetworkController {
	return &SecondaryNodeNetworkController{
		NodeNetworkControllerInfo: NodeNetworkControllerInfo{
			BaseNodeNetworkController: *bnnc,
			NetConfInfo:               netconfInfo,
			NetInfo:                   nInfo,
		},
		stopChan: make(chan struct{}),
	}
}

// Start starts the default controller; handles all events and creates all needed logical entities
func (nc *SecondaryNodeNetworkController) Start(ctx context.Context) error {
	klog.Infof("Start secondary node network controller of network %s", nc.GetNetworkName())
	if config.OvnKubeNode.Mode != ovntypes.NodeModeDPUHost {
		// start health check to ensure there are no stale OVS internal ports
		go wait.Until(func() {
			checkForStaleOVSInternalPorts()
			checkForStaleOVSRepresentorInterfaces(nc.NetInfo, nc.name, nc.watchFactory.(*factory.WatchFactory))
		}, time.Minute, nc.stopChan)
	}

	err := nc.updateLocalnetOvnBridgeMapping(true)
	if err != nil {
		return err
	}

	if config.OvnKubeNode.Mode == ovntypes.NodeModeDPU {
		handler, err := nc.watchPodsDPU(nc.isOvnUpEnabled)
		if err != nil {
			return err
		}
		nc.podHandler = handler
		return err
	}
	return nil
}

// Stop gracefully stops the controller
// deleteLogicalEntities will never be true for default network
func (nc *SecondaryNodeNetworkController) Stop(deleteLogicalEntities bool) error {
	klog.Infof("Stop secondary node network controller of network %s", nc.GetNetworkName())
	if nc.podHandler != nil {
		nc.watchFactory.RemovePodHandler(nc.podHandler)
	}
	if deleteLogicalEntities {
		// for dpu mode and full mode
		err := nc.updateLocalnetOvnBridgeMapping(false)
		if err != nil {
			return err
		}
		err = nc.DeleteLogicalEntities(nc.GetNetworkName())
		if err != nil {
			return err
		}
	}
	return nil
}

// DeleteLogicalEntities delete logical entities for this network
func (nc *SecondaryNodeNetworkController) DeleteLogicalEntities(netName string) error {
	if config.OvnKubeNode.Mode == ovntypes.NodeModeDPUHost {
		return nil
	}
	out, stderr, err := util.RunOVSVsctl("--columns=name", "--data=bare", "--no-headings",
		"--format=csv", "find", "Interface", "external_ids:sandbox!=\"\"",
		fmt.Sprintf("external_ids:%s==%s", ovntypes.NetworkNameExternalID, netName))
	if err != nil {
		klog.Errorf("Failed to list ovn-k8s OVS interfaces:, stderr: %q, error: %v", stderr, err)
		return nil
	}

	names := strings.Split(out, "\n")
	for _, name := range names {
		_, stderr, err := util.RunOVSVsctl("--if-exists", "--with-iface", "del-port", name)
		if err != nil {
			klog.Errorf("Failed to delete interface %q . stderr: %q, error: %v",
				name, stderr, err)
		}
	}

	return nil
}

func (nc *SecondaryNodeNetworkController) updateLocalnetOvnBridgeMapping(toAdd bool) error {
	if nc.GetTopologyType() != ovntypes.LocalnetAttachDefTopoType || config.OvnKubeNode.Mode == ovntypes.NodeModeDPUHost {
		return nil
	}

	bridgeName := ""
	if toAdd {
		// ngn-localnet-bridge-mappings exernal_ids is in the form of "<network_prefix1>:<br1>,<network_prefix2>:<br2>...".
		// It sets all the possible localnet networks and associated bridge names on this node.
		stdout, stderr, err := util.RunOVSVsctl("--if-exists", "get", "Open_vSwitch", ".",
			"external_ids:ovn-localnet-bridge-mappings")
		if err != nil {
			klog.Warningf("Failed to get ngn-localnet-bridge-mappings from Open_vSwitch table stderr:%s (%v)", stderr, err)
			return nil
		}

		bridgeMapConfs := strings.Split(stdout, ",")
		for _, bridgeMapConf := range bridgeMapConfs {
			maps := strings.Split(bridgeMapConf, ":")
			if len(maps) == 2 && strings.HasPrefix(nc.GetNetworkName(), maps[0]) {
				bridgeName = maps[1]
				break
			}
		}

		if bridgeName == "" {
			klog.V(5).Infof("Localnet network %s is not needed on this node %s", nc.GetNetworkName(), nc.name)
			return nil
		}
		nc.bridgeName = bridgeName
	} else {
		bridgeName = nc.bridgeName
	}

	// ovn-bridge-mappings maps a physical network name to a local ovs bridge
	// that provides connectivity to that network. It is in the form of physnet1:br1,physnet2:br2.
	// Note that there may be multiple ovs bridge mappings, be sure not to override
	// the mappings for the other physical network
	networkName := nc.GetPrefix() + ovntypes.LocalNetBridgeName
	stdout, stderr, err := util.RunOVSVsctl("--if-exists", "get", "Open_vSwitch", ".",
		"external_ids:ovn-bridge-mappings")
	if err != nil {
		return fmt.Errorf("failed to get ovn-bridge-mappings stderr:%s (%v)", stderr, err)
	}

	bridgeMap := map[string]string{}
	bridgeMappings := strings.Split(stdout, ",")
	for _, bridgeMapping := range bridgeMappings {
		m := strings.Split(bridgeMapping, ":")
		if len(m) == 2 {
			bridgeMap[m[0]] = m[1]
		}
	}

	bridge, ok := bridgeMap[networkName]
	if toAdd {
		if ok && bridge == bridgeName {
			return nil
		}
		bridgeMap[networkName] = bridgeName
	} else {
		if !ok {
			return nil
		}
		delete(bridgeMap, networkName)
	}

	if len(bridgeMap) == 0 {
		return nil
	}

	mapString := ""
	for networkName, bridge = range bridgeMap {
		if len(mapString) != 0 {
			mapString += ","
		}
		mapString = mapString + networkName + ":" + bridge
	}

	_, stderr, err = util.RunOVSVsctl("set", "Open_vSwitch", ".",
		fmt.Sprintf("external_ids:ovn-bridge-mappings=%s", mapString))
	if err != nil {
		return fmt.Errorf("failed to set ovn-bridge-mappings %s, stderr:%s (%v)", mapString, stderr, err)
	}
	return nil
}
