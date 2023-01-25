package node

import (
	"context"
	"fmt"
	"strings"
	"sync"
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
	BaseNodeNetworkController
	podHandler *factory.Handler
}

// NewSecondaryNodeNetworkController creates a new OVN controller for creating logical network
// infrastructure and policy for default l3 network
func NewSecondaryNodeNetworkController(cnnci *CommonNodeNetworkControllerInfo, netInfo util.NetInfo,
	netconfInfo util.NetConfInfo) *SecondaryNodeNetworkController {
	return &SecondaryNodeNetworkController{
		BaseNodeNetworkController: BaseNodeNetworkController{
			CommonNodeNetworkControllerInfo: *cnnci,
			NetConfInfo:                     netconfInfo,
			NetInfo:                         netInfo,
			stopChan:                        make(chan struct{}),
			wg:                              &sync.WaitGroup{},
		},
	}
}

// Start starts the default controller; handles all events and creates all needed logical entities
func (nc *SecondaryNodeNetworkController) Start(ctx context.Context) error {
	klog.Infof("Start secondary node network controller of network %s", nc.GetNetworkName())
	err := nc.updateIsOvnUpEnabled(ctx)
	if err != nil {
		return err
	}

	go wait.Until(func() {
		nc.checkForStaleOVSRepresentorInterfaces()
	}, time.Minute, nc.stopChan)

	if nc.TopologyType() == ovntypes.LocalnetTopology {
		err := nc.addLocalnetOvnBridgeMapping()
		if err != nil {
			return err
		}
	}

	if config.OvnKubeNode.Mode == ovntypes.NodeModeDPU {
		handler, err := nc.watchPodsDPU()
		if err != nil {
			return err
		}
		nc.podHandler = handler
		return err
	}
	return nil
}

// Stop gracefully stops the controller
func (nc *SecondaryNodeNetworkController) Stop() {
	klog.Infof("Stop secondary node network controller of network %s", nc.GetNetworkName())
	close(nc.stopChan)
	nc.wg.Wait()
	if nc.podHandler != nil {
		nc.watchFactory.RemovePodHandler(nc.podHandler)
	}
}

// Cleanup cleans up node entities for the given secondary network
func (nc *SecondaryNodeNetworkController) Cleanup(netName string) error {
	if nc.TopologyType() == ovntypes.LocalnetTopology {
		err := nc.updateLocalnetOvnBridgeMapping("")
		if err != nil {
			return err
		}
	}
	out, stderr, err := util.RunOVSVsctl("--columns=name", "--data=bare", "--no-headings",
		"--format=csv", "find", "Interface", "external_ids:sandbox!=\"\"",
		fmt.Sprintf("external_ids:%s==%s", ovntypes.NetworkExternalID, netName))
	if err != nil {
		return fmt.Errorf("failed to list ovn-k8s OVS interfaces:, stderr: %q, error: %v", stderr, err)
	}

	names := strings.Split(out, "\n")
	for _, name := range names {
		_, stderr, err := util.RunOVSVsctl("--if-exists", "--with-iface", "del-port", name)
		if err != nil {
			return fmt.Errorf("failed to delete interface %q . stderr: %q, error: %v",
				name, stderr, err)
		}
	}

	return nil
}

func (nc *SecondaryNodeNetworkController) addLocalnetOvnBridgeMapping() error {
	// ovn-localnet-bridge-mappings external_id is in the form of "<network1>:<br1>,<network2>:<br2>...".
	// It sets all the possible localnet networks and associated bridge names on this node.
	stdout, stderr, err := util.RunOVSVsctl("--if-exists", "get", "Open_vSwitch", ".",
		"external_ids:ovn-localnet-bridge-mappings")
	if err != nil {
		klog.Warningf("Failed to get ngn-localnet-bridge-mappings from Open_vSwitch table stderr:%s (%v)", stderr, err)
		return nil
	}

	bridgeName := ""
	bridgeMapConfs := strings.Split(stdout, ",")
	for _, bridgeMapConf := range bridgeMapConfs {
		maps := strings.Split(bridgeMapConf, ":")
		if len(maps) == 2 && nc.GetNetworkName() == strings.TrimSpace(maps[0]) {
			bridgeName = strings.TrimSpace(maps[1])
			break
		}
	}

	return nc.updateLocalnetOvnBridgeMapping(bridgeName)
}

// updateLocalnetOvnBridgeMapping updates ovn-bridge-mappings for the given localnet network:
// - if bridgeName is not empty, add/update its bridge mapping
// - if bridgeName is not empty, delete its bridge mapping
func (nc *SecondaryNodeNetworkController) updateLocalnetOvnBridgeMapping(bridgeName string) error {
	var cmdArgs []string

	klog.Infof("Update localnet OVN bridge mapping to %s for network %s", bridgeName, nc.GetNetworkName())
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
			bridgeMap[strings.TrimSpace(m[0])] = strings.TrimSpace(m[1])
		}
	}

	bridge, ok := bridgeMap[networkName]
	if bridgeName != "" {
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
		cmdArgs = []string{"remove", "Open_vSwitch", ".", "external_ids", "ovn-bridge-mappings"}
	} else {
		mapString := ""
		for networkName, bridge := range bridgeMap {
			if len(mapString) != 0 {
				mapString += ","
			}
			mapString = mapString + networkName + ":" + bridge
		}
		cmdArgs = []string{"set", "Open_vSwitch", ".",
			fmt.Sprintf("external_ids:ovn-bridge-mappings=%s", mapString)}
	}

	_, stderr, err = util.RunOVSVsctl(cmdArgs...)
	if err != nil {
		return fmt.Errorf("failed to update ovn-bridge-mappings for network:(%s), bridge:(%s), stderr:%s (%v)",
			nc.GetNetworkName(), bridgeName, stderr, err)
	}
	return nil
}
