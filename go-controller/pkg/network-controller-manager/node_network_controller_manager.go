package networkControllerManager

import (
	"context"
	"fmt"
	"strings"

	"github.com/containernetworking/cni/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/cni"
	ovncnitypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/cni/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/node"
	ovntypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
	kexec "k8s.io/utils/exec"
)

// nodeNetworkControllerManager structure is the object manages all controllers for all networks for ovnkube-node
type nodeNetworkControllerManager struct {
	name           string
	client         clientset.Interface
	Kube           kube.Interface
	watchFactory   factory.NodeWatchFactory
	stopChan       chan struct{}
	recorder       record.EventRecorder
	isOvnUpEnabled bool

	defaultNodeNetworkController BaseNetworkController

	// net-attach-def controller handle net-attach-def and create/delete secondary controllers
	// nil in dpu-host mode
	nadController *netAttachDefinitionController
}

// NewNetworkController create secondary node network controllers for the given NetInfo and NetConfInfo
func (ncm *nodeNetworkControllerManager) NewNetworkController(nInfo util.NetInfo,
	netConfInfo util.NetConfInfo) (NetworkController, error) {
	topoType := netConfInfo.TopologyType()
	if topoType == ovntypes.Layer3Topology || topoType == ovntypes.Layer2Topology ||
		topoType == ovntypes.LocalnetTopology {
		return node.NewSecondaryNodeNetworkController(ncm.newCommonNetworkControllerInfo(), nInfo, netConfInfo), nil
	}
	return nil, fmt.Errorf("topology type %s not supported", topoType)
}

// newDummyNetworkController creates a dummy network controller used to clean up specific network
func (ncm *nodeNetworkControllerManager) newDummyNetworkController(topoType, netName string) (NetworkController, error) {
	netInfo := util.NewNetInfo(&ovncnitypes.NetConf{NetConf: types.NetConf{Name: netName}, Topology: topoType})
	return node.NewSecondaryNodeNetworkController(ncm.newCommonNetworkControllerInfo(), netInfo, nil), nil
}

// CleanupDeletedNetworks cleans up all stale entities giving list of all existing secondary network controllers
func (ncm *nodeNetworkControllerManager) CleanupDeletedNetworks(allControllers []NetworkController) error {
	existingNetworksMap := map[string]struct{}{}
	for _, nc := range allControllers {
		existingNetworksMap[nc.GetNetworkName()] = struct{}{}
	}

	// find all secondary network OVN-k8s OVS interfaces
	ovsArgs := []string{"--columns=external_ids", "--data=bare", "--no-headings", "--format=csv",
		"find", "Interface", "external_ids:sandbox!=\"\"",
		fmt.Sprintf("external_ids:%s!=\"\"", ovntypes.NetworkExternalID)}

	out, stderr, err := util.RunOVSVsctl(ovsArgs...)
	if err != nil {
		return fmt.Errorf("failed to list ovn-k8s OVS interfaces:, stderr: %q, error: %v", stderr, err)
	}

	if out == "" {
		return nil
	}

	// delete all OVS interface of the stale networks
	staleNetworkControllers := map[string]NetworkController{}
	lines := strings.Split(out, "\n")
	for _, line := range lines {
		for _, attr := range strings.Split(line, " ") {
			keyVal := strings.SplitN(attr, "=", 2)
			if len(keyVal) != 2 || keyVal[0] != ovntypes.NetworkExternalID {
				continue
			}
			netName := keyVal[1]
			if _, ok := existingNetworksMap[netName]; !ok {
				if oc, err := ncm.newDummyNetworkController("dummyTopology", netName); err == nil {
					staleNetworkControllers[netName] = oc
				}
			}
			break
		}
	}

	for netName, oc := range staleNetworkControllers {
		klog.Infof("Cleanup entities for stale network %s", netName)
		err = oc.Cleanup(netName)
		if err != nil {
			klog.Errorf("Failed to delete stale OVN logical entities for network %s: %v", netName, err)
		}
	}

	return nil
}

// newCommonNetworkControllerInfo creates and returns the base node network controller info
func (ncm *nodeNetworkControllerManager) newCommonNetworkControllerInfo() *node.CommonNodeNetworkControllerInfo {
	return node.NewCommonNodeNetworkControllerInfo(ncm.client, ncm.watchFactory, ncm.recorder, ncm.name, ncm.isOvnUpEnabled)
}

// NewNodeNetworkControllerManager creates a new OVN controller manager to manage all the controller for all networks
func NewNodeNetworkControllerManager(ovnClient *util.OVNClientset, wf factory.NodeWatchFactory, name string,
	eventRecorder record.EventRecorder) *nodeNetworkControllerManager {
	ncm := &nodeNetworkControllerManager{
		name:         name,
		client:       ovnClient.KubeClient,
		Kube:         &kube.Kube{KClient: ovnClient.KubeClient},
		watchFactory: wf,
		stopChan:     make(chan struct{}),
		recorder:     eventRecorder,
	}

	// dpu host mode does not need to do anything special for secondary network support
	if config.OVNKubernetesFeature.EnableMultiNetwork && config.OvnKubeNode.Mode != ovntypes.NodeModeDPUHost {
		klog.Infof("Multiple network supported, creating %s", controllerName)
		ncm.nadController = newNetAttachDefinitionController(ncm, ovnClient.NetworkAttchDefClient, eventRecorder)
	}
	return ncm
}

// getOVNIfUpCheckMode check if OVN PortBinding.up can be used
func (ncm *nodeNetworkControllerManager) getOVNIfUpCheckMode() error {
	// this support is only used when configure Pod's OVS interface, it is not needed in DPU host mode
	if config.OvnKubeNode.DisableOVNIfaceIdVer || config.OvnKubeNode.Mode == ovntypes.NodeModeDPUHost {
		klog.Infof("'iface-id-ver' is manually disabled, ovn-installed feature can't be used")
		ncm.isOvnUpEnabled = false
		return nil
	}

	isOvnUpEnabled, err := util.GetOVNIfUpCheckMode()
	if err != nil {
		return err
	}
	ncm.isOvnUpEnabled = isOvnUpEnabled
	if isOvnUpEnabled {
		klog.Infof("Detected support for port binding with external IDs")
	}
	return nil
}

// Init initializes the node network controller manager and create default controller
func (ncm *nodeNetworkControllerManager) Init() error {
	if err := ncm.getOVNIfUpCheckMode(); err != nil {
		return err
	}

	// Initialize OVS exec runner; find OVS binaries that the CNI code uses.
	// Must happen before calling any OVS exec from pkg/cni to prevent races.
	// Not required in DPUHost mode as OVS is not present there.
	if err := cni.SetExec(kexec.New()); err != nil {
		return err
	}

	ncm.defaultNodeNetworkController = node.NewDefaultNodeNetworkController(ncm.newCommonNetworkControllerInfo())
	return nil
}

// Start initializes and starts the node network controller manager, which handles both default and secondary controllers
func (ncm *nodeNetworkControllerManager) Start(ctx context.Context) error {
	klog.Infof("OVN Kube Node initialization, Mode: %s", config.OvnKubeNode.Mode)

	// Start the watch factory to begin listening for events
	err := ncm.Init()
	if err != nil {
		return err
	}

	return ncm.Run(ctx)
}

// Run starts the node network controller manager, including default network controllers and the NAD controller
// that handles all net-attach-def and the associated secondary network controllers.
func (ncm *nodeNetworkControllerManager) Run(ctx context.Context) error {
	err := ncm.watchFactory.Start()
	if err != nil {
		return err
	}

	if ncm.defaultNodeNetworkController != nil {
		err = ncm.defaultNodeNetworkController.Start(ctx)
		if err != nil {
			return fmt.Errorf("failed to start default network controller: %v", err)
		}
	}

	if ncm.nadController != nil {
		klog.Infof("Starts net-attach-def controller")
		return ncm.nadController.Run(ncm.stopChan)
	}

	return nil
}

// Stop gracefully stops all managed controllers
func (ncm *nodeNetworkControllerManager) Stop() {
	close(ncm.stopChan)

	if ncm.defaultNodeNetworkController != nil {
		ncm.defaultNodeNetworkController.Stop()
	}

	// then stops each network controller associated with net-attach-def; it is ok
	// to call GetAllNetworkControllers here as net-attach-def controller has been stopped,
	// and no more change of network controllers
	if ncm.nadController != nil {
		for _, nc := range ncm.nadController.GetAllNetworkControllers() {
			nc.Stop()
		}
	}
}
