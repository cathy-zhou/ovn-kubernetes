package networkControllerManager

import (
	"context"
	"fmt"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/node"
	ovntypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
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

	defaultNodeNetworkController NetworkController

	// net-attach-def controller handle net-attach-def and create/delete secondary controllers
	nadController *netAttachDefinitionController
}

// newBaseNodeNetworkController creates and returns the base node network controller
func (ncm *nodeNetworkControllerManager) newBaseNodeNetworkController() *node.BaseNodeNetworkController {
	return node.NewBaseNodeNetworkController(ncm.client, ncm.watchFactory, ncm.recorder, ncm.name, ncm.isOvnUpEnabled)
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

	if config.OVNKubernetesFeature.EnableMultiNetwork {
		klog.Infof("Multiple network supported, creating net-attach-def controller")
		ncm.nadController = NewNadController(ncm, ovnClient)
	}
	return ncm
}

func (ncm *nodeNetworkControllerManager) NewSecondaryNetworkController(topoType string,
	nInfo util.NetInfo, netConfInfo util.NetConfInfo) (SecondaryNetworkController, error) {
	if topoType == ovntypes.Layer3AttachDefTopoType || topoType == ovntypes.Layer2AttachDefTopoType ||
		topoType == ovntypes.LocalnetAttachDefTopoType {
		return node.NewSecondaryNodeNetworkController(ncm.newBaseNodeNetworkController(), nInfo, netConfInfo), nil
	}
	return nil, fmt.Errorf("topology type %s not supported", topoType)
}

func (cm *nodeNetworkControllerManager) SyncAllSecondaryNetworkControllers(allControllers []SecondaryNetworkController) error {
	return nil
}

// Init initializes the controller manager and create/start default controller
func (cm *nodeNetworkControllerManager) Init() error {
	var isOvnUpEnabled bool
	var err error
	if config.OvnKubeNode.Mode != ovntypes.NodeModeDPUHost {
		isOvnUpEnabled, err = node.GetOVNIfUpCheckMode()
		if err != nil {
			return err
		}
	}
	cm.isOvnUpEnabled = isOvnUpEnabled
	cm.defaultNodeNetworkController = node.NewDefaultNodeNetworkController(cm.newBaseNodeNetworkController())
	return nil
}

// Run starts to handle all the secondary net-attach-def and creates and manages all the secondary controllers
func (ncm *nodeNetworkControllerManager) Start(ctx context.Context) error {
	klog.Infof("Starts net-attach-def controller")
	// Start and sync the watch factory to begin listening for events

	err := ncm.Init()
	if err != nil {
		return err
	}

	err = ncm.watchFactory.Start()
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
	var err error

	close(ncm.stopChan)

	if ncm.defaultNodeNetworkController != nil {
		err = ncm.defaultNodeNetworkController.Stop(false)
		if err != nil {
			klog.Errorf("Failed to stop default network controller")
		}
	}

	// and for each Controller of secondary network, call oc.Stop()
	// it is all right to call GetAllControllers here as nadController has been stopped
	// no more adding/deleting of the controllers
	for _, nc := range ncm.nadController.GetAllControllers() {
		err = nc.Stop(false)
		if err != nil {
			klog.Errorf("Failed to stop controller of network %s", nc.GetNetworkName())
		}
	}
}
