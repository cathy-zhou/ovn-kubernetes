package node

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	ovntypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
)

// SecondaryNodeNetworkController structure is the object which holds the controls for starting
// and reacting upon the watched resources (e.g. pods, endpoints) for secondary network
type SecondaryNodeNetworkController struct {
	BaseNodeNetworkController
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

	go wait.Until(func() {
		nc.checkForStaleOVSRepresentorInterfaces()
	}, time.Minute, nc.stopChan)

	return nil
}

// Stop gracefully stops the controller
func (nc *SecondaryNodeNetworkController) Stop() {
	klog.Infof("Stop secondary node network controller of network %s", nc.GetNetworkName())
	close(nc.stopChan)
	nc.wg.Wait()
}

// Cleanup cleans up node entities for the given secondary network
func (nc *SecondaryNodeNetworkController) Cleanup(netName string) error {
	out, stderr, err := util.RunOVSVsctl("--columns=name", "--data=bare", "--no-headings",
		"--format=csv", "find", "Interface", "external_ids:sandbox!=\"\"", "external_ids:vf-netdev-name!=\"\"",
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
