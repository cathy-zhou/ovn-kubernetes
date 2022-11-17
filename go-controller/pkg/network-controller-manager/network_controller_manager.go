package networkControllerManager

import (
	"context"
	"fmt"
	"sync"
	"time"

	libovsdbclient "github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdbops"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/metrics"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn"
	ovntypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
)

type ovnkubeMasterLeaderMetrics struct{}

func (ovnkubeMasterLeaderMetrics) On(string) {
	metrics.MetricMasterLeader.Set(1)
}

func (ovnkubeMasterLeaderMetrics) Off(string) {
	metrics.MetricMasterLeader.Set(0)
}

type ovnkubeMasterLeaderMetricsProvider struct{}

func (_ ovnkubeMasterLeaderMetricsProvider) NewLeaderMetric() leaderelection.SwitchMetric {
	return ovnkubeMasterLeaderMetrics{}
}

type NetworkController interface {
	Start(ctx context.Context) error
	Stop(deleteLogicalEntities bool) error
	GetNetworkName() string
}

type SecondaryNetworkController interface {
	NetworkController
	CompareNetConf(util.NetConfInfo) bool
	AddNad(nadName string)
	DeleteNad(nadName string)
	IsNadExist(nadName string) bool
	DeleteLogicalEntities(netName string) error
}

// NetworkControllerManager structure is the object manages all controllers for all networks
type NetworkControllerManager struct {
	ovnClientset *util.OVNClientset
	client       clientset.Interface
	kube         kube.Interface
	watchFactory *factory.WatchFactory
	podRecorder  *metrics.PodRecorder
	// event recorder used to post events to k8s
	recorder record.EventRecorder
	// libovsdb northbound client interface
	nbClient libovsdbclient.Client
	// libovsdb southbound client interface
	sbClient libovsdbclient.Client
	// has SCTP support
	SCTPSupport bool
	// ACL logging support enabled
	aclLoggingEnabled bool

	stopChan chan struct{}
	wg       *sync.WaitGroup

	// unique identity for controllerManager running on different ovnkube-master instance,
	// used for leader election
	identity string

	defaultNetworkController NetworkController

	// net-attach-def controller handle net-attach-def and create/delete secondary controllers
	nadController *netAttachDefinitionController
}

// Start waits until this process is the leader before starting master functions
func (cm *NetworkControllerManager) Start(ctx context.Context, cancel context.CancelFunc) error {
	// Set up leader election process first
	rl, err := resourcelock.New(
		// TODO (rravaiol) (bpickard)
		// https://github.com/kubernetes/kubernetes/issues/107454
		// leader election library no longer supports leader-election
		// locks based solely on `endpoints` or `configmaps` resources.
		// Slowly migrating to new API across three releases; with k8s 1.24
		// we're now in the second step ('x+2') bullet from the link above).
		// This will have to be updated for the next k8s bump: to 1.26.
		resourcelock.LeasesResourceLock,
		config.Kubernetes.OVNConfigNamespace,
		"ovn-kubernetes-master",
		cm.client.CoreV1(),
		cm.client.CoordinationV1(),
		resourcelock.ResourceLockConfig{
			Identity:      cm.identity,
			EventRecorder: cm.recorder,
		},
	)
	if err != nil {
		return err
	}

	lec := leaderelection.LeaderElectionConfig{
		Lock:            rl,
		LeaseDuration:   time.Duration(config.MasterHA.ElectionLeaseDuration) * time.Second,
		RenewDeadline:   time.Duration(config.MasterHA.ElectionRenewDeadline) * time.Second,
		RetryPeriod:     time.Duration(config.MasterHA.ElectionRetryPeriod) * time.Second,
		ReleaseOnCancel: true,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(ctx context.Context) {
				klog.Infof("Won leader election; in active mode")
				klog.Infof("Starting cluster master")
				start := time.Now()
				defer func() {
					end := time.Since(start)
					metrics.MetricMasterReadyDuration.Set(end.Seconds())
				}()

				if err := cm.Init(); err != nil {
					klog.Error(err)
					cancel()
					return
				}

				if err = cm.Run(ctx); err != nil {
					klog.Error(err)
					cancel()
					return
				}
			},
			OnStoppedLeading: func() {
				//This node was leader and it lost the election.
				// Whenever the node transitions from leader to follower,
				// we need to handle the transition properly like clearing
				// the cache.
				klog.Infof("No longer leader; exiting")
				cancel()
			},
			OnNewLeader: func(newLeaderName string) {
				if newLeaderName != cm.identity {
					klog.Infof("Lost the election to %s; in standby mode", newLeaderName)
				}
			},
		},
	}

	leaderelection.SetProvider(ovnkubeMasterLeaderMetricsProvider{})
	leaderElector, err := leaderelection.NewLeaderElector(lec)
	if err != nil {
		return err
	}

	cm.wg.Add(1)
	go func() {
		leaderElector.Run(ctx)
		klog.Infof("Stopped leader election")
		cm.wg.Done()
	}()

	return nil
}

// NewNetworkControllerManager creates a new OVN controller manager to manage all the controller for all networks
func NewNetworkControllerManager(ovnClient *util.OVNClientset, identity string, wf *factory.WatchFactory,
	libovsdbOvnNBClient libovsdbclient.Client, libovsdbOvnSBClient libovsdbclient.Client,
	recorder record.EventRecorder, wg *sync.WaitGroup) *NetworkControllerManager {
	podRecorder := metrics.NewPodRecorder()

	cm := &NetworkControllerManager{
		ovnClientset: ovnClient,
		client:       ovnClient.KubeClient,
		kube: &kube.Kube{
			KClient:              ovnClient.KubeClient,
			EIPClient:            ovnClient.EgressIPClient,
			EgressFirewallClient: ovnClient.EgressFirewallClient,
			CloudNetworkClient:   ovnClient.CloudNetworkClient,
		},
		stopChan:          make(chan struct{}),
		watchFactory:      wf,
		recorder:          recorder,
		nbClient:          libovsdbOvnNBClient,
		sbClient:          libovsdbOvnSBClient,
		podRecorder:       &podRecorder,
		SCTPSupport:       false,
		aclLoggingEnabled: true,
		wg:                wg,
		identity:          identity,
	}

	if config.OVNKubernetesFeature.EnableMultiNetwork {
		klog.Infof("Multiple network supported, creating net-attach-def controller")
		cm.nadController = cm.NewNadController()
	}
	return cm
}

// Init initializes the controller manager and create/start default controller
func (cm *NetworkControllerManager) Init() error {
	cm.configureMetrics(cm.stopChan)

	err := cm.configureSCTPSupport()
	if err != nil {
		return err
	}

	err = cm.createACLLoggingMeter()
	if err != nil {
		klog.Warningf("ACL logging support enabled, however acl-logging meter could not be created: %v. "+
			"Disabling ACL logging support", err)
		cm.aclLoggingEnabled = false
	}

	err = cm.enableOVNLogicalDataPathGroups()
	if err != nil {
		return err
	}

	cm.NewDefaultNetworkController()
	return nil
}

func (cm *NetworkControllerManager) configureSCTPSupport() error {
	hasSCTPSupport, err := util.DetectSCTPSupport()
	if err != nil {
		return err
	}

	if !hasSCTPSupport {
		klog.Warningf("SCTP unsupported by this version of OVN. Kubernetes service creation with SCTP will not work ")
	} else {
		klog.Info("SCTP support detected in OVN")
	}
	cm.SCTPSupport = hasSCTPSupport
	return nil
}

// enableOVNLogicalDataPathGroups sets an OVN flag to enable logical datapath
// groups on OVN 20.12 and later. The option is ignored if OVN doesn't
// understand it. Logical datapath groups reduce the size of the southbound
// database in large clusters. ovn-controllers should be upgraded to a version
// that supports them before the option is turned on by the master.
func (cm *NetworkControllerManager) enableOVNLogicalDataPathGroups() error {
	nbGlobal := nbdb.NBGlobal{
		Options: map[string]string{"use_logical_dp_groups": "true"},
	}
	if err := libovsdbops.UpdateNBGlobalSetOptions(cm.nbClient, &nbGlobal); err != nil {
		return fmt.Errorf("failed to set NB global option to enable logical datapath groups: %v", err)
	}
	return nil
}

func (cm *NetworkControllerManager) configureMetrics(stopChan <-chan struct{}) {
	metrics.RegisterMasterPerformance(cm.nbClient)
	metrics.RegisterMasterFunctional()
	metrics.RunTimestamp(stopChan, cm.sbClient, cm.nbClient)
	metrics.MonitorIPSec(cm.nbClient)
}

func (cm *NetworkControllerManager) createACLLoggingMeter() error {
	band := &nbdb.MeterBand{
		Action: ovntypes.MeterAction,
		Rate:   config.Logging.ACLLoggingRateLimit,
	}
	ops, err := libovsdbops.CreateMeterBandOps(cm.nbClient, nil, band)
	if err != nil {
		return fmt.Errorf("can't create meter band %v: %v", band, err)
	}

	meterFairness := true
	meter := &nbdb.Meter{
		Name: ovntypes.OvnACLLoggingMeter,
		Fair: &meterFairness,
		Unit: ovntypes.PacketsPerSecond,
	}
	ops, err = libovsdbops.CreateOrUpdateMeterOps(cm.nbClient, ops, meter, []*nbdb.MeterBand{band},
		&meter.Bands, &meter.Fair, &meter.Unit)
	if err != nil {
		return fmt.Errorf("can't create meter %v: %v", meter, err)
	}

	_, err = libovsdbops.TransactAndCheck(cm.nbClient, ops)
	if err != nil {
		return fmt.Errorf("can't transact ACL logging meter: %v", err)
	}

	return nil
}

// newBaseNetworkController creates and returns the base controller
func (cm *NetworkControllerManager) newBaseNetworkController() *ovn.BaseNetworkController {
	return ovn.NewBaseNetworkController(cm.client, cm.kube, cm.watchFactory, cm.recorder, cm.nbClient,
		cm.sbClient, cm.podRecorder, cm.SCTPSupport, cm.aclLoggingEnabled)
}

// NewDefaultNetworkController creates and returns the controller for default network
func (cm *NetworkControllerManager) NewDefaultNetworkController() {
	defaultController := ovn.NewDefaultNetworkController(cm.newBaseNetworkController())
	cm.defaultNetworkController = defaultController
}

// Run starts to handle all the secondary net-attach-def and creates and manages all the secondary controllers
func (cm *NetworkControllerManager) Run(ctx context.Context) error {
	if config.Metrics.EnableConfigDuration {
		// with k=10,
		//  for a cluster with 10 nodes, measurement of 1 in every 100 requests
		//  for a cluster with 100 nodes, measurement of 1 in every 1000 requests
		metrics.GetConfigDurationRecorder().Run(cm.nbClient, cm.kube, 10, time.Second*5, cm.stopChan)
	}
	cm.podRecorder.Run(cm.sbClient, cm.stopChan)

	err := cm.watchFactory.Start()
	if err != nil {
		return err
	}

	if cm.defaultNetworkController != nil {
		err = cm.defaultNetworkController.Start(ctx)
		if err != nil {
			return fmt.Errorf("failed to start default network controller: %v", err)
		}
	}

	if cm.nadController != nil {
		klog.Infof("Starts net-attach-def controller")
		return cm.nadController.Run(cm.stopChan)
	}
	return nil
}

// Stop gracefully stops all managed controllers
func (cm *NetworkControllerManager) Stop() {
	var err error

	close(cm.stopChan)

	if cm.defaultNetworkController != nil {
		err = cm.defaultNetworkController.Stop(false)
		if err != nil {
			klog.Errorf("Failed to stop default network controller")
		}
	}

	// and for each Controller of secondary network, call oc.Stop()
	// it is all right to call GetAllControllers here as nadController has been stopped
	// no more adding/deleting of the controllers
	for _, oc := range cm.nadController.GetAllControllers() {
		err = oc.Stop(false)
		if err != nil {
			klog.Errorf("Failed to stop controller of network %s", oc.GetNetworkName())
		}
	}
}
