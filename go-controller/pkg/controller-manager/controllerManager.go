package controllerManager

import (
	"context"
	"fmt"
	"sync"
	"time"

	nettypes "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	libovsdbclient "github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdbops"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/metrics"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn"
	addressset "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn/address_set"
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

// ControllerManager structure is the object manages all controllers for all networks
type ControllerManager struct {
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

	// default wait group and stop channel, used by default network controller
	defaultWg       *sync.WaitGroup
	defaultStopChan chan struct{}

	// unique identity for controllerManager running on different ovnkube-master instance,
	// used for leader election
	identity string

	// controller for all networks, key is netName of net-attach-def, value is *Controller
	// this map is updated either at the very beginning of ovnkube-master when initializing the default controller
	// or when net-attach-def is added/deleted. All these are serialized and no lock protection is needed
	allOvnControllers map[string]ovn.Controller
}

// Start waits until this process is the leader before starting master functions
func (cm *ControllerManager) Start(ctx context.Context, cancel context.CancelFunc) error {
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
				start := time.Now()
				defer func() {
					end := time.Since(start)
					metrics.MetricMasterReadyDuration.Set(end.Seconds())
				}()

				if err := cm.Init(ctx); err != nil {
					klog.Error(err)
					cancel()
					return
				}

				if err = cm.Run(); err != nil {
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

	cm.defaultWg.Add(1)
	go func() {
		leaderElector.Run(ctx)
		klog.Infof("Stopped leader election")
		cm.defaultWg.Done()
	}()

	return nil
}

func NewControllerManager(ovnClient *util.OVNClientset, identity string, wf *factory.WatchFactory,
	stopChan chan struct{}, libovsdbOvnNBClient libovsdbclient.Client, libovsdbOvnSBClient libovsdbclient.Client,
	recorder record.EventRecorder, wg *sync.WaitGroup) *ControllerManager {
	podRecorder := metrics.NewPodRecorder()

	return &ControllerManager{
		client: ovnClient.KubeClient,
		kube: &kube.Kube{
			KClient:              ovnClient.KubeClient,
			EIPClient:            ovnClient.EgressIPClient,
			EgressFirewallClient: ovnClient.EgressFirewallClient,
			CloudNetworkClient:   ovnClient.CloudNetworkClient,
		},
		watchFactory: wf,
		recorder:     recorder,
		nbClient:     libovsdbOvnNBClient,
		sbClient:     libovsdbOvnSBClient,
		podRecorder:  &podRecorder,

		defaultWg:         wg,
		defaultStopChan:   stopChan,
		identity:          identity,
		allOvnControllers: make(map[string]ovn.Controller),
	}
}

// Init initialize the controller manager and create/start default controller
func (cm *ControllerManager) Init(ctx context.Context) error {
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

	// enableOVNLogicalDataPathGroups sets an OVN flag to enable logical datapath
	// groups on OVN 20.12 and later. The option is ignored if OVN doesn't
	// understand it. Logical datapath groups reduce the size of the southbound
	// database in large clusters. ovn-controllers should be upgraded to a version
	// that supports them before the option is turned on by the master.
	nbGlobal := nbdb.NBGlobal{
		Options: map[string]string{"use_logical_dp_groups": "true"},
	}
	if err := libovsdbops.UpdateNBGlobalSetOptions(cm.nbClient, &nbGlobal); err != nil {
		return fmt.Errorf("failed to set NB global option to enable logical datapath groups: %v", err)
	}

	metrics.RunTimestamp(cm.defaultStopChan, cm.sbClient, cm.nbClient)
	metrics.MonitorIPSec(cm.nbClient)
	if config.Metrics.EnableConfigDuration {
		// with k=10,
		//  for a cluster with 10 nodes, measurement of 1 in every 100 requests
		//  for a cluster with 100 nodes, measurement of 1 in every 1000 requests
		metrics.GetConfigDurationRecorder().Run(cm.nbClient, cm.kube, 10, time.Second*5, cm.defaultStopChan)
	}

	cm.podRecorder.Run(cm.sbClient, cm.defaultStopChan)

	// Start and sync the watch factory to begin listening for events
	if err := cm.watchFactory.Start(); err != nil {
		return err
	}

	oc := cm.NewDefaultController(nil)
	return oc.Start(ctx)
}

func (cm *ControllerManager) NewDefaultController(addressSetFactory addressset.AddressSetFactory) *ovn.DefaultL3Controller {
	cInfo := ovn.NewControllerInfo(cm.client, cm.kube, cm.watchFactory, cm.recorder, cm.nbClient,
		cm.sbClient, cm.podRecorder, cm.SCTPSupport, nil)
	defaultController := ovn.NewDefaultL3Controller(cInfo, cm.defaultStopChan, cm.defaultWg, addressSetFactory)
	cm.allOvnControllers[ovntypes.DefaultNetworkName] = defaultController
	return defaultController
}

func (cm *ControllerManager) NewSecondaryController(netattachdef *nettypes.NetworkAttachmentDefinition) (ovn.Controller, error) {
	netInfo, netConfInfo, err := util.ParseNADInfo(netattachdef)
	if err != nil {
		if err == util.ErrorAttachDefNotOvnManaged {
			return nil, nil
		}
		return nil, err
	}

	cInfo := ovn.NewControllerInfo(cm.client, cm.kube, cm.watchFactory, cm.recorder, cm.nbClient,
		cm.sbClient, cm.podRecorder, cm.SCTPSupport, netInfo)

	// Note that net-attach-def add/delete/update events are serialized, so we don't need locks here.
	// Check if any Controller of the same netconf.Name already exists, if so, check its conf to see if they are the same.
	existingOc, ok := cm.allOvnControllers[netInfo.NetName]
	if !ok {
		existingOc = nil
	}
	return ovn.NewSecondaryL3Controller(existingOc, cInfo, netConfInfo, util.GetNadKeyName(netattachdef.Namespace, netattachdef.Name))
}

func (cm *ControllerManager) Run() error {
	// add the net-attach-def watcher handler,
	// for each Controller created, call oc.Start()
	return nil
}

func (cm *ControllerManager) Stop() {
	// remove the net-attach-def watch handler
	// and for each Controller of secondary network, call oc.Stop()
}
