package networkControllerManager

import (
	"fmt"
	"sync"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/klog/v2"

	nettypes "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	netattachdefinformers "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/informers/externalversions"
	networkattachmentdefinitioninformerfactory "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/informers/externalversions"
	networkattachmentdefinitionlisters "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/listers/k8s.cni.cncf.io/v1"
	libovsdbclient "github.com/ovn-org/libovsdb/client"
	"github.com/ovn-org/libovsdb/ovsdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdbops"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/metrics"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/syncmap"
	ovntypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	"golang.org/x/time/rate"
)

const (
	// maxRetries is the number of times a object will be retried before it is dropped out of the queue.
	// With the current rate-limiter in use (5ms*2^(maxRetries-1)) the following numbers represent the
	// sequence of delays between successive queuings of an object.
	//
	// 5ms, 10ms, 20ms, 40ms, 80ms, 160ms, 320ms, 640ms, 1.3s, 2.6s, 5.1s, 10.2s, 20.4s, 41s, 82s
	// maxRetries = 15

	controllerName  = "net-attach-def-controller"
	avoidResync     = 0
	numberOfWorkers = 1 // set it to 1 so event handler is serialized for now.
	qps             = 15
	maxRetries      = 10
)

type netAttachDefinitionController struct {
	SecondaryNetworkControllerManager
	bnc                *ovn.BaseNetworkController
	watchFactory       *factory.WatchFactory
	nbClient           libovsdbclient.Client
	nadFactory         networkattachmentdefinitioninformerfactory.SharedInformerFactory
	netAttachDefLister networkattachmentdefinitionlisters.NetworkAttachmentDefinitionLister
	netAttachDefSynced cache.InformerSynced
	queue              workqueue.RateLimitingInterface
	loopPeriod         time.Duration
}

func (cm *NetworkControllerManager) NewNadController() *netAttachDefinitionController {
	nadFactory := netattachdefinformers.NewSharedInformerFactoryWithOptions(
		cm.ovnClientset.NetworkAttchDefClient,
		avoidResync,
	)
	netAttachDefInformer := nadFactory.K8sCniCncfIo().V1().NetworkAttachmentDefinitions()
	rateLimter := workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 1000*time.Second),
		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(qps), qps*5)})

	nadController := &netAttachDefinitionController{
		bnc:                cm.newBaseNetworkController(),
		nbClient:           cm.nbClient,
		nadFactory:         nadFactory,
		watchFactory:       cm.watchFactory,
		netAttachDefLister: netAttachDefInformer.Lister(),
		netAttachDefSynced: netAttachDefInformer.Informer().HasSynced,
		queue:              workqueue.NewNamedRateLimitingQueue(rateLimter, "net-attach-def"),
		loopPeriod:         time.Second,

		SecondaryNetworkControllerManager: &secondaryNetworkControllerNameManager{
			perNadNetConfInfo:     syncmap.NewSyncMap[*nadNetConfInfo](),
			perNetworkNadNameInfo: syncmap.NewSyncMap[*nadNameInfo](),
		},
	}
	netAttachDefInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    nadController.addNetworkAttachDefinition,
			DeleteFunc: nadController.deleteNetworkAttachDefinition,
		})
	return nadController
}

func (nadController *netAttachDefinitionController) Run(stopChan <-chan struct{}) error {
	defer utilruntime.HandleCrash()
	defer nadController.queue.ShutDown()

	nadController.nadFactory.Start(stopChan)
	klog.Infof("Starting controller %s", controllerName)
	if !cache.WaitForNamedCacheSync(controllerName, stopChan, nadController.netAttachDefSynced) {
		return fmt.Errorf("error syncing cache")
	}

	err := nadController.repairNads()
	if err != nil {
		klog.Errorf("Failed to sync all existing nad entries: %v", err)
	}

	klog.Info("Starting workers for controller %s", controllerName)
	wg := &sync.WaitGroup{}
	for i := 0; i < numberOfWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wait.Until(nadController.worker, nadController.loopPeriod, stopChan)
		}()
	}
	wg.Wait()

	// wait until we're told to stop
	<-stopChan

	klog.Infof("Shutting down controller %s", controllerName)
	nadController.queue.ShutDown()
	return nil
}

// Find all the OVN logical switches/routers for the secondary networks
func findAllSecondaryNetworkLogicalEntities(nbClient libovsdbclient.Client) ([]*nbdb.LogicalSwitch,
	[]*nbdb.LogicalRouter, error) {
	p1 := func(item *nbdb.LogicalSwitch) bool {
		_, ok := item.ExternalIDs[ovntypes.NetworkNameExternalID]
		return ok
	}
	nodeSwitches, err := libovsdbops.FindLogicalSwitchesWithPredicate(nbClient, p1)
	if err != nil {
		klog.Errorf("Failed to get all logical switches of secondary network error: %v", err)
		return nil, nil, err
	}
	p2 := func(item *nbdb.LogicalRouter) bool {
		_, ok := item.ExternalIDs[ovntypes.NetworkNameExternalID]
		return ok
	}
	clusterRouters, err := libovsdbops.FindLogicalRoutersWithPredicate(nbClient, p2)
	if err != nil {
		klog.Errorf("Failed to get all distributed logical routers: %v", err)
		return nil, nil, err
	}
	return nodeSwitches, clusterRouters, nil
}

func createSecondaryNetworkController(topoType string, bnc *ovn.BaseNetworkController,
	nInfo util.NetInfo, netConfInfo util.NetConfInfo) (SecondaryNetworkController, error) {
	if topoType == ovntypes.Layer3AttachDefTopoType {
		return ovn.NewSecondaryLayer3NetworkController(bnc, nInfo, netConfInfo), nil
	} else if topoType == ovntypes.Layer2AttachDefTopoType {
		return ovn.NewSecondaryLayer2NetworkController(bnc, nInfo, netConfInfo), nil
	}
	return nil, fmt.Errorf("topotype %s not supported", topoType)
}

func (nadController *netAttachDefinitionController) repairNads() (err error) {
	startTime := time.Now()
	klog.V(4).Infof("Starting repairing loop for %s", controllerName)
	defer func() {
		klog.V(4).Infof("Finished repairing loop for %s: %v err: %v", controllerName,
			time.Since(startTime), err)
	}()

	existingNads, err := nadController.netAttachDefLister.List(labels.Everything())
	if err != nil {
		return fmt.Errorf("failed to get list of all net-attach-def")
	}

	// need to walk through all the nads and update nadNames of Controller. Controllers will be created as
	// the result, but they can only be started afterwards when all nads are added to the network controller.
	for _, nad := range existingNads {
		_ = nadController.AddSecondaryNetworkNad(nadController.bnc, nad, false)
	}

	existingNetworksMap := map[string]bool{}
	for _, oc := range nadController.GetAllControllers() {
		existingNetworksMap[oc.GetNetworkName()] = true
	}

	// Get all the existing secondary networks and its logical entities
	switches, routers, err := findAllSecondaryNetworkLogicalEntities(nadController.nbClient)
	if err != nil {
		return err
	}

	var ops []ovsdb.Operation
	staleNetworks := map[string]SecondaryNetworkController{}
	for _, ls := range switches {
		netName := ls.ExternalIDs[ovntypes.NetworkNameExternalID]
		if _, ok := existingNetworksMap[netName]; ok {
			// network still exists, no cleanup to do
			continue
		}
		if topoType, ok := ls.ExternalIDs[ovntypes.TopoTypeExternalID]; ok {
			// Create dummy network controllers to cleanup logical entities
			klog.V(5).Infof("Found stale %s network %s", topoType, netName)
			if oc, err := createSecondaryNetworkController(topoType, nadController.bnc, nil, nil); err == nil {
				staleNetworks[netName] = oc
				continue
			}
		}
		klog.Infof("Missing %s external-id on switch %s, simply deleted it", ovntypes.TopoTypeExternalID, ls.Name)
		ops, err = libovsdbops.DeleteLogicalSwitchOps(nadController.nbClient, ops, ls.Name)
		if err != nil {
			klog.Errorf("Failed to get ops to delete stale logical switch %s for network %s: %v", ls.Name, netName, err)
		}
	}
	for _, lr := range routers {
		netName := lr.ExternalIDs[ovntypes.NetworkNameExternalID]
		if _, ok := existingNetworksMap[netName]; ok {
			// network still exists, no cleanup to do
			continue
		}
		if topoType, ok := lr.ExternalIDs[ovntypes.TopoTypeExternalID]; ok {
			// Create dummy network controllers to cleanup logical entities
			klog.V(5).Infof("Found stale %s network %s", topoType, netName)
			if oc, err := createSecondaryNetworkController(topoType, nadController.bnc, nil, nil); err == nil {
				staleNetworks[netName] = oc
				continue
			}
		}
		klog.Infof("Missing %s external-id on router %s, simply deleted it", ovntypes.TopoTypeExternalID, lr.Name)
		ops, err = libovsdbops.DeleteLogicalRouterOps(nadController.nbClient, ops, lr)
		if err != nil {
			klog.Errorf("Failed to get ops to delete logical router %s for network %s: %v", lr.Name, netName, err)
		}
	}
	_, err = libovsdbops.TransactAndCheck(nadController.nbClient, ops)
	if err != nil {
		klog.Errorf("Failed to delete stale OVN logical entities", err)
	}

	for netName, oc := range staleNetworks {
		klog.Infof("Delete logical entities for stale network %s", netName)
		err = oc.DeleteLogicalEntities(netName)
		if err != nil {
			klog.Errorf("Failed to delete stale OVN logical entities for network %s: %v", netName, err)
		}
	}
	return nil
}

func (nadController *netAttachDefinitionController) worker() {
	for nadController.processNextWorkItem() {
	}
}

func (nadController *netAttachDefinitionController) processNextWorkItem() bool {
	key, quit := nadController.queue.Get()
	if quit {
		return false
	}
	defer nadController.queue.Done(key)

	err := nadController.sync(key.(string))
	if err == nil {
		nadController.queue.Forget(key)
		return true
	}
	nadController.handleErr(err, key)
	return true
}

func (nadController *netAttachDefinitionController) sync(key string) error {
	startTime := time.Now()
	namespace, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	klog.Infof("Sync event for net-attach-def %s/%s", namespace, name)
	defer func() {
		klog.V(4).Infof("Finished syncing net-attach-def %s/%s : %v", namespace, name, time.Since(startTime))
	}()

	nad, err := nadController.netAttachDefLister.NetworkAttachmentDefinitions(namespace).Get(name)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	if nad == nil {
		return nadController.DeleteSecondaryNetworkNad(key)
	} else {
		return nadController.AddSecondaryNetworkNad(nadController.bnc, nad, true)
	}
}

func (nadController *netAttachDefinitionController) handleErr(err error, key interface{}) {
	ns, name, keyErr := cache.SplitMetaNamespaceKey(key.(string))
	if keyErr != nil {
		klog.ErrorS(err, "Failed to split meta namespace cache key", "key", key)
	}
	if err == nil {
		metrics.GetConfigDurationRecorder().End("net-attach-def", ns, name)
		nadController.queue.Forget(key)
		return
	}

	//metrics.MetricRequeueNADCount.Inc()
	//
	if nadController.queue.NumRequeues(key) < maxRetries {
		nadController.queue.AddRateLimited(key)
		klog.V(2).InfoS("Error syncing net-attach-def, retrying", "net-attach-def", klog.KRef(ns, name), "err", err)
		return
	}

	klog.Warningf("Dropping net-attach-def %q out of the queue: %v", key, err)
	metrics.GetConfigDurationRecorder().End("net-attach-def", ns, name)
	nadController.queue.Forget(key)
	utilruntime.HandleError(err)
}

func (nadController *netAttachDefinitionController) addNetworkAttachDefinition(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %+v: %v", obj, err))
		return
	}
	klog.V(4).Infof("Adding network-attachment-definition %s", key)
	netAttachDef := obj.(*nettypes.NetworkAttachmentDefinition)
	metrics.GetConfigDurationRecorder().Start("net-attach-def", netAttachDef.Namespace, netAttachDef.Name)
	nadController.queue.Add(key)
}

func (nadController *netAttachDefinitionController) deleteNetworkAttachDefinition(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for object %+v: %v", obj, err))
		return
	}
	klog.V(4).Infof("Deleting network-attachment-definition %s", key)
	netAttachDef := obj.(*nettypes.NetworkAttachmentDefinition)
	metrics.GetConfigDurationRecorder().Start("net-attach-def", netAttachDef.Namespace, netAttachDef.Name)
	nadController.queue.Add(key)
}
