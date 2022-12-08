package networkControllerManager

import (
	"context"
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
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/metrics"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/syncmap"
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
	numberOfWorkers = 2
	qps             = 15
	maxRetries      = 10
)

type BaseNetworkController interface {
	Start(ctx context.Context) error
	Stop()
	GetNetworkName() string
}

type NetworkController interface {
	BaseNetworkController
	CompareNetConf(util.NetConfInfo) bool
	AddNad(nadName string)
	DeleteNad(nadName string)
	IsNadExist(nadName string) bool
	// Cleanup cleans up the given network, it could be called to clean up network controllers that are deleted when
	// ovn-k8s is down; so it's receiver could be a dummy network controller.
	Cleanup(netName string) error
}

// NetworkControllerManager manages all network controllers
type NetworkControllerManager interface {
	NewNetworkController(netInfo util.NetInfo, netConfInfo util.NetConfInfo) (NetworkController, error)
	SyncNetworkControllers(allControllers []NetworkController) error
	// NewDummyNetworkController creates a dummy network controller used to clean up specific network
	NewDummyNetworkController(topologyType, netName string) (NetworkController, error)
}

type netAttachDefinitionController struct {
	ncm                NetworkControllerManager
	nadFactory         networkattachmentdefinitioninformerfactory.SharedInformerFactory
	netAttachDefLister networkattachmentdefinitionlisters.NetworkAttachmentDefinitionLister
	netAttachDefSynced cache.InformerSynced
	queue              workqueue.RateLimitingInterface
	loopPeriod         time.Duration

	NetAttachDefManager
}

func NewNadController(ncm NetworkControllerManager, ovnClientset *util.OVNClientset) *netAttachDefinitionController {
	nadFactory := netattachdefinformers.NewSharedInformerFactoryWithOptions(
		ovnClientset.NetworkAttchDefClient,
		avoidResync,
	)
	netAttachDefInformer := nadFactory.K8sCniCncfIo().V1().NetworkAttachmentDefinitions()
	rateLimter := workqueue.NewMaxOfRateLimiter(
		workqueue.NewItemExponentialFailureRateLimiter(5*time.Millisecond, 1000*time.Second),
		&workqueue.BucketRateLimiter{Limiter: rate.NewLimiter(rate.Limit(qps), qps*5)})

	nadController := &netAttachDefinitionController{
		ncm:                ncm,
		nadFactory:         nadFactory,
		netAttachDefLister: netAttachDefInformer.Lister(),
		netAttachDefSynced: netAttachDefInformer.Informer().HasSynced,
		queue:              workqueue.NewNamedRateLimitingQueue(rateLimter, "net-attach-def"),
		loopPeriod:         time.Second,

		NetAttachDefManager: &netAttachDefManager{
			perNadNetConfInfo:     syncmap.NewSyncMap[*nadNetConfInfo](),
			perNetworkNadNameInfo: syncmap.NewSyncMap[*nadNameInfo](),
		},
	}
	netAttachDefInformer.Informer().AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    nadController.onNetworkAttachDefinitionAdd,
			UpdateFunc: nadController.onNetworkAttachDefinitionUpdate,
			DeleteFunc: nadController.onNetworkAttachDefinitionDelete,
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
		_ = nadController.AddNetAttachDef(nadController.ncm, nad, false)
	}

	allControllers := nadController.GetAllNetworkControllers()
	return nadController.ncm.SyncNetworkControllers(allControllers)
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
	klog.V(5).Infof("Sync net-attach-def %s/%s", namespace, name)
	defer func() {
		klog.V(4).Infof("Finished syncing net-attach-def %s/%s : %v", namespace, name, time.Since(startTime))
	}()

	nad, err := nadController.netAttachDefLister.NetworkAttachmentDefinitions(namespace).Get(name)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}

	if nad == nil {
		return nadController.DeleteNetAttachDef(key)
	} else {
		return nadController.AddNetAttachDef(nadController.ncm, nad, true)
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

func (nadController *netAttachDefinitionController) queueNetworkAttachDefinition(obj interface{}) {
	key, err := cache.MetaNamespaceKeyFunc(obj)
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("couldn't get key for net-attach-def %+v: %v", obj, err))
		return
	}
	//netAttachDef := obj.(*nettypes.NetworkAttachmentDefinition)
	//metrics.GetConfigDurationRecorder().Start("net-attach-def", netAttachDef.Namespace, netAttachDef.Name)
	nadController.queue.Add(key)
}

func (nadController *netAttachDefinitionController) onNetworkAttachDefinitionAdd(obj interface{}) {
	nad := obj.(*nettypes.NetworkAttachmentDefinition)
	if nad == nil {
		utilruntime.HandleError(fmt.Errorf("invalid net-attach-def provided to onNetworkAttachDefinitionAdd()"))
		return
	}

	klog.V(4).Infof("Adding net-attach-def %s/%s", nad.Namespace, nad.Name)
	nadController.queueNetworkAttachDefinition(obj)
}

func (nadController *netAttachDefinitionController) onNetworkAttachDefinitionUpdate(oldObj, newObj interface{}) {
	oldNad := oldObj.(*nettypes.NetworkAttachmentDefinition)
	newNad := newObj.(*nettypes.NetworkAttachmentDefinition)
	if oldNad == nil || newNad == nil {
		utilruntime.HandleError(fmt.Errorf("invalid net-attach-def provided to onNetworkAttachDefinitionUpdate()"))
		return
	}

	// don't process resync or objects that are marked for deletion
	if oldNad.ResourceVersion == newNad.ResourceVersion ||
		!newNad.GetDeletionTimestamp().IsZero() {
		return
	}

	klog.V(4).Infof("Updating net-attach-def %s/%s", newNad.Namespace, newNad.Name)
	nadController.queueNetworkAttachDefinition(newObj)
}

func (nadController *netAttachDefinitionController) onNetworkAttachDefinitionDelete(obj interface{}) {
	nad := obj.(*nettypes.NetworkAttachmentDefinition)
	if nad == nil {
		utilruntime.HandleError(fmt.Errorf("invalid net-attach-def provided to onNetworkAttachDefinitionDelete()"))
		return
	}

	klog.V(4).Infof("Deleting net-attach-def %s/%s", nad.Namespace, nad.Name)
	nadController.queueNetworkAttachDefinition(obj)
}

type NetAttachDefManager interface {
	// AddNetAttachDef adds the given net-attach-def to the specific networkController manager,
	// and start the newly created network controller if requested
	AddNetAttachDef(ncm NetworkControllerManager, netattachdef *nettypes.NetworkAttachmentDefinition, doStart bool) error
	// DeleteNetAttachDef deletes the given net-attach-def from the networkController manager,
	// and stops the network controller before it is deleted
	DeleteNetAttachDef(nadName string) error
	// GetAllNetworkControllers gets all managed network controllers
	GetAllNetworkControllers() []NetworkController
}

// netAttachDefManager holds all network controller associated with net-attach-def, currently only secondary
// networks, it add/delete network controllers when handlig net-attach-def events.
// All the network controller is added/deleted with lock protected by first perNadNetConfInfo (per nadName)
// then perNetworkNadNameInfo (per network name)
type netAttachDefManager struct {
	// key is nadName, value is nadNetConfInfo
	perNadNetConfInfo *syncmap.SyncMap[*nadNetConfInfo]
	// controller for all networks, key is netName of net-attach-def, value is NadNameInfo
	// this map is updated either at the very beginning of ovnkube-master when initializing the default controller
	// or when net-attach-def is added/deleted. All these are serialized by syncmap lock
	perNetworkNadNameInfo *syncmap.SyncMap[*nadNameInfo]
}

type nadNameInfo struct {
	nadNames  map[string]bool
	oc        NetworkController
	isStarted bool
}

type nadNetConfInfo struct {
	util.NetConfInfo
	netName string
}

// GetAllNetworkControllers returns a snapshot of all managed nad associated network controllers.
// Caller needs to note that there are no guarantees the return results reflect the real time
// condition. There maybe more controllers being added, and returned controllers nay be deleted
func (cnm *netAttachDefManager) GetAllNetworkControllers() []NetworkController {
	allNetworkNames := cnm.perNetworkNadNameInfo.GetKeys()
	allNetworkControllers := make([]NetworkController, 0, len(allNetworkNames))
	for _, netName := range allNetworkNames {
		cnm.perNetworkNadNameInfo.LockKey(netName)
		nni, ok := cnm.perNetworkNadNameInfo.Load(netName)
		if ok {
			allNetworkControllers = append(allNetworkControllers, nni.oc)
		}
		cnm.perNetworkNadNameInfo.UnlockKey(netName)
	}
	return allNetworkControllers
}

// AddNetAttachDef adds the given nad to the associated controller. It creates the controller if this
// is the first nad of the network
// note that for errors that are not retriable (configuration error etc.), just log the error and return nil
func (cnm *netAttachDefManager) AddNetAttachDef(ncm NetworkControllerManager,
	netattachdef *nettypes.NetworkAttachmentDefinition, doStart bool) error {
	var netConfInfo util.NetConfInfo
	var nInfo util.NetInfo
	var err error

	netAttachDefName := util.GetNadName(netattachdef.Namespace, netattachdef.Name)
	klog.Infof("Add net-attach-def %s", netAttachDefName)

	nInfo, netConfInfo, err = util.ParseNADInfo(netattachdef)
	netName := ""
	if err == nil {
		netName = nInfo.GetNetworkName()
	}

	return cnm.perNadNetConfInfo.DoWithLock(netAttachDefName, func(nadName string) error {
		nadNci, loaded := cnm.perNadNetConfInfo.LoadOrStore(nadName, &nadNetConfInfo{
			NetConfInfo: netConfInfo,
			netName:     netName,
		})
		if !loaded {
			// first time to process this nad
			if err != nil {
				// invalid nad, nothing to do
				klog.Warningf("net-attach-def %s is first seen and is invalid: %v", nadName, err)
				cnm.perNadNetConfInfo.Delete(nadName)
				return nil
			}
			klog.V(5).Infof("net-attach-def %s network %s first seen", nadName, netName)
			err = cnm.addNadToController(ncm, nadName, nInfo, netConfInfo, doStart)
			if err != nil {
				klog.Errorf("Failed to add net-attach-def %s to network %s: %v", nadName, netName, err)
				cnm.perNadNetConfInfo.Delete(nadName)
				return err
			}
		} else {
			klog.V(5).Infof("net-attach-def %s network %s already exists", nadName, netName)
			nadUpdated := false
			if err != nil {
				nadUpdated = true
			} else if nadNci.netName != netName {
				// netconf network name changed
				klog.V(5).Infof("net-attach-def %s network name %s has changed", netName, nadNci.netName)
				nadUpdated = true
			} else if !nadNci.CompareNetConf(netConfInfo) {
				// netconf spec changed
				klog.V(5).Infof("net-attach-def %s spec has changed", nadName)
				nadUpdated = true
			}

			if !nadUpdated {
				// nothing changed, may still need to start the controller
				if !doStart {
					return nil
				}
				err = cnm.addNadToController(ncm, nadName, nInfo, netConfInfo, doStart)
				if err != nil {
					klog.Errorf("Failed to add net-attach-def %s to network %s: %v", nadName, netName, err)
					return err
				}
				return nil
			}
			if nadUpdated {
				klog.V(5).Infof("net-attach-def %s network %s updated", nadName, netName)
				// delete the nad from the old network first
				err := cnm.deleteNadFromController(nadNci.netName, nadName)
				if err != nil {
					klog.Errorf("Failed to delete net-attach-def %s from network %s: %v", nadName, nadNci.netName, err)
					return err
				}
				cnm.perNadNetConfInfo.Delete(nadName)
			}
			if err != nil {
				klog.Warningf("net-attach-def %s is invalid: %v", nadName, err)
				return nil
			}
			klog.V(5).Infof("Add updated net-attach-def %s to network %s", nadName, netName)
			cnm.perNadNetConfInfo.LoadOrStore(nadName, &nadNetConfInfo{NetConfInfo: netConfInfo, netName: netName})
			err = cnm.addNadToController(ncm, nadName, nInfo, netConfInfo, doStart)
			if err != nil {
				klog.Errorf("Failed to add net-attach-def %s to network %s: %v", nadName, netName, err)
				cnm.perNadNetConfInfo.Delete(nadName)
				return err
			}
			return nil
		}
		return nil
	})
}

// DeleteNetAttachDef deletes the given nad from the associated controller. It delete the controller if this
// is the last nad of the network
func (cnm *netAttachDefManager) DeleteNetAttachDef(netAttachDefName string) error {
	klog.Infof("Delete net-attach-def %s", netAttachDefName)
	return cnm.perNadNetConfInfo.DoWithLock(netAttachDefName, func(nadName string) error {
		existingNadNetConfInfo, found := cnm.perNadNetConfInfo.Load(nadName)
		if !found {
			klog.V(5).Infof("net-attach-def %s not found", nadName)
			return nil
		}
		err := cnm.deleteNadFromController(existingNadNetConfInfo.netName, nadName)
		if err != nil {
			klog.Errorf("Failed to delete net-attach-def %s from network %s: %v", nadName, existingNadNetConfInfo.netName, err)
			return err
		}
		cnm.perNadNetConfInfo.Delete(nadName)
		return nil
	})
}

func (cnm *netAttachDefManager) addNadToController(ncm NetworkControllerManager, nadName string,
	nInfo util.NetInfo, netConfInfo util.NetConfInfo, doStart bool) error {
	var oc NetworkController
	var err error
	var nadExists, isStarted bool

	netName := nInfo.GetNetworkName()
	klog.V(5).Infof("Add net-attach-def %s to network %s", nadName, netName)
	return cnm.perNetworkNadNameInfo.DoWithLock(netName, func(networkName string) error {
		nni, loaded := cnm.perNetworkNadNameInfo.LoadOrStore(networkName, &nadNameInfo{
			nadNames:  map[string]bool{},
			oc:        nil,
			isStarted: false,
		})
		if !loaded {
			// first nad for this network, create controller
			klog.V(5).Infof("First net-attach-def %s of network %s added, create network controller", nadName, networkName)
			oc, err = ncm.NewNetworkController(nInfo, netConfInfo)
			if err != nil {
				cnm.perNetworkNadNameInfo.Delete(networkName)
				return fmt.Errorf("failed to create network controller for network %s: %v", networkName, err)
			}
			nni.oc = oc
		} else {
			klog.V(5).Infof("net-attach-def %s added to existing network %s", nadName, networkName)
			// controller of this network already exists
			oc = nni.oc
			isStarted = nni.isStarted
			_, nadExists = nni.nadNames[nadName]
		}
		if !nadExists {
			nni.nadNames[nadName] = true
			nni.oc.AddNad(nadName)
		}

		if !doStart || isStarted {
			return nil
		}

		klog.V(5).Infof("Start network controller for network %s", networkName)
		// start the controller if requested
		err = oc.Start(context.TODO())
		if err == nil {
			nni.isStarted = true
			return nil
		}

		if !nadExists {
			delete(nni.nadNames, nadName)
			nni.oc.DeleteNad(nadName)
		}
		if !loaded {
			klog.V(5).Infof("Delete network controller of network %s that is just created", networkName)
			cnm.perNetworkNadNameInfo.Delete(networkName)
		}
		return fmt.Errorf("network controller for network %s failed to be started: %v", networkName, err)
	})
}

func (cnm *netAttachDefManager) deleteNadFromController(netName, nadName string) error {
	klog.V(5).Infof("Delete net-attach-def %s from network %s", nadName, netName)
	return cnm.perNetworkNadNameInfo.DoWithLock(netName, func(networkName string) error {
		nni, found := cnm.perNetworkNadNameInfo.Load(networkName)
		if !found {
			klog.V(5).Infof("Network controller for network %s not found", networkName)
			return nil
		}
		_, nadExists := nni.nadNames[nadName]
		if !nadExists {
			klog.V(5).Infof("Nad %s does not exist on controller of network %s", nadName, networkName)
			return nil
		}

		oc := nni.oc
		klog.V(5).Infof("Delete nad %s from controller of network %s", nadName, networkName)
		delete(nni.nadNames, nadName)
		if len(nni.nadNames) == 0 {
			klog.V(5).Infof("The last nad %s of controller of network %s is deleted, stop controller", nadName, networkName)
			oc.Stop()
			err := oc.Cleanup(oc.GetNetworkName())
			// set isStarted to false even stop failed, as the operation could be half-done.
			// So if a new Nad with the same netconf comes in, it can restart the controller.
			nni.isStarted = false
			if err != nil {
				nni.nadNames[nadName] = true
				return fmt.Errorf("failed to stop network controller for network %s: %v", networkName, err)
			}
			cnm.perNetworkNadNameInfo.Delete(networkName)
		}
		nni.oc.DeleteNad(nadName)
		return nil
	})
}
