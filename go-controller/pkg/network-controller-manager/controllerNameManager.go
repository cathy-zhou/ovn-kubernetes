package networkControllerManager

import (
	"context"
	"fmt"
	nettypes "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/syncmap"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	"k8s.io/klog/v2"
)

type SecondaryNetworkControllerManager interface {
	AddSecondaryNetworkNad(cc *ovn.BaseNetworkController, netattachdef *nettypes.NetworkAttachmentDefinition, doStart bool) error
	DeleteSecondaryNetworkNad(nadName string) error
	GetAllControllers() []SecondaryNetworkController
}

// secondaryNetworkControllerNameManager holds all controller of all nad for all networks
// It is updated either at the very beginning of ovnkube-master when initializing the default controller
// or when net-attach-def is added/deleted.
// All secondary controller is added/deleted with lock protected by first perNadNetConfInfo (per nadName)
// then perNetworkNadNameInfo (per network name)
type secondaryNetworkControllerNameManager struct {
	// key is nadName, value is nadNetConfInfo
	perNadNetConfInfo *syncmap.SyncMap[*nadNetConfInfo]
	// controller for all secondary networks, key is netName of net-attach-def, value is NadNameInfo
	// this map is updated either at the very beginning of ovnkube-master when initializing the default controller
	// or when net-attach-def is added/deleted. All these are serialized by syncmap lock
	perNetworkNadNameInfo *syncmap.SyncMap[*nadNameInfo]
}

type nadNameInfo struct {
	nadNames  map[string]bool
	oc        SecondaryNetworkController
	isStarted bool
}

type nadNetConfInfo struct {
	util.NetConfInfo
	netName string
}

// AddSecondaryNetworkNad adds the given nad to the associated controller. It creates the controller if this
// is the first nad of the network
// note that for errors that are not retriable (configuration error etc.), just log the error and return nil
func (cnm *secondaryNetworkControllerNameManager) AddSecondaryNetworkNad(cc *ovn.BaseNetworkController,
	netattachdef *nettypes.NetworkAttachmentDefinition, doStart bool) error {
	var netConfInfo util.NetConfInfo
	var nInfo util.NetInfo
	var err error

	netAttachDefName := util.GetNadName(netattachdef.Namespace, netattachdef.Name)
	klog.Infof("Add net-attach-def %s", netAttachDefName)

	nInfo, netConfInfo, err = util.ParseNADInfo(netattachdef)
	if err == nil && !nInfo.IsSecondary() {
		err = fmt.Errorf("skip default network")
	}

	return cnm.perNadNetConfInfo.DoWithLock(netAttachDefName, func(nadName string) error {
		nadNetConfInfo, loaded := cnm.perNadNetConfInfo.LoadOrStore(nadName, &nadNetConfInfo{
			NetConfInfo: netConfInfo,
		})
		if !loaded {
			// first time to process this nad
			if err != nil {
				// invalid nad, nothing to do
				klog.Warningf("net-attach-def %s is invalid: %v", nadName, err)
				cnm.perNadNetConfInfo.Delete(nadName)
				return nil
			}
			nadNetConfInfo.netName = nInfo.GetNetworkName()
			err = cnm.addNadToController(nadName, cc, nInfo, netConfInfo, doStart)
			if err != nil {
				klog.Errorf("Failed to add net-attach-def %s to network %s: %v", nadName, nInfo.GetNetworkName(), err)
				cnm.perNadNetConfInfo.Delete(nadName)
				return err
			}

		} else {
			// this nad is already associated with a network controller
			if err != nil {
				klog.Warningf("net-attach-def %s is invalid: %v", nadName, err)
				return nil
			}
			klog.V(5).Infof("net-attach-def of the same name %s already processed: %v", nadName, nadNetConfInfo)
			// TBD, is it possible a delete event is missing? if so, we'd need to delete
			// the old nad from the associated network then add the new one
			if nadNetConfInfo.netName != nInfo.GetNetworkName() {
				// netconf network name changed or netconf spec changed
				klog.Warningf("net-attach-def %s network name %s has changed, expect %s, not supported",
					nInfo.GetNetworkName(), nadNetConfInfo.netName)
			} else if !nadNetConfInfo.CompareNetConf(netConfInfo) {
				// netconf network name changed or netconf spec changed
				klog.Warningf("net-attach-def %s spec has changed, not supported", nadName)
			}
			return nil
		}
		return nil
	})
}

// DeleteSecondaryNetworkNad deletes the given nad from the associated controller. It delete the controller if this
// is the last nad of the network
func (cnm *secondaryNetworkControllerNameManager) DeleteSecondaryNetworkNad(netAttachDefName string) error {
	klog.Infof("Delete net-attach-def %s", netAttachDefName)
	return cnm.perNadNetConfInfo.DoWithLock(netAttachDefName, func(nadName string) error {
		existingNadNetConfInfo, found := cnm.perNadNetConfInfo.Load(nadName)
		if !found {
			klog.Warningf("Nad %s not found", nadName)
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

// GetAllControllers returns a snapshot of all secondary controllers.
// Caller needs to note that there are no guarantees the return results reflect the real time
// condition. There maybe more controllers being added, and returned controllers nay be deleted
func (cnm *secondaryNetworkControllerNameManager) GetAllControllers() []SecondaryNetworkController {
	allNetworkNames := cnm.perNetworkNadNameInfo.GetKeys()
	allNetworkControllers := make([]SecondaryNetworkController, 0, len(allNetworkNames))
	for _, netName := range allNetworkNames {
		cnm.perNetworkNadNameInfo.LockKey(netName)
		defer cnm.perNetworkNadNameInfo.UnlockKey(netName)
		nni, ok := cnm.perNetworkNadNameInfo.Load(netName)
		if ok {
			allNetworkControllers = append(allNetworkControllers, nni.oc)
		}
	}
	return allNetworkControllers
}

func (cnm *secondaryNetworkControllerNameManager) addNadToController(nadName string, cc *ovn.BaseNetworkController,
	nInfo util.NetInfo, netConfInfo util.NetConfInfo, doStart bool) error {
	var oc SecondaryNetworkController
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
			topoType := netConfInfo.GetTopologyType()
			oc, err = createSecondaryNetworkController(topoType, cc, nInfo, netConfInfo)
			if err != nil {
				cnm.perNetworkNadNameInfo.Delete(networkName)
				return fmt.Errorf("failed to create secondary network controller for network %s: %v", networkName, err)
			}
			nni.oc = oc
		} else {
			klog.V(5).Infof("net-attach-def %s added to existing network %s", nadName, networkName)
			// controller of this network already exists
			oc = nni.oc
			isStarted = nni.isStarted
			_, nadExists = nni.nadNames[nadName]

			// netConf in spec has changed, delete this nad
			if !oc.CompareNetConf(netConfInfo) {
				if nadExists {
					// change of netConf spec is not supported, continue to start the existing controller if requested
					klog.Warningf("net-attach-def %s netconf spec changed, not supported", networkName)
				} else {
					// return error, it may be racing with an existing nad that was just failed to be deleted
					return fmt.Errorf("nad %s does not share the same CNI config with network %s",
						nadName, networkName)
				}
			}
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

		// if controller failed to start, undo start which deletes all the logical entities of this network
		if err := oc.Stop(true); err != nil {
			klog.Warningf("Failed to delete logical entities for network %s: %v", networkName, err)
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

func (cnm *secondaryNetworkControllerNameManager) deleteNadFromController(netName, nadName string) error {
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
			err := oc.Stop(true)
			// set isStarted to false even stop failed, if a new Nad race with this, it can restart the controller
			// TBD: what if they have different nadconf?
			nni.isStarted = false
			if err != nil {
				nni.nadNames[nadName] = true
				return fmt.Errorf("failed to delete network controller for network %s: %v", networkName, err)
			}
			cnm.perNetworkNadNameInfo.Delete(networkName)
		}
		nni.oc.DeleteNad(nadName)
		return nil
	})
}
