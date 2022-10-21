package networkControllerManager

import (
	"context"
	"fmt"

	nettypes "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	"k8s.io/klog/v2"
)

// controllerNameManager holds all controller of all nad for all networks
// It is updated either at the very beginning of ovnkube-master when initializing the default controller
// or when net-attach-def is added/deleted. All these are serialized and no lock protection is needed
type controllerNameManager struct {
	defaultNetworkController NetworkController
	// key is nadName, value is netName for secondary network controller
	controllersByNadName map[string]string
	// controller for all secondasry networks, key is netName of net-attach-def, value is *Controller
	// this map is updated either at the very beginning of ovnkube-master when initializing the default controller
	// or when net-attach-def is added/deleted. All these are serialized and no lock protection is needed
	networkControllers map[string]SecondaryNetworkController
}

// SyncSecondaryNetworkNad update controller with the given nad. It creates the controller if this is the first nad
// of the network, or delete the controller if this is the last nad of the network.
//
// nil netattachdef indicate the specified nad no longer exists
// note that for errors that are not retriable (configuration error etc.), just log the error and return nil
func (cnm *controllerNameManager) SyncSecondaryNetworkNad(cc *ovn.BaseNetworkController,
	netattachdef *nettypes.NetworkAttachmentDefinition, nadName string, doStart bool) error {
	var netConfInfo util.NetConfInfo
	var nInfo util.NetInfo
	var err error

	nInfo = (*util.NetNameInfo)(nil)
	if netattachdef != nil {
		nInfo, netConfInfo, err = util.ParseNADInfo(netattachdef)
		if err == nil && !nInfo.IsSecondary() {
			err = fmt.Errorf("net-attach-def %s is default network, skip", nadName)
		}
	} else {
		err = fmt.Errorf("net-attach-def %s not longer exists", nadName)
	}

	netName, nadExists := cnm.controllersByNadName[nadName]
	if nadExists {
		// if the nad spec has error or it netName has changed, delete the existing nad first
		if err == nil && netName != nInfo.GetNetworkName() {
			err = fmt.Errorf("net-attach-def %s netconf name changed from %s to %s", nadName, netName, nInfo.GetNetworkName())
		}
		if err != nil {
			klog.V(5).Infof("Delete Nad %s: %v", nadName, err)
			err = cnm.DeleteSecondaryNetworkNad(nadName)
			if err != nil {
				klog.Errorf("Deleting nad %s failed %v", nadName, err)
				return err
			}
		}
	}

	// if nad no longer exist, or its spec is invalid, directly return
	if err != nil {
		klog.V(5).Infof("Nothing to do with Nad %s: %v", nadName, err)
		return nil
	}

	oc, ok := cnm.networkControllers[nInfo.GetNetworkName()]
	if ok {
		klog.V(5).Infof("Found controller %s for nad %s", nInfo.GetNetworkName(), nadName)
		if !oc.CompareNetConf(netConfInfo) {
			if nadExists {
				// netConf in spec has changed, delete this nad
				klog.V(5).Infof("Delete Nad %s: netconf spec changed", nadName)
				err = cnm.DeleteSecondaryNetworkNad(nadName)
				if err != nil {
					klog.Errorf("Deleting nad %s failed %v", nadName, err)
					return err
				}
			} else {
				klog.Errorf("Nad %s does not share the same CNI config with network %s",
					nadName, nInfo.GetNetworkName())
				return nil
			}
		}
	} else {
		klog.V(5).Infof("Nad %s is the first nad for network %s, creating controller", nadName, nInfo.GetNetworkName())
		l3NetConfInfo := netConfInfo.(*util.L3NetConfInfo)
		oc, err = ovn.NewSecondaryL3Controller(cc, nInfo, l3NetConfInfo)
		if err != nil {
			klog.Errorf("Create controller for network %s failed %v", nInfo.GetNetworkName(), err)
			return nil
		}
		cnm.networkControllers[nInfo.GetNetworkName()] = oc
	}
	oc.AddNad(nadName)
	cnm.controllersByNadName[nadName] = nInfo.GetNetworkName()
	if doStart {
		return oc.Start(context.TODO())
	}
	return nil
}

// DeleteSecondaryNetworkNad delete a nad from an existing controller and stop/delete
// the controller if this is the last nad
func (cnm *controllerNameManager) DeleteSecondaryNetworkNad(nadName string) error {
	netName, ok := cnm.controllersByNadName[nadName]
	if !ok {
		return nil
	}
	oc, ok := cnm.networkControllers[netName]
	if !ok {
		return fmt.Errorf("failed to find controller of the given nad %s", nadName)
	}
	isLastNad := oc.DeleteNad(nadName)
	if isLastNad {
		klog.V(5).Infof("Last nad %s is deleted from network %s, delete controller now...", nadName, netName)
		err := oc.Stop(true)
		if err != nil {
			return err
		}
		delete(cnm.networkControllers, netName)
	}
	delete(cnm.controllersByNadName, nadName)
	return nil
}
