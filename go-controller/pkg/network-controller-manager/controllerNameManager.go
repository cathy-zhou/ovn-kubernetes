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
	// key is nadName, value is netName
	controllersByNadName map[string]string
	// controller for all networks, key is netName of net-attach-def, value is Controller, and NadNames map
	ovnControllers map[string]ovn.Controller
}

// AddSecondaryNetworkNad update controller with the given nad. It creates the controller if this is the
// first nad of the network or add the Nad to the controller
func (cnm *controllerNameManager) AddSecondaryNetworkNad(cc *ovn.BaseNetworkController,
	netattachdef *nettypes.NetworkAttachmentDefinition) (ovn.Controller, error) {
	nadName := fmt.Sprintf("%s/%s", netattachdef.Namespace, netattachdef.Name)
	netInfo, netConfInfo, err := util.ParseNADInfo(netattachdef)
	if err != nil {
		if err == util.ErrorAttachDefNotOvnManaged {
			return nil, nil
		}
		return nil, err
	}
	if !netInfo.IsSecondary {
		return nil, nil
	}
	oc, ok := cnm.ovnControllers[netInfo.NetName]
	if ok {
		if !oc.CompareNetConf(netConfInfo) {
			return nil, fmt.Errorf("network attachment definition %s does not share the same CNI config with network %s",
				nadName, netInfo.NetName)
		}
	} else {
		oc, err = ovn.NewSecondaryL3Controller(cc, netInfo, netConfInfo)
		if err != nil {
			return nil, err
		}
		cnm.ovnControllers[netInfo.NetName] = oc
	}
	oc.GetNetInfo().AddNad(nadName)
	cnm.controllersByNadName[nadName] = netInfo.NetName
	return oc, nil
}

// SyncSecondaryNetworkNad update controller with the given nad. It creates the controller if this is the first nad
// of the network, or delete the controller if this is the last nad of the network.
//
// nil netattachdef indicate the specified nad no longer exists
// note that for errors that are not retriable (configuration error etc.), just log the error and return nil
func (cnm *controllerNameManager) SyncSecondaryNetworkNad(cc *ovn.BaseNetworkController,
	netattachdef *nettypes.NetworkAttachmentDefinition, nadName string, doStart bool) error {
	var netInfo *util.NetInfo
	var netConfInfo util.NetConfInfo
	var err error
	var reason string

	if netattachdef != nil {
		netInfo, netConfInfo, err = util.ParseNADInfo(netattachdef)
		if err != nil {
			reason = "is invalid"
			if err == util.ErrorAttachDefNotOvnManaged {
				reason = "is not managed by OVN"
			}
		} else if !netInfo.IsSecondary {
			netInfo = nil
			reason = "is default network, skip"
			err = fmt.Errorf("net-attach-def is %s", reason)
		}
	} else {
		reason = "not longer exists"
	}

	netName, nadExists := cnm.controllersByNadName[nadName]
	if nadExists {
		// if the specific nad no longer exists, or its spec has changed to be invalid,
		// or it netName has changed, delete the existing nad first
		if netattachdef == nil || err != nil || netName != netInfo.NetName {
			if netInfo != nil && netName != netInfo.NetName {
				reason = "netConf name changed"
			}
			klog.V(5).Infof("Nad %s %s, delete it", nadName, reason)
			err = cnm.DeleteSecondaryNetworkNad(nadName)
			if err != nil {
				klog.Errorf("Deleting nad %s failed %v", nadName, err)
				return err
			}
		}
	}

	// if nad no longer exist, or its spec is now invalid, directly return
	if netInfo == nil {
		klog.V(5).Infof("Nad %s %s, nothing to do", nadName, reason)
		return nil
	}

	oc, ok := cnm.ovnControllers[netInfo.NetName]
	if ok {
		klog.V(5).Infof("Found controller %s for nad %s", netInfo.NetName, nadName)
		if !oc.CompareNetConf(netConfInfo) {
			if nadExists {
				reason = "netConf spec changed"
				klog.V(5).Infof("Nad %s %s, delete it", nadName, reason)
				// netConf in spec has changed, delete this nad
				err = cnm.DeleteSecondaryNetworkNad(nadName)
				if err != nil {
					klog.Errorf("Deleting nad %s failed %v", nadName, err)
					return err
				}
			} else {
				klog.Errorf("Nad %s does not share the same CNI config with network %s",
					nadName, netInfo.NetName)
				return nil
			}
		}
	} else {
		klog.V(5).Infof("Nad %s is the first nad for network %s, creating controller", nadName, netInfo.NetName)
		oc, err = ovn.NewSecondaryL3Controller(cc, netInfo, netConfInfo)
		if err != nil {
			klog.Errorf("Create controller for network %s failed %v", netInfo.NetName, err)
			return nil
		}
		cnm.ovnControllers[netInfo.NetName] = oc
	}
	oc.GetNetInfo().AddNad(nadName)
	cnm.controllersByNadName[nadName] = netInfo.NetName
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
	oc, ok := cnm.ovnControllers[netName]
	if !ok {
		return fmt.Errorf("failed to find controller of the given nad %s", nadName)
	}
	isLastNad := oc.GetNetInfo().DeleteNad(nadName)
	if isLastNad {
		klog.V(5).Infof("Last nad %s is deleted from network %s, delete controller now...", nadName, netName)
		err := oc.Stop(true)
		if err != nil {
			return err
		}
		delete(cnm.ovnControllers, netName)
		delete(cnm.controllersByNadName, nadName)
	}
	delete(cnm.controllersByNadName, nadName)
	return nil
}
