package util

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"

	nettypes "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	ovncnitypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/cni/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"

	kapi "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

var ErrorAttachDefNotOvnManaged = errors.New("net-attach-def not managed by OVN")

// NetInfo is structure which holds network name information
type NetInfo struct {
	// netconf's name, default for default network
	NetName string
	// Prefix of OVN logical entities for this network
	Prefix      string
	IsSecondary bool
	NadNames    *sync.Map
}

// NetConfInfo is structure which holds specific per-network information
type NetConfInfo interface {
	Compare(NetConfInfo) bool
}

// DefaultNetConfInfo is structure which holds default network information
type DefaultNetConfInfo struct{}

// L3NetConfInfo is structure which holds specific secondary L3 network information
type L3NetConfInfo struct {
	ProtoType string
	NetCidr   string
	MTU       int
}

func (netConfInfo *DefaultNetConfInfo) Compare(NetConfInfo) bool {
	// configuration of default network comes from command line; always return true
	return true
}

func (netConfInfo *L3NetConfInfo) Compare(newNetConfInfo NetConfInfo) bool {
	newL3NetConfInfo, ok := newNetConfInfo.(*L3NetConfInfo)
	if !ok || netConfInfo.NetCidr != newL3NetConfInfo.NetCidr || netConfInfo.MTU != newL3NetConfInfo.MTU {
		return false
	}
	return true
}

// AddNad adds the specified nad
func (nInfo *NetInfo) AddNad(nadName string) {
	klog.V(5).Infof("Add nad %s to network %s", nadName, nInfo.NetName)
	nInfo.NadNames.Store(nadName, true)
}

// DeleteNad deletes the specified nad and return true if no nads left
func (nInfo *NetInfo) DeleteNad(nadName string) bool {
	klog.V(5).Infof("Delete nad %s from network %s", nadName, nInfo.NetName)
	nInfo.NadNames.Delete(nadName)
	// check if there any other nads sharing the same CNI conf name left, if yes, just return
	nadLeft := false
	nInfo.NadNames.Range(func(key, value interface{}) bool {
		nadLeft = true
		return false
	})
	return !nadLeft
}

// CheckNadExist returns true if the given nad exists, used
// to check if the network is requested to be plumbed in a Pod
func (nInfo *NetInfo) CheckNadExist(nadName string) bool {
	_, ok := nInfo.NadNames.Load(nadName)
	return ok
}

// GetNetworkName returns the network name
func (nInfo *NetInfo) GetNetworkName() string {
	return nInfo.NetName
}

// GetNadKeyName returns key of NetAttachDefInfo.NetAttachDefs map, also used as Pod annotation key
func GetNadKeyName(namespace, name string) string {
	return fmt.Sprintf("%s/%s", namespace, name)
}

// GetSecondaryNetworkPrefix gets the string used as prefix of the logical entities
// of the secondary network of the given network name, in the form of <netName>_.
//
// Note that for port_group and address_set, it does not allow the '-' character,
// which will be replaced with ".". Also replace "/" in the nadName with "."
func GetSecondaryNetworkPrefix(netName string) string {
	name := strings.ReplaceAll(netName, "-", ".")
	name = strings.ReplaceAll(name, "/", ".")
	return name + "_"
}

// ParseNADInfo parses config in NAD spec and return a NetAttachDefInfo object
func ParseNADInfo(netattachdef *nettypes.NetworkAttachmentDefinition) (*NetInfo, NetConfInfo, error) {
	netconf, err := ParseNetConf(netattachdef)
	if err != nil {
		return nil, nil, err
	}

	netName := netconf.Name
	prefix := ""
	if netconf.IsSecondary {
		prefix = GetSecondaryNetworkPrefix(netName)
	}

	netInfo := NetInfo{
		NetName:     netconf.Name,
		Prefix:      prefix,
		IsSecondary: netconf.IsSecondary,
		NadNames:    &sync.Map{},
	}
	nadConfInfo := L3NetConfInfo{NetCidr: netconf.NetCidr, MTU: netconf.MTU}
	return &netInfo, &nadConfInfo, nil
}

// ParseNetConf parses config in NAD spec
func ParseNetConf(netattachdef *nettypes.NetworkAttachmentDefinition) (*ovncnitypes.NetConf, error) {
	netconf := &ovncnitypes.NetConf{MTU: config.Default.MTU}
	// looking for network attachment definition that use OVN K8S CNI only
	err := json.Unmarshal([]byte(netattachdef.Spec.Config), &netconf)
	if err != nil {
		return nil, fmt.Errorf("error parsing Network Attachment Definition %s/%s: %v", netattachdef.Namespace, netattachdef.Name, err)
	}
	// skip non-OVN nad or primary network OVN NAD
	if netconf.Type != "ovn-k8s-cni-overlay" || !netconf.IsSecondary {
		return nil, ErrorAttachDefNotOvnManaged
	}

	nadKey := GetNadKeyName(netattachdef.Namespace, netattachdef.Name)
	if netconf.NadName != nadKey {
		return nil, fmt.Errorf("net-attach-def name (%s) is inconsistent with config (%s)", nadKey, netconf.NadName)
	}

	if netconf.Name == "" {
		netconf.Name = netattachdef.Name
	}

	// validation
	if !netconf.IsSecondary {
		netconf.Name = types.DefaultNetworkName
	} else {
		if netconf.Name == types.DefaultNetworkName {
			return nil, fmt.Errorf("netconf name cannot be %s for secondary network net-attach-def", types.DefaultNetworkName)
		}
	}

	return netconf, nil
}

// See if this pod needs to plumb over this given network specified by netconf,
// and return the matching NetworkSelectionElement if any exists.
//
// Return value:
//    bool: if this Pod is on this Network; true or false
//    *networkattachmentdefinitionapi.NetworkSelectionElement: all NetworkSelectionElement that pod is requested for the specified network
//    error:  error in case of failure
func IsNetworkOnPod(pod *kapi.Pod, netInfo *NetInfo) (bool, *nettypes.NetworkSelectionElement, error) {
	podDesc := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
	if netInfo == nil || !netInfo.IsSecondary {
		defaultNetwork, err := GetK8sPodDefaultNetwork(pod)
		if err != nil {
			// multus won't add this Pod if this fails, should never happen
			return false, nil, fmt.Errorf("failed to get default network for pod %s: %v", podDesc, err)
		}
		return true, defaultNetwork, nil
	}

	// For non-default network controller, try to see if its name exists in the Pod's k8s.v1.cni.cncf.io/networks, if no,
	// return false;
	allNetworks, err := GetK8sPodAllNetworks(pod)
	if err != nil {
		return false, nil, err
	}

	nses := make([]*nettypes.NetworkSelectionElement, 0, len(allNetworks))
	for _, network := range allNetworks {
		nadKeyName := GetNadKeyName(network.Namespace, network.Name)
		if netInfo.CheckNadExist(nadKeyName) {
			nses = append(nses, network)
		}
	}
	if len(nses) > 1 {
		return false, nil, fmt.Errorf("unexpected error: more than one nad of the network %s specified for pod %s",
			netInfo.NetName, podDesc)
	} else if len(nses) == 0 {
		return false, nil, nil
	}
	return true, nses[0], nil
}

// GetPodLogicalPortNames gets logical port names on the given network
func GetPodLogicalPortNames(pod *kapi.Pod, netInfo *NetInfo) (string, error) {
	on, network, err := IsNetworkOnPod(pod, netInfo)
	if err != nil {
		return "", err
	}
	if !on {
		return "", fmt.Errorf("pod %s/%s is not on the given network %s", pod.Namespace, pod.Name, netInfo.NetName)
	}
	// the pod is attached to this given network
	portName := GetLogicalPortName(pod.Namespace, pod.Name)
	if netInfo.IsSecondary {
		nadName := GetNadKeyName(network.Namespace, network.Name)
		portName = GetSecondaryNetworkLogicalPortName(pod.Namespace, pod.Name, nadName)
	}
	return portName, nil
}
