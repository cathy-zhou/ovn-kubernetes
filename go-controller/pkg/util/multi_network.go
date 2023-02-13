package util

import (
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"

	nettypes "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	ovncnitypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/cni/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"

	kapi "k8s.io/api/core/v1"
	kerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"
)

var ErrorAttachDefNotOvnManaged = errors.New("net-attach-def not managed by OVN")

// NetInfo is interface which holds network name information
// for default network, this is set to nil
type NetInfo interface {
	GetNetworkName() string
	IsSecondary() bool
	GetPrefix() string
	GetNetworkScopedName(name string) string
	AddNAD(nadName string, nadConf *NADConfig)
	DeleteNAD(nadName string)
	HasNAD(nadName string) (bool, *NADConfig)
	//AddInterConnectNetwork(netName string, icInfo *InterConnectInfo)
	//DeleteInterConnectNetwork(netName string)
	//IterateInterConnectedNetwork(f func(netName string, icInfo *InterConnectInfo))
}

type BaseNetInfo struct {
	// all net-attach-def NAD names for this network, used to determine if a pod needs
	// to be plumbed for this network
	nadNames *sync.Map
	//// interconnection subnets, key is network name connected to this network,
	//// value is list of subnets belongs to the connected network
	//interConnectSubnets *sync.Map
}

type DefaultNetInfo struct {
	BaseNetInfo
}

// GetNetworkName returns the network name
func (nInfo *DefaultNetInfo) GetNetworkName() string {
	return types.DefaultNetworkName
}

// IsSecondary returns if this network is secondary
func (nInfo *DefaultNetInfo) IsSecondary() bool {
	return false
}

// GetPrefix returns if the logical entities prefix for this network
func (nInfo *DefaultNetInfo) GetPrefix() string {
	return ""
}

// GetNetworkScopedName returns if a name in this network scope
func (nInfo *DefaultNetInfo) GetNetworkScopedName(name string) string {
	return name
}

// AddNAD adds the specified NAD
func (nInfo *BaseNetInfo) AddNAD(nadName string, nadConf *NADConfig) {
	nInfo.nadNames.Store(nadName, nadConf)
}

// DeleteNAD deletes the specified NAD
func (nInfo *BaseNetInfo) DeleteNAD(nadName string) {
	nInfo.nadNames.Delete(nadName)
}

//// AddInterConnectNetwork adds the specified inter-connected network's inter-connect information
//func (nInfo *BaseNetInfo) AddInterConnectNetwork(netName string, icInfo *InterConnectInfo) {
//	nInfo.interConnectSubnets.Store(netName, icInfo)
//}
//
//// DeleteInterConnectNetwork deletes the specified inter-connected network's inter-connect information when disconnected
//func (nInfo *BaseNetInfo) DeleteInterConnectNetwork(netName string) {
//	nInfo.interConnectSubnets.Delete(netName)
//}
//
//// IterateInterConnectedSubnets iterate all inter-connected subnets
//func (nInfo *BaseNetInfo) IterateInterConnectedNetwork(f func(string, *InterConnectInfo)) {
//	nInfo.interConnectSubnets.Range(func(k, v interface{}) bool {
//		netName := k.(string)
//		icInfo := v.(*InterConnectInfo)
//		f(netName, icInfo)
//		return true
//	})
//}
//
// HasNAD returns true if the given NAD exists, already return true for
// default network
func (nInfo *BaseNetInfo) HasNAD(nadName string) (bool, *NADConfig) {
	var nadConf *NADConfig
	v, ok := nInfo.nadNames.Load(nadName)
	if v != nil {
		nadConf = v.(*NADConfig)
	}
	return ok, nadConf
}

// SecondaryNetInfo holds the network name information for secondary network if non-nil
type SecondaryNetInfo struct {
	BaseNetInfo
	// network name
	netName string
}

// GetNetworkName returns the network name
func (nInfo *SecondaryNetInfo) GetNetworkName() string {
	return nInfo.netName
}

// IsSecondary returns if this network is secondary
func (nInfo *SecondaryNetInfo) IsSecondary() bool {
	return true
}

// GetPrefix returns if the logical entities prefix for this network
func (nInfo *SecondaryNetInfo) GetPrefix() string {
	return GetSecondaryNetworkPrefix(nInfo.netName)
}

// GetNetworkScopedName returns if a name in this network scope
func (nInfo *SecondaryNetInfo) GetNetworkScopedName(name string) string {
	return nInfo.GetPrefix() + name
}

type InterConnectInfo struct {
	NetName string
	// the logical entity to connect to, it is either a logical router or a logical switch
	LogicalEntityToConnect interface{}
	// the subnets of this network to reach for, routes need to be added to the Pod of other network inter-connected with
	Subnets []*net.IPNet
}

// NetConfInfo is structure which holds specific per-network configuration
type NetConfInfo interface {
	CompareNetConf(NetConfInfo) bool
	TopologyType() string
	MTU() int
	NADToInterConnect() string
	InterConnectInfo(netInfo NetInfo) *InterConnectInfo
}

// DefaultNetConfInfo is structure which holds specific default network information
type DefaultNetConfInfo struct{}

// CompareNetConf compares the defaultNetConfInfo with the given newNetConfInfo and returns true
// unless the given newNetConfInfo is not the type of DefaultNetConfInfo
func (defaultNetConfInfo *DefaultNetConfInfo) CompareNetConf(newNetConfInfo NetConfInfo) bool {
	_, ok := newNetConfInfo.(*DefaultNetConfInfo)
	if !ok {
		klog.V(5).Infof("New netconf is different, expect default network netconf")
		return false
	}
	return true
}

// TopologyType returns the defaultNetConfInfo's topology type which is empty
func (defaultNetConfInfo *DefaultNetConfInfo) TopologyType() string {
	return ""
}

// MTU returns the defaultNetConfInfo's MTU value
func (defaultNetConfInfo *DefaultNetConfInfo) MTU() int {
	return config.Default.MTU
}

// NADToInterConnect returns the NAD this network is requested to inter-connected with
func (defaultNetConfInfo *DefaultNetConfInfo) NADToInterConnect() string {
	return ""
}

// InterConnectInfo returns the information used by inter-connection
func (defaultNetConfInfo *DefaultNetConfInfo) InterConnectInfo(netInfo NetInfo) *InterConnectInfo {
	subnets := make([]*net.IPNet, 0, len(config.Default.ClusterSubnets))
	for i, cidr := range config.Default.ClusterSubnets {
		subnets[i] = cidr.CIDR
	}

	logicalRouter := &nbdb.LogicalRouter{
		Name: types.OVNClusterRouter,
	}

	return &InterConnectInfo{
		NetName:                netInfo.GetNetworkName(),
		LogicalEntityToConnect: logicalRouter,
		Subnets:                subnets,
	}
}

func isSubnetsStringEqual(subnetsString, newSubnetsString string) bool {
	subnetsStringList := strings.Split(subnetsString, ",")
	newSubnetsStringList := strings.Split(newSubnetsString, ",")
	if len(subnetsStringList) != len(newSubnetsStringList) {
		return false
	}
	for index := range subnetsStringList {
		subnetsStringList[index] = strings.TrimSpace(subnetsStringList[index])
	}
	for index := range newSubnetsStringList {
		newSubnetsStringList[index] = strings.TrimSpace(newSubnetsStringList[index])
	}
	sort.Strings(subnetsStringList)
	sort.Strings(newSubnetsStringList)
	for i, subnetString := range subnetsStringList {
		if subnetString != newSubnetsStringList[i] {
			return false
		}
	}
	return true
}

// parseSubnetsString parses comma-seperated subnet string and returns the list of subnets
func parseSubnetsString(clusterSubnetString string) ([]*net.IPNet, error) {
	var subnetList []*net.IPNet

	if strings.TrimSpace(clusterSubnetString) == "" {
		return subnetList, nil
	}

	subnetStringList := strings.Split(clusterSubnetString, ",")
	for _, subnetString := range subnetStringList {
		subnetString = strings.TrimSpace(subnetString)
		_, subnet, err := net.ParseCIDR(subnetString)
		if err != nil {
			return nil, err
		}

		subnetList = append(subnetList, subnet)
	}
	return subnetList, nil
}

// Layer3NetConfInfo is structure which holds specific secondary layer3 network information
type Layer3NetConfInfo struct {
	subnets        string
	mtu            int
	ClusterSubnets []config.CIDRNetworkEntry
}

// CompareNetConf compares the layer3NetConfInfo with the given newNetConfInfo and returns true
// if they share the same netconf information
func (layer3NetConfInfo *Layer3NetConfInfo) CompareNetConf(newNetConfInfo NetConfInfo) bool {
	var errs []error
	var err error

	newLayer3NetConfInfo, ok := newNetConfInfo.(*Layer3NetConfInfo)
	if !ok {
		klog.V(5).Infof("New netconf topology type is different, expect %s",
			layer3NetConfInfo.TopologyType())
		return false
	}

	if !isSubnetsStringEqual(layer3NetConfInfo.subnets, newLayer3NetConfInfo.subnets) {
		err = fmt.Errorf("new %s netconf subnets %v has changed, expect %v",
			types.Layer3Topology, newLayer3NetConfInfo.subnets, layer3NetConfInfo.subnets)
		errs = append(errs, err)
	}

	if layer3NetConfInfo.mtu != newLayer3NetConfInfo.mtu {
		err = fmt.Errorf("new %s netconf mtu %v has changed, expect %v",
			types.Layer3Topology, newLayer3NetConfInfo.mtu, layer3NetConfInfo.mtu)
		errs = append(errs, err)
	}
	if len(errs) != 0 {
		err = kerrors.NewAggregate(errs)
		klog.V(5).Infof(err.Error())
		return false
	}
	return true
}

func newLayer3NetConfInfo(netconf *ovncnitypes.NetConf) (*Layer3NetConfInfo, error) {
	clusterSubnets, err := config.ParseClusterSubnetEntries(netconf.Subnets)
	if err != nil {
		return nil, fmt.Errorf("cluster subnet %s is invalid: %v", netconf.Subnets, err)
	}

	return &Layer3NetConfInfo{
		subnets:        netconf.Subnets,
		mtu:            netconf.MTU,
		ClusterSubnets: clusterSubnets,
	}, nil
}

// TopologyType returns the layer3NetConfInfo's topology type which is layer3 topology
func (layer3NetConfInfo *Layer3NetConfInfo) TopologyType() string {
	return types.Layer3Topology
}

// MTU returns the layer3NetConfInfo's MTU value
func (layer3NetConfInfo *Layer3NetConfInfo) MTU() int {
	return layer3NetConfInfo.mtu
}

// NADToInterConnect returns the NAD this network is requested to inter-connected with
func (layer3NetConfInfo *Layer3NetConfInfo) NADToInterConnect() string {
	return ""
}

// InterConnectInfo returns the information used by inter-connection
func (layer3NetConfInfo *Layer3NetConfInfo) InterConnectInfo(netInfo NetInfo) *InterConnectInfo {
	subnets := make([]*net.IPNet, len(config.Default.ClusterSubnets), len(config.Default.ClusterSubnets))
	for i, cidr := range layer3NetConfInfo.ClusterSubnets {
		subnets[i] = cidr.CIDR
	}

	logicalRouter := &nbdb.LogicalRouter{
		Name: netInfo.GetNetworkScopedName(types.OVNClusterRouter),
	}

	return &InterConnectInfo{
		NetName:                netInfo.GetNetworkName(),
		LogicalEntityToConnect: logicalRouter,
		Subnets:                subnets,
	}
}

// Layer2NetConfInfo is structure which holds specific secondary layer2 network information
type Layer2NetConfInfo struct {
	subnets        string
	mtu            int
	excludeSubnets string

	ClusterSubnets []*net.IPNet
	ExcludeSubnets []*net.IPNet
	ConnectToNAD   string
}

// CompareNetConf compares the layer2NetConfInfo with the given newNetConfInfo and returns true
// if they share the same netconf information
func (layer2NetConfInfo *Layer2NetConfInfo) CompareNetConf(newNetConfInfo NetConfInfo) bool {
	var errs []error
	var err error
	newLayer2NetConfInfo, ok := newNetConfInfo.(*Layer2NetConfInfo)
	if !ok {
		klog.V(5).Infof("New netconf topology type is different, expect %s",
			layer2NetConfInfo.TopologyType())
		return false
	}
	if !isSubnetsStringEqual(layer2NetConfInfo.subnets, newLayer2NetConfInfo.subnets) {
		err = fmt.Errorf("new %s netconf subnets %v has changed, expect %v",
			types.Layer2Topology, newLayer2NetConfInfo.subnets, layer2NetConfInfo.subnets)
		errs = append(errs, err)
	}
	if layer2NetConfInfo.mtu != newLayer2NetConfInfo.mtu {
		err = fmt.Errorf("new %s netconf mtu %v has changed, expect %v",
			types.Layer2Topology, newLayer2NetConfInfo.mtu, layer2NetConfInfo.mtu)
		errs = append(errs, err)
	}
	if !isSubnetsStringEqual(layer2NetConfInfo.excludeSubnets, newLayer2NetConfInfo.excludeSubnets) {
		err = fmt.Errorf("new %s netconf excludeSubnets %v has changed, expect %v",
			types.Layer2Topology, newLayer2NetConfInfo.excludeSubnets, layer2NetConfInfo.excludeSubnets)
		errs = append(errs, err)
	}
	if layer2NetConfInfo.ConnectToNAD != newLayer2NetConfInfo.ConnectToNAD {
		err = fmt.Errorf("new %s netconf connect %v has changed, expect %v",
			types.Layer2Topology, newLayer2NetConfInfo.excludeSubnets, layer2NetConfInfo.excludeSubnets)
		errs = append(errs, err)
	}
	if len(errs) != 0 {
		err = kerrors.NewAggregate(errs)
		klog.V(5).Infof(err.Error())
		return false
	}
	return true
}

func newLayer2NetConfInfo(netconf *ovncnitypes.NetConf, annotations map[string]string) (*Layer2NetConfInfo, error) {
	clusterSubnets, excludeSubnets, err := verifyExcludeIPs(netconf.Subnets, netconf.ExcludeSubnets)
	if err != nil {
		return nil, fmt.Errorf("invalid %s netconf %s: %v", netconf.Topology, netconf.Name, err)
	}

	nad := annotations[types.OvnK8sConnectToNad]

	return &Layer2NetConfInfo{
		subnets:        netconf.Subnets,
		mtu:            netconf.MTU,
		excludeSubnets: netconf.ExcludeSubnets,
		ClusterSubnets: clusterSubnets,
		ExcludeSubnets: excludeSubnets,
		ConnectToNAD:   nad,
	}, nil
}

func verifyExcludeIPs(subnetsString string, excludeSubnetsString string) ([]*net.IPNet, []*net.IPNet, error) {
	clusterSubnets, err := parseSubnetsString(subnetsString)
	if err != nil {
		return nil, nil, fmt.Errorf("subnets %s is invalid: %v", subnetsString, err)
	}
	if len(clusterSubnets) == 0 {
		return nil, nil, fmt.Errorf("subnets is not defined")
	}

	excludeSubnets, err := parseSubnetsString(excludeSubnetsString)
	if err != nil {
		return nil, nil, fmt.Errorf("excludeSubnets %s is invalid: %v", excludeSubnetsString, err)
	}

	for _, excludeSubnet := range excludeSubnets {
		found := false
		for _, subnet := range clusterSubnets {
			if ContainsCIDR(subnet, excludeSubnet) {
				found = true
				break
			}
		}
		if !found {
			return nil, nil, fmt.Errorf("the provided network subnets %v does not contain exluded subnets %v",
				clusterSubnets, excludeSubnet)
		}
	}

	return clusterSubnets, excludeSubnets, nil
}

// TopologyType returns the layer2NetConfInfo's topology type
func (layer2NetConfInfo *Layer2NetConfInfo) TopologyType() string {
	return types.Layer2Topology
}

// MTU returns the layer2NetConfInfo's MTU value
func (layer2NetConfInfo *Layer2NetConfInfo) MTU() int {
	return layer2NetConfInfo.mtu
}

// NADToInterConnect returns the NAD this network is requested to inter-connected with
func (layer2NetConfInfo *Layer2NetConfInfo) NADToInterConnect() string {
	return layer2NetConfInfo.ConnectToNAD
}

// InterConnectInfo returns the information used by inter-connection
func (layer2NetConfInfo *Layer2NetConfInfo) InterConnectInfo(netInfo NetInfo) *InterConnectInfo {
	logicalSwitch := &nbdb.LogicalSwitch{
		Name: netInfo.GetNetworkScopedName(types.OVNLayer2Switch),
	}
	return &InterConnectInfo{
		NetName:                netInfo.GetNetworkName(),
		LogicalEntityToConnect: logicalSwitch,
		Subnets:                layer2NetConfInfo.ClusterSubnets,
	}
}

// GetNADName returns key of NetAttachDefInfo.NetAttachDefs map, also used as Pod annotation key
func GetNADName(namespace, name string) string {
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

func newNetConfInfo(netconf *ovncnitypes.NetConf, annotations map[string]string) (NetConfInfo, error) {
	if netconf.Name == types.DefaultNetworkName {
		return &DefaultNetConfInfo{}, nil
	}
	switch netconf.Topology {
	case types.Layer3Topology:
		return newLayer3NetConfInfo(netconf)
	case types.Layer2Topology:
		return newLayer2NetConfInfo(netconf, annotations)
	default:
		// other topology NAD can be supported later
		return nil, fmt.Errorf("topology %s not supported", netconf.Topology)
	}
}

// ParseNADInfo parses config in NAD spec and return a NetAttachDefInfo object for secondary networks
func ParseNADInfo(netattachdef *nettypes.NetworkAttachmentDefinition) (NetInfo, NetConfInfo, *NADConfig, error) {
	netconf, err := ParseNetConf(netattachdef)
	if err != nil {
		return nil, nil, nil, err
	}

	netconfInfo, err := newNetConfInfo(netconf, netattachdef.Annotations)
	if err != nil {
		return nil, nil, nil, err
	}

	nadConf, err := GetNADConfig(netattachdef)
	if err != nil {
		return nil, nil, nil, err
	}
	return NewNetInfo(netconf), netconfInfo, nadConf, nil
}

func NewDefaultNetInfo() NetInfo {
	return &DefaultNetInfo{
		BaseNetInfo: BaseNetInfo{
			nadNames: &sync.Map{},
		},
	}
}

// ParseNetConf returns NetInfo for the given netconf
func NewNetInfo(netconf *ovncnitypes.NetConf) NetInfo {
	var nInfo NetInfo
	if netconf.Name == types.DefaultNetworkName {
		nInfo = NewDefaultNetInfo()
	} else {
		nInfo = &SecondaryNetInfo{
			BaseNetInfo: BaseNetInfo{
				nadNames: &sync.Map{},
			},
			netName: netconf.Name,
		}
	}
	return nInfo
}

// ParseNetConf parses config in NAD spec for secondary networks
func ParseNetConf(netattachdef *nettypes.NetworkAttachmentDefinition) (*ovncnitypes.NetConf, error) {
	netconf, err := config.ParseNetConf([]byte(netattachdef.Spec.Config))
	if err != nil {
		return nil, fmt.Errorf("error parsing Network Attachment Definition %s/%s: %v", netattachdef.Namespace, netattachdef.Name, err)
	}
	// skip non-OVN NAD
	if netconf.Type != "ovn-k8s-cni-overlay" {
		return nil, ErrorAttachDefNotOvnManaged
	}

	if netconf.Topology == "" {
		// NAD of default network
		netconf.Name = types.DefaultNetworkName
	} else {
		if netconf.NADName == "" {
			return nil, fmt.Errorf("missing NADName in secondary network netconf %s", netconf.Name)
		}
		// "ovn-kubernetes" network name is reserved for later
		if netconf.Name == "" || netconf.Name == types.DefaultNetworkName || netconf.Name == "ovn-kubernetes" {
			return nil, fmt.Errorf("invalid name in in secondary network netconf (%s)", netconf.Name)
		}
	}

	nadName := GetNADName(netattachdef.Namespace, netattachdef.Name)
	if netconf.NADName != "" {
		if netconf.NADName != nadName {
			return nil, fmt.Errorf("net-attach-def name (%s) is inconsistent with config (%s)", nadName, netconf.NADName)
		}
	} else {
		netconf.NADName = nadName
	}

	return netconf, nil
}

// per nad configuration, currently only rate limit config
type NADConfig struct {
}

// GetNADConfig returns the nad specific configuration obtained from the net-attach-def
func GetNADConfig(netattachdef *nettypes.NetworkAttachmentDefinition) (*NADConfig, error) {
	return &NADConfig{}, nil
}

// IsNADConfSame compares the given two NADConfig and returns true if they are the same
func IsNADConfSame(nadConf1 *NADConfig, nadConf2 *NADConfig) bool {
	return true
}

// GetPodNADToNetworkMapping sees if the given pod needs to plumb over this given network specified by netconf,
// and return the matching NetworkSelectionElement if any exists.
//
// Return value:
//    bool: if this Pod is on this Network; true or false
//    map[string]*nettypes.NetworkSelectionElement: all NetworkSelectionElement that pod is requested
//        for the specified network, key is NADName. Note multiple NADs of the same network are allowed
//        on one pod, as long as they are of different NADName.
//    error:  error in case of failure
func GetPodNADToNetworkMapping(pod *kapi.Pod, nInfo NetInfo) (bool, map[string]*nettypes.NetworkSelectionElement, error) {
	if pod.Spec.HostNetwork {
		return false, nil, nil
	}

	networkSelections := map[string]*nettypes.NetworkSelectionElement{}
	podDesc := fmt.Sprintf("%s/%s", pod.Namespace, pod.Name)
	if !nInfo.IsSecondary() {
		network, err := GetK8sPodDefaultNetworkSelection(pod)
		if err != nil {
			// multus won't add this Pod if this fails, should never happen
			return false, nil, fmt.Errorf("error getting default-network's network-attachment for pod %s: %v", podDesc, err)
		}
		if network != nil {
			nadName := GetNADName(network.Namespace, network.Name)
			if hasNAD, _ := nInfo.HasNAD(nadName); !hasNAD {
				return false, nil, fmt.Errorf("default-network's network-attachment-element for pod %s: %s is not managed by OVN",
					podDesc, nadName)
			}
			networkSelections[GetNADName(network.Namespace, network.Name)] = network
		}
		return true, networkSelections, nil
	}

	// For non-default network controller, try to see if its name exists in the Pod's k8s.v1.cni.cncf.io/networks, if no,
	// return false;
	allNetworks, err := GetK8sPodAllNetworkSelections(pod)
	if err != nil {
		return false, nil, err
	}

	for _, network := range allNetworks {
		nadName := GetNADName(network.Namespace, network.Name)
		if hasNAD, _ := nInfo.HasNAD(nadName); hasNAD {
			if _, ok := networkSelections[nadName]; ok {
				return false, nil, fmt.Errorf("unexpected error: more than one of the same NAD %s specified for pod %s",
					nadName, podDesc)
			}
			networkSelections[nadName] = network
		}
	}
	if len(networkSelections) == 0 {
		return false, nil, nil
	}
	return true, networkSelections, nil
}
