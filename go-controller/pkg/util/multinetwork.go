package util

import (
	"strings"

	ovncnitypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/cni/types"
	types "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
)

// NetNameInfo is structure which holds network name information
type NetNameInfo struct {
	// netconf's name, default for default network
	NetName string
	// Prefix of OVN logical entities for this network
	Prefix      string
	IsSecondary bool
}

// NetAttachDefInfo is structure which holds specific per-network information
type NetAttachDefInfo struct {
	NetNameInfo
	NetCidr string
	MTU     int
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

func NewNetAttachDefInfo(netconf *ovncnitypes.NetConf) (*NetAttachDefInfo, error) {
	netName := types.DefaultNetworkName
	prefix := ""
	if netconf.IsSecondary {
		netName = netconf.Name
		prefix = GetSecondaryNetworkPrefix(netName)
	}

	nadInfo := NetAttachDefInfo{
		NetNameInfo: NetNameInfo{NetName: netName, Prefix: prefix, IsSecondary: netconf.IsSecondary},
		NetCidr:     netconf.NetCidr,
		MTU:         netconf.MTU,
	}
	return &nadInfo, nil
}
