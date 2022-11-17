package types

import (
	"github.com/containernetworking/cni/pkg/types"
	"net"
)

// NetConf is CNI NetConf with DeviceID
type NetConf struct {
	types.NetConf

	// set to true if it is a secondary networkattachmentdefintion
	IsSecondary bool `json:"isSecondary,omitempty"`
	// specifies the OVN topology for this network configuration
	// when not specified, by default it is Layer3AttachDefTopoType
	Topology string `json:"topology,omitempty"`
	// captures net-attach-def name in the form of namespace/name
	NadName string `json:"netAttachDefName,omitempty"`
	// Network MTU
	MTU int `json:"mtu,omitempty"`
	// Network Cidr
	// for secondary layer3 network, eg. 10.128.0.0/14/23
	// for localnet or layer2 network, eg. 10.1.130.0/24
	NetCidr string `json:"netCIDR,omitempty"`
	// VlanID, valid in localnet topology network only
	VlanId int `json:"vlanId,omitempty"`
	// list of IPs, expressed with prefix length, to be excluded from being allocated for Pod
	// valid for localnet or layer 2 network topology
	ExcludeCIDRs []string `json:"excludeCidrs,omitempty"`

	// PciAddrs in case of using sriov
	DeviceID string `json:"deviceID,omitempty"`
	// LogFile to log all the messages from cni shim binary to
	LogFile string `json:"logFile,omitempty"`
	// Level is the logging verbosity level
	LogLevel string `json:"logLevel,omitempty"`
	// LogFileMaxSize is the maximum size in bytes of the logfile
	// before it gets rolled.
	LogFileMaxSize int `json:"logfile-maxsize"`
	// LogFileMaxBackups represents the the maximum number of
	// old log files to retain
	LogFileMaxBackups int `json:"logfile-maxbackups"`
	// LogFileMaxAge represents the maximum number
	// of days to retain old log files
	LogFileMaxAge int `json:"logfile-maxage"`
}

// NetworkSelectionElement represents one element of the JSON format
// Network Attachment Selection Annotation as described in section 4.1.2
// of the CRD specification.
type NetworkSelectionElement struct {
	// Name contains the name of the Network object this element selects
	Name string `json:"name"`
	// Namespace contains the optional namespace that the network referenced
	// by Name exists in
	Namespace string `json:"namespace,omitempty"`
	// MacRequest contains an optional requested MAC address for this
	// network attachment
	MacRequest string `json:"mac,omitempty"`
	// GatewayRequest contains default route IP address for the pod
	GatewayRequest []net.IP `json:"default-route,omitempty"`
}
