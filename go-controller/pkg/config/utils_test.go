package config

import (
	"net"
	"testing"
)

func returnIPNetPointers(input string) *net.IPNet {
	_, ipNet, _ := net.ParseCIDR(input)
	return ipNet
}

func TestParseClusterSubnetEntries(t *testing.T) {
	tests := []struct {
		name            string
		cmdLineArg      string
		clusterNetworks []CIDRNetworkEntry
		expectedErr     bool
	}{
		{
			name:            "Single CIDR correctly formatted",
			cmdLineArg:      "10.132.0.0/26/28",
			clusterNetworks: []CIDRNetworkEntry{{CIDR: returnIPNetPointers("10.132.0.0/26"), HostSubnetLength: 28}},
			expectedErr:     false,
		},
		{
			name:       "Two CIDRs correctly formatted",
			cmdLineArg: "10.132.0.0/26/28,10.133.0.0/26/28",
			clusterNetworks: []CIDRNetworkEntry{
				{CIDR: returnIPNetPointers("10.132.0.0/26"), HostSubnetLength: 28},
				{CIDR: returnIPNetPointers("10.133.0.0/26"), HostSubnetLength: 28},
			},
			expectedErr: false,
		},
		{
			name:            "Test that defaulting to hostsubnetlength 8 still works",
			cmdLineArg:      "10.128.0.0/14",
			clusterNetworks: []CIDRNetworkEntry{{CIDR: returnIPNetPointers("10.128.0.0/14"), HostSubnetLength: 24}},
			expectedErr:     false,
		},
		{
			name:            "empty cmdLineArg",
			cmdLineArg:      "",
			clusterNetworks: nil,
			expectedErr:     true,
		},
		{
			name:            "improperly formated CIDR",
			cmdLineArg:      "10.132.0.-/26/28",
			clusterNetworks: nil,
			expectedErr:     true,
		},
		{
			name:            "negative HostsubnetLength",
			cmdLineArg:      "10.132.0.0/26/-22",
			clusterNetworks: nil,
			expectedErr:     true,
		},
		{
			name:            "HostSubnetLength smaller than cluster subnet",
			cmdLineArg:      "10.132.0.0/26/24",
			clusterNetworks: nil,
			expectedErr:     true,
		},
		{
			name:            "Cluster Subnet length too large",
			cmdLineArg:      "10.132.0.0/25",
			clusterNetworks: nil,
			expectedErr:     true,
		},
		{
			name:            "Cluster Subnet length same as host subnet length",
			cmdLineArg:      "10.132.0.0/24/24",
			clusterNetworks: nil,
			expectedErr:     true,
		},
		{
			name:            "Test that defaulting to hostsubnetlength with 24 bit cluster prefix fails",
			cmdLineArg:      "10.128.0.0/24",
			clusterNetworks: nil,
			expectedErr:     true,
		},
		{
			name:            "IPv6",
			cmdLineArg:      "fda6::/48/64",
			clusterNetworks: []CIDRNetworkEntry{{CIDR: returnIPNetPointers("fda6::/48"), HostSubnetLength: 64}},
			expectedErr:     false,
		},
		{
			name:            "IPv6 defaults to /64 hostsubnets",
			cmdLineArg:      "fda6::/48",
			clusterNetworks: []CIDRNetworkEntry{{CIDR: returnIPNetPointers("fda6::/48"), HostSubnetLength: 64}},
			expectedErr:     false,
		},
		{
			name:            "IPv6 doesn't allow longer than /64 hostsubnet",
			cmdLineArg:      "fda6::/48/56",
			clusterNetworks: nil,
			expectedErr:     true,
		},
		{
			name:            "IPv6 doesn't allow shorter than /64 hostsubnet",
			cmdLineArg:      "fda6::/48/72",
			clusterNetworks: nil,
			expectedErr:     true,
		},
		{
			name:            "IPv6 can't use /64 cluster net",
			cmdLineArg:      "fda6::/64",
			clusterNetworks: nil,
			expectedErr:     true,
		},
	}

	for _, tc := range tests {

		parsedListMap, err := ParseClusterSubnetEntries(tc.cmdLineArg)
		if err != nil && !tc.expectedErr {
			t.Errorf("Test case \"%s\" expected no errors, got %v", tc.name, err)
		}
		if len(tc.clusterNetworks) != len(parsedListMap[DefaultNodeSubnetZoneName]) {
			t.Errorf("Test case \"%s\" expected to output the same number of entries as parseClusterSubnetEntries", tc.name)
		} else {
			for index, entry := range parsedListMap[DefaultNodeSubnetZoneName] {
				if entry.CIDR.String() != tc.clusterNetworks[index].CIDR.String() {
					t.Errorf("Test case \"%s\" expected entry[%d].CIDR: %s to equal tc.clusterNetworks[%d].CIDR: %s", tc.name, index, entry.CIDR.String(), index, tc.clusterNetworks[index].CIDR.String())
				}
				if entry.HostSubnetLength != tc.clusterNetworks[index].HostSubnetLength {
					t.Errorf("Test case \"%s\" expected entry[%d].HostSubnetLength: %d to equal tc.clusterNetworks[%d].HostSubnetLength: %d", tc.name, index, entry.HostSubnetLength, index, tc.clusterNetworks[index].HostSubnetLength)
				}
			}
		}
	}
}

func TestCidrsOverlap(t *testing.T) {
	tests := []struct {
		name           string
		cidr           *net.IPNet
		cidrList       []CIDRNetworkEntry
		expectedOutput bool
	}{
		{
			name:           "empty cidrList",
			cidr:           returnIPNetPointers("10.132.0.0/26"),
			cidrList:       nil,
			expectedOutput: false,
		},
		{
			name: "non-overlapping",
			cidr: returnIPNetPointers("10.132.0.0/26"),
			cidrList: []CIDRNetworkEntry{
				{CIDR: returnIPNetPointers("10.133.0.0/26")},
			},
			expectedOutput: false,
		},
		{
			name: "cidr equal to an entry in the list",
			cidr: returnIPNetPointers("10.132.0.0/26"),
			cidrList: []CIDRNetworkEntry{
				{CIDR: returnIPNetPointers("10.132.0.0/26")},
			},
			expectedOutput: true,
		},
		{
			name: "proposed cidr is a proper subset of a list entry",
			cidr: returnIPNetPointers("10.132.0.0/26"),
			cidrList: []CIDRNetworkEntry{
				{CIDR: returnIPNetPointers("10.0.0.0/8")},
			},
			expectedOutput: true,
		},
		{
			name: "proposed cidr is a proper superset of a list entry",
			cidr: returnIPNetPointers("10.0.0.0/8"),
			cidrList: []CIDRNetworkEntry{
				{CIDR: returnIPNetPointers("10.133.0.0/26")},
			},
			expectedOutput: true,
		},
		{
			name: "proposed cidr overlaps a list entry",
			cidr: returnIPNetPointers("10.1.0.0/14"),
			cidrList: []CIDRNetworkEntry{
				{CIDR: returnIPNetPointers("10.0.0.0/15")},
			},
			expectedOutput: true,
		},
	}

	for _, tc := range tests {

		result := cidrsOverlap(tc.cidr, map[string][]CIDRNetworkEntry{DefaultNodeSubnetZoneName: tc.cidrList})
		if result != tc.expectedOutput {
			t.Errorf("testcase \"%s\" expected output %t cidrsOverlap returns %t", tc.name, tc.expectedOutput, result)
		}
	}
}

func TestJoinSubnetOverlaps(t *testing.T) {
	tests := []struct {
		name        string
		cidrList    []*net.IPNet
		shouldError bool
	}{
		{
			name:        "empty ipnet entries",
			cidrList:    nil,
			shouldError: false,
		},
		{
			name: "non-overlapping V4 ipnet entries",
			cidrList: []*net.IPNet{
				returnIPNetPointers("10.132.0.0/26"),
				returnIPNetPointers("192.168.0.0/16"),
				returnIPNetPointers("172.16.0.0/16"),
			},
			shouldError: false,
		},
		{
			name: "non-overlapping V6 ipnet entries",
			cidrList: []*net.IPNet{
				returnIPNetPointers("fd98:8000::/65"),
				returnIPNetPointers("fd99::/64"),
				returnIPNetPointers("fd98:1::/64"),
			},
			shouldError: false,
		},

		{
			name: "ipnet entry equal to V4 joinSubnet value",
			cidrList: []*net.IPNet{
				returnIPNetPointers("100.64.0.0/16"),
			},
			shouldError: true,
		},
		{
			name: "ipnet entry equal to V6 joinSubnet value",
			cidrList: []*net.IPNet{
				returnIPNetPointers("fd98::/64"),
			},
			shouldError: true,
		},
		{
			name: "ipnet entry overlapping with V4 joinSubnet value",
			cidrList: []*net.IPNet{
				returnIPNetPointers("100.64.128.0/17"),
			},
			shouldError: true,
		},
		{
			name: "ipnet entry overlapping with V6 joinSubnet value",
			cidrList: []*net.IPNet{
				returnIPNetPointers("fd98::/68"),
			},
			shouldError: true,
		},
		{
			name: "ipnet entry encompassing the V4 joinSubnet value",
			cidrList: []*net.IPNet{
				returnIPNetPointers("100.0.0.0/8"),
			},
			shouldError: true,
		},
		{
			name: "ipnet entry encompassing the V6 joinSubnet value",
			cidrList: []*net.IPNet{
				returnIPNetPointers("fd98::/63"),
			},
			shouldError: true,
		},
	}

	for _, tc := range tests {
		err := overlapsWithJoinSubnet(tc.cidrList)
		if err == nil && tc.shouldError {
			t.Errorf("testcase \"%s\" expected function overlapsWithJoinSubnet to return err, but it did not",
				tc.name)
		}
		if err != nil && !tc.shouldError {
			t.Errorf("testcase \"%s\" expected function overlapsWithJoinSubnet not to return err, but it did: %v",
				tc.name, err)
		}
	}
}
