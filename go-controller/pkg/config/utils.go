package config

import (
	"fmt"
	"net"
	"strconv"
	"strings"
)

// CIDRNetworkEntry is the object that holds the definition for a single network CIDR range
type CIDRNetworkEntry struct {
	CIDR             *net.IPNet
	HostSubnetLength uint32
}

func (e CIDRNetworkEntry) HostBits() uint32 {
	_, addrLen := e.CIDR.Mask.Size()
	return uint32(addrLen) - e.HostSubnetLength
}

// ParseClusterSubnetEntries returns the parsed set of CIDRNetworkEntries passed by the user on the command line
// These entries define the clusters network space by specifying a set of CIDR and netmasks the SDN can allocate
// addresses from.
func ParseClusterSubnetEntries(clusterSubnetCmd string) (map[string][]CIDRNetworkEntry, error) {
	var parsedClusterListMap map[string][]CIDRNetworkEntry
	ipv6 := false
	clusterEntriesList := strings.Split(clusterSubnetCmd, ",")

	parsedClusterListMap = make(map[string][]CIDRNetworkEntry)
	for _, clusterEntryWithZone := range clusterEntriesList {
		var parsedClusterEntry CIDRNetworkEntry

		clusterEntryInfo := strings.SplitN(clusterEntryWithZone, "@", 2)

		// if no zone name is specified, assign the default zone name
		zoneName := DefaultNodeSubnetZoneName
		if len(clusterEntryInfo) == 2 {
			if !strings.HasPrefix(clusterEntryInfo[1], "zone=") {
				return nil, fmt.Errorf("zone name not properly formatted for %q", clusterEntryWithZone)
			}
			zoneName = strings.TrimPrefix(clusterEntryInfo[1], "zone=")
			if len(zoneName) == 0 {
				return nil, fmt.Errorf("invalid zone name %s for %q", zoneName, clusterEntryWithZone)
			}
		}
		clusterEntry := clusterEntryInfo[0]
		splitClusterEntry := strings.Split(clusterEntry, "/")

		if len(splitClusterEntry) < 2 || len(splitClusterEntry) > 3 {
			return nil, fmt.Errorf("CIDR %q not properly formatted", clusterEntry)
		}

		var err error
		_, parsedClusterEntry.CIDR, err = net.ParseCIDR(fmt.Sprintf("%s/%s", splitClusterEntry[0], splitClusterEntry[1]))
		if err != nil {
			return nil, err
		}

		if parsedClusterEntry.CIDR.IP.To4() == nil {
			ipv6 = true
		}

		entryMaskLength, _ := parsedClusterEntry.CIDR.Mask.Size()
		if len(splitClusterEntry) == 3 {
			tmp, err := strconv.ParseUint(splitClusterEntry[2], 10, 32)
			if err != nil {
				return nil, err
			}
			parsedClusterEntry.HostSubnetLength = uint32(tmp)

			if ipv6 && parsedClusterEntry.HostSubnetLength != 64 {
				return nil, fmt.Errorf("IPv6 only supports /64 host subnets")
			}
		} else {
			if ipv6 {
				parsedClusterEntry.HostSubnetLength = 64
			} else {
				// default for backward compatibility
				parsedClusterEntry.HostSubnetLength = 24
			}
		}

		if parsedClusterEntry.HostSubnetLength <= uint32(entryMaskLength) {
			return nil, fmt.Errorf("cannot use a host subnet length mask shorter than or equal to the cluster subnet mask. "+
				"host subnet length: %d, cluster subnet length: %d", parsedClusterEntry.HostSubnetLength, entryMaskLength)
		}

		//check to make sure that no cidrs overlap
		if cidrsOverlap(parsedClusterEntry.CIDR, parsedClusterListMap) {
			return nil, fmt.Errorf("CIDR %q overlaps with another cluster network CIDR", clusterEntry)
		}

		if _, ok := parsedClusterListMap[zoneName]; !ok {
			parsedClusterListMap[zoneName] = []CIDRNetworkEntry{}
		}
		parsedClusterListMap[zoneName] = append(parsedClusterListMap[zoneName], parsedClusterEntry)
	}

	if len(parsedClusterListMap) == 0 {
		return nil, fmt.Errorf("failed to parse any CIDRs from %q", clusterSubnetCmd)
	}

	return parsedClusterListMap, nil
}

//cidrsOverlap returns a true if the cidr range overlaps any in the map of cidr ranges
func cidrsOverlap(cidr *net.IPNet, cidrListMap map[string][]CIDRNetworkEntry) bool {
	for _, clusterList := range cidrListMap {
		for _, clusterEntry := range clusterList {
			if cidr.Contains(clusterEntry.CIDR.IP) || clusterEntry.CIDR.Contains(cidr.IP) {
				return true
			}
		}
	}
	return false
}

// joinSubnetOverlap returns an err if joinSubnet range contains the subnet
func overlapsWithJoinSubnet(subnets []*net.IPNet) error {
	_, v4JoinIPNet, _ := net.ParseCIDR(V4JoinSubnet)
	_, v6JoinIPNet, _ := net.ParseCIDR(V6JoinSubnet)
	for _, subnet := range subnets {
		for _, ipnet := range []*net.IPNet{v4JoinIPNet, v6JoinIPNet} {
			if ipnet.Contains(subnet.IP) || subnet.Contains(ipnet.IP) {
				return fmt.Errorf("%s is a reserved subnet. %s overlaps with this subnet",
					ipnet.String(), subnet)
			}
		}
	}
	return nil
}
