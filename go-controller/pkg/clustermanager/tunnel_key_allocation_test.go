// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package clustermanager

import (
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/allocator/id"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/config"
	ovntesting "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/testing"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
)

func TestInitTunnelKeysAllocatorOutOfRange(t *testing.T) {
	for _, invalid := range []int{-2, 1 << 24, math.MaxInt} {
		t.Run(fmt.Sprint(invalid), func(t *testing.T) {
			if err := config.PrepareTestConfig(); err != nil {
				t.Fatal(err)
			}
			nad := ovntesting.GenerateNAD("test", "test", "test", types.Layer2Topology, "10.0.0.0/24", types.NetworkRolePrimary)
			// Keep the derived transit key valid so the dynamic key reaches the bitmap.
			nad.Annotations = map[string]string{types.OvnNetworkTunnelKeysAnnotation: fmt.Sprintf("[%d,%d]", 16711683+1, invalid)}
			client := util.GetOVNClientset(nad)
			// Startup skips invalid reservations so reconciliation can allocate valid replacements.
			allocator, err := initTunnelKeysAllocator(client.NetworkAttchDefClient, client.NetworkConnectClient)
			if err != nil {
				t.Fatalf("invalid tunnel key blocked startup: %v", err)
			}
			if allocator == nil {
				t.Fatal("successful initialization returned no allocator")
			}
			// Allocate valid replacement keys for the network whose reservation was skipped.
			keys, err := allocator.AllocateKeys(id.NetworkTunnelKeyOwner("test"), 1, 2)
			if err != nil {
				t.Fatalf("failed to allocate replacement tunnel keys: %v", err)
			}
			if !slices.Equal(keys, []int{16711684, 16715779}) {
				t.Fatalf("unexpected replacement tunnel keys: %v", keys)
			}
		})
	}
}

func TestInitTunnelKeysAllocatorPreservesRequiredKeysWithInvalidSurplus(t *testing.T) {
	for _, surplus := range []int{-2, 1 << 24} {
		t.Run(fmt.Sprint(surplus), func(t *testing.T) {
			if err := config.PrepareTestConfig(); err != nil {
				t.Fatal(err)
			}
			nad := ovntesting.GenerateNAD("test", "test", "test", types.Layer2Topology, "10.0.0.0/24", types.NetworkRolePrimary)
			nad.Annotations = map[string]string{
				types.OvnNetworkIDAnnotation:         "1",
				types.OvnNetworkTunnelKeysAnnotation: fmt.Sprintf("[16711684,16715779,%d]", surplus),
			}
			client := util.GetOVNClientset(nad)
			allocator, err := initTunnelKeysAllocator(client.NetworkAttchDefClient, client.NetworkConnectClient)
			if err != nil {
				t.Fatal(err)
			}
			// A different network must not take the required key before this NAD reconciles.
			otherKeys, err := allocator.AllocateKeys(id.NetworkTunnelKeyOwner("other"), 2, 2)
			if err != nil || !slices.Equal(otherKeys, []int{16711685, 16715780}) {
				t.Fatalf("required key was not preserved or surplus was reserved: keys=%v, err=%v", otherKeys, err)
			}
			keys, err := allocator.AllocateKeys(id.NetworkTunnelKeyOwner("test"), 1, 2)
			if err != nil || !slices.Equal(keys, []int{16711684, 16715779}) {
				t.Fatalf("existing network keys changed: keys=%v, err=%v", keys, err)
			}
		})
	}
}

func TestInitTunnelKeysAllocatorSkipsOversizedInvalidRequiredKeys(t *testing.T) {
	for _, tc := range []struct {
		name      string
		networkID string
		switchKey int
	}{
		{name: "dynamic switch key", networkID: "1", switchKey: 16715779},
		{name: "another network switch key", networkID: "1", switchKey: 16711685},
		{name: "missing network ID", switchKey: 16711684},
		{name: "malformed network ID", networkID: "invalid", switchKey: 16711684},
		{name: "out of range network ID", networkID: "4096", switchKey: 16711684},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := config.PrepareTestConfig(); err != nil {
				t.Fatal(err)
			}
			nad := ovntesting.GenerateNAD("test", "test", "test", types.Layer2Topology, "10.0.0.0/24", types.NetworkRolePrimary)
			nad.Annotations = map[string]string{
				types.OvnNetworkIDAnnotation:         tc.networkID,
				types.OvnNetworkTunnelKeysAnnotation: fmt.Sprintf("[%d,16715790,-2]", tc.switchKey),
			}
			client := util.GetOVNClientset(nad)
			allocator, err := initTunnelKeysAllocator(client.NetworkAttchDefClient, client.NetworkConnectClient)
			if err != nil {
				t.Fatal(err)
			}
			// Invalid required entries must not become trusted existing reservations.
			if err := allocator.ReserveKeys("other", []int{16715779, 16715790}); err != nil {
				t.Fatalf("invalid required keys were reserved at startup: %v", err)
			}
		})
	}
}
