package ovn

import (
	"context"
	"fmt"
	"math"
	"strconv"

	libovsdbclient "github.com/ovn-org/libovsdb/client"
	globalconfig "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/libovsdbops"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/nbdb"
	ovntypes "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	corev1apply "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"
)

func (oc *DefaultNetworkController) ovnTopologyCleanup() error {
	ver, err := oc.determineOVNTopoVersionFromOVN()
	if err != nil {
		return err
	}

	// Cleanup address sets in non dual stack formats in all versions known to possibly exist.
	if ver <= ovntypes.OvnPortBindingTopoVersion {
		err = oc.addressSetFactory.NonDualStackAddressSetCleanup()
	}
	return err
}

func (nci *NetworkControllerInfo) updateL3TopologyVersion() error {
	currentTopologyVersion := strconv.Itoa(ovntypes.OvnCurrentTopologyVersion)
	clusterRouterName := nci.GetPrefix() + ovntypes.OVNClusterRouter
	logicalRouter := nbdb.LogicalRouter{
		Name:        clusterRouterName,
		ExternalIDs: map[string]string{"k8s-ovn-topo-version": currentTopologyVersion},
	}
	err := libovsdbops.UpdateLogicalRouterSetExternalIDs(nci.nbClient, &logicalRouter)
	if err != nil {
		return fmt.Errorf("failed to generate set topology version, err: %v", err)
	}
	klog.Infof("Updated Logical_Router %s topology version to %s", clusterRouterName, currentTopologyVersion)
	return nil
}

func (nci *NetworkControllerInfo) updateL2TopologyVersion() error {
	var switchName string

	currentTopologyVersion := strconv.Itoa(ovntypes.OvnCurrentTopologyVersion)
	topoType := nci.GetTopologyType()
	if topoType == ovntypes.Layer2AttachDefTopoType {
		switchName = nci.GetPrefix() + ovntypes.OvnLayer2Switch
	} else if topoType == ovntypes.LocalnetAttachDefTopoType {
		switchName = nci.GetPrefix() + ovntypes.OVNLocalnetSwitch
	} else {
		return fmt.Errorf("topology type %s is not supported", topoType)
	}
	logicalSwitch := nbdb.LogicalSwitch{
		Name:        switchName,
		ExternalIDs: map[string]string{"k8s-ovn-topo-version": currentTopologyVersion},
	}
	err := libovsdbops.UpdateLogicalSwtichSetExternalIDs(nci.nbClient, &logicalSwitch)
	if err != nil {
		return fmt.Errorf("failed to generate set topology version, err: %v", err)
	}
	klog.Infof("Updated Logical_Switch %s topology version to %s", switchName, currentTopologyVersion)
	return nil
}

// reportTopologyVersion saves the topology version to two places:
// - an ExternalID on the ovn_cluster_router LogicalRouter in nbdb
// - a ConfigMap. This is used by nodes to determine the cluster's topology
func (oc *DefaultNetworkController) reportTopologyVersion(ctx context.Context) error {
	err := oc.updateL3TopologyVersion()
	if err != nil {
		return err
	}

	currentTopologyVersion := strconv.Itoa(ovntypes.OvnCurrentTopologyVersion)
	// Report topology version in a ConfigMap
	// (we used to report this via annotations on our Node)
	cm := corev1apply.ConfigMap(ovntypes.OvnK8sStatusCMName, globalconfig.Kubernetes.OVNConfigNamespace)
	cm.Data = map[string]string{ovntypes.OvnK8sStatusKeyTopoVersion: currentTopologyVersion}
	if _, err := oc.client.CoreV1().ConfigMaps(globalconfig.Kubernetes.OVNConfigNamespace).Apply(ctx, cm, metav1.ApplyOptions{
		Force:        true,
		FieldManager: "ovn-kubernetes",
	}); err != nil {
		return err
	}

	klog.Infof("Updated ConfigMap %s/%s topology version to %s", *cm.Namespace, *cm.Name, currentTopologyVersion)

	return oc.cleanTopologyAnnotation()
}

// Remove the old topology annotation from nodes, if it exists.
func (oc *DefaultNetworkController) cleanTopologyAnnotation() error {
	// Unset the old topology annotation on all Node objects
	nodes, err := oc.watchFactory.GetNodes()
	if err != nil {
		return err
	}
	anno := ovntypes.OvnK8sTopoAnno //nolint // otherwise we get deprecation warnings (this variable is deprecated)
	for _, node := range nodes {
		if _, ok := node.Annotations[anno]; !ok {
			continue
		}
		err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			node, err := oc.kube.GetNode(node.Name)
			if err != nil {
				if apierrors.IsNotFound(err) {
					return nil
				}
				return err
			}
			if _, ok := node.Annotations[anno]; ok {
				newNode := node.DeepCopy()
				delete(newNode.Annotations, anno)
				klog.Infof("Deleting topology annotation from node %s", node.Name)
				return oc.kube.PatchNode(node, newNode)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

func (bnc *BaseNetworkController) getOVNTopoVersionFromLogicalRouter(clusterRouterName string) (int, error) {
	logicalRouter := &nbdb.LogicalRouter{Name: clusterRouterName}
	logicalRouter, err := libovsdbops.GetLogicalRouter(bnc.nbClient, logicalRouter)
	if err != nil && err != libovsdbclient.ErrNotFound {
		return 0, fmt.Errorf("error getting router %s: %v", clusterRouterName, err)
	}
	if err == libovsdbclient.ErrNotFound {
		// no OVNClusterRouter exists, DB is empty, nothing to upgrade
		return math.MaxInt32, nil
	}
	v, exists := logicalRouter.ExternalIDs["k8s-ovn-topo-version"]
	if !exists {
		klog.Infof("No version string found. The OVN topology is before versioning is introduced. Upgrade needed")
		return 0, nil
	}
	ver, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid OVN topology version string for the cluster, err: %v", err)
	}
	return ver, nil
}

func (bnc *BaseNetworkController) getOVNTopoVersionFromLogicalSwitch(switchName string) (int, error) {
	logicalSwitch := &nbdb.LogicalSwitch{Name: switchName}
	logicalSwitch, err := libovsdbops.GetLogicalSwitch(bnc.nbClient, logicalSwitch)
	if err != nil && err != libovsdbclient.ErrNotFound {
		return 0, fmt.Errorf("error getting switch %s: %v", switchName, err)
	}
	if err == libovsdbclient.ErrNotFound {
		// no OVNClusterRouter exists, DB is empty, nothing to upgrade
		return math.MaxInt32, nil
	}
	v, exists := logicalSwitch.ExternalIDs["k8s-ovn-topo-version"]
	if !exists {
		klog.Infof("No version string found. The OVN topology is before versioning is introduced. Upgrade needed")
		return 0, nil
	}
	ver, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("invalid OVN topology version string for the cluster, err: %v", err)
	}
	return ver, nil
}

// determineOVNTopoVersionFromOVN determines what OVN Topology version is being used
// If "k8s-ovn-topo-version" key in external_ids column does not exist, it is prior to OVN topology versioning
// and therefore set version number to OvnCurrentTopologyVersion
func (nci *NetworkControllerInfo) determineOVNTopoVersionFromOVN() (int, error) {
	if nci.IsSecondary() {
		topoType := nci.GetTopologyType()
		var switchName string
		if topoType == ovntypes.Layer2AttachDefTopoType {
			switchName = nci.GetPrefix() + ovntypes.OvnLayer2Switch
		} else if topoType == ovntypes.LocalnetAttachDefTopoType {
			switchName = nci.GetPrefix() + ovntypes.OVNLocalnetSwitch
		}
		if switchName != "" {
			return nci.getOVNTopoVersionFromLogicalSwitch(switchName)
		}
	}
	clusterRouterName := nci.GetPrefix() + ovntypes.OVNClusterRouter
	return nci.getOVNTopoVersionFromLogicalRouter(clusterRouterName)
}
