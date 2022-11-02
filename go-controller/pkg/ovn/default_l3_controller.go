package ovn

import (
	"fmt"
	"net"
	"reflect"

	ocpcloudnetworkapi "github.com/openshift/api/cloudnetwork/v1"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	egressfirewall "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/egressfirewall/v1"
	egressipv1 "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/egressip/v1"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/metrics"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	kapi "k8s.io/api/core/v1"
	knet "k8s.io/api/networking/v1"
	"k8s.io/client-go/kubernetes/scheme"
	ref "k8s.io/client-go/tools/reference"
	"k8s.io/klog/v2"
)

func (oc *DefaultL3Controller) RecordAddEvent(eventObjType reflect.Type, obj interface{}) {
	switch eventObjType {
	case factory.PodType:
		pod := obj.(*kapi.Pod)
		klog.V(5).Infof("Recording add event on pod %s/%s", pod.Namespace, pod.Name)
		oc.podRecorder.AddPod(pod.UID)
		metrics.GetConfigDurationRecorder().Start("pod", pod.Namespace, pod.Name)
	case factory.PolicyType:
		np := obj.(*knet.NetworkPolicy)
		klog.V(5).Infof("Recording add event on network policy %s/%s", np.Namespace, np.Name)
		metrics.GetConfigDurationRecorder().Start("networkpolicy", np.Namespace, np.Name)
	}
}

func (oc *DefaultL3Controller) RecordUpdateEvent(eventObjType reflect.Type, obj interface{}) {
	switch eventObjType {
	case factory.PodType:
		pod := obj.(*kapi.Pod)
		klog.V(5).Infof("Recording update event on pod %s/%s", pod.Namespace, pod.Name)
		metrics.GetConfigDurationRecorder().Start("pod", pod.Namespace, pod.Name)
	case factory.PolicyType:
		np := obj.(*knet.NetworkPolicy)
		klog.V(5).Infof("Recording update event on network policy %s/%s", np.Namespace, np.Name)
		metrics.GetConfigDurationRecorder().Start("networkpolicy", np.Namespace, np.Name)
	}
}

func (oc *DefaultL3Controller) RecordDeleteEvent(eventObjType reflect.Type, obj interface{}) {
	switch eventObjType {
	case factory.PodType:
		pod := obj.(*kapi.Pod)
		klog.V(5).Infof("Recording delete event on pod %s/%s", pod.Namespace, pod.Name)
		oc.podRecorder.CleanPod(pod.UID)
		metrics.GetConfigDurationRecorder().Start("pod", pod.Namespace, pod.Name)
	case factory.PolicyType:
		np := obj.(*knet.NetworkPolicy)
		klog.V(5).Infof("Recording delete event on network policy %s/%s", np.Namespace, np.Name)
		metrics.GetConfigDurationRecorder().Start("networkpolicy", np.Namespace, np.Name)
	}
}

func (oc *DefaultL3Controller) RecordSuccessEvent(eventObjType reflect.Type, obj interface{}) {
	switch eventObjType {
	case factory.PodType:
		pod := obj.(*kapi.Pod)
		klog.V(5).Infof("Recording success event on pod %s/%s", pod.Namespace, pod.Name)
		metrics.GetConfigDurationRecorder().End("pod", pod.Namespace, pod.Name)
	case factory.PolicyType:
		np := obj.(*knet.NetworkPolicy)
		klog.V(5).Infof("Recording success event on network policy %s/%s", np.Namespace, np.Name)
		metrics.GetConfigDurationRecorder().End("networkpolicy", np.Namespace, np.Name)
	}
}

func (oc *DefaultL3Controller) RecordErrorEvent(eventObjType reflect.Type, addErr error, reason string, obj interface{}) {
	switch eventObjType {
	case factory.PodType:
		pod := obj.(*kapi.Pod)
		podRef, err := ref.GetReference(scheme.Scheme, pod)
		if err != nil {
			klog.Errorf("Couldn't get a reference to pod %s/%s to post an event: '%v'",
				pod.Namespace, pod.Name, err)
		} else {
			klog.V(5).Infof("Posting a %s event for Pod %s/%s", kapi.EventTypeWarning, pod.Namespace, pod.Name)
			oc.recorder.Eventf(podRef, kapi.EventTypeWarning, "ErrorAddingLogicalPort", addErr.Error())
		}
	}
}

func (oc *DefaultL3Controller) AddResource(eventObjType reflect.Type, obj interface{}, fromRetryLoop bool, extraParameters interface{}) error {
	var err error

	switch eventObjType {
	case factory.PodType:
		pod, ok := obj.(*kapi.Pod)
		if !ok {
			return fmt.Errorf("could not cast %T object to *knet.Pod", obj)
		}
		return oc.ensurePod(nil, pod, true)

	case factory.PolicyType:
		np, ok := obj.(*knet.NetworkPolicy)
		if !ok {
			return fmt.Errorf("could not cast %T object to *knet.NetworkPolicy", obj)
		}

		if err = oc.addNetworkPolicy(np); err != nil {
			klog.Infof("Network Policy add failed for %s/%s, will try again later: %v",
				np.Namespace, np.Name, err)
			return err
		}

	case factory.NodeType:
		node, ok := obj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast %T object to *kapi.Node", obj)
		}
		var nodeParams *nodeSyncs
		if fromRetryLoop {
			_, nodeSync := oc.addNodeFailed.Load(node.Name)
			_, clusterRtrSync := oc.nodeClusterRouterPortFailed.Load(node.Name)
			_, mgmtSync := oc.mgmtPortFailed.Load(node.Name)
			_, gwSync := oc.gatewaysFailed.Load(node.Name)
			_, hoSync := oc.hybridOverlayFailed.Load(node.Name)
			nodeParams = &nodeSyncs{
				nodeSync,
				clusterRtrSync,
				mgmtSync,
				gwSync,
				hoSync}
		} else {
			nodeParams = &nodeSyncs{true, true, true, true, config.HybridOverlay.Enabled}
		}

		if err = oc.addUpdateNodeEvent(node, nodeParams); err != nil {
			klog.Infof("Node add failed for %s, will try again later: %v",
				node.Name, err)
			return err
		}

	case factory.PeerServiceType:
		service, ok := obj.(*kapi.Service)
		if !ok {
			return fmt.Errorf("could not cast peer service of type %T to *kapi.Service", obj)
		}
		extraParameters := extraParameters.(*NetworkPolicyExtraParameters)
		return oc.handlePeerServiceAdd(extraParameters.np, extraParameters.gp, service)

	case factory.PeerPodSelectorType:
		extraParameters := extraParameters.(*NetworkPolicyExtraParameters)
		return oc.handlePeerPodSelectorAddUpdate(extraParameters.np, extraParameters.gp, obj)

	case factory.PeerNamespaceAndPodSelectorType:
		extraParameters := extraParameters.(*NetworkPolicyExtraParameters)
		return oc.handlePeerNamespaceAndPodAdd(extraParameters.np, extraParameters.gp,
			extraParameters.podSelector, obj)

	case factory.PeerPodForNamespaceAndPodSelectorType:
		extraParameters := extraParameters.(*NetworkPolicyExtraParameters)
		return oc.handlePeerPodSelectorAddUpdate(extraParameters.np, extraParameters.gp, obj)

	case factory.PeerNamespaceSelectorType:
		extraParameters := extraParameters.(*NetworkPolicyExtraParameters)
		return oc.handlePeerNamespaceSelectorAdd(extraParameters.np, extraParameters.gp, obj)

	case factory.LocalPodSelectorType:
		extraParameters := extraParameters.(*NetworkPolicyExtraParameters)
		return oc.handleLocalPodSelectorAddFunc(
			extraParameters.np,
			obj)

	case factory.EgressFirewallType:
		var err error
		egressFirewall := obj.(*egressfirewall.EgressFirewall).DeepCopy()
		if err = oc.addEgressFirewall(egressFirewall); err != nil {
			egressFirewall.Status.Status = egressFirewallAddError
		} else {
			egressFirewall.Status.Status = egressFirewallAppliedCorrectly
			metrics.UpdateEgressFirewallRuleCount(float64(len(egressFirewall.Spec.Egress)))
			metrics.IncrementEgressFirewallCount()
		}
		if err = oc.updateEgressFirewallStatusWithRetry(egressFirewall); err != nil {
			klog.Errorf("Failed to update egress firewall status %s, error: %v",
				getEgressFirewallNamespacedName(egressFirewall), err)
		}
		return err

	case factory.EgressIPType:
		eIP := obj.(*egressipv1.EgressIP)
		return oc.reconcileEgressIP(nil, eIP)

	case factory.EgressIPNamespaceType:
		namespace := obj.(*kapi.Namespace)
		return oc.reconcileEgressIPNamespace(nil, namespace)

	case factory.EgressIPPodType:
		pod := obj.(*kapi.Pod)
		return oc.reconcileEgressIPPod(nil, pod)

	case factory.EgressNodeType:
		node := obj.(*kapi.Node)
		if err := oc.setupNodeForEgress(node); err != nil {
			return err
		}
		nodeEgressLabel := util.GetNodeEgressLabel()
		nodeLabels := node.GetLabels()
		_, hasEgressLabel := nodeLabels[nodeEgressLabel]
		if hasEgressLabel {
			oc.setNodeEgressAssignable(node.Name, true)
		}
		isReady := oc.isEgressNodeReady(node)
		if isReady {
			oc.setNodeEgressReady(node.Name, true)
		}
		isReachable := oc.isEgressNodeReachable(node)
		if isReachable {
			oc.setNodeEgressReachable(node.Name, true)
		}
		if hasEgressLabel && isReachable && isReady {
			if err := oc.addEgressNode(node.Name); err != nil {
				return err
			}
		}

	case factory.CloudPrivateIPConfigType:
		cloudPrivateIPConfig := obj.(*ocpcloudnetworkapi.CloudPrivateIPConfig)
		return oc.reconcileCloudPrivateIPConfig(nil, cloudPrivateIPConfig)

	case factory.NamespaceType:
		ns, ok := obj.(*kapi.Namespace)
		if !ok {
			return fmt.Errorf("could not cast %T object to *kapi.Namespace", obj)
		}
		return oc.AddNamespace(ns)

	default:
		return fmt.Errorf("no add function for object type %s", eventObjType)
	}

	return nil
}

func (oc *DefaultL3Controller) UpdateResource(eventObjType reflect.Type, oldObj interface{}, newObj interface{}, inRetryCache bool, extraParameters interface{}) error {
	switch eventObjType {
	case factory.PodType:
		oldPod := oldObj.(*kapi.Pod)
		newPod := newObj.(*kapi.Pod)

		return oc.ensurePod(oldPod, newPod, inRetryCache || util.PodScheduled(oldPod) != util.PodScheduled(newPod))

	case factory.NodeType:
		newNode, ok := newObj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast newObj of type %T to *kapi.Node", newObj)
		}
		oldNode, ok := oldObj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast oldObj of type %T to *kapi.Node", oldObj)
		}
		// determine what actually changed in this update
		_, nodeSync := oc.addNodeFailed.Load(newNode.Name)
		_, failed := oc.nodeClusterRouterPortFailed.Load(newNode.Name)
		clusterRtrSync := failed || nodeChassisChanged(oldNode, newNode) || nodeSubnetChanged(oldNode, newNode)
		_, failed = oc.mgmtPortFailed.Load(newNode.Name)
		mgmtSync := failed || macAddressChanged(oldNode, newNode) || nodeSubnetChanged(oldNode, newNode)
		_, failed = oc.gatewaysFailed.Load(newNode.Name)
		gwSync := (failed || gatewayChanged(oldNode, newNode) ||
			nodeSubnetChanged(oldNode, newNode) || hostAddressesChanged(oldNode, newNode))
		_, hoSync := oc.hybridOverlayFailed.Load(newNode.Name)

		return oc.addUpdateNodeEvent(newNode, &nodeSyncs{nodeSync, clusterRtrSync, mgmtSync, gwSync, hoSync})

	case factory.PeerPodSelectorType:
		extraParameters := extraParameters.(*NetworkPolicyExtraParameters)
		return oc.handlePeerPodSelectorAddUpdate(extraParameters.np, extraParameters.gp, newObj)

	case factory.PeerPodForNamespaceAndPodSelectorType:
		extraParameters := extraParameters.(*NetworkPolicyExtraParameters)
		return oc.handlePeerPodSelectorAddUpdate(extraParameters.np, extraParameters.gp, newObj)

	case factory.LocalPodSelectorType:
		extraParameters := extraParameters.(*NetworkPolicyExtraParameters)
		return oc.handleLocalPodSelectorAddFunc(
			extraParameters.np,
			newObj)

	case factory.EgressIPType:
		oldEIP := oldObj.(*egressipv1.EgressIP)
		newEIP := newObj.(*egressipv1.EgressIP)
		return oc.reconcileEgressIP(oldEIP, newEIP)

	case factory.EgressIPNamespaceType:
		oldNamespace := oldObj.(*kapi.Namespace)
		newNamespace := newObj.(*kapi.Namespace)
		return oc.reconcileEgressIPNamespace(oldNamespace, newNamespace)

	case factory.EgressIPPodType:
		oldPod := oldObj.(*kapi.Pod)
		newPod := newObj.(*kapi.Pod)
		return oc.reconcileEgressIPPod(oldPod, newPod)

	case factory.EgressNodeType:
		oldNode := oldObj.(*kapi.Node)
		newNode := newObj.(*kapi.Node)
		// Initialize the allocator on every update,
		// ovnkube-node/cloud-network-config-controller will make sure to
		// annotate the node with the egressIPConfig, but that might have
		// happened after we processed the ADD for that object, hence keep
		// retrying for all UPDATEs.
		if err := oc.initEgressIPAllocator(newNode); err != nil {
			klog.Warningf("Egress node initialization error: %v", err)
		}
		nodeEgressLabel := util.GetNodeEgressLabel()
		oldLabels := oldNode.GetLabels()
		newLabels := newNode.GetLabels()
		_, oldHadEgressLabel := oldLabels[nodeEgressLabel]
		_, newHasEgressLabel := newLabels[nodeEgressLabel]
		// If the node is not labeled for egress assignment, just return
		// directly, we don't really need to set the ready / reachable
		// status on this node if the user doesn't care about using it.
		if !oldHadEgressLabel && !newHasEgressLabel {
			return nil
		}
		if oldHadEgressLabel && !newHasEgressLabel {
			klog.Infof("Node: %s has been un-labeled, deleting it from egress assignment", newNode.Name)
			oc.setNodeEgressAssignable(oldNode.Name, false)
			return oc.deleteEgressNode(oldNode.Name)
		}
		isOldReady := oc.isEgressNodeReady(oldNode)
		isNewReady := oc.isEgressNodeReady(newNode)
		isNewReachable := oc.isEgressNodeReachable(newNode)
		oc.setNodeEgressReady(newNode.Name, isNewReady)
		oc.setNodeEgressReachable(newNode.Name, isNewReachable)
		if !oldHadEgressLabel && newHasEgressLabel {
			klog.Infof("Node: %s has been labeled, adding it for egress assignment", newNode.Name)
			oc.setNodeEgressAssignable(newNode.Name, true)
			if isNewReady && isNewReachable {
				if err := oc.addEgressNode(newNode.Name); err != nil {
					return err
				}
			} else {
				klog.Warningf("Node: %s has been labeled, but node is not ready"+
					" and reachable, cannot use it for egress assignment", newNode.Name)
			}
			return nil
		}
		if isOldReady == isNewReady {
			return nil
		}
		if !isNewReady {
			klog.Warningf("Node: %s is not ready, deleting it from egress assignment", newNode.Name)
			if err := oc.deleteEgressNode(newNode.Name); err != nil {
				return err
			}
		} else if isNewReady && isNewReachable {
			klog.Infof("Node: %s is ready and reachable, adding it for egress assignment", newNode.Name)
			if err := oc.addEgressNode(newNode.Name); err != nil {
				return err
			}
		}
		return nil

	case factory.CloudPrivateIPConfigType:
		oldCloudPrivateIPConfig := oldObj.(*ocpcloudnetworkapi.CloudPrivateIPConfig)
		newCloudPrivateIPConfig := newObj.(*ocpcloudnetworkapi.CloudPrivateIPConfig)
		return oc.reconcileCloudPrivateIPConfig(oldCloudPrivateIPConfig, newCloudPrivateIPConfig)

	case factory.NamespaceType:
		oldNs, newNs := oldObj.(*kapi.Namespace), newObj.(*kapi.Namespace)
		return oc.updateNamespace(oldNs, newNs)
	}
	return fmt.Errorf("no update function for object type %s", eventObjType)
}

func (oc *DefaultL3Controller) DeleteResource(eventObjType reflect.Type, obj, cachedObj, extraParameters interface{}) error {
	switch eventObjType {
	case factory.PodType:
		var portInfo *lpInfo
		pod := obj.(*kapi.Pod)

		if cachedObj != nil {
			portInfo = cachedObj.(*lpInfo)
		}
		oc.logicalPortCache.remove(util.GetLogicalPortName(pod.Namespace, pod.Name))
		return oc.removePod(pod, portInfo)

	case factory.PolicyType:
		knp, ok := obj.(*knet.NetworkPolicy)
		if !ok {
			return fmt.Errorf("could not cast obj of type %T to *knet.NetworkPolicy", obj)
		}
		return oc.deleteNetworkPolicy(knp)

	case factory.NodeType:
		node, ok := obj.(*kapi.Node)
		if !ok {
			return fmt.Errorf("could not cast obj of type %T to *knet.Node", obj)
		}
		return oc.deleteNodeEvent(node)

	case factory.PeerServiceType:
		service, ok := obj.(*kapi.Service)
		if !ok {
			return fmt.Errorf("could not cast peer service of type %T to *kapi.Service", obj)
		}
		extraParameters := extraParameters.(*NetworkPolicyExtraParameters)
		return oc.handlePeerServiceDelete(extraParameters.np, extraParameters.gp, service)

	case factory.PeerPodSelectorType:
		extraParameters := extraParameters.(*NetworkPolicyExtraParameters)
		return oc.handlePeerPodSelectorDelete(extraParameters.np, extraParameters.gp, obj)

	case factory.PeerNamespaceAndPodSelectorType:
		extraParameters := extraParameters.(*NetworkPolicyExtraParameters)
		return oc.handlePeerNamespaceAndPodDel(extraParameters.np, extraParameters.gp, obj)

	case factory.PeerPodForNamespaceAndPodSelectorType:
		extraParameters := extraParameters.(*NetworkPolicyExtraParameters)
		return oc.handlePeerPodSelectorDelete(extraParameters.np, extraParameters.gp, obj)

	case factory.PeerNamespaceSelectorType:
		extraParameters := extraParameters.(*NetworkPolicyExtraParameters)
		return oc.handlePeerNamespaceSelectorDel(extraParameters.np, extraParameters.gp, obj)

	case factory.LocalPodSelectorType:
		extraParameters := extraParameters.(*NetworkPolicyExtraParameters)
		return oc.handleLocalPodSelectorDelFunc(
			extraParameters.np,
			obj)

	case factory.EgressFirewallType:
		egressFirewall := obj.(*egressfirewall.EgressFirewall)
		if err := oc.deleteEgressFirewall(egressFirewall); err != nil {
			return err
		}
		metrics.UpdateEgressFirewallRuleCount(float64(-len(egressFirewall.Spec.Egress)))
		metrics.DecrementEgressFirewallCount()
		return nil

	case factory.EgressIPType:
		eIP := obj.(*egressipv1.EgressIP)
		return oc.reconcileEgressIP(eIP, nil)

	case factory.EgressIPNamespaceType:
		namespace := obj.(*kapi.Namespace)
		return oc.reconcileEgressIPNamespace(namespace, nil)

	case factory.EgressIPPodType:
		pod := obj.(*kapi.Pod)
		return oc.reconcileEgressIPPod(pod, nil)

	case factory.EgressNodeType:
		node := obj.(*kapi.Node)
		if err := oc.deleteNodeForEgress(node); err != nil {
			return err
		}
		nodeEgressLabel := util.GetNodeEgressLabel()
		nodeLabels := node.GetLabels()
		if _, hasEgressLabel := nodeLabels[nodeEgressLabel]; hasEgressLabel {
			if err := oc.deleteEgressNode(node.Name); err != nil {
				return err
			}
		}
		return nil

	case factory.CloudPrivateIPConfigType:
		cloudPrivateIPConfig := obj.(*ocpcloudnetworkapi.CloudPrivateIPConfig)
		return oc.reconcileCloudPrivateIPConfig(cloudPrivateIPConfig, nil)

	case factory.NamespaceType:
		ns := obj.(*kapi.Namespace)
		return oc.deleteNamespace(ns)

	default:
		return fmt.Errorf("object type %s not supported", eventObjType)
	}
}

func (oc *DefaultL3Controller) GetSyncFunc(eventObjType reflect.Type) (syncFunc func([]interface{}) error, err error) {
	switch eventObjType {
	case factory.PodType:
		syncFunc = oc.syncPods

	case factory.PolicyType:
		syncFunc = oc.syncNetworkPolicies

	case factory.NodeType:
		syncFunc = oc.syncNodes

	case factory.LocalPodSelectorType,
		factory.PeerServiceType,
		factory.PeerNamespaceAndPodSelectorType,
		factory.PeerPodSelectorType,
		factory.PeerPodForNamespaceAndPodSelectorType,
		factory.PeerNamespaceSelectorType:
		syncFunc = nil

	case factory.EgressFirewallType:
		syncFunc = oc.syncEgressFirewall

	case factory.EgressIPNamespaceType:
		syncFunc = oc.syncEgressIPs

	case factory.EgressNodeType:
		syncFunc = oc.initClusterEgressPolicies

	case factory.EgressIPPodType,
		factory.EgressIPType,
		factory.CloudPrivateIPConfigType:
		syncFunc = nil

	case factory.NamespaceType:
		syncFunc = oc.syncNamespaces

	default:
		return nil, fmt.Errorf("no sync function for object type %s", eventObjType)
	}

	return syncFunc, nil
}

func (oc *DefaultL3Controller) getPortInfo(pod *kapi.Pod) *lpInfo {
	var portInfo *lpInfo
	key := util.GetLogicalPortName(pod.Namespace, pod.Name)
	if !util.PodWantsNetwork(pod) {
		// create dummy logicalPortInfo for host-networked pods
		mac, _ := net.ParseMAC("00:00:00:00:00:00")
		portInfo = &lpInfo{
			logicalSwitch: "host-networked",
			name:          key,
			uuid:          "host-networked",
			ips:           []*net.IPNet{},
			mac:           mac,
		}
	} else {
		portInfo, _ = oc.logicalPortCache.get(key)
	}
	return portInfo
}
func (oc *DefaultL3Controller) GetInternalCacheEntry(eventObjType reflect.Type, obj interface{}) interface{} {
	switch eventObjType {
	case factory.PodType:
		pod := obj.(*kapi.Pod)
		return oc.getPortInfo(pod)
	default:
		return nil
	}
}
