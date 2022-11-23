package ovn

import (
	"fmt"
	"reflect"

	mnpapi "github.com/k8snetworkplumbingwg/multi-networkpolicy/pkg/apis/k8s.cni.cncf.io/v1beta1"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/metrics"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/retry"

	knet "k8s.io/api/networking/v1"
	"k8s.io/klog/v2"
)

func (bnc *BaseNetworkController) initRetryFramework() {
	// Init the retry framework for pods, namespaces, nodes, network policies, egress firewalls,
	// egress IP (and dependent namespaces, pods, nodes), cloud private ip config.
	if !bnc.IsSecondary() {
		bnc.retryNetworkPolicies = bnc.newRetryFrameworkWithParameters(factory.PolicyType, nil, nil)
	} else {
		bnc.retryNetworkPolicies = bnc.newRetryFrameworkWithParameters(factory.MultiNetworkPolicyType, nil, nil)
	}
}

// newRetryFrameworkWithParameters builds and returns a retry framework for the input resource
// type and assigns all ovnk-master-specific function attributes in the returned struct;
// these functions will then be called by the retry logic in the retry package when
// WatchResource() is called.
// newRetryFrameworkWithParameters takes as input a resource type (required)
// and the following optional parameters: a namespace and a label filter for the
// shared informer, a sync function to process all objects of this type at startup,
// and resource-specific extra parameters (used now for network-policy-dependant types).
// In order to create a retry framework for most resource types, newRetryFrameworkMaster is
// to be preferred, as it calls newRetryFrameworkWithParameters with all optional parameters unset.
// newRetryFrameworkWithParameters is instead called directly by the watchers that are
// dynamically created when a network policy is added: PeerNamespaceAndPodSelectorType,
// PeerPodForNamespaceAndPodSelectorType, PeerNamespaceSelectorType, PeerPodSelectorType.
func (bnc *BaseNetworkController) newRetryFrameworkWithParameters(
	objectType reflect.Type,
	syncFunc func([]interface{}) error,
	extraParameters interface{}) *retry.RetryFramework {
	eventHandler := &networkControllerPolicyEventHandler{
		baseHandler:     baseNetworkControllerEventHandler{},
		objType:         objectType,
		watchFactory:    bnc.watchFactory,
		bnc:             bnc,
		extraParameters: extraParameters, // in use by network policy dynamic watchers
		syncFunc:        syncFunc,
	}
	resourceHandler := &retry.ResourceHandler{
		HasUpdateFunc:          hasResourceAnUpdateFunc(objectType),
		NeedsUpdateDuringRetry: needsUpdateDuringRetry(objectType),
		ObjType:                objectType,
		EventHandler:           eventHandler,
	}
	r := retry.NewRetryFramework(
		bnc.stopChan,
		bnc.wg,
		bnc.watchFactory,
		resourceHandler,
	)
	return r
}

// event handlers handles policy related events
type networkControllerPolicyEventHandler struct {
	baseHandler     baseNetworkControllerEventHandler
	watchFactory    *factory.WatchFactory
	objType         reflect.Type
	bnc             *BaseNetworkController
	extraParameters interface{}
	syncFunc        func([]interface{}) error
}

// AreResourcesEqual returns true if, given two objects of a known resource type, the update logic for this resource
// type considers them equal and therefore no update is needed. It returns false when the two objects are not considered
// equal and an update needs be executed. This is regardless of how the update is carried out (whether with a dedicated update
// function or with a delete on the old obj followed by an add on the new obj).
func (h *networkControllerPolicyEventHandler) AreResourcesEqual(obj1, obj2 interface{}) (bool, error) {
	return h.baseHandler.areResourcesEqual(h.objType, obj1, obj2)
}

// GetInternalCacheEntry returns the internal cache entry for this object, given an object and its type.
// This is now used only for pods, which will get their the logical port cache entry.
func (h *networkControllerPolicyEventHandler) GetInternalCacheEntry(obj interface{}) interface{} {
	return nil
}

// GetResourceFromInformerCache returns the latest state of the object, given an object key and its type.
// from the informers cache.
func (h *networkControllerPolicyEventHandler) GetResourceFromInformerCache(key string) (interface{}, error) {
	return h.baseHandler.getResourceFromInformerCache(h.objType, h.watchFactory, key)
}

// RecordAddEvent records the add event on this given object.
func (h *networkControllerPolicyEventHandler) RecordAddEvent(obj interface{}) {
	switch h.objType {
	case factory.PolicyType:
		np := obj.(*knet.NetworkPolicy)
		klog.V(5).Infof("Recording add event on network policy %s/%s", np.Namespace, np.Name)
		metrics.GetConfigDurationRecorder().Start("networkpolicy", np.Namespace, np.Name)
	case factory.MultiNetworkPolicyType:
		mnp := obj.(*mnpapi.MultiNetworkPolicy)
		klog.V(5).Infof("Recording add event on multinetwork policy %s/%s", mnp.Namespace, mnp.Name)
		metrics.GetConfigDurationRecorder().Start("multinetworkpolicy", mnp.Namespace, mnp.Name)
	}
}

// RecordUpdateEvent records the update event on this given object.
func (h *networkControllerPolicyEventHandler) RecordUpdateEvent(obj interface{}) {
	switch h.objType {
	case factory.PolicyType:
		np := obj.(*knet.NetworkPolicy)
		klog.V(5).Infof("Recording update event on network policy %s/%s", np.Namespace, np.Name)
		metrics.GetConfigDurationRecorder().Start("networkpolicy", np.Namespace, np.Name)
	case factory.MultiNetworkPolicyType:
		mnp := obj.(*mnpapi.MultiNetworkPolicy)
		klog.V(5).Infof("Recording update event on multinetwork policy %s/%s", mnp.Namespace, mnp.Name)
		metrics.GetConfigDurationRecorder().Start("multinetworkpolicy", mnp.Namespace, mnp.Name)
	}
}

// RecordDeleteEvent records the delete event on this given object.
func (h *networkControllerPolicyEventHandler) RecordDeleteEvent(obj interface{}) {
	switch h.objType {
	case factory.PolicyType:
		np := obj.(*knet.NetworkPolicy)
		klog.V(5).Infof("Recording delete event on network policy %s/%s", np.Namespace, np.Name)
		metrics.GetConfigDurationRecorder().Start("networkpolicy", np.Namespace, np.Name)
	case factory.MultiNetworkPolicyType:
		mnp := obj.(*mnpapi.MultiNetworkPolicy)
		klog.V(5).Infof("Recording delete event on multinetwork policy %s/%s", mnp.Namespace, mnp.Name)
		metrics.GetConfigDurationRecorder().Start("multinetworkpolicy", mnp.Namespace, mnp.Name)
	}
}

// RecordSuccessEvent records the success event on this given object.
func (h *networkControllerPolicyEventHandler) RecordSuccessEvent(obj interface{}) {
	switch h.objType {
	case factory.PolicyType:
		np := obj.(*knet.NetworkPolicy)
		klog.V(5).Infof("Recording success event on network policy %s/%s", np.Namespace, np.Name)
		metrics.GetConfigDurationRecorder().End("networkpolicy", np.Namespace, np.Name)
	case factory.MultiNetworkPolicyType:
		mnp := obj.(*mnpapi.MultiNetworkPolicy)
		klog.V(5).Infof("Recording success event on multinetwork policy %s/%s", mnp.Namespace, mnp.Name)
		metrics.GetConfigDurationRecorder().End("multinetworkpolicy", mnp.Namespace, mnp.Name)
	}
}

// RecordErrorEvent records an error event on the given object.
// Only used for pods now.
func (h *networkControllerPolicyEventHandler) RecordErrorEvent(obj interface{}, reason string, err error) {
}

// IsResourceScheduled returns true if the given object has been scheduled.
// Only applied to pods for now. Returns true for all other types.
func (h *networkControllerPolicyEventHandler) IsResourceScheduled(obj interface{}) bool {
	return h.baseHandler.isResourceScheduled(h.objType, obj)
}

// AddResource adds the specified object to the cluster according to its type and returns the error,
// if any, yielded during object creation.
// Given an object to add and a boolean specifying if the function was executed from iterateRetryResources
func (h *networkControllerPolicyEventHandler) AddResource(obj interface{}, fromRetryLoop bool) error {
	var err error

	switch h.objType {
	case factory.PolicyType:
		np, ok := obj.(*knet.NetworkPolicy)
		if !ok {
			return fmt.Errorf("could not cast %T object to *knet.NetworkPolicy", obj)
		}

		if err = h.bnc.addNetworkPolicy(np); err != nil {
			klog.Infof("Network Policy add failed for %s/%s, will try again later: %v",
				np.Namespace, np.Name, err)
			return err
		}

	case factory.MultiNetworkPolicyType:
		mp, ok := obj.(*mnpapi.MultiNetworkPolicy)
		if !ok {
			return fmt.Errorf("could not cast %T object to *multinetworkpolicyapi.MultiNetworkPolicy", obj)
		}

		if !h.bnc.shouldApplyMultiPolicy(mp) {
			return nil
		}

		np := convertMultiNetPolicyToNetPolicy(mp)
		if err = h.bnc.addNetworkPolicy(np); err != nil {
			klog.Infof("MultiNetworkPolicy add failed for %s/%s, will try again later: %v",
				mp.Namespace, mp.Name, err)
			return err
		}

	case factory.PeerPodSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.bnc.handlePeerPodSelectorAddUpdate(extraParameters.np, extraParameters.gp, obj)

	case factory.PeerNamespaceAndPodSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.bnc.handlePeerNamespaceAndPodAdd(extraParameters.np, extraParameters.gp,
			extraParameters.podSelector, obj)

	case factory.PeerPodForNamespaceAndPodSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.bnc.handlePeerPodSelectorAddUpdate(extraParameters.np, extraParameters.gp, obj)

	case factory.PeerNamespaceSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.bnc.handlePeerNamespaceSelectorAdd(extraParameters.np, extraParameters.gp, obj)

	case factory.LocalPodSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.bnc.handleLocalPodSelectorAddFunc(
			extraParameters.np,
			obj)

	default:
		return fmt.Errorf("no add function for object type %s", h.objType)
	}

	return nil
}

// UpdateResource updates the specified object in the cluster to its version in newObj according to its
// type and returns the error, if any, yielded during the object update.
// Given an old and a new object; The inRetryCache boolean argument is to indicate if the given resource
// is in the retryCache or not.
func (h *networkControllerPolicyEventHandler) UpdateResource(oldObj, newObj interface{}, inRetryCache bool) error {
	switch h.objType {
	case factory.MultiNetworkPolicyType:
		oldMp, ok := oldObj.(*mnpapi.MultiNetworkPolicy)
		if !ok {
			return fmt.Errorf("could not cast %T object to *multinetworkpolicyapi.MultiNetworkPolicy", oldObj)
		}
		newMp, ok := newObj.(*mnpapi.MultiNetworkPolicy)
		if !ok {
			return fmt.Errorf("could not cast %T object to *multinetworkpolicyapi.MultiNetworkPolicy", newObj)
		}

		oldShouldApply := h.bnc.shouldApplyMultiPolicy(oldMp)
		newShouldApply := h.bnc.shouldApplyMultiPolicy(newMp)
		if oldShouldApply == newShouldApply {
			return nil
		}
		// annotation changed,
		if newShouldApply {
			// now this multi-netpol applies to this network controller
			np := convertMultiNetPolicyToNetPolicy(newMp)
			if err := h.bnc.addNetworkPolicy(np); err != nil {
				klog.Infof("MultiNetworkPolicy add failed for %s/%s, will try again later: %v",
					newMp.Namespace, newMp.Name, err)
				return err
			}
		} else {
			// this multi-netpol no longer applies to this network controller, delete it
			np := convertMultiNetPolicyToNetPolicy(oldMp)
			if err := h.bnc.deleteNetworkPolicy(np); err != nil {
				klog.Infof("MultiNetworkPolicy delete failed for %s/%s, will try again later: %v",
					oldMp.Namespace, oldMp.Name, err)
				return err
			}
		}
	case factory.PeerPodSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.bnc.handlePeerPodSelectorAddUpdate(extraParameters.np, extraParameters.gp, newObj)

	case factory.PeerPodForNamespaceAndPodSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.bnc.handlePeerPodSelectorAddUpdate(extraParameters.np, extraParameters.gp, newObj)

	case factory.LocalPodSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.bnc.handleLocalPodSelectorAddFunc(
			extraParameters.np,
			newObj)
	}
	return fmt.Errorf("no update function for object type %s", h.objType)
}

// DeleteResource deletes the object from the cluster according to the delete logic of its resource type.
// Given an object and optionally a cachedObj; cachedObj is the internal cache entry for this object,
// used for now for pods and network policies.
func (h *networkControllerPolicyEventHandler) DeleteResource(obj, cachedObj interface{}) error {
	switch h.objType {
	case factory.PolicyType:
		knp, ok := obj.(*knet.NetworkPolicy)
		if !ok {
			return fmt.Errorf("could not cast obj of type %T to *knet.NetworkPolicy", obj)
		}
		return h.bnc.deleteNetworkPolicy(knp)

	case factory.MultiNetworkPolicyType:
		mp, ok := obj.(*mnpapi.MultiNetworkPolicy)
		if !ok {
			return fmt.Errorf("could not cast %T object to *multinetworkpolicyapi.MultiNetworkPolicy", obj)
		}
		np := convertMultiNetPolicyToNetPolicy(mp)
		// delete this policy regardless it applies to this network controller, in case of missing update event
		if err := h.bnc.deleteNetworkPolicy(np); err != nil {
			klog.Infof("MultiNetworkPolicy delete failed for %s/%s, will try again later: %v",
				mp.Namespace, mp.Name, err)
			return err
		}

	case factory.PeerPodSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.bnc.handlePeerPodSelectorDelete(extraParameters.np, extraParameters.gp, obj)

	case factory.PeerNamespaceAndPodSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.bnc.handlePeerNamespaceAndPodDel(extraParameters.np, extraParameters.gp, obj)

	case factory.PeerPodForNamespaceAndPodSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.bnc.handlePeerPodSelectorDelete(extraParameters.np, extraParameters.gp, obj)

	case factory.PeerNamespaceSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.bnc.handlePeerNamespaceSelectorDel(extraParameters.np, extraParameters.gp, obj)

	case factory.LocalPodSelectorType:
		extraParameters := h.extraParameters.(*NetworkPolicyExtraParameters)
		return h.bnc.handleLocalPodSelectorDelFunc(
			extraParameters.np,
			obj)

	default:
		return fmt.Errorf("object type %s not supported", h.objType)
	}
	return nil
}

func (h *networkControllerPolicyEventHandler) SyncFunc(objs []interface{}) error {
	var syncFunc func([]interface{}) error

	if h.syncFunc != nil {
		// syncFunc was provided explicitly
		syncFunc = h.syncFunc
	} else {
		switch h.objType {
		case factory.PolicyType:
			syncFunc = h.bnc.syncNetworkPolicies
		case factory.MultiNetworkPolicyType:
			syncFunc = h.bnc.syncMultiNetworkPolicies
		case factory.LocalPodSelectorType,
			factory.PeerNamespaceAndPodSelectorType,
			factory.PeerPodSelectorType,
			factory.PeerPodForNamespaceAndPodSelectorType,
			factory.PeerNamespaceSelectorType:
			syncFunc = nil

		default:
			return fmt.Errorf("no sync function for object type %s", h.objType)
		}
	}
	if syncFunc == nil {
		return nil
	}
	return syncFunc(objs)
}

// IsObjectInTerminalState returns true if the given object is a in terminal state.
// This is used now for pods that are either in a PodSucceeded or in a PodFailed state.
func (h *networkControllerPolicyEventHandler) IsObjectInTerminalState(obj interface{}) bool {
	return h.baseHandler.isObjectInTerminalState(h.objType, obj)
}
