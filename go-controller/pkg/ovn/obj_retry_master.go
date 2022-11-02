package ovn

import (
	"fmt"
	"reflect"

	kapi "k8s.io/api/core/v1"
	knet "k8s.io/api/networking/v1"
	cache "k8s.io/client-go/tools/cache"

	"k8s.io/klog/v2"

	egressfirewall "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/egressfirewall/v1"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/retry"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
)

type masterEventHandler struct {
	retry.EventHandler

	objType         reflect.Type
	watchFactory    *factory.WatchFactory
	oc              Controller
	extraParameters interface{}
	syncFunc        func([]interface{}) error
}

// newRetryFrameworkMasterWithParameters builds and returns a retry framework for the input resource
// type and assigns all ovnk-master-specific function attributes in the returned struct;
// these functions will then be called by the retry logic in the retry package when
// WatchResource() is called.
// newRetryFrameworkMasterWithParameters takes as input a resource type (required)
// and the following optional parameters: a namespace and a label filter for the
// shared informer, a sync function to process all objects of this type at startup,
// and resource-specific extra parameters (used now for network-policy-dependant types).
// In order to create a retry framework for most resource types, newRetryFrameworkMaster is
// to be preferred, as it calls newRetryFrameworkMasterWithParameters with all optional parameters unset.
// newRetryFrameworkMasterWithParameters is instead called directly by the watchers that are
// dynamically created when a network policy is added: PeerServiceType, PeerNamespaceAndPodSelectorType,
// PeerPodForNamespaceAndPodSelectorType, PeerNamespaceSelectorType, PeerPodSelectorType.
func newRetryFrameworkMasterWithParameters(
	oc Controller,
	watchFactory *factory.WatchFactory,
	objectType reflect.Type,
	syncFunc func([]interface{}) error,
	extraParameters interface{}) *retry.RetryFramework {
	resourceHandler := &retry.ResourceHandler{
		HasUpdateFunc:          hasResourceAnUpdateFunc(objectType),
		NeedsUpdateDuringRetry: needsUpdateDuringRetry(objectType),
		ObjType:                objectType,
		EventHandler: &masterEventHandler{
			objType:         objectType,
			watchFactory:    watchFactory,
			oc:              oc,
			extraParameters: extraParameters, // in use by network policy dynamic watchers
			syncFunc:        syncFunc,
		},
	}
	r := retry.NewRetryFramework(
		watchFactory,
		resourceHandler,
	)
	return r
}

// newRetryFrameworkMaster takes as input a resource type and returns a retry framework
// as defined for that type. This constructor is used for resources (1) that do not need
// any namespace or label filtering in their shared informer, (2) whose sync function
// is assigned statically based on the resource type, (3) that do not need extra
// configuration parameters (extraParameters field).
// This is true for all resource types except for those that are dynamically created when
// adding a network policy.
func newRetryFrameworkMaster(oc Controller, watchFactory *factory.WatchFactory,
	objectType reflect.Type) *retry.RetryFramework {
	return newRetryFrameworkMasterWithParameters(oc, watchFactory, objectType, nil, nil)
}

// hasResourceAnUpdateFunc returns true if the given resource type has a dedicated update function.
// It returns false if, upon an update event on this resource type, we instead need to first delete the old
// object and then add the new one.
func hasResourceAnUpdateFunc(objType reflect.Type) bool {
	switch objType {
	case factory.PodType,
		factory.NodeType,
		factory.PeerPodSelectorType,
		factory.PeerPodForNamespaceAndPodSelectorType,
		factory.EgressIPType,
		factory.EgressIPNamespaceType,
		factory.EgressIPPodType,
		factory.EgressNodeType,
		factory.CloudPrivateIPConfigType,
		factory.LocalPodSelectorType,
		factory.NamespaceType:
		return true
	}
	return false
}

// AreResourcesEqual returns true if, given two objects of a known resource type, the update logic for this resource
// type considers them equal and therefore no update is needed. It returns false when the two objects are not considered
// equal and an update needs be executed. This is regardless of how the update is carried out (whether with a dedicated update
// function or with a delete on the old obj followed by an add on the new obj).
func (h *masterEventHandler) AreResourcesEqual(obj1, obj2 interface{}) (bool, error) {
	// switch based on type
	switch h.objType {
	case factory.PolicyType:
		np1, ok := obj1.(*knet.NetworkPolicy)
		if !ok {
			return false, fmt.Errorf("could not cast obj1 of type %T to *knet.NetworkPolicy", obj1)
		}
		np2, ok := obj2.(*knet.NetworkPolicy)
		if !ok {
			return false, fmt.Errorf("could not cast obj2 of type %T to *knet.NetworkPolicy", obj2)
		}
		return reflect.DeepEqual(np1, np2), nil

	case factory.NodeType:
		node1, ok := obj1.(*kapi.Node)
		if !ok {
			return false, fmt.Errorf("could not cast obj1 of type %T to *kapi.Node", obj1)
		}
		node2, ok := obj2.(*kapi.Node)
		if !ok {
			return false, fmt.Errorf("could not cast obj2 of type %T to *kapi.Node", obj2)
		}

		// when shouldUpdateNode is false, the hostsubnet is not assigned by ovn-kubernetes
		shouldUpdate, err := shouldUpdateNode(node2, node1)
		if err != nil {
			klog.Errorf(err.Error())
		}
		return !shouldUpdate, nil

	case factory.PeerServiceType:
		service1, ok := obj1.(*kapi.Service)
		if !ok {
			return false, fmt.Errorf("could not cast obj1 of type %T to *kapi.Service", obj1)
		}
		service2, ok := obj2.(*kapi.Service)
		if !ok {
			return false, fmt.Errorf("could not cast obj2 of type %T to *kapi.Service", obj2)
		}
		areEqual := reflect.DeepEqual(service1.Spec.ExternalIPs, service2.Spec.ExternalIPs) &&
			reflect.DeepEqual(service1.Spec.ClusterIP, service2.Spec.ClusterIP) &&
			reflect.DeepEqual(service1.Spec.ClusterIPs, service2.Spec.ClusterIPs) &&
			reflect.DeepEqual(service1.Spec.Type, service2.Spec.Type) &&
			reflect.DeepEqual(service1.Status.LoadBalancer.Ingress, service2.Status.LoadBalancer.Ingress)
		return areEqual, nil

	case factory.PodType,
		factory.EgressIPPodType,
		factory.PeerPodSelectorType,
		factory.PeerPodForNamespaceAndPodSelectorType,
		factory.LocalPodSelectorType:
		// For these types, there was no old vs new obj comparison in the original update code,
		// so pretend they're always different so that the update code gets executed
		return false, nil

	case factory.PeerNamespaceSelectorType,
		factory.PeerNamespaceAndPodSelectorType:
		// For these types there is no update code, so pretend old and new
		// objs are always equivalent and stop processing the update event.
		return true, nil

	case factory.EgressFirewallType:
		oldEgressFirewall, ok := obj1.(*egressfirewall.EgressFirewall)
		if !ok {
			return false, fmt.Errorf("could not cast obj1 of type %T to *egressfirewall.EgressFirewall", obj1)
		}
		newEgressFirewall, ok := obj2.(*egressfirewall.EgressFirewall)
		if !ok {
			return false, fmt.Errorf("could not cast obj2 of type %T to *egressfirewall.EgressFirewall", obj2)
		}
		return reflect.DeepEqual(oldEgressFirewall.Spec, newEgressFirewall.Spec), nil

	case factory.EgressIPType,
		factory.EgressIPNamespaceType,
		factory.EgressNodeType,
		factory.CloudPrivateIPConfigType:
		// force update path for EgressIP resource.
		return false, nil

	case factory.NamespaceType:
		// force update path for Namespace resource.
		return false, nil
	}

	return false, fmt.Errorf("no object comparison for type %s", h.objType)
}

// Given an object and its type, GetInternalCacheEntry returns the internal cache entry for this object.
// This is now used only for pods, which will get their the logical port cache entry.
func (h *masterEventHandler) GetInternalCacheEntry(obj interface{}) interface{} {
	return h.oc.GetInternalCacheEntry(h.objType, obj)
}

// Given an object key and its type, getResourceFromInformerCache returns the latest state of the object
// from the informers cache.
func (h *masterEventHandler) GetResourceFromInformerCache(key string) (interface{}, error) {
	var obj interface{}
	var namespace, name string
	var err error

	namespace, name, err = cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return nil, fmt.Errorf("failed to split key %s: %v", key, err)
	}

	switch h.objType {
	case factory.PolicyType:
		obj, err = h.watchFactory.GetNetworkPolicy(namespace, name)

	case factory.NodeType,
		factory.EgressNodeType:
		obj, err = h.watchFactory.GetNode(name)

	case factory.PeerServiceType:
		obj, err = h.watchFactory.GetService(namespace, name)

	case factory.PodType,
		factory.PeerPodSelectorType,
		factory.PeerPodForNamespaceAndPodSelectorType,
		factory.LocalPodSelectorType,
		factory.EgressIPPodType:
		obj, err = h.watchFactory.GetPod(namespace, name)

	case factory.PeerNamespaceAndPodSelectorType,
		factory.PeerNamespaceSelectorType,
		factory.EgressIPNamespaceType,
		factory.NamespaceType:
		obj, err = h.watchFactory.GetNamespace(name)

	case factory.EgressFirewallType:
		obj, err = h.watchFactory.GetEgressFirewall(namespace, name)

	case factory.EgressIPType:
		obj, err = h.watchFactory.GetEgressIP(name)

	case factory.CloudPrivateIPConfigType:
		obj, err = h.watchFactory.GetCloudPrivateIPConfig(name)

	default:
		err = fmt.Errorf("object type %s not supported, cannot retrieve it from informers cache",
			h.objType)
	}
	return obj, err
}

// Given an object and its type, RecordAddEvent records the add event on this object.
func (h *masterEventHandler) RecordAddEvent(obj interface{}) {
	h.oc.RecordAddEvent(obj)
}

// Given an object and its type, RecordUpdateEvent records the update event on this object.
func (h *masterEventHandler) RecordUpdateEvent(obj interface{}) {
	h.oc.RecordUpdateEvent(obj)
}

// Given an object and its type, RecordDeleteEvent records the delete event on this object. Only used for pods now.
func (h *masterEventHandler) RecordDeleteEvent(obj interface{}) {
	h.oc.RecordDeleteEvent(obj)
}

func (h *masterEventHandler) RecordSuccessEvent(obj interface{}) {
	h.oc.RecordSuccessEvent(h.objType, obj)
}

// Given an object and its type, RecordErrorEvent records an error event on this object.
// Only used for pods now.
func (h *masterEventHandler) RecordErrorEvent(obj interface{}, err error) {
	h.oc.RecordObjErrorEvent(err, obj)
}

// Given an object and its type, isResourceScheduled returns true if the object has been scheduled.
// Only applied to pods for now. Returns true for all other types.
func (h *masterEventHandler) IsResourceScheduled(obj interface{}) bool {
	switch h.objType {
	case factory.PodType:
		pod := obj.(*kapi.Pod)
		return util.PodScheduled(pod)
	}
	return true
}

// Given an object type, resourceNeedsUpdate returns true if the object needs to invoke update during iterate retry.
func needsUpdateDuringRetry(objType reflect.Type) bool {
	switch objType {
	case factory.EgressNodeType,
		factory.EgressIPType,
		factory.EgressIPPodType,
		factory.EgressIPNamespaceType,
		factory.CloudPrivateIPConfigType:
		return true
	}
	return false
}

// Given a *RetryFramework instance, an object to add and a boolean specifying if the function was executed from
// iterateRetryResources, AddResource adds the specified object to the cluster according to its type and
// returns the error, if any, yielded during object creation.
func (h *masterEventHandler) AddResource(obj interface{}, fromRetryLoop bool) error {
	return h.oc.AddResource(h.objType, obj, fromRetryLoop, h.extraParameters)
}

// Given a *RetryFramework instance, an old and a new object, UpdateResource updates the specified object in the cluster
// to its version in newObj according to its type and returns the error, if any, yielded during the object update.
// The inRetryCache boolean argument is to indicate if the given resource is in the retryCache or not.
func (h *masterEventHandler) UpdateResource(oldObj, newObj interface{}, inRetryCache bool) error {
	return h.oc.UpdateResource(h.objType, oldObj, newObj, inRetryCache, h.extraParameters)
}

// Given a *RetryFramework instance, an object and optionally a cachedObj, DeleteResource deletes the object from the cluster
// according to the delete logic of its resource type. cachedObj is the internal cache entry for this object,
// used for now for pods and network policies.
func (h *masterEventHandler) DeleteResource(obj, cachedObj interface{}) error {
	return h.oc.DeleteResource(h.objType, obj, cachedObj, h.extraParameters)
}

func (h *masterEventHandler) SyncFunc(objs []interface{}) error {
	var syncFunc func([]interface{}) error
	var err error

	if h.syncFunc != nil {
		// syncFunc was provided explicitly
		syncFunc = h.syncFunc
	} else {
		syncFunc, err = h.oc.GetSyncFunc(h.objType)
		if err != nil {
			return err
		}
	}
	if syncFunc == nil {
		return nil
	}
	return syncFunc(objs)
}

// Given an object and its type, IsObjectInTerminalState returns true if the object is a in terminal state.
// This is used now for pods that are either in a PodSucceeded or in a PodFailed state.
func (h *masterEventHandler) IsObjectInTerminalState(obj interface{}) bool {
	switch h.objType {
	case factory.PodType,
		factory.PeerPodSelectorType,
		factory.PeerPodForNamespaceAndPodSelectorType,
		factory.LocalPodSelectorType,
		factory.EgressIPPodType:
		pod := obj.(*kapi.Pod)
		return util.PodCompleted(pod)

	default:
		return false
	}
}
