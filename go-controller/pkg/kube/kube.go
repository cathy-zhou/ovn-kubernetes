package kube

import (
	"context"
	"encoding/json"
	"k8s.io/klog/v2"

	nettypes "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	networkattchmentdefclientset "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/clientset/versioned"
	egressfirewall "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/egressfirewall/v1"
	egressfirewallclientset "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/egressfirewall/v1/apis/clientset/versioned"
	egressipv1 "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/egressip/v1"
	egressipclientset "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/crd/egressip/v1/apis/clientset/versioned"
	kapi "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	kv1core "k8s.io/client-go/kubernetes/typed/core/v1"
)

// Interface represents the exported methods for dealing with getting/setting
// kubernetes resources
type Interface interface {
	SetAnnotationsOnPod(namespace, podName string, annotations map[string]string) error
	SetAnnotationsOnNode(node *kapi.Node, annotations map[string]interface{}) error
	SetAnnotationsOnNamespace(namespace *kapi.Namespace, annotations map[string]string) error
	UpdateEgressFirewall(egressfirewall *egressfirewall.EgressFirewall) error
	UpdateEgressIP(eIP *egressipv1.EgressIP) error
	UpdateNodeStatus(node *kapi.Node) error
	UpdateNode(node *kapi.Node) error
	GetAnnotationsOnPod(namespace, name string) (map[string]string, error)
	GetNodes() (*kapi.NodeList, error)
	GetEgressIP(name string) (*egressipv1.EgressIP, error)
	GetEgressIPs() (*egressipv1.EgressIPList, error)
	GetNamespaces(labelSelector metav1.LabelSelector) (*kapi.NamespaceList, error)
	GetPods(namespace string, labelSelector metav1.LabelSelector) (*kapi.PodList, error)
	GetAllPods(namespace string) (*kapi.PodList, error)
	GetNode(name string) (*kapi.Node, error)
	GetPod(namespace, name string) (*kapi.Pod, error)
	GetEndpoint(namespace, name string) (*kapi.Endpoints, error)
	CreateEndpoint(namespace string, ep *kapi.Endpoints) (*kapi.Endpoints, error)
	Events() kv1core.EventInterface
	UpdatePod(pod *kapi.Pod) error
	GetNetAttachDef(namespace, name string) (*nettypes.NetworkAttachmentDefinition, error)
	CreateNetAttachDef(namespace string, netattchdef *nettypes.NetworkAttachmentDefinition) (*nettypes.NetworkAttachmentDefinition, error)
	DeleteNetAttachDef(namespace, name string) error
}

// Kube is the structure object upon which the Interface is implemented
type Kube struct {
	KClient              kubernetes.Interface
	EIPClient            egressipclientset.Interface
	EgressFirewallClient egressfirewallclientset.Interface
	NetAttachDefClient   networkattchmentdefclientset.Interface
}

// SetAnnotationsOnPod takes the pod object and map of key/value string pairs to set as annotations
func (k *Kube) SetAnnotationsOnPod(namespace, podName string, annotations map[string]string) error {
	var err error
	var patchData []byte
	patch := struct {
		Metadata map[string]interface{} `json:"metadata"`
	}{
		Metadata: map[string]interface{}{
			"annotations": annotations,
		},
	}

	podDesc := namespace + "/" + podName
	klog.Infof("Setting annotations %v on pod %s", annotations, podDesc)
	patchData, err = json.Marshal(&patch)
	if err != nil {
		klog.Errorf("Error in setting annotations on pod %s: %v", podDesc, err)
		return err
	}

	_, err = k.KClient.CoreV1().Pods(namespace).Patch(context.TODO(), podName, types.MergePatchType, patchData, metav1.PatchOptions{})
	if err != nil {
		klog.Errorf("Error in setting annotation on pod %s: %v", podDesc, err)
	}
	return err
}

// SetAnnotationsOnNode takes the node object and map of key/value string pairs to set as annotations
func (k *Kube) SetAnnotationsOnNode(node *kapi.Node, annotations map[string]interface{}) error {
	var err error
	var patchData []byte
	patch := struct {
		Metadata map[string]interface{} `json:"metadata"`
	}{
		Metadata: map[string]interface{}{
			"annotations": annotations,
		},
	}

	klog.Infof("Setting annotations %v on node %s", annotations, node.Name)
	patchData, err = json.Marshal(&patch)
	if err != nil {
		klog.Errorf("Error in setting annotations on node %s: %v", node.Name, err)
		return err
	}

	_, err = k.KClient.CoreV1().Nodes().Patch(context.TODO(), node.Name, types.MergePatchType, patchData, metav1.PatchOptions{})
	if err != nil {
		klog.Errorf("Error in setting annotation on node %s: %v", node.Name, err)
	}
	return err
}

// SetAnnotationsOnNamespace takes the namespace object and map of key/value string pairs to set as annotations
func (k *Kube) SetAnnotationsOnNamespace(namespace *kapi.Namespace, annotations map[string]string) error {
	var err error
	var patchData []byte
	patch := struct {
		Metadata map[string]interface{} `json:"metadata"`
	}{
		Metadata: map[string]interface{}{
			"annotations": annotations,
		},
	}

	klog.Infof("Setting annotations %v on namespace %s", annotations, namespace.Name)
	patchData, err = json.Marshal(&patch)
	if err != nil {
		klog.Errorf("Error in setting annotations on namespace %s: %v", namespace.Name, err)
		return err
	}

	_, err = k.KClient.CoreV1().Namespaces().Patch(context.TODO(), namespace.Name, types.MergePatchType, patchData, metav1.PatchOptions{})
	if err != nil {
		klog.Errorf("Error in setting annotation on namespace %s: %v", namespace.Name, err)
	}
	return err
}

// UpdateEgressFirewall updates the EgressFirewall with the provided EgressFirewall data
func (k *Kube) UpdateEgressFirewall(egressfirewall *egressfirewall.EgressFirewall) error {
	klog.Infof("Updating status on EgressFirewall %s in namespace %s", egressfirewall.Name, egressfirewall.Namespace)
	_, err := k.EgressFirewallClient.K8sV1().EgressFirewalls(egressfirewall.Namespace).Update(context.TODO(), egressfirewall, metav1.UpdateOptions{})
	return err
}

// UpdateEgressIP updates the EgressIP with the provided EgressIP data
func (k *Kube) UpdateEgressIP(eIP *egressipv1.EgressIP) error {
	klog.Infof("Updating status on EgressIP %s", eIP.Name)
	_, err := k.EIPClient.K8sV1().EgressIPs().Update(context.TODO(), eIP, metav1.UpdateOptions{})
	return err
}

// UpdateNodeStatus takes the node object and sets the provided update status
func (k *Kube) UpdateNodeStatus(node *kapi.Node) error {
	klog.Infof("Updating status on node %s", node.Name)
	_, err := k.KClient.CoreV1().Nodes().UpdateStatus(context.TODO(), node, metav1.UpdateOptions{})
	if err != nil {
		klog.Errorf("Error in updating status on node %s: %v", node.Name, err)
	}
	return err
}

// UpdateNode takes the node object and sets the provided update status
func (k *Kube) UpdateNode(node *kapi.Node) error {
	klog.Infof("Updating node %s", node.Name)
	_, err := k.KClient.CoreV1().Nodes().Update(context.TODO(), node, metav1.UpdateOptions{})
	return err
}

// UpdatePod update pod with provided pod data
func (k *Kube) UpdatePod(pod *kapi.Pod) error {
	klog.Infof("Updating pod %s/%s", pod.Namespace, pod.Name)
	_, err := k.KClient.CoreV1().Pods(pod.Namespace).Update(context.TODO(), pod, metav1.UpdateOptions{})
	return err
}

// GetAnnotationsOnPod obtains the pod annotations from kubernetes apiserver, given the name and namespace
func (k *Kube) GetAnnotationsOnPod(namespace, name string) (map[string]string, error) {
	pod, err := k.KClient.CoreV1().Pods(namespace).Get(context.TODO(), name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	return pod.ObjectMeta.Annotations, nil
}

// GetNamespaces returns the list of all Namespace objects matching the labelSelector
func (k *Kube) GetNamespaces(labelSelector metav1.LabelSelector) (*kapi.NamespaceList, error) {
	return k.KClient.CoreV1().Namespaces().List(context.TODO(), metav1.ListOptions{
		LabelSelector: labels.Set(labelSelector.MatchLabels).String(),
	})
}

// GetPods returns the list of all Pod objects in a namespace matching the labelSelector
func (k *Kube) GetPods(namespace string, labelSelector metav1.LabelSelector) (*kapi.PodList, error) {
	return k.KClient.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{
		LabelSelector: labels.Set(labelSelector.MatchLabels).String(),
	})
}

// GetAllPods returns the list of all Pod objects in a namespace
func (k *Kube) GetAllPods(namespace string) (*kapi.PodList, error) {
	return k.KClient.CoreV1().Pods(namespace).List(context.TODO(), metav1.ListOptions{})
}

// GetPod obtains the pod from kubernetes apiserver, given the name and namespace
func (k *Kube) GetPod(namespace, name string) (*kapi.Pod, error) {
	return k.KClient.CoreV1().Pods(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// GetNodes returns the list of all Node objects from kubernetes
func (k *Kube) GetNodes() (*kapi.NodeList, error) {
	return k.KClient.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
}

// GetNode returns the Node resource from kubernetes apiserver, given its name
func (k *Kube) GetNode(name string) (*kapi.Node, error) {
	return k.KClient.CoreV1().Nodes().Get(context.TODO(), name, metav1.GetOptions{})
}

// GetEgressIP returns the EgressIP object from kubernetes
func (k *Kube) GetEgressIP(name string) (*egressipv1.EgressIP, error) {
	return k.EIPClient.K8sV1().EgressIPs().Get(context.TODO(), name, metav1.GetOptions{})
}

// GetEgressIPs returns the list of all EgressIP objects from kubernetes
func (k *Kube) GetEgressIPs() (*egressipv1.EgressIPList, error) {
	return k.EIPClient.K8sV1().EgressIPs().List(context.TODO(), metav1.ListOptions{})
}

// GetEndpoint returns the Endpoints resource
func (k *Kube) GetEndpoint(namespace, name string) (*kapi.Endpoints, error) {
	return k.KClient.CoreV1().Endpoints(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// CreateEndpoint creates the Endpoints resource
func (k *Kube) CreateEndpoint(namespace string, ep *kapi.Endpoints) (*kapi.Endpoints, error) {
	return k.KClient.CoreV1().Endpoints(namespace).Create(context.TODO(), ep, metav1.CreateOptions{})
}

// GetNetAttachDef returns the NetworkAttachmentDefinition resource
func (k *Kube) GetNetAttachDef(namespace, name string) (*nettypes.NetworkAttachmentDefinition, error) {
	return k.NetAttachDefClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(namespace).Get(context.TODO(), name, metav1.GetOptions{})
}

// CreateNetAttachDef creates the NetworkAttachmentDefinition resource
func (k *Kube) CreateNetAttachDef(namespace string, netattchdef *nettypes.NetworkAttachmentDefinition) (*nettypes.NetworkAttachmentDefinition, error) {
	return k.NetAttachDefClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(namespace).Create(context.TODO(), netattchdef, metav1.CreateOptions{})
}

// DeleteNetAttachDef creates the NetworkAttachmentDefinition resource
func (k *Kube) DeleteNetAttachDef(namespace, name string) error {
	return k.NetAttachDefClient.K8sCniCncfIoV1().NetworkAttachmentDefinitions(namespace).Delete(context.TODO(), name, metav1.DeleteOptions{})
}

// Events returns events to use when creating an EventSinkImpl
func (k *Kube) Events() kv1core.EventInterface {
	return k.KClient.CoreV1().Events("")
}
