package util

import (
	"encoding/json"
	"fmt"
	"strings"

	kapi "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/cert"

	networkclient "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/client/clientset/versioned"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/cni/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
)

const (
	defaultMultusNamespace = "kube-system"
	defaultNetAnnot        = "v1.multus-cni.io/default-network"
	networkAttachmentAnnot = "k8s.v1.cni.cncf.io/networks"
)

// NewClientset creates a Kubernetes clientset from either a kubeconfig,
// TLS properties, or an apiserver URL
func NewClientset(conf *config.KubernetesConfig) (*kubernetes.Clientset, *networkclient.Clientset, error) {
	var kconfig *rest.Config
	var err error

	if conf.Kubeconfig != "" {
		// uses the current context in kubeconfig
		kconfig, err = clientcmd.BuildConfigFromFlags("", conf.Kubeconfig)
	} else if strings.HasPrefix(conf.APIServer, "https") {
		if conf.APIServer == "" || conf.Token == "" {
			return nil, nil, fmt.Errorf("TLS-secured apiservers require token and CA certificate")
		}
		kconfig = &rest.Config{
			Host:        conf.APIServer,
			BearerToken: conf.Token,
		}
		if conf.CACert != "" {
			if _, err := cert.NewPool(conf.CACert); err != nil {
				return nil, nil, err
			}
			kconfig.TLSClientConfig = rest.TLSClientConfig{CAFile: conf.CACert}
		}
	} else if strings.HasPrefix(conf.APIServer, "http") {
		kconfig, err = clientcmd.BuildConfigFromFlags(conf.APIServer, "")
	} else {
		// Assume we are running from a container managed by kubernetes
		// and read the apiserver address and tokens from the
		// container's environment.
		kconfig, err = rest.InClusterConfig()
	}
	if err != nil {
		return nil, nil, err
	}

	coreClient, err := kubernetes.NewForConfig(kconfig)
	if err != nil {
		return nil, nil, err
	}
	networkClient, err := networkclient.NewForConfig(kconfig)
	if err != nil {
		return nil, nil, err
	}
	return coreClient, networkClient, nil
}

// IsClusterIPSet checks if the service is an headless service or not
func IsClusterIPSet(service *kapi.Service) bool {
	return service.Spec.ClusterIP != kapi.ClusterIPNone && service.Spec.ClusterIP != ""
}

// ServiceTypeHasClusterIP checks if the service has an associated ClusterIP or not
func ServiceTypeHasClusterIP(service *kapi.Service) bool {
	return service.Spec.Type == kapi.ServiceTypeClusterIP || service.Spec.Type == kapi.ServiceTypeNodePort || service.Spec.Type == kapi.ServiceTypeLoadBalancer
}

// ServiceTypeHasNodePort checks if the service has an associated NodePort or not
func ServiceTypeHasNodePort(service *kapi.Service) bool {
	return service.Spec.Type == kapi.ServiceTypeNodePort || service.Spec.Type == kapi.ServiceTypeLoadBalancer
}

func validateOVNConfigEndpoint(ep *kapi.Endpoints) bool {
	return len(ep.Subsets) == 1 && len(ep.Subsets[0].Ports) == 2 && len(ep.Subsets[0].Addresses) > 0
}

// ExtractDbRemotesFromEndpoint extracts the DB endpoints
func ExtractDbRemotesFromEndpoint(ep *kapi.Endpoints) ([]string, int32, int32, error) {
	var nbDBPort int32
	var sbDBPort int32
	var masterIPList []string

	if !validateOVNConfigEndpoint(ep) {
		return masterIPList, nbDBPort, sbDBPort, fmt.Errorf("endpoint %s is not in the right format to configure OVN", ep.Name)
	}

	for _, ovnDB := range ep.Subsets[0].Ports {
		if ovnDB.Name == "south" {
			sbDBPort = ovnDB.Port
		} else if ovnDB.Name == "north" {
			nbDBPort = ovnDB.Port
		}
	}
	for _, address := range ep.Subsets[0].Addresses {
		masterIPList = append(masterIPList, address.IP)
	}

	return masterIPList, sbDBPort, nbDBPort, nil
}

func parsePodNetworkObjectName(podnetwork string) (string, string, error) {
	var netNsName string
	var networkName string

	slashItems := strings.Split(podnetwork, "/")
	if len(slashItems) == 2 {
		netNsName = strings.TrimSpace(slashItems[0])
		networkName = slashItems[1]
	} else if len(slashItems) == 1 {
		networkName = slashItems[0]
	} else {
		return "", "", fmt.Errorf("Invalid network object (failed at '/')")
	}

	atItems := strings.Split(networkName, "@")
	networkName = strings.TrimSpace(atItems[0])

	return netNsName, networkName, nil
}

func parsePodNetworkAnnotation(podNetworks, defaultNamespace string) ([]*types.NetworkSelectionElement, error) {
	var networks []*types.NetworkSelectionElement

	if podNetworks == "" {
		return nil, fmt.Errorf("parsePodNetworkAnnotation: pod annotation not having \"network\" as key, refer Multus README.md for the usage guide")
	}

	if strings.IndexAny(podNetworks, "[{\"") >= 0 {
		if err := json.Unmarshal([]byte(podNetworks), &networks); err != nil {
			return nil, fmt.Errorf("parsePodNetworkAnnotation: failed to parse pod Network Attachment Selection Annotation JSON format: %v", err)
		}
	} else {
		// Comma-delimited list of network attachment object names
		for _, item := range strings.Split(podNetworks, ",") {
			// Remove leading and trailing whitespace.
			item = strings.TrimSpace(item)

			// Parse network name (i.e. <namespace>/<network name>@<ifname>)
			netNsName, networkName, err := parsePodNetworkObjectName(item)
			if err != nil {
				return nil, fmt.Errorf("parsePodNetworkAnnotation: %v", err)
			}

			networks = append(networks, &types.NetworkSelectionElement{
				Name:      networkName,
				Namespace: netNsName,
			})
		}
	}

	for _, net := range networks {
		if net.Namespace == "" {
			net.Namespace = defaultNamespace
		}
	}

	return networks, nil
}

// GetK8sPodDefaultNetwork get pod default network from annotations
func GetK8sPodDefaultNetwork(pod *kapi.Pod) (*types.NetworkSelectionElement, error) {
	var netAnnot string

	netAnnot, ok := pod.Annotations[defaultNetAnnot]
	if !ok {
		return nil, nil
	}

	// The CRD object of default network should only be defined in multusNamespace
	networks, err := parsePodNetworkAnnotation(netAnnot, defaultMultusNamespace)
	if err != nil {
		return nil, fmt.Errorf("GetK8sPodDefaultNetwork: failed to parse CRD object: %v", err)
	}
	if len(networks) > 1 {
		return nil, fmt.Errorf("GetK8sPodDefaultNetwork: more than one default network is specified: %s", netAnnot)
	}

	return networks[0], nil
}

// GetPodNetwork gets net-attach-def annotation from pod
func GetPodNetwork(pod *kapi.Pod) ([]*types.NetworkSelectionElement, error) {
	var networks []*types.NetworkSelectionElement
	var err error

	netAnnot, ok := pod.Annotations[networkAttachmentAnnot]
	if !ok {
		return nil, nil
	}

	if len(netAnnot) == 0 {
		return nil, fmt.Errorf("Invalid kubernetes network %v", networkAttachmentAnnot)
	}
	networks, err = parsePodNetworkAnnotation(netAnnot, pod.ObjectMeta.Namespace)
	if err != nil {
		return nil, err
	}

	return networks, nil
}
