package node

import (
	"context"
	"fmt"
	"sync"
	"time"

	kapi "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/cni"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
)

// watchPodsDPU watch updates for pod dpu annotations
func (nnci *NodeNetworkControllerInfo) watchPodsDPU(isOvnUpEnabled bool) (*factory.Handler, error) {
	var retryPods sync.Map
	// servedPods tracks the pods that got a VF
	var servedPods sync.Map

	// when specific nad is in podNadCache but not in servedPod cache, we'd need to retry

	klog.Infof("Starting Pod watch on network %s", nnci.GetNetworkName())

	podLister := corev1listers.NewPodLister(nnci.watchFactory.LocalPodInformer().GetIndexer())
	kclient := nnci.Kube.(*kube.Kube)

	podHandler, err := nnci.watchFactory.AddPodHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*kapi.Pod)
			klog.Infof("Add for Pod: %s/%s for network", pod.ObjectMeta.GetNamespace(), pod.ObjectMeta.GetName(), nnci.GetNetworkName())
			if !util.PodWantsNetwork(pod) || pod.Status.Phase == kapi.PodRunning {
				return
			}
			on, network, err := util.IsNetworkOnPod(pod, nnci)
			if err != nil || !on {
				// the Pod is not attached to this specific network
				klog.V(5).Infof("Skipping add for Pod %s/%s as it is not attached to network: %s",
					pod.Namespace, pod.Name, nnci.GetNetworkName())
				return
			}
			nadName := types.DefaultNetworkName
			if nnci.IsSecondary() {
				nadName = util.GetNadName(network.Namespace, network.Name)
			}
			if util.PodScheduled(pod) {
				// Is this pod created on same node as the DPU
				if nnci.name != pod.Spec.NodeName {
					return
				}

				vfRepName, err := nnci.getVfRepName(pod, nadName)
				if err != nil {
					klog.Infof("Failed to get rep name, %s. retrying", err)
					retryPods.Store(pod.UID, nadName)
					return
				}
				mtu := config.Default.MTU
				if nnci.IsSecondary() {
					mtu = nnci.GetMtu()
				}
				podInterfaceInfo, err := cni.PodAnnotation2PodInfo(pod.Annotations, isOvnUpEnabled, string(pod.UID),
					"", nadName, mtu, nnci.IsSecondary())
				if err != nil {
					retryPods.Store(pod.UID, nadName)
					return
				}
				err = nnci.addRepPort(pod, vfRepName, podInterfaceInfo, podLister, kclient.KClient)
				if err != nil {
					klog.Infof("Failed to add rep port, %s. retrying", err)
					retryPods.Store(pod.UID, nadName)
				} else {
					servedPods.Store(pod.UID, nadName)
				}
			} else {
				// Handle unscheduled pods later in UpdateFunc
				retryPods.Store(pod.UID, nadName)
				return
			}
		},
		UpdateFunc: func(old, newer interface{}) {
			pod := newer.(*kapi.Pod)
			klog.Infof("Update for Pod: %s/%s", pod.ObjectMeta.GetNamespace(), pod.ObjectMeta.GetName())
			if !util.PodWantsNetwork(pod) || pod.Status.Phase == kapi.PodRunning {
				retryPods.Delete(pod.UID)
				return
			}
			v, retry := retryPods.Load(pod.UID)
			if util.PodScheduled(pod) && retry {
				if nnci.name != pod.Spec.NodeName {
					retryPods.Delete(pod.UID)
					return
				}
				nadName := v.(string)
				vfRepName, err := nnci.getVfRepName(pod, nadName)
				if err != nil {
					klog.Infof("Failed to get rep name, %s. retrying", err)
					return
				}
				podInterfaceInfo, err := cni.PodAnnotation2PodInfo(pod.Annotations, isOvnUpEnabled, string(pod.UID),
					"", nadName, nnci.GetMtu(), nnci.IsSecondary())
				if err != nil {
					return
				}
				err = nnci.addRepPort(pod, vfRepName, podInterfaceInfo, podLister, kclient.KClient)
				if err != nil {
					klog.Infof("Failed to add rep port, %s. retrying", err)
				} else {
					servedPods.Store(pod.UID, nadName)
					retryPods.Delete(pod.UID)
				}
			}
		},
		DeleteFunc: func(obj interface{}) {
			pod := obj.(*kapi.Pod)
			klog.Infof("Delete for Pod: %s/%s", pod.ObjectMeta.GetNamespace(), pod.ObjectMeta.GetName())
			v, ok := servedPods.Load(pod.UID)
			if !ok {
				return
			}
			nadName := v.(string)
			servedPods.Delete(pod.UID)
			retryPods.Delete(pod.UID)
			vfRepName, err := nnci.getVfRepName(pod, nadName)
			if err != nil {
				klog.Errorf("Failed to get VF Representor Name from Pod: %s. Representor port may have been deleted.", err)
				return
			}
			err = nnci.delRepPort(vfRepName)
			if err != nil {
				klog.Errorf("Failed to delete VF representor %s. %s", vfRepName, err)
			}
		},
	}, nil)

	return podHandler, err
}

// getVfRepName returns the VF's representor of the VF assigned to the pod
func (nnci *NodeNetworkControllerInfo) getVfRepName(pod *kapi.Pod, nadName string) (string, error) {
	dpuCD, err := util.UnmarshalPodDPUConnDetails(pod.Annotations, nadName)
	if err != nil {
		return "", fmt.Errorf("failed to get dpu annotation for pod %s/%s: %v", pod.Namespace, pod.Name, err)
	}
	return util.GetSriovnetOps().GetVfRepresentorDPU(dpuCD.PfId, dpuCD.VfId)
}

// updatePodDPUConnStatusWithRetry update the pod annotion with the givin connection details
func (nnci *NodeNetworkControllerInfo) updatePodDPUConnStatusWithRetry(origPod *kapi.Pod,
	dpuConnStatus *util.DPUConnectionStatus) error {
	podDesc := fmt.Sprintf("pod %s/%s", origPod.Namespace, origPod.Name)
	resultErr := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		pod, err := nnci.watchFactory.GetPod(origPod.Namespace, origPod.Name)
		if err != nil {
			return err
		}
		// Informer cache should not be mutated, so get a copy of the object
		cpod := pod.DeepCopy()
		cpod.Annotations, err = util.MarshalPodDPUConnStatus(cpod.Annotations, dpuConnStatus, types.DefaultNetworkName)
		if err != nil {
			return err
		}
		return nnci.Kube.UpdatePod(cpod)
	})
	if resultErr != nil {
		return fmt.Errorf("failed to update %s annotation for %s: %v", util.DPUConnetionStatusAnnot, podDesc, resultErr)
	}
	return nil
}

// addRepPort adds the representor of the VF to the ovs bridge
func (nnci *NodeNetworkControllerInfo) addRepPort(pod *kapi.Pod, vfRepName string, ifInfo *cni.PodInterfaceInfo, podLister corev1listers.PodLister, kclient kubernetes.Interface) error {
	klog.Infof("Adding VF representor %s", vfRepName)
	nadName := ifInfo.NadName
	dpuCD, err := util.UnmarshalPodDPUConnDetails(pod.Annotations, nadName)
	if err != nil {
		return fmt.Errorf("failed to get dpu annotation. %v", err)
	}

	err = cni.ConfigureOVS(context.TODO(), pod.Namespace, pod.Name, vfRepName, ifInfo, dpuCD.SandboxId, podLister, kclient)
	if err != nil {
		// Note(adrianc): we are lenient with cleanup in this method as pod is going to be retried anyway.
		_ = nnci.delRepPort(vfRepName)
		return err
	}
	klog.Infof("Port %s added to bridge br-int", vfRepName)

	link, err := util.GetNetLinkOps().LinkByName(vfRepName)
	if err != nil {
		_ = nnci.delRepPort(vfRepName)
		return fmt.Errorf("failed to get link device for interface %s", vfRepName)
	}

	if err = util.GetNetLinkOps().LinkSetMTU(link, ifInfo.MTU); err != nil {
		_ = nnci.delRepPort(vfRepName)
		return fmt.Errorf("failed to setup representor port. failed to set MTU for interface %s", vfRepName)
	}

	if err = util.GetNetLinkOps().LinkSetUp(link); err != nil {
		_ = nnci.delRepPort(vfRepName)
		return fmt.Errorf("failed to setup representor port. failed to set link up for interface %s", vfRepName)
	}

	// Update connection-status annotation
	// TODO(adrianc): we should update Status in case of error as well
	connStatus := util.DPUConnectionStatus{Status: util.DPUConnectionStatusReady, Reason: ""}
	err = nnci.updatePodDPUConnStatusWithRetry(pod, &connStatus)
	if err != nil {
		_ = util.GetNetLinkOps().LinkSetDown(link)
		_ = nnci.delRepPort(vfRepName)
		return fmt.Errorf("failed to setup representor port. failed to set pod annotations. %v", err)
	}
	return nil
}

// delRepPort delete the representor of the VF from the ovs bridge
func (nnci *NodeNetworkControllerInfo) delRepPort(vfRepName string) error {
	//TODO(adrianc): handle: clearPodBandwidth(pr.SandboxID), pr.deletePodConntrack()
	klog.Infof("Delete VF representor %s port", vfRepName)
	// Set link down for representor port
	link, err := util.GetNetLinkOps().LinkByName(vfRepName)
	if err != nil {
		klog.Warningf("Failed to get link device for representor port %s. %v", vfRepName, err)
	} else {
		if linkDownErr := util.GetNetLinkOps().LinkSetDown(link); linkDownErr != nil {
			klog.Warningf("Failed to set link down for representor port %s. %v", vfRepName, linkDownErr)
		}
	}

	// remove from br-int
	return wait.PollImmediate(500*time.Millisecond, 60*time.Second, func() (bool, error) {
		_, _, err := util.RunOVSVsctl("--if-exists", "del-port", "br-int", vfRepName)
		if err != nil {
			return false, nil
		}
		klog.Infof("Port %s deleted from bridge br-int", vfRepName)
		return true, nil
	})
}
