package node

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	kapi "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/retry"
	"k8s.io/klog/v2"

	nettypes "github.com/k8snetworkplumbingwg/network-attachment-definition-client/pkg/apis/k8s.cni.cncf.io/v1"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/cni"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/factory"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/kube"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/types"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
)

// Check if the Pod is ready so that we can add its associated DPU to br-int.
// If true, return its dpuConnDetails, otherwise return nil
func (bnnc *BaseNodeNetworkController) podReadyToAddDPU(pod *kapi.Pod, nadName string) *util.DPUConnectionDetails {
	if bnnc.name != pod.Spec.NodeName {
		klog.V(5).Infof("Pod %s/%s is not scheduled on this node %s", pod.Namespace, pod.Name, bnnc.name)
		return nil
	}

	dpuCD, err := util.UnmarshalPodDPUConnDetails(pod.Annotations, nadName)
	if err != nil {
		if !util.IsAnnotationNotSetError(err) {
			klog.Errorf("Failed to get dpu annotation for pod %s/%s nad %s: %v",
				pod.Namespace, pod.Name, nadName, err)
		} else {
			klog.V(5).Infof("DPU connection details annotation still not found for %s/%s for NAD %s",
				pod.Namespace, pod.Name, nadName)
		}
		return nil
	}

	return dpuCD
}

func (bnnc *BaseNodeNetworkController) addDPUPod4Nad(pod *kapi.Pod, dpuCD *util.DPUConnectionDetails, nadName string,
	podLister corev1listers.PodLister, kclient kubernetes.Interface) error {
	podDesc := fmt.Sprintf("pod %s/%s for nad %s", pod.Namespace, pod.Name, nadName)
	klog.Infof("Adding %s on DPU", podDesc)
	isOvnUpEnabled := atomic.LoadInt32(&bnnc.atomicOvnUpEnabled) > 0
	podInterfaceInfo, err := cni.PodAnnotation2PodInfo(pod.Annotations, nil, isOvnUpEnabled,
		string(pod.UID), "", nadName, bnnc.GetNetworkName(), bnnc.MTU())
	if err != nil {
		klog.Errorf("Failed to get pod interface information of %s: %v. retrying", podDesc, err)
		return err
	}
	err = bnnc.addRepPort(pod, dpuCD, podInterfaceInfo, podLister, kclient)
	if err != nil {
		klog.Errorf("Failed to add rep port for %s, %v. retrying", podDesc, err)
	}
	return err
}

// watchPodsDPU watch updates for pod dpu annotations
func (bnnc *BaseNodeNetworkController) watchPodsDPU() (*factory.Handler, error) {
	podLister := corev1listers.NewPodLister(bnnc.watchFactory.LocalPodInformer().GetIndexer())
	kclient := bnnc.Kube.(*kube.Kube)

	podHandler, err := bnnc.watchFactory.AddPodHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			pod := obj.(*kapi.Pod)
			klog.V(5).Infof("Add for Pod: %s/%s for network %s", pod.Namespace, pod.Name, bnnc.GetNetworkName())
			if !util.PodWantsNetwork(pod) || pod.Status.Phase == kapi.PodRunning {
				return
			}
			// add all the Pod's Nad into Pod's podNadCache
			networkMap := make(map[string]*nettypes.NetworkSelectionElement)
			if !bnnc.IsSecondary() {
				networkMap[types.DefaultNetworkName] = nil
			} else {
				var on bool
				var err error
				on, networkMap, err = util.PodWantsMultiNetwork(pod, bnnc.NetInfo)
				if !on || err != nil {
					if err != nil {
						klog.Warningf("Error getting network-attachment for pod %s/%s network %s: %v",
							pod.Namespace, pod.Name, bnnc.GetNetworkName(), err)
					}
					return
				}
			}
			bnnc.podNadCache.Store(pod.UID, networkMap)

			// initialize serverCache to be empty
			servedCache := map[string]*util.DPUConnectionDetails{}
			for nadName := range networkMap {
				dpuCD := bnnc.podReadyToAddDPU(pod, nadName)
				if dpuCD != nil {
					err := bnnc.addDPUPod4Nad(pod, dpuCD, nadName, podLister, kclient.KClient)
					if err == nil {
						servedCache[nadName] = dpuCD
					}
				}
			}
			bnnc.servedPods.Store(pod.UID, servedCache)
		},
		UpdateFunc: func(old, newer interface{}) {
			oldPod := old.(*kapi.Pod)
			newPod := newer.(*kapi.Pod)
			klog.V(5).Infof("Update for Pod: %s/%s for network %s", newPod.Namespace, newPod.Name, bnnc.GetNetworkName())
			v, ok := bnnc.podNadCache.Load(newPod.UID)
			if !ok {
				klog.V(5).Infof("Skipping update for Pod %s/%s as it is not attached to network: %s",
					newPod.Namespace, newPod.Name, bnnc.GetNetworkName())
				return
			}

			networkMap := v.(map[string]*nettypes.NetworkSelectionElement)

			servedCache := map[string]*util.DPUConnectionDetails{}
			v, ok = bnnc.servedPods.Load(newPod.UID)
			if ok {
				servedCache = v.(map[string]*util.DPUConnectionDetails)
			}
			for nadName := range networkMap {
				podDesc := fmt.Sprintf("pod %s/%s for nad %s", newPod.Namespace, newPod.Name, nadName)
				var oldDpuCD *util.DPUConnectionDetails
				v, ok := servedCache[nadName]
				if ok {
					oldDpuCD = v
				}
				newDpuCD := bnnc.podReadyToAddDPU(newPod, nadName)
				if oldDpuCD == nil && newDpuCD == nil {
					continue
				}
				if oldDpuCD != nil {
					// VF already added, but new Pod has changed, we'd need to delete the old VF
					if newDpuCD == nil || oldDpuCD.PfId != newDpuCD.PfId ||
						oldDpuCD.VfId != newDpuCD.VfId || oldDpuCD.SandboxId != newDpuCD.SandboxId {
						klog.Infof("Deleting the old VF since either kubelet issued cmdDEL or assigned a new VF or "+
							"the sandbox id itself changed. Old connection details (%v), New connection details (%v)",
							oldDpuCD, newDpuCD)
						err := bnnc.updatePodDPUConnStatusWithRetry(oldPod, nil, nadName)
						if err != nil {
							klog.Errorf("Failed to remove the old DPU connection status annotation for %s: %v", podDesc, err)
						}
						vfRepName, err := util.GetSriovnetOps().GetVfRepresentorDPU(oldDpuCD.PfId, oldDpuCD.VfId)
						if err != nil {
							klog.Errorf("Failed to get old VF representor for %s, dpuConnDetail %+v Representor port may have been deleted", podDesc, oldDpuCD, err)
						} else {
							err = bnnc.delRepPort(oldDpuCD, vfRepName, nadName, podDesc)
							if err != nil {
								klog.Errorf("Failed to delete VF representor for %s: %v", podDesc, err)
							}
						}
						delete(servedCache, nadName)
					}
				}
				if newDpuCD != nil {
					// if VF was failed to be added before or, if new Pod has changed, we'd need to add the new VF
					if oldDpuCD == nil || oldDpuCD.PfId != newDpuCD.PfId ||
						oldDpuCD.VfId != newDpuCD.VfId || oldDpuCD.SandboxId != newDpuCD.SandboxId {
						klog.Infof("Adding VF during update because either during Pod Add we failed to add VF or "+
							"connection details weren't present or the VF ID has changed. Old connection details (%v), "+
							"New connection details (%v)", oldDpuCD, newDpuCD)
						err := bnnc.addDPUPod4Nad(newPod, newDpuCD, nadName, podLister, kclient.KClient)
						if err == nil {
							servedCache[nadName] = newDpuCD
						}
					}
				}
			}
			bnnc.servedPods.Store(newPod.UID, servedCache)
		},
		DeleteFunc: func(obj interface{}) {
			pod := obj.(*kapi.Pod)
			_, ok := bnnc.podNadCache.Load(pod.UID)
			if !ok {
				klog.V(5).Infof("Skipping delete for Pod %s/%s as it is not attached to network: %s",
					pod.Namespace, pod.Name, bnnc.GetNetworkName())
				return
			}
			klog.V(5).Infof("Delete for Pod: %s/%s for network %s", pod.Namespace, pod.Name, bnnc.GetNetworkName())
			bnnc.podNadCache.Delete(pod.UID)
			v, ok := bnnc.servedPods.Load(pod.UID)
			if !ok {
				klog.V(5).Infof("Pod %s/%s is not attached to network: %s", pod.Namespace, pod.Name, bnnc.GetNetworkName())
				return
			}
			servedCache := v.(map[string]*util.DPUConnectionDetails)
			bnnc.servedPods.Delete(pod.UID)
			for nadName, dpuCD := range servedCache {
				podDesc := fmt.Sprintf("pod %s/%s for nad %s", pod.Namespace, pod.Name, nadName)
				klog.Infof("Deleting %s from DPU", podDesc)
				vfRepName, err := util.GetSriovnetOps().GetVfRepresentorDPU(dpuCD.PfId, dpuCD.VfId)
				if err != nil {
					klog.Errorf("Failed to get VF representor for %s, dpuConnDetail %+v. Representor port may have been deleted", podDesc, dpuCD, err)
					continue
				}
				err = bnnc.delRepPort(dpuCD, vfRepName, nadName, podDesc)
				if err != nil {
					klog.Errorf("Failed to delete VF representor for %s: %v", podDesc, err)
				}
			}
		},
	}, nil)
	return podHandler, err
}

// updatePodDPUConnStatusWithRetry update the pod annotion with the givin connection details
func (bnnc *BaseNodeNetworkController) updatePodDPUConnStatusWithRetry(origPod *kapi.Pod,
	dpuConnStatus *util.DPUConnectionStatus, nadName string) error {
	podDesc := fmt.Sprintf("pod %s/%s", origPod.Namespace, origPod.Name)
	klog.Infof("Updating pod %s with connection status (%+v) for NAD %s", podDesc, dpuConnStatus, nadName)
	resultErr := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
		pod, err := bnnc.watchFactory.GetPod(origPod.Namespace, origPod.Name)
		if err != nil {
			return err
		}
		// Informer cache should not be mutated, so get a copy of the object
		cpod := pod.DeepCopy()
		cpod.Annotations, err = util.MarshalPodDPUConnStatus(cpod.Annotations, dpuConnStatus, nadName)
		if err != nil {
			if util.IsAnnotationAlreadySetError(err) {
				return nil
			}
			return err
		}
		return bnnc.Kube.UpdatePod(cpod)
	})
	if resultErr != nil {
		return fmt.Errorf("failed to update %s annotation for %s: %v", util.DPUConnetionStatusAnnot, podDesc, resultErr)
	}
	return nil
}

// addRepPort adds the representor of the VF to the ovs bridge
func (bnnc *BaseNodeNetworkController) addRepPort(pod *kapi.Pod, dpuCD *util.DPUConnectionDetails, ifInfo *cni.PodInterfaceInfo, podLister corev1listers.PodLister, kclient kubernetes.Interface) error {

	nadName := ifInfo.NADName
	podDesc := fmt.Sprintf("pod %s/%s for NAD %s", pod.Namespace, pod.Name, nadName)
	vfRepName, err := util.GetSriovnetOps().GetVfRepresentorDPU(dpuCD.PfId, dpuCD.VfId)
	if err != nil {
		klog.Infof("Failed to get VF representor for %s dpuConnDetail %+v: %v", podDesc, dpuCD, err)
		return err
	}

	// set VfNetdevName so OVS interface can be added with external_ids:vf-netdev-name, and is able to
	// be part of healthcheck.
	ifInfo.VfNetdevName = vfRepName
	klog.Infof("Adding VF representor %s for %s", vfRepName, podDesc)
	err = cni.ConfigureOVS(context.TODO(), pod.Namespace, pod.Name, vfRepName, ifInfo, dpuCD.SandboxId, podLister, kclient)
	if err != nil {
		// Note(adrianc): we are lenient with cleanup in this method as pod is going to be retried anyway.
		_ = bnnc.delRepPort(dpuCD, vfRepName, nadName, podDesc)
		return err
	}
	klog.Infof("Port %s added to bridge br-int", vfRepName)

	link, err := util.GetNetLinkOps().LinkByName(vfRepName)
	if err != nil {
		_ = bnnc.delRepPort(dpuCD, vfRepName, nadName, podDesc)
		return fmt.Errorf("failed to get link device for interface %s", vfRepName)
	}

	if err = util.GetNetLinkOps().LinkSetMTU(link, ifInfo.MTU); err != nil {
		_ = bnnc.delRepPort(dpuCD, vfRepName, nadName, podDesc)
		return fmt.Errorf("failed to setup representor port. failed to set MTU for interface %s", vfRepName)
	}

	if err = util.GetNetLinkOps().LinkSetUp(link); err != nil {
		_ = bnnc.delRepPort(dpuCD, vfRepName, nadName, podDesc)
		return fmt.Errorf("failed to setup representor port. failed to set link up for interface %s", vfRepName)
	}

	// Update connection-status annotation
	// TODO(adrianc): we should update Status in case of error as well
	connStatus := util.DPUConnectionStatus{Status: util.DPUConnectionStatusReady, Reason: ""}
	err = bnnc.updatePodDPUConnStatusWithRetry(pod, &connStatus, nadName)
	if err != nil {
		_ = util.GetNetLinkOps().LinkSetDown(link)
		_ = bnnc.delRepPort(dpuCD, vfRepName, nadName, podDesc)
		return fmt.Errorf("failed to setup representor port. failed to set pod annotations. %v", err)
	}
	return nil
}

// delRepPort delete the representor of the VF from the ovs bridge
func (bnnc *BaseNodeNetworkController) delRepPort(dpuCD *util.DPUConnectionDetails, vfRepName, nadName, podDesc string) error {
	//TODO(adrianc): handle: clearPodBandwidth(pr.SandboxID), pr.deletePodConntrack()
	klog.Infof("Delete VF representor %s for %s", vfRepName, podDesc)
	ifExists, sandbox, expectedNADName, err := util.GetOVSPortPodInfo(vfRepName)
	if err != nil {
		return fmt.Errorf(err.Error())
	}
	if !ifExists {
		klog.Infof("VF representor %s for %s is not an OVS interface, nothing to do", vfRepName, podDesc)
		return nil
	}
	if sandbox != dpuCD.SandboxId {
		return fmt.Errorf("OVS port %s was added for sandbox (%s), expecting (%s)", vfRepName, sandbox, dpuCD.SandboxId)
	}
	if expectedNADName != nadName {
		return fmt.Errorf("OVS port %s was added for nad (%s), expecting (%s)", vfRepName, expectedNADName, nadName)
	}
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
