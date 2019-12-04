package cluster

import (
	"fmt"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/ovn"
	"io/ioutil"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/cni"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/config"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"

	kapi "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/tools/cache"
)

type postReadyFn func() error

func isOVNControllerReady(name string) (bool, error) {
	const runDir string = "/var/run/openvswitch/"

	pid, err := ioutil.ReadFile(runDir + "ovn-controller.pid")
	if err != nil {
		return false, fmt.Errorf("unknown pid for ovn-controller process: %v", err)
	}

	err = wait.PollImmediate(500*time.Millisecond, 60*time.Second, func() (bool, error) {
		ctlFile := runDir + fmt.Sprintf("ovn-controller.%s.ctl", strings.TrimSuffix(string(pid), "\n"))
		ret, _, err := util.RunOVSAppctl("-t", ctlFile, "connection-status")
		if err == nil {
			logrus.Infof("node %s connection status = %s", name, ret)
			return ret == "connected", nil
		}
		return false, err
	})
	if err != nil {
		return false, fmt.Errorf("timed out waiting sbdb for node %s: %v", name, err)
	}

	err = wait.PollImmediate(500*time.Millisecond, 60*time.Second, func() (bool, error) {
		_, _, err := util.RunOVSVsctl("--", "br-exists", "br-int")
		if err != nil {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return false, fmt.Errorf("timed out checking whether br-int exists or not on node %s: %v", name, err)
	}

	err = wait.PollImmediate(500*time.Millisecond, 60*time.Second, func() (bool, error) {
		stdout, _, err := util.RunOVSOfctl("dump-aggregate", "br-int")
		if err != nil {
			return false, nil
		}
		return !strings.Contains(stdout, "flow_count=0"), nil
	})
	if err != nil {
		return false, fmt.Errorf("timed out dumping br-int flow entries for node %s: %v", name, err)
	}

	return true, nil
}

// StartClusterNode learns the subnet assigned to it by the master controller
// and calls the SetupNode script which establishes the logical switch
func (cluster *OvnClusterController) StartClusterNode(name string) error {
	count := 300
	var err error
	var node *kapi.Node
	var subnet *net.IPNet
	var clusterSubnets []string
	var nodeCidr, lsCidr string
	var gotAnnotation, ok bool
	var wg sync.WaitGroup

	if config.MasterHA.ManageDBServers {
		var readyChan = make(chan bool, 1)

		err = cluster.watchConfigEndpoints(readyChan)
		if err != nil {
			return err
		}
		// Hold until we are certain that the endpoint has been setup.
		// We risk polling an inactive master if we don't wait while a new leader election is on-going
		<-readyChan
	} else {
		for _, auth := range []config.OvnAuthConfig{config.OvnNorth, config.OvnSouth} {
			if err := auth.SetDBAuth(); err != nil {
				return err
			}
		}
	}

	err = setupOVNNode(name)
	if err != nil {
		return err
	}

	for _, clusterSubnet := range config.Default.ClusterSubnets {
		clusterSubnets = append(clusterSubnets, clusterSubnet.CIDR.String())
	}

	for count > 0 {
		if count != 300 {
			time.Sleep(time.Second)
		}
		count--

		if !gotAnnotation {
			// setup the node, create the logical switch
			node, err = cluster.Kube.GetNode(name)
			if err != nil {
				logrus.Errorf("Error starting node %s, no node found - %v", name, err)
				continue
			}

			nodeCidr, ok = node.Annotations[ovn.OvnHostSubnet]
			if !ok {
				logrus.Errorf("Error starting node %s, no annotation found on node for subnet - %v", name, err)
				continue
			}
			_, subnet, err = net.ParseCIDR(nodeCidr)
			if err != nil {
				logrus.Errorf("Invalid hostsubnet found for node %s - %v", node.Name, err)
				return err
			}
			gotAnnotation = true
		}

		lsCidr, _, err = util.RunOVNNbctl("get", "logical_switch", node.Name, "other-config:subnet")
		if err != nil {
			logrus.Errorf("Error getting CIDR for the node %s's logical switch", node.Name)
			continue
		}
		if lsCidr != nodeCidr {
			return fmt.Errorf("OVN logical switch's CIDR (%q) and K8s node's CIDR (%q) are not the same. Please "+
				"delete the node and add it back to the cluster", lsCidr, nodeCidr)
		}

		break
	}

	if count == 0 {
		logrus.Errorf("Failed to get node/node-annotation for %s - %v", name, err)
		return err
	}

	logrus.Infof("Node %s ready for ovn initialization with subnet %s", node.Name, subnet.String())

	if _, err = isOVNControllerReady(name); err != nil {
		return err
	}

	type readyFunc func(string, string) (bool, error)
	var readyFuncs []readyFunc
	var nodeAnnotations map[string]string
	var postReady postReadyFn

	// If gateway is enabled, get gateway annotations
	if config.Gateway.Mode != config.GatewayModeDisabled {
		nodeAnnotations, postReady, err = cluster.initGateway(node.Name, subnet.String())
		if err != nil {
			return err
		}
		readyFuncs = append(readyFuncs, GatewayReady)
	}

	// Get management port annotations
	mgmtPortAnnotations, err := CreateManagementPort(node.Name, subnet, clusterSubnets)
	if err != nil {
		return err
	}

	readyFuncs = append(readyFuncs, ManagementPortReady)

	// Combine mgmtPortAnnotations with any existing gwyAnnotations
	for k, v := range mgmtPortAnnotations {
		nodeAnnotations[k] = v
	}

	wg.Add(len(readyFuncs))

	// Set node annotations
	err = cluster.Kube.SetAnnotationsOnNode(node, nodeAnnotations)
	if err != nil {
		return fmt.Errorf("Failed to set node %s annotation: %v", node.Name, nodeAnnotations)
	}

	portName := "k8s-" + node.Name
	messages := make(chan error)

	// Wait for the portMac to be created
	for _, f := range readyFuncs {
		go func(rf readyFunc) {
			defer wg.Done()
			err := wait.PollImmediate(500*time.Millisecond, 300*time.Second, func() (bool, error) {
				return rf(node.Name, portName)
			})
			messages <- err
		}(f)
	}
	go func() {
		wg.Wait()
		close(messages)
	}()

	for i := range messages {
		if i != nil {
			return fmt.Errorf("Timeout error while obtaining addresses for %s (%v)", portName, i)
		}
	}

	if postReady != nil {
		err = postReady()
		if err != nil {
			return err
		}
	}

	confFile := filepath.Join(config.CNI.ConfDir, config.CNIConfFileName)
	_, err = os.Stat(confFile)
	if os.IsNotExist(err) {
		err = config.WriteCNIConfig(config.CNI.ConfDir, config.CNIConfFileName)
		if err != nil {
			return err
		}
	}

	// start the cni server
	cniServer := cni.NewCNIServer("")
	err = cniServer.Start(cni.HandleCNIRequest)

	return err
}

func updateOVNConfig(ep *kapi.Endpoints, readyChan chan bool) error {
	masterIPList, southboundDBPort, northboundDBPort, err := util.ExtractDbRemotesFromEndpoint(ep)
	if err != nil {
		return err
	}

	config.UpdateOVNNodeAuth(masterIPList, strconv.Itoa(int(southboundDBPort)), strconv.Itoa(int(northboundDBPort)))

	for _, auth := range []config.OvnAuthConfig{config.OvnNorth, config.OvnSouth} {
		if err := auth.SetDBAuth(); err != nil {
			return err
		}
	}

	logrus.Infof("OVN databases reconfigured, masterIPs %v, northbound-db %v, southbound-db %v", masterIPList, northboundDBPort, southboundDBPort)

	readyChan <- true
	return nil
}

//watchConfigEndpoints starts the watching of Endpoint resource and calls back to the appropriate handler logic
func (cluster *OvnClusterController) watchConfigEndpoints(readyChan chan bool) error {
	_, err := cluster.watchFactory.AddFilteredEndpointsHandler(config.Kubernetes.OVNConfigNamespace, nil,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				ep := obj.(*kapi.Endpoints)
				if ep.Name == "ovnkube-db" {
					if err := updateOVNConfig(ep, readyChan); err != nil {
						logrus.Errorf(err.Error())
					}
				}
			},
			UpdateFunc: func(old, new interface{}) {
				epNew := new.(*kapi.Endpoints)
				epOld := old.(*kapi.Endpoints)
				if !reflect.DeepEqual(epNew.Subsets, epOld.Subsets) && epNew.Name == "ovnkube-db" {
					if err := updateOVNConfig(epNew, readyChan); err != nil {
						logrus.Errorf(err.Error())
					}
				}
			},
		}, nil)
	return err
}
