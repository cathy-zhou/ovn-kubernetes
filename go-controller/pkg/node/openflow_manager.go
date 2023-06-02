package node

import (
	"os"
	"sync"
	"time"

	OFManager "github.com/ovn-org/ovn-kubernetes/go-controller/pkg/openflow-manager"
	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	"github.com/pkg/errors"

	"k8s.io/klog/v2"
)

type openflowManager struct {
	defaultBridge         *bridgeConfiguration
	externalGatewayBridge *bridgeConfiguration
	defaultBridgeFlowID   string
	extGWBridgeFlowID     string
}

func (c *openflowManager) updateFlowCacheEntry(key string, flows []string) {
	OFManager.OpenFlowCacheManager.UpdateFlowCacheEntry(c.defaultBridgeFlowID, key, flows, false)
}

func (c *openflowManager) deleteFlowsByKey(key string) {
	OFManager.OpenFlowCacheManager.DeleteFlowsByKey(c.defaultBridgeFlowID, key, false)
}

func (c *openflowManager) getFlowCacheEntry(key string) []string {
	return OFManager.OpenFlowCacheManager.GetFlowsByKey(c.defaultBridgeFlowID, key)
}

func (c *openflowManager) updateExBridgeFlowCacheEntry(key string, flows []string) {
	OFManager.OpenFlowCacheManager.UpdateFlowCacheEntry(c.extGWBridgeFlowID, key, flows, false)
}

func (c *openflowManager) requestFlowSync() {
	OFManager.OpenFlowCacheManager.RequestFlowSync(c.defaultBridgeFlowID)
	if c.externalGatewayBridge != nil {
		OFManager.OpenFlowCacheManager.RequestFlowSync(c.extGWBridgeFlowID)
	}
}

// checkDefaultOpenFlow checks for the existence of default OpenFlow rules and
// exits if the output is not as expected
func (c *openflowManager) Run(stopChan <-chan struct{}, doneWg *sync.WaitGroup) {
	doneWg.Add(1)
	go func() {
		defer doneWg.Done()
		syncPeriod := 15 * time.Second
		timer := time.NewTicker(syncPeriod)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				if err := checkPorts(c.defaultBridge.patchPort, c.defaultBridge.ofPortPatch,
					c.defaultBridge.uplinkName, c.defaultBridge.ofPortPhys); err != nil {
					klog.Errorf("Checkports failed %v", err)
					continue
				}
				if c.externalGatewayBridge != nil {
					if err := checkPorts(
						c.externalGatewayBridge.patchPort, c.externalGatewayBridge.ofPortPatch,
						c.externalGatewayBridge.uplinkName, c.externalGatewayBridge.ofPortPhys); err != nil {
						klog.Errorf("Checkports failed %v", err)
						continue
					}
				}
			case <-stopChan:
				return
			}
		}
	}()
}

func checkPorts(patchIntf, ofPortPatch, physIntf, ofPortPhys string) error {
	// it could be that the ovn-controller recreated the patch between the host OVS bridge and
	// the integration bridge, as a result the ofport number changed for that patch interface
	curOfportPatch, stderr, err := util.GetOVSOfPort("--if-exists", "get", "Interface", patchIntf, "ofport")
	if err != nil {
		return errors.Wrapf(err, "Failed to get ofport of %s, stderr: %q", patchIntf, stderr)

	}
	if ofPortPatch != curOfportPatch {
		klog.Errorf("Fatal error: patch port %s ofport changed from %s to %s",
			patchIntf, ofPortPatch, curOfportPatch)
		os.Exit(1)
	}

	// it could be that someone removed the physical interface and added it back on the OVS host
	// bridge, as a result the ofport number changed for that physical interface
	curOfportPhys, stderr, err := util.GetOVSOfPort("--if-exists", "get", "interface", physIntf, "ofport")
	if err != nil {
		return errors.Wrapf(err, "Failed to get ofport of %s, stderr: %q", physIntf, stderr)
	}
	if ofPortPhys != curOfportPhys {
		klog.Errorf("Fatal error: phys port %s ofport changed from %s to %s",
			physIntf, ofPortPhys, curOfportPhys)
		os.Exit(1)
	}
	return nil
}
