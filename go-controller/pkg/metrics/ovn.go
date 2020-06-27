package metrics

import (
	"strings"

	"github.com/ovn-org/ovn-kubernetes/go-controller/pkg/util"
	"github.com/prometheus/client_golang/prometheus"
	"k8s.io/klog"
)

var (
	ovnVersion    string
	ovsLibVersion string
)

func getOvnVersionInfo() {
	stdout, _, err := util.RunOVNNbctl("--version")
	if err != nil {
		return
	}

	// post ovs/ovn split, the output is:
	//	ovn-nbctl 20.03.90
	//	Open vSwitch Library 2.13.1
	//
	// and before the split we have:
	// ovn-nbctl (Open vSwitch) 2.12.0
	for _, line := range strings.Split(stdout, "\n") {
		if strings.HasPrefix("ovn-nbctl (Open vSwitch) ", line) {
			ovnVersion = strings.Fields(line)[3]
		} else if strings.HasPrefix("ovn-nbctl ", line) {
			ovnVersion = strings.Fields(line)[1]
		} else if strings.HasPrefix("Open  vSwitch Library ", line) {
			ovsLibVersion = strings.Fields(line)[3]
		}
	}
}

func RegisterOvnMetrics() {
	getOvnVersionInfo()
	prometheus.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: MetricOvnNamespace,
			Name:      "build_info",
			Help: "A metric with a constant '1' value labeled by version and library " +
				"from which ovn binaries were built",
			ConstLabels: prometheus.Labels{
				"version":         ovnVersion,
				"ovs_lib_version": ovsLibVersion,
			},
		},
		func() float64 { return 1 },
	))
	prometheus.MustRegister(prometheus.NewGaugeFunc(
		prometheus.GaugeOpts{
			Namespace: MetricOvnNamespace,
			Subsystem: MetricOvnSubsystemController,
			Name:      "geneve_ports_total",
			Help:      "Total number of OVN geneve ports on the node",
		}, func() float64 {
			stdout, stderr, err := util.RunOVSVsctl("--no-headings", "--data=bare", "--format=csv",
				"--columns=name", "find", "interface", "type=geneve")
			if err != nil {
				klog.Errorf("Failed to get geneve port count, stderr(%s): (%v)",
					stderr, err)
				return 0
			}
			ports := strings.Split(stdout, "\n")
			return float64(len(ports))
		}))
}
