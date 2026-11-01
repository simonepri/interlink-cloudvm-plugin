// Tests for the transfer meter: deltas of cumulative runner readings, peer classes and paths, sender-side pricing,
// per-pod series and their removal.
package main

import (
	"math"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

const gib = int64(1) << 30

func newTestTransferMeter(t *testing.T) (*TransferMeter, *ObservabilityMetrics, *time.Time) {
	t.Helper()
	metrics := NewObservabilityMetrics()
	book := fakeTransferBook{"us-east-1>eu-south-2>cross_region": 0.02}
	meter := NewTransferMeter(metrics, NewTransferCostModel(testTransferCostConfig(), book))
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	meter.now = func() time.Time { return now }
	return meter, metrics, &now
}

func approx(t *testing.T, what string, got, want float64) {
	t.Helper()
	if math.Abs(got-want) > 1e-9 {
		t.Errorf("%s = %v, want %v", what, got, want)
	}
}

var testTransferVM = TransferVM{Provider: "aws", Region: "eu-south-2", InstanceType: "g6e.xlarge", InstanceID: "i-1"}

func TestTransferMeter_ClassifiesAndPricesBySender(t *testing.T) {
	meter, m, _ := newTestTransferMeter(t)
	meter.Record(testTransferVM, &TrafficReport{
		Classified: true,
		Peers: map[string]TrafficBytes{
			"tunnel":                {Rx: 1 * gib, Tx: 2 * gib},
			"aws/us-east-1":         {Rx: 10 * gib, Tx: 4 * gib},
			"aws/us-east-1/storage": {Tx: 5 * gib},
			"aws/eu-south-2":        {Rx: 1 * gib, Tx: 1 * gib},
			"private":               {Rx: 1 * gib, Tx: 1 * gib},
			"aws/eu-central-2":      {Rx: 3 * gib, Tx: 1 * gib},
			"gcp/europe-west4":      {Tx: 1 * gib},
			"internet":              {Rx: 5 * gib, Tx: 1 * gib},
			"unclassified":          {Rx: 100},
		},
		Owners: map[string]TrafficOwner{
			"pulls": {Peers: map[string]TrafficBytes{"home_region": {Rx: 8 * gib, Tx: 1}}},
			"uid-1": {Namespace: "ray", Pod: "worker-0", Peers: map[string]TrafficBytes{
				"home_region":  {Rx: 1 * gib, Tx: 2 * gib},
				"home_storage": {Tx: 3 * gib},
				"tunnel":       {Tx: 1 * gib},
			}},
		},
	})

	bytes := func(direction, peer, path string) float64 {
		return counterValue(m.transferBytes.WithLabelValues("aws", "eu-south-2", "g6e.xlarge", direction, peer, path))
	}
	approx(t, "transmit to home, direct", bytes("transmit", "home_region", "direct"), float64(4*gib))
	approx(t, "transmit to home, tunnel", bytes("transmit", "home_region", "tunnel"), float64(2*gib))
	approx(t, "transmit to home object storage", bytes("transmit", "home_storage", "direct"), float64(5*gib))
	approx(t, "receive from the same region, own region and private", bytes("receive", "same_region", "direct"), float64(2*gib))
	approx(t, "receive from another region", bytes("receive", "cross_region", "direct"), float64(3*gib))
	approx(t, "transmit to another cloud and the internet", bytes("transmit", "internet", "direct"), float64(2*gib))
	approx(t, "unclassified receive", bytes("receive", "unclassified", "direct"), 100)

	cost := func(direction, peer, path, chargedRegion string) float64 {
		return counterValue(m.transferCost.WithLabelValues("aws", "eu-south-2", direction, peer, path, "aws", chargedRegion))
	}
	approx(t, "VM transmit to home is billed to the VM's region", cost("transmit", "home_region", "direct", "eu-south-2"), 4*0.02)
	approx(t, "tunnel transmit is billed to the VM's region", cost("transmit", "home_region", "tunnel", "eu-south-2"), 2*0.02)
	approx(t, "transmit to home object storage is billed to the VM's region", cost("transmit", "home_storage", "direct", "eu-south-2"), 5*0.02)
	approx(t, "receive from home is billed to home", cost("receive", "home_region", "direct", "us-east-1"), 10*0.02)
	approx(t, "tunnel receive is billed to home", cost("receive", "home_region", "tunnel", "us-east-1"), 1*0.02)
	approx(t, "transmit to another region", cost("transmit", "cross_region", "direct", "eu-south-2"), 1*0.02)
	approx(t, "transmit to the internet and another cloud", cost("transmit", "internet", "direct", "eu-south-2"), 2*0.09)
	approx(t, "receive from a region that is not home is not ours to pay", cost("receive", "cross_region", "direct", "eu-central-2"), 0)
	approx(t, "receive from the internet is not ours to pay", cost("receive", "internet", "direct", "eu-south-2"), 0)

	approx(t, "image pull bytes", counterValue(m.imagePullBytes.WithLabelValues("aws", "eu-south-2", "g6e.xlarge", "home_region")), float64(8*gib))
	pod := func(metric, direction, peer string) float64 {
		vec := m.podTransferBytes
		if metric == "cost" {
			vec = m.podTransferCost
		}
		return counterValue(vec.WithLabelValues("ray", "worker-0", "uid-1", "aws", "eu-south-2", direction, peer))
	}
	approx(t, "pod transmit bytes", pod("bytes", "transmit", "home_region"), float64(2*gib))
	approx(t, "pod transmit cost", pod("cost", "transmit", "home_region"), 2*0.02)
	approx(t, "pod receive cost billed from home", pod("cost", "receive", "home_region"), 1*0.02)
	approx(t, "pod transmit to home object storage", pod("bytes", "transmit", "home_storage"), float64(3*gib))
	approx(t, "pod transmit cost to home object storage", pod("cost", "transmit", "home_storage"), 3*0.02)
	approx(t, "pod tunnel transmit cost", pod("cost", "transmit", "tunnel"), 1*0.02)
}

func TestTransferMeter_CountsDeltasAndResets(t *testing.T) {
	meter, m, _ := newTestTransferMeter(t)
	report := func(tx int64) *TrafficReport {
		return &TrafficReport{Peers: map[string]TrafficBytes{"aws/us-east-1": {Tx: tx}}}
	}
	vm := testTransferVM
	vm.BootID = "boot-1"
	meter.Record(vm, report(1*gib))
	meter.Record(vm, report(1*gib)) // the same reading through another poll counts once
	meter.Record(vm, report(3*gib))
	meter.Record(vm, report(2*gib)) // an older reading answered after a newer one adds nothing
	vm.BootID = "boot-2"
	meter.Record(vm, report(1*gib)) // the VM rebooted and its counters started over
	meter.Record(TransferVM{Provider: "aws", Region: "eu-south-2", InstanceType: "g6e.xlarge"}, report(9*gib))
	meter.Record(vm, nil)
	approx(t, "transmit bytes", counterValue(m.transferBytes.WithLabelValues("aws", "eu-south-2", "g6e.xlarge", "transmit", "home_region", "direct")), float64(4*gib))
	approx(t, "transmit cost", counterValue(m.transferCost.WithLabelValues("aws", "eu-south-2", "transmit", "home_region", "direct", "aws", "eu-south-2")), 4*0.02)
}

func TestTransferMeter_DropsSeriesOfGonePods(t *testing.T) {
	meter, m, now := newTestTransferMeter(t)
	podReport := &TrafficReport{Owners: map[string]TrafficOwner{
		"uid-1": {Namespace: "ray", Pod: "worker-0", Peers: map[string]TrafficBytes{"internet": {Tx: 1 * gib}}},
	}}
	meter.Record(testTransferVM, podReport)
	series := func() int {
		ch := make(chan prometheus.Metric, 16)
		go func() { m.podTransferBytes.Collect(ch); close(ch) }()
		n := 0
		for range ch {
			n++
		}
		return n
	}
	if n := series(); n != 1 {
		t.Fatalf("pod series = %d, want 1", n)
	}
	*now = now.Add(transferPodIdle + 2*time.Minute)
	meter.Record(testTransferVM, &TrafficReport{Peers: map[string]TrafficBytes{"internet": {Tx: 1}}})
	if n := series(); n != 0 {
		t.Errorf("pod series after the pod went unreported = %d, want 0", n)
	}
	// A pod that comes back starts from its reading again instead of a stale baseline.
	meter.Record(testTransferVM, podReport)
	approx(t, "returning pod bytes", counterValue(m.podTransferBytes.WithLabelValues("ray", "worker-0", "uid-1", "aws", "eu-south-2", "transmit", "internet")), float64(1*gib))
}

func TestTransferMeter_ReportsWhetherTheMeterIsUp(t *testing.T) {
	meter, m, _ := newTestTransferMeter(t)
	up := func() float64 {
		return testutil.ToFloat64(m.trafficMeterUp.WithLabelValues("aws", "eu-south-2", "g6e.xlarge", "i-1"))
	}
	down := false
	meter.Record(testTransferVM, &TrafficReport{Up: &down, Error: "cannot install the traffic table"})
	approx(t, "meter up while it cannot start", up(), 0)
	// Runners that predate the field report bytes only while their meter runs.
	meter.Record(testTransferVM, &TrafficReport{Peers: map[string]TrafficBytes{"internet": {Tx: 1}}})
	approx(t, "meter up while it counts", up(), 1)
}

func TestClassifyTrafficPeer_TunnelWithoutHome(t *testing.T) {
	class, path, _, priced := classifyTrafficPeer("tunnel", TransferLocation{"aws", "eu-south-2"}, TransferLocation{}, false)
	if class != PeerUnclassified || path != PathTunnel || priced {
		t.Errorf("tunnel without a home = %s, %s, %v", class, path, priced)
	}
	class, _, _, _ = classifyTrafficPeer("aws/us-east-1", TransferLocation{"aws", "us-east-1"}, TransferLocation{"aws", "us-east-1"}, true)
	if class != PeerSameRegion {
		t.Errorf("home peer of a VM in the home region = %s, want same_region", class)
	}
	if class, _, _, priced := classifyTrafficPeer("bogus", TransferLocation{"aws", "eu-south-2"}, TransferLocation{}, false); class != PeerUnclassified || priced {
		t.Errorf("malformed peer = %s, %v", class, priced)
	}
}
