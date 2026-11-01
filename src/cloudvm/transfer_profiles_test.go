// Tests for NodePool transfer profiles, the catalog's transfer rates, and the transfer spend gauge.
package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestTransferMeter_NodePoolProfiles(t *testing.T) {
	meter, _, now := newTestTransferMeter(t)
	remote := TransferVM{Provider: "aws", Region: "eu-south-2", InstanceType: "g6e.xlarge", InstanceID: "i-remote", NodePool: "gpu"}
	home := TransferVM{Provider: "aws", Region: "us-east-1", InstanceType: "g6e.xlarge", InstanceID: "i-home", NodePool: "gpu"}
	young := TransferVM{Provider: "aws", Region: "eu-south-2", InstanceType: "g6e.xlarge", InstanceID: "i-young", NodePool: "gpu"}
	report := func(pull, rx, tx, tunnelTx int64) *TrafficReport {
		return &TrafficReport{
			Peers: map[string]TrafficBytes{"aws/us-east-1": {Rx: rx, Tx: tx}, "tunnel": {Tx: tunnelTx}, "internet": {Rx: 50 * gib}},
			Owners: map[string]TrafficOwner{"pulls": {Peers: map[string]TrafficBytes{
				"home_region": {Rx: pull},
				"internet":    {Rx: 7 * gib}, // a public registry's pulls cost the home region nothing
			}}},
		}
	}
	meter.Record(remote, report(20*gib, 22*gib, 1*gib, 1*gib))
	meter.Record(home, report(20*gib, 22*gib, 9*gib, 0))
	*now = now.Add(30 * time.Minute)
	meter.Record(remote, report(20*gib, 22*gib, 2*gib, 1*gib))
	meter.Record(home, report(20*gib, 22*gib, 9*gib, 0))
	meter.Record(young, report(20*gib, 22*gib, 2*gib, 1*gib))

	got := meter.NodePoolTransfers()
	if len(got) != 1 || got[0].NodePool != "gpu" || got[0].VMs != 1 {
		t.Fatalf("profiles = %+v, want one from the remote VM that reported for 30 minutes", got)
	}
	p := got[0]
	approx(t, "image pulls per VM", p.ImagePullGBPerVM, 20)
	approx(t, "transmit to home per hour, direct and tunneled", p.HomeTransmitGBPerHour, 3/0.5)
	approx(t, "receive from home per hour besides pulls", p.HomeReceiveGBPerHour, 2/0.5)
	if p.LifetimeHours != 0 {
		t.Errorf("lifetime before any VM finished = %v", p.LifetimeHours)
	}

	// The remote VM stops reporting; its profile stays with the NodePool and gives it a lifetime.
	*now = now.Add(transferVMIdle + 2*time.Minute)
	meter.Record(home, report(20*gib, 22*gib, 9*gib, 0))
	got = meter.NodePoolTransfers()
	if len(got) != 1 || got[0].VMs != 1 {
		t.Fatalf("profiles after the VM finished = %+v", got)
	}
	approx(t, "finished VM lifetime", got[0].LifetimeHours, 0.5)
}

func TestTransferMeter_CatalogTransfer(t *testing.T) {
	meter, _, _ := newTestTransferMeter(t)
	got := meter.CatalogTransfer(map[string][]string{"aws": {"eu-south-2", "us-east-1", "eu-south-2"}, "gcp": {"europe-west4"}})
	if got == nil || got.HomeProvider != "aws" || got.HomeRegion != "us-east-1" {
		t.Fatalf("catalog transfer = %+v", got)
	}
	if len(got.Regions) != 3 {
		t.Fatalf("regions = %+v, want each provider's region once", got.Regions)
	}
	byRegion := map[string]int{}
	for i, r := range got.Regions {
		byRegion[r.Provider+"/"+r.Region] = i
	}
	eu := got.Regions[byRegion["aws/eu-south-2"]]
	if eu.FromHomeUSDPerGB == nil || *eu.FromHomeUSDPerGB != 0.02 || eu.ToHomeUSDPerGB == nil || *eu.ToHomeUSDPerGB != 0.02 {
		t.Errorf("eu-south-2 rates = %+v", eu)
	}
	if h := got.Regions[byRegion["aws/us-east-1"]]; h.FromHomeUSDPerGB == nil || *h.FromHomeUSDPerGB != 0 {
		t.Errorf("home region rates = %+v, want the same-region rate", h)
	}
	// The test tables have no internet rate for Google Cloud, so sending home from it is unknown.
	if g := got.Regions[byRegion["gcp/europe-west4"]]; g.ToHomeUSDPerGB != nil {
		t.Errorf("gcp rates = %+v, want no rate to home", g)
	}

	noHome := NewTransferMeter(NewObservabilityMetrics(), NewTransferCostModel(TransferCostConfig{}, nil))
	if got := noHome.CatalogTransfer(map[string][]string{"aws": {"eu-south-2"}}); got != nil {
		t.Errorf("catalog transfer without a home = %+v", got)
	}
}

func TestTransferMeter_SpendPerHourCoversTheLastWindow(t *testing.T) {
	meter, _, now := newTestTransferMeter(t)
	meter.Record(testTransferVM, &TrafficReport{Peers: map[string]TrafficBytes{"aws/us-east-1": {Tx: 10 * gib}}})
	approx(t, "spend per hour", meter.SpendPerHour("i-1"), 10*0.02/transferSpendWindow.Hours())
	*now = now.Add(transferSpendWindow + time.Minute)
	if got := meter.SpendPerHour("i-1"); got != 0 {
		t.Errorf("spend after the window = %v, want 0", got)
	}
	if got := meter.SpendPerHour("i-unknown"); got != 0 {
		t.Errorf("spend of an unknown VM = %v", got)
	}
}

func TestFleetTracker_TransferSpendSitsNextToComputeSpend(t *testing.T) {
	metrics := NewObservabilityMetrics()
	tracker := NewFleetTracker(metrics, NewHardwareCatalog())
	tracker.transferSpend = func(id string) float64 {
		if id == "i-a" || id == "i-b" {
			return 0.75
		}
		return 0
	}
	var instances []*Instance
	for _, id := range []string{"i-a", "i-b"} {
		instances = append(instances, &Instance{ID: id, Provider: ProviderAWS, Region: "eu-south-2", InstanceType: "g5.xlarge", State: StateRunning, CreatedAt: time.Now(), Metadata: map[string]string{"marketType": "on-demand"}})
	}
	tracker.UpdateLiveFleet(instances)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	want := `cloudvm_spend_usd_per_hour{accelerator="gpu",capacity_type="on-demand",component="transfer",gpu_model="A10G 24GB",instance_type="g5.xlarge",provider="aws",region="eu-south-2"} 1.5`
	if !strings.Contains(body, want) {
		t.Errorf("metrics lack %s:\n%s", want, body)
	}
	if !strings.Contains(body, `component="compute"`) {
		t.Error("compute spend lost its component label")
	}
}
