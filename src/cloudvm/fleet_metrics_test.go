// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestClassifyErrors(t *testing.T) {
	// AWS error classification
	if ClassifyAWSError(nil) != "ok" {
		t.Errorf("expected ok for nil err")
	}
	if ClassifyAWSError(fmt.Errorf("InsufficientInstanceCapacity: We do not have capacity")) != "no_capacity" {
		t.Errorf("expected no_capacity")
	}
	if ClassifyAWSError(fmt.Errorf("MaxSpotInstanceCountExceeded")) != "quota" {
		t.Errorf("expected quota")
	}
	if ClassifyAWSError(fmt.Errorf("VcpuLimitExceeded")) != "quota" {
		t.Errorf("expected quota")
	}
	if ClassifyAWSError(fmt.Errorf("unexpected network error")) != "error" {
		t.Errorf("expected error")
	}

	// GCP error classification
	if ClassifyGCPError(nil) != "ok" {
		t.Errorf("expected ok for nil err")
	}
	if ClassifyGCPError(fmt.Errorf("ZONE_RESOURCE_POOL_EXHAUSTED")) != "no_capacity" {
		t.Errorf("expected no_capacity")
	}
	if ClassifyGCPError(fmt.Errorf("QUOTA_EXCEEDED: Quota limit hit")) != "quota" {
		t.Errorf("expected quota")
	}
}

func TestFleetTracker_LiveFleetAndInterruption(t *testing.T) {
	metrics := NewObservabilityMetrics()
	cat := NewHardwareCatalog()
	tracker := NewFleetTracker(metrics, cat)

	// Simulate 2 running instances
	now := time.Now().Add(-10 * time.Minute)
	instances := []*Instance{
		{
			ID:           "i-spot-1",
			Provider:     ProviderAWS,
			Region:       "us-west-2",
			InstanceType: "g5.xlarge",
			State:        StateRunning,
			CreatedAt:    now,
			Metadata: map[string]string{
				"marketType":       "spot",
				"availabilityZone": "us-west-2a",
			},
		},
		{
			ID:           "i-spot-2",
			Provider:     ProviderAWS,
			Region:       "us-west-2",
			InstanceType: "g5.xlarge",
			State:        StateRunning,
			CreatedAt:    now,
			Metadata: map[string]string{
				"marketType":       "spot",
				"availabilityZone": "us-west-2a",
			},
		},
	}

	tracker.UpdateLiveFleet(instances)

	activePods := map[string]string{
		"pod-1": "i-spot-1",
		"pod-2": "i-spot-1",
		"pod-3": "i-spot-2",
	}

	// Advance time and update again with active pods
	time.Sleep(10 * time.Millisecond)
	tracker.UpdateLiveFleet(instances, activePods)

	// Record launch attempt & duration
	tracker.RecordLaunchAttempt("aws", "us-west-2", "g5.xlarge", "spot", "ok")
	tracker.RecordLaunchDuration("aws", "us-west-2", "g5.xlarge", "spot", 45*time.Second)

	// Record interruption
	tracker.RecordInterruption("aws", "us-west-2", "g5.xlarge", "spot")

	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	if !strings.Contains(body, "cloudvm_instances_running") || !strings.Contains(body, "A10G 24GB") {
		t.Errorf("expected 2 running instances in metrics: %s", body)
	}
	if !strings.Contains(body, "cloudvm_gpus_running") {
		t.Errorf("expected 2 running GPUs in metrics: %s", body)
	}
	if !strings.Contains(body, "cloudvm_interruptions_total") {
		t.Errorf("expected interruption count in metrics: %s", body)
	}
	if !strings.Contains(body, "cloudvm_launch_attempts_total") {
		t.Errorf("expected launch attempts in metrics: %s", body)
	}
	if !strings.Contains(body, "cloudvm_pods_running") {
		t.Errorf("expected cloudvm_pods_running in metrics: %s", body)
	}
	if !strings.Contains(body, "cloudvm_spend_ondemand_baseline_usd_per_hour") {
		t.Errorf("expected cloudvm_spend_ondemand_baseline_usd_per_hour in metrics: %s", body)
	}
	if !strings.Contains(body, `accelerator="gpu"`) {
		t.Errorf("expected accelerator=gpu in metrics: %s", body)
	}

	// Test idle fleet emits zero gauges
	tracker.UpdateLiveFleet([]*Instance{})
	recIdle := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(recIdle, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	bodyIdle := recIdle.Body.String()
	if !strings.Contains(bodyIdle, "cloudvm_instances_running") {
		t.Errorf("expected cloudvm_instances_running in idle metrics: %s", bodyIdle)
	}
	if !strings.Contains(bodyIdle, `accelerator="none"`) {
		t.Errorf("expected accelerator=none in idle metrics: %s", bodyIdle)
	}
	if !strings.Contains(bodyIdle, "cloudvm_pods_running") {
		t.Errorf("expected cloudvm_pods_running in idle metrics: %s", bodyIdle)
	}
}

func TestFleetTracker_NetworkMetrics(t *testing.T) {
	metrics := NewObservabilityMetrics()
	cat := NewHardwareCatalog()
	tracker := NewFleetTracker(metrics, cat)

	// 1. Initial cumulative reading: 1000 RX, 2000 TX
	tracker.RecordNetworkTraffic("aws", "us-west-2", "g5.xlarge", "i-net-1", 1000, 2000)

	// 2. Second cumulative reading: 1500 RX (+500), 2800 TX (+800)
	tracker.RecordNetworkTraffic("aws", "us-west-2", "g5.xlarge", "i-net-1", 1500, 2800)

	// 3. Counter reset (e.g. reboot): 300 RX (+300), 400 TX (+400)
	tracker.RecordNetworkTraffic("aws", "us-west-2", "g5.xlarge", "i-net-1", 300, 400)

	// 4. CollectFleetTelemetry delegation
	tracker.CollectFleetTelemetry("aws", "us-west-2", "g5.xlarge", "i-net-2", 5000, 10000)

	// 5. Direct delta recording
	tracker.RecordNetworkBytes("gcp", "us-central1", "a2-highgpu-1g", 1024, 2048)

	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	if !strings.Contains(body, "cloudvm_network_receive_bytes_total") {
		t.Fatalf("expected cloudvm_network_receive_bytes_total in metrics output")
	}
	if !strings.Contains(body, "cloudvm_network_transmit_bytes_total") {
		t.Fatalf("expected cloudvm_network_transmit_bytes_total in metrics output")
	}
	// For i-net-1: initial=1000 + delta=500 + reset=300 = 1800 RX; initial=2000 + delta=800 + reset=400 = 3200 TX
	// Total for aws us-west-2 g5.xlarge: 1800 + 5000 (i-net-2) = 6800 RX, 3200 + 10000 = 13200 TX
	if !strings.Contains(body, `cloudvm_network_receive_bytes_total{accelerator="gpu",instance_type="g5.xlarge",provider="aws",region="us-west-2"} 6800`) {
		t.Errorf("expected 6800 RX bytes for aws g5.xlarge, got metrics:\n%s", body)
	}
	if !strings.Contains(body, `cloudvm_network_transmit_bytes_total{accelerator="gpu",instance_type="g5.xlarge",provider="aws",region="us-west-2"} 13200`) {
		t.Errorf("expected 13200 TX bytes for aws g5.xlarge, got metrics:\n%s", body)
	}
	// For GCP: 1024 RX, 2048 TX
	if !strings.Contains(body, `cloudvm_network_receive_bytes_total{accelerator="gpu",instance_type="a2-highgpu-1g",provider="gcp",region="us-central1"} 1024`) {
		t.Errorf("expected 1024 RX bytes for gcp a2-highgpu-1g, got metrics:\n%s", body)
	}
	if !strings.Contains(body, `cloudvm_network_transmit_bytes_total{accelerator="gpu",instance_type="a2-highgpu-1g",provider="gcp",region="us-central1"} 2048`) {
		t.Errorf("expected 2048 TX bytes for gcp a2-highgpu-1g, got metrics:\n%s", body)
	}
}

func TestFleetTracker_SpotSpendUsesSpotPrice(t *testing.T) {
	metrics := NewObservabilityMetrics()
	tracker := NewFleetTracker(metrics, NewHardwareCatalog())
	tracker.SetSpotPrices(func(region, zone, instanceType string) (float64, bool) {
		if region == "us-west-2" && zone == "us-west-2a" && instanceType == "g5.xlarge" {
			return 0.4024, true
		}
		return 0, false
	})

	tracker.UpdateLiveFleet([]*Instance{{
		ID: "i-spot", Provider: ProviderAWS, Region: "us-west-2", InstanceType: "g5.xlarge", State: StateRunning, CreatedAt: time.Now(),
		Metadata: map[string]string{"marketType": "spot", "availabilityZone": "us-west-2a"},
	}})

	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()
	labels := `{accelerator="gpu",capacity_type="spot",gpu_model="A10G 24GB",instance_type="g5.xlarge",provider="aws",region="us-west-2"}`
	if !strings.Contains(body, "cloudvm_spend_usd_per_hour"+labels+" 0.4024") {
		t.Errorf("spot spend is not the zone's spot price:\n%s", body)
	}
	// The baseline stays at the g5.xlarge on-demand catalog rate.
	if !strings.Contains(body, "cloudvm_spend_ondemand_baseline_usd_per_hour"+labels+" 1.006") {
		t.Errorf("on-demand baseline is not the on-demand price:\n%s", body)
	}
}
