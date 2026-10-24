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
				"marketType":     "spot",
				"fulfilledPrice": "0.4024",
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
				"marketType":     "spot",
				"fulfilledPrice": "0.4024",
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
