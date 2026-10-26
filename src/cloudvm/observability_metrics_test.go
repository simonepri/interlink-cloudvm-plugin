// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestObservabilityMetrics_Exposition(t *testing.T) {
	metrics := NewObservabilityMetrics()

	// 1. Record market prices
	metrics.SetGPUPrice("aws", "us-west-2", "us-west-2a", "gpu", "A10G 24GB", 24, 1, "spot", "g5.xlarge", 0.4024)
	metrics.SetGPUPrice("aws", "us-west-2", "us-west-2a", "gpu", "A10G 24GB", 24, 1, "on-demand", "g5.xlarge", 1.0060)
	metrics.SetVCPUPrice("aws", "us-west-2", "us-west-2a", "spot", "c6i.xlarge", 0.0520)

	// 2. Record availability and signals
	metrics.SetAvailabilityScore("aws", "us-west-2", "us-west-2a", "gpu", "A10G 24GB", "g5.xlarge", "aws_placement_score", 1, 0.9)
	metrics.SetExpectedUptime("aws", "us-west-2", "gpu", "g5.xlarge", "A10G 24GB", "aws_spot_advisor", 50400)
	metrics.SetSignalAge("aws", "us-west-2", "aws_spot_advisor", 3600)

	// 3. Record quotas
	metrics.SetQuota("aws", "us-west-2", "gpu", "g_vt_spot", 64)
	metrics.SetQuotaUsed("aws", "us-west-2", "gpu", "g_vt_spot", 8)

	// 4. Record fill quotes
	metrics.SetFillPlan(FillPlan{MinGPUMemoryGB: 24, TargetGPUs: 64, PlannedGPUs: 64, ExpectedGPUs: 57.6, PricePerGPUHour: 0.45,
		Allocations: []FillAllocation{{Provider: "aws", Region: "us-west-2", GPUModel: "A10G 24GB", CapacityType: "spot", GPUs: 64, ExpectedGPUs: 57.6}}})
	metrics.SetFillPlan(FillPlan{MinGPUMemoryGB: 80, TargetGPUs: 1000, LimitedReason: "capacity"})

	// 5. Record fleet metrics
	metrics.SetFleetGauges("aws", "us-west-2", "gpu", "g5.xlarge", "A10G 24GB", "spot", 4, 16, 64, 4, 96, 1.6096)
	metrics.RecordLaunchAttempt("aws", "us-west-2", "gpu", "g5.xlarge", "A10G 24GB", "spot", "ok")
	metrics.RecordLaunchAttempt("aws", "us-west-2", "gpu", "g5.xlarge", "A10G 24GB", "spot", "no_capacity")
	metrics.RecordLaunchDuration("aws", "us-west-2", "gpu", "g5.xlarge", "A10G 24GB", "spot", 35.5)
	metrics.RecordInterruption("aws", "us-west-2", "gpu", "g5.xlarge", "A10G 24GB", "spot")
	metrics.AddInstanceSeconds("aws", "us-west-2", "gpu", "g5.xlarge", "A10G 24GB", "spot", 3600)
	metrics.AddNetworkReceiveBytes("aws", "us-west-2", "gpu", "g5.xlarge", 1024000)
	metrics.AddNetworkTransmitBytes("aws", "us-west-2", "gpu", "g5.xlarge", 2048000)
	metrics.AddNetworkBytes("aws", "us-west-2", "gpu", "g5.xlarge", 512, 1024)

	// 6. Record legacy metrics
	metrics.SetLegacySpotMetrics("us-west-2", "us-west-2a", "g5.xlarge", 0.4024, 1.0060, 60.0)
	metrics.SetLegacyPlacementScore("us-west-2", "us-west-2a", "g5.xlarge", 9)

	// Test HTTP handler
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected HTTP 200, got %d", rec.Code)
	}

	body := rec.Body.String()
	expectedMetrics := []string{
		"cloudvm_gpu_price_usd_per_hour",
		"cloudvm_vcpu_price_usd_per_hour",
		"cloudvm_instance_price_usd_per_hour",
		"cloudvm_availability_score",
		"cloudvm_expected_uptime_seconds",
		"cloudvm_signal_age_seconds",
		"cloudvm_quota_gpus",
		"cloudvm_quota_used_gpus",
		"cloudvm_fill_price_usd_per_gpu_hour",
		"cloudvm_fill_gpus",
		"cloudvm_fill_limited",
		"cloudvm_instances_running",
		"cloudvm_vcpus_running",
		"cloudvm_memory_gib_running",
		"cloudvm_gpus_running",
		"cloudvm_vram_gb_running",
		"cloudvm_spend_usd_per_hour",
		"cloudvm_launch_attempts_total",
		"cloudvm_launch_duration_seconds",
		"cloudvm_interruptions_total",
		"cloudvm_instance_seconds_total",
		"cloudvm_network_receive_bytes_total",
		"cloudvm_network_transmit_bytes_total",
		"cloudvm_spot_price_usd_per_hour",
		"cloudvm_ondemand_price_usd_per_hour",
		"cloudvm_spot_savings_percentage",
		"cloudvm_spot_placement_score",
	}

	for _, m := range expectedMetrics {
		if !strings.Contains(body, m) {
			t.Errorf("metrics output missing expected metric %s", m)
		}
	}
}

func TestObservabilityMetrics_SweepDropsSeriesAPassNoLongerSets(t *testing.T) {
	metrics := NewObservabilityMetrics()

	// Pass 1: a quote is limited by capacity and draws GPUs from two regions.
	metrics.SetFillPlan(FillPlan{MinGPUMemoryGB: 80, TargetGPUs: 64, PlannedGPUs: 64, PricePerGPUHour: 2.5, LimitedReason: "capacity", Allocations: []FillAllocation{
		{Provider: "aws", Region: "us-east-1", GPUModel: "H100 80GB", CapacityType: "spot", GPUs: 32},
		{Provider: "aws", Region: "us-west-2", GPUModel: "H100 80GB", CapacityType: "spot", GPUs: 32},
	}})
	metrics.SweepMarketGauges()

	// Pass 2: the same quote fills from one region without limits.
	metrics.SetFillPlan(FillPlan{MinGPUMemoryGB: 80, TargetGPUs: 64, PlannedGPUs: 64, PricePerGPUHour: 2.4, Allocations: []FillAllocation{
		{Provider: "aws", Region: "us-east-1", GPUModel: "H100 80GB", CapacityType: "spot", GPUs: 64},
	}})
	metrics.SweepMarketGauges()

	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	if strings.Contains(body, `reason="capacity"`) {
		t.Errorf("stale fill_limited reason survived the sweep:\n%s", body)
	}
	if strings.Contains(body, `region="us-west-2"`) {
		t.Errorf("stale fill allocation survived the sweep:\n%s", body)
	}
	if !strings.Contains(body, `cloudvm_fill_gpus{capacity_type="spot",gpu_model="H100 80GB",gpus="64",min_gpu_memory_gb="80",provider="aws",region="us-east-1"} 64`) {
		t.Errorf("current fill allocation missing:\n%s", body)
	}
}
