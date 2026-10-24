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
	metrics.SetQuota("aws", "us-west-2", "gpu", "g_vt_spot", 64, 8)

	// 4. Record fill quotes
	metrics.SetFillQuote(24, 64, "1", "spot", 0.4500, nil, "")
	metrics.SetFillGPUAllocation(24, 64, "1", "spot", "aws", "us-west-2", "A10G 24GB", 64)
	metrics.SetFillQuote(80, 1000, "1", "spot", 0.0, nil, "capacity")

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
