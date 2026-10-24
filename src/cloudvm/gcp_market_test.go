// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGCPMarket_PricesAndAdvice(t *testing.T) {
	ctx := context.Background()

	// 1. Test baseline prices
	onDemand, spot := GetGCPPrice("g2-standard-4")
	if onDemand != 0.7024 || spot != 0.2308 {
		t.Errorf("expected g2-standard-4 prices (0.7024, 0.2308), got (%f, %f)", onDemand, spot)
	}

	onDemandA3, spotA3 := GetGCPPrice("a3-highgpu-8g")
	if onDemandA3 != 31.2184 || spotA3 != 9.3655 {
		t.Errorf("expected a3-highgpu-8g prices (31.2184, 9.3655), got (%f, %f)", onDemandA3, spotA3)
	}

	// 2. Test advice client
	adviceClient := &DefaultGCPAdviceClient{}
	resp, err := adviceClient.QueryAdviceCapacity(ctx, "us-central1", "g2-standard-4", 8)
	if err != nil || resp == nil {
		t.Fatalf("unexpected advice error: %v", err)
	}
	if resp.ObtainabilityScore <= 0 || resp.EstimatedUptimeSec <= 0 {
		t.Errorf("expected positive score and uptime, got %+v", resp)
	}

	// 3. Test collector metrics export
	metrics := NewObservabilityMetrics()
	collector := NewGCPMarketCollector([]string{"us-central1"}, adviceClient)
	collector.Collect(ctx, metrics)

	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	if !strings.Contains(body, "provider=\"gcp\"") {
		t.Errorf("metrics missing gcp provider labels: %s", body)
	}
	if !strings.Contains(body, "a2-highgpu-1g") || !strings.Contains(body, "g2-standard-4") {
		t.Errorf("metrics missing gcp machine types: %s", body)
	}
	if !strings.Contains(body, "NVIDIA_L4_GPUS") {
		t.Errorf("metrics missing gcp quota metrics: %s", body)
	}
}
