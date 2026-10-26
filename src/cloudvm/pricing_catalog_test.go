// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPricingCatalog_Defaults(t *testing.T) {
	catalog := NewPricingCatalog()

	rate, ok := catalog.GetAWSRate("g5.xlarge")
	if !ok || rate != 1.006 {
		t.Fatalf("expected g5.xlarge rate 1.006, got %f (ok=%v)", rate, ok)
	}

	onDemand, spot, ok := catalog.GetGCPRate("g2-standard-4")
	if !ok || onDemand != 0.7024 || spot != 0.2308 {
		t.Fatalf("expected g2-standard-4 0.7024/0.2308, got %f/%f (ok=%v)", onDemand, spot, ok)
	}
}

func TestPricingCatalog_FileOverride(t *testing.T) {
	tmpDir := t.TempDir()
	customPath := filepath.Join(tmpDir, "custom_prices.json")

	customJSON := `{
  "aws": {
    "custom.shape": 2.50
  },
  "gcp": {
    "custom-gcp": {"onDemandUSD": 3.0, "spotUSD": 0.9}
  }
}`
	if err := os.WriteFile(customPath, []byte(customJSON), 0600); err != nil {
		t.Fatalf("failed to write custom json: %v", err)
	}

	catalog := &PricingCatalog{
		awsRates: make(map[string]float64),
		gcpRates: make(map[string]GCPPriceBaseline),
	}
	if err := catalog.LoadFile(customPath); err != nil {
		t.Fatalf("failed to load custom file: %v", err)
	}

	rate, ok := catalog.GetAWSRate("custom.shape")
	if !ok || rate != 2.50 {
		t.Fatalf("expected custom.shape rate 2.50, got %f", rate)
	}

	od, sp, ok := catalog.GetGCPRate("custom-gcp")
	if !ok || od != 3.0 || sp != 0.9 {
		t.Fatalf("expected custom-gcp 3.0/0.9, got %f/%f", od, sp)
	}
}

func TestLadderSolver_CustomConfig(t *testing.T) {
	t.Setenv("LADDER_SIZES", "2,16,64,512")
	t.Setenv("LADDER_MEMORY_TIERS", "16,80")

	LoadLadderConfigFromEnv()

	if len(LadderSizes) != 4 || LadderSizes[0] != 2 || LadderSizes[3] != 512 {
		t.Fatalf("unexpected LadderSizes from env: %v", LadderSizes)
	}
	if len(LadderMemoryTiers) != 2 || LadderMemoryTiers[0] != 16 {
		t.Fatalf("unexpected LadderMemoryTiers from env: %v", LadderMemoryTiers)
	}

	// Reset to defaults
	LadderSizes = DefaultLadderSizes
	LadderMemoryTiers = DefaultLadderMemoryTiers
}
