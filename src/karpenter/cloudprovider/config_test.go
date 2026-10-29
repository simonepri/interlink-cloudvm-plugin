// Unit tests of configuration loading, GPU class mapping and data cost lookup.

package cloudprovider

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"sigs.k8s.io/yaml"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadConfigDefaults(t *testing.T) {
	cfg, err := LoadConfig("")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CatalogTTL.Duration != DefaultCatalogTTL || cfg.MaxPods != DefaultMaxPods || cfg.EphemeralStorage != DefaultEphemeralStorage {
		t.Fatalf("defaults not applied: %+v", cfg)
	}
}

func TestLoadConfigFile(t *testing.T) {
	cfg, err := LoadConfig(writeConfig(t, `
gpuClasses:
  - class: a100-80gb
    models: [a100]
    memoryGB: 80
  - class: a100-40gb
    models: [a100]
    memoryGB: 40
  - class: a10g
    models: [a10g, "A10G 24GB"]
dataCost:
  homeRegion: us-east-1
  defaultUSDPerHour: 0.02
  perRegionUSDPerHour:
    ap-south-1: 0.15
catalogTTL: 30s
nodePoolSelector:
  cloudvm-controller: primary
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CatalogTTL.Duration != 30*time.Second {
		t.Errorf("catalogTTL = %s", cfg.CatalogTTL.Duration)
	}
	for _, tc := range []struct {
		model    string
		memoryGB int
		want     string
	}{
		{"a100", 80, "a100-80gb"},
		{"A100", 40, "a100-40gb"},
		{"a100", 16, ""},
		{"A10G", 24, "a10g"},
		{" A10G  24GB ", 0, "a10g"},
		{"h100", 80, ""},
	} {
		if got := cfg.GPUClassFor(tc.model, tc.memoryGB); got != tc.want {
			t.Errorf("GPUClassFor(%q, %d) = %q, want %q", tc.model, tc.memoryGB, got, tc.want)
		}
	}
	for region, want := range map[string]float64{"us-east-1": 0, "ap-south-1": 0.15, "eu-west-1": 0.02} {
		if got := cfg.DataCostFor(region); got != want {
			t.Errorf("DataCostFor(%s) = %v, want %v", region, got, want)
		}
	}
}

func TestLoadConfigRejectsInvalid(t *testing.T) {
	for name, body := range map[string]string{
		"conflicting model": "gpuClasses: [{class: a, models: [L4]}, {class: b, models: [l4]}]",
		"negative memory":   "gpuClasses: [{class: a, models: [l4], memoryGB: -1}]",
		"negative cost":     "dataCost: {homeRegion: x, perRegionUSDPerHour: {y: -1}}",
		"unknown field":     "gpuClass: []",
		"bad duration":      "catalogTTL: soon",
		"bad selector":      `nodePoolSelector: {"bad key!": x}`,
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadConfig(writeConfig(t, body)); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestDataCostWithoutHomeRegionIsZero(t *testing.T) {
	cfg := &Config{DataCost: DataCost{DefaultUSDPerHour: 1}}
	if got := cfg.DataCostFor("anywhere"); got != 0 {
		t.Fatalf("got %v, want 0 when no home region is set", got)
	}
}

func TestGPUName(t *testing.T) {
	for model, want := range map[string]string{"A100 80GB": "a100", "A10G 24GB": "a10g", "RTX PRO 6000 96GB": "rtx-pro-6000", "L4": "l4"} {
		if got := gpuName(model); got != want {
			t.Errorf("gpuName(%q) = %q, want %q", model, got, want)
		}
	}
}

func TestChartDefaultConfigLoads(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "..", "charts", "cloudvm-karpenter", "values.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	var values struct {
		Config map[string]any `json:"config"`
	}
	if err := yaml.Unmarshal(raw, &values); err != nil {
		t.Fatal(err)
	}
	rendered, err := yaml.Marshal(values.Config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := LoadConfig(writeConfig(t, string(rendered))); err != nil {
		t.Fatalf("chart default config does not load: %v", err)
	}
}
