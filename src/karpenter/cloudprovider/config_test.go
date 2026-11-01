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
	if cfg.CatalogTTL.Duration != DefaultCatalogTTL || cfg.MaxPods != DefaultMaxPods || cfg.EphemeralStorage != DefaultEphemeralStorage ||
		cfg.OfferingSpread != (OfferingSpread{PriceTolerance: DefaultSpreadPriceTolerance, MaxOfferings: DefaultSpreadMaxOfferings}) ||
		cfg.LaunchFallback.MaxAttempts != DefaultLaunchMaxAttempts || cfg.LaunchFallback.Timeout.Duration != DefaultLaunchFallbackTimeout ||
		cfg.QuotaExceededTTL.Duration != DefaultQuotaExceededTTL {
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
transferCost:
  default:
    imagePullGB: 40
    expectedLifetime: 6h
  nodePools:
    batch:
      homeTransmitGBPerHour: 2.5
  minMeasuredVMs: 5
priceCap:
  default:
    maxPricePerGPUHour: 2.50
  nodePools:
    gpu-capped:
      maxPricePerGPUHour: 1.50
      maxPriceAboveCheapestPercent: 20
dataCost:
  homeRegion: us-east-1
  defaultUSDPerHour: 0.02
catalogTTL: 30s
offeringSpread:
  priceTolerance: 0.15
  maxOfferings: 2
nodePoolSelector:
  cloudvm-controller: primary
`))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.CatalogTTL.Duration != 30*time.Second {
		t.Errorf("catalogTTL = %s", cfg.CatalogTTL.Duration)
	}
	if cfg.OfferingSpread != (OfferingSpread{PriceTolerance: 0.15, MaxOfferings: 2}) {
		t.Errorf("offeringSpread = %+v", cfg.OfferingSpread)
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
	if got := cfg.TransferProfileFor("gpu"); got.ImagePullGB != 40 || got.ExpectedLifetime.Duration != 6*time.Hour {
		t.Errorf("default transfer profile = %+v", got)
	}
	if got := cfg.TransferProfileFor("batch"); got.HomeTransmitGBPerHour != 2.5 || got.ImagePullGB != 0 {
		t.Errorf("batch transfer profile = %+v, want its own entry in place of the default", got)
	}
	if cfg.TransferCost.MinMeasuredVMs != 5 {
		t.Errorf("minMeasuredVMs = %d", cfg.TransferCost.MinMeasuredVMs)
	}
	defCap := cfg.PriceCapProfileFor("unknown")
	if defCap.MaxPricePerGPUHour == nil || *defCap.MaxPricePerGPUHour != 2.50 || defCap.MaxPriceAboveCheapestPercent != nil {
		t.Errorf("default price cap = %+v", defCap)
	}
	gpuCap := cfg.PriceCapProfileFor("gpu-capped")
	if gpuCap.MaxPricePerGPUHour == nil || *gpuCap.MaxPricePerGPUHour != 1.50 || gpuCap.MaxPriceAboveCheapestPercent == nil || *gpuCap.MaxPriceAboveCheapestPercent != 20 {
		t.Errorf("gpu-capped price cap = %+v", gpuCap)
	}
	// The flat dataCost adder is ignored, but an old config that sets it still loads.
	if !cfg.DeprecatedDataCost() {
		t.Error("the deprecated dataCost is not reported")
	}
}

func TestLoadConfigRejectsInvalid(t *testing.T) {
	for name, body := range map[string]string{
		"conflicting model":    "gpuClasses: [{class: a, models: [L4]}, {class: b, models: [l4]}]",
		"negative memory":      "gpuClasses: [{class: a, models: [l4], memoryGB: -1}]",
		"negative pulls":       "transferCost: {default: {imagePullGB: -1}}",
		"negative rate":        "transferCost: {nodePools: {a: {homeReceiveGBPerHour: -1}}}",
		"negative measured":    "transferCost: {minMeasuredVMs: -1}",
		"negative price cap":   "priceCap: {default: {maxPricePerGPUHour: -1}}",
		"negative percent cap": "priceCap: {nodePools: {a: {maxPriceAboveCheapestPercent: -5}}}",
		"unknown field":        "gpuClass: []",
		"bad duration":         "catalogTTL: soon",
		"bad selector":         `nodePoolSelector: {"bad key!": x}`,
		"negative spread":      "offeringSpread: {priceTolerance: -0.1}",
		"negative offers":      "offeringSpread: {maxOfferings: -1}",
		"negative attempts":    "launchFallback: {maxAttempts: -1}",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadConfig(writeConfig(t, body)); err == nil {
				t.Fatal("expected an error")
			}
		})
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
