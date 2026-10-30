// Controller configuration: the GPU class mapping, cross-region data cost, NodePool scope and catalog cache settings.

package cloudprovider

import (
	"fmt"
	"os"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/yaml"
)

// Config is the deployment-provided configuration of the CloudVM provider, read from a YAML file.
type Config struct {
	// GPUClasses maps GPU models to the gpu-class label value the cluster's GPU classes use.
	GPUClasses []GPUClass `json:"gpuClasses,omitempty"`
	// DataCost adds a per-hour cost to offerings outside the home region, so cheapest-offering selection accounts
	// for cross-region data transfer.
	DataCost DataCost `json:"dataCost,omitzero"`
	// NodePoolSelector restricts the provider to NodePools whose labels match; empty matches every CloudVM NodePool.
	NodePoolSelector map[string]string `json:"nodePoolSelector,omitempty"`
	// CatalogTTL is how long a fetched catalog is reused before the plugin is asked again.
	CatalogTTL Duration `json:"catalogTTL,omitzero"`
	// CatalogMaxStaleness is how long a cached catalog keeps serving after the plugin stops answering.
	CatalogMaxStaleness Duration `json:"catalogMaxStaleness,omitzero"`
	// UnavailableOfferingTTL is how long an offering stays unavailable after the plugin reports no capacity for it.
	UnavailableOfferingTTL Duration `json:"unavailableOfferingTTL,omitzero"`
	// MaxPods is the pods capacity of every node.
	MaxPods int64 `json:"maxPods,omitempty"`
	// EphemeralStorage is the ephemeral-storage every instance type advertises for planning; a launched NodeClaim
	// reports the size of the disk its VM keeps containers on, which the plugin reads.
	EphemeralStorage string `json:"ephemeralStorage,omitempty"`
}

// GPUClass assigns a gpu-class label value to GPU models, optionally only at one GPU memory size.
type GPUClass struct {
	// Class is the gpu-class label value, such as a100-80gb.
	Class string `json:"class"`
	// Models are catalog GPU model names, such as "a100", compared case-insensitively.
	Models []string `json:"models"`
	// MemoryGB restricts the class to GPUs with this much memory each; 0 matches any size.
	MemoryGB int `json:"memoryGB,omitempty"`
}

// DataCost holds the per-hour cost added to offerings outside the home region.
type DataCost struct {
	// HomeRegion is the region the cluster runs in; its offerings carry no adder.
	HomeRegion string `json:"homeRegion,omitempty"`
	// DefaultUSDPerHour is the adder for remote regions missing from PerRegionUSDPerHour.
	DefaultUSDPerHour float64 `json:"defaultUSDPerHour,omitempty"`
	// PerRegionUSDPerHour is the adder for each remote region.
	PerRegionUSDPerHour map[string]float64 `json:"perRegionUSDPerHour,omitempty"`
}

// Duration is a time.Duration that reads from a YAML string such as "60s".
type Duration struct{ time.Duration }

// UnmarshalJSON parses a Go duration string.
func (d *Duration) UnmarshalJSON(b []byte) error {
	s := strings.Trim(string(b), `"`)
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("parsing duration %q: %w", s, err)
	}
	d.Duration = v
	return nil
}

// Defaults applied to unset Config fields.
const (
	DefaultCatalogTTL             = time.Minute
	DefaultCatalogMaxStaleness    = 15 * time.Minute
	DefaultUnavailableOfferingTTL = 3 * time.Minute
	DefaultMaxPods                = 110
	DefaultEphemeralStorage       = "100Gi"
)

// LoadConfig reads a Config from a YAML file; an empty path returns the defaults.
func LoadConfig(path string) (*Config, error) {
	cfg := &Config{}
	if path != "" {
		raw, err := os.ReadFile(path) //nolint:gosec // the path is the operator-supplied config flag
		if err != nil {
			return nil, fmt.Errorf("reading config: %w", err)
		}
		if err := yaml.UnmarshalStrict(raw, cfg); err != nil {
			return nil, fmt.Errorf("parsing config %s: %w", path, err)
		}
	}
	if err := cfg.complete(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func (c *Config) complete() error {
	if c.CatalogTTL.Duration == 0 {
		c.CatalogTTL.Duration = DefaultCatalogTTL
	}
	if c.CatalogMaxStaleness.Duration == 0 {
		c.CatalogMaxStaleness.Duration = DefaultCatalogMaxStaleness
	}
	if c.UnavailableOfferingTTL.Duration == 0 {
		c.UnavailableOfferingTTL.Duration = DefaultUnavailableOfferingTTL
	}
	if c.MaxPods == 0 {
		c.MaxPods = DefaultMaxPods
	}
	if c.EphemeralStorage == "" {
		c.EphemeralStorage = DefaultEphemeralStorage
	}
	if _, err := resource.ParseQuantity(c.EphemeralStorage); err != nil {
		return fmt.Errorf("ephemeralStorage: %w", err)
	}
	if _, err := labels.ValidatedSelectorFromSet(c.NodePoolSelector); err != nil {
		return fmt.Errorf("nodePoolSelector: %w", err)
	}
	seen := map[string]string{}
	for _, gc := range c.GPUClasses {
		if gc.Class == "" {
			return fmt.Errorf("gpuClasses: class is required")
		}
		if gc.MemoryGB < 0 {
			return fmt.Errorf("gpuClasses[%s]: memoryGB must not be negative", gc.Class)
		}
		for _, m := range gc.Models {
			key := fmt.Sprintf("%s/%d", normalizeModel(m), gc.MemoryGB)
			if prev, ok := seen[key]; ok && prev != gc.Class {
				return fmt.Errorf("gpuClasses: model %q at %d GB maps to both %q and %q", m, gc.MemoryGB, prev, gc.Class)
			}
			seen[key] = gc.Class
		}
	}
	for region, v := range c.DataCost.PerRegionUSDPerHour {
		if v < 0 {
			return fmt.Errorf("dataCost.perRegionUSDPerHour[%s]: must not be negative", region)
		}
	}
	if c.DataCost.DefaultUSDPerHour < 0 {
		return fmt.Errorf("dataCost.defaultUSDPerHour: must not be negative")
	}
	return nil
}

// GPUClassFor returns the gpu-class of a GPU model with memoryGB per GPU, or "" when the mapping has none. A class
// pinned to the GPU's memory size wins over one that matches any size.
func (c *Config) GPUClassFor(model string, memoryGB int) string {
	key := normalizeModel(model)
	anySize := ""
	for _, gc := range c.GPUClasses {
		for _, m := range gc.Models {
			if normalizeModel(m) != key {
				continue
			}
			if gc.MemoryGB == memoryGB {
				return gc.Class
			}
			if gc.MemoryGB == 0 && anySize == "" {
				anySize = gc.Class
			}
		}
	}
	return anySize
}

// DataCostFor returns the per-hour adder for an offering in a region.
func (c *Config) DataCostFor(region string) float64 {
	if c.DataCost.HomeRegion == "" || region == c.DataCost.HomeRegion {
		return 0
	}
	if v, ok := c.DataCost.PerRegionUSDPerHour[region]; ok {
		return v
	}
	return c.DataCost.DefaultUSDPerHour
}

func normalizeModel(m string) string {
	return strings.ToLower(strings.Join(strings.Fields(m), " "))
}
