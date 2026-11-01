// Controller configuration: GPU class mapping, transfer cost, offering spread, launch fallback, NodePool scope and caching.

package cloudprovider

import (
	"encoding/json"
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
	// TransferCost adds each offering's expected data transfer with the home region to its price, so Karpenter ranks
	// and consolidates offerings by what they cost with their transfer.
	TransferCost TransferCost `json:"transferCost,omitzero"`
	// PriceCap bounds the offering prices a NodePool considers, dropping offerings above the cap before Karpenter
	// sees them.
	PriceCap PriceCap `json:"priceCap,omitzero"`
	// DataCost is the flat per-hour adder TransferCost replaced; it is accepted so older configs load, and ignored.
	DataCost json.RawMessage `json:"dataCost,omitempty"`
	// NodePoolSelector restricts the provider to NodePools whose labels match; empty matches every CloudVM NodePool.
	NodePoolSelector map[string]string `json:"nodePoolSelector,omitempty"`
	// CatalogTTL is how long a fetched catalog is reused before the plugin is asked again.
	CatalogTTL Duration `json:"catalogTTL,omitzero"`
	// CatalogMaxStaleness is how long a cached catalog keeps serving after the plugin stops answering.
	CatalogMaxStaleness Duration `json:"catalogMaxStaleness,omitzero"`
	// UnavailableOfferingTTL is how long an offering stays unavailable after the plugin reports no capacity for it.
	UnavailableOfferingTTL Duration `json:"unavailableOfferingTTL,omitzero"`
	// QuotaExceededTTL is how long an offering stays unavailable after a cloud quota refused it; a quota frees up
	// only as the account's usage drops, so it is longer than UnavailableOfferingTTL.
	QuotaExceededTTL Duration `json:"quotaExceededTTL,omitzero"`
	// OfferingSpread spreads the claims of one scale-up across the cheapest compatible offerings.
	OfferingSpread OfferingSpread `json:"offeringSpread,omitzero"`
	// LaunchFallback bounds how many offerings one Create call tries after capacity refusals.
	LaunchFallback LaunchFallback `json:"launchFallback,omitzero"`
	// MaxPods is the pods capacity of every node.
	MaxPods int64 `json:"maxPods,omitempty"`
	// EphemeralStorage is the ephemeral-storage an instance type advertises when the plugin does not report the size
	// of the disk its VMs keep containers on.
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

// OfferingSpread bounds the offerings a claim may launch with besides the cheapest compatible one. Each claim picks
// one of them by a hash of its name, so a batch of claims probes several offerings for capacity at once; the price of
// that is that a claim may cost up to PriceTolerance more than the cheapest offering.
type OfferingSpread struct {
	// PriceTolerance is how much more than the cheapest compatible offering, as a fraction of its price, an offering
	// may cost and still be chosen, such as 0.10 for 10%.
	PriceTolerance float64 `json:"priceTolerance,omitempty"`
	// MaxOfferings is how many of the cheapest compatible offerings within the tolerance claims spread across; 1
	// launches every claim with the cheapest offering.
	MaxOfferings int `json:"maxOfferings,omitempty"`
}

// LaunchFallback bounds the offerings a Create call tries: after the cloud refuses an offering for lack of capacity or
// quota, the call tries the claim's next choice until MaxAttempts offerings were tried or Timeout has passed since the
// call began, and only then reports the shortage to Karpenter.
type LaunchFallback struct {
	// MaxAttempts is the most offerings one Create call tries; 1 reports the first refusal.
	MaxAttempts int `json:"maxAttempts,omitempty"`
	// Timeout is how long after a Create call began it may still start a launch with another offering.
	Timeout Duration `json:"timeout,omitzero"`
}

// TransferCost holds the traffic each NodePool's VMs exchange with the home region. An offering in a region costs its
// instance price plus the region's share of a VM's one-time image pulls, amortized over its expected lifetime, plus its
// steady traffic per hour, each priced at the catalog's rate for the side that sends it.
type TransferCost struct {
	// Default is the profile of NodePools that NodePools does not list.
	Default TransferProfile `json:"default,omitzero"`
	// NodePools holds the profile of each named NodePool; an entry replaces Default for that NodePool.
	NodePools map[string]TransferProfile `json:"nodePools,omitempty"`
	// MinMeasuredVMs is how many VMs outside the home region the plugin must have measured for a NodePool before its
	// measured image pulls and steady traffic replace the configured ones; 0 keeps the configured profile.
	MinMeasuredVMs int `json:"minMeasuredVMs,omitempty"`
}

// TransferProfile is what one VM of a NodePool is expected to exchange with the home region.
type TransferProfile struct {
	// ImagePullGB is the GB of images a VM pulls from the home region once, after it boots.
	ImagePullGB float64 `json:"imagePullGB,omitempty"`
	// ExpectedLifetime is how long a VM is expected to run, over which its image pulls are amortized.
	ExpectedLifetime Duration `json:"expectedLifetime,omitzero"`
	// HomeTransmitGBPerHour and HomeReceiveGBPerHour are the GB a VM sends to and receives from the home region per
	// hour besides image pulls, directly or through the tunnel.
	HomeTransmitGBPerHour float64 `json:"homeTransmitGBPerHour,omitempty"`
	HomeReceiveGBPerHour  float64 `json:"homeReceiveGBPerHour,omitempty"`
}

func (p TransferProfile) validate(field string) error {
	switch {
	case p.ImagePullGB < 0:
		return fmt.Errorf("%s.imagePullGB: must not be negative", field)
	case p.ExpectedLifetime.Duration < 0:
		return fmt.Errorf("%s.expectedLifetime: must not be negative", field)
	case p.HomeTransmitGBPerHour < 0 || p.HomeReceiveGBPerHour < 0:
		return fmt.Errorf("%s: GB per hour must not be negative", field)
	}
	return nil
}

// PriceCap bounds the offering prices a NodePool considers, dropping offerings above the cap before Karpenter sees
// them.
type PriceCap struct {
	// Default is the price cap profile for NodePools that NodePools does not list.
	Default PriceCapProfile `json:"default,omitzero"`
	// NodePools holds the price cap profile of each named NodePool; an entry replaces Default for that NodePool.
	NodePools map[string]PriceCapProfile `json:"nodePools,omitempty"`
}

// PriceCapProfile holds the price cap rules for one NodePool.
type PriceCapProfile struct {
	// MaxPricePerGPUHour is an absolute dollar limit on an offering's price per GPU-hour.
	MaxPricePerGPUHour *float64 `json:"maxPricePerGPUHour,omitempty"`
	// MaxPriceAboveCheapestPercent is a relative percentage cap above the cheapest offering of the same GPU class
	// and capacity type in the NodePool's footprint. The reference price is the cheapest offering price per GPU-hour
	// (including transfer cost) for that GPU class and capacity type across the NodePool's footprint.
	MaxPriceAboveCheapestPercent *float64 `json:"maxPriceAboveCheapestPercent,omitempty"`
}

func (p PriceCapProfile) validate(field string) error {
	if p.MaxPricePerGPUHour != nil && *p.MaxPricePerGPUHour < 0 {
		return fmt.Errorf("%s.maxPricePerGPUHour: must not be negative", field)
	}
	if p.MaxPriceAboveCheapestPercent != nil && *p.MaxPriceAboveCheapestPercent < 0 {
		return fmt.Errorf("%s.maxPriceAboveCheapestPercent: must not be negative", field)
	}
	return nil
}

// Enabled reports whether any price cap is configured in this profile.
func (p PriceCapProfile) Enabled() bool {
	return p.MaxPricePerGPUHour != nil || p.MaxPriceAboveCheapestPercent != nil
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
	DefaultQuotaExceededTTL       = 10 * time.Minute
	DefaultSpreadPriceTolerance   = 0.10
	DefaultSpreadMaxOfferings     = 4
	DefaultLaunchMaxAttempts      = 3
	DefaultLaunchFallbackTimeout  = time.Minute
	DefaultMaxPods                = 110
	DefaultEphemeralStorage       = "100Gi"
	DefaultVMLifetime             = 4 * time.Hour
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
	if c.OfferingSpread.PriceTolerance == 0 {
		c.OfferingSpread.PriceTolerance = DefaultSpreadPriceTolerance
	}
	if c.OfferingSpread.PriceTolerance < 0 {
		return fmt.Errorf("offeringSpread.priceTolerance: must not be negative")
	}
	if c.OfferingSpread.MaxOfferings == 0 {
		c.OfferingSpread.MaxOfferings = DefaultSpreadMaxOfferings
	}
	if c.OfferingSpread.MaxOfferings < 0 {
		return fmt.Errorf("offeringSpread.maxOfferings: must be positive")
	}
	if c.LaunchFallback.MaxAttempts == 0 {
		c.LaunchFallback.MaxAttempts = DefaultLaunchMaxAttempts
	}
	if c.LaunchFallback.MaxAttempts < 0 {
		return fmt.Errorf("launchFallback.maxAttempts: must be positive")
	}
	if c.LaunchFallback.Timeout.Duration == 0 {
		c.LaunchFallback.Timeout.Duration = DefaultLaunchFallbackTimeout
	}
	if c.QuotaExceededTTL.Duration == 0 {
		c.QuotaExceededTTL.Duration = DefaultQuotaExceededTTL
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
	if err := c.TransferCost.Default.validate("transferCost.default"); err != nil {
		return err
	}
	for name, p := range c.TransferCost.NodePools {
		if err := p.validate("transferCost.nodePools[" + name + "]"); err != nil {
			return err
		}
	}
	if c.TransferCost.MinMeasuredVMs < 0 {
		return fmt.Errorf("transferCost.minMeasuredVMs: must not be negative")
	}
	if err := c.PriceCap.Default.validate("priceCap.default"); err != nil {
		return err
	}
	for name, p := range c.PriceCap.NodePools {
		if err := p.validate("priceCap.nodePools[" + name + "]"); err != nil {
			return err
		}
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

// TransferProfileFor returns the configured profile of a NodePool.
func (c *Config) TransferProfileFor(nodePool string) TransferProfile {
	if p, ok := c.TransferCost.NodePools[nodePool]; ok {
		return p
	}
	return c.TransferCost.Default
}

// PriceCapProfileFor returns the configured price cap profile of a NodePool.
func (c *Config) PriceCapProfileFor(nodePool string) PriceCapProfile {
	if p, ok := c.PriceCap.NodePools[nodePool]; ok {
		return p
	}
	return c.PriceCap.Default
}

// DeprecatedDataCost reports whether the config sets the ignored dataCost field.
func (c *Config) DeprecatedDataCost() bool {
	trimmed := strings.TrimSpace(string(c.DataCost))
	return trimmed != "" && trimmed != "{}" && trimmed != "null"
}

func normalizeModel(m string) string {
	return strings.ToLower(strings.Join(strings.Fields(m), " "))
}
