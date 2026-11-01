// Transfer cost model: per-provider USD per GB rate tables from the chart, falling back to the AWS Price List book, that
// price a byte by the side clouds bill, the sender, between a VM, the cluster's home region, and the internet.
package main

import (
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"sync"
)

// TransferLocation is a cloud region; an empty Provider means the internet.
type TransferLocation struct {
	Provider string
	Region   string
}

// Key returns the location as provider/region, the form the runner names peers by.
func (l TransferLocation) Key() string {
	return l.Provider + "/" + l.Region
}

// TransferRouteRate prices transfer out of regions matching From to regions matching To, both path.Match patterns; an
// internet rate has no To.
type TransferRouteRate struct {
	From     string  `json:"from"`
	To       string  `json:"to,omitempty"`
	USDPerGB float64 `json:"usdPerGB"`
}

// TransferRates is one provider's rate table, where the first matching rule wins. A route no rule covers is unknown
// and leaves the bytes it would price unpriced.
type TransferRates struct {
	SameRegionUSDPerGB *float64            `json:"sameRegionUSDPerGB,omitempty"`
	CrossRegion        []TransferRouteRate `json:"crossRegion,omitempty"`
	Internet           []TransferRouteRate `json:"internet,omitempty"`
}

// TransferCostConfig is the chart's transferCost value: the cluster's home region and each provider's rates.
type TransferCostConfig struct {
	HomeProvider string                   `json:"homeProvider"`
	HomeRegion   string                   `json:"homeRegion"`
	Rates        map[string]TransferRates `json:"rates"`
}

// ParseTransferCostConfig decodes the JSON the chart renders into TRANSFER_COST_CONFIG; an empty string is no config.
func ParseTransferCostConfig(raw string) (TransferCostConfig, error) {
	var cfg TransferCostConfig
	if strings.TrimSpace(raw) == "" {
		return cfg, nil
	}
	if err := json.Unmarshal([]byte(raw), &cfg); err != nil {
		return cfg, fmt.Errorf("parse transfer cost config: %w", err)
	}
	for provider, rates := range cfg.Rates {
		if rates.SameRegionUSDPerGB != nil && *rates.SameRegionUSDPerGB < 0 {
			return cfg, fmt.Errorf("transfer rates for %s: negative same-region rate", provider)
		}
		for _, r := range append(append([]TransferRouteRate(nil), rates.CrossRegion...), rates.Internet...) {
			if _, err := path.Match(r.From, ""); err != nil {
				return cfg, fmt.Errorf("transfer rates for %s: bad from pattern %q: %w", provider, r.From, err)
			}
			if _, err := path.Match(r.To, ""); err != nil {
				return cfg, fmt.Errorf("transfer rates for %s: bad to pattern %q: %w", provider, r.To, err)
			}
			if r.USDPerGB < 0 {
				return cfg, fmt.Errorf("transfer rates for %s: negative rate from %q to %q", provider, r.From, r.To)
			}
		}
	}
	return cfg, nil
}

// transferPriceSource reads list prices of data leaving a region, as TransferPriceBook does for AWS.
type transferPriceSource interface {
	TransferPrice(src, dst string, kind TransferKind) (float64, bool)
}

// TransferCostModel prices transfers between locations. Rate table entries win over the price book, which only
// covers AWS.
type TransferCostModel struct {
	mu   sync.RWMutex
	cfg  TransferCostConfig
	book transferPriceSource
}

// GlobalTransferCost is the process-wide transfer cost model, configured from TRANSFER_COST_CONFIG.
var GlobalTransferCost = NewTransferCostModel(TransferCostConfig{}, GlobalTransferPrices)

// NewTransferCostModel creates a model over cfg that falls back to book for AWS routes; book may be nil.
func NewTransferCostModel(cfg TransferCostConfig, book transferPriceSource) *TransferCostModel {
	return &TransferCostModel{cfg: cfg, book: book}
}

// Configure replaces the home region and rate tables.
func (m *TransferCostModel) Configure(cfg TransferCostConfig) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfg = cfg
}

// Home returns the region the cluster runs in, or false when it is not configured.
func (m *TransferCostModel) Home() (TransferLocation, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	home := TransferLocation{Provider: m.cfg.HomeProvider, Region: m.cfg.HomeRegion}
	return home, home.Provider != "" && home.Region != ""
}

// HomeKey returns the home region as provider/region, or "" when it is not configured.
func (m *TransferCostModel) HomeKey() string {
	if home, ok := m.Home(); ok {
		return home.Key()
	}
	return ""
}

// Rate returns the USD per GB the sender pays to move data from src to dst, where dst with no provider is the internet
// and dst with no region is an unknown region of its provider. False means no table entry or price list covers it.
func (m *TransferCostModel) Rate(src, dst TransferLocation) (float64, bool) {
	m.mu.RLock()
	rates, hasRates := m.cfg.Rates[src.Provider]
	m.mu.RUnlock()
	switch {
	case dst.Provider == src.Provider && dst.Region == src.Region:
		if hasRates && rates.SameRegionUSDPerGB != nil {
			return *rates.SameRegionUSDPerGB, true
		}
		return 0, false
	case dst.Provider == src.Provider:
		for _, r := range rates.CrossRegion {
			if globMatch(r.From, src.Region) && (r.To == "*" || dst.Region != "" && globMatch(r.To, dst.Region)) {
				return r.USDPerGB, true
			}
		}
		if src.Provider == "aws" && dst.Region != "" && m.book != nil {
			return m.book.TransferPrice(src.Region, dst.Region, TransferCrossRegion)
		}
		return 0, false
	default:
		for _, r := range rates.Internet {
			if globMatch(r.From, src.Region) {
				return r.USDPerGB, true
			}
		}
		if src.Provider == "aws" && m.book != nil {
			return m.book.TransferPrice(src.Region, TransferDestinationInternet, TransferInternet)
		}
		return 0, false
	}
}

func globMatch(pattern, value string) bool {
	ok, err := path.Match(pattern, value)
	return err == nil && ok
}

// bytesPerGB is the gigabyte rates apply to: Google Cloud prices per GiB, and AWS, which prices per GB, is held to the
// same unit, which would understate its cost by 6.9 percent if it meters decimal gigabytes.
const bytesPerGB = 1 << 30
