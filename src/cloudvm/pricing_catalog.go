// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

//go:embed default_pricing_catalog.json
var defaultPricingCatalogData []byte

// PricingCatalogSchema defines the declarative JSON schema for pricing baselines.
type PricingCatalogSchema struct {
	AWS map[string]float64          `json:"aws"`
	GCP map[string]GCPPriceBaseline `json:"gcp"`
}

// PricingCatalog manages on-demand and spot baseline pricing across cloud providers.
type PricingCatalog struct {
	mu       sync.RWMutex
	awsRates map[string]float64
	gcpRates map[string]GCPPriceBaseline
}

// GlobalPricingCatalog is the singleton active pricing catalog instance.
var GlobalPricingCatalog = NewPricingCatalog()

// NewPricingCatalog initializes a pricing catalog with defaults or env overrides.
func NewPricingCatalog() *PricingCatalog {
	c := &PricingCatalog{
		awsRates: make(map[string]float64),
		gcpRates: make(map[string]GCPPriceBaseline),
	}
	if err := c.LoadDefault(); err != nil {
		log.Printf("[cloudvm-pricing] Warning: failed to load default pricing catalog: %v", err)
	}
	return c
}

// LoadDefault loads the embedded default catalog, then checks PRICING_CATALOG_FILE or PRICING_CATALOG_JSON.
func (c *PricingCatalog) LoadDefault() error {
	if len(defaultPricingCatalogData) > 0 {
		if err := c.LoadJSON(defaultPricingCatalogData); err != nil {
			return err
		}
	}

	if path := os.Getenv("PRICING_CATALOG_FILE"); path != "" {
		cleanPath := filepath.Clean(path)
		if err := c.LoadFile(cleanPath); err != nil {
			log.Printf("[cloudvm-pricing] Failed to load PRICING_CATALOG_FILE (%s): %v", sanitizeLog(cleanPath), err) //nolint:gosec // sanitized
			return err
		}
		log.Printf("[cloudvm-pricing] Loaded pricing catalog from file: %s", sanitizeLog(cleanPath)) //nolint:gosec // sanitized
	} else if rawJSON := os.Getenv("PRICING_CATALOG_JSON"); rawJSON != "" {
		if err := c.LoadJSON([]byte(rawJSON)); err != nil {
			log.Printf("[cloudvm-pricing] Failed to parse PRICING_CATALOG_JSON: %v", err)
			return err
		}
		log.Printf("[cloudvm-pricing] Loaded pricing catalog from PRICING_CATALOG_JSON environment variable")
	}

	return nil
}

// LoadFile parses a JSON file into the pricing catalog.
func (c *PricingCatalog) LoadFile(path string) error {
	cleanPath := filepath.Clean(path)
	data, err := os.ReadFile(cleanPath) //nolint:gosec // operator-configured pricing file path
	if err != nil {
		return fmt.Errorf("failed to read pricing file %s: %w", cleanPath, err)
	}
	return c.LoadJSON(data)
}

// LoadJSON unmarshals catalog JSON into the active rate maps.
func (c *PricingCatalog) LoadJSON(data []byte) error {
	var schema PricingCatalogSchema
	if err := json.Unmarshal(data, &schema); err != nil {
		return fmt.Errorf("failed to parse pricing catalog JSON: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for k, v := range schema.AWS {
		c.awsRates[strings.ToLower(strings.TrimSpace(k))] = v
	}
	for k, v := range schema.GCP {
		c.gcpRates[strings.ToLower(strings.TrimSpace(k))] = v
	}

	return nil
}

// GetAWSRate returns the hourly USD on-demand baseline rate for an EC2 shape.
func (c *PricingCatalog) GetAWSRate(instanceType string) (float64, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	rate, ok := c.awsRates[strings.ToLower(strings.TrimSpace(instanceType))]
	return rate, ok
}

// GetGCPRate returns the on-demand and spot rates for a GCP machine type.
func (c *PricingCatalog) GetGCPRate(machineType string) (onDemand float64, spot float64, ok bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	b, found := c.gcpRates[strings.ToLower(strings.TrimSpace(machineType))]
	if !found {
		return 0, 0, false
	}
	return b.OnDemandUSD, b.SpotUSD, true
}
