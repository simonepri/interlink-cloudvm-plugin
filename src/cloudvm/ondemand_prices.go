// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"sync"
	"time"
)

// On-demand list prices change a few times a year, so a day-old lookup is current; a failed lookup is retried sooner.
const (
	onDemandPriceTTL      = 24 * time.Hour
	onDemandPriceRetry    = time.Hour
	onDemandLookupTimeout = 10 * time.Second
)

// OnDemandPriceBook caches AWS on-demand hourly prices per region and instance type from the Price List API.
type OnDemandPriceBook struct {
	mu      sync.Mutex
	client  AWSPricingAPI
	entries map[string]onDemandPriceEntry
	now     func() time.Time
}

type onDemandPriceEntry struct {
	price   float64
	expires time.Time
}

// GlobalOnDemandPrices is the process-wide price book shared by the spot ticker and the fleet tracker.
var GlobalOnDemandPrices = NewOnDemandPriceBook(nil)

// NewOnDemandPriceBook creates a price book that looks prices up through client, or only in the pricing catalog when client is nil.
func NewOnDemandPriceBook(client AWSPricingAPI) *OnDemandPriceBook {
	return &OnDemandPriceBook{
		client:  client,
		entries: make(map[string]onDemandPriceEntry),
		now:     time.Now,
	}
}

// SetClient replaces the Price List API client and forgets cached prices.
func (b *OnDemandPriceBook) SetClient(client AWSPricingAPI) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.client = client
	b.entries = make(map[string]onDemandPriceEntry)
}

// Price returns the hourly on-demand USD price of an instance type in a region, or 0 when no source knows it.
func (b *OnDemandPriceBook) Price(ctx context.Context, region, instanceType string) float64 {
	key := region + "/" + instanceType
	b.mu.Lock()
	entry, ok := b.entries[key]
	client := b.client
	b.mu.Unlock()
	if ok && b.now().Before(entry.expires) {
		return entry.price
	}

	lookupCtx, cancel := context.WithTimeout(ctx, onDemandLookupTimeout)
	defer cancel()
	price := GetAWSOnDemandPrice(lookupCtx, client, region, instanceType)

	ttl := onDemandPriceTTL
	if price <= 0 {
		ttl = onDemandPriceRetry
	}
	b.mu.Lock()
	b.entries[key] = onDemandPriceEntry{price: price, expires: b.now().Add(ttl)}
	b.mu.Unlock()
	return price
}
