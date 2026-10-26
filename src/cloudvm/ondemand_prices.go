// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"sync"
	"time"
)

// On-demand list prices change a few times a year, so a day-old API price is current; a fallback is retried sooner.
const (
	onDemandPriceTTL      = 24 * time.Hour
	onDemandPriceRetry    = time.Hour
	onDemandLookupTimeout = 10 * time.Second
)

// OnDemandPriceBook caches AWS on-demand hourly prices per region and instance type from the Price List API.
type OnDemandPriceBook struct {
	mu       sync.Mutex
	client   AWSPricingAPI
	entries  map[string]onDemandPriceEntry
	inflight map[string]chan struct{}
	now      func() time.Time
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
		client:   client,
		entries:  make(map[string]onDemandPriceEntry),
		inflight: make(map[string]chan struct{}),
		now:      time.Now,
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
// It waits for the Price List API when the cached value is missing or expired.
func (b *OnDemandPriceBook) Price(ctx context.Context, region, instanceType string) float64 {
	key := region + "/" + instanceType
	for {
		b.mu.Lock()
		if entry, ok := b.entries[key]; ok && b.now().Before(entry.expires) {
			b.mu.Unlock()
			return entry.price
		}
		if wait, busy := b.inflight[key]; busy {
			b.mu.Unlock()
			select {
			case <-wait:
				continue
			case <-ctx.Done():
				return b.Cached(region, instanceType)
			}
		}
		done := make(chan struct{})
		b.inflight[key] = done
		client := b.client
		b.mu.Unlock()
		return b.lookup(ctx, client, key, region, instanceType, done)
	}
}

// Cached returns the last known price, or the catalog rate, without waiting on the network.
// On a miss or an expired entry it refreshes the price in the background.
func (b *OnDemandPriceBook) Cached(region, instanceType string) float64 {
	key := region + "/" + instanceType
	b.mu.Lock()
	entry, ok := b.entries[key]
	fresh := ok && b.now().Before(entry.expires)
	if !fresh {
		if _, busy := b.inflight[key]; !busy {
			done := make(chan struct{})
			b.inflight[key] = done
			client := b.client
			go b.lookup(context.Background(), client, key, region, instanceType, done)
		}
	}
	b.mu.Unlock()
	if ok {
		return entry.price
	}
	return OnDemandBaselineUSD(instanceType)
}

func (b *OnDemandPriceBook) lookup(ctx context.Context, client AWSPricingAPI, key, region, instanceType string, done chan struct{}) float64 {
	lookupCtx, cancel := context.WithTimeout(ctx, onDemandLookupTimeout)
	defer cancel()
	price, listed := listAWSOnDemandPrice(lookupCtx, client, region, instanceType)
	ttl := onDemandPriceTTL
	if !listed {
		price = OnDemandBaselineUSD(instanceType)
		ttl = onDemandPriceRetry
	}

	b.mu.Lock()
	b.entries[key] = onDemandPriceEntry{price: price, expires: b.now().Add(ttl)}
	delete(b.inflight, key)
	b.mu.Unlock()
	close(done)
	return price
}
