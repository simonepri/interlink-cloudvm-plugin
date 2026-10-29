// Caches plugin catalogs per footprint so scheduling loops do not call the plugin for every NodePool.

package cloudprovider

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
)

type cachedCatalog struct {
	catalog   *nodeclaim.Catalog
	fetchedAt time.Time
}

// catalogCache serves the last catalog per region set for ttl, and keeps serving it for maxStale while the plugin
// fails, so a short plugin outage does not empty every NodePool.
type catalogCache struct {
	api      nodeclaim.API
	clock    clock.Clock
	ttl      time.Duration
	maxStale time.Duration

	mu      sync.Mutex
	entries map[string]cachedCatalog
}

func newCatalogCache(api nodeclaim.API, clk clock.Clock, ttl, maxStale time.Duration) *catalogCache {
	return &catalogCache{api: api, clock: clk, ttl: ttl, maxStale: maxStale, entries: map[string]cachedCatalog{}}
}

func (c *catalogCache) Get(ctx context.Context, regions []string) (*nodeclaim.Catalog, error) {
	regions = slices.Sorted(slices.Values(regions))
	key := strings.Join(regions, ",")
	now := c.clock.Now()

	c.mu.Lock()
	entry, ok := c.entries[key]
	c.mu.Unlock()
	if ok && now.Sub(entry.fetchedAt) < c.ttl {
		return entry.catalog, nil
	}

	fresh, err := c.api.Catalog(ctx, regions)
	if err != nil {
		if ok && now.Sub(entry.fetchedAt) < c.maxStale {
			log.FromContext(ctx).Error(err, "serving cached catalog after plugin catalog failure", "age", now.Sub(entry.fetchedAt))
			return entry.catalog, nil
		}
		return nil, fmt.Errorf("fetching plugin catalog: %w", err)
	}
	c.mu.Lock()
	c.entries[key] = cachedCatalog{catalog: fresh, fetchedAt: now}
	c.mu.Unlock()
	return fresh, nil
}
