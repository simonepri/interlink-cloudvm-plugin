// Short-lived memory of offerings the plugin refused for lack of capacity, so Karpenter tries other offerings.

package cloudprovider

import (
	"sync"
	"time"

	"k8s.io/utils/clock"
)

// unavailableOfferings remembers (instance type, zone, capacity type) triples that recently ran out of capacity.
type unavailableOfferings struct {
	mu    sync.Mutex
	clock clock.Clock
	ttl   time.Duration
	until map[string]time.Time
}

func newUnavailableOfferings(clk clock.Clock, ttl time.Duration) *unavailableOfferings {
	return &unavailableOfferings{clock: clk, ttl: ttl, until: map[string]time.Time{}}
}

func offeringKey(instanceType, zone, capacityType string) string {
	return instanceType + "/" + zone + "/" + capacityType
}

// MarkUnavailable hides the offering for the configured TTL.
func (u *unavailableOfferings) MarkUnavailable(instanceType, zone, capacityType string) {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.until[offeringKey(instanceType, zone, capacityType)] = u.clock.Now().Add(u.ttl)
}

// IsUnavailable reports whether the offering is hidden, dropping expired entries.
func (u *unavailableOfferings) IsUnavailable(instanceType, zone, capacityType string) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	key := offeringKey(instanceType, zone, capacityType)
	until, ok := u.until[key]
	if !ok {
		return false
	}
	if !u.clock.Now().Before(until) {
		delete(u.until, key)
		return false
	}
	return true
}
