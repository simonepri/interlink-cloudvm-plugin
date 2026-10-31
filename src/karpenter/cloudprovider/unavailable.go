// Short-lived memory of offerings the plugin refused for lack of capacity or quota, so Karpenter tries other offerings.

package cloudprovider

import (
	"sync"
	"time"

	"k8s.io/utils/clock"
)

// unavailableOfferings remembers (instance type, zone, capacity type) triples that recently ran out of capacity or
// quota.
type unavailableOfferings struct {
	mu    sync.Mutex
	clock clock.Clock
	until map[string]time.Time
}

func newUnavailableOfferings(clk clock.Clock) *unavailableOfferings {
	return &unavailableOfferings{clock: clk, until: map[string]time.Time{}}
}

func offeringKey(instanceType, zone, capacityType string) string {
	return instanceType + "/" + zone + "/" + capacityType
}

// MarkUnavailable hides the offering for ttl, or for longer if an earlier refusal already hides it longer.
func (u *unavailableOfferings) MarkUnavailable(instanceType, zone, capacityType string, ttl time.Duration) {
	u.mu.Lock()
	defer u.mu.Unlock()
	key := offeringKey(instanceType, zone, capacityType)
	if until := u.clock.Now().Add(ttl); until.After(u.until[key]) {
		u.until[key] = until
	}
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
