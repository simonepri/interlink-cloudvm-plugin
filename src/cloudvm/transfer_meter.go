// Transfer meter: turns the cumulative per-peer and per-pod byte counts each VM's runner reports into Prometheus
// counters by direction, peer class, and path, and prices each delta with the transfer cost model.
package main

import (
	"strings"
	"sync"
	"time"
)

// TrafficBytes is a cumulative byte count in each direction.
type TrafficBytes struct {
	Rx int64 `json:"rx"`
	Tx int64 `json:"tx"`
}

// TrafficOwner is the traffic of one pod, or of the VM's image pulls, by peer class.
type TrafficOwner struct {
	Namespace string                  `json:"namespace,omitempty"`
	Pod       string                  `json:"pod,omitempty"`
	Peers     map[string]TrafficBytes `json:"peers"`
}

// TrafficReport is the runner's traffic meter reading: the VM's bytes per peer, a peer being "tunnel", "private",
// "internet", "unclassified", a provider/region, or a region's object storage, provider/region/storage, and the bytes
// of each owner, a pod UID or "pulls", per peer class.
type TrafficReport struct {
	Peers      map[string]TrafficBytes `json:"peers"`
	Owners     map[string]TrafficOwner `json:"owners,omitempty"`
	Classified bool                    `json:"classified"`
}

// Peer classes relative to a VM, which label the transfer metrics.
const (
	PeerSameRegion = "same_region"
	PeerHomeRegion = "home_region"
	// PeerHomeStorage is the object storage of the home region, such as its S3 endpoints.
	PeerHomeStorage  = "home_storage"
	PeerCrossRegion  = "cross_region"
	PeerInternet     = "internet"
	PeerUnclassified = "unclassified"
	// PeerTunnel is a pod's traffic through the tunnel agent; the VM's tunnel traffic takes its peer's class instead.
	PeerTunnel = "tunnel"
)

// Paths transfer takes: through the reverse tunnel to the cluster, or directly to its peer.
const (
	PathTunnel = "tunnel"
	PathDirect = "direct"
)

const (
	trafficPeerTunnel  = "tunnel"
	trafficPeerPrivate = "private"
	trafficOwnerPulls  = "pulls"

	directionReceive  = "receive"
	directionTransmit = "transmit"

	// transferPodIdle is how long a pod may go unreported before its series are dropped.
	transferPodIdle = 15 * time.Minute
	// transferVMIdle is how long a VM may go unreported before its last readings are forgotten.
	transferVMIdle = time.Hour
)

// TransferVM identifies the VM a traffic report came from.
type TransferVM struct {
	Provider     string
	Region       string
	InstanceType string
	InstanceID   string
	// NodePool is the Karpenter NodePool of a NodeClaim VM, whose transfer profile the VM's traffic feeds.
	NodePool string
}

type meteredVM struct {
	last map[string]int64
	seen time.Time
	pods map[string]time.Time
	// profile is the VM's traffic with the home region, for its NodePool's transfer profile.
	profile vmTransferProfile
	// costs are the VM's priced transfer per minute over the last transferSpendWindow.
	costs []minuteCost
}

type minuteCost struct {
	minute int64
	usd    float64
}

// TransferMeter accumulates traffic report deltas into metrics. Reports are cumulative, so the same reading arriving
// through several polls counts once, and a counter that went down restarted from zero.
type TransferMeter struct {
	mu      sync.Mutex
	metrics *ObservabilityMetrics
	cost    *TransferCostModel
	now     func() time.Time
	vms     map[string]*meteredVM
	swept   time.Time
	// finished holds the profiles of each NodePool's VMs that stopped reporting, newest last.
	finished map[string][]vmTransferProfile
}

// GlobalTransferMeter records the traffic every runner reports.
var GlobalTransferMeter = NewTransferMeter(GlobalMetrics, GlobalTransferCost)

// NewTransferMeter creates a meter that exports to metrics and prices with cost.
func NewTransferMeter(metrics *ObservabilityMetrics, cost *TransferCostModel) *TransferMeter {
	return &TransferMeter{metrics: metrics, cost: cost, now: time.Now, vms: make(map[string]*meteredVM), finished: make(map[string][]vmTransferProfile)}
}

// Record adds the traffic in report since the VM's previous report.
func (t *TransferMeter) Record(vm TransferVM, report *TrafficReport) {
	if t == nil || report == nil || vm.InstanceID == "" {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	now := t.now()
	state := t.vms[vm.InstanceID]
	if state == nil {
		state = &meteredVM{last: make(map[string]int64), pods: make(map[string]time.Time)}
		state.profile.first = now
		t.vms[vm.InstanceID] = state
	}
	state.seen = now
	loc := TransferLocation{Provider: vm.Provider, Region: vm.Region}
	home, hasHome := t.cost.Home()
	if vm.NodePool != "" {
		state.profile.nodePool = vm.NodePool
	}
	state.profile.last = now
	// Only VMs outside the home region pay to reach it, so only theirs describe what an offering's region adds.
	remote := hasHome && loc != home
	state.profile.remote = remote

	for peer, b := range report.Peers {
		class, path, peerLoc, priced := classifyTrafficPeer(peer, loc, home, hasHome)
		for _, d := range []struct {
			direction string
			cur       int64
		}{{directionReceive, b.Rx}, {directionTransmit, b.Tx}} {
			delta := state.delta("vm|"+peer+"|"+d.direction, d.cur)
			if delta <= 0 {
				continue
			}
			t.metrics.AddTransferBytes(vm.Provider, vm.Region, vm.InstanceType, d.direction, class, path, float64(delta))
			if remote && peerLoc == home && priced {
				state.profile.addHome(d.direction, delta)
			}
			if !priced {
				continue
			}
			if usd, charged, ok := t.price(loc, peerLoc, home, hasHome, d.direction, delta); ok {
				t.metrics.AddTransferCost(vm.Provider, vm.Region, d.direction, class, path, charged.Provider, charged.Region, usd)
				state.addCost(now, usd)
			}
		}
	}

	for owner, o := range report.Owners {
		if owner != trafficOwnerPulls {
			state.pods[owner] = now
		}
		for class, b := range o.Peers {
			peerLoc, priced := classLocation(class, loc, home, hasHome)
			for _, d := range []struct {
				direction string
				cur       int64
			}{{directionReceive, b.Rx}, {directionTransmit, b.Tx}} {
				delta := state.delta("owner|"+owner+"|"+class+"|"+d.direction, d.cur)
				if delta <= 0 {
					continue
				}
				if owner == trafficOwnerPulls {
					if d.direction == directionReceive {
						t.metrics.AddImagePullBytes(vm.Provider, vm.Region, vm.InstanceType, class, float64(delta))
						if remote && priced && peerLoc == home {
							state.profile.pullBytes += delta
						}
					}
					continue
				}
				var usd float64
				if priced {
					usd, _, _ = t.price(loc, peerLoc, home, hasHome, d.direction, delta)
				}
				t.metrics.AddPodTransfer(o.Namespace, o.Pod, owner, vm.Provider, vm.Region, d.direction, class, float64(delta), usd)
			}
		}
	}
	t.sweep(now)
}

// price returns the estimated USD of delta bytes between a VM at loc and a peer at peerLoc and the location billed for
// them. Clouds bill the sender: a VM's transmit at the VM's rate, and its receive only when the cluster's home region
// sent it, since other senders are either another VM, which bills its own transmit, or not ours.
func (t *TransferMeter) price(loc, peerLoc, home TransferLocation, hasHome bool, direction string, delta int64) (float64, TransferLocation, bool) {
	src, dst := loc, peerLoc
	if direction == directionReceive {
		if !hasHome || peerLoc != home {
			return 0, TransferLocation{}, false
		}
		src, dst = home, loc
	}
	rate, ok := t.cost.Rate(src, dst)
	if !ok {
		return 0, TransferLocation{}, false
	}
	return rate * float64(delta) / bytesPerGB, src, true
}

// classifyTrafficPeer returns a VM-level peer's class, path, and location, and false when the location is unknown.
// The runner's traffic_peer_class applies the same rules to the pod classes it reports.
func classifyTrafficPeer(peer string, loc, home TransferLocation, hasHome bool) (class, path string, peerLoc TransferLocation, priced bool) {
	switch peer {
	case trafficPeerTunnel:
		if !hasHome {
			return PeerUnclassified, PathTunnel, TransferLocation{}, false
		}
		return classOfLocation(home, loc, home, hasHome), PathTunnel, home, true
	case trafficPeerPrivate:
		return PeerSameRegion, PathDirect, loc, true
	case PeerInternet:
		return PeerInternet, PathDirect, TransferLocation{}, true
	case PeerUnclassified:
		return PeerUnclassified, PathDirect, TransferLocation{}, false
	}
	parts := strings.Split(peer, "/")
	if len(parts) < 2 || len(parts) > 3 || parts[0] == "" || parts[1] == "" || len(parts) == 3 && parts[2] != "storage" {
		return PeerUnclassified, PathDirect, TransferLocation{}, false
	}
	peerLoc = TransferLocation{Provider: parts[0], Region: parts[1]}
	class = classOfLocation(peerLoc, loc, home, hasHome)
	if class == PeerHomeRegion && len(parts) == 3 {
		class = PeerHomeStorage
	}
	return class, PathDirect, peerLoc, true
}

// classOfLocation returns the class of a peer region relative to a VM at loc.
func classOfLocation(peer, loc, home TransferLocation, hasHome bool) string {
	switch {
	case peer == loc:
		return PeerSameRegion
	case hasHome && peer == home:
		return PeerHomeRegion
	case peer.Provider == loc.Provider:
		return PeerCrossRegion
	default:
		return PeerInternet
	}
}

// classLocation returns where a pod's peer class points: cross_region is an unknown region of the VM's provider.
func classLocation(class string, loc, home TransferLocation, hasHome bool) (TransferLocation, bool) {
	switch class {
	case PeerSameRegion:
		return loc, true
	case PeerHomeRegion, PeerHomeStorage, PeerTunnel:
		return home, hasHome
	case PeerCrossRegion:
		return TransferLocation{Provider: loc.Provider}, true
	case PeerInternet:
		return TransferLocation{}, true
	}
	return TransferLocation{}, false
}

func (v *meteredVM) delta(key string, cur int64) int64 {
	prev, ok := v.last[key]
	v.last[key] = cur
	switch {
	case !ok:
		return cur
	case cur < prev:
		return cur
	}
	return cur - prev
}

// sweep drops the series of pods no report mentioned lately and forgets VMs that stopped reporting, once a minute.
func (t *TransferMeter) sweep(now time.Time) {
	if now.Sub(t.swept) < time.Minute {
		return
	}
	t.swept = now
	for id, v := range t.vms {
		for uid, seen := range v.pods {
			if now.Sub(seen) > transferPodIdle {
				delete(v.pods, uid)
				t.metrics.DeletePodTransfer(uid)
				for key := range v.last {
					if strings.HasPrefix(key, "owner|"+uid+"|") {
						delete(v.last, key)
					}
				}
			}
		}
		if now.Sub(v.seen) > transferVMIdle {
			for uid := range v.pods {
				t.metrics.DeletePodTransfer(uid)
			}
			t.retire(v.profile)
			delete(t.vms, id)
		}
	}
}
