// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
)

// PodStateTracker abstracts queries to the running InterLink server.
type PodStateTracker interface {
	GetActivePods(ctx context.Context) (map[string]string, error)
	MarkPodFailed(ctx context.Context, podName string, reason string) error
}

// PodCreationTracker provides launch timestamps to protect recently created VMs from eventual consistency eviction.
type PodCreationTracker interface {
	GetPodCreatedAt(ctx context.Context, podKey string) (time.Time, bool)
}

// PodRegionTracker provides the home cloud region of an active pod/instance.
type PodRegionTracker interface {
	GetPodRegion(ctx context.Context, podKey string) (string, bool)
}

// CoTenantReconciler tracks and retries nodes with unverified co-tenant workloads.
type CoTenantReconciler interface {
	RetryUnknownCoTenants(ctx context.Context)
	HasUnknownCoTenants(instanceID string) bool
}

// TombstoneChecker reports whether an instance or its associated pod has been explicitly deleted.
type TombstoneChecker interface {
	IsTombstoned(inst *Instance) bool
}

// InstanceTerminator abstracts VM termination for the reconciler.
type InstanceTerminator interface {
	Terminate(ctx context.Context, instanceID string) error
}

// NodePoolProvider retrieves a NodePool if implemented by a tracker or server.
type NodePoolProvider interface {
	NodePool() *NodePool
}

// Reconciler handles background garbage collection for opportunistic VMs.
const defaultMaxPingConcurrency = 20

type Reconciler struct {
	inventory         CloudInventory
	tracker           PodStateTracker
	terminator        InstanceTerminator
	clusterTag        string
	graceTTL          time.Duration
	launchGracePeriod time.Duration
	onTerminate       func(instanceID string)
	nodePool          *NodePool
	httpClient        *http.Client
	pingInterval      time.Duration
	pingConcurrency   int
	pingJitter        time.Duration
}

// SetOnTerminated registers a callback invoked when a zombie instance is terminated.
func (r *Reconciler) SetOnTerminated(fn func(instanceID string)) {
	r.onTerminate = fn
}

// SetLaunchGracePeriod configures the minimum age a cloud VM must have before being declared vanished.
// This prevents premature pod eviction caused by EC2 DescribeInstances eventual consistency.
func (r *Reconciler) SetLaunchGracePeriod(d time.Duration) {
	r.launchGracePeriod = d
}

// ReconcileResult summarizes the actions taken by one reconciliation pass.
type ReconcileResult struct {
	TerminatedZombies []string
	EvictedPods       []string
}

// NewReconciler creates a background garbage collector.
func NewReconciler(inventory CloudInventory, tracker PodStateTracker, terminator InstanceTerminator, clusterTag string, graceTTL time.Duration) *Reconciler {
	if graceTTL <= 0 {
		graceTTL = 10 * time.Minute
	}
	r := &Reconciler{
		inventory:         inventory,
		tracker:           tracker,
		terminator:        terminator,
		clusterTag:        clusterTag,
		graceTTL:          graceTTL,
		launchGracePeriod: 2 * time.Minute,
		pingInterval:      30 * time.Second,
		pingConcurrency:   defaultMaxPingConcurrency,
		pingJitter:        5 * time.Millisecond,
		httpClient:        &http.Client{Timeout: 5 * time.Second},
	}
	if p, ok := tracker.(NodePoolProvider); ok {
		r.nodePool = p.NodePool()
	}
	return r
}

// SetNodePool configures the active NodePool for keep-alive pings.
func (r *Reconciler) SetNodePool(np *NodePool) {
	r.nodePool = np
}

// SetHTTPClient configures a custom HTTP client for keep-alive pings.
func (r *Reconciler) SetHTTPClient(client *http.Client) {
	r.httpClient = client
}

// SetPingInterval configures the keep-alive ping interval.
func (r *Reconciler) SetPingInterval(d time.Duration) {
	if d > 0 {
		r.pingInterval = d
	}
}

// SetPingConcurrency configures the maximum concurrent keep-alive pings.
func (r *Reconciler) SetPingConcurrency(n int) {
	r.pingConcurrency = n
}

// SetPingJitter configures the per-node keep-alive ping jitter.
func (r *Reconciler) SetPingJitter(d time.Duration) {
	r.pingJitter = d
}

func (r *Reconciler) getNodePool() *NodePool {
	if r.nodePool != nil {
		return r.nodePool
	}
	if p, ok := r.tracker.(NodePoolProvider); ok {
		return p.NodePool()
	}
	return nil
}

// PingActiveNodes sends authenticated keep-alive pings to all active, non-cordoned nodes in the pool.
func (r *Reconciler) PingActiveNodes(ctx context.Context) {
	r.pingActiveNodes(ctx)
}

func (r *Reconciler) pingActiveNodes(ctx context.Context) {
	np := r.getNodePool()
	if np == nil {
		return
	}

	nodes := np.ActiveNodes()
	if len(nodes) == 0 {
		return
	}

	client := r.httpClient
	if client == nil {
		client = &http.Client{Timeout: 5 * time.Second}
	}

	limit := r.pingConcurrency
	if limit <= 0 {
		limit = defaultMaxPingConcurrency
	}
	sem := make(chan struct{}, limit)

	var wg sync.WaitGroup
NodeLoop:
	for _, n := range nodes {
		if n == nil {
			continue
		}
		if n.IsCordoned() || n.Cordoned {
			continue
		}
		// Only ping idle nodes! Running workloads keep the VM alive without provider pings.
		if n.AllocatedPods > 0 || len(n.Pods) > 0 || n.ActivePodCount() > 0 {
			continue
		}

		ip := n.GetInternalIP()
		if ip == "" {
			ip = n.GetMeshIP()
		}
		if ip == "" {
			ip = n.IP
		}
		if ip == "" {
			continue
		}

		select {
		case <-ctx.Done():
			break NodeLoop
		case sem <- struct{}{}:
		}

		wg.Add(1)
		go func(node *CloudVMNode, nodeIP string) {
			defer wg.Done()
			defer func() { <-sem }()

			if r.pingJitter > 0 {
				jitter := cryptoRandDuration(r.pingJitter)
				if jitter > 0 {
					select {
					case <-ctx.Done():
						return
					case <-time.After(jitter):
					}
				}
			}

			r.pingSingleNode(ctx, client, node, nodeIP)
		}(n, ip)
	}
	wg.Wait()
}

func (r *Reconciler) pingSingleNode(ctx context.Context, client *http.Client, node *CloudVMNode, ip string) {
	var hostPort string
	if strings.Contains(ip, ":") && !strings.HasPrefix(ip, "[") {
		if h, p, err := net.SplitHostPort(ip); err == nil {
			hostPort = net.JoinHostPort(h, p)
		} else {
			hostPort = net.JoinHostPort(ip, "9090")
		}
	} else {
		hostPort = net.JoinHostPort(ip, "9090")
	}

	url := fmt.Sprintf("http://%s/healthz", hostPort)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil) //nolint:gosec // internal mesh endpoint
	if err != nil {
		log.Printf("[reconciler] Debug: failed to build keep-alive ping request for node %s: %v", sanitizeLog(node.InstanceID), err) //nolint:gosec // sanitized
		return
	}

	token := node.RunnerAuthToken
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := client.Do(req) //nolint:gosec // internal mesh call
	if err != nil {
		log.Printf("[reconciler] Debug: keep-alive ping failed for node %s (%s): %v", sanitizeLog(node.InstanceID), sanitizeLog(ip), err) //nolint:gosec // sanitized
		return
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)

	if resp.StatusCode != http.StatusOK {
		log.Printf("[reconciler] Debug: keep-alive ping returned unexpected status %d for node %s (%s)", resp.StatusCode, sanitizeLog(node.InstanceID), sanitizeLog(ip)) //nolint:gosec // sanitized
		return
	}
}

func cryptoRandDuration(maxDuration time.Duration) time.Duration {
	if maxDuration <= 0 {
		return 0
	}
	n, err := rand.Int(rand.Reader, big.NewInt(int64(maxDuration)))
	if err != nil {
		return 0
	}
	return time.Duration(n.Int64())
}

// Start runs the keep-alive ping ticker loop (every 30 seconds by default) until ctx is cancelled.
func (r *Reconciler) Start(ctx context.Context) {
	interval := r.pingInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}

	// Dispatch initial ping immediately
	r.pingActiveNodes(ctx)

	for {
		var jitter time.Duration
		if interval > 10*time.Millisecond {
			jitter = cryptoRandDuration(interval / 10)
		}
		timer := time.NewTimer(interval + jitter)

		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
			r.pingActiveNodes(ctx)
		}
	}
}

// ReconcileOnce performs a single pass of bidirectional synchronization.
func (r *Reconciler) ReconcileOnce(ctx context.Context) (*ReconcileResult, error) {
	result := &ReconcileResult{}

	// Retry discovering containers on nodes with unverified co-tenants
	if retrier, ok := r.tracker.(CoTenantReconciler); ok {
		retrier.RetryUnknownCoTenants(ctx)
	}

	var activeInstances []*Instance
	var errorsByRegion map[string]error

	if regInv, ok := r.inventory.(RegionInventory); ok {
		byRegion, errs := regInv.ListActiveInstancesByRegion(ctx, r.clusterTag)
		errorsByRegion = errs
		for _, insts := range byRegion {
			activeInstances = append(activeInstances, insts...)
		}
		if len(byRegion) == 0 && len(errs) > 0 {
			var errMsgs []string
			for reg, err := range errs {
				errMsgs = append(errMsgs, fmt.Sprintf("%s: %v", reg, err))
			}
			return nil, fmt.Errorf("failed to query all cloud inventory regions: %s", strings.Join(errMsgs, "; "))
		}
	} else {
		insts, err := r.inventory.ListActiveInstances(ctx, r.clusterTag)
		if err != nil {
			return nil, fmt.Errorf("failed to query cloud inventory: %w", err)
		}
		activeInstances = insts
	}

	activePods, err := r.tracker.GetActivePods(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query active pods: %w", err)
	}

	// Update live fleet metrics from cloud inventory
	GlobalFleetTracker.UpdateLiveFleet(activeInstances, activePods)

	cloudInstances := make(map[string]*Instance, len(activeInstances))
	for _, inst := range activeInstances {
		if inst != nil {
			cloudInstances[inst.ID] = inst
		}
	}

	// 1. Forward check: detect pods whose cloud VM vanished or terminated
	for podKey, instID := range activePods {
		if instID == "" {
			continue
		}
		cleanKey := sanitizeLog(podKey)
		cleanID := sanitizeLog(instID)

		// Skip judging pods whose home cloud region failed to answer
		if len(errorsByRegion) > 0 {
			if rt, ok := r.tracker.(PodRegionTracker); ok {
				if podReg, ok := rt.GetPodRegion(ctx, podKey); ok && podReg != "" {
					if err, failed := errorsByRegion[podReg]; failed && err != nil {
						log.Printf("[reconciler] Skipping pod %s in region %s because region query failed: %v", cleanKey, sanitizeLog(podReg), err) //nolint:gosec // sanitized
						continue
					}
				}
			}
		}

		inst, alive := cloudInstances[instID]

		if !alive && r.launchGracePeriod > 0 {
			if pt, ok := r.tracker.(PodCreationTracker); ok {
				if createdAt, ok := pt.GetPodCreatedAt(ctx, podKey); ok && !createdAt.IsZero() {
					if time.Since(createdAt) < r.launchGracePeriod {
						log.Printf("[reconciler] Skipping recently launched VM %s for pod %s (age %v < launch grace %v)", cleanID, cleanKey, time.Since(createdAt).Round(time.Second), r.launchGracePeriod) //nolint:gosec // sanitized
						continue
					}
				}
			}
		}

		if !alive || (inst != nil && (inst.State == StateTerminated || inst.State == StateFailed)) {
			reason := "SpotInterruption"
			if checker, ok := r.inventory.(InstanceStatusChecker); ok {
				if st, err := checker.Status(ctx, instID); err == nil && st != nil && st.Metadata != nil {
					if code := st.Metadata["stateReasonCode"]; code != "" {
						if strings.Contains(code, "SpotInstanceTermination") {
							reason = "SpotInterruption"
						} else {
							reason = code
						}
					} else if st.Metadata["marketType"] == "on-demand" {
						reason = "NodeLost"
					}
				}
			}
			log.Printf("[reconciler] Detected vanished/interrupted cloud VM %s for pod %s (reason: %s). Marking pod failed...", cleanID, cleanKey, reason) //nolint:gosec // sanitized
			if reason == "SpotInterruption" || strings.Contains(reason, "Preempt") || strings.Contains(reason, "Spot") {
				prov := "aws"
				reg := ""
				itype := ""
				capType := "spot"
				if inst != nil {
					prov = string(inst.Provider)
					reg = inst.Region
					itype = inst.InstanceType
				}
				GlobalFleetTracker.RecordInterruption(prov, reg, itype, capType)
			}
			if err := r.tracker.MarkPodFailed(ctx, podKey, reason); err != nil {
				log.Printf("[reconciler] Error marking pod %s failed: %v", cleanKey, err)
			} else {
				result.EvictedPods = append(result.EvictedPods, podKey)
			}
		}
	}

	// 2. Reverse check: detect zombie VMs whose pods vanished from tracker
	activeInstanceIDs := make(map[string]bool, len(activePods))
	for _, id := range activePods {
		if id != "" {
			activeInstanceIDs[id] = true
		}
	}

	now := time.Now()
	for _, inst := range activeInstances {
		if activeInstanceIDs[inst.ID] {
			continue
		}

		// Re-adoption check: if this VM has valid pod tags and was missed during startup recovery, re-adopt it
		if adopter, ok := r.tracker.(InstanceAdopter); ok {
			if adopter.AdoptInstance(ctx, inst) {
				log.Printf("[reconciler] Re-adopted active VM %s with valid pod tags", sanitizeLog(inst.ID)) //nolint:gosec // sanitized
				activeInstanceIDs[inst.ID] = true
				continue
			}
		}

		// Co-tenant protection check: do not terminate VMs whose co-tenant workloads are still unverified
		if checker, ok := r.tracker.(CoTenantReconciler); ok {
			if checker.HasUnknownCoTenants(inst.ID) {
				log.Printf("[reconciler] Skipping VM %s with unverified co-tenants pending runner connectivity", sanitizeLog(inst.ID)) //nolint:gosec // sanitized
				continue
			}
		}

		podName := inst.Metadata["podName"]
		if podName == "" {
			podName = inst.Metadata["PodName"]
		}
		if podName == "" {
			continue
		}

		ns := inst.Metadata["namespace"]
		if ns == "" {
			ns = inst.Metadata["Namespace"]
		}
		if ns == "" {
			ns = "default"
		}

		fullKey := fmt.Sprintf("%s/%s", ns, podName)
		_, existsFull := activePods[fullKey]
		_, existsBare := activePods[podName]

		isTombstoned := false
		if checker, ok := r.tracker.(TombstoneChecker); ok {
			isTombstoned = checker.IsTombstoned(inst)
		}

		if !existsFull && !existsBare {
			if isTombstoned || now.Sub(inst.CreatedAt) > r.graceTTL {
				cleanID := sanitizeLog(inst.ID)
				cleanKey := sanitizeLog(fullKey)
				log.Printf("[reconciler] Detected zombie instance %s for vanished pod %s. Terminating...", cleanID, cleanKey) //nolint:gosec // sanitized
				result.TerminatedZombies = append(result.TerminatedZombies, inst.ID)
				if r.terminator != nil {
					if err := r.terminator.Terminate(ctx, inst.ID); err != nil {
						log.Printf("[reconciler] Error terminating zombie instance %s: %v", cleanID, err) //nolint:gosec // sanitized
					} else if r.onTerminate != nil {
						r.onTerminate(inst.ID)
					}
				}
			}
		}
	}

	return result, nil
}
