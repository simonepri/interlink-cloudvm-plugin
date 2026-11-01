// VM-per-NodeClaim API: launches one VM for each Karpenter NodeClaim, persists the claims, places the pods bound to a
// claim's Node on its VM, keeps idle claim VMs alive, and terminates a VM only when its claim is deleted.
package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"maps"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim/lifecycletrace"
)

const (
	// claimPodUIDPrefix prefixes the UID a claim's launch uses in place of a pod UID, which keeps CreateFleet client
	// tokens and runner tokens stable across retries of the same claim.
	claimPodUIDPrefix = "nodeclaim-"
	// claimSyncInterval is how often the claim loop checks readiness, heartbeats idle VMs and persists allocations.
	claimSyncInterval = 15 * time.Second
	// claimHeartbeatInterval is how often an idle claim VM's runner is called, which keeps its in-VM idle watchdog
	// from powering it off.
	claimHeartbeatInterval = time.Minute
	// claimReadyTimeout bounds how long a claim VM may take to become ready when no tunnel connect timeout applies.
	claimReadyTimeout = 10 * time.Minute
	// claimProbeConcurrency bounds parallel runner calls of one claim loop pass.
	claimProbeConcurrency = 32
	// claimTerminateTimeout bounds the cloud call that terminates a claim's VM.
	claimTerminateTimeout = 30 * time.Second
	// claimPodWatchLag is how long a pod placed on a claim VM is kept although the pod watch does not list it, because
	// the watch can trail the node agent's informer whose pod the create carried.
	claimPodWatchLag = time.Minute
	// claimDeletedMemory is how long the pods of a deleted claim's Node keep being refused.
	claimDeletedMemory = time.Hour
	// unavailableOfferingTTL is how long an offering that failed for lack of capacity is reported unavailable.
	unavailableOfferingTTL = 3 * time.Minute
	// defaultClaimPods is the pod capacity a claim Node advertises.
	defaultClaimPods = 110
)

// errClaimPodRefused reports a pod bound to a claim's Node that the claim's VM cannot take.
var errClaimPodRefused = errors.New("claim VM refuses pod")

// claimPod is one pod placed on a claim's VM, persisted so a restarted plugin keeps the pod's GPU devices.
type claimPod struct {
	Namespace string `json:"namespace"`
	Name      string `json:"name"`
	GPUs      []int  `json:"gpus,omitempty"`
	TPUs      []int  `json:"tpus,omitempty"`
	// DaemonSet marks a pod a DaemonSet owns, which never binds the VM's namespace.
	DaemonSet bool `json:"daemonSet,omitempty"`
}

// claimRecord is the persisted state of one NodeClaim's VM.
type claimRecord struct {
	Request    nodeclaim.CreateRequest `json:"request"`
	InstanceID string                  `json:"instanceId"`
	// ProviderID is the providerID the claim's Node and NodeClaim carry, empty for a claim persisted by an earlier
	// release, which carries the legacy format; see claimRecord.providerID.
	ProviderID   string         `json:"providerID,omitempty"`
	Provider     ProviderType   `json:"provider"`
	Region       string         `json:"region"`
	Zone         string         `json:"zone,omitempty"`
	InstanceType string         `json:"instanceType"`
	CapacityType string         `json:"capacityType"`
	IP           string         `json:"ip,omitempty"`
	Namespace    string         `json:"namespace,omitempty"`
	CreatedAt    time.Time      `json:"createdAt"`
	Capacity     ResourceVector `json:"capacity"`
	// EphemeralStorage is the size of the disk the VM keeps its containers on, empty until the provider reported it.
	EphemeralStorage string              `json:"ephemeralStorage,omitempty"`
	Pods             map[string]claimPod `json:"pods,omitempty"`
}

// providerID returns the providerID the claim was launched with. A Node's providerID never changes and Karpenter
// matches NodeClaims to cloud instances by it, so a claim persisted by an earlier release keeps the legacy format.
func (rec *claimRecord) providerID() string {
	if rec.ProviderID != "" {
		return rec.ProviderID
	}
	return nodeclaim.FormatLegacyProviderID(string(rec.Provider), rec.Region, rec.InstanceID)
}

// claimFlight lets concurrent creates of one claim wait for the first.
type claimFlight struct {
	done   chan struct{}
	record *claimRecord
	err    error
}

// claimRegistry holds the claims of the plugin process.
type claimRegistry struct {
	mu          sync.Mutex
	store       *diskMap[*claimRecord]
	loadErr     error             // set when the store file could not be read, which disables the API instead of losing claims
	byInstance  map[string]string // instance ID -> claim name
	flights     map[string]*claimFlight
	deleted     map[string]time.Time // claim name -> when it was deleted
	ready       map[string]bool      // instance ID -> runner answered, for VMs without a tunnel
	heartbeats  map[string]time.Time // instance ID -> last runner call
	unavailable map[string]time.Time // offering key -> until when it is unavailable
}

// newClaimRegistry loads the claims persisted at path, or in the temporary directory under the cluster tag when path is
// empty.
func newClaimRegistry(path, clusterTag string) *claimRegistry {
	store, err := newDiskMap[*claimRecord]("claim store", path, "cloudvm-nodeclaims-"+clusterTag+".json", true, nil)
	if err != nil {
		log.Printf("[cloudvm] ERROR loading claim store: %v", err)
	}
	byInstance := make(map[string]string)
	for name, rec := range store.getAll() {
		byInstance[rec.InstanceID] = name
	}
	return &claimRegistry{
		loadErr:     err,
		store:       store,
		byInstance:  byInstance,
		flights:     make(map[string]*claimFlight),
		deleted:     make(map[string]time.Time),
		ready:       make(map[string]bool),
		heartbeats:  make(map[string]time.Time),
		unavailable: make(map[string]time.Time),
	}
}

func offeringKey(instanceType, region, zone, capacityType string) string {
	return strings.Join([]string{instanceType, region, zone, capacityType}, "/")
}

// claimRecords returns the persisted claims.
func (s *Server) claimRecords() map[string]*claimRecord {
	return s.claims.store.getAll()
}

// IsClaimInstance reports whether a VM belongs to a NodeClaim, whose lifecycle only the claim API drives.
func (s *Server) IsClaimInstance(instanceID string) bool {
	s.claims.mu.Lock()
	defer s.claims.mu.Unlock()
	_, ok := s.claims.byInstance[instanceID]
	return ok
}

// claimForPod reports whether the pod is bound to a NodeClaim's Node, and returns that claim's node when it is live.
// A pod of a claim that is gone, or of a node agent Node the plugin does not know, gets no node but is still a claim
// pod, so it never launches a VM of its own.
func (s *Server) claimForPod(pod *Pod) (*CloudVMNode, bool) {
	name := pod.Spec.NodeName
	if name == "" {
		return nil, false
	}
	if rec, ok := s.claims.store.get(name); ok {
		node, _ := s.nodePool.Node(rec.InstanceID)
		return node, true
	}
	s.claims.mu.Lock()
	_, deleted := s.claims.deleted[name]
	s.claims.mu.Unlock()
	if deleted {
		return nil, true
	}
	s.mu.RLock()
	lookup, _ := s.nodeRegionLookup.(NodeAgentLookup)
	s.mu.RUnlock()
	return nil, lookup != nil && lookup.ServedByNodeAgent(name)
}

// allocateClaimPod places a pod on its claim's VM. The scheduler already fitted the pod's requests against the Node's
// allocatable, so only GPU and TPU devices are assigned here. The VM serves the namespace of its first workload pod and
// refuses workload pods of any other; a DaemonSet pod of any namespace runs on it like on a kubelet's node and never
// binds or changes that namespace. When the VM lacks free devices, the pods that no longer exist are released first,
// since the scheduler counted only existing ones.
func (s *Server) allocateClaimPod(ctx context.Context, node *CloudVMNode, pod *Pod, spec *InstanceSpec, req ResourceVector) (*CloudVMNode, []int, []int, error) {
	if node == nil {
		return nil, nil, nil, fmt.Errorf("%w: its NodeClaim has no VM", errClaimPodRefused)
	}
	daemonSet := pod.Metadata.DaemonSetOwned()
	if err := s.checkClaimPodNamespace(node, spec.PodNamespace, daemonSet); err != nil {
		return nil, nil, nil, err
	}
	request := AllocationRequest{
		Resources: ResourceVector{GPUs: req.GPUs, TPUs: req.TPUs},
		PodUID:    spec.PodUID,
		Namespace: spec.PodNamespace,
		PodName:   spec.PodName,
		ClaimName: node.ClaimName,
		DaemonSet: daemonSet,
	}
	alloc, err := s.nodePool.AllocatePod(request)
	if err != nil && s.releaseDepartedClaimPods(ctx, node) > 0 {
		alloc, err = s.nodePool.AllocatePod(request)
	}
	if err != nil {
		return nil, nil, nil, fmt.Errorf("%w: VM %s: %w", errClaimPodRefused, node.InstanceID, err)
	}
	s.persistClaimPods(alloc.Node)
	return alloc.Node, alloc.GPUs, alloc.TPUs, nil
}

// checkClaimPodNamespace refuses a workload pod whose namespace the claim's VM does not serve: a pod of a regional
// namespace off a regional VM or the reverse, or a pod of a namespace other than the one the VM is bound to. A
// DaemonSet pod passes, since it never binds the VM's namespace.
func (s *Server) checkClaimPodNamespace(node *CloudVMNode, namespace string, daemonSet bool) error {
	if daemonSet {
		return nil
	}
	if regional := s.claimIsRegional(node.ClaimName); regional != s.regionalNamespaces[namespace] {
		if regional {
			return fmt.Errorf("%w: regional VM %s runs pods of the regional namespaces only", errClaimPodRefused, node.InstanceID)
		}
		return fmt.Errorf("%w: pods of regional namespace %q run only on regional VMs", errClaimPodRefused, namespace)
	}
	if owner, ok := s.nodePool.InstanceNamespace(node.InstanceID); ok && owner != namespace {
		return fmt.Errorf("%w: VM %s runs pods of namespace %q only", errClaimPodRefused, node.InstanceID, owner)
	}
	return nil
}

// claimIsRegional reports whether the claim's taints mark its VM as a regional VM.
func (s *Server) claimIsRegional(claimName string) bool {
	rec, ok := s.claims.store.get(claimName)
	if !ok {
		return false
	}
	for _, t := range rec.Request.Taints {
		if t.Key == TaintRegional {
			return true
		}
	}
	return false
}

// releaseDepartedClaimPods runs the delete path for every pod on a claim's VM that the pod watch no longer lists, such
// as one deleted while the node agent was down, and returns how many it released. Without a synced pod watch it
// releases nothing.
func (s *Server) releaseDepartedClaimPods(ctx context.Context, node *CloudVMNode) int {
	s.mu.RLock()
	watch := s.podExistence
	s.mu.RUnlock()
	if watch == nil || !watch.HasSynced() {
		return 0
	}
	cutoff := time.Now().Add(-claimPodWatchLag)
	s.nodePool.mu.RLock()
	placed := make([]PodAllocation, 0, len(node.ActivePods))
	for _, a := range node.ActivePods {
		if a.StartedAt.Before(cutoff) {
			placed = append(placed, *a)
		}
	}
	s.nodePool.mu.RUnlock()
	released := 0
	for _, a := range placed {
		if watch.PodExists(a.PodUID) {
			continue
		}
		log.Printf("[cloudvm] Pod %s/%s (uid %s) on VM %s of claim %s no longer exists; releasing it", sanitizeLog(a.PodNamespace), sanitizeLog(a.PodName), sanitizeLog(a.PodUID), sanitizeLog(node.InstanceID), sanitizeLog(node.ClaimName)) //nolint:gosec // sanitized
		s.DeleteOrphanedPod(ctx, a.PodNamespace, a.PodName, a.PodUID)
		released++
	}
	return released
}

// persistClaimPods writes the pods on a claim's VM to its record when they changed.
func (s *Server) persistClaimPods(node *CloudVMNode) {
	if node == nil || node.ClaimName == "" {
		return
	}
	s.nodePool.mu.RLock()
	pods := make(map[string]claimPod, len(node.ActivePods))
	for uid, a := range node.ActivePods {
		if !a.Completed {
			pods[uid] = claimPod{Namespace: a.PodNamespace, Name: a.PodName, GPUs: a.AssignedGPUs, TPUs: a.AssignedTPUs, DaemonSet: a.DaemonSet}
		}
	}
	namespace := node.Namespace
	s.nodePool.mu.RUnlock()

	s.claims.mu.Lock()
	defer s.claims.mu.Unlock()
	rec, ok := s.claims.store.get(node.ClaimName)
	if !ok || (namespace == rec.Namespace && maps.EqualFunc(pods, rec.Pods, func(a, b claimPod) bool {
		return a.Namespace == b.Namespace && a.Name == b.Name && a.DaemonSet == b.DaemonSet && slices.Equal(a.GPUs, b.GPUs) && slices.Equal(a.TPUs, b.TPUs)
	})) {
		return
	}
	updated := *rec
	updated.Pods, updated.Namespace = pods, namespace
	if err := s.claims.store.set(node.ClaimName, &updated, false); err != nil {
		log.Printf("[cloudvm] Warning: persisting pods of claim %s: %v", sanitizeLog(node.ClaimName), err) //nolint:gosec // sanitized
	}
}

// claimReady reports whether a claim's VM takes pods: its tunnel is connected or reconnecting within the grace, or
// without a tunnel its runner answered.
func (s *Server) claimReady(instanceID string) bool {
	if t := s.tunnelConfig(); t != nil {
		return t.connected(instanceID)
	}
	s.claims.mu.Lock()
	defer s.claims.mu.Unlock()
	return s.claims.ready[instanceID]
}

// claimUnreadySince returns since when a claim's VM has not taken pods: its creation, or with a tunnel the time it has
// been without one, so a VM whose tunnel dropped gets the full timeout to reconnect.
func (s *Server) claimUnreadySince(rec *claimRecord) time.Time {
	if t := s.tunnelConfig(); t != nil {
		return t.disconnectedSince(rec.InstanceID, rec.CreatedAt)
	}
	return rec.CreatedAt
}

// claimView renders a claim in the API shape.
func (s *Server) claimView(rec *claimRecord) nodeclaim.NodeClaim {
	state := nodeclaim.StateLaunching
	if s.claimReady(rec.InstanceID) {
		state = nodeclaim.StateRunning
	}
	s.nodePool.mu.RLock()
	node := s.nodePool.nodes[rec.InstanceID]
	interrupted := node != nil && node.SpinDownReason == SpinDownInterruption
	if node != nil && node.SpinDownReason == SpinDownCleanupFailed {
		state = nodeclaim.StateUnhealthy
	}
	var pods []nodeclaim.PodRef
	if node != nil {
		for uid, a := range node.ActivePods {
			pods = append(pods, nodeclaim.PodRef{Namespace: a.PodNamespace, Name: a.PodName, UID: uid})
		}
	}
	s.nodePool.mu.RUnlock()
	slices.SortFunc(pods, func(a, b nodeclaim.PodRef) int { return strings.Compare(a.UID, b.UID) })
	return nodeclaim.NodeClaim{
		Name:         rec.Request.Name,
		ProviderID:   rec.providerID(),
		InstanceType: rec.InstanceType,
		Region:       rec.Region,
		Zone:         rec.Zone,
		CapacityType: rec.CapacityType,
		State:        state,
		CreatedAt:    rec.CreatedAt,
		Labels:       rec.Request.Labels,
		Taints:       rec.Request.Taints,
		Capacity:     claimCapacity(rec),
		Interrupted:  interrupted,
		Pods:         pods,
	}
}

// claimCapacity renders a claim VM's hardware as Kubernetes resource quantities.
func claimCapacity(rec *claimRecord) map[string]string {
	return nodeclaim.Hardware{
		CPU:              rec.Capacity.CPU.String(),
		Memory:           rec.Capacity.Memory.String(),
		EphemeralStorage: rec.EphemeralStorage,
		GPUs:             rec.Capacity.GPUs,
		TPUs:             rec.Capacity.TPUs,
		Pods:             defaultClaimPods,
	}.Capacity()
}

// claimSpec builds the launch spec of a claim through the pod path, so launch annotations mean what they mean on a
// pod. The claim names one offering, so candidate types and regions are dropped, and its VM always runs the runner.
func (s *Server) claimSpec(req *nodeclaim.CreateRequest) (*InstanceSpec, error) {
	ann := make(map[string]string, len(req.Annotations)+3)
	for k, v := range req.Annotations {
		if strings.HasPrefix(k, "cloudvm.vk.io/") {
			ann[k] = v
		}
	}
	for _, k := range []string{"cloudvm.vk.io/candidate-instance-types", "cloudvm.vk.io/candidate-regions", "cloudvm.vk.io/region-selection", "cloudvm.vk.io/ami-mode", "cloudvm.vk.io/spot"} {
		delete(ann, k)
	}
	ann["cloudvm.vk.io/instance-type"] = req.InstanceType
	ann["cloudvm.vk.io/capacity-type"] = req.CapacityType
	ann["cloudvm.vk.io/region"] = req.Region
	namespace := req.Labels[nodeclaim.LabelNamespace]
	pod := &Pod{Metadata: PodMeta{Name: req.Name, Namespace: namespace, UID: claimPodUIDPrefix + req.Name, Annotations: ann}}
	spec, err := PodToInstanceSpec(pod, s.clusterTag, req.Region)
	if err != nil {
		return nil, err
	}
	spec.PodNamespace = namespace
	spec.Zone = req.Zone
	spec.NodeClaim = true
	s.applyServerDefaults(spec)
	return spec, nil
}

// createClaim launches the VM of a claim, or returns the claim already launched under its name.
func (s *Server) createClaim(ctx context.Context, req *nodeclaim.CreateRequest) (*claimRecord, bool, error) {
	if err := req.Validate(); err != nil {
		return nil, false, apiError(http.StatusBadRequest, nodeclaim.ReasonInvalidRequest, err.Error())
	}
	reg := s.claims
	reg.mu.Lock()
	if rec, ok := reg.store.get(req.Name); ok {
		reg.mu.Unlock()
		return rec, false, nil
	}
	if f, ok := reg.flights[req.Name]; ok {
		reg.mu.Unlock()
		select {
		case <-f.done:
			return f.record, false, f.err
		case <-ctx.Done():
			return nil, false, ctx.Err()
		}
	}
	key := offeringKey(req.InstanceType, req.Region, req.Zone, req.CapacityType)
	if until, ok := reg.unavailable[key]; ok && time.Now().Before(until) {
		reg.mu.Unlock()
		return nil, false, apiError(http.StatusServiceUnavailable, nodeclaim.ReasonInsufficientCapacity, "offering "+key+" recently had no capacity")
	}
	f := &claimFlight{done: make(chan struct{})}
	reg.flights[req.Name] = f
	delete(reg.deleted, req.Name)
	reg.mu.Unlock()

	f.record, f.err = s.launchClaim(ctx, req)
	reg.mu.Lock()
	delete(reg.flights, req.Name)
	if _, deleted := reg.deleted[req.Name]; deleted && f.err == nil {
		// The claim was deleted while its VM launched, so the VM has no claim to serve.
		rec := f.record
		f.record, f.err = nil, apiError(http.StatusConflict, nodeclaim.ReasonInvalidRequest, "claim "+req.Name+" was deleted while its VM launched")
		reg.mu.Unlock()
		close(f.done)
		s.terminateOrphanClaimVM(ctx, rec)
		return nil, false, f.err
	}
	if f.err == nil {
		reg.byInstance[f.record.InstanceID] = req.Name
		if err := reg.store.set(req.Name, f.record, false); err != nil {
			log.Printf("[cloudvm] ERROR persisting claim %s: %v", sanitizeLog(req.Name), err) //nolint:gosec // sanitized
		}
	} else if apiErr := (*claimAPIError)(nil); errors.As(f.err, &apiErr) && apiErr.reason == nodeclaim.ReasonInsufficientCapacity {
		reg.unavailable[key] = time.Now().Add(unavailableOfferingTTL)
	}
	reg.mu.Unlock()
	close(f.done)
	return f.record, f.err == nil, f.err
}

// launchClaim launches one VM for the claim and registers it in the node pool without a pod.
func (s *Server) launchClaim(ctx context.Context, req *nodeclaim.CreateRequest) (*claimRecord, error) {
	spec, err := s.claimSpec(req)
	if err != nil {
		return nil, apiError(http.StatusBadRequest, nodeclaim.ReasonInvalidRequest, err.Error())
	}
	prov := s.selectProvider(spec)
	if err := s.prepareLaunch(ctx, spec, prov); err != nil {
		return nil, err
	}
	launchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), s.LaunchTimeout())
	defer cancel()
	launchRequested := time.Now()
	inst, err := prov.Launch(launchCtx, spec)
	s.lifecycle.VMLaunched(lifecycletrace.ForNodeClaim(req.Name), spec, req.Name, launchRequested, inst, err)
	if err != nil {
		log.Printf("[cloudvm] Failed to launch VM for claim %s: %v", sanitizeLog(req.Name), err) //nolint:gosec // sanitized
		if isRegionFallbackError(err) {
			refusal := &claimAPIError{status: http.StatusServiceUnavailable, reason: nodeclaim.ReasonInsufficientCapacity, msg: err.Error()}
			if ClassifyAWSError(err) == "quota" || ClassifyGCPError(err) == "quota" {
				refusal.cause = nodeclaim.CauseQuota
			}
			return nil, refusal
		}
		if errors.Is(err, errUserDataTooLarge) {
			return nil, apiError(http.StatusBadRequest, nodeclaim.ReasonUserDataTooLarge, err.Error())
		}
		if errors.Is(err, errNamespaceFirewallMissing) || errors.Is(err, errNoInstanceTypeFits) {
			return nil, apiError(http.StatusBadRequest, nodeclaim.ReasonInvalidRequest, err.Error())
		}
		return nil, fmt.Errorf("launch failed: %w", err)
	}
	if s.tokenStore != nil && spec.RunnerAuthToken != "" {
		_ = s.tokenStore.Set(inst.ID, spec.RunnerAuthToken)
	}
	itype := inst.InstanceType
	if itype == "" {
		itype = spec.InstanceType
	}
	zone := inst.Metadata["availabilityZone"]
	if zone == "" {
		zone = req.Zone
	}
	rec := &claimRecord{
		Request:      *req,
		InstanceID:   inst.ID,
		ProviderID:   nodeclaim.FormatProviderID(string(inst.Provider), inst.Region, inst.ID),
		Provider:     inst.Provider,
		Region:       inst.Region,
		Zone:         zone,
		InstanceType: itype,
		CapacityType: req.CapacityType,
		IP:           inst.IP,
		Namespace:    spec.PodNamespace,
		CreatedAt:    time.Now(),
		Capacity:     s.resolveCapacity(ctx, prov, inst.Region, itype, ResourceVector{}),
	}
	rec.EphemeralStorage = cmp.Or(claimContainerStorage(ctx, prov, rec), plannedContainerStorage(ctx, prov, rec, spec))
	s.registerClaimNode(rec, spec.RunnerAuthToken)
	log.Printf("[cloudvm] Launched VM %s (%s, %s) for claim %s", sanitizeLog(inst.ID), sanitizeLog(itype), sanitizeLog(zone), sanitizeLog(req.Name)) //nolint:gosec // sanitized
	return rec, nil
}

// resolveCapacity returns the hardware of an instance type from the catalog, asking EC2 when the provider is AWS.
func (s *Server) resolveCapacity(ctx context.Context, prov Provider, region, itype string, fallback ResourceVector) ResourceVector {
	var describer InstanceTypeDescriber
	if p, ok := prov.(*AWSProvider); ok {
		if cl, err := p.getClient(ctx, region); err == nil {
			if d, ok := cl.(InstanceTypeDescriber); ok {
				describer = d
			}
		}
	}
	return GlobalCatalog.ResolveCapacity(ctx, describer, itype, fallback)
}

// claimContainerStorage returns the size of the disk a claim VM keeps its containers on, or "" when the provider cannot
// tell yet; the claim loop asks again until it can.
func claimContainerStorage(ctx context.Context, prov Provider, rec *claimRecord) string {
	sizer, ok := prov.(ContainerStorageSizer)
	if !ok {
		return ""
	}
	size, err := sizer.ContainerStorageSize(ctx, rec.Region, rec.InstanceID)
	if err != nil {
		log.Printf("[cloudvm] Reading the container storage size of VM %s of claim %s: %v", sanitizeLog(rec.InstanceID), sanitizeLog(rec.Request.Name), err) //nolint:gosec // sanitized
		return ""
	}
	return size.String()
}

// plannedContainerStorage returns the container storage size the claim's instance type gives a VM, or "" when the
// provider cannot plan it. It covers a launch whose instance cannot be read yet, such as one EC2 has not propagated.
func plannedContainerStorage(ctx context.Context, prov Provider, rec *claimRecord, spec *InstanceSpec) string {
	planner, ok := prov.(ContainerStoragePlanner)
	if !ok {
		return ""
	}
	size, err := planner.PlannedContainerStorage(ctx, rec.Region, rec.InstanceType, LaunchTemplateFact{ID: spec.LaunchTemplateID, Name: spec.LaunchTemplateName})
	if err != nil {
		log.Printf("[cloudvm] Planning the container storage size of %s for claim %s: %v", sanitizeLog(rec.InstanceType), sanitizeLog(rec.Request.Name), err) //nolint:gosec // sanitized
		return ""
	}
	return size.String()
}

// fillClaimContainerStorage records the container storage size of a claim that lacks it, such as one whose read failed
// at launch.
func (s *Server) fillClaimContainerStorage(ctx context.Context, rec *claimRecord) {
	size := claimContainerStorage(ctx, s.ProviderForType(rec.Provider), rec)
	if size == "" {
		return
	}
	s.claims.mu.Lock()
	defer s.claims.mu.Unlock()
	current, ok := s.claims.store.get(rec.Request.Name)
	if !ok || current.InstanceID != rec.InstanceID || current.EphemeralStorage != "" {
		return
	}
	updated := *current
	updated.EphemeralStorage = size
	if err := s.claims.store.set(rec.Request.Name, &updated, false); err != nil {
		log.Printf("[cloudvm] Warning: persisting the container storage size of claim %s: %v", sanitizeLog(rec.Request.Name), err) //nolint:gosec // sanitized
	}
}

// registerClaimNode puts a claim's VM into the node pool with the pods its record lists, and points their pod records
// at its instance so status and delete find them.
func (s *Server) registerClaimNode(rec *claimRecord, token string) {
	node := NewCloudVMNode(rec.InstanceID, rec.Provider, rec.Region, rec.IP, rec.Capacity)
	node.InstanceType = rec.InstanceType
	node.Namespace = rec.Namespace
	node.Spot = rec.CapacityType == nodeclaim.CapacityTypeSpot
	node.Zone = rec.Zone
	node.RunnerAuthToken = token
	node.ClaimName = rec.Request.Name
	node.CreatedAt = rec.CreatedAt
	for uid, p := range rec.Pods {
		node.AssignPod(&PodAllocation{PodUID: uid, PodNamespace: p.Namespace, PodName: p.Name, AssignedGPUs: p.GPUs, AssignedTPUs: p.TPUs, StartedAt: rec.CreatedAt, DaemonSet: p.DaemonSet})
	}
	inst := &Instance{
		ID: rec.InstanceID, Provider: rec.Provider, Region: rec.Region, InstanceType: rec.InstanceType, IP: rec.IP,
		State: StateRunning, CreatedAt: rec.CreatedAt,
		Metadata: map[string]string{"marketType": rec.CapacityType, "clusterTag": s.clusterTag, "nodeClaim": rec.Request.Name},
	}
	s.nodePool.RegisterNode(node)
	s.mu.Lock()
	for uid, p := range rec.Pods {
		s.instances[uid] = inst
		s.instances[p.Namespace+"/"+p.Name] = inst
	}
	s.mu.Unlock()
}

// RecoverClaims puts the VMs of persisted claims back into the node pool after a restart.
func (s *Server) RecoverClaims() {
	for _, rec := range s.claimRecords() {
		token := ""
		if s.tokenStore != nil {
			token = s.tokenStore.Get(rec.InstanceID)
		}
		s.registerClaimNode(rec, token)
		log.Printf("[cloudvm] Recovered VM %s of claim %s with %d pods", sanitizeLog(rec.InstanceID), sanitizeLog(rec.Request.Name), len(rec.Pods)) //nolint:gosec // sanitized
	}
}

// deleteClaim terminates a claim's VM and forgets the claim. It reports false when the claim is unknown. A claim
// whose VM is still launching is marked deleted, so its VM is terminated as soon as the launch returns.
func (s *Server) deleteClaim(ctx context.Context, name string) (bool, error) {
	s.claims.mu.Lock()
	if _, launching := s.claims.flights[name]; launching {
		s.claims.deleted[name] = time.Now()
		s.claims.mu.Unlock()
		return true, nil
	}
	rec, ok := s.claims.store.get(name)
	s.claims.mu.Unlock()
	if !ok {
		return false, nil
	}
	prov := s.ProviderForType(rec.Provider)
	if prov == nil {
		return true, fmt.Errorf("no %q provider is registered to terminate VM %s", rec.Provider, rec.InstanceID)
	}
	termCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), claimTerminateTimeout)
	defer cancel()
	if err := prov.Terminate(termCtx, rec.InstanceID); err != nil {
		return true, fmt.Errorf("terminate VM %s: %w", rec.InstanceID, err)
	}
	s.forgetClaim(rec)
	log.Printf("[cloudvm] Terminated VM %s of deleted claim %s", sanitizeLog(rec.InstanceID), sanitizeLog(name)) //nolint:gosec // sanitized
	return true, nil
}

// terminateOrphanClaimVM terminates the VM of a claim deleted during its launch.
func (s *Server) terminateOrphanClaimVM(ctx context.Context, rec *claimRecord) {
	s.claims.mu.Lock()
	deletedAt := s.claims.deleted[rec.Request.Name]
	s.claims.mu.Unlock()
	if prov := s.ProviderForType(rec.Provider); prov != nil {
		termCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), claimTerminateTimeout)
		defer cancel()
		if err := prov.Terminate(termCtx, rec.InstanceID); err != nil {
			// The reconciler terminates the tombstoned VM on a later pass.
			log.Printf("[cloudvm] Error terminating VM %s of a claim deleted during launch: %v", sanitizeLog(rec.InstanceID), err) //nolint:gosec // sanitized
		}
	}
	s.forgetClaim(rec)
	s.claims.mu.Lock()
	s.claims.deleted[rec.Request.Name] = deletedAt
	s.claims.mu.Unlock()
}

// forgetClaim drops a claim whose VM is gone, tombstones the VM, and remembers the name so pods still bound to the
// claim's Node are refused instead of launching VMs of their own.
func (s *Server) forgetClaim(rec *claimRecord) {
	s.forgetTerminatedInstance(rec.InstanceID)
	launchUID := claimPodUIDPrefix + rec.Request.Name
	s.mu.Lock()
	delete(s.launchTokens, launchUID)
	s.mu.Unlock()
	if s.tokenStore != nil {
		_ = s.tokenStore.Delete(launchUID)
	}
	// s.mu is never taken while holding s.claims.mu, because RecoverActiveInstances asks IsClaimInstance under s.mu.
	s.claims.mu.Lock()
	defer s.claims.mu.Unlock()
	_ = s.claims.store.delete(rec.Request.Name, true)
	delete(s.claims.byInstance, rec.InstanceID)
	s.claims.deleted[rec.Request.Name] = time.Now()
	delete(s.claims.ready, rec.InstanceID)
	delete(s.claims.heartbeats, rec.InstanceID)
}

// ObserveInventory forgets claims whose VMs the cloud no longer runs, so their NodeClaims disappear from the API.
// Claims in regions whose inventory failed, and claims younger than the launch grace period, are kept.
func (s *Server) ObserveInventory(active map[string]*Instance, failedRegions map[string]error) {
	for _, rec := range s.claimRecords() {
		if _, failed := failedRegions[rec.Region]; failed || time.Since(rec.CreatedAt) < 2*time.Minute {
			continue
		}
		inst, alive := active[rec.InstanceID]
		if alive && inst != nil && inst.State != StateTerminated && inst.State != StateFailed {
			if inst.IP != "" && inst.IP != rec.IP {
				if node, ok := s.nodePool.Node(rec.InstanceID); ok {
					node.mu.Lock()
					node.IP = inst.IP
					node.mu.Unlock()
				}
			}
			continue
		}
		log.Printf("[cloudvm] VM %s of claim %s is gone from the cloud; forgetting the claim", sanitizeLog(rec.InstanceID), sanitizeLog(rec.Request.Name)) //nolint:gosec // sanitized
		s.forgetClaim(rec)
	}
}

// RunClaimSync runs the claim loop until ctx is cancelled.
func (s *Server) RunClaimSync(ctx context.Context) {
	ticker := time.NewTicker(claimSyncInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.SyncClaims(ctx)
		}
	}
}

// SyncClaims terminates claim VMs that stayed unready past the timeout, records the container storage size of claims that lack it,
// calls the runner of every claim VM that has not heard from the plugin for a minute, which marks VMs without a tunnel
// ready and keeps idle VMs alive, releases the pods on each VM that no longer exist and persists the rest, and prunes
// expired memory of deleted claims and unavailable offerings.
func (s *Server) SyncClaims(ctx context.Context) {
	now := time.Now()
	readyTimeout := claimReadyTimeout
	if t := s.tunnelConfig(); t != nil {
		readyTimeout = t.ConnectTimeout
	}
	sem := make(chan struct{}, claimProbeConcurrency)
	var wg sync.WaitGroup
	for _, rec := range s.claimRecords() {
		node, ok := s.nodePool.Node(rec.InstanceID)
		if !ok {
			continue
		}
		s.releaseDepartedClaimPods(ctx, node)
		s.persistClaimPods(node)
		if rec.EphemeralStorage == "" {
			s.fillClaimContainerStorage(ctx, rec)
		}
		ready := s.claimReady(rec.InstanceID)
		if !ready && now.Sub(s.claimUnreadySince(rec)) > readyTimeout {
			log.Printf("[cloudvm] VM %s of claim %s has not been ready for %v; terminating it", sanitizeLog(rec.InstanceID), sanitizeLog(rec.Request.Name), readyTimeout) //nolint:gosec // sanitized
			if _, err := s.deleteClaim(ctx, rec.Request.Name); err != nil {
				log.Printf("[cloudvm] Error terminating unready claim VM %s: %v", sanitizeLog(rec.InstanceID), err) //nolint:gosec // sanitized
			}
			continue
		}
		s.claims.mu.Lock()
		due := now.Sub(s.claims.heartbeats[rec.InstanceID]) >= claimHeartbeatInterval
		s.claims.mu.Unlock()
		if !due {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			probeCtx, cancel := context.WithTimeout(ctx, reuseProbeTimeout)
			stats, err := s.runner.GetHostStats(probeCtx, node)
			cancel()
			if err != nil {
				return
			}
			if stats != nil {
				GlobalTransferMeter.Record(TransferVM{
					Provider: string(node.Provider), Region: node.Region, InstanceType: node.InstanceType, InstanceID: node.InstanceID,
					NodePool: rec.Request.Labels[karpenterNodePoolLabel],
				}, stats.Traffic)
			}
			s.claims.mu.Lock()
			s.claims.heartbeats[rec.InstanceID] = time.Now()
			s.claims.ready[rec.InstanceID] = true
			s.claims.mu.Unlock()
		}()
	}
	wg.Wait()
	s.claims.mu.Lock()
	for name, at := range s.claims.deleted {
		if now.Sub(at) > claimDeletedMemory {
			delete(s.claims.deleted, name)
		}
	}
	for key, until := range s.claims.unavailable {
		if now.After(until) {
			delete(s.claims.unavailable, key)
		}
	}
	s.claims.mu.Unlock()
}

// claimAPIError is an API failure with the HTTP status and reason the contract names.
type claimAPIError struct {
	status int
	reason string
	cause  string
	msg    string
}

func (e *claimAPIError) Error() string { return e.msg }

func apiError(status int, reason, msg string) error {
	return &claimAPIError{status: status, reason: reason, msg: msg}
}

func writeClaimJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeClaimError(w http.ResponseWriter, err error) {
	var apiErr *claimAPIError
	if errors.As(err, &apiErr) {
		writeClaimJSON(w, apiErr.status, nodeclaim.ErrorResponse{Reason: apiErr.reason, Cause: apiErr.cause, Message: apiErr.msg})
		return
	}
	writeClaimJSON(w, http.StatusInternalServerError, nodeclaim.ErrorResponse{Reason: nodeclaim.ReasonInternal, Message: err.Error()})
}

// handleNodeClaims serves GET /v1/nodeclaims (optionally ?providerID=) and POST /v1/nodeclaims.
func (s *Server) handleNodeClaims(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		if err := s.claims.loadErr; err != nil {
			writeClaimError(w, fmt.Errorf("claim store is unreadable: %w", err))
			return
		}
		providerID := r.URL.Query().Get("providerID")
		items := make([]nodeclaim.NodeClaim, 0)
		for _, rec := range s.claimRecords() {
			view := s.claimView(rec)
			if providerID == "" || view.ProviderID == providerID {
				items = append(items, view)
			}
		}
		slices.SortFunc(items, func(a, b nodeclaim.NodeClaim) int { return strings.Compare(a.Name, b.Name) })
		writeClaimJSON(w, http.StatusOK, nodeclaim.List{Items: items})
	case http.MethodPost:
		if err := s.claims.loadErr; err != nil {
			writeClaimError(w, fmt.Errorf("claim store is unreadable: %w", err))
			return
		}
		var req nodeclaim.CreateRequest
		if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20)).Decode(&req); err != nil {
			writeClaimError(w, apiError(http.StatusBadRequest, nodeclaim.ReasonInvalidRequest, "invalid JSON: "+err.Error()))
			return
		}
		rec, created, err := s.createClaim(r.Context(), &req)
		if err != nil {
			writeClaimError(w, err)
			return
		}
		status := http.StatusOK
		if created {
			status = http.StatusCreated
		}
		writeClaimJSON(w, status, s.claimView(rec))
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleNodeClaim serves GET and DELETE /v1/nodeclaims/{name}.
func (s *Server) handleNodeClaim(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	switch r.Method {
	case http.MethodGet:
		rec, ok := s.claims.store.get(name)
		if !ok {
			writeClaimError(w, apiError(http.StatusNotFound, nodeclaim.ReasonNotFound, "no claim named "+name))
			return
		}
		writeClaimJSON(w, http.StatusOK, s.claimView(rec))
	case http.MethodDelete:
		found, err := s.deleteClaim(r.Context(), name)
		switch {
		case err != nil:
			writeClaimError(w, err)
		case !found:
			writeClaimError(w, apiError(http.StatusNotFound, nodeclaim.ReasonNotFound, "no claim named "+name))
		default:
			w.WriteHeader(http.StatusAccepted)
		}
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
