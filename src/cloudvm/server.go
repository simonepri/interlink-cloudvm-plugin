// Package main implements the interlink-provider-cloudvm daemon.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func generateToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("crypto rand failed: %w", err)
	}
	return hex.EncodeToString(b), nil
}

// Server implements the InterLink REST API for the cloudvm provider plugin.
type inFlightCreate struct {
	done  chan struct{}
	token string
	resp  *CreateStruct
	err   error
}

type Server struct {
	mu              sync.RWMutex
	providers       map[ProviderType]Provider
	defaultProv     Provider
	instances       map[string]*Instance // key: UID and namespace/podName -> *Instance
	clusterTag      string
	defaultRegion   string
	reconciler      *Reconciler
	nodePool        *NodePool
	runner          ContainerRunner
	spotTicker      *SpotPriceTicker
	tokenStore      *TokenStore
	completedStore  *CompletedStore
	tombstoneStore  *TombstoneStore
	tombstones      map[string]time.Time                 // podUID or instanceID -> deletion timestamp
	activeSightings map[string]time.Time                 // podUID -> last seen in /status or /create
	statusPolled    bool                                 // set true when first /status query is received
	launchTokens    map[string]string                    // podUID -> runnerAuthToken (reused across retries)
	completedPods   map[string]*ContainerTerminatedState // podUID -> terminal state (retained until /delete)
	inFlightCreates map[string]*inFlightCreate           // podUID -> in-flight single flight tracker
}

// NewServer initializes an InterLink Server with a default provider.
func NewServer(defaultProvider Provider, clusterTag string) *Server {
	tokenPath := os.Getenv("TOKEN_STORE_PATH")
	if tokenPath == "" {
		tokenPath = filepath.Join(os.TempDir(), fmt.Sprintf("cloudvm-tokens-%s.json", clusterTag))
	}
	tokStore, err := NewTokenStore(tokenPath)
	if err != nil {
		log.Printf("[cloudvm] ERROR loading token store: %v", err)
	}
	completedPath := os.Getenv("COMPLETED_STORE_PATH")
	if completedPath == "" {
		completedPath = filepath.Join(os.TempDir(), fmt.Sprintf("cloudvm-completed-pods-%s.json", clusterTag))
	}
	compStore, err := NewCompletedStore(completedPath)
	if err != nil {
		log.Printf("[cloudvm] ERROR loading completed store: %v", err)
	}
	tombstonePath := os.Getenv("TOMBSTONE_STORE_PATH")
	if tombstonePath == "" {
		tombstonePath = filepath.Join(os.TempDir(), fmt.Sprintf("cloudvm-tombstones-%s.json", clusterTag))
	}
	tombStore, err := NewTombstoneStore(tombstonePath)
	if err != nil {
		log.Printf("[cloudvm] ERROR loading tombstone store: %v", err)
	}
	s := &Server{
		providers:       make(map[ProviderType]Provider),
		defaultProv:     defaultProvider,
		instances:       make(map[string]*Instance),
		clusterTag:      clusterTag,
		defaultRegion:   "us-east-1",
		nodePool:        NewNodePool(60 * time.Second),
		runner:          NewMockContainerRunner(),
		tokenStore:      tokStore,
		completedStore:  compStore,
		tombstoneStore:  tombStore,
		tombstones:      make(map[string]time.Time),
		activeSightings: make(map[string]time.Time),
		launchTokens:    make(map[string]string),
		completedPods:   make(map[string]*ContainerTerminatedState),
		inFlightCreates: make(map[string]*inFlightCreate),
	}
	if tombStore != nil {
		for k, v := range tombStore.GetAll() {
			s.tombstones[k] = v
		}
	}
	return s
}

// SetTokenStore overrides the token store for runner credentials.
func (s *Server) SetTokenStore(ts *TokenStore) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tokenStore = ts
}

// SetCompletedStore overrides the completed store for terminal pod execution records.
func (s *Server) SetCompletedStore(cs *CompletedStore) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.completedStore = cs
}

// SetTombstoneStore overrides the tombstone store for deleted pod records.
func (s *Server) SetTombstoneStore(ts *TombstoneStore) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tombstoneStore = ts
	if ts != nil {
		for k, v := range ts.GetAll() {
			s.tombstones[k] = v
		}
	}
}

func (s *Server) isTombstonedLocked(id string) bool {
	if id == "" {
		return false
	}
	if !s.tombstones[id].IsZero() {
		return true
	}
	if s.tombstoneStore != nil && s.tombstoneStore.Has(id) {
		return true
	}
	return false
}

// HasUnknownCoTenants reports whether the node has unverified co-tenant workloads.
func (s *Server) HasUnknownCoTenants(instanceID string) bool {
	if s.nodePool == nil {
		return false
	}
	return s.nodePool.HasUnknownCoTenants(instanceID)
}

// IsTombstoned reports whether the instance or its associated pod has been explicitly deleted.
func (s *Server) IsTombstoned(inst *Instance) bool {
	if inst == nil {
		return false
	}
	if s.nodePool != nil {
		if s.nodePool.HasActivePods(inst.ID) || s.nodePool.HasUnknownCoTenants(inst.ID) {
			return false
		}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.isTombstonedLocked(inst.ID) {
		return true
	}
	if inst.Metadata != nil {
		if podUID := inst.Metadata["podUID"]; podUID != "" && s.isTombstonedLocked(podUID) {
			return true
		}
	}
	return false
}

// SetContainerRunner overrides the container runner (e.g. HTTPContainerRunner).
func (s *Server) SetContainerRunner(r ContainerRunner) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.runner = r
}

// SetSpotTicker binds the live spot price ticker to export /prices and /metrics.
func (s *Server) SetSpotTicker(t *SpotPriceTicker) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.spotTicker = t
}

// NodePool returns the underlying multi-cloud node pool.
func (s *Server) NodePool() *NodePool {
	return s.nodePool
}

// SetDefaultRegion sets the baseline cloud region for pods without explicit region annotations.
func (s *Server) SetDefaultRegion(region string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if region != "" {
		s.defaultRegion = region
	}
}

// RegisterProvider registers an alternative provider backend.
func (s *Server) RegisterProvider(pt ProviderType, p Provider) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.providers[pt] = p
}

// SetReconciler binds the background reconciler.
func (s *Server) SetReconciler(r *Reconciler) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reconciler = r
	if r != nil {
		r.SetOnTerminated(func(id string) {
			if s.tokenStore != nil {
				_ = s.tokenStore.Delete(id)
			}
		})
	}
}

// ProviderForType returns the provider registered for the given provider type, or the default provider.
func (s *Server) ProviderForType(pt ProviderType) Provider {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if p, ok := s.providers[pt]; ok {
		return p
	}
	return s.defaultProv
}

// TerminateIdleNode cleanly terminates an idle VM using its specific cloud provider
// and prunes associated runner tokens and in-memory instance records.
func (s *Server) TerminateIdleNode(ctx context.Context, node *CloudVMNode) {
	if node == nil {
		return
	}
	prov := s.ProviderForType(node.Provider)
	cleanID := sanitizeLog(node.InstanceID)
	log.Printf("[cloudvm] Terminating expired idle node %s", cleanID) //nolint:gosec
	if prov != nil {
		if err := prov.Terminate(ctx, node.InstanceID); err != nil {
			log.Printf("[cloudvm] Error terminating idle instance %s: %v", cleanID, err) //nolint:gosec // sanitized
			return
		}
	}
	if s.tokenStore != nil {
		_ = s.tokenStore.Delete(node.InstanceID)
	}
	s.mu.Lock()
	for k, inst := range s.instances {
		if inst != nil && inst.ID == node.InstanceID {
			delete(s.instances, k)
		}
	}
	s.mu.Unlock()
}

// Routes returns the standard InterLink HTTP routing handler.
func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	// Standard InterLink REST provider endpoints
	mux.HandleFunc("/create", s.handleCreate)
	mux.HandleFunc("/delete", s.handleDelete)
	mux.HandleFunc("/status", s.handleStatus)
	mux.HandleFunc("/getLogs", s.handleGetLogs)
	mux.HandleFunc("/healthz", s.handleHealthz)

	// Deprecated / compat endpoints
	mux.HandleFunc("/pods/create", s.handleCreate)
	mux.HandleFunc("/pods/delete", s.handleDelete)
	mux.HandleFunc("/pods/status", s.handleStatus)

	// Spot price ticker endpoints (when ticker is enabled)
	mux.HandleFunc("/prices", func(w http.ResponseWriter, r *http.Request) {
		s.mu.RLock()
		ticker := s.spotTicker
		s.mu.RUnlock()
		if ticker == nil {
			http.Error(w, "spot ticker not enabled", http.StatusNotFound)
			return
		}
		ticker.Routes().ServeHTTP(w, r)
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		s.mu.RLock()
		ticker := s.spotTicker
		s.mu.RUnlock()
		if ticker == nil {
			http.Error(w, "spot ticker not enabled", http.StatusNotFound)
			return
		}
		ticker.Routes().ServeHTTP(w, r)
	})

	return mux
}

func (s *Server) selectProvider(spec *InstanceSpec) Provider {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check explicit annotation override
	if spec != nil && spec.Provider != "" {
		if p, ok := s.providers[ProviderType(strings.ToLower(spec.Provider))]; ok {
			return p
		}
	}
	return s.defaultProv
}

func (s *Server) handleCreate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("failed to read request: %v", err), http.StatusBadRequest)
		return
	}

	pod, err := parsePodFromPayload(body)
	if err != nil {
		http.Error(w, fmt.Sprintf("invalid pod payload: %v", err), http.StatusBadRequest)
		return
	}

	var flight *inFlightCreate
	var finalResp *CreateStruct
	var finalErr error

	spec, err := PodToInstanceSpec(pod, s.clusterTag, s.defaultRegion)
	if err != nil {
		finalErr = err
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Single-flight in-flight create check and idempotency under lock
	s.mu.Lock()
	if spec.PodUID != "" {
		s.activeSightings[spec.PodUID] = time.Now()
		delete(s.tombstones, spec.PodUID)
		if inFlightTracker, exists := s.inFlightCreates[spec.PodUID]; exists {
			s.mu.Unlock()
			select {
			case <-inFlightTracker.done:
				if inFlightTracker.err != nil {
					http.Error(w, inFlightTracker.err.Error(), http.StatusInternalServerError)
					return
				}
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(inFlightTracker.resp)
				return
			case <-r.Context().Done():
				http.Error(w, r.Context().Err().Error(), http.StatusGatewayTimeout)
				return
			}
		}

		if s.completedStore != nil {
			if rec := s.completedStore.Get(spec.PodUID); rec != nil {
				s.mu.Unlock()
				podJID := rec.PodJID
				if podJID == "" {
					podJID = fmt.Sprintf("cvm-%s", spec.PodUID)
				}
				w.WriteHeader(http.StatusOK)
				_ = json.NewEncoder(w).Encode(CreateStruct{
					PodUID: spec.PodUID,
					PodJID: podJID,
				})
				return
			}
		}
		if term, done := s.completedPods[spec.PodUID]; done && term != nil {
			s.mu.Unlock()
			w.WriteHeader(http.StatusOK)
			_ = json.NewEncoder(w).Encode(CreateStruct{
				PodUID: spec.PodUID,
				PodJID: fmt.Sprintf("cvm-%s", spec.PodUID),
			})
			return
		}
	}

	var existingInst *Instance
	if spec.PodUID != "" {
		existingInst = s.instances[spec.PodUID]
	} else {
		existingInst = s.instances[fmt.Sprintf("%s/%s", spec.PodNamespace, spec.PodName)]
	}
	if existingInst != nil {
		podJID := existingInst.ID
		if spec.PodUID != "" {
			podJID = fmt.Sprintf("%s:%s", existingInst.ID, spec.PodUID)
		}
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(CreateStruct{
			PodUID: spec.PodUID,
			PodJID: podJID,
		})
		return
	}

	if spec.PodUID != "" {
		flight = &inFlightCreate{done: make(chan struct{})}
		s.inFlightCreates[spec.PodUID] = flight
	}
	s.mu.Unlock()

	if flight != nil {
		defer func() {
			s.mu.Lock()
			delete(s.inFlightCreates, spec.PodUID)
			s.mu.Unlock()
			flight.resp = finalResp
			flight.err = finalErr
			close(flight.done)
		}()
	}

	prov := s.selectProvider(spec)
	reqVec := ResourceVectorFromPod(pod)

	var cpuLimitMilli int64
	var memLimitBytes int64
	if len(pod.Spec.Containers) > 0 {
		c := pod.Spec.Containers[0]
		if q, ok := c.Resources.Limits[string(corev1.ResourceCPU)]; ok {
			if parsed, err := resource.ParseQuantity(q); err == nil {
				cpuLimitMilli = parsed.MilliValue()
			}
		}
		if q, ok := c.Resources.Limits[string(corev1.ResourceMemory)]; ok {
			if parsed, err := resource.ParseQuantity(q); err == nil {
				memLimitBytes = parsed.Value()
			}
		}
	}

	// 1. Try to bin-pack into an existing warm node
	isSpot := spec.MarketType != "on-demand"
	warmNode, assignedGPUs, assignedTPUs, err := s.nodePool.Allocate(reqVec, spec.Region, spec.PodUID, spec.PodNamespace, spec.PodName, isSpot)
	if err == nil && warmNode != nil {
		log.Printf("[cloudvm] Bin-pack hit! Slicing pod %s/%s into warm node %s (GPUs=%v, TPUs=%v)",
			spec.PodNamespace, spec.PodName, warmNode.InstanceID, assignedGPUs, assignedTPUs)

		// Start container via runner
		cName := "worker"
		if len(pod.Spec.Containers) > 0 && pod.Spec.Containers[0].Name != "" {
			cName = pod.Spec.Containers[0].Name
		}
		_ = s.runner.StartContainer(r.Context(), warmNode, &StartContainerRequest{
			PodUID:        spec.PodUID,
			Namespace:     spec.PodNamespace,
			PodName:       spec.PodName,
			ContainerName: cName,
			Image:         spec.TargetContainerImage,
			Command:       spec.Command,
			Args:          spec.Args,
			Env:           spec.Env,
			CPUMillicores: cpuLimitMilli,
			MemoryBytes:   memLimitBytes,
			AssignedGPUs:  assignedGPUs,
			AssignedTPUs:  assignedTPUs,
			RunAsUser:     spec.RunAsUser,
			RunAsGroup:    spec.RunAsGroup,
		})

		marketType := "on-demand"
		if warmNode.Spot {
			marketType = "spot"
		}

		s.mu.Lock()
		inst := &Instance{
			ID:        warmNode.InstanceID,
			Provider:  warmNode.Provider,
			Region:    warmNode.Region,
			IP:        warmNode.MeshIP,
			State:     StateRunning,
			CreatedAt: warmNode.CreatedAt,
			Metadata: map[string]string{
				"podUID":      spec.PodUID,
				"podName":     spec.PodName,
				"namespace":   spec.PodNamespace,
				"marketType":  marketType,
				"clusterTag":  s.clusterTag,
				"runnerToken": warmNode.RunnerAuthToken,
			},
		}
		key := fmt.Sprintf("%s/%s", spec.PodNamespace, spec.PodName)
		s.instances[key] = inst
		if spec.PodUID != "" {
			s.instances[spec.PodUID] = inst
		}
		s.mu.Unlock()

		var podJID string
		if spec.PodUID != "" {
			podJID = fmt.Sprintf("%s:%s", warmNode.InstanceID, spec.PodUID)
		} else {
			podJID = warmNode.InstanceID
		}

		finalResp = &CreateStruct{
			PodUID: spec.PodUID,
			PodJID: podJID,
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(finalResp)
		return
	}

	// 2. Bin-pack miss: Launch a new cloud VM
	log.Printf("[cloudvm] Bin-pack miss. Launching new VM for pod %s/%s (type=%s, gpus=%d, region=%s)...",
		spec.PodNamespace, spec.PodName, spec.InstanceType, spec.NumGPUs, spec.Region)

	s.mu.Lock()
	tok := ""
	if spec.PodUID != "" && s.tokenStore != nil {
		tok = s.tokenStore.Get(spec.PodUID)
	}
	if tok == "" && spec.PodUID != "" {
		tok = s.launchTokens[spec.PodUID]
	}
	if tok == "" {
		var err error
		tok, err = generateToken()
		if err != nil {
			s.mu.Unlock()
			finalErr = err
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if spec.PodUID != "" {
			s.launchTokens[spec.PodUID] = tok
			if s.tokenStore != nil {
				_ = s.tokenStore.Set(spec.PodUID, tok)
			}
		}
	}
	if flight != nil {
		flight.token = tok
	}
	s.mu.Unlock()
	spec.RunnerAuthToken = tok

	var preAssignedGPUs []int
	for i := 0; i < spec.NumGPUs; i++ {
		preAssignedGPUs = append(preAssignedGPUs, i)
	}
	spec.AssignedGPUs = preAssignedGPUs

	// Detach from client HTTP request context so client timeouts or cancellations do not abort in-flight VM launch
	launchCtx, launchCancel := context.WithTimeout(context.WithoutCancel(r.Context()), 5*time.Minute)
	defer launchCancel()

	inst, err := prov.Launch(launchCtx, spec)
	if err != nil {
		log.Printf("[cloudvm] Failed to launch VM for %s: %v", spec.PodName, err)
		finalErr = fmt.Errorf("launch failed: %w", err)
		http.Error(w, finalErr.Error(), http.StatusInternalServerError)
		return
	}
	if inst.Metadata == nil {
		inst.Metadata = make(map[string]string)
	}
	inst.Metadata["marketType"] = spec.MarketType
	if s.tokenStore != nil && spec.RunnerAuthToken != "" {
		_ = s.tokenStore.Set(inst.ID, spec.RunnerAuthToken)
	}

	// Register new node in pool with hardware capacity from instance type catalog
	itype := inst.InstanceType
	if itype == "" {
		itype = spec.InstanceType
	}
	var describer InstanceTypeDescriber
	if p, ok := prov.(*AWSProvider); ok {
		if cl, err := p.getClient(r.Context(), spec.Region); err == nil {
			if d, ok := cl.(InstanceTypeDescriber); ok {
				describer = d
			}
		}
	}
	nodeCapacity := GlobalCatalog.ResolveCapacity(r.Context(), describer, itype, reqVec)

	newNode := NewCloudVMNode(inst.ID, inst.Provider, inst.Region, inst.IP, nodeCapacity)
	newNode.Namespace = spec.PodNamespace
	newNode.RunnerAuthToken = spec.RunnerAuthToken
	newNode.Spot = spec.MarketType != "on-demand"
	if spec.AMIMode == "dlami" {
		newNode.Cordoned = true // Native DLAMI runs single-tenant workload; exclude from bin-packing
	}
	gpus, tpus := s.nodePool.RegisterAndAllocate(newNode, reqVec, spec.PodUID, spec.PodNamespace, spec.PodName)

	cName := "worker"
	if len(pod.Spec.Containers) > 0 && pod.Spec.Containers[0].Name != "" {
		cName = pod.Spec.Containers[0].Name
	}
	_ = s.runner.StartContainer(r.Context(), newNode, &StartContainerRequest{
		PodUID:        spec.PodUID,
		Namespace:     spec.PodNamespace,
		PodName:       spec.PodName,
		ContainerName: cName,
		Image:         spec.TargetContainerImage,
		Command:       spec.Command,
		Args:          spec.Args,
		Env:           spec.Env,
		CPUMillicores: reqVec.CPU.MilliValue(),
		MemoryBytes:   reqVec.Memory.Value(),
		AssignedGPUs:  gpus,
		AssignedTPUs:  tpus,
		RunAsUser:     spec.RunAsUser,
		RunAsGroup:    spec.RunAsGroup,
	})

	s.mu.Lock()
	if inst.Metadata == nil {
		inst.Metadata = make(map[string]string)
	}
	if spec.PodUID != "" {
		inst.Metadata["podUID"] = spec.PodUID
	}
	marketType := "on-demand"
	if newNode.Spot {
		marketType = "spot"
	}
	inst.Metadata["marketType"] = marketType
	key := fmt.Sprintf("%s/%s", spec.PodNamespace, spec.PodName)
	s.instances[key] = inst
	if spec.PodUID != "" {
		s.instances[spec.PodUID] = inst
	}
	s.mu.Unlock()

	var podJID string
	if spec.PodUID != "" {
		podJID = fmt.Sprintf("%s:%s", inst.ID, spec.PodUID)
	} else {
		podJID = inst.ID
	}

	finalResp = &CreateStruct{
		PodUID: spec.PodUID,
		PodJID: podJID,
	}
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(finalResp)
}

func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	pod, err := parsePodFromPayload(body)
	if err != nil {
		http.Error(w, "invalid pod JSON", http.StatusBadRequest)
		return
	}

	podName := pod.Metadata.Name
	ns := pod.Metadata.Namespace
	if ns == "" {
		ns = "default"
	}
	key := fmt.Sprintf("%s/%s", ns, podName)
	s.mu.Lock()
	inst, exists := s.instances[pod.Metadata.UID]
	if !exists || inst == nil {
		inst, exists = s.instances[key]
	}
	if !exists || inst == nil {
		inst, exists = s.instances[podName]
	}

	uid := pod.Metadata.UID
	if uid == "" && exists && inst != nil && inst.Metadata != nil {
		uid = inst.Metadata["podUID"]
	}
	if uid == "" {
		uid = key
	}

	now := time.Now()
	s.tombstones[uid] = now
	delete(s.activeSightings, uid)
	if s.tombstoneStore != nil {
		_ = s.tombstoneStore.Set(uid)
	}
	if pod.Metadata.UID != "" {
		s.tombstones[pod.Metadata.UID] = now
		delete(s.activeSightings, pod.Metadata.UID)
		if s.tombstoneStore != nil {
			_ = s.tombstoneStore.Set(pod.Metadata.UID)
		}
	}

	if pod.Metadata.UID != "" {
		delete(s.instances, pod.Metadata.UID)
		delete(s.completedPods, pod.Metadata.UID)
		delete(s.launchTokens, pod.Metadata.UID)
		if s.tokenStore != nil {
			_ = s.tokenStore.Delete(pod.Metadata.UID)
		}
		if s.completedStore != nil {
			_ = s.completedStore.Delete(pod.Metadata.UID)
		}
	}
	delete(s.instances, key)
	delete(s.instances, podName)
	delete(s.completedPods, key)
	delete(s.completedPods, podName)
	s.mu.Unlock()

	// Release resources from NodePool
	remainingPods := 0
	foundInPool := false
	if node, _, found := s.nodePool.FindPodNode(uid); found {
		foundInPool = true
		_ = s.runner.StopContainer(r.Context(), node, uid)
		remainingPods = s.nodePool.Release(uid)
		log.Printf("[cloudvm] Released resources for pod %s from node %s (remaining active pods: %d)", uid, node.InstanceID, remainingPods)
		if s.nodePool.HasUnknownCoTenants(node.InstanceID) {
			log.Printf("[cloudvm] Node %s has unknown co-tenants because container listing previously failed. Retaining VM.", node.InstanceID)
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	if !foundInPool {
		// A lookup miss for a pod in node pool must not lead to terminating the VM
		if inst != nil && s.nodePool != nil && (s.nodePool.HasActivePods(inst.ID) || s.nodePool.HasNode(inst.ID)) {
			log.Printf("[cloudvm] Pod %s not allocated to node %s in node pool. Retaining VM.", uid, inst.ID)
			w.WriteHeader(http.StatusOK)
			return
		}
	}

	if inst != nil && inst.ID != "" && remainingPods == 0 {
		s.mu.Lock()
		s.tombstones[inst.ID] = time.Now()
		if s.tombstoneStore != nil {
			_ = s.tombstoneStore.Set(inst.ID)
		}
		s.mu.Unlock()
	}

	if !exists || inst == nil || remainingPods > 0 {
		w.WriteHeader(http.StatusOK)
		return
	}

	prov := s.ProviderForType(inst.Provider)

	s.nodePool.UnregisterNode(inst.ID)
	if s.tokenStore != nil {
		_ = s.tokenStore.Delete(inst.ID)
	}
	if err := prov.Terminate(r.Context(), inst.ID); err != nil {
		log.Printf("[cloudvm] Error terminating instance %s: %v", inst.ID, err)
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var pods []Pod
	body, _ := io.ReadAll(r.Body)
	if len(body) > 0 {
		var retrievedList []struct {
			Pod Pod `json:"pod"`
		}
		if err := json.Unmarshal(body, &retrievedList); err == nil && len(retrievedList) > 0 && retrievedList[0].Pod.Metadata.Name != "" {
			for _, item := range retrievedList {
				pods = append(pods, item.Pod)
			}
		} else if err := json.Unmarshal(body, &pods); err != nil {
			var single Pod
			if err2 := json.Unmarshal(body, &single); err2 == nil && single.Metadata.Name != "" {
				pods = append(pods, single)
			}
		}
	}

	s.mu.Lock()
	s.statusPolled = true
	s.mu.Unlock()

	if len(pods) > 0 {
		s.mu.Lock()
		now := time.Now()
		for _, p := range pods {
			if p.Metadata.UID != "" {
				s.activeSightings[p.Metadata.UID] = now
			}
		}
		// Prune expired sightings older than 10 minutes
		for uid, seenAt := range s.activeSightings {
			if now.Sub(seenAt) > 10*time.Minute {
				delete(s.activeSightings, uid)
			}
		}
		s.mu.Unlock()
	}

	type statusTarget struct {
		name  string
		ns    string
		uid   string
		jid   string
		inst  *Instance
		node  *CloudVMNode
		alloc PodAllocation
	}

	var targets []statusTarget

	s.mu.RLock()
	if len(pods) == 0 {
		seen := make(map[string]bool)
		for key, inst := range s.instances {
			if inst == nil || seen[inst.ID] {
				continue
			}
			seen[inst.ID] = true

			ns := "default"
			name := key
			uid := ""
			if inst.Metadata != nil {
				if n := inst.Metadata["podName"]; n != "" {
					name = n
				}
				if n := inst.Metadata["namespace"]; n != "" {
					ns = n
				}
				uid = inst.Metadata["podUID"]
			} else {
				parts := strings.SplitN(key, "/", 2)
				if len(parts) == 2 {
					ns = parts[0]
					name = parts[1]
				}
			}

			pods = append(pods, Pod{
				Metadata: PodMeta{
					Name:      name,
					Namespace: ns,
					UID:       uid,
				},
			})
		}
	}

	for _, pod := range pods {
		name := pod.Metadata.Name
		ns := pod.Metadata.Namespace
		if ns == "" {
			ns = "default"
		}
		key := fmt.Sprintf("%s/%s", ns, name)
		var inst *Instance
		var exists bool
		if pod.Metadata.UID != "" {
			inst, exists = s.instances[pod.Metadata.UID]
		}
		if !exists || inst == nil {
			inst, exists = s.instances[key]
		}
		if !exists || inst == nil {
			inst, exists = s.instances[name]
		}

		uid := pod.Metadata.UID
		if uid == "" && exists && inst != nil && inst.Metadata != nil {
			uid = inst.Metadata["podUID"]
		}

		jid := ""
		var node *CloudVMNode
		var alloc PodAllocation
		if exists && inst != nil {
			if uid != "" {
				jid = fmt.Sprintf("%s:%s", inst.ID, uid)
				if n, a, found := s.nodePool.FindPodNode(uid); found {
					node = n
					alloc = a
				}
			} else {
				jid = inst.ID
			}
		}

		targets = append(targets, statusTarget{
			name:  name,
			ns:    ns,
			uid:   uid,
			jid:   jid,
			inst:  inst,
			node:  node,
			alloc: alloc,
		})
	}
	s.mu.RUnlock()

	responses := make([]PodStatusResponse, len(targets))
	var wg sync.WaitGroup
	sem := make(chan struct{}, 16)

	for i, target := range targets {
		wg.Add(1)
		go func(idx int, tgt statusTarget) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			status := ContainerStatus{
				Name: "worker",
			}

			var completedTerm *ContainerTerminatedState
			if tgt.uid != "" {
				if s.completedStore != nil {
					if rec := s.completedStore.Get(tgt.uid); rec != nil {
						completedTerm = &ContainerTerminatedState{
							ExitCode:   rec.ExitCode,
							Reason:     rec.Reason,
							Message:    rec.Message,
							FinishedAt: rec.FinishedAt,
						}
					}
				}
				if completedTerm == nil {
					s.mu.RLock()
					completedTerm = s.completedPods[tgt.uid]
					s.mu.RUnlock()
				}
			} else {
				s.mu.RLock()
				completedTerm = s.completedPods[fmt.Sprintf("%s/%s", tgt.ns, tgt.name)]
				s.mu.RUnlock()
			}

			switch {
			case completedTerm != nil:
				status.Ready = false
				status.State.Terminated = completedTerm
			case tgt.alloc.Completed:
				status.Ready = false
				finishedAt := time.Now()
				if tgt.alloc.FinishedAt != nil {
					finishedAt = *tgt.alloc.FinishedAt
				}
				status.State.Terminated = &ContainerTerminatedState{
					ExitCode:   tgt.alloc.ExitCode,
					Reason:     tgt.alloc.Reason,
					Message:    tgt.alloc.Message,
					FinishedAt: finishedAt,
				}
			case tgt.inst != nil:
				switch tgt.inst.State {
				case StateProvisioning:
					status.State.Waiting = &ContainerWaitingState{
						Reason:  "ContainerCreating",
						Message: fmt.Sprintf("Provisioning cloud VM %s in %s...", tgt.inst.ID, tgt.inst.Region),
					}
				case StateRunning:
					if s.runner != nil && tgt.node != nil && tgt.uid != "" {
						cResult, err := s.runner.GetStatus(r.Context(), tgt.node, tgt.uid)
						if err != nil {
							status.Ready = false
							status.State.Waiting = &ContainerWaitingState{
								Reason:  "ContainerStatusUnknown",
								Message: err.Error(),
							}
						} else if cResult != nil {
							switch {
							case cResult.Running:
								status.Ready = true
								status.State.Running = &ContainerRunningState{
									StartedAt: cResult.StartedAt,
								}
							case cResult.Status == "created" || cResult.Status == "starting":
								status.Ready = false
								status.State.Waiting = &ContainerWaitingState{
									Reason:  "ContainerCreating",
									Message: "Container created, starting up...",
								}
							case cResult.Status == "restarting" || cResult.Status == "paused" || cResult.Status == "removing":
								status.Ready = false
								status.State.Waiting = &ContainerWaitingState{
									Reason:  cResult.Reason,
									Message: fmt.Sprintf("Container is %s", cResult.Status),
								}
							case cResult.Status == "exited" || cResult.Status == "dead" || cResult.Reason == "Completed" || cResult.Reason == "ErrImagePull" || cResult.Reason == "StartError" || (cResult.ExitCode != 0 && !cResult.Running):
								status.Ready = false
								finAt := cResult.FinishedAt
								if finAt.IsZero() {
									finAt = time.Now()
								}
								term := &ContainerTerminatedState{
									ExitCode:   cResult.ExitCode,
									Reason:     cResult.Reason,
									Message:    cResult.Message,
									FinishedAt: finAt,
								}
								status.State.Terminated = term
								s.nodePool.MarkPodCompleted(tgt.uid, cResult.ExitCode, cResult.Reason, cResult.Message, finAt)
								s.mu.Lock()
								if tgt.uid != "" {
									s.completedPods[tgt.uid] = term
								}
								s.mu.Unlock()
								if tgt.uid != "" && s.completedStore != nil {
									_ = s.completedStore.Set(tgt.uid, &CompletedPodRecord{
										PodUID:     tgt.uid,
										PodJID:     tgt.jid,
										ExitCode:   cResult.ExitCode,
										Reason:     cResult.Reason,
										Message:    cResult.Message,
										FinishedAt: finAt,
									})
								}
							default:
								status.Ready = false
								status.State.Waiting = &ContainerWaitingState{
									Reason:  "ContainerStatusUnknown",
									Message: fmt.Sprintf("Container status: %s", cResult.Status),
								}
							}
						}
					} else {
						status.Ready = true
						status.State.Running = &ContainerRunningState{
							StartedAt: tgt.inst.CreatedAt,
						}
					}
				case StateTerminating:
					status.State.Terminated = &ContainerTerminatedState{
						ExitCode:   0,
						Reason:     "Terminating",
						FinishedAt: time.Now(),
					}
				case StateTerminated:
					status.State.Terminated = &ContainerTerminatedState{
						ExitCode:   0,
						Reason:     "Completed",
						FinishedAt: time.Now(),
					}
				case StateFailed:
					status.State.Terminated = &ContainerTerminatedState{
						ExitCode:   1,
						Reason:     "Failed",
						Message:    "Cloud instance failed",
						FinishedAt: time.Now(),
					}
				}
			default:
				status.State.Waiting = &ContainerWaitingState{
					Reason:  "ContainerCreating",
					Message: fmt.Sprintf("Pod %s/%s is provisioning or awaiting discovery", tgt.ns, tgt.name),
				}
			}

			responses[idx] = PodStatusResponse{
				PodUID:    tgt.uid,
				Name:      tgt.name,
				Namespace: tgt.ns,
				JID:       tgt.jid,
				Containers: []ContainerStatus{
					status,
				},
			}
		}(i, target)
	}
	wg.Wait()

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(responses)
}

// AdoptInstance re-adopts an active cloud VM into the server tracked state if it has valid pod tags
func (s *Server) handleGetLogs(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("[cloudvm] Logs streamed directly via cluster logging stack or cloudwatch.\n"))
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}
