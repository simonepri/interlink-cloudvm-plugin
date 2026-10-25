// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"bufio"
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
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode"

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
	mu                    sync.RWMutex
	providers             map[ProviderType]Provider
	defaultProv           Provider
	instances             map[string]*Instance // key: UID and namespace/podName -> *Instance
	clusterTag            string
	defaultRegion         string
	nodeRegions           map[string]string // virtual node name -> region its pods launch in
	reconciler            *Reconciler
	nodePool              *NodePool
	kmsClient             KMSClient
	kmsKeyID              string
	secretResolver        SecretResolver
	runner                ContainerRunner
	tunnel                *tunnelState
	startRetryInterval    time.Duration
	spotTicker            *SpotPriceTicker
	tokenStore            *TokenStore
	completedStore        *CompletedStore
	tombstoneStore        *TombstoneStore
	registryMirrors       []string
	otelEndpoint          string
	enableNPD             bool
	customBootstrapScript string
	runnerConcurrency     int
	launchTimeout         time.Duration
	podSightingTimeout    time.Duration
	tombstones            map[string]time.Time                 // podUID or instanceID -> deletion timestamp
	activeSightings       map[string]time.Time                 // podUID -> last seen in /status or /create
	lastSightingsPrune    time.Time                            // last timestamp activeSightings was pruned
	statusPolled          bool                                 // set true when first /status query is received
	launchTokens          map[string]string                    // podUID -> runnerAuthToken (reused across retries)
	completedPods         map[string]*ContainerTerminatedState // podUID -> terminal state (retained until /delete)
	inFlightCreates       map[string]*inFlightCreate           // podUID -> in-flight single flight tracker
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
		providers:          make(map[ProviderType]Provider),
		defaultProv:        defaultProvider,
		instances:          make(map[string]*Instance),
		clusterTag:         clusterTag,
		defaultRegion:      "us-east-1",
		launchTimeout:      5 * time.Minute,
		startRetryInterval: 2 * time.Second,
		podSightingTimeout: 10 * time.Minute,
		nodePool:           NewNodePool(0),
		runner:             NewMockContainerRunner(),
		tokenStore:         tokStore,
		completedStore:     compStore,
		tombstoneStore:     tombStore,
		tombstones:         make(map[string]time.Time),
		activeSightings:    make(map[string]time.Time),
		launchTokens:       make(map[string]string),
		completedPods:      make(map[string]*ContainerTerminatedState),
		inFlightCreates:    make(map[string]*inFlightCreate),
	}
	if tombStore != nil {
		for k, v := range tombStore.GetAll() {
			s.tombstones[k] = v
		}
	}
	if raw := os.Getenv("IDLE_GRACE_PERIOD"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d >= 0 {
			s.nodePool.SetIdleGracePeriod(d)
		} else if sec, err := strconv.Atoi(raw); err == nil && sec >= 0 {
			s.nodePool.SetIdleGracePeriod(time.Duration(sec) * time.Second)
		}
	}
	if raw := os.Getenv("MAX_PODS_PER_NODE"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			s.nodePool.SetMaxPodsPerNode(n)
		} else {
			s.nodePool.SetMaxPodsPerNode(100)
		}
	}
	if kmsKey := os.Getenv("KMS_KEY_ID"); kmsKey != "" {
		s.kmsKeyID = kmsKey
	}
	if sr, err := NewKubeSecretResolverFromConfig(); err == nil {
		s.secretResolver = sr
	}
	var regMirrors []string
	if raw := os.Getenv("REGISTRY_MIRRORS"); raw != "" {
		for _, m := range strings.Split(raw, ",") {
			if trimmed := strings.TrimSpace(m); trimmed != "" {
				regMirrors = append(regMirrors, trimmed)
			}
		}
	}
	s.registryMirrors = regMirrors
	s.otelEndpoint = os.Getenv("OTEL_ENDPOINT")
	s.enableNPD = os.Getenv("ENABLE_NPD") == "true" || os.Getenv("ENABLE_NPD") == "1"
	s.customBootstrapScript = os.Getenv("CUSTOM_BOOTSTRAP_SCRIPT")
	if raw := os.Getenv("MAX_RUNNER_CONCURRENCY"); raw != "" {
		if c, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil && c > 0 {
			s.runnerConcurrency = c
		}
	}
	if raw := os.Getenv("NODE_REGIONS"); raw != "" {
		if err := json.Unmarshal([]byte(raw), &s.nodeRegions); err != nil {
			log.Printf("[cloudvm] ERROR parsing NODE_REGIONS: %v", err)
		}
	}
	return s
}

// regionForNode returns the region of the virtual node a pod is bound to, falling back to the default region.
func (s *Server) regionForNode(nodeName string) string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if region := s.nodeRegions[nodeName]; region != "" {
		return region
	}
	return s.defaultRegion
}

// SetRegistryMirrors configures default container registry mirror endpoints.
func (s *Server) SetRegistryMirrors(mirrors []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registryMirrors = mirrors
}

// SetOtelEndpoint configures default OpenTelemetry collector endpoint.
func (s *Server) SetOtelEndpoint(endpoint string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.otelEndpoint = endpoint
}

// SetEnableNPD enables or disables Node Problem Detector on provisioned VMs.
func (s *Server) SetEnableNPD(enabled bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enableNPD = enabled
}

// SetCustomBootstrapScript sets a custom bootstrap script appended to cloud-init.
func (s *Server) SetCustomBootstrapScript(script string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.customBootstrapScript = script
}

// SetRunnerConcurrency configures default runner concurrency for provisioned VMs.
func (s *Server) SetRunnerConcurrency(c int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if c < 0 {
		c = 0
	}
	s.runnerConcurrency = c
}

// RunnerConcurrency returns the configured default runner concurrency.
func (s *Server) RunnerConcurrency() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.runnerConcurrency
}

// BuildCloudInitConfig constructs a CloudInitConfig from an InstanceSpec, populating
// server-level defaults including RunnerConcurrency if not explicitly set.
func (s *Server) BuildCloudInitConfig(spec *InstanceSpec) *CloudInitConfig {
	if spec == nil {
		return nil
	}
	rc := spec.RunnerConcurrency
	if rc <= 0 && s != nil {
		s.mu.RLock()
		rc = s.runnerConcurrency
		s.mu.RUnlock()
	}
	runnerToken := spec.RunnerAuthToken
	if spec.KMSEncryptedToken || spec.KMSCiphertext != "" {
		runnerToken = ""
	}
	mirrors := spec.RegistryMirrors
	if len(mirrors) == 0 && s != nil {
		s.mu.RLock()
		mirrors = s.registryMirrors
		s.mu.RUnlock()
	}
	otel := spec.OtelEndpoint
	if otel == "" && s != nil {
		s.mu.RLock()
		otel = s.otelEndpoint
		s.mu.RUnlock()
	}
	npd := spec.NodeProblemDetector
	if !npd && s != nil {
		s.mu.RLock()
		npd = s.enableNPD
		s.mu.RUnlock()
	}
	script := spec.CustomBootstrapScript
	if script == "" && s != nil {
		s.mu.RLock()
		script = s.customBootstrapScript
		s.mu.RUnlock()
	}
	return &CloudInitConfig{
		PodName:               spec.PodName,
		PodNamespace:          spec.PodNamespace,
		PodUID:                spec.PodUID,
		RayHeadAddress:        spec.RayHeadAddress,
		RayVersion:            spec.RayVersion,
		TargetContainerImage:  spec.TargetContainerImage,
		Command:               spec.Command,
		Args:                  spec.Args,
		Env:                   spec.Env,
		UseNativeDLAMI:        spec.AMIMode == "dlami",
		NumGPUs:               spec.NumGPUs,
		AssignedGPUs:          spec.AssignedGPUs,
		CPUMillicores:         spec.CPUMillicores,
		MemoryBytes:           spec.MemoryBytes,
		RunnerAuthToken:       runnerToken,
		KMSEncryptedToken:     spec.KMSEncryptedToken,
		KMSKeyID:              spec.KMSKeyID,
		KMSCiphertext:         spec.KMSCiphertext,
		MeshAuthKey:           spec.MeshAuthKey,
		MeshAuthKeyCiphertext: spec.MeshAuthKeyCiphertext,
		MeshTags:              spec.MeshTags,
		TunnelEndpoint:        spec.TunnelEndpoint,
		RunnerDirectHTTP:      spec.RunnerDirectHTTP,
		Region:                spec.Region,
		InstanceType:          spec.InstanceType,
		EgressBandwidth:       spec.EgressBandwidth,
		EgressBurst:           spec.EgressBurst,
		RunAsUser:             spec.RunAsUser,
		RunAsGroup:            spec.RunAsGroup,
		DockerConfigJSON:      spec.DockerConfigJSON,
		RegistryMirrors:       mirrors,
		OtelEndpoint:          otel,
		NodeProblemDetector:   npd,
		CustomBootstrapScript: script,
		RunnerConcurrency:     rc,
	}
}

// SetRegionStore overrides the region store for instance-to-region mapping persistence.
func (s *Server) SetRegionStore(rs *RegionStore) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if setter, ok := s.defaultProv.(interface{ SetRegionStore(*RegionStore) }); ok {
		setter.SetRegionStore(rs)
	}
	for _, prov := range s.providers {
		if setter, ok := prov.(interface{ SetRegionStore(*RegionStore) }); ok {
			setter.SetRegionStore(rs)
		}
	}
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
// SetKMSConfig configures AWS KMS encryption for runner authentication tokens.
func (s *Server) SetKMSConfig(client KMSClient, keyID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.kmsClient = client
	s.kmsKeyID = keyID
}

// SetSecretResolver sets the Kubernetes secret resolver for imagePullSecrets.
func (s *Server) SetSecretResolver(sr SecretResolver) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.secretResolver = sr
}

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

// SetIdleGracePeriod updates the idle grace period on the underlying node pool.
func (s *Server) SetIdleGracePeriod(d time.Duration) {
	if s.nodePool != nil {
		s.nodePool.SetIdleGracePeriod(d)
	}
}

// MaxPodsPerNode returns the configured max active pods per node.
func (s *Server) MaxPodsPerNode() int {
	if s.nodePool != nil {
		return s.nodePool.MaxPodsPerNode()
	}
	return 100
}

// SetMaxPodsPerNode updates the max active pods per node on the node pool.
func (s *Server) SetMaxPodsPerNode(n int) {
	if s.nodePool != nil {
		s.nodePool.SetMaxPodsPerNode(n)
	}
}

// SetLaunchTimeout overrides the maximum allowed duration for bare VM creation and bootstrap.
func (s *Server) SetLaunchTimeout(d time.Duration) {
	if d > 0 {
		s.launchTimeout = d
	}
}

// SetPodSightingTimeout overrides the TTL for keeping active pod sightings before garbage collection.
func (s *Server) SetPodSightingTimeout(d time.Duration) {
	if d > 0 {
		s.podSightingTimeout = d
	}
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
		r.SetNodePool(s.nodePool)
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
	s.tombstones[node.InstanceID] = time.Now()
	if s.tombstoneStore != nil {
		_ = s.tombstoneStore.Set(node.InstanceID)
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
	mux.HandleFunc("/pinglink", s.handleHealthz)

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
	mux.HandleFunc("/metrics", s.handleMetrics)

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

	spec, err := PodToInstanceSpec(pod, s.clusterTag, s.regionForNode(pod.Spec.NodeName))
	if err != nil {
		finalErr = err
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	// Resolve imagePullSecrets via Kubernetes SecretResolver
	if len(pod.Spec.ImagePullSecrets) > 0 {
		if s.secretResolver != nil {
			dockerConfigJSON, err := ResolveImagePullSecrets(r.Context(), s.secretResolver, spec.PodNamespace, pod.Spec.ImagePullSecrets)
			if err != nil {
				finalErr = fmt.Errorf("failed to resolve imagePullSecrets: %w", err)
				http.Error(w, finalErr.Error(), http.StatusInternalServerError)
				return
			}
			spec.DockerConfigJSON = dockerConfigJSON
		}
	}

	s.mu.Lock()
	if len(spec.RegistryMirrors) == 0 && len(s.registryMirrors) > 0 {
		spec.RegistryMirrors = s.registryMirrors
	}
	if spec.OtelEndpoint == "" && s.otelEndpoint != "" {
		spec.OtelEndpoint = s.otelEndpoint
	}
	if !spec.NodeProblemDetector && s.enableNPD {
		spec.NodeProblemDetector = true
	}
	if spec.CustomBootstrapScript == "" && s.customBootstrapScript != "" {
		spec.CustomBootstrapScript = s.customBootstrapScript
	}
	if spec.RunnerConcurrency <= 0 && s.runnerConcurrency > 0 {
		spec.RunnerConcurrency = s.runnerConcurrency
	}

	// Single-flight in-flight create check and idempotency under lock
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
		startReq := &StartContainerRequest{
			PodUID:           spec.PodUID,
			Namespace:        spec.PodNamespace,
			PodName:          spec.PodName,
			ContainerName:    cName,
			Image:            spec.TargetContainerImage,
			Command:          spec.Command,
			Args:             spec.Args,
			Env:              spec.Env,
			CPUMillicores:    cpuLimitMilli,
			MemoryBytes:      memLimitBytes,
			AssignedGPUs:     assignedGPUs,
			AssignedTPUs:     assignedTPUs,
			RunAsUser:        spec.RunAsUser,
			RunAsGroup:       spec.RunAsGroup,
			DockerConfigJSON: spec.DockerConfigJSON,
		}
		startErr := s.runner.StartContainer(r.Context(), warmNode, startReq)
		if startErr != nil {
			log.Printf("[cloudvm] Initial StartContainer call for bin-pack slice returned error (VM may still be booting): %v. Launching background worker to retry until runner is up...", startErr)
			go s.retryStartContainer(context.WithoutCancel(r.Context()), warmNode, startReq) //nolint:gosec // detached background retry outlives HTTP request
		}

		marketType := "on-demand"
		if warmNode.Spot {
			marketType = "spot"
		}

		s.mu.Lock()
		inst := &Instance{
			ID:        warmNode.InstanceID,
			Provider:  warmNode.Provider,
			Region:    warmNode.Region,
			IP:        warmNode.GetMeshIP(),
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
	s.applyTunnelSpec(spec)

	// Resolve KMS token encryption (AWS EC2 instances only, key taken strictly from provider configuration)
	_, isAWS := prov.(*AWSProvider)
	if !isAWS && spec != nil && strings.ToLower(spec.Provider) == "aws" {
		isAWS = true
	}
	if isAWS && (s.kmsClient != nil || s.kmsKeyID != "") {
		kmsKeyID := s.kmsKeyID
		kmsClient := s.kmsClient
		if kmsClient == nil {
			region := spec.Region
			if region == "" {
				region = s.defaultRegion
			}
			var err error
			kmsClient, err = NewKMSClientFromConfig(r.Context(), region)
			if err != nil {
				finalErr = fmt.Errorf("failed to initialize KMS client for region %s: %w", region, err)
				http.Error(w, finalErr.Error(), http.StatusInternalServerError)
				return
			}
		}
		ciphertext, err := EncryptRunnerToken(r.Context(), kmsClient, kmsKeyID, tok)
		if err != nil {
			finalErr = fmt.Errorf("failed to encrypt runner token with KMS key %s: %w", kmsKeyID, err)
			http.Error(w, finalErr.Error(), http.StatusInternalServerError)
			return
		}
		spec.KMSEncryptedToken = true
		spec.KMSKeyID = kmsKeyID
		spec.KMSCiphertext = ciphertext
		if err := encryptMeshAuthKey(r.Context(), kmsClient, kmsKeyID, spec); err != nil {
			finalErr = err
			http.Error(w, finalErr.Error(), http.StatusInternalServerError)
			return
		}
	}

	var preAssignedGPUs []int
	for i := 0; i < spec.NumGPUs; i++ {
		preAssignedGPUs = append(preAssignedGPUs, i)
	}
	spec.AssignedGPUs = preAssignedGPUs

	// Detach from client HTTP request context so client timeouts or cancellations do not abort in-flight VM launch
	launchTimeout := s.launchTimeout
	if launchTimeout <= 0 {
		launchTimeout = 5 * time.Minute
	}
	launchCtx, launchCancel := context.WithTimeout(context.WithoutCancel(r.Context()), launchTimeout)
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
	newNode.InstanceType = itype
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
	startReq := &StartContainerRequest{
		PodUID:           spec.PodUID,
		Namespace:        spec.PodNamespace,
		PodName:          spec.PodName,
		ContainerName:    cName,
		Image:            spec.TargetContainerImage,
		Command:          spec.Command,
		Args:             spec.Args,
		Env:              spec.Env,
		CPUMillicores:    reqVec.CPU.MilliValue(),
		MemoryBytes:      reqVec.Memory.Value(),
		AssignedGPUs:     gpus,
		AssignedTPUs:     tpus,
		RunAsUser:        spec.RunAsUser,
		RunAsGroup:       spec.RunAsGroup,
		DockerConfigJSON: spec.DockerConfigJSON,
	}
	startErr := s.runner.StartContainer(r.Context(), newNode, startReq)
	if startErr != nil {
		log.Printf("[cloudvm] Initial StartContainer call returned error (VM may still be booting): %v. Launching background worker to retry until runner is up...", startErr)
		go s.retryStartContainer(context.WithoutCancel(r.Context()), newNode, startReq) //nolint:gosec // detached background retry outlives HTTP request
	}

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
	var nodeRef *CloudVMNode
	wasCordoned := false
	if node, _, found := s.nodePool.FindPodNode(uid); found {
		foundInPool = true
		nodeRef = node
		wasCordoned = node.Cordoned
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

	// If the node pool has an active idle grace period and the node was not cordoned prior to release,
	// retain the warm VM during its cooldown window before idle termination.
	if foundInPool && nodeRef != nil && !wasCordoned && s.nodePool.IdleGracePeriod() > 0 {
		log.Printf("[cloudvm] Node %s has 0 active pods; holding warm for %v for idle cooldown",
			nodeRef.InstanceID, s.nodePool.IdleGracePeriod())
		w.WriteHeader(http.StatusOK)
		return
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

type statusPodMetadata struct {
	Name      string `json:"name"`
	Namespace string `json:"namespace"`
	UID       string `json:"uid"`
}

type statusPodItem struct {
	Pod *struct {
		Metadata statusPodMetadata `json:"metadata"`
	} `json:"pod"`
	Metadata statusPodMetadata `json:"metadata"`
}

type statusPodRef struct {
	Name      string
	Namespace string
	UID       string
}

func extractPodRef(item *statusPodItem) *statusPodRef {
	if item == nil {
		return nil
	}
	if item.Pod != nil && (item.Pod.Metadata.Name != "" || item.Pod.Metadata.UID != "") {
		return &statusPodRef{
			Name:      item.Pod.Metadata.Name,
			Namespace: item.Pod.Metadata.Namespace,
			UID:       item.Pod.Metadata.UID,
		}
	}
	if item.Metadata.Name != "" || item.Metadata.UID != "" {
		return &statusPodRef{
			Name:      item.Metadata.Name,
			Namespace: item.Metadata.Namespace,
			UID:       item.Metadata.UID,
		}
	}
	return nil
}

func decodeStatusPods(r io.Reader) []*statusPodRef {
	if r == nil {
		return nil
	}
	br := bufio.NewReader(r)
	var firstByte byte
	for {
		b, err := br.ReadByte()
		if err != nil {
			return nil
		}
		if !unicode.IsSpace(rune(b)) {
			firstByte = b
			_ = br.UnreadByte()
			break
		}
	}

	dec := json.NewDecoder(br)
	var pods []*statusPodRef

	if firstByte == '[' {
		tok, err := dec.Token()
		if err != nil || tok != json.Delim('[') {
			return nil
		}
		for dec.More() {
			var item statusPodItem
			if err := dec.Decode(&item); err != nil {
				break
			}
			if ref := extractPodRef(&item); ref != nil {
				pods = append(pods, ref)
			}
		}
		_, _ = dec.Token()
	} else if firstByte == '{' {
		var item statusPodItem
		if err := dec.Decode(&item); err == nil {
			if ref := extractPodRef(&item); ref != nil {
				pods = append(pods, ref)
			}
		}
	}
	return pods
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost && r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var pods []*statusPodRef
	if r.Body != nil {
		pods = decodeStatusPods(r.Body)
	}

	now := time.Now()
	s.mu.Lock()
	s.statusPolled = true
	if len(pods) > 0 {
		for _, p := range pods {
			if p.UID != "" {
				s.activeSightings[p.UID] = now
			}
		}
		sightingTTL := s.podSightingTimeout
		if sightingTTL <= 0 {
			sightingTTL = 10 * time.Minute
		}
		if s.lastSightingsPrune.IsZero() || now.Sub(s.lastSightingsPrune) > time.Minute {
			s.lastSightingsPrune = now
			for uid, seenAt := range s.activeSightings {
				if now.Sub(seenAt) > sightingTTL {
					delete(s.activeSightings, uid)
				}
			}
		}
	}
	s.mu.Unlock()

	type statusTarget struct {
		name          string
		ns            string
		uid           string
		jid           string
		inst          *Instance
		completedTerm *ContainerTerminatedState
	}

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

			pods = append(pods, &statusPodRef{
				Name:      name,
				Namespace: ns,
				UID:       uid,
			})
		}
	}

	targets := make([]*statusTarget, len(pods))
	for i, pod := range pods {
		name := pod.Name
		ns := pod.Namespace
		if ns == "" {
			ns = "default"
		}
		var inst *Instance
		var exists bool
		if pod.UID != "" {
			inst, exists = s.instances[pod.UID]
		}
		var key string
		if !exists || inst == nil {
			key = ns + "/" + name
			inst, exists = s.instances[key]
			if !exists || inst == nil {
				inst, exists = s.instances[name]
			}
		}

		uid := pod.UID
		if uid == "" && exists && inst != nil && inst.Metadata != nil {
			uid = inst.Metadata["podUID"]
		}

		jid := ""
		if exists && inst != nil {
			if uid != "" {
				jid = inst.ID + ":" + uid
			} else {
				jid = inst.ID
			}
		}

		var completedTerm *ContainerTerminatedState
		if uid != "" {
			completedTerm = s.completedPods[uid]
		} else {
			if key == "" {
				key = ns + "/" + name
			}
			completedTerm = s.completedPods[key]
		}

		targets[i] = &statusTarget{
			name:          name,
			ns:            ns,
			uid:           uid,
			jid:           jid,
			inst:          inst,
			completedTerm: completedTerm,
		}
	}
	s.mu.RUnlock()

	responses := make([]PodStatusResponse, len(targets))
	if len(targets) > 0 {
		numWorkers := 16
		if s.runnerConcurrency > numWorkers {
			numWorkers = s.runnerConcurrency
		}
		if numWorkers > len(targets) {
			numWorkers = len(targets)
		}
		if numWorkers <= 0 {
			numWorkers = 1
		}

		var curIdx atomic.Int64
		var wg sync.WaitGroup

		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					idx := int(curIdx.Add(1) - 1)
					if idx >= len(targets) {
						return
					}
					tgt := targets[idx]

					status := ContainerStatus{
						Name: "worker",
					}

					completedTerm := tgt.completedTerm
					if completedTerm == nil && tgt.uid != "" && s.completedStore != nil {
						if rec := s.completedStore.Get(tgt.uid); rec != nil {
							completedTerm = &ContainerTerminatedState{
								ExitCode:   rec.ExitCode,
								Reason:     rec.Reason,
								Message:    rec.Message,
								FinishedAt: rec.FinishedAt,
							}
						}
					}

					var node *CloudVMNode
					var alloc *PodAllocation
					if completedTerm == nil && tgt.inst != nil && s.nodePool != nil && tgt.uid != "" {
						if n, a, found := s.nodePool.FindPodNode(tgt.uid); found {
							node = n
							alloc = &a
						}
					}

					switch {
					case completedTerm != nil:
						status.Ready = false
						status.State.Terminated = completedTerm
					case alloc != nil && alloc.Completed:
						status.Ready = false
						finishedAt := time.Now()
						if alloc.FinishedAt != nil {
							finishedAt = *alloc.FinishedAt
						}
						status.State.Terminated = &ContainerTerminatedState{
							ExitCode:   alloc.ExitCode,
							Reason:     alloc.Reason,
							Message:    alloc.Message,
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
							if s.runner != nil && node != nil && tgt.uid != "" {
								if node.GetMeshIP() == "" {
									if prov := s.ProviderForType(node.Provider); prov != nil {
										if st, err := prov.Status(r.Context(), node.InstanceID); err == nil && st != nil && st.IP != "" {
											node.SetMeshIP(st.IP)
											if tgt.inst != nil {
												tgt.inst.IP = st.IP
											}
											s.mu.Lock()
											if inst, ok := s.instances[tgt.uid]; ok && inst != nil {
												inst.IP = st.IP
											}
											s.mu.Unlock()
										}
									}
								}
								cResult, err := s.runner.GetStatus(r.Context(), node, tgt.uid)
								if err != nil {
									status.Ready = false
									status.State.Waiting = &ContainerWaitingState{
										Reason:  "ContainerCreating",
										Message: fmt.Sprintf("Waiting for runner daemon on %s (%s)...", node.InstanceID, node.GetMeshIP()),
									}
								} else if cResult != nil {
									if cResult.NetworkRxBytes > 0 || cResult.NetworkTxBytes > 0 {
										prov := string(node.Provider)
										if prov == "" && tgt.inst != nil {
											prov = string(tgt.inst.Provider)
										}
										reg := node.Region
										if reg == "" && tgt.inst != nil {
											reg = tgt.inst.Region
										}
										itype := node.InstanceType
										if itype == "" && tgt.inst != nil {
											itype = tgt.inst.InstanceType
										}
										instID := node.InstanceID
										if instID == "" && tgt.inst != nil {
											instID = tgt.inst.ID
										}
										GlobalFleetTracker.RecordNetworkTraffic(prov, reg, itype, instID, cResult.NetworkRxBytes, cResult.NetworkTxBytes)
									}
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
				}
			}()
		}
		wg.Wait()
	}

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

func (s *Server) retryStartContainer(ctx context.Context, node *CloudVMNode, req *StartContainerRequest) {
	launchedAt := time.Now()
	ctx, cancel := context.WithTimeout(ctx, s.startRetryTimeout())
	defer cancel()

	ticker := time.NewTicker(s.startRetryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("[cloudvm] Timed out waiting to start container for pod %s/%s on node %s: %v", req.Namespace, req.PodName, node.InstanceID, ctx.Err())
			s.mu.Lock()
			term := &ContainerTerminatedState{
				ExitCode:   1,
				Reason:     "StartError",
				Message:    fmt.Sprintf("timed out waiting for in-VM runner on node %s: %v", node.InstanceID, ctx.Err()),
				FinishedAt: time.Now(),
			}
			s.completedPods[req.PodUID] = term
			if s.completedStore != nil {
				_ = s.completedStore.Set(req.PodUID, &CompletedPodRecord{
					PodUID:     req.PodUID,
					ExitCode:   1,
					Reason:     term.Reason,
					Message:    term.Message,
					FinishedAt: term.FinishedAt,
				})
			}
			if s.nodePool != nil {
				s.nodePool.MarkPodCompleted(req.PodUID, 1, "StartError", term.Message, term.FinishedAt)
				s.nodePool.Cordon(node.InstanceID)
			}
			s.mu.Unlock()
			return
		case <-ticker.C:
			s.mu.RLock()
			tombstoned := s.isTombstonedLocked(req.PodUID)
			s.mu.RUnlock()
			if tombstoned {
				log.Printf("[cloudvm] Pod %s was tombstoned while waiting to start container; aborting retry", req.PodUID)
				return
			}
			if s.tunnelDeadlineExceeded(node, launchedAt) {
				s.failTunnelDeadline(ctx, node, req)
				return
			}

			if node.GetMeshIP() == "" {
				if prov := s.ProviderForType(node.Provider); prov != nil {
					st, err := prov.Status(ctx, node.InstanceID)
					if err == nil && st != nil && st.IP != "" {
						node.SetMeshIP(st.IP)
						log.Printf("[cloudvm] Discovered IP %s for node %s during retryStartContainer", st.IP, node.InstanceID)
						s.mu.Lock()
						if inst, ok := s.instances[req.PodUID]; ok && inst != nil {
							inst.IP = st.IP
						}
						key := fmt.Sprintf("%s/%s", req.Namespace, req.PodName)
						if inst, ok := s.instances[key]; ok && inst != nil {
							inst.IP = st.IP
						}
						s.mu.Unlock()
					}
				}
			}

			err := s.runner.StartContainer(ctx, node, req)
			if err == nil {
				log.Printf("[cloudvm] Successfully started container for pod %s/%s on node %s", req.Namespace, req.PodName, node.InstanceID)
				return
			}
			log.Printf("[cloudvm] Retrying StartContainer for pod %s/%s on node %s: %v", req.Namespace, req.PodName, node.InstanceID, err)
		}
	}
}
