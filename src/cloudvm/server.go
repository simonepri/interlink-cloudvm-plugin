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
	"strconv"
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

// EgressRateToBandwidth converts an hourly budget in USD/hr to an equivalent network rate string (e.g. "22mbit").
// Uses provider baseline egress rates ($0.09/GB for AWS, $0.085/GB for GCP).
func EgressRateToBandwidth(dollarPerHour float64, provider string) string {
	if dollarPerHour <= 0 {
		return ""
	}
	costPerGB := 0.09 // default AWS
	if strings.EqualFold(provider, "gcp") {
		costPerGB = 0.085
	}
	// Megabits per second = (GB/hr * 8 * 1000) / 3600
	mbps := (dollarPerHour / costPerGB) * 8000.0 / 3600.0
	if mbps < 1.0 {
		return fmt.Sprintf("%dkbit", int(mbps*1000))
	}
	return fmt.Sprintf("%dmbit", int(mbps))
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
		tombStore.mu.RLock()
		for k, v := range tombStore.tombstones {
			s.tombstones[k] = v
		}
		tombStore.mu.RUnlock()
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
		ts.mu.RLock()
		for k, v := range ts.tombstones {
			s.tombstones[k] = v
		}
		ts.mu.RUnlock()
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
	cleanID := strings.ReplaceAll(strings.ReplaceAll(node.InstanceID, "\n", ""), "\r", "")
	log.Printf("[cloudvm] Terminating expired idle node %s", cleanID) //nolint:gosec
	if prov != nil {
		if err := prov.Terminate(ctx, node.InstanceID); err != nil {
			log.Printf("[cloudvm] Error terminating idle instance %s: %v", cleanID, err)
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

// PodToInstanceSpec converts an incoming InterLink Pod into an InstanceSpec.
func PodToInstanceSpec(pod *Pod, defaultClusterTag string, defaultRegion ...string) (*InstanceSpec, error) {
	// Enforce non-root containers on host network: reject runAsUser 0 or runAsGroup 0
	for _, c := range pod.Spec.Containers {
		if c.SecurityContext != nil {
			if c.SecurityContext.RunAsUser != nil && *c.SecurityContext.RunAsUser == 0 {
				return nil, fmt.Errorf("cloudvm provider enforces non-root containers; container %q specifies runAsUser 0 which is forbidden on host network", c.Name)
			}
			if c.SecurityContext.RunAsGroup != nil && *c.SecurityContext.RunAsGroup == 0 {
				return nil, fmt.Errorf("cloudvm provider enforces non-root containers; container %q specifies runAsGroup 0 which is forbidden on host network", c.Name)
			}
		}
	}
	if pod.Spec.SecurityContext != nil {
		if pod.Spec.SecurityContext.RunAsUser != nil && *pod.Spec.SecurityContext.RunAsUser == 0 {
			return nil, fmt.Errorf("cloudvm provider enforces non-root containers; pod securityContext specifies runAsUser 0 which is forbidden on host network")
		}
		if pod.Spec.SecurityContext.RunAsGroup != nil && *pod.Spec.SecurityContext.RunAsGroup == 0 {
			return nil, fmt.Errorf("cloudvm provider enforces non-root containers; pod securityContext specifies runAsGroup 0 which is forbidden on host network")
		}
	}
	ns := pod.Metadata.Namespace
	if ns == "" {
		ns = "default"
	}
	name := pod.Metadata.Name
	uid := pod.Metadata.UID
	if uid == "" {
		uid = pod.Metadata.Annotations["kubernetes.io/pod-uid"]
	}
	if uid == "" {
		uid = fmt.Sprintf("%s/%s", ns, name)
	}

	// 0. Cloud Provider selection
	prov := pod.Metadata.Annotations["cloudvm.vk.io/provider"]
	if prov == "" {
		prov = pod.Spec.NodeSelector["cloudvm.vk.io/provider"]
	}

	// 1. Region
	region := pod.Metadata.Annotations["cloudvm.vk.io/region"]
	if region == "" {
		region = pod.Spec.NodeSelector["topology.kubernetes.io/region"]
	}
	if region == "" {
		if len(defaultRegion) > 0 && defaultRegion[0] != "" {
			region = defaultRegion[0]
		} else {
			region = "us-east-1"
		}
	}

	var candidateRegions []string
	if cand := pod.Metadata.Annotations["cloudvm.vk.io/candidate-regions"]; cand != "" {
		for _, r := range strings.Split(cand, ",") {
			r = strings.TrimSpace(r)
			if r != "" {
				candidateRegions = append(candidateRegions, r)
			}
		}
	}

	// 2. Instance Type
	instanceType := pod.Metadata.Annotations["cloudvm.vk.io/instance-type"]
	if instanceType == "" {
		instanceType = pod.Spec.NodeSelector["node.kubernetes.io/instance-type"]
	}
	if instanceType == "" {
		instanceType = "g5.2xlarge"
	}

	// 3. Market / Capacity Type (spot vs on-demand)
	marketType := pod.Metadata.Annotations["cloudvm.vk.io/capacity-type"]
	if marketType == "" && pod.Metadata.Annotations["cloudvm.vk.io/spot"] == "false" {
		marketType = "on-demand"
	}
	if marketType == "" {
		marketType = pod.Spec.NodeSelector["cloudvm.vk.io/capacity-type"]
	}
	if marketType == "" {
		marketType = "spot"
	}

	// 4. AMI Mode (dlami vs container)
	amiMode := pod.Metadata.Annotations["cloudvm.vk.io/ami-mode"]
	if amiMode == "" {
		amiMode = "container"
	}

	// 5. Max Price cap & policy
	maxPrice := pod.Metadata.Annotations["cloudvm.vk.io/max-price"]
	if maxPrice == "" {
		maxPrice = pod.Metadata.Annotations["cloudvm.vk.io/max-spot-price"]
	}
	pricePolicy := pod.Metadata.Annotations["cloudvm.vk.io/price-policy"]
	if pricePolicy == "" {
		pricePolicy = "fail-fast"
	}

	// Cost allocation & tags
	costCenter := pod.Metadata.Annotations["cloudvm.vk.io/cost-center"]
	jobID := pod.Metadata.Annotations["cloudvm.vk.io/job-id"]
	launchTemplateID := pod.Metadata.Annotations["cloudvm.vk.io/aws-launch-template"]
	launchTemplateName := pod.Metadata.Annotations["cloudvm.vk.io/aws-launch-template-name"]

	// 6. Network Egress Rate Limiting (Token Bucket Filter via rate or dollar/hr spend)
	maxEgressRate := pod.Metadata.Annotations["cloudvm.vk.io/max-egress-rate"]
	egressBandwidth := pod.Metadata.Annotations["cloudvm.vk.io/egress-bandwidth"]
	if egressBandwidth == "" {
		egressBandwidth = pod.Metadata.Annotations["kubernetes.io/egress-bandwidth"]
	}
	egressBurst := pod.Metadata.Annotations["cloudvm.vk.io/egress-burst"]

	// If dollar/hr rate is configured and raw bandwidth is omitted, calculate equivalent token rate
	if maxEgressRate != "" && egressBandwidth == "" {
		clean := strings.TrimPrefix(strings.TrimSpace(maxEgressRate), "$")
		if rateVal, err := strconv.ParseFloat(clean, 64); err == nil && rateVal > 0 {
			egressBandwidth = EgressRateToBandwidth(rateVal, prov)
			if egressBurst == "" {
				egressBurst = "100mb" // generous burst allowance for checkpoints
			}
		}
	}

	var targetImage string
	var command []string
	var args []string
	envMap := make(map[string]string)
	var rayHeadAddress string
	var meshAuthKey string
	numGPUs := 0

	if len(pod.Spec.Containers) > 0 {
		c := pod.Spec.Containers[0]
		targetImage = c.Image
		command = c.Command
		args = c.Args

		// Extract GPU count
		if gpuStr, exists := c.Resources.Limits["nvidia.com/gpu"]; exists {
			if n, err := strconv.Atoi(gpuStr); err == nil {
				numGPUs = n
			}
		}

		// Extract environment variables
		for _, env := range c.Env {
			envMap[env.Name] = env.Value
			switch env.Name {
			case "RAY_HEAD_ADDRESS":
				rayHeadAddress = env.Value
			case "TAILSCALE_AUTHKEY", "MESH_AUTH_KEY":
				meshAuthKey = env.Value
			}
		}
	}

	var runAsUser *int64
	var runAsGroup *int64
	if len(pod.Spec.Containers) > 0 {
		c := pod.Spec.Containers[0]
		if c.SecurityContext != nil && c.SecurityContext.RunAsUser != nil {
			runAsUser = c.SecurityContext.RunAsUser
			runAsGroup = c.SecurityContext.RunAsGroup
		}
	}
	if runAsUser == nil && pod.Spec.SecurityContext != nil && pod.Spec.SecurityContext.RunAsUser != nil {
		runAsUser = pod.Spec.SecurityContext.RunAsUser
		runAsGroup = pod.Spec.SecurityContext.RunAsGroup
	}

	var cpuMillicores int64
	var memoryBytes int64
	if len(pod.Spec.Containers) > 0 {
		c := pod.Spec.Containers[0]
		if cpuLimit, exists := c.Resources.Limits["cpu"]; exists {
			if q, err := resource.ParseQuantity(cpuLimit); err == nil {
				cpuMillicores = q.MilliValue()
			}
		}
		if memLimit, exists := c.Resources.Limits["memory"]; exists {
			if q, err := resource.ParseQuantity(memLimit); err == nil {
				memoryBytes = q.Value()
			}
		}
	}

	if rayHeadAddress == "" {
		rayHeadAddress = pod.Metadata.Annotations["ray.io/head-address"]
	}

	return &InstanceSpec{
		PodName:              name,
		PodNamespace:         ns,
		PodUID:               uid,
		Provider:             prov,
		CostCenter:           costCenter,
		JobID:                jobID,
		LaunchTemplateID:     launchTemplateID,
		LaunchTemplateName:   launchTemplateName,
		Region:               region,
		CandidateRegions:     candidateRegions,
		InstanceType:         instanceType,
		MarketType:           marketType,
		AMIMode:              amiMode,
		MaxPrice:             maxPrice,
		MaxSpotPrice:         maxPrice,
		PricePolicy:          pricePolicy,
		MaxEgressRate:        maxEgressRate,
		EgressBandwidth:      egressBandwidth,
		EgressBurst:          egressBurst,
		NumGPUs:              numGPUs,
		TargetContainerImage: targetImage,
		Command:              command,
		Args:                 args,
		Env:                  envMap,
		RayHeadAddress:       rayHeadAddress,
		MeshAuthKey:          meshAuthKey,
		RunAsUser:            runAsUser,
		RunAsGroup:           runAsGroup,
		CPUMillicores:        cpuMillicores,
		MemoryBytes:          memoryBytes,
		ClusterTag:           defaultClusterTag,
	}, nil
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

// parsePodFromPayload parses either InterLink standard RetrievedPodData ({"pod": ...}) or flat Pod JSON.
func parsePodFromPayload(body []byte) (*Pod, error) {
	var retrieved struct {
		Pod Pod `json:"pod"`
	}
	if err := json.Unmarshal(body, &retrieved); err == nil && retrieved.Pod.Metadata.Name != "" {
		return &retrieved.Pod, nil
	}

	var direct Pod
	if err := json.Unmarshal(body, &direct); err == nil && direct.Metadata.Name != "" {
		return &direct, nil
	}

	return nil, fmt.Errorf("invalid pod JSON payload")
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
// and has not been explicitly deleted or completed.
func (s *Server) AdoptInstance(ctx context.Context, inst *Instance) bool {
	if inst == nil || inst.Metadata == nil {
		return false
	}
	podUID := inst.Metadata["podUID"]
	podName := inst.Metadata["podName"]
	if podName == "" {
		podName = inst.Metadata["PodName"]
	}
	ns := inst.Metadata["namespace"]
	if ns == "" {
		ns = inst.Metadata["Namespace"]
	}
	if ns == "" {
		ns = "default"
	}

	if podUID == "" || podName == "" {
		return false
	}

	s.mu.RLock()
	// 1. If this pod or instance has been tombstoned (explicitly deleted), NEVER adopt
	if s.isTombstonedLocked(podUID) || s.isTombstonedLocked(inst.ID) {
		s.mu.RUnlock()
		return false
	}

	// 2. If this pod was already recorded as completed or terminated, do not re-adopt
	if s.completedPods[podUID] != nil || (s.completedStore != nil && s.completedStore.Get(podUID) != nil) {
		s.mu.RUnlock()
		return false
	}

	// 3. Evidence check: require that InterLink has polled status and sighted the pod within 10m
	if !s.statusPolled {
		s.mu.RUnlock()
		return false
	}
	sighting, sighted := s.activeSightings[podUID]
	if !sighted || time.Since(sighting) > 10*time.Minute {
		s.mu.RUnlock()
		return false
	}

	// 4. Check if already tracked
	if s.instances[podUID] != nil || s.instances[inst.ID] != nil {
		s.mu.RUnlock()
		return true
	}
	s.mu.RUnlock()

	marketType := inst.Metadata["marketType"]
	if marketType == "" {
		marketType = "on-demand"
	}
	inst.Metadata["marketType"] = marketType

	var recNode *CloudVMNode
	var defaultVec ResourceVector
	if s.nodePool != nil {
		defaultVec, _ = NewResourceVector("2", "4Gi", 0, 0)
		capVec := CapacityForInstanceType(inst.InstanceType, defaultVec)
		recNode = NewCloudVMNode(inst.ID, inst.Provider, inst.Region, inst.IP, capVec)
		recNode.Namespace = ns
		recNode.Spot = (marketType == "spot")
		recNode.Cordoned = true

		// Look up runner auth token from tokenStore (by inst.ID, then podUID), then metadata
		tok := ""
		if s.tokenStore != nil {
			tok = s.tokenStore.Get(inst.ID)
			if tok == "" && podUID != "" {
				tok = s.tokenStore.Get(podUID)
			}
			if tok != "" && s.tokenStore.Get(inst.ID) == "" {
				_ = s.tokenStore.Set(inst.ID, tok)
			}
		}
		if tok == "" {
			tok = inst.Metadata["runnerToken"]
		}
		if tok == "" {
			tok = inst.Metadata["cloudvm:runner-token"]
		}
		if tok != "" && s.tokenStore != nil {
			_ = s.tokenStore.Set(inst.ID, tok)
		}
		recNode.RunnerAuthToken = tok
	}

	// Gather containers outside s.mu lock to avoid stalling HTTP handlers on unreachable nodes
	var containers []*ContainerStatusResult
	if s.runner != nil && recNode != nil {
		containers, _ = s.runner.ListContainers(ctx, recNode)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Double check tombstones under lock
	if s.isTombstonedLocked(podUID) || s.isTombstonedLocked(inst.ID) {
		return false
	}
	if s.completedPods[podUID] != nil || (s.completedStore != nil && s.completedStore.Get(podUID) != nil) {
		return false
	}
	if s.instances[podUID] != nil || s.instances[inst.ID] != nil {
		return true
	}

	key := fmt.Sprintf("%s/%s", ns, podName)
	s.instances[key] = inst
	s.instances[podUID] = inst
	s.instances[inst.ID] = inst

	if s.nodePool != nil && recNode != nil {
		// Reconstruct allocations: allocate primary pod
		s.nodePool.RegisterAndAllocate(recNode, defaultVec, podUID, ns, podName)

		// Reconstruct allocations for any co-tenant containers running in this VM
		for _, c := range containers {
			if c != nil && c.PodUID != "" && c.PodUID != podUID && !s.isTombstonedLocked(c.PodUID) {
				s.instances[c.PodUID] = inst
				s.nodePool.RegisterAndAllocate(recNode, defaultVec, c.PodUID, ns, c.PodUID)
			}
		}
	}

	return true
}

// RecoverActiveInstances queries the cloud inventory to rebuild in-memory state on startup.
func (s *Server) RecoverActiveInstances(ctx context.Context, inv CloudInventory) error {
	if inv == nil {
		return nil
	}
	instances, err := inv.ListActiveInstances(ctx, s.clusterTag)
	if err != nil {
		return fmt.Errorf("failed to recover cloud instances: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, inst := range instances {
		podUID := inst.Metadata["podUID"]
		if s.isTombstonedLocked(podUID) || s.isTombstonedLocked(inst.ID) {
			cleanID := strings.ReplaceAll(strings.ReplaceAll(inst.ID, "\n", ""), "\r", "")
			cleanUID := strings.ReplaceAll(strings.ReplaceAll(podUID, "\n", ""), "\r", "")
			log.Printf("[cloudvm] Skipping tombstoned instance %s (pod %s) during startup recovery", cleanID, cleanUID) //nolint:gosec // cleanID, cleanUID sanitized
			continue
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
		marketType := inst.Metadata["marketType"]
		if marketType == "" {
			marketType = "on-demand"
		}
		inst.Metadata["marketType"] = marketType

		key := fmt.Sprintf("%s/%s", ns, podName)
		s.instances[key] = inst
		if podUID == "" {
			podUID = key
		} else {
			s.instances[podUID] = inst
		}
		s.instances[inst.ID] = inst
		defaultVec, _ := NewResourceVector("2", "4Gi", 0, 0)
		capVec := CapacityForInstanceType(inst.InstanceType, defaultVec)
		recNode := NewCloudVMNode(inst.ID, inst.Provider, inst.Region, inst.IP, capVec)
		recNode.Namespace = ns
		recNode.Spot = (marketType == "spot")
		recNode.Cordoned = true
		tok := ""
		if s.tokenStore != nil {
			tok = s.tokenStore.Get(inst.ID)
			if tok == "" && podUID != "" {
				tok = s.tokenStore.Get(podUID)
			}
			if tok != "" && s.tokenStore.Get(inst.ID) == "" {
				_ = s.tokenStore.Set(inst.ID, tok)
			}
		}
		if tok == "" {
			tok = inst.Metadata["runnerToken"]
		}
		if tok == "" {
			tok = inst.Metadata["cloudvm:runner-token"]
		}
		if tok != "" && s.tokenStore != nil {
			_ = s.tokenStore.Set(inst.ID, tok)
		}
		recNode.RunnerAuthToken = tok
		s.nodePool.RegisterAndAllocate(recNode, defaultVec, podUID, ns, podName)

		if s.runner != nil {
			containers, err := s.runner.ListContainers(ctx, recNode)
			if err == nil {
				for _, c := range containers {
					if c.PodUID != "" && c.PodUID != podUID && !s.isTombstonedLocked(c.PodUID) {
						s.instances[c.PodUID] = inst
						s.nodePool.RegisterAndAllocate(recNode, defaultVec, c.PodUID, ns, c.PodUID)
					}
				}
			} else {
				recNode.UnknownCoTenants = true
				cleanID := strings.ReplaceAll(strings.ReplaceAll(inst.ID, "\n", ""), "\r", "")
				log.Printf("[cloudvm] Warning: failed to list containers on recovered VM %s: %v. Flagged with UnknownCoTenants.", cleanID, err) //nolint:gosec // cleanID sanitized
			}
		}
		cleanID := strings.ReplaceAll(strings.ReplaceAll(inst.ID, "\n", ""), "\r", "")
		cleanKey := strings.ReplaceAll(strings.ReplaceAll(key, "\n", ""), "\r", "")
		log.Printf("[cloudvm] Recovered existing cloud VM %s for pod %s", cleanID, cleanKey)
	}
	return nil
}

func (s *Server) handleGetLogs(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("[cloudvm] Logs streamed directly via cluster logging stack or cloudwatch.\n"))
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok"))
}

// ActiveInstances returns a snapshot of known active instances.
func (s *Server) ActiveInstances() map[string]*Instance {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]*Instance, len(s.instances))
	for k, v := range s.instances {
		out[k] = v
	}
	return out
}

// GetPodRegion returns the cloud region associated with podKey.
func (s *Server) GetPodRegion(_ context.Context, podKey string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	inst, ok := s.instances[podKey]
	if !ok || inst == nil {
		return "", false
	}
	return inst.Region, true
}

// GetPodCreatedAt returns the creation timestamp of the instance associated with podKey.
func (s *Server) GetPodCreatedAt(_ context.Context, podKey string) (time.Time, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	inst, ok := s.instances[podKey]
	if !ok || inst == nil {
		return time.Time{}, false
	}
	return inst.CreatedAt, true
}

// GetActivePods returns a map of genuinely active (running/pending) pod UIDs to their cloud instance IDs.
func (s *Server) GetActivePods(_ context.Context) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make(map[string]string)
	for k, inst := range s.instances {
		if inst == nil {
			continue
		}
		uid := k
		if inst.Metadata != nil && inst.Metadata["podUID"] != "" {
			uid = inst.Metadata["podUID"]
		}
		// If pod has been tombstoned (explicitly deleted), skip it!
		if s.isTombstonedLocked(uid) {
			continue
		}
		// If pod is already recorded completed or terminated, skip it!
		if s.completedPods[uid] != nil || (s.completedStore != nil && s.completedStore.Get(uid) != nil) {
			continue
		}
		// Check nodepool allocation completion
		if s.nodePool != nil {
			if _, alloc, found := s.nodePool.FindPodNode(uid); found && alloc.Completed {
				continue
			}
		}
		// Only track active running or provisioning instances
		if inst.State == StateRunning || inst.State == StateProvisioning {
			res[uid] = inst.ID
		}
	}
	return res, nil
}

// MarkPodFailed marks a pod instance state as failed and commits terminal execution telemetry.
func (s *Server) MarkPodFailed(_ context.Context, podKey string, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var uid string
	var jid string
	var inst *Instance
	if i, ok := s.instances[podKey]; ok {
		inst = i
		if inst.Metadata != nil {
			uid = inst.Metadata["podUID"]
		}
	}
	if uid == "" {
		uid = podKey
	}

	// 1. Guard against overwriting successful completed pods
	if existing := s.completedPods[uid]; existing != nil && existing.Reason == "Completed" {
		return nil
	}
	if s.completedStore != nil {
		if rec := s.completedStore.Get(uid); rec != nil && rec.Reason == "Completed" {
			return nil
		}
	}

	// 2. Guard against idempotent duplicate calls on already-failed pods
	if existing := s.completedPods[uid]; existing != nil && (existing.Reason == "SpotInterruption" || existing.Reason == "NodeLost") {
		return nil
	}

	// 3. Distinguish SpotInterruption vs NodeLost vs AWS StateReason
	if inst != nil && inst.Metadata != nil {
		if code := inst.Metadata["stateReasonCode"]; code != "" {
			if strings.Contains(code, "SpotInstanceTermination") {
				reason = "SpotInterruption"
			} else {
				reason = code
			}
		} else if reason == "SpotInterruption" {
			if inst.Metadata["marketType"] == "on-demand" {
				reason = "NodeLost"
			}
		}
	}

	if inst != nil {
		inst.State = StateFailed
		if inst.Metadata == nil {
			inst.Metadata = make(map[string]string)
		}
		inst.Metadata["failureReason"] = reason
		jid = fmt.Sprintf("%s:%s", inst.ID, uid)
	} else {
		jid = fmt.Sprintf("cvm-%s", uid)
	}

	exitCode := int32(137)
	finAt := time.Now()
	term := &ContainerTerminatedState{
		ExitCode:   exitCode,
		Reason:     reason,
		Message:    fmt.Sprintf("Underlying cloud VM terminated (%s)", reason),
		FinishedAt: finAt,
	}
	s.completedPods[uid] = term

	if s.nodePool != nil {
		s.nodePool.MarkPodCompleted(uid, exitCode, reason, term.Message, finAt)
		if inst != nil {
			if node, _, found := s.nodePool.FindPodNode(uid); !found || node.ActivePodCount() == 0 {
				s.nodePool.UnregisterNode(inst.ID)
			}
		}
	}
	if s.completedStore != nil {
		_ = s.completedStore.Set(uid, &CompletedPodRecord{
			PodUID:     uid,
			PodJID:     jid,
			ExitCode:   exitCode,
			Reason:     reason,
			Message:    term.Message,
			FinishedAt: finAt,
		})
	}
	return nil
}

// MaxUnknownCoTenantRetries defines the retry budget before an unverified node is released for garbage collection.
const MaxUnknownCoTenantRetries = 10

// RetryUnknownCoTenants attempts to query the in-VM runner for nodes whose startup listing failed.
func (s *Server) RetryUnknownCoTenants(ctx context.Context) {
	if s.nodePool == nil || s.runner == nil {
		return
	}
	nodes := s.nodePool.NodesWithUnknownCoTenants()
	for _, node := range nodes {
		containers, err := s.runner.ListContainers(ctx, node)
		if err == nil {
			s.mu.Lock()
			defaultVec, _ := NewResourceVector("2", "4Gi", 0, 0)
			for _, c := range containers {
				if c != nil && c.PodUID != "" && !s.isTombstonedLocked(c.PodUID) {
					if _, _, exists := s.nodePool.FindPodNode(c.PodUID); !exists {
						if inst := s.instances[node.InstanceID]; inst != nil {
							s.instances[c.PodUID] = inst
						}
						s.nodePool.RegisterAndAllocate(node, defaultVec, c.PodUID, node.Namespace, c.PodUID)
					}
				}
			}
			s.mu.Unlock()
			s.nodePool.SetUnknownCoTenants(node.InstanceID, false)
			cleanID := strings.ReplaceAll(strings.ReplaceAll(node.InstanceID, "\n", ""), "\r", "")
			log.Printf("[cloudvm] Successfully resolved co-tenants for node %s via background reconciliation", cleanID) //nolint:gosec // cleanID sanitized
		} else {
			retries, exceeded := s.nodePool.IncrementUnknownCoTenantRetries(node.InstanceID, MaxUnknownCoTenantRetries)
			cleanID := strings.ReplaceAll(strings.ReplaceAll(node.InstanceID, "\n", ""), "\r", "")
			if exceeded {
				log.Printf("[cloudvm] ERROR: node %s exceeded %d co-tenant reconciliation retries (err: %v). Clearing unknown flag to allow garbage collection.", cleanID, retries, err) //nolint:gosec // cleanID sanitized
			} else {
				log.Printf("[cloudvm] Retrying co-tenant discovery for node %s (attempt %d/%d): %v", cleanID, retries, MaxUnknownCoTenantRetries, err) //nolint:gosec // cleanID sanitized
			}
		}
	}
}
