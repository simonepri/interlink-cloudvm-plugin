// Package main implements the interlink-provider-cloudvm daemon.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"
)

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
type Server struct {
	mu          sync.RWMutex
	providers   map[ProviderType]Provider
	defaultProv Provider
	instances   map[string]*Instance // key: namespace/podName -> *Instance
	clusterTag  string
	reconciler  *Reconciler
}

// NewServer initializes an InterLink Server with a default provider.
func NewServer(defaultProvider Provider, clusterTag string) *Server {
	return &Server{
		providers:   make(map[ProviderType]Provider),
		defaultProv: defaultProvider,
		instances:   make(map[string]*Instance),
		clusterTag:  clusterTag,
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
	return mux
}

// PodToInstanceSpec converts an incoming InterLink Pod into an InstanceSpec.
func PodToInstanceSpec(pod *Pod, defaultClusterTag string) *InstanceSpec {
	ns := pod.Metadata.Namespace
	if ns == "" {
		ns = "default"
	}
	name := pod.Metadata.Name
	uid := pod.Metadata.UID
	if uid == "" {
		uid = pod.Metadata.Annotations["kubernetes.io/pod-uid"]
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
		region = "us-east-1"
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
		ClusterTag:           defaultClusterTag,
	}
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

	spec := PodToInstanceSpec(pod, s.clusterTag)
	prov := s.selectProvider(spec)

	log.Printf("[cloudvm] Launching opportunistic VM for pod %s/%s (type=%s, gpus=%d, region=%s)...",
		spec.PodNamespace, spec.PodName, spec.InstanceType, spec.NumGPUs, spec.Region)

	inst, err := prov.Launch(r.Context(), spec)
	if err != nil {
		log.Printf("[cloudvm] Failed to launch VM for %s: %v", spec.PodName, err)
		http.Error(w, fmt.Sprintf("launch failed: %v", err), http.StatusInternalServerError)
		return
	}

	s.mu.Lock()
	key := fmt.Sprintf("%s/%s", spec.PodNamespace, spec.PodName)
	s.instances[key] = inst
	s.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	// Return InterLink CreateStruct contract
	_ = json.NewEncoder(w).Encode(CreateStruct{
		PodUID: spec.PodUID,
		PodJID: inst.ID,
	})
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
	inst, exists := s.instances[key]
	if !exists {
		inst, exists = s.instances[podName] // fallback for legacy keys
	}
	delete(s.instances, key)
	delete(s.instances, podName)
	s.mu.Unlock()

	if !exists || inst == nil {
		w.WriteHeader(http.StatusOK)
		return
	}

	prov := s.defaultProv
	s.mu.RLock()
	if p, ok := s.providers[inst.Provider]; ok {
		prov = p
	}
	s.mu.RUnlock()

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

	s.mu.RLock()
	defer s.mu.RUnlock()

	if len(pods) == 0 {
		for key, inst := range s.instances {
			parts := strings.SplitN(key, "/", 2)
			ns := "default"
			name := key
			if len(parts) == 2 {
				ns = parts[0]
				name = parts[1]
			}
			uid := ""
			if inst.Metadata != nil {
				uid = inst.Metadata["podUID"]
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

	var responses []PodStatusResponse
	for _, pod := range pods {
		name := pod.Metadata.Name
		ns := pod.Metadata.Namespace
		if ns == "" {
			ns = "default"
		}
		key := fmt.Sprintf("%s/%s", ns, name)
		inst, exists := s.instances[key]
		if !exists {
			inst, exists = s.instances[name] // fallback
		}

		status := ContainerStatus{
			Name: "worker",
		}

		jid := ""
		if exists && inst != nil {
			jid = inst.ID
			switch inst.State {
			case StateProvisioning:
				status.State.Waiting = &ContainerWaitingState{
					Reason:  "ContainerCreating",
					Message: fmt.Sprintf("Provisioning cloud VM %s in %s...", inst.ID, inst.Region),
				}
			case StateRunning:
				status.Ready = true
				status.State.Running = &ContainerRunningState{
					StartedAt: inst.CreatedAt,
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
		} else {
			status.State.Waiting = &ContainerWaitingState{
				Reason:  "ContainerCreating",
				Message: fmt.Sprintf("Instance for %s is pending discovery or provisioning", key),
			}
		}

		uid := pod.Metadata.UID
		if uid == "" && exists && inst.Metadata != nil {
			uid = inst.Metadata["podUID"]
		}

		responses = append(responses, PodStatusResponse{
			PodUID:    uid,
			Name:      name,
			Namespace: ns,
			JID:       jid,
			Containers: []ContainerStatus{
				status,
			},
		})
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(responses)
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
		key := fmt.Sprintf("%s/%s", ns, podName)
		s.instances[key] = inst
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

// GetActivePods returns a map of active pod keys known to the server.
func (s *Server) GetActivePods(_ context.Context) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make(map[string]string)
	for k, inst := range s.instances {
		res[k] = inst.ID
	}
	return res, nil
}

// MarkPodFailed marks a pod instance state as failed.
func (s *Server) MarkPodFailed(_ context.Context, podKey string, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if inst, ok := s.instances[podKey]; ok {
		inst.State = StateFailed
		if inst.Metadata == nil {
			inst.Metadata = make(map[string]string)
		}
		inst.Metadata["failureReason"] = reason
	}
	return nil
}
