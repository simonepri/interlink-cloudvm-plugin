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

// Server implements the InterLink REST API for the cloudvm provider plugin.
type Server struct {
	mu          sync.RWMutex
	providers   map[ProviderType]Provider
	defaultProv Provider
	instances   map[string]*Instance // key: podName -> *Instance
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

	// 1. Region
	region := pod.Metadata.Annotations["cloudvm-job.vk.io/region"]
	if region == "" {
		region = pod.Spec.NodeSelector["topology.kubernetes.io/region"]
	}
	if region == "" {
		region = "us-east-1"
	}

	// Candidate regions for multi-region spot arbitrage
	var candidateRegions []string
	if regionsStr := pod.Metadata.Annotations["cloudvm-job.vk.io/regions"]; regionsStr != "" {
		for _, r := range strings.Split(regionsStr, ",") {
			r = strings.TrimSpace(r)
			if r != "" {
				candidateRegions = append(candidateRegions, r)
			}
		}
	}

	// 2. Instance Type
	instanceType := pod.Metadata.Annotations["cloudvm-job.vk.io/instance-type"]
	if instanceType == "" {
		instanceType = pod.Spec.NodeSelector["node.kubernetes.io/instance-type"]
	}
	if instanceType == "" {
		instanceType = "g5.xlarge"
	}

	// 3. Market / Capacity Type (spot vs on-demand)
	marketType := pod.Metadata.Annotations["cloudvm-job.vk.io/capacity-type"]
	if marketType == "" {
		marketType = pod.Spec.NodeSelector["cloudvm.vk.io/capacity-type"]
	}
	if marketType == "" {
		marketType = "spot"
	}

	// 4. AMI Mode (dlami vs container)
	amiMode := pod.Metadata.Annotations["cloudvm-job.vk.io/ami-mode"]
	if amiMode == "" {
		amiMode = "container"
	}

	// 5. Max Spot Price bid
	maxSpotPrice := pod.Metadata.Annotations["cloudvm-job.vk.io/max-spot-price"]

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
		Region:               region,
		CandidateRegions:     candidateRegions,
		InstanceType:         instanceType,
		MarketType:           marketType,
		AMIMode:              amiMode,
		MaxSpotPrice:         maxSpotPrice,
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
	if p, ok := s.providers[ProviderType(spec.MarketType)]; ok {
		return p
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

	var pod Pod
	if err := json.Unmarshal(body, &pod); err != nil {
		http.Error(w, fmt.Sprintf("invalid pod JSON: %v", err), http.StatusBadRequest)
		return
	}

	spec := PodToInstanceSpec(&pod, s.clusterTag)
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
	s.instances[spec.PodName] = inst
	s.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(inst)
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

	var pod Pod
	if err := json.Unmarshal(body, &pod); err != nil {
		http.Error(w, "invalid pod JSON", http.StatusBadRequest)
		return
	}

	podName := pod.Metadata.Name
	s.mu.Lock()
	inst, exists := s.instances[podName]
	delete(s.instances, podName)
	s.mu.Unlock()

	if !exists {
		w.WriteHeader(http.StatusOK)
		return
	}

	prov := s.selectProvider(&InstanceSpec{})
	if err := prov.Terminate(r.Context(), inst.ID); err != nil {
		log.Printf("[cloudvm] Error terminating instance %s: %v", inst.ID, err)
	}

	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	var pods []Pod
	if err := json.Unmarshal(body, &pods); err != nil {
		var single Pod
		if err2 := json.Unmarshal(body, &single); err2 != nil {
			http.Error(w, "invalid status request JSON", http.StatusBadRequest)
			return
		}
		pods = append(pods, single)
	}

	var responses []PodStatusResponse
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, pod := range pods {
		name := pod.Metadata.Name
		ns := pod.Metadata.Namespace
		inst, exists := s.instances[name]

		status := ContainerStatus{
			Name: "worker",
		}

		if !exists {
			status.State.Terminated = &ContainerTerminatedState{
				ExitCode:   0,
				Reason:     "Completed",
				FinishedAt: time.Now(),
			}
		} else {
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
			case StateTerminating, StateTerminated:
				status.State.Terminated = &ContainerTerminatedState{
					ExitCode:   0,
					Reason:     "Terminated",
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
		}

		responses = append(responses, PodStatusResponse{
			PodUID:     pod.Metadata.UID,
			Name:       name,
			Namespace:  ns,
			Containers: []ContainerStatus{status},
		})
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(responses)
}

func (s *Server) handleGetLogs(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("InterLink CloudVM log streaming active.\n"))
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("OK"))
}

func (s *Server) GetActivePods(_ context.Context) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	active := make(map[string]string)
	for podName, inst := range s.instances {
		active[podName] = inst.ID
	}
	return active, nil
}

func (s *Server) MarkPodFailed(_ context.Context, podName string, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if inst, ok := s.instances[podName]; ok {
		inst.State = StateFailed
	}
	return nil
}
