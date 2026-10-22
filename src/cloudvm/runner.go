// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// StartContainerRequest contains the parameters required to execute a sliced container on a warm VM.
type StartContainerRequest struct {
	PodUID           string            `json:"podUID"`
	Namespace        string            `json:"namespace"`
	PodName          string            `json:"podName"`
	ContainerName    string            `json:"containerName"`
	Image            string            `json:"image"`
	Command          []string          `json:"command,omitempty"`
	Args             []string          `json:"args,omitempty"`
	Env              map[string]string `json:"env,omitempty"`
	CPUMillicores    int64             `json:"cpuMillicores"`
	MemoryBytes      int64             `json:"memoryBytes"`
	AssignedGPUs     []int             `json:"assignedGPUs,omitempty"`
	AssignedTPUs     []int             `json:"assignedTPUs,omitempty"`
	RunAsUser        *int64            `json:"runAsUser,omitempty"`
	RunAsGroup       *int64            `json:"runAsGroup,omitempty"`
	DockerConfigJSON string            `json:"dockerConfigJSON,omitempty"`
}

// ContainerStatusResult encapsulates container execution telemetry from the in-VM runner.
type ContainerStatusResult struct {
	PodUID     string    `json:"podUID"`
	Running    bool      `json:"running"`
	Status     string    `json:"status,omitempty"` // "created", "running", "exited", "dead"
	ExitCode   int32     `json:"exitCode"`
	Reason     string    `json:"reason,omitempty"`
	Message    string    `json:"message,omitempty"`
	StartedAt  time.Time `json:"startedAt"`
	FinishedAt time.Time `json:"finishedAt,omitempty"`
}

// ContainerRunner abstracts remote container lifecycle execution on a warm cloud node.
type ContainerRunner interface {
	StartContainer(ctx context.Context, node *CloudVMNode, req *StartContainerRequest) error
	GetStatus(ctx context.Context, node *CloudVMNode, podUID string) (*ContainerStatusResult, error)
	StopContainer(ctx context.Context, node *CloudVMNode, podUID string) error
	ListContainers(ctx context.Context, node *CloudVMNode) ([]*ContainerStatusResult, error)
}

// HTTPContainerRunner connects to the in-VM micro-runner daemon over the private WireGuard mesh.
type HTTPContainerRunner struct {
	client *http.Client
	port   int
}

// NewHTTPContainerRunner creates an HTTPContainerRunner.
func NewHTTPContainerRunner(port int) *HTTPContainerRunner {
	if port <= 0 {
		port = 9090
	}
	return &HTTPContainerRunner{
		port: port,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

func (h *HTTPContainerRunner) setAuthHeader(httpReq *http.Request, node *CloudVMNode) {
	if node != nil && node.RunnerAuthToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+node.RunnerAuthToken)
	}
}

func (h *HTTPContainerRunner) endpoint(node *CloudVMNode, path string) (string, error) {
	if node == nil || node.MeshIP == "" {
		return "", fmt.Errorf("node has no mesh or internal IP assigned")
	}
	return fmt.Sprintf("http://%s:%d%s", node.MeshIP, h.port, path), nil
}

func (h *HTTPContainerRunner) StartContainer(ctx context.Context, node *CloudVMNode, req *StartContainerRequest) error {
	if req != nil {
		if req.RunAsUser != nil && *req.RunAsUser == 0 {
			return fmt.Errorf("cloudvm provider enforces non-root containers; runAsUser 0 is forbidden on host network")
		}
		if req.RunAsGroup != nil && *req.RunAsGroup == 0 {
			return fmt.Errorf("cloudvm provider enforces non-root containers; runAsGroup 0 is forbidden on host network")
		}
	}
	payload, err := json.Marshal(req)
	if err != nil {
		return err
	}

	url, err := h.endpoint(node, "/containers/start")
	if err != nil {
		return err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload)) //nolint:gosec // mesh endpoint is an internal private WireGuard IP
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	h.setAuthHeader(httpReq, node)

	resp, err := h.client.Do(httpReq) //nolint:gosec // internal mesh call
	if err != nil {
		return fmt.Errorf("failed to contact in-VM runner at %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("in-VM runner error (%d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

func (h *HTTPContainerRunner) GetStatus(ctx context.Context, node *CloudVMNode, podUID string) (*ContainerStatusResult, error) {
	baseEndpoint, err := h.endpoint(node, "/containers/status")
	if err != nil {
		return nil, err
	}
	url := fmt.Sprintf("%s?podUID=%s", baseEndpoint, podUID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil) //nolint:gosec // internal private WireGuard mesh endpoint
	if err != nil {
		return nil, err
	}
	h.setAuthHeader(httpReq, node)

	resp, err := h.client.Do(httpReq) //nolint:gosec // internal mesh call
	if err != nil {
		return nil, fmt.Errorf("failed to query in-VM runner at %s: %w", url, err)
	}
	defer resp.Body.Close()

	var result ContainerStatusResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

func (h *HTTPContainerRunner) StopContainer(ctx context.Context, node *CloudVMNode, podUID string) error {
	baseEndpoint, err := h.endpoint(node, "/containers/stop")
	if err != nil {
		return err
	}
	url := fmt.Sprintf("%s?podUID=%s", baseEndpoint, podUID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil) //nolint:gosec // internal private WireGuard mesh endpoint
	if err != nil {
		return err
	}
	h.setAuthHeader(httpReq, node)

	resp, err := h.client.Do(httpReq) //nolint:gosec // internal mesh call
	if err != nil {
		return fmt.Errorf("failed to stop container on VM at %s: %w", url, err)
	}
	defer resp.Body.Close()
	return nil
}

func (h *HTTPContainerRunner) ListContainers(ctx context.Context, node *CloudVMNode) ([]*ContainerStatusResult, error) {
	url, err := h.endpoint(node, "/containers/list")
	if err != nil {
		return nil, err
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil) //nolint:gosec // internal mesh call
	if err != nil {
		return nil, err
	}
	h.setAuthHeader(httpReq, node)

	resp, err := h.client.Do(httpReq) //nolint:gosec // internal mesh call
	if err != nil {
		return nil, fmt.Errorf("failed to query in-VM runner list at %s: %w", url, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("in-VM runner list error: status %d", resp.StatusCode)
	}

	var results []*ContainerStatusResult
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return nil, err
	}
	return results, nil
}

// MockContainerRunner simulates in-VM container execution for tests and mock environments.
type MockContainerRunner struct {
	mu         sync.RWMutex
	containers map[string]*ContainerStatusResult
	requests   map[string]*StartContainerRequest
}

// NewMockContainerRunner initializes an in-memory mock container runner.
func NewMockContainerRunner() *MockContainerRunner {
	return &MockContainerRunner{
		containers: make(map[string]*ContainerStatusResult),
		requests:   make(map[string]*StartContainerRequest),
	}
}

func (m *MockContainerRunner) StartContainer(_ context.Context, _ *CloudVMNode, req *StartContainerRequest) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.requests == nil {
		m.requests = make(map[string]*StartContainerRequest)
	}
	m.requests[req.PodUID] = req
	m.containers[req.PodUID] = &ContainerStatusResult{
		PodUID:    req.PodUID,
		Running:   true,
		StartedAt: time.Now(),
	}
	return nil
}

// GetStartRequest retrieves the StartContainerRequest recorded for podUID.
func (m *MockContainerRunner) GetStartRequest(podUID string) *StartContainerRequest {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.requests[podUID]
}

func (m *MockContainerRunner) GetStatus(_ context.Context, _ *CloudVMNode, podUID string) (*ContainerStatusResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if c, ok := m.containers[podUID]; ok {
		return c, nil
	}
	return &ContainerStatusResult{
		PodUID:  podUID,
		Running: true,
	}, nil
}

func (m *MockContainerRunner) StopContainer(_ context.Context, _ *CloudVMNode, podUID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if c, ok := m.containers[podUID]; ok {
		c.Running = false
		c.ExitCode = 0
		c.Reason = "Completed"
		c.FinishedAt = time.Now()
	}
	return nil
}

func (m *MockContainerRunner) ListContainers(_ context.Context, _ *CloudVMNode) ([]*ContainerStatusResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*ContainerStatusResult
	for _, c := range m.containers {
		out = append(out, c)
	}
	return out, nil
}

func (m *MockContainerRunner) SetStatus(podUID string, res *ContainerStatusResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.containers[podUID] = res
}
