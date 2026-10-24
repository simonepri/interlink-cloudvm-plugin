// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/yamux"
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
	PodUID         string    `json:"podUID"`
	Running        bool      `json:"running"`
	Status         string    `json:"status,omitempty"` // "created", "running", "exited", "dead"
	ExitCode       int32     `json:"exitCode"`
	Reason         string    `json:"reason,omitempty"`
	Message        string    `json:"message,omitempty"`
	StartedAt      time.Time `json:"startedAt"`
	FinishedAt     time.Time `json:"finishedAt,omitempty"`
	NetworkRxBytes int64     `json:"networkRxBytes,omitempty"`
	NetworkTxBytes int64     `json:"networkTxBytes,omitempty"`
}

// HostStatsResult encapsulates VM host-level telemetry from the in-VM runner.
type HostStatsResult struct {
	NetworkRxBytes int64 `json:"networkRxBytes"`
	NetworkTxBytes int64 `json:"networkTxBytes"`
}

// ContainerRunner abstracts remote container lifecycle execution on a warm cloud node.
type ContainerRunner interface {
	StartContainer(ctx context.Context, node *CloudVMNode, req *StartContainerRequest) error
	GetStatus(ctx context.Context, node *CloudVMNode, podUID string) (*ContainerStatusResult, error)
	StopContainer(ctx context.Context, node *CloudVMNode, podUID string) error
	ListContainers(ctx context.Context, node *CloudVMNode) ([]*ContainerStatusResult, error)
	GetHostStats(ctx context.Context, node *CloudVMNode) (*HostStatsResult, error)
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
	if node == nil || node.GetMeshIP() == "" {
		return "", fmt.Errorf("node has no mesh or internal IP assigned")
	}
	return fmt.Sprintf("http://%s:%d%s", node.GetMeshIP(), h.port, path), nil
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
	defer func() { _ = resp.Body.Close() }()

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
	defer func() { _ = resp.Body.Close() }()

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
	defer func() { _ = resp.Body.Close() }()
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
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("in-VM runner list error: status %d", resp.StatusCode)
	}

	var results []*ContainerStatusResult
	if err := json.NewDecoder(resp.Body).Decode(&results); err != nil {
		return nil, err
	}
	return results, nil
}

func (h *HTTPContainerRunner) GetHostStats(ctx context.Context, node *CloudVMNode) (*HostStatsResult, error) {
	url, err := h.endpoint(node, "/host/stats")
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
		return nil, fmt.Errorf("failed to query in-VM runner host stats at %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("in-VM runner host stats error: status %d", resp.StatusCode)
	}

	var result HostStatsResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// MockContainerRunner simulates in-VM container execution for tests and mock environments.
type MockContainerRunner struct {
	mu         sync.RWMutex
	containers map[string]*ContainerStatusResult
	requests   map[string]*StartContainerRequest
	hostStats  *HostStatsResult
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

func (m *MockContainerRunner) SetHostStats(stats *HostStatsResult) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hostStats = stats
}

func (m *MockContainerRunner) GetHostStats(_ context.Context, _ *CloudVMNode) (*HostStatsResult, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.hostStats != nil {
		return m.hostStats, nil
	}
	return &HostStatsResult{}, nil
}

// FormatTunnelEndpoint constructs the reverse tunnel gateway address for a given cell.
func FormatTunnelEndpoint(cell string) string {
	return fmt.Sprintf("clvm-tunnel.%s.c.corp.boltz.com:443", cell)
}

// ComputeJitter applies random jitter within [1-fraction, 1+fraction] to the provided duration.
func ComputeJitter(d time.Duration, fraction float64) time.Duration {
	if fraction <= 0 {
		return d
	}
	if fraction > 1.0 {
		fraction = 1.0
	}
	var b [8]byte
	_, _ = rand.Read(b[:])
	r := float64(binary.BigEndian.Uint64(b[:])>>11) / (1 << 53)
	factor := (1.0 - fraction) + (2.0 * fraction * r)
	jittered := time.Duration(float64(d) * factor)
	if jittered < 0 {
		return 0
	}
	return jittered
}

// ContainerRunnerHandler defines the operations an in-VM runner executes locally.
type ContainerRunnerHandler interface {
	StartContainer(ctx context.Context, req *StartContainerRequest) error
	GetStatus(ctx context.Context, podUID string) (*ContainerStatusResult, error)
	StopContainer(ctx context.Context, podUID string) error
	ListContainers(ctx context.Context) ([]*ContainerStatusResult, error)
	GetHostStats(ctx context.Context) (*HostStatsResult, error)
}

// RunnerHandlerAdapter adapts a ContainerRunner and CloudVMNode into a ContainerRunnerHandler.
type RunnerHandlerAdapter struct {
	Runner ContainerRunner
	Node   *CloudVMNode
}

// StartContainer adapts ContainerRunner.StartContainer.
func (a *RunnerHandlerAdapter) StartContainer(ctx context.Context, req *StartContainerRequest) error {
	return a.Runner.StartContainer(ctx, a.Node, req)
}

// GetStatus adapts ContainerRunner.GetStatus.
func (a *RunnerHandlerAdapter) GetStatus(ctx context.Context, podUID string) (*ContainerStatusResult, error) {
	return a.Runner.GetStatus(ctx, a.Node, podUID)
}

// StopContainer adapts ContainerRunner.StopContainer.
func (a *RunnerHandlerAdapter) StopContainer(ctx context.Context, podUID string) error {
	return a.Runner.StopContainer(ctx, a.Node, podUID)
}

// ListContainers adapts ContainerRunner.ListContainers.
func (a *RunnerHandlerAdapter) ListContainers(ctx context.Context) ([]*ContainerStatusResult, error) {
	return a.Runner.ListContainers(ctx, a.Node)
}

// GetHostStats adapts ContainerRunner.GetHostStats.
func (a *RunnerHandlerAdapter) GetHostStats(ctx context.Context) (*HostStatsResult, error) {
	return a.Runner.GetHostStats(ctx, a.Node)
}

// NewRunnerHTTPMux constructs an HTTP mux serving the runner API endpoints.
func NewRunnerHTTPMux(handler ContainerRunnerHandler, authToken string) *http.ServeMux {
	mux := http.NewServeMux()

	checkAuth := func(r *http.Request) bool {
		if authToken == "" {
			return true
		}
		hdr := r.Header.Get("Authorization")
		return hdr == "Bearer "+authToken
	}

	mux.HandleFunc("/containers/start", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !checkAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		var req StartContainerRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, fmt.Sprintf("invalid request payload: %v", err), http.StatusBadRequest)
			return
		}
		if err := handler.StartContainer(r.Context(), &req); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/containers/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !checkAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		podUID := r.URL.Query().Get("podUID")
		if podUID == "" {
			http.Error(w, "missing podUID query parameter", http.StatusBadRequest)
			return
		}
		status, err := handler.GetStatus(r.Context(), podUID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(status)
	})

	mux.HandleFunc("/containers/stop", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !checkAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		podUID := r.URL.Query().Get("podUID")
		if podUID == "" {
			var body struct {
				PodUID string `json:"podUID"`
			}
			_ = json.NewDecoder(r.Body).Decode(&body)
			podUID = body.PodUID
		}
		if podUID == "" {
			http.Error(w, "missing podUID", http.StatusBadRequest)
			return
		}
		if err := handler.StopContainer(r.Context(), podUID); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/containers/list", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !checkAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		containers, err := handler.ListContainers(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(containers)
	})

	mux.HandleFunc("/host/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if !checkAuth(r) {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		stats, err := handler.GetHostStats(r.Context())
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(stats)
	})

	return mux
}

// ReverseTunnelDialerConfig configures the reverse dialer connecting to the tunnel gateway.
type ReverseTunnelDialerConfig struct {
	Endpoint       string
	Cell           string
	InstanceID     string
	AuthToken      string
	Platform       string // "aws" or "gcp"
	Attestation    []byte
	TLSConfig      *tls.Config
	DialContext    func(ctx context.Context, network, addr string) (net.Conn, error)
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	BackoffFactor  float64
	JitterFraction float64
	YamuxConfig    *yamux.Config
}

// ReverseTunnelDialer continuously dials the reverse tunnel gateway and serves the runner HTTP mux.
type ReverseTunnelDialer struct {
	cfg ReverseTunnelDialerConfig
}

// NewReverseTunnelDialer creates a new ReverseTunnelDialer.
func NewReverseTunnelDialer(cfg ReverseTunnelDialerConfig) (*ReverseTunnelDialer, error) {
	if cfg.Endpoint == "" {
		if cfg.Cell != "" {
			cfg.Endpoint = FormatTunnelEndpoint(cfg.Cell)
		} else {
			cfg.Endpoint = "clvm-tunnel.default.c.corp.boltz.com:443"
		}
	}
	if cfg.InstanceID == "" {
		return nil, errors.New("instanceID is required")
	}
	if cfg.InitialBackoff <= 0 {
		cfg.InitialBackoff = 500 * time.Millisecond
	}
	if cfg.MaxBackoff <= 0 {
		cfg.MaxBackoff = 30 * time.Second
	}
	if cfg.BackoffFactor <= 1.0 {
		cfg.BackoffFactor = 2.0
	}
	if cfg.JitterFraction <= 0 {
		cfg.JitterFraction = 0.2
	}
	if cfg.YamuxConfig == nil {
		cfg.YamuxConfig = DefaultYamuxConfig()
	}
	return &ReverseTunnelDialer{cfg: cfg}, nil
}

// Run starts the dialer loop with exponential backoff and jitter upon disconnect.
func (d *ReverseTunnelDialer) Run(ctx context.Context, handler http.Handler) error {
	backoff := d.cfg.InitialBackoff
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		err := d.DialAndServeOnce(ctx, handler)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			log.Printf("[cloudvm-runner] Reverse tunnel disconnected: %v. Reconnecting...", err)
		}

		sleep := ComputeJitter(backoff, d.cfg.JitterFraction)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleep):
		}

		next := time.Duration(float64(backoff) * d.cfg.BackoffFactor)
		if next > d.cfg.MaxBackoff {
			backoff = d.cfg.MaxBackoff
		} else {
			backoff = next
		}
	}
}

// DialAndServeOnce performs a single connection, handshake, and serves until disconnected.
func (d *ReverseTunnelDialer) DialAndServeOnce(ctx context.Context, handler http.Handler) error {
	var conn net.Conn
	var err error
	switch {
	case d.cfg.DialContext != nil:
		conn, err = d.cfg.DialContext(ctx, "tcp", d.cfg.Endpoint)
	case d.cfg.TLSConfig != nil:
		dialer := &tls.Dialer{
			Config: d.cfg.TLSConfig,
		}
		conn, err = dialer.DialContext(ctx, "tcp", d.cfg.Endpoint)
	default:
		dialer := &net.Dialer{Timeout: 10 * time.Second}
		conn, err = dialer.DialContext(ctx, "tcp", d.cfg.Endpoint)
	}
	if err != nil {
		return fmt.Errorf("failed to dial tunnel gateway at %s: %w", d.cfg.Endpoint, err)
	}
	defer conn.Close()

	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetReadBuffer(64 * 1024)
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}

	// Send handshake request
	req := TunnelHandshakeRequest{
		InstanceID:  d.cfg.InstanceID,
		AuthToken:   d.cfg.AuthToken,
		Platform:    d.cfg.Platform,
		Attestation: d.cfg.Attestation,
	}
	data, err := json.Marshal(req) //nolint:gosec // AuthToken is required for reverse tunnel authentication handshake
	if err != nil {
		return fmt.Errorf("failed to marshal handshake request: %w", err)
	}
	data = append(data, '\n')
	if _, err := conn.Write(data); err != nil {
		return fmt.Errorf("failed to write handshake request: %w", err)
	}

	// Read handshake response
	reader := bufio.NewReader(conn)
	respLine, err := reader.ReadBytes('\n')
	if err != nil {
		return fmt.Errorf("failed to read handshake response: %w", err)
	}
	var resp TunnelHandshakeResponse
	if err := json.Unmarshal(respLine, &resp); err != nil {
		return fmt.Errorf("invalid handshake response: %w", err)
	}
	if resp.Status != "ok" {
		return fmt.Errorf("handshake rejected by gateway: %s", resp.Error)
	}

	// Initialize Yamux client session
	session, err := yamux.Client(conn, d.cfg.YamuxConfig)
	if err != nil {
		return fmt.Errorf("failed to initialize yamux client session: %w", err)
	}
	defer session.Close()

	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	errChan := make(chan error, 1)
	go func() {
		errChan <- server.Serve(session)
	}()

	select {
	case <-ctx.Done():
		_ = server.Close()
		_ = session.Close()
		return ctx.Err()
	case <-session.CloseChan():
		_ = server.Close()
		return errors.New("yamux session closed by remote peer")
	case err := <-errChan:
		return err
	}
}
