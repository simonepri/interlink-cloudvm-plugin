// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"bufio"
	"bytes"
	"context"
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

// DefaultYamuxConfig returns a tuned Yamux configuration for reverse tunnel gateway and workers.
// Configures keepalive ping interval of 30 seconds and a 2 MiB receive window per stream, matching the agent's STREAM_WINDOW.
func DefaultYamuxConfig() *yamux.Config {
	cfg := yamux.DefaultConfig()
	cfg.EnableKeepAlive = true
	cfg.KeepAliveInterval = 30 * time.Second
	cfg.MaxStreamWindowSize = 2 * 1024 * 1024
	cfg.LogOutput = io.Discard
	return cfg
}

// maxHandshakeBytes bounds the handshake line a worker may send before authentication.
const maxHandshakeBytes = 64 * 1024

// TunnelHandshakeRequest carries worker identification and optional attestation on connection initiation.
type TunnelHandshakeRequest struct {
	InstanceID  string `json:"instanceId"`
	AuthToken   string `json:"authToken,omitempty"`
	Platform    string `json:"platform,omitempty"`    // "aws" or "gcp"
	Attestation []byte `json:"attestation,omitempty"` // PKCS#7 or JWT bytes
}

// TunnelHandshakeResponse acknowledges the reverse tunnel connection status.
type TunnelHandshakeResponse struct {
	Status string `json:"status"` // "ok" or "error"
	Error  string `json:"error,omitempty"`
}

// SessionRegistry maps instance IDs to active Yamux multiplexed sessions.
type SessionRegistry struct {
	mu       sync.RWMutex
	sessions map[string]*yamux.Session
}

// NewSessionRegistry initializes an empty session registry.
func NewSessionRegistry() *SessionRegistry {
	return &SessionRegistry{
		sessions: make(map[string]*yamux.Session),
	}
}

// Register maps an instance ID to an active Yamux session, closing any previous session for that instance.
func (r *SessionRegistry) Register(instanceID string, session *yamux.Session) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.sessions[instanceID]; ok && old != nil && old != session {
		_ = old.Close()
	}
	r.sessions[instanceID] = session
}

// Unregister removes an instance ID from the session registry.
func (r *SessionRegistry) Unregister(instanceID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.sessions, instanceID)
}

// Get retrieves the active Yamux session for the given instance ID.
func (r *SessionRegistry) Get(instanceID string) (*yamux.Session, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s, ok := r.sessions[instanceID]
	if !ok || s == nil || s.IsClosed() {
		return nil, false
	}
	return s, true
}

// Has checks whether an active session exists for the given instance ID.
func (r *SessionRegistry) Has(instanceID string) bool {
	_, ok := r.Get(instanceID)
	return ok
}

// Count returns the number of active sessions in the registry.
func (r *SessionRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	count := 0
	for _, s := range r.sessions {
		if s != nil && !s.IsClosed() {
			count++
		}
	}
	return count
}

// List returns a list of instance IDs with registered sessions.
func (r *SessionRegistry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	ids := make([]string, 0, len(r.sessions))
	for id, s := range r.sessions {
		if s != nil && !s.IsClosed() {
			ids = append(ids, id)
		}
	}
	return ids
}

// Close terminates and unregisters the session for the given instance ID.
func (r *SessionRegistry) Close(instanceID string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s, ok := r.sessions[instanceID]; ok {
		delete(r.sessions, instanceID)
		if s != nil {
			return s.Close()
		}
	}
	return nil
}

// CloseAll terminates all active sessions in the registry.
func (r *SessionRegistry) CloseAll() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	var firstErr error
	for id, s := range r.sessions {
		if s != nil {
			if err := s.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		delete(r.sessions, id)
	}
	return firstErr
}

// TunnelGateway accepts incoming reverse tunnel connections from cloud VM runners.
type TunnelGateway struct {
	listener            net.Listener
	registry            *SessionRegistry
	attestationVerifier AttestationVerifier
	tokenValidator      func(ctx context.Context, instanceID, token string) error
	identityValidator   func(ctx context.Context, req *TunnelHandshakeRequest) error
	yamuxConfig         *yamux.Config
	handshakeTimeout    time.Duration
	onReject            func(reason string)
	onBootstrap         func(result string)
	logSink             WorkerLogSink
	resolver            InstanceAddressResolver
	directTCPNetworks   []*net.IPNet
	clusterDomain       string
	clusterDNSServers   []string
	closeOnce           sync.Once
	closed              chan struct{}
}

// TunnelGatewayConfig configures the reverse tunnel gateway.
type TunnelGatewayConfig struct {
	Listener            net.Listener
	Registry            *SessionRegistry
	AttestationVerifier AttestationVerifier
	TokenValidator      func(ctx context.Context, instanceID, token string) error
	YamuxConfig         *yamux.Config
	HandshakeTimeout    time.Duration
	// OnReject is called with a short machine-readable reason for every refused handshake.
	OnReject func(reason string)
	// OnBootstrapRequest is called with "served", "not_found", or "bad_request" for every bootstrap script request.
	OnBootstrapRequest func(result string)
	// LogSink receives the log stream each worker opens on its session; workers' log streams are closed when nil.
	LogSink WorkerLogSink
	// IdentityValidator, when set, runs after the token check and may demand proof of the worker's cloud identity.
	IdentityValidator func(ctx context.Context, req *TunnelHandshakeRequest) error
	// Resolver maps a worker VM address to its instance, so tcp streams and CONNECT requests reach that worker.
	Resolver InstanceAddressResolver
	// DirectTCPNetworks lists the networks workers' tcp streams may reach directly, besides other workers, on ports
	// from 1024 up; direct forwarding is off when empty.
	DirectTCPNetworks []*net.IPNet
	// ClusterDomain is the DNS domain whose names workers' dns streams may resolve; DefaultClusterDomain when empty.
	ClusterDomain string
	// ClusterDNSServers are the host:port resolvers that answer workers' dns streams; the nameservers of the plugin
	// pod's /etc/resolv.conf when empty.
	ClusterDNSServers []string
}

// WorkerLogSink consumes the newline-delimited WorkerLogRecord stream a worker opens on its tunnel session.
type WorkerLogSink interface {
	ConsumeWorkerLogs(instanceID string, r io.Reader)
}

// WorkerLogStreamKind names the worker-opened stream that carries logs.
const WorkerLogStreamKind = "logs"

// WorkerStreamHeader is the first line of every stream a worker opens and says what the stream carries.
type WorkerStreamHeader struct {
	Stream  string `json:"stream"`
	Version int    `json:"version"`
	// Target is the host:port a tcp stream connects to.
	Target string `json:"target,omitempty"`
}

// NewTunnelGateway creates a new TunnelGateway.
func NewTunnelGateway(cfg TunnelGatewayConfig) (*TunnelGateway, error) {
	if cfg.Listener == nil {
		return nil, errors.New("gateway listener is required")
	}
	if cfg.TokenValidator == nil {
		return nil, errors.New("gateway token validator is required")
	}
	if cfg.Registry == nil {
		cfg.Registry = NewSessionRegistry()
	}
	if cfg.YamuxConfig == nil {
		cfg.YamuxConfig = DefaultYamuxConfig()
	}
	if cfg.HandshakeTimeout <= 0 {
		cfg.HandshakeTimeout = 10 * time.Second
	}
	if cfg.ClusterDomain == "" {
		cfg.ClusterDomain = DefaultClusterDomain
	}
	if len(cfg.ClusterDNSServers) == 0 {
		cfg.ClusterDNSServers = ResolvConfNameservers(podResolvConf)
	}

	gw := &TunnelGateway{
		listener:            cfg.Listener,
		registry:            cfg.Registry,
		attestationVerifier: cfg.AttestationVerifier,
		tokenValidator:      cfg.TokenValidator,
		identityValidator:   cfg.IdentityValidator,
		yamuxConfig:         cfg.YamuxConfig,
		handshakeTimeout:    cfg.HandshakeTimeout,
		onReject:            cfg.OnReject,
		onBootstrap:         cfg.OnBootstrapRequest,
		logSink:             cfg.LogSink,
		resolver:            cfg.Resolver,
		directTCPNetworks:   cfg.DirectTCPNetworks,
		clusterDomain:       cfg.ClusterDomain,
		clusterDNSServers:   cfg.ClusterDNSServers,
		closed:              make(chan struct{}),
	}
	return gw, nil
}

// Start begins accepting incoming reverse tunnel connections in a background goroutine.
func (g *TunnelGateway) Start(ctx context.Context) {
	go g.acceptLoop(ctx)
}

func (g *TunnelGateway) acceptLoop(ctx context.Context) {
	for {
		conn, err := g.listener.Accept()
		if err != nil {
			select {
			case <-g.closed:
				return
			case <-ctx.Done():
				return
			default:
				log.Printf("[cloudvm-tunnel] Accept error: %v", err)
				continue
			}
		}
		go g.handleConnection(ctx, conn)
	}
}

func (g *TunnelGateway) handleConnection(ctx context.Context, conn net.Conn) {
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetReadBuffer(64 * 1024)
		_ = tcpConn.SetKeepAlive(true)
		_ = tcpConn.SetKeepAlivePeriod(30 * time.Second)
	}
	if err := conn.SetDeadline(time.Now().Add(g.handshakeTimeout)); err != nil {
		_ = conn.Close()
		return
	}

	reader := bufio.NewReader(io.LimitReader(conn, maxHandshakeBytes))
	if isBootstrapRequest(reader) {
		g.serveBootstrapScript(conn, reader)
		return
	}
	reqLine, err := reader.ReadBytes('\n')
	if err != nil {
		_ = conn.Close()
		return
	}

	var req TunnelHandshakeRequest
	if err := json.Unmarshal(reqLine, &req); err != nil {
		g.reject(conn, "", "invalid_payload", fmt.Sprintf("invalid handshake payload: %v", err))
		return
	}
	if req.InstanceID == "" {
		g.reject(conn, "", "missing_instance", "instanceId is required")
		return
	}
	if err := g.verifyAttestation(ctx, &req); err != nil {
		g.reject(conn, req.InstanceID, "attestation", err.Error())
		return
	}
	if err := g.tokenValidator(ctx, req.InstanceID, req.AuthToken); err != nil {
		g.reject(conn, req.InstanceID, "unauthorized", fmt.Sprintf("unauthorized runner token: %v", err))
		return
	}
	if g.identityValidator != nil {
		if err := g.identityValidator(ctx, &req); err != nil {
			g.reject(conn, req.InstanceID, "identity", fmt.Sprintf("instance identity rejected: %v", err))
			return
		}
	}

	// Acknowledge handshake success
	if err := g.sendHandshakeResponse(conn, "ok", ""); err != nil {
		_ = conn.Close()
		return
	}

	// Reset deadline for persistent Yamux session
	if err := conn.SetDeadline(time.Time{}); err != nil {
		_ = conn.Close()
		return
	}

	// Initialize Yamux server session over connection
	session, err := yamux.Server(conn, g.yamuxConfig)
	if err != nil {
		_ = conn.Close()
		return
	}

	g.registry.Register(req.InstanceID, session)
	log.Printf("[cloudvm-tunnel] Registered reverse tunnel session for instance %s from %s", sanitizeLog(req.InstanceID), conn.RemoteAddr()) //nolint:gosec // sanitized

	go g.acceptWorkerStreams(req.InstanceID, session)

	// Wait for session termination to unregister
	go func(id string, s *yamux.Session) {
		<-s.CloseChan()
		if cur, ok := g.registry.Get(id); ok && cur == s {
			g.registry.Unregister(id)
		}
	}(req.InstanceID, session)
}

// acceptWorkerStreams serves the streams a worker opens on its session until the session closes.
func (g *TunnelGateway) acceptWorkerStreams(instanceID string, session *yamux.Session) {
	for {
		stream, err := session.AcceptStream()
		if err != nil {
			return
		}
		go g.serveWorkerStream(instanceID, stream)
	}
}

// serveWorkerStream reads the stream header and hands a log stream to the log sink, a tcp stream to its target, and a
// dns stream to the cluster resolver; it closes any other stream.
func (g *TunnelGateway) serveWorkerStream(instanceID string, stream *yamux.Stream) {
	defer stream.Close()
	if err := stream.SetReadDeadline(time.Now().Add(g.handshakeTimeout)); err != nil {
		return
	}
	reader := bufio.NewReaderSize(stream, maxWorkerLogLine)
	line, err := reader.ReadSlice('\n')
	if err != nil {
		return
	}
	var header WorkerStreamHeader
	if err := json.Unmarshal(line, &header); err != nil {
		return
	}
	if err := stream.SetReadDeadline(time.Time{}); err != nil {
		return
	}
	switch {
	case header.Stream == WorkerLogStreamKind && g.logSink != nil:
		g.logSink.ConsumeWorkerLogs(instanceID, reader)
	case header.Stream == WorkerTCPStreamKind:
		g.serveWorkerTCP(instanceID, reader, stream, header.Target)
	case header.Stream == WorkerDNSStreamKind:
		g.serveWorkerDNS(instanceID, reader, stream)
	}
}

// verifyAttestation checks the platform identity document when a verifier is configured and the worker sent one.
func (g *TunnelGateway) verifyAttestation(ctx context.Context, req *TunnelHandshakeRequest) error {
	if g.attestationVerifier == nil || len(req.Attestation) == 0 {
		return nil
	}
	var instanceID string
	switch strings.ToLower(req.Platform) {
	case "aws":
		identity, err := g.attestationVerifier.VerifyAWS(ctx, req.Attestation)
		if err != nil {
			return fmt.Errorf("AWS attestation verification failed: %w", err)
		}
		instanceID = identity.InstanceID
	case "gcp":
		identity, err := g.attestationVerifier.VerifyGCP(ctx, string(req.Attestation), "")
		if err != nil {
			return fmt.Errorf("GCP attestation verification failed: %w", err)
		}
		instanceID = identity.InstanceID
	default:
		return nil
	}
	if instanceID != req.InstanceID {
		return fmt.Errorf("attestation instance ID mismatch: %s != %s", instanceID, req.InstanceID)
	}
	return nil
}

// reject answers a refused handshake, records why, and closes the connection; it never logs the presented token.
func (g *TunnelGateway) reject(conn net.Conn, instanceID, reason, message string) {
	log.Printf("[cloudvm-tunnel] Rejected handshake from %s (instance %q): %s", conn.RemoteAddr(), sanitizeLog(instanceID), reason) //nolint:gosec // sanitized
	if g.onReject != nil {
		g.onReject(reason)
	}
	_ = g.sendHandshakeResponse(conn, "error", message)
	_ = conn.Close()
}

func (g *TunnelGateway) sendHandshakeResponse(w io.Writer, status, errMsg string) error {
	resp := TunnelHandshakeResponse{
		Status: status,
		Error:  errMsg,
	}
	data, _ := json.Marshal(resp)
	data = append(data, '\n')
	_, err := w.Write(data)
	return err
}

// Registry returns the session registry.
func (g *TunnelGateway) Registry() *SessionRegistry {
	return g.registry
}

// Runner returns a ContainerRunner that dispatches calls down the registered Yamux sessions.
func (g *TunnelGateway) Runner() *TunnelContainerRunner {
	return NewTunnelContainerRunner(g.registry)
}

// Addr returns the listener address.
func (g *TunnelGateway) Addr() net.Addr {
	return g.listener.Addr()
}

// Close stops the listener and terminates all active sessions.
func (g *TunnelGateway) Close() error {
	var err error
	g.closeOnce.Do(func() {
		close(g.closed)
		err = g.listener.Close()
		_ = g.registry.CloseAll()
	})
	return err
}

// TunnelContainerRunner implements ContainerRunner by dispatching HTTP calls down active Yamux sessions.
type TunnelContainerRunner struct {
	registry      *SessionRegistry
	clientTimeout time.Duration
	fallback      ContainerRunner
}

// NewTunnelContainerRunner initializes a TunnelContainerRunner.
func NewTunnelContainerRunner(registry *SessionRegistry) *TunnelContainerRunner {
	return &TunnelContainerRunner{
		registry:      registry,
		clientTimeout: 15 * time.Second,
	}
}

// SetFallback configures an optional fallback ContainerRunner used when no tunnel session is registered.
func (t *TunnelContainerRunner) SetFallback(fallback ContainerRunner) {
	t.fallback = fallback
}

// SetClientTimeout configures the HTTP client timeout for reverse tunnel dispatches.
func (t *TunnelContainerRunner) SetClientTimeout(timeout time.Duration) {
	t.clientTimeout = timeout
}

func (t *TunnelContainerRunner) clientForSession(session *yamux.Session) *http.Client {
	return &http.Client{
		Timeout: t.clientTimeout,
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return session.Open()
			},
			DisableKeepAlives: false,
			IdleConnTimeout:   30 * time.Second,
		},
	}
}

func (t *TunnelContainerRunner) setAuthHeader(httpReq *http.Request, node *CloudVMNode) {
	if node != nil && node.RunnerAuthToken != "" {
		httpReq.Header.Set("Authorization", "Bearer "+node.RunnerAuthToken)
	}
}

// StartContainer dispatches container execution down the worker's active Yamux session.
func (t *TunnelContainerRunner) StartContainer(ctx context.Context, node *CloudVMNode, req *StartContainerRequest) error {
	if node == nil || node.InstanceID == "" {
		return errors.New("node or instanceID is missing")
	}

	session, ok := t.registry.Get(node.InstanceID)
	if !ok {
		if t.fallback != nil {
			return t.fallback.StartContainer(ctx, node, req)
		}
		return fmt.Errorf("no active reverse tunnel session for instance %s", node.InstanceID)
	}

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

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://clvm-tunnel/containers/start", bytes.NewReader(payload))
	if err != nil {
		return err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	t.setAuthHeader(httpReq, node)

	client := t.clientForSession(session)
	resp, err := client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("reverse tunnel dispatch StartContainer failed for %s: %w", node.InstanceID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("in-VM runner error (%d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// GetStatus queries container execution telemetry down the worker's active Yamux session.
func (t *TunnelContainerRunner) GetStatus(ctx context.Context, node *CloudVMNode, podUID string) (*ContainerStatusResult, error) {
	if node == nil || node.InstanceID == "" {
		return nil, errors.New("node or instanceID is missing")
	}

	session, ok := t.registry.Get(node.InstanceID)
	if !ok {
		if t.fallback != nil {
			return t.fallback.GetStatus(ctx, node, podUID)
		}
		return nil, fmt.Errorf("no active reverse tunnel session for instance %s", node.InstanceID)
	}

	url := fmt.Sprintf("http://clvm-tunnel/containers/status?podUID=%s", podUID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	t.setAuthHeader(httpReq, node)

	client := t.clientForSession(session)
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("reverse tunnel dispatch GetStatus failed for %s: %w", node.InstanceID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("in-VM runner error (%d) getting status for pod %s", resp.StatusCode, podUID)
	}

	var result ContainerStatusResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}

// StopContainer stops a container execution down the worker's active Yamux session.
func (t *TunnelContainerRunner) StopContainer(ctx context.Context, node *CloudVMNode, podUID string) error {
	if node == nil || node.InstanceID == "" {
		return errors.New("node or instanceID is missing")
	}

	session, ok := t.registry.Get(node.InstanceID)
	if !ok {
		if t.fallback != nil {
			return t.fallback.StopContainer(ctx, node, podUID)
		}
		return fmt.Errorf("no active reverse tunnel session for instance %s", node.InstanceID)
	}

	url := fmt.Sprintf("http://clvm-tunnel/containers/stop?podUID=%s", podUID)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, nil)
	if err != nil {
		return err
	}
	t.setAuthHeader(httpReq, node)

	client := t.clientForSession(session)
	resp, err := client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("reverse tunnel dispatch StopContainer failed for %s: %w", node.InstanceID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("in-VM runner error (%d) stopping container %s", resp.StatusCode, podUID)
	}
	return nil
}

// ListContainers queries container list down the worker's active Yamux session.
func (t *TunnelContainerRunner) ListContainers(ctx context.Context, node *CloudVMNode) ([]*ContainerStatusResult, error) {
	if node == nil || node.InstanceID == "" {
		return nil, errors.New("node or instanceID is missing")
	}

	session, ok := t.registry.Get(node.InstanceID)
	if !ok {
		if t.fallback != nil {
			return t.fallback.ListContainers(ctx, node)
		}
		return nil, fmt.Errorf("no active reverse tunnel session for instance %s", node.InstanceID)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://clvm-tunnel/containers/list", nil)
	if err != nil {
		return nil, err
	}
	t.setAuthHeader(httpReq, node)

	client := t.clientForSession(session)
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("reverse tunnel dispatch ListContainers failed for %s: %w", node.InstanceID, err)
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

// GetHostStats queries host stats down the worker's active Yamux session.
func (t *TunnelContainerRunner) GetHostStats(ctx context.Context, node *CloudVMNode) (*HostStatsResult, error) {
	if node == nil || node.InstanceID == "" {
		return nil, errors.New("node or instanceID is missing")
	}

	session, ok := t.registry.Get(node.InstanceID)
	if !ok {
		if t.fallback != nil {
			return t.fallback.GetHostStats(ctx, node)
		}
		return nil, fmt.Errorf("no active reverse tunnel session for instance %s", node.InstanceID)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://clvm-tunnel/host/stats", nil)
	if err != nil {
		return nil, err
	}
	t.setAuthHeader(httpReq, node)

	client := t.clientForSession(session)
	resp, err := client.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("reverse tunnel dispatch GetHostStats failed for %s: %w", node.InstanceID, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("in-VM runner host stats error: status %d", resp.StatusCode)
	}

	var result HostStatsResult
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return &result, nil
}
