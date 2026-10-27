// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
)

func TestDefaultYamuxConfig(t *testing.T) {
	cfg := DefaultYamuxConfig()
	if !cfg.EnableKeepAlive {
		t.Error("expected EnableKeepAlive to be true")
	}
	if cfg.KeepAliveInterval != 30*time.Second {
		t.Errorf("expected KeepAliveInterval 30s, got %v", cfg.KeepAliveInterval)
	}
	if cfg.MaxStreamWindowSize != 2*1024*1024 {
		t.Errorf("expected MaxStreamWindowSize 2 MiB, got %d", cfg.MaxStreamWindowSize)
	}
}

func TestSessionRegistry(t *testing.T) {
	reg := NewSessionRegistry()
	if reg.Count() != 0 {
		t.Errorf("expected count 0, got %d", reg.Count())
	}

	p1, p2 := net.Pipe()
	defer p1.Close()
	defer p2.Close()

	cfg := DefaultYamuxConfig()
	go func() {
		_, _ = yamux.Server(p1, cfg)
	}()
	clientSess, err := yamux.Client(p2, cfg)
	if err != nil {
		t.Fatalf("failed to init client session: %v", err)
	}
	defer clientSess.Close()

	instID := "i-test-reg-1"
	reg.Register(instID, clientSess)

	if !reg.Has(instID) {
		t.Errorf("expected registry to have %s", instID)
	}
	if reg.Count() != 1 {
		t.Errorf("expected count 1, got %d", reg.Count())
	}
	s, ok := reg.Get(instID)
	if !ok || s != clientSess {
		t.Errorf("expected retrieved session to match registered session")
	}
	list := reg.List()
	if len(list) != 1 || list[0] != instID {
		t.Errorf("expected list to contain %s, got %v", instID, list)
	}

	// Close session
	if err := reg.Close(instID); err != nil {
		t.Errorf("unexpected error on Close: %v", err)
	}
	if reg.Has(instID) {
		t.Errorf("expected session to be gone after Close")
	}
}

func TestNewReverseTunnelDialer_RequiresEndpoint(t *testing.T) {
	if _, err := NewReverseTunnelDialer(ReverseTunnelDialerConfig{InstanceID: "i-123"}); err == nil || !strings.Contains(err.Error(), "endpoint") {
		t.Fatalf("a dialer without an endpoint must fail, got %v", err)
	}
}

func TestComputeJitter(t *testing.T) {
	base := 1 * time.Second
	fraction := 0.2 // 20%
	for range 100 {
		jittered := ComputeJitter(base, fraction)
		if jittered < 800*time.Millisecond || jittered > 1200*time.Millisecond {
			t.Fatalf("jittered value %v outside [800ms, 1200ms]", jittered)
		}
	}
}

func TestTunnelContainerRunner_NonRootValidation(t *testing.T) {
	reg := NewSessionRegistry()
	runner := NewTunnelContainerRunner(reg)
	node := &CloudVMNode{InstanceID: "i-inst-1"}

	zero := int64(0)
	reqUser := &StartContainerRequest{PodUID: "pod-1", RunAsUser: &zero}
	if err := runner.StartContainer(context.Background(), node, reqUser); err == nil {
		t.Error("expected error for runAsUser 0")
	}

	reqGroup := &StartContainerRequest{PodUID: "pod-1", RunAsGroup: &zero}
	if err := runner.StartContainer(context.Background(), node, reqGroup); err == nil {
		t.Error("expected error for runAsGroup 0")
	}
}

func TestTunnelContainerRunner_Fallback(t *testing.T) {
	reg := NewSessionRegistry()
	runner := NewTunnelContainerRunner(reg)
	mock := NewMockContainerRunner()
	runner.SetFallback(mock)

	node := &CloudVMNode{InstanceID: "i-fallback-node"}
	req := &StartContainerRequest{PodUID: "pod-fallback-1"}

	// When no active tunnel session exists, fallback should be invoked
	if err := runner.StartContainer(context.Background(), node, req); err != nil {
		t.Fatalf("expected fallback StartContainer to succeed, got: %v", err)
	}

	status, err := runner.GetStatus(context.Background(), node, "pod-fallback-1")
	if err != nil {
		t.Fatalf("expected fallback GetStatus to succeed, got: %v", err)
	}
	if !status.Running {
		t.Errorf("expected pod-fallback-1 to be running via mock fallback")
	}
}

type dummyRunnerHandler struct {
	started    map[string]*StartContainerRequest
	status     map[string]*ContainerStatusResult
	stopped    map[string]bool
	hostStats  *HostStatsResult
	containers []*ContainerStatusResult
}

func newDummyRunnerHandler() *dummyRunnerHandler {
	return &dummyRunnerHandler{
		started: make(map[string]*StartContainerRequest),
		status:  make(map[string]*ContainerStatusResult),
		stopped: make(map[string]bool),
		hostStats: &HostStatsResult{
			NetworkRxBytes: 1024,
			NetworkTxBytes: 2048,
		},
	}
}

func (d *dummyRunnerHandler) StartContainer(_ context.Context, req *StartContainerRequest) error {
	d.started[req.PodUID] = req
	d.status[req.PodUID] = &ContainerStatusResult{
		PodUID:  req.PodUID,
		Running: true,
	}
	return nil
}

func (d *dummyRunnerHandler) GetStatus(_ context.Context, podUID string) (*ContainerStatusResult, error) {
	if s, ok := d.status[podUID]; ok {
		return s, nil
	}
	return nil, fmt.Errorf("pod %s not found", podUID)
}

func (d *dummyRunnerHandler) StopContainer(_ context.Context, podUID string) error {
	d.stopped[podUID] = true
	if s, ok := d.status[podUID]; ok {
		s.Running = false
	}
	return nil
}

func (d *dummyRunnerHandler) ListContainers(_ context.Context) ([]*ContainerStatusResult, error) {
	return d.containers, nil
}

func (d *dummyRunnerHandler) GetHostStats(_ context.Context) (*HostStatsResult, error) {
	return d.hostStats, nil
}

func TestRunnerHTTPMux_Endpoints(t *testing.T) {
	handler := newDummyRunnerHandler()
	token := "secret-auth-token-123" //nolint:gosec // mock test token
	mux := NewRunnerHTTPMux(handler, token)

	ts := httptest.NewServer(mux)
	defer ts.Close()

	client := ts.Client()

	// 1. Unauthorized request
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodGet, ts.URL+"/host/stats", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized, got %d", resp.StatusCode)
	}

	// 2. Authorized GetHostStats
	req, _ = http.NewRequestWithContext(context.Background(), http.MethodGet, ts.URL+"/host/stats", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err = client.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 OK, got %d", resp.StatusCode)
	}
}

func TestReverseTunnel_EndToEnd(t *testing.T) {
	// 1. Create Gateway Listener
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to create listener: %v", err)
	}
	defer listener.Close()

	reg := NewSessionRegistry()
	instID := "i-warm-worker-42"
	runnerToken := "runner-token-xyz" //nolint:gosec // mock test token

	gw, err := NewTunnelGateway(TunnelGatewayConfig{
		Listener: listener,
		Registry: reg,
		TokenValidator: func(_ context.Context, id, tok string) error {
			if id != instID || tok != runnerToken {
				return fmt.Errorf("invalid credentials")
			}
			return nil
		},
	})
	if err != nil {
		t.Fatalf("failed to create gateway: %v", err)
	}
	defer gw.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	gw.Start(ctx)

	// 2. Setup Worker Runner Mux & Reverse Dialer
	handler := newDummyRunnerHandler()
	runnerMux := NewRunnerHTTPMux(handler, runnerToken)

	dialer, err := NewReverseTunnelDialer(ReverseTunnelDialerConfig{
		Endpoint:       listener.Addr().String(),
		InstanceID:     instID,
		AuthToken:      runnerToken,
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     200 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("failed to create dialer: %v", err)
	}

	dialerErrCh := make(chan error, 1)
	go func() {
		dialerErrCh <- dialer.Run(ctx, runnerMux)
	}()

	// Wait for worker session to register in gateway
	deadline := time.Now().Add(5 * time.Second)
	for !reg.Has(instID) {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for worker session to register")
		}
		time.Sleep(20 * time.Millisecond)
	}

	tunnelRunner := gw.Runner()
	node := &CloudVMNode{
		InstanceID:      instID,
		RunnerAuthToken: runnerToken,
	}

	// 3. Dispatch StartContainer
	startReq := &StartContainerRequest{
		PodUID:        "pod-sliced-1",
		ContainerName: "app",
		Image:         "alpine:latest",
	}
	if err := tunnelRunner.StartContainer(ctx, node, startReq); err != nil {
		t.Fatalf("StartContainer failed: %v", err)
	}

	// 4. Dispatch GetStatus
	status, err := tunnelRunner.GetStatus(ctx, node, "pod-sliced-1")
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if !status.Running {
		t.Errorf("expected pod-sliced-1 to be running, got %+v", status)
	}

	// 5. Dispatch GetHostStats
	hostStats, err := tunnelRunner.GetHostStats(ctx, node)
	if err != nil {
		t.Fatalf("GetHostStats failed: %v", err)
	}
	if hostStats.NetworkRxBytes != 1024 || hostStats.NetworkTxBytes != 2048 {
		t.Errorf("unexpected host stats: %+v", hostStats)
	}

	// 6. Dispatch StopContainer
	if err := tunnelRunner.StopContainer(ctx, node, "pod-sliced-1"); err != nil {
		t.Fatalf("StopContainer failed: %v", err)
	}
	statusStopped, err := tunnelRunner.GetStatus(ctx, node, "pod-sliced-1")
	if err != nil {
		t.Fatalf("GetStatus after stop failed: %v", err)
	}
	if statusStopped.Running {
		t.Errorf("expected container to be stopped")
	}

	// 7. Test unregister on close
	cancel()
	_ = gw.Close()
}
