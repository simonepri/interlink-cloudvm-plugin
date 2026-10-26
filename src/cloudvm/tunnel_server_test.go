// Package main implements the interlink-cloudvm-plugin daemon.
// This file tests tunnel wiring: worker authentication, per-session routing, reconnects, metrics, and the connect deadline.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// tunnelFixture is a Server with tunnel routing enabled and a running gateway on a loopback port.
type tunnelFixture struct {
	server   *Server
	provider *MockProvider
	registry *SessionRegistry
	gateway  *TunnelGateway
	metrics  *TunnelMetrics
}

func newTunnelFixture(t *testing.T, connectTimeout time.Duration) *tunnelFixture {
	t.Helper()
	cleanTempStores()
	t.Cleanup(cleanTempStores)
	provider := NewMockProvider()
	server := NewServer(provider, "test-cluster")
	server.RegisterProvider(ProviderMock, provider)
	registry := NewSessionRegistry()
	metrics := server.EnableTunnel(TunnelSettings{
		Registry:       registry,
		Endpoint:       "clvm-tunnel.cell-test.c.corp.boltz.com:443",
		MeshAuthKey:    "tskey-auth-test",
		ConnectTimeout: connectTimeout,
	})
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	gateway, err := NewTunnelGateway(TunnelGatewayConfig{
		Listener:       listener,
		Registry:       registry,
		TokenValidator: server.ValidateTunnelToken,
		OnReject:       metrics.RecordRejection,
		LogSink:        server,
	})
	if err != nil {
		t.Fatalf("gateway: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	gateway.Start(ctx)
	t.Cleanup(func() {
		cancel()
		_ = gateway.Close()
	})
	return &tunnelFixture{server: server, provider: provider, registry: registry, gateway: gateway, metrics: metrics}
}

func (f *tunnelFixture) addNode(id, region, token string) *CloudVMNode {
	capacity, _ := NewResourceVector("4", "16Gi", 0, 0)
	node := NewCloudVMNode(id, ProviderMock, region, "", capacity)
	node.RunnerAuthToken = token
	f.server.NodePool().RegisterNode(node)
	return node
}

func (f *tunnelFixture) dialer(t *testing.T, id, token string) *ReverseTunnelDialer {
	t.Helper()
	d, err := NewReverseTunnelDialer(ReverseTunnelDialerConfig{
		Endpoint:       f.gateway.Addr().String(),
		InstanceID:     id,
		AuthToken:      token,
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("dialer: %v", err)
	}
	return d
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	waitForWithin(t, 10*time.Second, what, cond)
}

func waitForWithin(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestNewTunnelGateway_RequiresTokenValidator(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer listener.Close()
	if _, err := NewTunnelGateway(TunnelGatewayConfig{Listener: listener}); err == nil {
		t.Fatal("expected an error when no token validator is configured")
	}
}

func TestTunnelGateway_RejectsUnknownAndWrongToken(t *testing.T) {
	f := newTunnelFixture(t, time.Minute)
	f.addNode("i-known", "us-west-2", "good-token")

	cases := map[string]struct{ id, token string }{
		"unknown instance": {"i-stranger", "good-token"},
		"wrong token":      {"i-known", "bad-token"},
		"empty token":      {"i-known", ""},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			err := f.dialer(t, tc.id, tc.token).DialAndServeOnce(context.Background(), http.NewServeMux())
			if err == nil || !strings.Contains(err.Error(), "handshake rejected") {
				t.Fatalf("expected handshake rejection, got %v", err)
			}
			if f.registry.Has(tc.id) {
				t.Fatalf("rejected worker %s must not be registered", tc.id)
			}
		})
	}
	if body := scrapeMetrics(t, f.server); !strings.Contains(body, `cloudvm_tunnel_handshake_rejections_total{reason="unauthorized"} 3`) {
		t.Fatalf("expected 3 unauthorized rejections in metrics, got:\n%s", body)
	}
}

func TestTunnelGateway_RoutesCallsToEachWorkersSession(t *testing.T) {
	f := newTunnelFixture(t, time.Minute)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workers := map[string]string{"i-west": "us-west-2", "i-east": "us-east-1"}
	nodes := map[string]*CloudVMNode{}
	for id, region := range workers {
		token := "token-" + id
		nodes[id] = f.addNode(id, region, token)
		id := id
		mux := http.NewServeMux()
		mux.HandleFunc("/host/stats", func(w http.ResponseWriter, r *http.Request) {
			if r.Header.Get("Authorization") != "Bearer "+token {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			// Each worker reports a distinct value so a misrouted call is detectable.
			_ = json.NewEncoder(w).Encode(HostStatsResult{NetworkRxBytes: int64(len(id))})
			_, _ = io.WriteString(w, "")
		})
		go func() { _ = f.dialer(t, id, token).Run(ctx, mux) }()
	}
	waitFor(t, "both sessions", func() bool { return f.registry.Has("i-west") && f.registry.Has("i-east") })

	f.server.mu.RLock()
	runner := f.server.runner
	f.server.mu.RUnlock()
	for id, node := range nodes {
		stats, err := runner.GetHostStats(ctx, node)
		if err != nil {
			t.Fatalf("GetHostStats(%s): %v", id, err)
		}
		if stats.NetworkRxBytes != int64(len(id)) {
			t.Fatalf("call for %s reached the wrong worker: %+v", id, stats)
		}
	}

	if _, err := runner.GetHostStats(ctx, &CloudVMNode{InstanceID: "i-offline"}); err == nil {
		t.Fatal("expected an error for a worker without a session; the private-IP path must stay off by default")
	}

	body := scrapeMetrics(t, f.server)
	for _, want := range []string{`cloudvm_tunnel_sessions{region="us-east-1"} 1`, `cloudvm_tunnel_sessions{region="us-west-2"} 1`} {
		if !strings.Contains(body, want) {
			t.Fatalf("metrics missing %q:\n%s", want, body)
		}
	}
}

func TestTunnelGateway_WorkerReconnectsAfterSessionLoss(t *testing.T) {
	f := newTunnelFixture(t, time.Minute)
	node := f.addNode("i-flaky", "us-west-2", "tok")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		_ = f.dialer(t, "i-flaky", "tok").Run(ctx, NewRunnerHTTPMux(&RunnerHandlerAdapter{Runner: NewMockContainerRunner()}, "tok"))
	}()
	waitFor(t, "first session", func() bool { return f.registry.Has("i-flaky") })
	first, _ := f.registry.Get("i-flaky")

	_ = f.registry.Close("i-flaky")
	waitFor(t, "reconnected session", func() bool {
		s, ok := f.registry.Get("i-flaky")
		return ok && s != first
	})
	if _, err := f.gateway.Runner().GetHostStats(ctx, node); err != nil {
		t.Fatalf("call over reconnected session failed: %v", err)
	}
}

func TestServer_TerminatesWorkerWhoseTunnelNeverConnects(t *testing.T) {
	f := newTunnelFixture(t, 100*time.Millisecond)
	f.server.startRetryInterval = 10 * time.Millisecond

	body, _ := json.Marshal(Pod{
		Metadata: PodMeta{Name: "silent-worker", Namespace: "default", UID: "uid-silent"},
		Spec:     PodSpecWrapper{Containers: []ContainerSpec{{Name: "worker", Image: "ubuntu:24.04"}}},
	})
	rec := httptest.NewRecorder()
	f.server.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("/create: %d %s", rec.Code, rec.Body.String())
	}
	spec := f.provider.LastSpec()
	if spec.TunnelEndpoint != "clvm-tunnel.cell-test.c.corp.boltz.com:443" || spec.MeshAuthKey != "tskey-auth-test" || spec.MeshTags != DefaultMeshTags {
		t.Fatalf("launch spec missing tunnel settings: endpoint=%q tags=%q", spec.TunnelEndpoint, spec.MeshTags)
	}

	waitFor(t, "tunnel deadline", func() bool {
		f.server.mu.RLock()
		defer f.server.mu.RUnlock()
		term := f.server.completedPods["uid-silent"]
		return term != nil && term.Reason == "TunnelConnectTimeout"
	})
	waitFor(t, "worker termination", func() bool {
		inst, err := f.provider.Status(context.Background(), "i-mock-0001")
		return err == nil && inst.State == StateTerminated
	})
	if f.server.NodePool().HasNode("i-mock-0001") {
		t.Fatal("expected terminated worker to leave the node pool")
	}
}

func TestServer_KeepsWorkerWhoseTunnelConnects(t *testing.T) {
	f := newTunnelFixture(t, 300*time.Millisecond)
	f.server.startRetryInterval = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	body, _ := json.Marshal(Pod{
		Metadata: PodMeta{Name: "chatty-worker", Namespace: "default", UID: "uid-chatty"},
		Spec:     PodSpecWrapper{Containers: []ContainerSpec{{Name: "worker", Image: "ubuntu:24.04"}}},
	})
	rec := httptest.NewRecorder()
	f.server.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(body)))
	if rec.Code != http.StatusOK {
		t.Fatalf("/create: %d %s", rec.Code, rec.Body.String())
	}
	spec := f.provider.LastSpec()
	handler := &RunnerHandlerAdapter{Runner: NewMockContainerRunner()}
	go func() {
		_ = f.dialer(t, "i-mock-0001", spec.RunnerAuthToken).Run(ctx, NewRunnerHTTPMux(handler, spec.RunnerAuthToken))
	}()
	waitFor(t, "container start over tunnel", func() bool {
		_, err := f.gateway.Runner().GetStatus(ctx, &CloudVMNode{InstanceID: "i-mock-0001", RunnerAuthToken: spec.RunnerAuthToken}, "uid-chatty")
		return err == nil
	})
	time.Sleep(400 * time.Millisecond)
	if inst, _ := f.provider.Status(ctx, "i-mock-0001"); inst == nil || inst.State == StateTerminated {
		t.Fatalf("connected worker must not be terminated: %+v", inst)
	}
}

func TestApplyTunnelSpec_EncryptsMeshKeyWithKMS(t *testing.T) {
	f := newTunnelFixture(t, time.Minute)
	spec := &InstanceSpec{MeshAuthKey: "pod-supplied"}
	f.server.applyTunnelSpec(spec)
	if spec.MeshAuthKey != "tskey-auth-test" {
		t.Fatalf("plugin mesh key must override pod env, got %q", spec.MeshAuthKey)
	}
	if err := encryptMeshAuthKey(context.Background(), &mockKMSClient{}, "arn:aws:kms:us-west-2:111122223333:key/k", spec); err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	if spec.MeshAuthKey != "" || spec.MeshAuthKeyCiphertext == "" {
		t.Fatalf("expected only ciphertext to remain, got key=%q ciphertext=%q", spec.MeshAuthKey, spec.MeshAuthKeyCiphertext)
	}
}

func scrapeMetrics(t *testing.T, s *Server) string {
	t.Helper()
	rec := httptest.NewRecorder()
	s.Routes().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics: %d %s", rec.Code, rec.Body.String())
	}
	return rec.Body.String()
}
