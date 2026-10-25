// Package main implements the interlink-cloudvm-plugin daemon.
// This file runs the embedded Python tunnel agent against the Go gateway behind a TLS-terminating proxy, as deployed in a cell.
package main

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// syncBuffer collects subprocess output that the test reads while the process is still running.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// startTLSTerminator mimics the cell's private gateway: it terminates TLS for "localhost" and forwards plaintext to backend.
func startTLSTerminator(t *testing.T, backend string) (addr, caFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "localhost"},
		DNSNames:              []string{"localhost"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("cert: %v", err)
	}
	caFile = filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		t.Fatalf("write ca: %v", err)
	}
	listener, err := tls.Listen("tcp", "127.0.0.1:0", &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}},
		MinVersion:   tls.VersionTLS12,
	})
	if err != nil {
		t.Fatalf("tls listen: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })
	go func() {
		for {
			client, err := listener.Accept()
			if err != nil {
				return
			}
			go func() {
				defer client.Close()
				upstream, err := net.Dial("tcp", backend)
				if err != nil {
					return
				}
				defer upstream.Close()
				go func() { _, _ = io.Copy(upstream, client) }()
				_, _ = io.Copy(client, upstream)
			}()
		}
	}()
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	return "localhost:" + port, caFile
}

func TestTunnelAgent_PythonAgentServesRunnerThroughGateway(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	f := newTunnelFixture(t, time.Minute)
	const instanceID, token = "i-python-agent", "agent-token"
	node := f.addNode(instanceID, "eu-west-1", token)

	// Local runner the agent bridges to: the real runner API plus an echo route to push data past the 256 KiB stream window.
	mux := NewRunnerHTTPMux(&RunnerHandlerAdapter{Runner: NewMockContainerRunner()}, token)
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(w, r.Body)
	})
	runnerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("runner listen: %v", err)
	}
	runnerServer := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { _ = runnerServer.Serve(runnerListener) }()
	t.Cleanup(func() { _ = runnerServer.Close() })

	endpoint, caFile := startTLSTerminator(t, f.gateway.Addr().String())
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "runner-token")
	if err := os.WriteFile(tokenFile, []byte(token+"\n"), 0o600); err != nil {
		t.Fatalf("write token: %v", err)
	}
	agentFile := filepath.Join(dir, "agent.py")
	if err := os.WriteFile(agentFile, []byte(tunnelAgentScript), 0o600); err != nil {
		t.Fatalf("write agent: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var agentLog syncBuffer
	cmd := exec.CommandContext(ctx, python, "-I", agentFile) //nolint:gosec // test-controlled script path
	cmd.Env = append(os.Environ(),
		"CLOUDVM_TUNNEL_CA_FILE="+caFile,
		"CLOUDVM_TUNNEL_ENDPOINT="+endpoint,
		"CLOUDVM_INSTANCE_ID="+instanceID,
		"CLOUDVM_RUNNER_ADDR="+runnerListener.Addr().String(),
		"CLOUDVM_RUNNER_TOKEN_FILE="+tokenFile,
		"CLOUDVM_ATTESTATION_FILE="+filepath.Join(dir, "absent"),
		"CLOUDVM_TUNNEL_MAX_BACKOFF=0.2",
	)
	cmd.Stderr = &agentLog
	if err := cmd.Start(); err != nil {
		t.Fatalf("start agent: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("agent log:\n%s", agentLog.String())
		}
	})

	// Interpreter start-up dominates on loaded CI runners.
	waitForWithin(t, 60*time.Second, "python agent session", func() bool { return f.registry.Has(instanceID) })
	runner := f.gateway.Runner()
	if err := runner.StartContainer(ctx, node, &StartContainerRequest{PodUID: "pod-py", Image: "alpine"}); err != nil {
		t.Fatalf("StartContainer: %v", err)
	}
	status, err := runner.GetStatus(ctx, node, "pod-py")
	if err != nil || !status.Running {
		t.Fatalf("GetStatus: %+v %v", status, err)
	}

	session, _ := f.registry.Get(instanceID)
	client := &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{
		DialContext: func(context.Context, string, string) (net.Conn, error) { return session.Open() },
	}}
	payload := bytes.Repeat([]byte("0123456789abcdef"), 64*1024) // 1 MiB
	resp, err := client.Post("http://clvm-tunnel/echo", "application/octet-stream", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("echo: %v", err)
	}
	echoed, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if !bytes.Equal(echoed, payload) {
		t.Fatalf("echo returned %d bytes, want %d", len(echoed), len(payload))
	}

	// Dropping the session must make the agent reconnect on its own.
	_ = f.registry.Close(instanceID)
	waitForWithin(t, 60*time.Second, "python agent reconnect", func() bool {
		s, ok := f.registry.Get(instanceID)
		return ok && s != session
	})
	if _, err := runner.GetHostStats(ctx, node); err != nil {
		t.Fatalf("GetHostStats after reconnect: %v", err)
	}
	if strings.Contains(agentLog.String(), token) {
		t.Fatal("agent must never log the runner token")
	}
}

func TestTunnelAgent_PythonAgentRejectedWithWrongToken(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	f := newTunnelFixture(t, time.Minute)
	f.addNode("i-imposter", "eu-west-1", "real-token")
	endpoint, caFile := startTLSTerminator(t, f.gateway.Addr().String())
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "runner-token")
	_ = os.WriteFile(tokenFile, []byte("forged-token"), 0o600)
	agentFile := filepath.Join(dir, "agent.py")
	_ = os.WriteFile(agentFile, []byte(tunnelAgentScript), 0o600)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var agentLog syncBuffer
	cmd := exec.CommandContext(ctx, python, "-I", agentFile) //nolint:gosec // test-controlled script path
	cmd.Env = append(os.Environ(),
		"CLOUDVM_TUNNEL_CA_FILE="+caFile,
		"CLOUDVM_TUNNEL_ENDPOINT="+endpoint,
		"CLOUDVM_INSTANCE_ID=i-imposter",
		"CLOUDVM_RUNNER_TOKEN_FILE="+tokenFile,
		"CLOUDVM_TUNNEL_MAX_BACKOFF=0.2",
	)
	cmd.Stderr = &agentLog
	if err := cmd.Start(); err != nil {
		t.Fatalf("start agent: %v", err)
	}
	defer func() { cancel(); _ = cmd.Wait() }()
	waitForWithin(t, 60*time.Second, "agent rejection", func() bool {
		return strings.Contains(agentLog.String(), "handshake rejected")
	})
	if f.registry.Has("i-imposter") {
		t.Fatal("agent with a forged token must not get a session")
	}
}
