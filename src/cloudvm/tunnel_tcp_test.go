// Tests TCP forwarding over worker tunnel sessions between the real Python agent and the gateway, in all three directions.
package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"
)

func startEchoServer(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("echo listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return l.Addr().String()
}

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// connectAndEcho opens a CONNECT tunnel through proxyAddr to target and checks a 1 MiB payload comes back unchanged.
func connectAndEcho(t *testing.T, proxyAddr, target string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", proxyAddr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial proxy %s: %v", proxyAddr, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(30 * time.Second))
	if _, err := fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		t.Fatalf("CONNECT %s via %s: %v", target, proxyAddr, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("CONNECT %s via %s: status %d", target, proxyAddr, resp.StatusCode)
	}
	payload := bytes.Repeat([]byte("0123456789abcdef"), 64*1024) // 1 MiB crosses the 256 KiB stream window
	go func() { _, _ = conn.Write(payload) }()
	echoed := make([]byte, len(payload))
	if _, err := io.ReadFull(reader, echoed); err != nil {
		t.Fatalf("read echo via %s: %v", proxyAddr, err)
	}
	if !bytes.Equal(echoed, payload) {
		t.Fatalf("echo via %s differs", proxyAddr)
	}
}

func TestTunnelTCP_PythonAgentForwardsBetweenWorkersAndCluster(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	f := newTunnelFixture(t, time.Minute)
	const instanceID, token, workerIP = "i-python-tcp", "agent-token", "10.9.9.9"
	f.addNode(instanceID, "eu-west-1", token)
	f.gateway.resolver = func(ip string) (string, bool) { return instanceID, ip == workerIP }
	f.gateway.directTCPNetworks, _ = ParseDirectTCPNetworks("127.0.0.0/8")

	echoAddr := startEchoServer(t)
	_, echoPort, _ := net.SplitHostPort(echoAddr)
	agentProxyPort := freePort(t)

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
		"CLOUDVM_RUNNER_ADDR=127.0.0.1:1",
		"CLOUDVM_RUNNER_TOKEN_FILE="+tokenFile,
		"CLOUDVM_ATTESTATION_FILE="+filepath.Join(dir, "absent"),
		"CLOUDVM_CONTAINER_LOGS=0",
		fmt.Sprintf("CLOUDVM_TCP_PROXY_PORT=%d", agentProxyPort),
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
	waitForWithin(t, 60*time.Second, "python agent session", func() bool { return f.registry.Has(instanceID) })

	clusterProxy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cluster proxy listen: %v", err)
	}
	go f.gateway.ServeConnectProxy(ctx, clusterProxy)
	agentProxy := fmt.Sprintf("127.0.0.1:%d", agentProxyPort)

	t.Run("worker to cluster", func(t *testing.T) {
		connectAndEcho(t, agentProxy, echoAddr)
	})
	t.Run("cluster to worker", func(t *testing.T) {
		connectAndEcho(t, clusterProxy.Addr().String(), net.JoinHostPort(workerIP, echoPort))
	})
	t.Run("worker to worker", func(t *testing.T) {
		connectAndEcho(t, agentProxy, net.JoinHostPort(workerIP, echoPort))
	})
	t.Run("direct forwarding refuses well-known ports and other networks", func(t *testing.T) {
		g := &TunnelGateway{}
		g.directTCPNetworks, _ = ParseDirectTCPNetworks("10.112.0.0/12")
		for _, tc := range []struct{ host, port string }{
			{"10.112.0.5", "443"},
			{"10.112.0.5", "22"},
			{"10.200.0.5", "6379"},
			{"api.example.com", "6379"},
		} {
			if err := g.allowDirect(tc.host, tc.port); err == nil {
				t.Errorf("allowDirect(%s, %s) = nil, want refusal", tc.host, tc.port)
			}
		}
		if err := g.allowDirect("10.112.0.5", "6379"); err != nil {
			t.Errorf("allowDirect(10.112.0.5, 6379) = %v, want nil", err)
		}
		if err := (&TunnelGateway{}).allowDirect("10.112.0.5", "6379"); err == nil {
			t.Error("allowDirect without networks = nil, want refusal")
		}
	})
	t.Run("cluster proxy refuses unknown addresses", func(t *testing.T) {
		conn, err := net.DialTimeout("tcp", clusterProxy.Addr().String(), 5*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close()
		_, _ = fmt.Fprintf(conn, "CONNECT 10.1.1.1:80 HTTP/1.1\r\nHost: 10.1.1.1:80\r\n\r\n")
		resp, err := http.ReadResponse(bufio.NewReader(conn), &http.Request{Method: http.MethodConnect})
		if err != nil {
			t.Fatalf("read response: %v", err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusBadGateway {
			t.Fatalf("want 502, got %d", resp.StatusCode)
		}
	})
}
