// Tests TCP forwarding over worker tunnel sessions between the real Python agent and the gateway, in all three directions.
package main

import (
	"bufio"
	"bytes"
	"context"
	"errors"
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
	return startEchoServerOn(t, "127.0.0.1:0")
}

func startEchoServerOn(t *testing.T, addr string) string {
	t.Helper()
	l, err := net.Listen("tcp", addr)
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

// startResetServer accepts connections, reads one chunk, and closes each with a TCP reset.
func startResetServer(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reset listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			_, _ = conn.Read(make([]byte, 1))
			_ = conn.(*net.TCPConn).SetLinger(0)
			_ = conn.Close()
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

// startPythonAgent runs the embedded agent as worker instanceID against the fixture's gateway, with env added to its
// environment, and waits until its session registers; it stops the agent when the test ends.
func startPythonAgent(t *testing.T, f *tunnelFixture, instanceID, token string, env ...string) {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
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
	var agentLog syncBuffer
	cmd := exec.CommandContext(ctx, python, "-I", agentFile) //nolint:gosec // test-controlled script path
	cmd.Env = append(append(os.Environ(),
		"CLOUDVM_TUNNEL_CA_FILE="+caFile,
		"CLOUDVM_TUNNEL_ENDPOINT="+endpoint,
		"CLOUDVM_INSTANCE_ID="+instanceID,
		"CLOUDVM_RUNNER_ADDR=127.0.0.1:1",
		"CLOUDVM_RUNNER_TOKEN_FILE="+tokenFile,
		"CLOUDVM_ATTESTATION_FILE="+filepath.Join(dir, "absent"),
		"CLOUDVM_CONTAINER_LOGS=0",
	), env...)
	cmd.Stderr = &agentLog
	if err := cmd.Start(); err != nil {
		cancel()
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
}

func TestTunnelTCP_PythonAgentForwardsBetweenWorkersAndCluster(t *testing.T) {
	f := newTunnelFixture(t, time.Minute)
	const instanceID, token, workerIP = "i-python-tcp", "agent-token", "10.9.9.9"
	f.addNode(instanceID, "eu-west-1", token)
	f.gateway.resolver = func(ip string) (string, bool) { return instanceID, ip == workerIP }
	f.gateway.directTCPNetworks, _ = ParseDirectTCPNetworks("127.0.0.0/8")

	echoAddr := startEchoServer(t)
	_, echoPort, _ := net.SplitHostPort(echoAddr)
	agentProxyPort := freePort(t)
	startPythonAgent(t, f, instanceID, token,
		fmt.Sprintf("CLOUDVM_TCP_PROXY_PORT=%d", agentProxyPort),
		// The gateway forwards only to 127.0.0.0/8, so a target on ::1 succeeds only when the agent connects it directly.
		"CLOUDVM_DIRECT_CIDRS=::1/128",
	)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

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
	t.Run("worker connects its direct networks without the tunnel", func(t *testing.T) {
		connectAndEcho(t, agentProxy, startEchoServerOn(t, "[::1]:0"))
	})
	t.Run("a reset on the worker closes the cluster connection", func(t *testing.T) {
		_, resetPort, _ := net.SplitHostPort(startResetServer(t))
		target := net.JoinHostPort(workerIP, resetPort)
		conn, err := net.DialTimeout("tcp", clusterProxy.Addr().String(), 5*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		_, _ = fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target)
		reader := bufio.NewReader(conn)
		resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
		if err != nil || resp.StatusCode != http.StatusOK {
			t.Fatalf("CONNECT %s: %v %v", target, resp, err)
		}
		_ = resp.Body.Close()
		_, _ = conn.Write([]byte("x"))
		_, err = reader.ReadByte()
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			t.Fatal("cluster connection stayed open after the worker side reset")
		}
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

func TestTunnelTCP_PythonAgentTunnelsOnlyTunnelNetworks(t *testing.T) {
	f := newTunnelFixture(t, time.Minute)
	const instanceID, token, workerIP = "i-python-egress", "agent-token", "10.9.9.9"
	f.addNode(instanceID, "eu-west-1", token)
	f.gateway.resolver = func(ip string) (string, bool) { return instanceID, ip == workerIP }
	// The gateway forwards nothing directly, so a target outside the workers succeeds only when the agent dials it.
	f.gateway.directTCPNetworks = nil

	echoAddr := startEchoServer(t)
	_, echoPort, _ := net.SplitHostPort(echoAddr)
	agentProxyPort := freePort(t)
	startPythonAgent(t, f, instanceID, token,
		fmt.Sprintf("CLOUDVM_TCP_PROXY_PORT=%d", agentProxyPort),
		"CLOUDVM_DIRECT_CIDRS=::1/128",
		"CLOUDVM_TUNNEL_CIDRS=10.0.0.0/8",
	)
	agentProxy := fmt.Sprintf("127.0.0.1:%d", agentProxyPort)

	t.Run("a target inside the tunnel networks goes through the tunnel", func(t *testing.T) {
		connectAndEcho(t, agentProxy, net.JoinHostPort(workerIP, echoPort))
	})
	t.Run("a target inside the direct networks is dialed from the worker", func(t *testing.T) {
		connectAndEcho(t, agentProxy, startEchoServerOn(t, "[::1]:0"))
	})
	t.Run("a target outside both is dialed from the worker", func(t *testing.T) {
		connectAndEcho(t, agentProxy, echoAddr)
	})
	t.Run("a host name is routed by its address", func(t *testing.T) {
		// localhost may resolve to either loopback address first, so both serve the echo on one port.
		startEchoServerOn(t, net.JoinHostPort("::1", echoPort))
		connectAndEcho(t, agentProxy, net.JoinHostPort("localhost", echoPort))
	})
}
