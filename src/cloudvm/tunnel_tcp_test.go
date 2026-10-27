// Tests TCP forwarding over worker tunnel sessions between the real Python agent and the gateway, in all three directions,
// and the gateway's refusal of streams between namespaces.
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
	"strings"
	"sync"
	"sync/atomic"
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

// startResetServerOn accepts connections on addr, reads one chunk, and closes each with a TCP reset.
func startResetServerOn(t *testing.T, addr string) string {
	t.Helper()
	l, err := net.Listen("tcp", addr)
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

// localWorkerIP returns an IPv4 address of this host other than loopback, which stands in for a worker's private IP.
func localWorkerIP(t *testing.T) string {
	t.Helper()
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatalf("interface addresses: %v", err)
	}
	for _, addr := range addrs {
		if n, ok := addr.(*net.IPNet); ok && n.IP.To4() != nil && n.IP.IsGlobalUnicast() {
			return n.IP.String()
		}
	}
	t.Skip("no non-loopback IPv4 address")
	return ""
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

// fakePodNamespaces maps pod IPs to namespaces once synced.
type fakePodNamespaces struct {
	mu     sync.Mutex
	synced bool
	byIP   map[string]string
}

func (p *fakePodNamespaces) HasSynced() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.synced
}

func (p *fakePodNamespaces) PodNamespace(ip string) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	namespace, ok := p.byIP[ip]
	return namespace, ok
}

func (p *fakePodNamespaces) set(synced bool, byIP map[string]string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.synced, p.byIP = synced, byIP
}

// isolateNamespaces puts worker instanceID and the cluster pods on loopback, which stand in for every cluster client
// and target, in namespace, and returns the pod mapping and the refusal reasons the gateway records.
func isolateNamespaces(f *tunnelFixture, instanceID, namespace string) (*fakePodNamespaces, chan string) {
	if node, ok := f.server.NodePool().Node(instanceID); ok {
		node.Namespace = namespace
	}
	pods := &fakePodNamespaces{synced: true, byIP: map[string]string{"127.0.0.1": namespace}}
	refusals := make(chan string, 64)
	f.gateway.podNamespaces = pods
	f.gateway.workerNamespace = f.server.WorkerNamespace
	f.gateway.onTCPRefusal = func(reason string) {
		f.metrics.RecordTCPRefusal(reason)
		refusals <- reason
	}
	return pods, refusals
}

// expectRefused checks that a CONNECT through proxyAddr to target carries no data, and that the gateway refused it
// with reason.
func expectRefused(t *testing.T, proxyAddr, target string, refusals <-chan string, reason string) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", proxyAddr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial proxy %s: %v", proxyAddr, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
	_, _ = fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target)
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err == nil {
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			_, _ = conn.Write([]byte("x"))
			_, err = reader.ReadByte()
			if err == nil {
				t.Fatalf("CONNECT %s via %s carried data", target, proxyAddr)
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() {
				t.Fatalf("CONNECT %s via %s stayed open after the refusal", target, proxyAddr)
			}
		}
	}
	select {
	case got := <-refusals:
		if got != reason {
			t.Fatalf("CONNECT %s via %s refused with %q, want %q", target, proxyAddr, got, reason)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("CONNECT %s via %s recorded no refusal", target, proxyAddr)
	}
}

func TestTunnelTCP_PythonAgentForwardsBetweenWorkersAndCluster(t *testing.T) {
	f := newTunnelFixture(t, time.Minute)
	const instanceID, token = "i-python-tcp", "agent-token"
	// The worker's servers listen only on its private address, as Ray's agents do; 192.0.2.1 is another host's.
	workerIP := localWorkerIP(t)
	f.addNode(instanceID, "eu-west-1", token)
	f.gateway.resolver = func(ip string) (string, bool) { return instanceID, ip == workerIP || ip == "192.0.2.1" }
	f.gateway.directTCPNetworks, _ = ParseDirectTCPNetworks("127.0.0.0/8")
	isolateNamespaces(f, instanceID, "team-a")

	echoAddr := startEchoServer(t)
	_, workerEchoPort, _ := net.SplitHostPort(startEchoServerOn(t, net.JoinHostPort(workerIP, "0")))
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
		connectAndEcho(t, clusterProxy.Addr().String(), net.JoinHostPort(workerIP, workerEchoPort))
	})
	t.Run("worker to worker", func(t *testing.T) {
		connectAndEcho(t, agentProxy, net.JoinHostPort(workerIP, workerEchoPort))
	})
	t.Run("the worker refuses a stream to an address it does not own", func(t *testing.T) {
		target := net.JoinHostPort("192.0.2.1", workerEchoPort)
		conn, err := net.DialTimeout("tcp", clusterProxy.Addr().String(), 5*time.Second)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		defer conn.Close()
		_ = conn.SetDeadline(time.Now().Add(10 * time.Second))
		_, _ = fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target)
		reader := bufio.NewReader(conn)
		resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
		if err != nil {
			t.Fatalf("CONNECT %s: %v", target, err)
		}
		_ = resp.Body.Close()
		_, _ = conn.Write([]byte("x"))
		if _, err := reader.ReadByte(); err == nil {
			t.Fatal("the worker forwarded a stream to another host's address")
		}
		var netErr net.Error
		if errors.As(err, &netErr) && netErr.Timeout() {
			t.Fatal("the cluster connection stayed open after the worker refused the stream")
		}
	})
	t.Run("worker connects its direct networks without the tunnel", func(t *testing.T) {
		connectAndEcho(t, agentProxy, startEchoServerOn(t, "[::1]:0"))
	})
	t.Run("a reset on the worker closes the cluster connection", func(t *testing.T) {
		_, resetPort, _ := net.SplitHostPort(startResetServerOn(t, net.JoinHostPort(workerIP, "0")))
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
	const instanceID, token = "i-python-egress", "agent-token"
	workerIP := localWorkerIP(t)
	f.addNode(instanceID, "eu-west-1", token)
	// The resolver records the addresses the gateway forwards to, which shows whether a connection used the tunnel.
	var tunneled atomic.Bool
	f.gateway.resolver = func(ip string) (string, bool) {
		if ip == workerIP {
			tunneled.Store(true)
		}
		return instanceID, ip == workerIP
	}
	// The gateway forwards nothing directly, so a target outside the workers succeeds only when the agent dials it.
	f.gateway.directTCPNetworks = nil
	isolateNamespaces(f, instanceID, "team-a")

	echoAddr := startEchoServer(t)
	_, echoPort, _ := net.SplitHostPort(echoAddr)
	_, workerEchoPort, _ := net.SplitHostPort(startEchoServerOn(t, net.JoinHostPort(workerIP, "0")))
	agentProxyPort := freePort(t)
	startPythonAgent(t, f, instanceID, token,
		fmt.Sprintf("CLOUDVM_TCP_PROXY_PORT=%d", agentProxyPort),
		"CLOUDVM_DIRECT_CIDRS=::1/128",
		"CLOUDVM_TUNNEL_CIDRS="+workerIP+"/32",
	)
	agentProxy := fmt.Sprintf("127.0.0.1:%d", agentProxyPort)

	t.Run("a target inside the tunnel networks goes through the tunnel", func(t *testing.T) {
		connectAndEcho(t, agentProxy, net.JoinHostPort(workerIP, workerEchoPort))
		if !tunneled.Load() {
			t.Fatal("the connection did not go through the gateway")
		}
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

func TestTunnelTCP_PythonAgentRefusesStreamsAcrossNamespaces(t *testing.T) {
	f := newTunnelFixture(t, time.Minute)
	const instanceID, otherID, token = "i-python-team-a", "i-python-team-b", "team-a-token"
	// 192.0.2.1 stands in for a worker of another namespace, which the gateway refuses before opening its session.
	workerIP := localWorkerIP(t)
	f.addNode(instanceID, "eu-west-1", token)
	f.addNode(otherID, "eu-west-1", "other-token").Namespace = "team-b"
	f.gateway.resolver = func(ip string) (string, bool) {
		switch ip {
		case workerIP:
			return instanceID, true
		case "192.0.2.1":
			return otherID, true
		}
		return "", false
	}
	f.gateway.directTCPNetworks, _ = ParseDirectTCPNetworks("127.0.0.0/8")
	pods, refusals := isolateNamespaces(f, instanceID, "team-a")

	echoAddr := startEchoServer(t)
	_, workerEchoPort, _ := net.SplitHostPort(startEchoServerOn(t, net.JoinHostPort(workerIP, "0")))
	workerEcho := net.JoinHostPort(workerIP, workerEchoPort)
	agentProxyPort := freePort(t)
	startPythonAgent(t, f, instanceID, token, fmt.Sprintf("CLOUDVM_TCP_PROXY_PORT=%d", agentProxyPort))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	clusterProxy, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("cluster proxy listen: %v", err)
	}
	go f.gateway.ServeConnectProxy(ctx, clusterProxy)
	agentProxy := fmt.Sprintf("127.0.0.1:%d", agentProxyPort)

	t.Run("worker to worker of another namespace", func(t *testing.T) {
		expectRefused(t, agentProxy, net.JoinHostPort("192.0.2.1", workerEchoPort), refusals, "namespace_mismatch")
	})
	pods.set(true, map[string]string{"127.0.0.1": "team-b"})
	t.Run("cluster pod of another namespace to worker", func(t *testing.T) {
		expectRefused(t, clusterProxy.Addr().String(), workerEcho, refusals, "namespace_mismatch")
	})
	t.Run("worker to cluster pod of another namespace", func(t *testing.T) {
		expectRefused(t, agentProxy, echoAddr, refusals, "namespace_mismatch")
	})
	pods.set(false, map[string]string{"127.0.0.1": "team-a"})
	t.Run("cluster to worker before the pod watch syncs", func(t *testing.T) {
		expectRefused(t, clusterProxy.Addr().String(), workerEcho, refusals, "unsynced")
	})
	t.Run("worker to cluster before the pod watch syncs", func(t *testing.T) {
		expectRefused(t, agentProxy, echoAddr, refusals, "unsynced")
	})
	pods.set(true, map[string]string{})
	t.Run("a cluster client that is no pod", func(t *testing.T) {
		expectRefused(t, clusterProxy.Addr().String(), workerEcho, refusals, "unknown_source")
	})
	t.Run("a cluster target that is no pod", func(t *testing.T) {
		expectRefused(t, agentProxy, echoAddr, refusals, "unknown_target")
	})
	pods.set(true, map[string]string{"127.0.0.1": "team-a"})
	t.Run("the same namespace is still allowed", func(t *testing.T) {
		connectAndEcho(t, clusterProxy.Addr().String(), workerEcho)
		connectAndEcho(t, agentProxy, echoAddr)
	})
	t.Run("refusals are counted by reason", func(t *testing.T) {
		body := scrapeMetrics(t, f.server)
		for _, want := range []string{
			`cloudvm_tunnel_tcp_refused_total{reason="namespace_mismatch"} 3`,
			`cloudvm_tunnel_tcp_refused_total{reason="unsynced"} 2`,
		} {
			if !strings.Contains(body, want) {
				t.Errorf("metrics lack %s:\n%s", want, body)
			}
		}
	})
}

func TestTunnelTCP_GatewayRefusesWorkersWithoutOneNamespace(t *testing.T) {
	np := NewNodePool(time.Minute)
	capacity, _ := NewResourceVector("4", "16Gi", 0, 0)
	np.RegisterNode(NewCloudVMNode("i-empty", ProviderMock, "eu-west-1", "10.0.0.1", capacity))
	g := &TunnelGateway{workerNamespace: np.InstanceNamespace}
	if _, err := g.dialFromWorker("i-empty", "10.0.0.2", "6379", "10.0.0.2:6379"); !isRefusal(err, "unknown_source") {
		t.Fatalf("stream from a worker without pods: %v, want unknown_source", err)
	}
	if _, err := (&TunnelGateway{}).dialFromWorker("i-empty", "10.0.0.2", "6379", "10.0.0.2:6379"); !isRefusal(err, "unknown_source") {
		t.Fatalf("stream without a worker namespace resolver: %v, want unknown_source", err)
	}
}

func isRefusal(err error, reason string) bool {
	var refusal *tcpRefusal
	return errors.As(err, &refusal) && refusal.reason == reason
}

func TestTunnelTCP_RefusalLogHoldsBackRepeatsPerReason(t *testing.T) {
	var l refusalLog
	now := time.Now()
	if _, ok := l.allow("namespace_mismatch", now); !ok {
		t.Fatal("first refusal not logged")
	}
	if _, ok := l.allow("namespace_mismatch", now.Add(time.Second)); ok {
		t.Fatal("repeat within the interval logged")
	}
	if _, ok := l.allow("unsynced", now.Add(time.Second)); !ok {
		t.Fatal("refusal with another reason held back")
	}
	if suppressed, ok := l.allow("namespace_mismatch", now.Add(tcpRefusalLogInterval)); !ok || suppressed != 1 {
		t.Fatalf("refusal after the interval: logged %v with %d held back, want true with 1", ok, suppressed)
	}
}
