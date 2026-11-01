// Tests the tunnel agent's per-pod attribution from fixture /proc files and its synthetic answers for allow-listed
// external hostnames, running the real agent against the Go gateway.
package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

// procSocket is one socket of a fixture /proc: its local address, given as /proc/net hex, and the process holding it.
type procSocket struct {
	proto, local string
	port, inode  int
	pid          int
	cgroup       string
}

// writeProcFixture lays out /proc/net/{tcp,tcp6,udp} and /proc/<pid>/{fd,cgroup} for sockets under a new directory.
func writeProcFixture(t *testing.T, sockets []procSocket) string {
	t.Helper()
	root := t.TempDir()
	tables := map[string][]string{"tcp": nil, "tcp6": nil, "udp": nil}
	for i, s := range sockets {
		tables[s.proto] = append(tables[s.proto], fmt.Sprintf(
			"%4d: %s:%04X %s:0000 01 00000000:00000000 00:00000000 00000000  1000        0 %d 1 0000000000000000 20 4 30 10 -1",
			i, s.local, s.port, strings.Repeat("0", len(s.local)), s.inode))
		if s.pid == 0 {
			continue
		}
		dir := filepath.Join(root, fmt.Sprint(s.pid))
		if err := os.MkdirAll(filepath.Join(dir, "fd"), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink("/dev/null", filepath.Join(dir, "fd", "0")); err != nil {
			t.Fatal(err)
		}
		if err := os.Symlink(fmt.Sprintf("socket:[%d]", s.inode), filepath.Join(dir, "fd", "7")); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "cgroup"), []byte(s.cgroup), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.MkdirAll(filepath.Join(root, "net"), 0o750); err != nil {
		t.Fatal(err)
	}
	const header = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode\n"
	for name, lines := range tables {
		content := header + strings.Join(lines, "\n") + "\n"
		if err := os.WriteFile(filepath.Join(root, "net", name), []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return root
}

const (
	loopbackHex       = "0100007F"                         // 127.0.0.1 in /proc/net byte order on little-endian hosts
	mappedLoopbackHex = "0000000000000000FFFF00000100007F" // ::ffff:127.0.0.1
	unattributedPod   = "unattributed"
	burstablePod      = "6f1c2a4e-0b7d-4e55-9a51-1d2e3f405162"
	besteffortPod     = "0a0b0c0d-1e2f-4a5b-8c6d-7e8f9a0b1c2d"
)

func TestTunnelAgent_AttributesSocketsToPodsFromProc(t *testing.T) {
	root := writeProcFixture(t, []procSocket{
		{proto: "tcp", local: loopbackHex, port: 8080, inode: 4242, pid: 100,
			cgroup: "0::/kubepods/burstable/pod" + burstablePod + "/" + strings.Repeat("ab", 32) + "\n"},
		{proto: "tcp6", local: mappedLoopbackHex, port: 9090, inode: 4343, pid: 200,
			cgroup: "12:memory:/kubepods.slice/kubepods-besteffort.slice/kubepods-besteffort-pod" +
				strings.ReplaceAll(besteffortPod, "-", "_") + ".slice/cri-containerd-0123.scope\n0::/\n"},
		{proto: "udp", local: "00000000", port: 5353, inode: 4444, pid: 300, cgroup: "0::/system.slice/sshd.service\n"},
		{proto: "tcp", local: loopbackHex, port: 7070, inode: 4545},
	})
	out := runAgentPython(t, fmt.Sprintf(`
r = agent.PodResolver(%q)
for proto, port in [("tcp", 8080), ("tcp", 9090), ("udp", 5353), ("tcp", 7070), ("tcp", 1)]:
    print(proto, port, repr(r.pod_of(proto, ("127.0.0.1", port))))
print("cached", repr(r.pod_of("tcp", ("127.0.0.1", 8080))))
`, root))
	want := fmt.Sprintf("tcp 8080 '%s'\ntcp 9090 '%s'\nudp 5353 ''\ntcp 7070 None\ntcp 1 None\ncached '%s'\n",
		burstablePod, besteffortPod, burstablePod)
	if out != want {
		t.Errorf("attribution =\n%s\nwant\n%s", out, want)
	}
}

func TestTunnelAgent_StreamHeadersNameThePod(t *testing.T) {
	out := runAgentPython(t, `
print(agent.stream_header({"stream": "tcp", "version": 1, "target": "10.0.0.1:80"}, "").decode(), end="")
print(agent.stream_header({"stream": "tcp", "version": 1, "target": "10.0.0.1:80"}, "abc").decode(), end="")
`)
	want := `{"stream": "tcp", "version": 1, "target": "10.0.0.1:80"}` + "\n" +
		`{"stream": "tcp", "version": 1, "target": "10.0.0.1:80", "pod": "abc"}` + "\n"
	if out != want {
		t.Errorf("headers =\n%s\nwant\n%s", out, want)
	}
}

// connectFrom sends a CONNECT for target to the proxy at proxyAddr from localPort and returns the response status, or 0
// when the proxy closes the connection without one, with the connection.
func connectFrom(t *testing.T, localPort int, proxyAddr, target string) (int, net.Conn, *bufio.Reader) {
	t.Helper()
	dialer := net.Dialer{Timeout: 5 * time.Second, LocalAddr: &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: localPort}}
	conn, err := dialer.Dial("tcp", proxyAddr)
	if err != nil {
		t.Fatalf("dial proxy from port %d: %v", localPort, err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	if _, err := fmt.Fprintf(conn, "CONNECT %s HTTP/1.1\r\nHost: %s\r\n\r\n", target, target); err != nil {
		t.Fatalf("write CONNECT: %v", err)
	}
	reader := bufio.NewReader(conn)
	resp, err := http.ReadResponse(reader, &http.Request{Method: http.MethodConnect})
	if err != nil {
		return 0, conn, reader
	}
	_ = resp.Body.Close()
	return resp.StatusCode, conn, reader
}

func TestTunnelTCP_PythonAgentAuthorizesStreamsByTheirPod(t *testing.T) {
	f := newTunnelFixture(t, time.Minute)
	const instanceID, token = "i-python-pods", "pods-token"
	f.addNode(instanceID, "eu-west-1", token)
	f.gateway.directTCPNetworks, _ = ParseDirectTCPNetworks("127.0.0.0/8")
	pods, refusals := isolateNamespaces(f, instanceID, "team-b")
	// The cluster echo server is a pod of the node agent's namespace, which the worker's own namespace may not reach.
	pods.set(true, map[string]string{"127.0.0.1": "profiler"})
	var mu sync.Mutex
	var named []string
	f.gateway.podOnWorker = func(id, pod string) (string, bool) {
		mu.Lock()
		defer mu.Unlock()
		named = append(named, pod)
		return "profiler", id == instanceID && pod == burstablePod
	}
	lastNamed := func() string {
		mu.Lock()
		defer mu.Unlock()
		if len(named) == 0 {
			return ""
		}
		return named[len(named)-1]
	}

	ports := []int{freePort(t), freePort(t), freePort(t), freePort(t)}
	root := writeProcFixture(t, []procSocket{
		{proto: "tcp", local: loopbackHex, port: ports[0], inode: 5001, pid: 101,
			cgroup: "0::/kubepods/burstable/pod" + burstablePod + "/" + strings.Repeat("cd", 32) + "\n"},
		{proto: "tcp", local: loopbackHex, port: ports[1], inode: 5002, pid: 102,
			cgroup: "0::/kubepods/besteffort/pod" + besteffortPod + "/" + strings.Repeat("ef", 32) + "\n"},
		{proto: "tcp", local: loopbackHex, port: ports[2], inode: 5003, pid: 103, cgroup: "0::/system.slice/cron.service\n"},
	})
	agentProxyPort := freePort(t)
	startPythonAgent(t, f, instanceID, token, fmt.Sprintf("CLOUDVM_TCP_PROXY_PORT=%d", agentProxyPort), "CLOUDVM_PROC_ROOT="+root)
	agentProxy := fmt.Sprintf("127.0.0.1:%d", agentProxyPort)
	echo := startEchoServer(t)

	t.Run("a node agent pod reaches its namespace", func(t *testing.T) {
		status, conn, reader := connectFrom(t, ports[0], agentProxy, echo)
		if status != http.StatusOK {
			t.Fatalf("CONNECT status %d", status)
		}
		if _, err := conn.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}
		got := make([]byte, 4)
		if _, err := io.ReadFull(reader, got); err != nil || string(got) != "ping" {
			t.Fatalf("echo = %q, %v", got, err)
		}
		if pod := lastNamed(); pod != burstablePod {
			t.Errorf("stream named pod %q, want %q", pod, burstablePod)
		}
	})
	for _, tc := range []struct {
		name, reason string
		port         int
	}{
		{"a pod the plugin did not place on the worker", "foreign_pod", ports[1]},
		{"a process of the VM acts for the worker's namespace", "namespace_mismatch", ports[2]},
		{"a connection no process holds", "foreign_pod", ports[3]},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, conn, reader := connectFrom(t, tc.port, agentProxy, echo)
			_, _ = conn.Write([]byte("x"))
			if _, err := reader.ReadByte(); err == nil {
				t.Fatal("refused stream carried data")
			}
			select {
			case got := <-refusals:
				if got != tc.reason {
					t.Fatalf("refused with %q, want %q", got, tc.reason)
				}
			case <-time.After(10 * time.Second):
				t.Fatal("no refusal recorded")
			}
		})
	}
	if pod := lastNamed(); pod != unattributedPod {
		t.Errorf("unattributed stream named pod %q, want %q", pod, unattributedPod)
	}
}

func TestTunnelDNS_PythonAgentAnswersAllowListedHostnamesWithSyntheticAddresses(t *testing.T) {
	destinations, err := ParseNamespaceDestinations(`{"profiler": ["profiles.example.com:443"]}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	f := newTunnelFixture(t, time.Minute)
	f.gateway.destinations = destinations
	const instanceID, token = "i-python-synthetic", "synthetic-token"
	f.addNode(instanceID, "eu-west-1", token)
	upstream := startFakeResolver(t, nil)
	dnsAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	resolvConf := filepath.Join(t.TempDir(), "resolv.conf")
	if err := os.WriteFile(resolvConf, []byte("nameserver 127.0.0.1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	startPythonAgent(t, f, instanceID, token, "CLOUDVM_DNS_LISTEN="+dnsAddr, "CLOUDVM_DNS_RESOLV_CONF="+resolvConf,
		"CLOUDVM_DNS_UPSTREAM="+upstream.addr)
	want := destinations.SyntheticHosts()["profiles.example.com"]

	reply := queryDNS(t, dnsAddr, "Profiles.Example.com.")
	if got := answerAddrs(reply); len(got) != 1 || got[0] != want || reply.RCode != dnsmessage.RCodeSuccess {
		t.Fatalf("A answers = %v (rcode %v), want [%s]", got, reply.RCode, want)
	}
	if upstream.wasAsked("Profiles.Example.com.") || upstream.wasAsked("profiles.example.com.") {
		t.Error("the agent forwarded an allow-listed hostname to the VM's resolver")
	}
	aaaa := queryDNSType(t, dnsAddr, "profiles.example.com.", dnsmessage.TypeAAAA)
	if aaaa.RCode != dnsmessage.RCodeSuccess || len(aaaa.Answers) != 0 {
		t.Errorf("AAAA reply = rcode %v with %d answers, want NOERROR without answers", aaaa.RCode, len(aaaa.Answers))
	}

	out := runAgentPython(t, fmt.Sprintf(`
import asyncio
p = agent.TCPProxy(tunnel_networks=agent.parse_networks("10.0.0.0/8"))
p.set_destinations({"profiles.example.com": %q, "bad.example.com": "10.0.0.1"}, ["203.0.113.7:4317"])
loop = asyncio.new_event_loop()
for target in ["%s:443", "profiles.example.com:443", "203.0.113.7:4317", "203.0.113.7:22"]:
    print(loop.run_until_complete(p.route(target)))
print(p.tunnel_target("%s:8443"), sorted(p.synthetic_names))
`, want, want, want))
	wantOut := "(True, 'profiles.example.com:443')\n(True, 'profiles.example.com:443')\n(True, '203.0.113.7:4317')\n" +
		"(False, '203.0.113.7:22')\nprofiles.example.com:8443 ['profiles.example.com']\n"
	if out != wantOut {
		t.Errorf("routes =\n%s\nwant\n%s", out, wantOut)
	}
}

func TestTunnelAgent_LogsUnattributedAndRefusedStreamsAtABoundedRate(t *testing.T) {
	out := runAgentPython(t, `
import asyncio, sys
sys.stderr = sys.stdout
now = [0.0]
limited = agent.FailureLog(interval=10, clock=lambda: now[0])
for t in (0, 1, 2):
    now[0] = t
    limited("refused", "refused %d" % t)
now[0] = 12
limited("refused", "refused 12")
limited("other", "other kind")

agent.log_failure = agent.FailureLog()
class Unknown(object):
    def pod_of(self, proto, peer):
        return None
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
proxy = agent.TCPProxy(pods=Unknown())
print("pod", loop.run_until_complete(proxy.attribute("udp", ("127.0.0.1", 40000))))
refused = agent.Stream(None, 3, local=(None, None), target="10.1.2.3:6379", pod="uid-a")
refused.on_fin()
answered = agent.Stream(None, 5, local=(None, None), target="10.1.2.3:6380", pod="uid-a")
answered.on_data(b"+OK")
answered.on_fin()
`)
	for _, want := range []string{
		"[cloudvm-tunnel-agent] refused 0\n",
		"[cloudvm-tunnel-agent] refused 12 (2 more since the last report)\n",
		"[cloudvm-tunnel-agent] other kind\n",
		"no process holds the udp socket at ('127.0.0.1', 40000); the gateway refuses its stream as pod unattributed\n",
		"pod unattributed\n",
		"gateway closed the stream to 10.1.2.3:6379 for pod uid-a before sending any data",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("agent output lacks %q:\n%s", want, out)
		}
	}
	for _, unwanted := range []string{"refused 1\n", "refused 2\n", "10.1.2.3:6380"} {
		if strings.Contains(out, unwanted) {
			t.Errorf("agent output has %q:\n%s", unwanted, out)
		}
	}
}

func TestTunnelAgent_LabelsContainerLogsWithTheirPodAndInitContainer(t *testing.T) {
	out := runAgentPython(t, fmt.Sprintf(`
import json
for name in ["cloudvm-%[1]s", "cloudvm-%[1]s-init-1-wait-gcs-ready"]:
    pod, container = agent.container_identity(name)
    print(json.dumps(json.loads(agent.log_record("container", "m", "t", pod, "stdout", container)), sort_keys=True))
print("tunnel=/var/log/cloudvm-tunnel-agent.log" in agent.DEFAULT_LOG_FILES)
`, burstablePod))
	want := fmt.Sprintf(`{"fd": "stdout", "msg": "m", "pod": "%[1]s", "src": "container", "time": "t"}
{"container": "wait-gcs-ready", "fd": "stdout", "msg": "m", "pod": "%[1]s", "src": "container", "time": "t"}
True
`, burstablePod)
	if out != want {
		t.Errorf("records =\n%s\nwant\n%s", out, want)
	}
}
