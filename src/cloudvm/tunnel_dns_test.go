// Tests cluster DNS over worker tunnel sessions: the real Python agent's forwarder against the gateway and fake resolvers.
package main

import (
	"bufio"
	"fmt"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

// fakeResolver answers A queries over UDP and TCP on one port from a fixed table, truncating UDP replies over 512
// bytes and ignoring names that start with "silent.", and records every name it is asked.
type fakeResolver struct {
	addr    string
	records map[string][]netip.Addr
	mu      sync.Mutex
	asked   []string
}

func startFakeResolver(t *testing.T, records map[string][]netip.Addr) *fakeResolver {
	t.Helper()
	tcp, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("resolver listen: %v", err)
	}
	t.Cleanup(func() { _ = tcp.Close() })
	udp, err := net.ListenPacket("udp", tcp.Addr().String())
	if err != nil {
		t.Fatalf("resolver listen udp: %v", err)
	}
	t.Cleanup(func() { _ = udp.Close() })
	r := &fakeResolver{addr: tcp.Addr().String(), records: records}
	go func() {
		buf := make([]byte, maxDNSReply)
		for {
			n, from, err := udp.ReadFrom(buf)
			if err != nil {
				return
			}
			if reply := r.answer(buf[:n], true); reply != nil {
				_, _ = udp.WriteTo(reply, from)
			}
		}
	}()
	go func() {
		for {
			conn, err := tcp.Accept()
			if err != nil {
				return
			}
			go func() {
				defer conn.Close()
				query, err := readDNSMessage(bufio.NewReader(conn), maxDNSQuery)
				if err != nil {
					return
				}
				if reply := r.answer(query, false); reply != nil {
					_ = writeDNSMessage(conn, reply)
				}
			}()
		}
	}()
	return r
}

func (r *fakeResolver) answer(query []byte, udp bool) []byte {
	header, question, err := parseDNSQuery(query)
	if err != nil {
		return nil
	}
	name := strings.ToLower(question.Name.String())
	r.mu.Lock()
	r.asked = append(r.asked, name)
	r.mu.Unlock()
	if strings.HasPrefix(name, "silent.") {
		return nil
	}
	addrs, ok := r.records[name]
	reply := dnsmessage.Message{
		Header:    dnsmessage.Header{ID: header.ID, Response: true, RecursionDesired: header.RecursionDesired},
		Questions: []dnsmessage.Question{question},
	}
	if !ok {
		reply.RCode = dnsmessage.RCodeNameError
	}
	for _, addr := range addrs {
		reply.Answers = append(reply.Answers, dnsmessage.Resource{
			Header: dnsmessage.ResourceHeader{Name: question.Name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET, TTL: 30},
			Body:   &dnsmessage.AResource{A: addr.As4()},
		})
	}
	packed, err := reply.Pack()
	if err != nil {
		return nil
	}
	if udp && len(packed) > 512 {
		reply.Truncated, reply.Answers = true, nil
		packed, _ = reply.Pack()
	}
	return packed
}

func (r *fakeResolver) wasAsked(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, asked := range r.asked {
		if asked == name {
			return true
		}
	}
	return false
}

func packDNSQuery(t *testing.T, name string) []byte {
	t.Helper()
	return packDNSQueryType(t, name, dnsmessage.TypeA)
}

func packDNSQueryType(t *testing.T, name string, qtype dnsmessage.Type) []byte {
	t.Helper()
	msg := dnsmessage.Message{
		Header: dnsmessage.Header{ID: 0x5151, RecursionDesired: true},
		Questions: []dnsmessage.Question{
			{Name: dnsmessage.MustNewName(name), Type: qtype, Class: dnsmessage.ClassINET},
		},
	}
	query, err := msg.Pack()
	if err != nil {
		t.Fatalf("pack query: %v", err)
	}
	return query
}

// queryDNS asks the resolver at addr for the A records of name over UDP.
func queryDNS(t *testing.T, addr, name string) dnsmessage.Message {
	t.Helper()
	return queryDNSType(t, addr, name, dnsmessage.TypeA)
}

// queryDNSType asks the resolver at addr for the qtype records of name over UDP.
func queryDNSType(t *testing.T, addr, name string, qtype dnsmessage.Type) dnsmessage.Message {
	t.Helper()
	conn, err := net.DialTimeout("udp", addr, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", addr, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(15 * time.Second))
	if _, err := conn.Write(packDNSQueryType(t, name, qtype)); err != nil {
		t.Fatalf("write query: %v", err)
	}
	buf := make([]byte, maxDNSReply)
	n, err := conn.Read(buf)
	if err != nil {
		t.Fatalf("read reply for %s: %v", name, err)
	}
	var msg dnsmessage.Message
	if err := msg.Unpack(buf[:n]); err != nil {
		t.Fatalf("unpack reply for %s: %v", name, err)
	}
	if msg.ID != 0x5151 {
		t.Fatalf("reply for %s has ID %#x", name, msg.ID)
	}
	return msg
}

func answerAddrs(msg dnsmessage.Message) []string {
	var addrs []string
	for _, answer := range msg.Answers {
		if a, ok := answer.Body.(*dnsmessage.AResource); ok {
			addrs = append(addrs, netip.AddrFrom4(a.A).String())
		}
	}
	return addrs
}

func TestTunnelDNS_PythonAgentResolvesClusterNamesThroughTheGateway(t *testing.T) {
	f := newTunnelFixture(t, time.Minute)
	const instanceID, token = "i-python-dns", "agent-token"
	f.addNode(instanceID, "eu-west-1", token)
	many := make([]netip.Addr, 40) // 40 answers outgrow a 512-byte UDP reply
	for i := range many {
		many[i] = netip.AddrFrom4([4]byte{10, 96, 1, byte(i)})
	}
	cluster := startFakeResolver(t, map[string][]netip.Addr{
		"my-svc.my-ns.svc.cluster.local.": {netip.MustParseAddr("10.96.0.10")},
		"big.my-ns.svc.cluster.local.":    many,
	})
	upstream := startFakeResolver(t, map[string][]netip.Addr{
		"example.com.": {netip.MustParseAddr("192.0.2.1")},
	})
	f.gateway.clusterDNSServers = []string{cluster.addr}
	dnsAddr := fmt.Sprintf("127.0.0.1:%d", freePort(t))
	startPythonAgent(t, f, instanceID, token,
		"CLOUDVM_TCP_PROXY_PORT=0",
		"CLOUDVM_DNS_LISTEN="+dnsAddr,
		"CLOUDVM_DNS_UPSTREAM="+upstream.addr,
	)

	t.Run("cluster name", func(t *testing.T) {
		msg := queryDNS(t, dnsAddr, "my-svc.my-ns.svc.cluster.local.")
		if got := answerAddrs(msg); len(got) != 1 || got[0] != "10.96.0.10" {
			t.Fatalf("answers = %v, want [10.96.0.10]", got)
		}
	})
	t.Run("other name", func(t *testing.T) {
		msg := queryDNS(t, dnsAddr, "example.com.")
		if got := answerAddrs(msg); len(got) != 1 || got[0] != "192.0.2.1" {
			t.Fatalf("answers = %v, want [192.0.2.1]", got)
		}
	})
	t.Run("unknown cluster name", func(t *testing.T) {
		if msg := queryDNS(t, dnsAddr, "absent.my-ns.svc.cluster.local."); msg.RCode != dnsmessage.RCodeNameError {
			t.Fatalf("rcode = %v, want NXDOMAIN", msg.RCode)
		}
	})
	t.Run("each resolver sees only its own names", func(t *testing.T) {
		if upstream.wasAsked("my-svc.my-ns.svc.cluster.local.") {
			t.Error("the VM's upstream resolver was asked for a cluster name")
		}
		if cluster.wasAsked("example.com.") {
			t.Error("the cluster resolver was asked for an outside name")
		}
	})
	t.Run("a reply the cluster resolver truncates arrives complete", func(t *testing.T) {
		if got := answerAddrs(queryDNS(t, dnsAddr, "big.my-ns.svc.cluster.local.")); len(got) != len(many) {
			t.Fatalf("reply has %d answers, want %d", len(got), len(many))
		}
	})
	t.Run("a cluster name the cluster resolver ignores fails", func(t *testing.T) {
		if msg := queryDNS(t, dnsAddr, "silent.my-ns.svc.cluster.local."); msg.RCode != dnsmessage.RCodeServerFailure {
			t.Fatalf("rcode = %v, want SERVFAIL", msg.RCode)
		}
	})
}

func TestTunnelDNS_GatewayRefusesNamesOutsideTheClusterDomain(t *testing.T) {
	cluster := startFakeResolver(t, map[string][]netip.Addr{"example.com.": {netip.MustParseAddr("192.0.2.1")}})
	g := &TunnelGateway{clusterDomain: DefaultClusterDomain, clusterDNSServers: []string{cluster.addr}}
	for _, name := range []string{"example.com.", "svc.notcluster.local.", "cluster.local.example.com."} {
		reply, err := g.answerClusterDNS(packDNSQuery(t, name))
		if err != nil {
			t.Fatalf("answerClusterDNS(%s): %v", name, err)
		}
		var msg dnsmessage.Message
		if err := msg.Unpack(reply); err != nil {
			t.Fatalf("unpack reply for %s: %v", name, err)
		}
		if msg.RCode != dnsmessage.RCodeRefused || msg.ID != 0x5151 || len(msg.Questions) != 1 {
			t.Errorf("reply for %s = %+v, want REFUSED with the question", name, msg.Header)
		}
	}
	for _, name := range []string{"example.com.", "svc.notcluster.local.", "cluster.local.example.com."} {
		if cluster.wasAsked(name) {
			t.Errorf("the cluster resolver was asked for %s", name)
		}
	}
	if _, err := g.answerClusterDNS([]byte("not a dns message")); err == nil {
		t.Error("answerClusterDNS accepted a malformed message")
	}
}

func TestTunnelDNS_ResolvConfNameservers(t *testing.T) {
	path := filepath.Join(t.TempDir(), "resolv.conf")
	if err := os.WriteFile(path, []byte("search my-ns.svc.cluster.local\nnameserver 10.96.0.10\nnameserver fd00::a\noptions ndots:5\n"), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	got := ResolvConfNameservers(path)
	if want := []string{"10.96.0.10:53", "[fd00::a]:53"}; strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("nameservers = %v, want %v", got, want)
	}
}

// runAgentPython runs code with the embedded agent importable as the module agent and returns its standard output.
func runAgentPython(t *testing.T, code string) string {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "agent.py"), []byte(tunnelAgentScript), 0o600); err != nil {
		t.Fatalf("write agent: %v", err)
	}
	cmd := exec.Command(python, "-I", "-c", "import sys; sys.path.insert(0, sys.argv[1]); import agent\n"+code, dir) //nolint:gosec // test-controlled code
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("python: %v\n%s", err, out)
	}
	return string(out)
}

func TestTunnelDNS_AgentTakesOverResolvConf(t *testing.T) {
	path := filepath.Join(t.TempDir(), "resolv.conf")
	const original = "nameserver 127.0.0.53\noptions edns0 trust-ad\nsearch eu-west-1.compute.internal\n"
	if err := os.WriteFile(path, []byte(original), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	// The second start models an agent restart, which must still forward to the VM's own resolver.
	out := runAgentPython(t, fmt.Sprintf(`
import asyncio, os
os.environ.update(CLOUDVM_DNS_RESOLV_CONF=%q, CLOUDVM_DNS_TAKEOVER="1")
loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
for _ in range(2):
    print(loop.run_until_complete(agent.start_dns_forwarder("127.0.0.1:0", None)).upstreams)
`, path))
	if want := "[('127.0.0.53', 53)]\n[('127.0.0.53', 53)]\n"; out != want {
		t.Errorf("upstreams = %q, want %q", out, want)
	}
	got, err := os.ReadFile(path) //nolint:gosec // test path
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	want := "nameserver 127.0.0.1\nnameserver 127.0.0.53\n" +
		"search svc.cluster.local cluster.local eu-west-1.compute.internal\noptions edns0 trust-ad\n"
	if string(got) != want {
		t.Errorf("resolv.conf =\n%s\nwant\n%s", got, want)
	}
}
