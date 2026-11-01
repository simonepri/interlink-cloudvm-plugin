// Tests the gateway's forwarding of worker streams to Service ClusterIPs: namespace and shared-Service authorization,
// exposed ports, and the Service watch's ClusterIP index.
package main

import (
	"context"
	"io"
	"net"
	"reflect"
	"strconv"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type fakeServices struct {
	synced bool
	byIP   map[string]ClusterService
}

func (f *fakeServices) HasSynced() bool { return f.synced }

func (f *fakeServices) ServiceByIP(ip string) (ClusterService, bool) {
	svc, ok := f.byIP[ip]
	return svc, ok
}

func TestTunnelTCP_WorkerReachesServicesOfItsNamespaceAndSharedOnes(t *testing.T) {
	echo := startEchoServer(t)
	_, rawPort, _ := net.SplitHostPort(echo)
	parsed, _ := strconv.ParseInt(rawPort, 10, 32)
	port := int32(parsed)
	np := NewNodePool(time.Minute)
	capacity, _ := NewResourceVector("4", "16Gi", 0, 0)
	np.RegisterNode(NewCloudVMNode("i-team-a", ProviderMock, "eu-west-1", "10.0.0.1", capacity))
	if node, ok := np.Node("i-team-a"); ok {
		node.Namespace = "team-a"
	}
	networks, _ := ParseDirectTCPNetworks("127.0.0.0/8")
	shared, _ := ParseSharedServices("s3-system/s3-gateway")
	services := &fakeServices{synced: true}
	g := &TunnelGateway{workerNamespace: np.InstanceNamespace, serviceNetworks: networks, services: services, sharedServices: shared}
	dial := func() (net.Conn, error) { return g.dialFromWorker("i-team-a", "", "127.0.0.1", rawPort, echo) }
	expect := func(t *testing.T, reason string) {
		t.Helper()
		conn, err := dial()
		if reason == "" {
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			defer conn.Close()
			if _, err := conn.Write([]byte("ping")); err != nil {
				t.Fatal(err)
			}
			got := make([]byte, 4)
			if _, err := io.ReadFull(conn, got); err != nil || string(got) != "ping" {
				t.Fatalf("echo = %q, %v", got, err)
			}
			return
		}
		if !isRefusal(err, reason) {
			t.Fatalf("dial: %v, want refusal %s", err, reason)
		}
	}

	for _, tc := range []struct {
		name    string
		synced  bool
		service *ClusterService
		reason  string
	}{
		{"own namespace", true, &ClusterService{Namespace: "team-a", Name: "api", Ports: []int32{port}}, ""},
		{"shared service", true, &ClusterService{Namespace: "s3-system", Name: "s3-gateway", Ports: []int32{port}}, ""},
		{"another namespace", true, &ClusterService{Namespace: "team-b", Name: "api", Ports: []int32{port}}, "namespace_mismatch"},
		{"unshared service of a shared namespace", true, &ClusterService{Namespace: "s3-system", Name: "admin", Ports: []int32{port}}, "namespace_mismatch"},
		{"port the service does not expose", true, &ClusterService{Namespace: "team-a", Name: "api", Ports: []int32{1}}, "direct_forbidden"},
		{"no service", true, nil, "unknown_target"},
		{"before the watch syncs", false, &ClusterService{Namespace: "team-a", Name: "api", Ports: []int32{port}}, "unsynced"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			services.synced, services.byIP = tc.synced, map[string]ClusterService{}
			if tc.service != nil {
				services.byIP["127.0.0.1"] = *tc.service
			}
			expect(t, tc.reason)
		})
	}
}

func TestParseSharedServices(t *testing.T) {
	got, err := ParseSharedServices(" s3-system/s3-gateway, s3-system/s3-resolver ,")
	if err != nil {
		t.Fatal(err)
	}
	if want := map[string]bool{"s3-system/s3-gateway": true, "s3-system/s3-resolver": true}; !reflect.DeepEqual(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for _, bad := range []string{"s3-gateway", "/s3-gateway", "s3-system/", "a/b/c"} {
		if _, err := ParseSharedServices(bad); err == nil {
			t.Errorf("%q must be rejected", bad)
		}
	}
}

func TestServiceWatch_IndexesClusterIPsAndTCPPorts(t *testing.T) {
	client := fake.NewClientset(
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: "s3-system", Name: "s3-gateway"},
			Spec: corev1.ServiceSpec{ClusterIP: "10.100.0.10", ClusterIPs: []string{"10.100.0.10"}, Ports: []corev1.ServicePort{
				{Port: 80, Protocol: corev1.ProtocolTCP}, {Port: 53, Protocol: corev1.ProtocolUDP},
			}},
		},
		&corev1.Service{
			ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "headless"},
			Spec:       corev1.ServiceSpec{ClusterIP: corev1.ClusterIPNone, Ports: []corev1.ServicePort{{Port: 80}}},
		},
	)
	w := NewServiceWatch(client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx.Done())
	waitForWithin(t, 10*time.Second, "service watch sync", w.HasSynced)
	svc, ok := w.ServiceByIP("10.100.0.10")
	if want := (ClusterService{Namespace: "s3-system", Name: "s3-gateway", Ports: []int32{80}}); !ok || !reflect.DeepEqual(svc, want) {
		t.Fatalf("ServiceByIP = %+v, %v; want %+v", svc, ok, want)
	}
	if _, ok := w.ServiceByIP(corev1.ClusterIPNone); ok {
		t.Fatal("a headless service must hold no ClusterIP")
	}
}

func TestTunnelAgent_TunnelsRedirectedConnectionsToTheirOriginalDestination(t *testing.T) {
	out := runAgentPython(t, `
import asyncio, socket, struct
class Sock:
    dest = "172.20.0.10"
    def getsockopt(self, level, opt, size):
        assert (level, opt, size) == (socket.SOL_IP, agent.SO_ORIGINAL_DST, 16)
        return struct.pack("!HH4s8x", socket.AF_INET, 80, socket.inet_aton(Sock.dest))
class Writer:
    closed = False
    def get_extra_info(self, name):
        return Sock()
    def close(self):
        self.closed = True
class Stream:
    async def run(self):
        pass
class Session:
    def open_tcp_stream(self, target, reader, writer, pod):
        print("tunnel", target, repr(pod))
        return Stream()
async def main():
    proxy = agent.TCPProxy()
    writer = Writer()
    await proxy.handle_redirected(None, writer)
    print("closed without a session", writer.closed)
    proxy.session = Session()
    await proxy.handle_redirected(None, Writer())
    proxy.set_destinations({"profiles.example.com": "198.18.7.9"}, [])
    Sock.dest = "198.18.7.9"
    await proxy.handle_redirected(None, Writer())
    await asyncio.sleep(0)
asyncio.run(main())
`)
	if want := "[cloudvm-tunnel-agent] no tunnel session for the redirected connection to 172.20.0.10:80; closing it\n" +
		"closed without a session True\ntunnel 172.20.0.10:80 ''\ntunnel profiles.example.com:80 ''\n"; out != want {
		t.Fatalf("agent printed %q, want %q", out, want)
	}
}
