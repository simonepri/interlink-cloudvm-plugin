// Tests the per-namespace tunnel destinations: parsing, synthetic hostname addresses and redirects, and the gateway's
// per-pod authorization of worker streams to Services and external hosts.
package main

import (
	"context"
	"net"
	"net/netip"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
)

func TestParseNamespaceDestinations(t *testing.T) {
	d, err := ParseNamespaceDestinations(`{"profiler": ["monitoring/parca", "Profiles.Example.com:443", "203.0.113.7:4317"]}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if !d.AllowsService("profiler", "monitoring", "parca") || d.AllowsService("team-a", "monitoring", "parca") {
		t.Error("the Service must be reachable from its namespace only")
	}
	if !d.AllowsHost("profiler", "profiles.example.com", "443") || d.AllowsHost("profiler", "profiles.example.com", "80") {
		t.Error("the hostname must be reachable on its port only, case-insensitively")
	}
	if !d.AllowsHost("profiler", "203.0.113.7", "4317") || d.AllowsHost("team-a", "203.0.113.7", "4317") {
		t.Error("the address must be reachable from its namespace only")
	}
	if !d.HasServices() {
		t.Error("HasServices = false")
	}
	if got := d.ExternalAddresses(); !reflect.DeepEqual(got, []string{"203.0.113.7:4317"}) {
		t.Errorf("external addresses = %v", got)
	}
	empty, err := ParseNamespaceDestinations("")
	if err != nil || empty.HasServices() || empty.SyntheticHosts() != nil || empty.Redirects() != nil {
		t.Errorf("empty configuration = %+v, %v", empty, err)
	}

	for _, raw := range []string{
		`["monitoring/parca"]`,
		`{"Team_A": ["monitoring/parca"]}`,
		`{"team-a": ["monitoring/"]}`,
		`{"team-a": ["monitoring/parca/extra"]}`,
		`{"team-a": ["profiles.example.com"]}`,
		`{"team-a": ["profiles.example.com:0"]}`,
		`{"team-a": ["profiles.example.com:http"]}`,
		`{"team-a": ["profiles:443"]}`,
		`{"team-a": ["bad_host.example.com:443"]}`,
		`{"team-a": ["[2001:db8::1]:443"]}`,
		`{"team-a": ["198.18.0.9:443"]}`,
	} {
		if _, err := ParseNamespaceDestinations(raw); err == nil {
			t.Errorf("ParseNamespaceDestinations(%s) accepted an invalid configuration", raw)
		}
	}
}

func TestNamespaceDestinations_SyntheticHostsAndRedirects(t *testing.T) {
	d, err := ParseNamespaceDestinations(`{"a": ["one.example.com:443", "two.example.com:443", "203.0.113.7:4317"],` +
		` "b": ["one.example.com:8443", "three.example.org:80"]}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	hosts := d.SyntheticHosts()
	if len(hosts) != 3 {
		t.Fatalf("synthetic hosts = %v, want one per hostname", hosts)
	}
	seen := map[string]bool{}
	for name, raw := range hosts {
		addr := netip.MustParseAddr(raw)
		if !syntheticHostNetwork.Contains(addr) || addr == syntheticHostNetwork.Addr() || seen[raw] {
			t.Errorf("%s = %s, want a distinct host address inside %s", name, raw, syntheticHostNetwork)
		}
		seen[raw] = true
	}
	// Another namespace's hostname must not move the address of one.example.com.
	other, _ := ParseNamespaceDestinations(`{"c": ["one.example.com:443"]}`)
	if got := other.SyntheticHosts()["one.example.com"]; got != hosts["one.example.com"] {
		t.Errorf("one.example.com moved from %s to %s", hosts["one.example.com"], got)
	}
	want := []TunnelRedirect{{Network: "198.18.0.0/15"}, {Network: "203.0.113.7/32", Port: 4317}}
	if got := d.Redirects(); !reflect.DeepEqual(got, want) {
		t.Errorf("redirects = %+v, want %+v", got, want)
	}
}

func TestTunnelTCP_GatewayAuthorizesWorkerStreamsPerPod(t *testing.T) {
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
	const teamPod, profilerPod, foreignPod = "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222", "33333333-3333-3333-3333-333333333333"
	podsOnWorker := func(instanceID, pod string) (string, bool) {
		if instanceID != "i-team-a" {
			return "", false
		}
		namespace, ok := map[string]string{teamPod: "team-a", profilerPod: "profiler"}[pod]
		return namespace, ok
	}
	destinations, err := ParseNamespaceDestinations(`{"profiler": ["monitoring/parca", "profiles.example.com:443"]}`)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	networks, _ := ParseDirectTCPNetworks("127.0.0.0/8")
	shared, _ := ParseSharedServices("s3-system/s3-gateway")
	services := &fakeServices{synced: true}
	var dialedExternal []string
	g := &TunnelGateway{
		workerNamespace: np.InstanceNamespace, podOnWorker: podsOnWorker, destinations: destinations,
		serviceNetworks: networks, services: services, sharedServices: shared,
		externalDialer: func(target string) (net.Conn, error) {
			dialedExternal = append(dialedExternal, target)
			client, server := net.Pipe()
			_ = server.Close()
			return client, nil
		},
	}

	for _, tc := range []struct {
		name    string
		pod     string
		service *ClusterService
		host    string
		reason  string
	}{
		{"host process to its worker's namespace", "", &ClusterService{Namespace: "team-a", Name: "api"}, "", ""},
		{"host process to a per-namespace service of another namespace", "", &ClusterService{Namespace: "monitoring", Name: "parca"}, "", "namespace_mismatch"},
		{"team pod to its namespace", teamPod, &ClusterService{Namespace: "team-a", Name: "api"}, "", ""},
		{"node agent pod to its own namespace", profilerPod, &ClusterService{Namespace: "profiler", Name: "collector"}, "", ""},
		{"node agent pod to the worker's namespace", profilerPod, &ClusterService{Namespace: "team-a", Name: "api"}, "", "namespace_mismatch"},
		{"node agent pod to its per-namespace service", profilerPod, &ClusterService{Namespace: "monitoring", Name: "parca"}, "", ""},
		{"team pod to another namespace's per-namespace service", teamPod, &ClusterService{Namespace: "monitoring", Name: "parca"}, "", "namespace_mismatch"},
		{"node agent pod to a shared service", profilerPod, &ClusterService{Namespace: "s3-system", Name: "s3-gateway"}, "", ""},
		{"pod not on the worker", foreignPod, &ClusterService{Namespace: "team-a", Name: "api"}, "", "foreign_pod"},
		{"node agent pod to its external host", profilerPod, nil, "profiles.example.com:443", ""},
		{"node agent pod to its external host on another port", profilerPod, nil, "profiles.example.com:80", "direct_forbidden"},
		{"team pod to another namespace's external host", teamPod, nil, "profiles.example.com:443", "direct_forbidden"},
		{"host process to an external host", "", nil, "profiles.example.com:443", "direct_forbidden"},
		{"pod not on the worker to an external host", foreignPod, nil, "profiles.example.com:443", "foreign_pod"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dialedExternal = nil
			target := echo
			if tc.service != nil {
				tc.service.Ports = []int32{port}
				services.byIP = map[string]ClusterService{"127.0.0.1": *tc.service}
			} else {
				target = tc.host
			}
			host, targetPort, _ := net.SplitHostPort(target)
			conn, err := g.dialFromWorker("i-team-a", tc.pod, host, targetPort, target)
			if tc.reason != "" {
				if !isRefusal(err, tc.reason) {
					t.Fatalf("dial: %v, want refusal %s", err, tc.reason)
				}
				return
			}
			if err != nil {
				t.Fatalf("dial: %v", err)
			}
			_ = conn.Close()
			if tc.service == nil && !reflect.DeepEqual(dialedExternal, []string{tc.host}) {
				t.Errorf("gateway dialed %v, want %s", dialedExternal, tc.host)
			}
		})
	}

	t.Run("allow-listed address outside the service networks", func(t *testing.T) {
		literal, err := ParseNamespaceDestinations(`{"profiler": ["127.0.0.1:` + rawPort + `"]}`)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		direct := &TunnelGateway{workerNamespace: np.InstanceNamespace, podOnWorker: podsOnWorker, destinations: literal}
		conn, err := direct.dialFromWorker("i-team-a", profilerPod, "127.0.0.1", rawPort, echo)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		_ = conn.Close()
		if _, err := direct.dialFromWorker("i-team-a", teamPod, "127.0.0.1", rawPort, echo); !isRefusal(err, "direct_forbidden") {
			t.Fatalf("team pod dial: %v, want direct_forbidden", err)
		}
	})

	t.Run("without a pod resolver every stream acts for the worker's namespace", func(t *testing.T) {
		legacy := &TunnelGateway{workerNamespace: np.InstanceNamespace, serviceNetworks: networks, services: services}
		services.byIP = map[string]ClusterService{"127.0.0.1": {Namespace: "team-a", Name: "api", Ports: []int32{port}}}
		conn, err := legacy.dialFromWorker("i-team-a", foreignPod, "127.0.0.1", rawPort, echo)
		if err != nil {
			t.Fatalf("dial: %v", err)
		}
		_ = conn.Close()
	})
}

func TestTunnelGateway_StreamNamespaceFollowsThePod(t *testing.T) {
	g := &TunnelGateway{
		workerNamespace: func(string) (string, bool) { return "team-a", true },
		podOnWorker: func(_, pod string) (string, bool) {
			return "profiler", pod == "22222222-2222-2222-2222-222222222222"
		},
	}
	for _, tc := range []struct {
		pod     string
		want    string
		wantErr bool
	}{
		{"", "team-a", false},
		{"22222222-2222-2222-2222-222222222222", "profiler", false},
		{"33333333-3333-3333-3333-333333333333", "", true},
	} {
		got, err := g.streamNamespaceOf("i-1", tc.pod)
		if got != tc.want || (err != nil) != tc.wantErr {
			t.Errorf("streamNamespaceOf(%q) = %q, %v; want %q", tc.pod, got, err, tc.want)
		}
	}
}

func TestTunnelTCP_DaemonSetPodStreamActsForItsOwnNamespace(t *testing.T) {
	mock := NewMockProvider()
	s, client, url := newClaimServer(t, mock)
	claim, err := client.Create(context.Background(), claimRequest("nc-tunnel", "g5.12xlarge"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _, instanceID, _ := nodeclaim.ParseProviderID(claim.ProviderID)
	if code := createClaimPod(t, url, "nc-tunnel", "team-a", "w1", 1); code != 200 {
		t.Fatalf("workload pod: HTTP %d", code)
	}
	if code := postClaimPod(t, url, daemonSetPod("nc-tunnel", "monitoring", "exporter-1")); code != 200 {
		t.Fatalf("DaemonSet pod: HTTP %d", code)
	}

	echo := startEchoServer(t)
	_, rawPort, _ := net.SplitHostPort(echo)
	parsed, _ := strconv.ParseInt(rawPort, 10, 32)
	networks, _ := ParseDirectTCPNetworks("127.0.0.0/8")
	services := &fakeServices{synced: true, byIP: map[string]ClusterService{
		"127.0.0.1": {Namespace: "monitoring", Name: "collector", Ports: []int32{int32(parsed)}},
	}}
	g := &TunnelGateway{workerNamespace: s.WorkerNamespace, podOnWorker: s.PodNamespaceOnWorker,
		serviceNetworks: networks, services: services}

	conn, err := g.dialFromWorker(instanceID, "uid-exporter-1", "127.0.0.1", rawPort, echo)
	if err != nil {
		t.Fatalf("DaemonSet pod to its namespace's Service: %v", err)
	}
	_ = conn.Close()
	if _, err := g.dialFromWorker(instanceID, "uid-w1", "127.0.0.1", rawPort, echo); !isRefusal(err, "namespace_mismatch") {
		t.Fatalf("workload pod to another namespace's Service: %v, want namespace_mismatch", err)
	}
	if _, err := g.dialFromWorker(instanceID, "", "127.0.0.1", rawPort, echo); !isRefusal(err, "namespace_mismatch") {
		t.Fatalf("host process to another namespace's Service: %v, want namespace_mismatch", err)
	}
	if _, err := g.dialFromWorker(instanceID, "uid-elsewhere", "127.0.0.1", rawPort, echo); !isRefusal(err, "foreign_pod") {
		t.Fatalf("pod of another worker: %v, want foreign_pod", err)
	}
}
