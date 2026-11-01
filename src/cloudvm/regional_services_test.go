// Tests regional stand-ins: the Service watch's stand-in index, DNS answers that send a region's workers to their
// stand-in or fall back to the real Service, and the tunnel reach a stand-in's own pods get to the Service it serves.
package main

import (
	"context"
	"net"
	"net/netip"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

type fakeRegionalServices struct {
	standIns map[string]ClusterService // namespace/name|region -> stand-in
}

func (f fakeRegionalServices) StandIn(namespace, name, region string) (ClusterService, bool) {
	svc, ok := f.standIns[namespace+"/"+name+"|"+region]
	return svc, ok
}

func (f fakeRegionalServices) StandsInFor(standInNamespace, namespace, name string) bool {
	for key, svc := range f.standIns {
		if target, _, _ := strings.Cut(key, "|"); svc.Namespace == standInNamespace && target == namespace+"/"+name {
			return true
		}
	}
	return false
}

func standInService(namespace, name, target, region string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Namespace: namespace, Name: name,
			Annotations: map[string]string{AnnotationRegionalServiceFor: target}, Labels: map[string]string{LabelRegion: region}},
		Spec: corev1.ServiceSpec{ClusterIP: corev1.ClusterIPNone},
	}
}

func TestParseRegionalNamespaces(t *testing.T) {
	got, err := ParseRegionalNamespaces(" regional , edge,")
	if err != nil || !reflect.DeepEqual(got, map[string]bool{"regional": true, "edge": true}) {
		t.Fatalf("got %v, %v", got, err)
	}
	if _, err := ParseRegionalNamespaces("Not_A_Namespace"); err == nil {
		t.Fatal("an invalid namespace must be rejected")
	}
}

func TestServiceWatch_IndexesStandInsOfRegionalNamespacesOnly(t *testing.T) {
	client := fake.NewClientset(
		standInService("regional", "cache-us-east-1", "team-a/cache", "us-east-1"),
		standInService("regional", "cache-eu-west-1", "team-a/cache", "eu-west-1"),
		standInService("regional", "no-region", "team-a/cache", ""),
		standInService("regional", "malformed", "team-a", "us-east-1"),
		standInService("team-b", "spoof", "team-a/cache", "ap-south-1"),
	)
	w := NewServiceWatch(client, map[string]bool{"regional": true})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	w.Start(ctx.Done())
	waitForWithin(t, 10*time.Second, "service watch sync", w.HasSynced)

	if got, ok := w.StandIn("team-a", "cache", "us-east-1"); !ok || got.Namespace != "regional" || got.Name != "cache-us-east-1" {
		t.Fatalf("StandIn(us-east-1) = %+v, %v", got, ok)
	}
	if _, ok := w.StandIn("team-a", "cache", "ap-south-1"); ok {
		t.Fatal("a stand-in outside the regional namespaces must be ignored")
	}
	if _, ok := w.StandIn("team-a", "other", "us-east-1"); ok {
		t.Fatal("a Service nothing stands in for has no stand-in")
	}
	if !w.StandsInFor("regional", "team-a", "cache") || w.StandsInFor("team-b", "team-a", "cache") || w.StandsInFor("regional", "team-a", "other") {
		t.Fatal("StandsInFor must hold only for the regional namespace's declared target")
	}
}

func TestTunnelDNS_StandInAnswersItsRegionsWorkersAndFallsBack(t *testing.T) {
	cluster := startFakeResolver(t, map[string][]netip.Addr{
		"cache.team-a.svc.cluster.local.":             {netip.MustParseAddr("10.96.0.10")},
		"cache-us-east-1.regional.svc.cluster.local.": {netip.MustParseAddr("10.20.0.5")},
		"cache-eu-west-1.regional.svc.cluster.local.": {},
	})
	np := NewNodePool(time.Minute)
	capacity, _ := NewResourceVector("4", "16Gi", 0, 0)
	for _, w := range []struct{ id, region, namespace string }{
		{"i-use1", "us-east-1", "team-a"}, {"i-euw1", "eu-west-1", "team-a"}, {"i-usw2", "us-west-2", "team-a"}, {"i-team-b", "us-east-1", "team-b"},
	} {
		np.RegisterNode(NewCloudVMNode(w.id, ProviderMock, w.region, "10.0.0.1", capacity))
		node, _ := np.Node(w.id)
		node.Namespace = w.namespace
	}
	s := &Server{nodePool: np}
	g := &TunnelGateway{
		clusterDomain: DefaultClusterDomain, clusterDNSServers: []string{cluster.addr},
		workerNamespace: np.InstanceNamespace, workerRegion: s.WorkerRegion,
		regionalServices: fakeRegionalServices{standIns: map[string]ClusterService{
			"team-a/cache|us-east-1": {Namespace: "regional", Name: "cache-us-east-1"},
			"team-a/cache|eu-west-1": {Namespace: "regional", Name: "cache-eu-west-1"},
			"team-b/cache|us-east-1": {Namespace: "regional", Name: "cache-us-east-1"},
		}},
	}
	for _, tc := range []struct {
		name, instance, query, want string
		ttl                         uint32
	}{
		{"a worker of the stand-in's region gets the stand-in", "i-use1", "cache.team-a.svc.cluster.local.", "10.20.0.5", standInTTL},
		{"a stand-in without endpoints falls back", "i-euw1", "cache.team-a.svc.cluster.local.", "10.96.0.10", 30},
		{"a region without a stand-in gets the Service", "i-usw2", "cache.team-a.svc.cluster.local.", "10.96.0.10", 30},
		{"a worker of another namespace gets the Service it asked for", "i-team-b", "cache.team-a.svc.cluster.local.", "10.96.0.10", 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reply, err := g.answerWorkerDNS(tc.instance, "", packDNSQuery(t, tc.query))
			if err != nil {
				t.Fatal(err)
			}
			var msg dnsmessage.Message
			if err := msg.Unpack(reply); err != nil {
				t.Fatal(err)
			}
			if got := answerAddrs(msg); len(got) != 1 || got[0] != tc.want || msg.ID != 0x5151 || msg.Questions[0].Name.String() != tc.query {
				t.Fatalf("reply %+v answers %v, want [%s] for %s", msg.Header, got, tc.want, tc.query)
			}
			if msg.Answers[0].Header.Name.String() != tc.query || msg.Answers[0].Header.TTL != tc.ttl {
				t.Fatalf("answer header = %+v, want name %s ttl %d", msg.Answers[0].Header, tc.query, tc.ttl)
			}
		})
	}
}

func TestServiceOfDNSName(t *testing.T) {
	for _, tc := range []struct{ fqdn, name, namespace string }{
		{"Cache.Team-A.svc.cluster.local.", "cache", "team-a"},
		{"cache.team-a.svc.cluster.local", "cache", "team-a"},
		{"pod.cache.team-a.svc.cluster.local.", "", ""},
		{"team-a.svc.cluster.local.", "", ""},
		{"cache.team-a.svc.other.local.", "", ""},
	} {
		name, namespace, ok := serviceOfDNSName(tc.fqdn, DefaultClusterDomain)
		if name != tc.name || namespace != tc.namespace || ok != (tc.name != "") {
			t.Errorf("serviceOfDNSName(%s) = %q, %q, %v", tc.fqdn, name, namespace, ok)
		}
	}
}

func TestTunnelTCP_StandInPodsReachTheServiceTheyStandInFor(t *testing.T) {
	echo := startEchoServer(t)
	_, rawPort, _ := net.SplitHostPort(echo)
	parsed, _ := strconv.ParseInt(rawPort, 10, 32)
	np := NewNodePool(time.Minute)
	capacity, _ := NewResourceVector("4", "16Gi", 0, 0)
	np.RegisterNode(NewCloudVMNode("i-regional", ProviderMock, "us-east-1", "10.0.0.1", capacity))
	node, _ := np.Node("i-regional")
	node.Namespace = "regional"
	networks, _ := ParseDirectTCPNetworks("127.0.0.0/8")
	services := &fakeServices{synced: true, byIP: map[string]ClusterService{}}
	g := &TunnelGateway{workerNamespace: np.InstanceNamespace, serviceNetworks: networks, services: services,
		regionalServices: fakeRegionalServices{standIns: map[string]ClusterService{"team-a/cache|us-east-1": {Namespace: "regional", Name: "cache-us-east-1"}}}}

	services.byIP["127.0.0.1"] = ClusterService{Namespace: "team-a", Name: "cache", Ports: []int32{int32(parsed)}}
	conn, err := g.dialFromWorker("i-regional", "", "127.0.0.1", rawPort, echo)
	if err != nil {
		t.Fatalf("the stand-in's pods must reach the Service they stand in for: %v", err)
	}
	_ = conn.Close()
	services.byIP["127.0.0.1"] = ClusterService{Namespace: "team-a", Name: "api", Ports: []int32{int32(parsed)}}
	if _, err := g.dialFromWorker("i-regional", "", "127.0.0.1", rawPort, echo); !isRefusal(err, "namespace_mismatch") {
		t.Fatalf("dial to another Service of the namespace: %v, want namespace_mismatch", err)
	}
}
