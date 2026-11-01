// Tests the VM-per-NodeClaim API end to end through its HTTP client: launch, idempotency, pods on claim VMs and their
// release, deletion, recovery after a restart, capacity errors, readiness and inventory loss, the catalog, and zones.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/yamux"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
)

// newClaimServer returns a server on the mock provider with its claim store in a test directory, and a client.
func newClaimServer(t *testing.T, prov Provider) (*Server, *nodeclaim.Client, string) {
	t.Helper()
	path := t.TempDir() + "/claims.json"
	t.Setenv("CLAIM_STORE_PATH", path)
	s := NewServer(prov, "claims-"+t.Name())
	reviewer, _ := fakeTokenReviewer()
	s.SetClaimAuthenticator(newClaimAuthenticator(reviewer, nodeclaim.DefaultTokenAudience, []string{testCaller}))
	srv := httptest.NewServer(s.Routes())
	t.Cleanup(srv.Close)
	return s, nodeclaim.NewClient(srv.URL, &http.Client{Transport: bearer("good")}), srv.URL
}

func claimRequest(name, itype string) *nodeclaim.CreateRequest {
	return &nodeclaim.CreateRequest{
		Name: name, InstanceType: itype, Region: "us-east-1", Zone: "us-east-1a", CapacityType: nodeclaim.CapacityTypeSpot,
		Labels:      map[string]string{"gpu-class": "a10g"},
		Taints:      []nodeclaim.Taint{{Key: "virtual-node.interlink/provider", Value: "cloudvm", Effect: "NoSchedule"}},
		Annotations: map[string]string{"cloudvm.vk.io/ami-mode": "dlami", "cloudvm.vk.io/max-egress-rate": "$0.50", "other/key": "x"},
	}
}

// createClaimPod posts an interLink create request for a pod bound to node and returns the HTTP status.
func createClaimPod(t *testing.T, url, node, namespace, name string, gpus int) int {
	t.Helper()
	limits := map[string]string{"cpu": "1", "memory": "1Gi"}
	if gpus > 0 {
		limits["nvidia.com/gpu"] = fmt.Sprint(gpus)
	}
	pod := Pod{
		Metadata: PodMeta{Name: name, Namespace: namespace, UID: "uid-" + name},
		Spec: PodSpecWrapper{NodeName: node, Containers: []ContainerSpec{{
			Name: "main", Image: "busybox", Resources: ContainerResourceRequirements{Limits: limits},
		}}},
	}
	return postClaimPod(t, url, pod)
}

// postClaimPod posts an interLink create request for the pod and returns the HTTP status.
func postClaimPod(t *testing.T, url string, pod Pod) int {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"pod": pod})
	resp, err := http.Post(url+"/create", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("create pod: %v", err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

func TestClaimLaunchIsIdempotentAndReportsCapacity(t *testing.T) {
	mock := NewMockProvider()
	s, client, _ := newClaimServer(t, mock)
	ctx := context.Background()

	claim, err := client.Create(ctx, claimRequest("nc-a", "g5.xlarge"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if _, _, id, err := nodeclaim.ParseProviderID(claim.ProviderID); err != nil || id == "" {
		t.Fatalf("providerID %q: %v", claim.ProviderID, err)
	}
	if claim.State != nodeclaim.StateLaunching || claim.Capacity["nvidia.com/gpu"] != "1" || claim.Capacity["cpu"] != "4" || claim.Labels["gpu-class"] != "a10g" {
		t.Fatalf("claim = %+v", claim)
	}
	if got := claim.Capacity[nodeclaim.ResourceEphemeralStorage]; got != mockContainerStorage {
		t.Fatalf("ephemeral-storage = %q, want the container storage size %s", got, mockContainerStorage)
	}
	spec := mock.LastSpec()
	if !spec.NodeClaim || spec.Zone != "us-east-1a" || spec.InstanceType != "g5.xlarge" || spec.MarketType != "spot" || spec.AMIMode != "container" || spec.PodUID != "nodeclaim-nc-a" || spec.MaxEgressRate == "" {
		t.Fatalf("launch spec = %+v", spec)
	}

	again, err := client.Create(ctx, claimRequest("nc-a", "g5.xlarge"))
	if err != nil || again.ProviderID != claim.ProviderID || mock.LaunchCount() != 1 {
		t.Fatalf("repeated Create = %+v, %v after %d launches", again, err, mock.LaunchCount())
	}

	s.SyncClaims(ctx)
	got, err := client.GetByProviderID(ctx, claim.ProviderID)
	if err != nil || got.Name != "nc-a" || got.State != nodeclaim.StateRunning {
		t.Fatalf("GetByProviderID after sync = %+v, %v", got, err)
	}
	if items, err := client.List(ctx); err != nil || len(items) != 1 {
		t.Fatalf("List = %v, %v", items, err)
	}

	_, _, instanceID, _ := nodeclaim.ParseProviderID(claim.ProviderID)
	s.nodePool.Cordon(instanceID, SpinDownInterruption)
	if got, err := client.Get(ctx, "nc-a"); err != nil || !got.Interrupted {
		t.Fatalf("Get after a spot interruption = %+v, %v", got, err)
	}
}

func TestClaimKeepsTheProviderIDItsNodeWasCreatedWith(t *testing.T) {
	mock := NewMockProvider()
	s, client, _ := newClaimServer(t, mock)
	ctx := context.Background()
	claim, err := client.Create(ctx, claimRequest("nc-new", "g5.xlarge"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	cloud, region, instanceID, _ := nodeclaim.ParseProviderID(claim.ProviderID)
	if want := nodeclaim.FormatProviderID(cloud, region, instanceID); claim.ProviderID != want {
		t.Fatalf("new claim providerID = %q, want %q", claim.ProviderID, want)
	}

	// A claim persisted by an earlier release has no stored providerID; its Node and NodeClaim carry the legacy one.
	rec, _ := s.claims.store.get("nc-new")
	legacy := *rec
	legacy.ProviderID = ""
	_ = s.claims.store.set("nc-new", &legacy, false)
	want := nodeclaim.FormatLegacyProviderID(cloud, region, instanceID)
	if got, err := client.Get(ctx, "nc-new"); err != nil || got.ProviderID != want {
		t.Fatalf("legacy claim = %+v, %v, want providerID %q", got, err, want)
	}
	if got, err := client.GetByProviderID(ctx, want); err != nil || got.Name != "nc-new" {
		t.Fatalf("GetByProviderID(legacy) = %+v, %v", got, err)
	}
	if items, err := client.List(ctx); err != nil || len(items) != 1 || items[0].ProviderID != want {
		t.Fatalf("List = %+v, %v, want the legacy providerID", items, err)
	}
}

func TestClaimPodsShareTheClaimVM(t *testing.T) {
	mock := NewMockProvider()
	s, client, url := newClaimServer(t, mock)
	s.nodePool.SetIdleGracePeriod(0)
	ctx := context.Background()
	claim, err := client.Create(ctx, claimRequest("nc-b", "g5.12xlarge"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _, instanceID, _ := nodeclaim.ParseProviderID(claim.ProviderID)

	for _, name := range []string{"p1", "p2"} {
		if code := createClaimPod(t, url, "nc-b", "team-a", name, 2); code != http.StatusOK {
			t.Fatalf("create %s: HTTP %d", name, code)
		}
	}
	if mock.LaunchCount() != 1 {
		t.Fatalf("launches = %d, want only the claim's", mock.LaunchCount())
	}
	runner := s.runner.(*MockContainerRunner)
	g1, g2 := runner.GetStartRequest("uid-p1").AssignedGPUs, runner.GetStartRequest("uid-p2").AssignedGPUs
	if len(g1) != 2 || len(g2) != 2 || g1[0] == g2[0] || g1[0] == g2[1] {
		t.Fatalf("GPUs p1=%v p2=%v overlap", g1, g2)
	}
	if code := createClaimPod(t, url, "nc-b", "team-b", "p3", 0); code != http.StatusConflict {
		t.Fatalf("pod of a second namespace: HTTP %d, want 409", code)
	}

	// Deleting every pod leaves the claim VM running, and the idle collector keeps it.
	for _, name := range []string{"p1", "p2"} {
		body, _ := json.Marshal(map[string]any{"pod": Pod{Metadata: PodMeta{Name: name, Namespace: "team-a", UID: "uid-" + name}}})
		req, _ := http.NewRequest(http.MethodDelete, url+"/delete", bytes.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("delete %s: %v", name, err)
		}
		_ = resp.Body.Close()
	}
	s.CollectIdleNodes(ctx)
	if inst, _ := mock.Status(ctx, instanceID); inst.State != StateRunning || !s.nodePool.HasNode(instanceID) {
		t.Fatalf("claim VM after its pods left: state %s, pooled %v", inst.State, s.nodePool.HasNode(instanceID))
	}
}

// daemonSetPod returns a pod that a DaemonSet of namespace owns, bound to node.
func daemonSetPod(node, namespace, name string) Pod {
	return Pod{
		Metadata: PodMeta{Name: name, Namespace: namespace, UID: "uid-" + name,
			OwnerReferences: []OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: "node-proxy"}}},
		Spec: PodSpecWrapper{NodeName: node, Containers: []ContainerSpec{{Name: "proxy", Image: "proxy"}}},
	}
}

func TestClaimRunsDaemonSetPodsWithoutBindingTheNamespace(t *testing.T) {
	mock := NewMockProvider()
	s, client, url := newClaimServer(t, mock)
	claim, err := client.Create(context.Background(), claimRequest("nc-ds", "g5.12xlarge"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _, instanceID, _ := nodeclaim.ParseProviderID(claim.ProviderID)
	runner := s.runner.(*MockContainerRunner)

	// A DaemonSet pod that arrives before the workload runs, but leaves the VM unbound.
	if code := postClaimPod(t, url, daemonSetPod("nc-ds", "system", "proxy-1")); code != http.StatusOK {
		t.Fatalf("DaemonSet pod: HTTP %d, want 200", code)
	}
	if runner.GetStartRequest("uid-proxy-1") == nil {
		t.Fatal("the DaemonSet pod did not start on the claim VM")
	}
	if ns, ok := s.nodePool.InstanceNamespace(instanceID); ok {
		t.Fatalf("a DaemonSet pod bound the VM to namespace %q", ns)
	}

	if code := createClaimPod(t, url, "nc-ds", "team-a", "w1", 1); code != http.StatusOK {
		t.Fatalf("workload pod after the DaemonSet pod: HTTP %d, want 200", code)
	}
	if ns, _ := s.nodePool.InstanceNamespace(instanceID); ns != "team-a" {
		t.Fatalf("VM namespace = %q, want the workload's team-a", ns)
	}

	// DaemonSet pods of other namespaces share the bound VM and leave its namespace alone; workload pods do not.
	if code := postClaimPod(t, url, daemonSetPod("nc-ds", "monitoring", "exporter-1")); code != http.StatusOK {
		t.Fatalf("DaemonSet pod of another namespace: HTTP %d, want 200", code)
	}
	if ns, _ := s.nodePool.InstanceNamespace(instanceID); ns != "team-a" {
		t.Fatalf("VM namespace after a DaemonSet pod = %q, want team-a", ns)
	}
	if code := createClaimPod(t, url, "nc-ds", "team-b", "w2", 0); code != http.StatusConflict {
		t.Fatalf("workload pod of a second namespace: HTTP %d, want 409", code)
	}
	if ns, ok := s.PodNamespaceOnWorker(instanceID, "uid-exporter-1"); !ok || ns != "monitoring" {
		t.Fatalf("PodNamespaceOnWorker(exporter) = %q, %v, want monitoring", ns, ok)
	}

	// The claim record keeps which pods are DaemonSet pods, so a restart does not let one bind the VM.
	rec, _ := s.claims.store.get("nc-ds")
	if !rec.Pods["uid-proxy-1"].DaemonSet || rec.Pods["uid-w1"].DaemonSet || rec.Namespace != "team-a" {
		t.Fatalf("claim record pods = %+v, namespace %q", rec.Pods, rec.Namespace)
	}
	restarted := NewServer(mock, s.clusterTag)
	restarted.RecoverClaims()
	restarted.nodePool.Release("uid-w1")
	if ns, _ := restarted.nodePool.InstanceNamespace(instanceID); ns != "team-a" {
		t.Fatalf("recovered VM namespace = %q, want team-a", ns)
	}
	node, _ := restarted.nodePool.Node(instanceID)
	if !node.ActivePods["uid-exporter-1"].DaemonSet {
		t.Fatalf("recovered DaemonSet pod = %+v", node.ActivePods["uid-exporter-1"])
	}
}

func TestClaimDeleteTerminatesAndRefusesLaterPods(t *testing.T) {
	mock := NewMockProvider()
	s, client, url := newClaimServer(t, mock)
	ctx := context.Background()
	claim, err := client.Create(ctx, claimRequest("nc-c", "g5.xlarge"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _, instanceID, _ := nodeclaim.ParseProviderID(claim.ProviderID)

	if err := client.Delete(ctx, "nc-c"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if inst, _ := mock.Status(ctx, instanceID); inst.State != StateTerminated {
		t.Fatalf("VM state after Delete = %s", inst.State)
	}
	if err := client.Delete(ctx, "nc-c"); !nodeclaim.IsNotFound(err) {
		t.Fatalf("second Delete error = %v, want not found", err)
	}
	if _, err := client.Get(ctx, "nc-c"); !nodeclaim.IsNotFound(err) {
		t.Fatalf("Get after Delete error = %v, want not found", err)
	}
	if code := createClaimPod(t, url, "nc-c", "team-a", "late", 0); code != http.StatusConflict || mock.LaunchCount() != 1 {
		t.Fatalf("pod of a deleted claim: HTTP %d after %d launches, want 409 and no launch", code, mock.LaunchCount())
	}
	if s.IsClaimInstance(instanceID) {
		t.Fatal("deleted claim's VM still counts as a claim VM")
	}
}

func TestClaimRecoveryKeepsPodDevices(t *testing.T) {
	mock := NewMockProvider()
	s, client, url := newClaimServer(t, mock)
	ctx := context.Background()
	claim, err := client.Create(ctx, claimRequest("nc-d", "g5.12xlarge"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _, instanceID, _ := nodeclaim.ParseProviderID(claim.ProviderID)
	if code := createClaimPod(t, url, "nc-d", "team-a", "p1", 2); code != http.StatusOK {
		t.Fatalf("create pod: HTTP %d", code)
	}
	first := s.runner.(*MockContainerRunner).GetStartRequest("uid-p1").AssignedGPUs

	restarted := NewServer(mock, s.clusterTag)
	restarted.RecoverClaims()
	if !restarted.IsClaimInstance(instanceID) {
		t.Fatal("restarted server lost the claim")
	}
	node, ok := restarted.nodePool.Node(instanceID)
	if !ok || node.ClaimName != "nc-d" || node.Namespace != "team-a" {
		t.Fatalf("recovered node = %+v", node)
	}
	srv := httptest.NewServer(restarted.Routes())
	defer srv.Close()
	if code := createClaimPod(t, srv.URL, "nc-d", "team-a", "p2", 2); code != http.StatusOK {
		t.Fatalf("create pod after restart: HTTP %d", code)
	}
	second := restarted.runner.(*MockContainerRunner).GetStartRequest("uid-p2").AssignedGPUs
	for _, g := range second {
		for _, f := range first {
			if g == f {
				t.Fatalf("pod after restart got GPU %d of the recovered pod (%v vs %v)", g, first, second)
			}
		}
	}
	if err := restarted.RecoverActiveInstances(ctx, mock); err != nil {
		t.Fatalf("RecoverActiveInstances: %v", err)
	}
	if n, _ := restarted.nodePool.Node(instanceID); n.Cordoned {
		t.Fatal("inventory recovery adopted the claim VM as a pod VM")
	}
}

// capacityProvider fails every launch for lack of capacity, or with err when set.
type capacityProvider struct {
	*MockProvider
	launches int
	err      error
}

func (p *capacityProvider) Launch(context.Context, *InstanceSpec) (*Instance, error) {
	p.launches++
	if p.err != nil {
		return nil, p.err
	}
	return nil, fmt.Errorf("launch fleet failed: %w", errNoInstancesLaunched)
}

func TestClaimCapacityErrorMarksOfferingUnavailable(t *testing.T) {
	prov := &capacityProvider{MockProvider: NewMockProvider()}
	_, client, _ := newClaimServer(t, prov)
	ctx := context.Background()
	for range 2 {
		if _, err := client.Create(ctx, claimRequest("nc-e", "g5.xlarge")); !nodeclaim.IsInsufficientCapacity(err) || nodeclaim.IsQuotaExceeded(err) {
			t.Fatalf("Create error = %v, want insufficient capacity without a quota cause", err)
		}
	}
	if prov.launches != 1 {
		t.Fatalf("launches = %d, want the second create answered from the unavailable offering", prov.launches)
	}
	if _, err := client.Create(ctx, &nodeclaim.CreateRequest{Name: "x"}); err == nil || nodeclaim.IsInsufficientCapacity(err) {
		t.Fatalf("invalid Create error = %v", err)
	}
}

func TestClaimQuotaRefusalCarriesTheQuotaCause(t *testing.T) {
	prov := &capacityProvider{MockProvider: NewMockProvider(), err: errors.New("operation error EC2: CreateFleet, VcpuLimitExceeded: You have requested more vCPU capacity than your current vCPU limit allows")}
	_, client, _ := newClaimServer(t, prov)
	if _, err := client.Create(context.Background(), claimRequest("nc-q", "g5.xlarge")); !nodeclaim.IsQuotaExceeded(err) {
		t.Fatalf("Create error = %v, want insufficient capacity with the quota cause", err)
	}
}

func TestClaimWhoseUserDataIsTooLargeIsRefusedWithItsReason(t *testing.T) {
	prov := &capacityProvider{MockProvider: NewMockProvider(), err: fmt.Errorf("%w: the boot script of nc-u compresses to 17000 bytes", errUserDataTooLarge)}
	_, client, _ := newClaimServer(t, prov)
	_, err := client.Create(context.Background(), claimRequest("nc-u", "g5.xlarge"))
	if !nodeclaim.IsUserDataTooLarge(err) || nodeclaim.IsInsufficientCapacity(err) || !strings.Contains(err.Error(), "17000 bytes") {
		t.Fatalf("Create error = %v, want a UserDataTooLarge refusal with the size", err)
	}
}

// downRunner never answers.
type downRunner struct{ *MockContainerRunner }

func (downRunner) GetHostStats(context.Context, *CloudVMNode) (*HostStatsResult, error) {
	return nil, errors.New("connection refused")
}

func TestClaimSyncTerminatesVMsThatNeverBecomeReady(t *testing.T) {
	mock := NewMockProvider()
	s, client, _ := newClaimServer(t, mock)
	s.SetContainerRunner(downRunner{NewMockContainerRunner()})
	ctx := context.Background()
	claim, err := client.Create(ctx, claimRequest("nc-f", "g5.xlarge"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _, instanceID, _ := nodeclaim.ParseProviderID(claim.ProviderID)
	s.SyncClaims(ctx)
	if got, _ := client.Get(ctx, "nc-f"); got == nil || got.State != nodeclaim.StateLaunching {
		t.Fatalf("young unready claim = %+v", got)
	}

	rec, _ := s.claims.store.get("nc-f")
	old := *rec
	old.CreatedAt = time.Now().Add(-claimReadyTimeout - time.Minute)
	_ = s.claims.store.set("nc-f", &old, false)
	s.SyncClaims(ctx)
	if inst, _ := mock.Status(ctx, instanceID); inst.State != StateTerminated {
		t.Fatalf("unready VM state = %s, want terminated", inst.State)
	}
	if _, err := client.Get(ctx, "nc-f"); !nodeclaim.IsNotFound(err) {
		t.Fatalf("Get error = %v, want not found", err)
	}
}

func TestClaimInventoryLossForgetsClaim(t *testing.T) {
	mock := NewMockProvider()
	s, client, _ := newClaimServer(t, mock)
	ctx := context.Background()
	if _, err := client.Create(ctx, claimRequest("nc-g", "g5.xlarge")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	rec, _ := s.claims.store.get("nc-g")
	old := *rec
	old.CreatedAt = time.Now().Add(-time.Hour)
	_ = s.claims.store.set("nc-g", &old, false)

	s.ObserveInventory(map[string]*Instance{}, map[string]error{"us-east-1": errors.New("throttled")})
	if _, err := client.Get(ctx, "nc-g"); err != nil {
		t.Fatalf("claim forgotten although its region's inventory failed: %v", err)
	}
	s.ObserveInventory(map[string]*Instance{}, nil)
	if _, err := client.Get(ctx, "nc-g"); !nodeclaim.IsNotFound(err) {
		t.Fatalf("Get error = %v, want not found after the VM vanished", err)
	}
}

func TestReconcilerLeavesIdleClaimVMs(t *testing.T) {
	mock := NewMockProvider()
	s, client, _ := newClaimServer(t, mock)
	ctx := context.Background()
	claim, err := client.Create(ctx, claimRequest("nc-h", "g5.xlarge"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _, instanceID, _ := nodeclaim.ParseProviderID(claim.ProviderID)
	r := NewReconciler(mock, s, mock, s.clusterTag, 0)
	if _, err := r.ReconcileOnce(ctx); err != nil {
		t.Fatalf("ReconcileOnce: %v", err)
	}
	if inst, _ := mock.Status(ctx, instanceID); inst.State != StateRunning {
		t.Fatalf("reconciler terminated the idle claim VM: %s", inst.State)
	}
}

func TestClaimReadsContainerStorageAgainAfterAFailedRead(t *testing.T) {
	mock := NewMockProvider()
	mock.containerStorageErr = errors.New("throttled")
	mock.plannedStorageErr = errors.New("throttled")
	s, client, _ := newClaimServer(t, mock)
	ctx := context.Background()

	claim, err := client.Create(ctx, claimRequest("nc-r", "g5.xlarge"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got, ok := claim.Capacity[nodeclaim.ResourceEphemeralStorage]; ok {
		t.Fatalf("ephemeral-storage = %q before the container storage size was read", got)
	}
	mock.mu.Lock()
	mock.containerStorageErr = nil
	mock.mu.Unlock()
	s.SyncClaims(ctx)
	got, err := client.Get(ctx, "nc-r")
	if err != nil || got.Capacity[nodeclaim.ResourceEphemeralStorage] != mockContainerStorage {
		t.Fatalf("Get after sync = %+v, %v; want ephemeral-storage %s", got, err, mockContainerStorage)
	}
}

func TestClaimReportsThePlannedDiskWhenTheInstanceCannotBeRead(t *testing.T) {
	mock := NewMockProvider()
	mock.containerStorageErr = errors.New("InvalidInstanceID.NotFound")
	_, client, _ := newClaimServer(t, mock)

	claim, err := client.Create(context.Background(), claimRequest("nc-p", "g5.xlarge"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if got := claim.Capacity[nodeclaim.ResourceEphemeralStorage]; got != mockContainerStorage {
		t.Fatalf("ephemeral-storage at launch = %q, want the planned %s", got, mockContainerStorage)
	}
}

// TestClaimReportsEveryCatalogResource checks that a claim launched for any catalog type reports every resource the
// shared Hardware contract renders for that type, which is what the Karpenter provider advertises for it.
func TestClaimReportsEveryCatalogResource(t *testing.T) {
	s, client, _ := newClaimServer(t, NewMockProvider())
	ctx := context.Background()
	ticker := NewSpotPriceTicker([]string{"us-east-1"}, []string{"g5.xlarge", "g5.12xlarge"}, time.Hour)
	s.SetSpotTicker(ticker)
	cat, err := client.Catalog(ctx, []string{"us-east-1"})
	if err != nil {
		t.Fatalf("Catalog: %v", err)
	}
	if len(cat.InstanceTypes) == 0 {
		t.Fatal("catalog is empty")
	}
	for i, it := range cat.InstanceTypes {
		want := nodeclaim.Hardware{CPU: it.CPU, Memory: it.Memory, EphemeralStorage: "1", GPUs: it.GPUCount, Pods: 1}.Capacity()
		claim, err := client.Create(ctx, claimRequest(fmt.Sprintf("nc-cat-%d", i), it.Name))
		if err != nil {
			t.Fatalf("Create %s: %v", it.Name, err)
		}
		for name := range want {
			if claim.Capacity[name] == "" {
				t.Errorf("claim for %s lacks %s: capacity %v", it.Name, name, claim.Capacity)
			}
		}
	}
}

func TestClaimCatalogOfferings(t *testing.T) {
	s, client, _ := newClaimServer(t, NewMockProvider())
	ctx := context.Background()
	if _, err := client.Catalog(ctx, nil); err == nil {
		t.Fatal("Catalog without a ticker succeeded")
	}
	ticker := NewSpotPriceTicker([]string{"us-east-1"}, []string{"g5.xlarge"}, time.Hour)
	ticker.prices[tickerKey("us-east-1", "us-east-1a", "g5.xlarge")] = &SpotPriceEntry{Region: "us-east-1", AvailabilityZone: "us-east-1a", InstanceType: "g5.xlarge", SpotPriceUSD: 0.4}
	s.SetSpotTicker(ticker)
	s.claims.unavailable[offeringKey("g5.xlarge", "us-east-1", "us-east-1a", nodeclaim.CapacityTypeSpot)] = time.Now().Add(time.Minute)

	cat, err := client.Catalog(ctx, []string{"us-east-1"})
	if err != nil {
		t.Fatalf("Catalog: %v", err)
	}
	var g5 *nodeclaim.InstanceType
	for i := range cat.InstanceTypes {
		if cat.InstanceTypes[i].Name == "g5.xlarge" {
			g5 = &cat.InstanceTypes[i]
		}
	}
	if g5 == nil || g5.GPUCount != 1 || g5.CPU != "4" || g5.Family != "g5" || g5.Size != "xlarge" {
		t.Fatalf("g5.xlarge = %+v", g5)
	}
	if g5.EphemeralStorage != mockContainerStorage {
		t.Fatalf("g5.xlarge ephemeral storage = %q, want the planned %s", g5.EphemeralStorage, mockContainerStorage)
	}
	var spot, onDemand bool
	for _, o := range g5.Offerings {
		switch {
		case o.CapacityType == nodeclaim.CapacityTypeSpot && o.Zone == "us-east-1a":
			spot = o.PricePerHour == 0.4 && !o.Available
		case o.CapacityType == nodeclaim.CapacityTypeOnDemand:
			onDemand = o.PricePerHour > 0 && o.Available
		}
	}
	if !spot || !onDemand {
		t.Fatalf("offerings = %+v", g5.Offerings)
	}
}

func TestOfferedFleetOverridesPinZone(t *testing.T) {
	p := newTestProvider(t)
	region := &regionMock{
		subnetAZ: map[string]string{"subnet-a": "us-west-1a", "subnet-b": "us-west-1b"},
		offered:  map[string][]string{"g5.xlarge": {"us-west-1a", "us-west-1b"}},
	}
	ovs, err := p.offeredFleetOverrides(context.Background(), region.client(), "us-west-1", "us-west-1b", []string{"g5.xlarge"}, []string{"subnet-a", "subnet-b"})
	if err != nil || len(ovs) != 1 || *ovs[0].SubnetId != "subnet-b" {
		t.Fatalf("overrides = %+v, %v", ovs, err)
	}
	if _, err := p.offeredFleetOverrides(context.Background(), region.client(), "us-west-1", "us-west-1c", []string{"g5.xlarge"}, []string{"subnet-a"}); !errors.Is(err, errNoOfferedPlacements) {
		t.Fatalf("zone without subnets error = %v", err)
	}
}

// claimPodWatch is a synced pod watch that lists the pod UIDs in its set.
type claimPodWatch struct {
	mu   sync.Mutex
	uids map[string]bool
}

func (w *claimPodWatch) HasSynced() bool { return true }

func (w *claimPodWatch) PodExists(uid string) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.uids[uid]
}

func (w *claimPodWatch) set(uids ...string) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.uids = make(map[string]bool, len(uids))
	for _, uid := range uids {
		w.uids[uid] = true
	}
}

// deleteClaimPod posts an interLink delete request for the pod, as the node agent does when the pod leaves its Node.
func deleteClaimPod(t *testing.T, url, namespace, name string) {
	t.Helper()
	body, _ := json.Marshal(map[string]any{"pod": Pod{Metadata: PodMeta{Name: name, Namespace: namespace, UID: "uid-" + name}}})
	req, _ := http.NewRequest(http.MethodDelete, url+"/delete", bytes.NewReader(body))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("delete %s: %v", name, err)
	}
	_ = resp.Body.Close()
}

// agePlacement backdates when a pod was placed on the claim VM, past the pod watch's lag.
func agePlacement(s *Server, instanceID, uid string) {
	node, _ := s.nodePool.Node(instanceID)
	s.nodePool.mu.Lock()
	defer s.nodePool.mu.Unlock()
	node.ActivePods[uid].StartedAt = time.Now().Add(-2 * claimPodWatchLag)
}

func claimPodUIDs(t *testing.T, client *nodeclaim.Client, name string) []string {
	t.Helper()
	claim, err := client.Get(context.Background(), name)
	if err != nil {
		t.Fatalf("Get %s: %v", name, err)
	}
	uids := make([]string, 0, len(claim.Pods))
	for _, p := range claim.Pods {
		uids = append(uids, p.UID)
	}
	return uids
}

func TestClaimPodDeleteFreesItsDevices(t *testing.T) {
	s, client, url := newClaimServer(t, NewMockProvider())
	if _, err := client.Create(context.Background(), claimRequest("nc-free", "g5.xlarge")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if code := createClaimPod(t, url, "nc-free", "team-a", "p1", 1); code != http.StatusOK {
		t.Fatalf("create p1: HTTP %d", code)
	}
	if got := claimPodUIDs(t, client, "nc-free"); !slices.Equal(got, []string{"uid-p1"}) {
		t.Fatalf("claim pods = %v, want uid-p1", got)
	}
	// p1 still exists, so its GPU stays taken however long it has run.
	watch := &claimPodWatch{}
	watch.set("uid-p1")
	s.SetPodExistence(watch)
	_, _, instanceID, _ := nodeclaim.ParseProviderID(mustGet(t, client, "nc-free").ProviderID)
	agePlacement(s, instanceID, "uid-p1")
	if code := createClaimPod(t, url, "nc-free", "team-a", "p2", 1); code != http.StatusConflict {
		t.Fatalf("create p2 while p1 holds the only GPU: HTTP %d, want 409", code)
	}

	deleteClaimPod(t, url, "team-a", "p1")
	if code := createClaimPod(t, url, "nc-free", "team-a", "p2", 1); code != http.StatusOK {
		t.Fatalf("create p2 after p1 was deleted: HTTP %d, want 200", code)
	}
	if got := claimPodUIDs(t, client, "nc-free"); !slices.Equal(got, []string{"uid-p2"}) {
		t.Fatalf("claim pods = %v, want uid-p2", got)
	}
}

func TestClaimReleasesPodsDeletedWithoutDeleteCall(t *testing.T) {
	s, client, url := newClaimServer(t, NewMockProvider())
	ctx := context.Background()
	if _, err := client.Create(ctx, claimRequest("nc-stale", "g5.xlarge")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _, instanceID, _ := nodeclaim.ParseProviderID(mustGet(t, client, "nc-stale").ProviderID)
	if code := createClaimPod(t, url, "nc-stale", "team-a", "p1", 1); code != http.StatusOK {
		t.Fatalf("create p1: HTTP %d", code)
	}

	// p1 was deleted while the node agent was down, so the plugin never got its /delete; the next pod the scheduler
	// binds to the Node, which it sees empty, takes the GPU.
	watch := &claimPodWatch{}
	watch.set("uid-p2")
	s.SetPodExistence(watch)
	agePlacement(s, instanceID, "uid-p1")
	if code := createClaimPod(t, url, "nc-stale", "team-a", "p2", 1); code != http.StatusOK {
		t.Fatalf("create p2 on the Node p1 left: HTTP %d, want 200", code)
	}
	if st, _ := s.runner.GetStatus(ctx, nil, "uid-p1"); st.Running {
		t.Fatal("the container of the deleted pod p1 still runs")
	}
	if got := claimPodUIDs(t, client, "nc-stale"); !slices.Equal(got, []string{"uid-p2"}) {
		t.Fatalf("claim pods = %v, want uid-p2", got)
	}

	// The claim loop releases a departed pod without waiting for a create, but keeps one placed within the watch's lag.
	watch.set()
	s.SyncClaims(ctx)
	if got := claimPodUIDs(t, client, "nc-stale"); !slices.Equal(got, []string{"uid-p2"}) {
		t.Fatalf("claim pods after a sync within the watch lag = %v, want uid-p2", got)
	}
	agePlacement(s, instanceID, "uid-p2")
	s.SyncClaims(ctx)
	if got := claimPodUIDs(t, client, "nc-stale"); len(got) != 0 {
		t.Fatalf("claim pods after the sync = %v, want none", got)
	}
	if rec, _ := s.claims.store.get("nc-stale"); len(rec.Pods) != 0 {
		t.Fatalf("persisted claim pods = %v, want none", rec.Pods)
	}
}

func mustGet(t *testing.T, client *nodeclaim.Client, name string) *nodeclaim.NodeClaim {
	t.Helper()
	claim, err := client.Get(context.Background(), name)
	if err != nil {
		t.Fatalf("Get %s: %v", name, err)
	}
	return claim
}

// pipeSession returns the gateway end of a Yamux session over an in-memory pipe.
func pipeSession(t *testing.T) *yamux.Session {
	t.Helper()
	server, client := net.Pipe()
	t.Cleanup(func() { _ = server.Close(); _ = client.Close() })
	go func() { _, _ = yamux.Client(client, DefaultYamuxConfig()) }()
	session, err := yamux.Server(server, DefaultYamuxConfig())
	if err != nil {
		t.Fatalf("yamux server: %v", err)
	}
	return session
}

func TestClaimStaysReadyWhileItsTunnelReconnects(t *testing.T) {
	mock := NewMockProvider()
	s, client, _ := newClaimServer(t, mock)
	registry := NewSessionRegistry()
	registry.started = time.Now().Add(-time.Hour)
	const connectTimeout, grace = 10 * time.Minute, 2 * time.Minute
	s.EnableTunnel(TunnelSettings{
		Registry: registry, Endpoint: "tunnel.example.internal:443", MeshAuthKey: "tskey-auth-test",
		ConnectTimeout: connectTimeout, ReconnectGrace: grace,
	})
	ctx := context.Background()
	claim, err := client.Create(ctx, claimRequest("nc-r", "g5.xlarge"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _, instanceID, _ := nodeclaim.ParseProviderID(claim.ProviderID)
	rec, _ := s.claims.store.get("nc-r")
	old := *rec
	old.CreatedAt = time.Now().Add(-time.Hour)
	_ = s.claims.store.set("nc-r", &old, false)
	state := func() nodeclaim.State {
		got, err := client.Get(ctx, "nc-r")
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		return got.State
	}
	backdateLoss := func(by time.Duration) {
		registry.mu.Lock()
		registry.lost[instanceID] = time.Now().Add(-by)
		registry.mu.Unlock()
	}

	registry.Register(instanceID, pipeSession(t))
	if got := state(); got != nodeclaim.StateRunning {
		t.Fatalf("connected claim state = %s, want Running", got)
	}

	_ = registry.Close(instanceID)
	s.SyncClaims(ctx)
	if got := state(); got != nodeclaim.StateRunning {
		t.Fatalf("claim state right after its tunnel dropped = %s, want Running", got)
	}

	backdateLoss(grace + time.Second)
	s.SyncClaims(ctx)
	if got := state(); got != nodeclaim.StateLaunching {
		t.Fatalf("claim state after the reconnect grace = %s, want Launching", got)
	}
	if inst, _ := mock.Status(ctx, instanceID); inst.State == StateTerminated {
		t.Fatal("a VM of an hour-old claim was terminated minutes after its tunnel dropped")
	}

	registry.Register(instanceID, pipeSession(t))
	if got := state(); got != nodeclaim.StateRunning {
		t.Fatalf("reconnected claim state = %s, want Running", got)
	}

	_ = registry.Close(instanceID)
	backdateLoss(connectTimeout + time.Second)
	s.SyncClaims(ctx)
	if inst, _ := mock.Status(ctx, instanceID); inst.State != StateTerminated {
		t.Fatalf("VM without a tunnel for longer than the connect timeout: state = %s, want terminated", inst.State)
	}
}

func TestClaimSyncGivesVMsTheConnectTimeoutAfterAPluginRestart(t *testing.T) {
	mock := NewMockProvider()
	s, client, _ := newClaimServer(t, mock)
	s.EnableTunnel(TunnelSettings{Endpoint: "tunnel.example.internal:443", MeshAuthKey: "tskey-auth-test"})
	ctx := context.Background()
	claim, err := client.Create(ctx, claimRequest("nc-s", "g5.xlarge"))
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	_, _, instanceID, _ := nodeclaim.ParseProviderID(claim.ProviderID)
	rec, _ := s.claims.store.get("nc-s")
	old := *rec
	old.CreatedAt = time.Now().Add(-time.Hour)
	_ = s.claims.store.set("nc-s", &old, false)

	s.SyncClaims(ctx)
	if inst, _ := mock.Status(ctx, instanceID); inst.State == StateTerminated {
		t.Fatal("an hour-old VM was terminated before it could reconnect to a restarted plugin")
	}
}

// flakyStopRunner fails the first failures stop requests of the mock runner.
type flakyStopRunner struct {
	*MockContainerRunner
	failures int
	calls    int
}

func (f *flakyStopRunner) StopContainer(ctx context.Context, node *CloudVMNode, uid string) error {
	f.calls++
	if f.calls <= f.failures {
		return fmt.Errorf("stop timed out")
	}
	return f.MockContainerRunner.StopContainer(ctx, node, uid)
}

func TestClaimPodDeleteRetriesSlowContainerStop(t *testing.T) {
	s, client, url := newClaimServer(t, NewMockProvider())
	runner := &flakyStopRunner{MockContainerRunner: NewMockContainerRunner(), failures: 2}
	s.SetContainerRunner(runner)
	s.stopRetryDelay = time.Millisecond
	if _, err := client.Create(context.Background(), claimRequest("nc-slow", "g5.xlarge")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if code := createClaimPod(t, url, "nc-slow", "team-a", "p1", 0); code != http.StatusOK {
		t.Fatalf("create pod: HTTP %d", code)
	}
	deleteClaimPod(t, url, "team-a", "p1")
	if got := mustGet(t, client, "nc-slow"); got.State == nodeclaim.StateUnhealthy {
		t.Fatalf("claim state = %s after a stop that succeeded on retry", got.State)
	}
	if runner.calls != 3 {
		t.Fatalf("stop attempts = %d, want 3", runner.calls)
	}
}

func TestClaimReportsUnhealthyWhenContainerCannotBeRemoved(t *testing.T) {
	s, client, url := newClaimServer(t, NewMockProvider())
	runner := &flakyStopRunner{MockContainerRunner: NewMockContainerRunner(), failures: 1 << 30}
	s.SetContainerRunner(runner)
	s.stopRetryDelay = time.Millisecond
	if _, err := client.Create(context.Background(), claimRequest("nc-stuck", "g5.xlarge")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	if code := createClaimPod(t, url, "nc-stuck", "team-a", "p1", 0); code != http.StatusOK {
		t.Fatalf("create pod: HTTP %d", code)
	}
	deleteClaimPod(t, url, "team-a", "p1")
	if got := mustGet(t, client, "nc-stuck"); got.State != nodeclaim.StateUnhealthy {
		t.Fatalf("claim state = %s, want %s so the node agent makes the Node NotReady", got.State, nodeclaim.StateUnhealthy)
	}
}

func TestRegionalVMsRunOnlyRegionalNamespaces(t *testing.T) {
	t.Setenv("REGIONAL_NAMESPACES", "regional")
	mock := NewMockProvider()
	s, client, url := newClaimServer(t, mock)
	ctx := context.Background()
	regional := claimRequest("nc-regional", "m7i.large")
	regional.Taints = append(regional.Taints, nodeclaim.Taint{Key: TaintRegional, Value: "true", Effect: "NoSchedule"})
	if _, err := client.Create(ctx, regional); err != nil {
		t.Fatalf("Create regional: %v", err)
	}
	if _, err := client.Create(ctx, claimRequest("nc-team", "m7i.large")); err != nil {
		t.Fatalf("Create team: %v", err)
	}

	for _, tc := range []struct {
		name, node, namespace string
		want                  int
	}{
		{"a team pod on a regional VM", "nc-regional", "team-a", http.StatusConflict},
		{"a regional pod on a team VM", "nc-team", "regional", http.StatusConflict},
		{"a regional pod on a regional VM", "nc-regional", "regional", http.StatusOK},
		{"a team pod on a team VM", "nc-team", "team-a", http.StatusOK},
	} {
		if code := createClaimPod(t, url, tc.node, tc.namespace, "p-"+tc.node+"-"+tc.namespace, 0); code != tc.want {
			t.Errorf("%s: HTTP %d, want %d", tc.name, code, tc.want)
		}
	}
	if runner := s.runner.(*MockContainerRunner); runner.GetStartRequest("uid-p-nc-regional-team-a") != nil {
		t.Fatal("the team pod started on the regional VM")
	}

	virtual := Pod{
		Metadata: PodMeta{Name: "v", Namespace: "regional", UID: "uid-v", Annotations: map[string]string{"cloudvm.vk.io/region": "us-east-1"}},
		Spec:     PodSpecWrapper{Containers: []ContainerSpec{{Name: "main", Image: "busybox"}}},
	}
	if code := postClaimPod(t, url, virtual); code != http.StatusBadRequest {
		t.Fatalf("a regional pod outside any NodeClaim: HTTP %d, want 400", code)
	}
	virtual.Metadata.Namespace, virtual.Metadata.UID = "team-a", "uid-v-team"
	if code := postClaimPod(t, url, virtual); code != http.StatusOK {
		t.Fatalf("the same pod of a team namespace: HTTP %d, want 200", code)
	}
}

func TestClaimPodGetsResolvedEnvSources(t *testing.T) {
	s, client, url := newClaimServer(t, NewMockProvider())
	s.secretResolver = mapSecretResolver{"team-a/cache": {"url": []byte("redis://cache:6379")}}
	if _, err := client.Create(context.Background(), claimRequest("nc-env", "m7i.large")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	pod := Pod{
		Metadata: PodMeta{Name: "env", Namespace: "team-a", UID: "uid-env"},
		Spec: PodSpecWrapper{NodeName: "nc-env", Containers: []ContainerSpec{{Name: "main", Image: "busybox",
			Env: []EnvVar{{Name: "LITERAL", Value: "x"}, secretEnv("CACHE_URL", "cache", "url", false), fieldEnv("POD_NAMESPACE", "metadata.namespace")}}}},
	}
	if code := postClaimPod(t, url, pod); code != http.StatusOK {
		t.Fatalf("create: HTTP %d", code)
	}
	env := s.runner.(*MockContainerRunner).GetStartRequest("uid-env").Env
	if env["LITERAL"] != "x" || env["CACHE_URL"] != "redis://cache:6379" || env["POD_NAMESPACE"] != "team-a" {
		t.Fatalf("env = %v", env)
	}
	pod.Metadata.Name, pod.Metadata.UID = "missing", "uid-missing"
	pod.Spec.Containers[0].Env = []EnvVar{secretEnv("CACHE_URL", "absent", "url", false)}
	if code := postClaimPod(t, url, pod); code != http.StatusBadRequest {
		t.Fatalf("a pod whose required env secret is missing: HTTP %d, want 400", code)
	}
}
