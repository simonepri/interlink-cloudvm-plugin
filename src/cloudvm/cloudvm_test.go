// Package main implements the interlink-provider-cloudvm daemon.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

func TestMain(m *testing.M) {
	cleanTempStores()
	code := m.Run()
	cleanTempStores()
	os.Exit(code)
}

func cleanTempStores() {
	pattern := filepath.Join(os.TempDir(), "cloudvm-*.json")
	if matches, err := filepath.Glob(pattern); err == nil {
		for _, m := range matches {
			_ = os.Remove(m)
		}
	}
}
func TestServerCreateAndDelete(t *testing.T) {
	mock := NewMockProvider()
	server := NewServer(mock, "test-cluster")
	handler := server.Routes()

	pod := Pod{
		Metadata: PodMeta{
			Name:      "test-worker-1",
			Namespace: "default",
			Annotations: map[string]string{
				"cloudvm.vk.io/capacity-type": "spot",
				"cloudvm.vk.io/region":        "us-east-1",
				"cloudvm.vk.io/instance-type": "g5.xlarge",
			},
		},
		Spec: PodSpecWrapper{
			Containers: []ContainerSpec{
				{
					Name:  "worker",
					Image: "ubuntu:24.04",
					Resources: ContainerResourceRequirements{
						Limits: map[string]string{"nvidia.com/gpu": "1"},
					},
				},
			},
		},
	}

	body, err := json.Marshal(pod)
	if err != nil {
		t.Fatalf("failed to marshal pod: %v", err)
	}

	// 1. Test /create
	req := httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 from /create, got %d: %s", rec.Code, rec.Body.String())
	}

	// 2. Test /status
	reqStatus := httptest.NewRequest(http.MethodPost, "/status", bytes.NewReader(body))
	recStatus := httptest.NewRecorder()
	handler.ServeHTTP(recStatus, reqStatus)

	if recStatus.Code != http.StatusOK {
		t.Fatalf("expected 200 from /status, got %d", recStatus.Code)
	}

	var statusResp []PodStatusResponse
	if err := json.Unmarshal(recStatus.Body.Bytes(), &statusResp); err != nil {
		t.Fatalf("failed to parse status response: %v", err)
	}
	if len(statusResp) != 1 || len(statusResp[0].Containers) != 1 {
		t.Fatalf("unexpected status response shape: %+v", statusResp)
	}
	if !statusResp[0].Containers[0].Ready {
		t.Fatalf("expected container to be ready")
	}

	// 3. Test /delete
	reqDel := httptest.NewRequest(http.MethodPost, "/delete", bytes.NewReader(body))
	recDel := httptest.NewRecorder()
	handler.ServeHTTP(recDel, reqDel)

	if recDel.Code != http.StatusOK {
		t.Fatalf("expected 200 from /delete, got %d", recDel.Code)
	}
}

func TestFleetOverridesWeightedCapacity(t *testing.T) {
	instanceTypes := []string{"g5.xlarge", "g5.12xlarge", "p4d.24xlarge"}
	subnets := []string{"subnet-1"}

	overrides := BuildFleetOverrides(instanceTypes, subnets)
	if len(overrides) != 3 {
		t.Fatalf("expected 3 overrides, got %d", len(overrides))
	}

	// In 1 Pod = 1 Dedicated VM model, every candidate shape carries weight 1.0
	for _, ov := range overrides {
		itype := string(ov.InstanceType)
		if ov.WeightedCapacity == nil || *ov.WeightedCapacity != 1.0 {
			t.Errorf("expected weight 1.0 for %s in 1:1 pod VM model, got %v", itype, ov.WeightedCapacity)
		}
	}
}

func TestGPUCountForInstanceType(t *testing.T) {
	cases := []struct {
		itype    string
		expected int
	}{
		{"p5.48xlarge", 8},
		{"p4d.24xlarge", 8},
		{"g5.48xlarge", 8},
		{"g4dn.metal", 8},
		{"g5.12xlarge", 4},
		{"g6.24xlarge", 4},
		{"p3.8xlarge", 4},
		{"g5.xlarge", 1},
		{"g6.2xlarge", 1},
		{"g4dn.xlarge", 1},
		// CPU types must strictly return 0
		{"c6i.48xlarge", 0},
		{"c6i.24xlarge", 0},
		{"m6i.12xlarge", 0},
		{"r6i.32xlarge", 0},
		{"t3.medium", 0},
	}

	for _, tc := range cases {
		got := GPUCountForInstanceType(tc.itype)
		if got != tc.expected {
			t.Errorf("GPUCountForInstanceType(%q) = %d; expected %d", tc.itype, got, tc.expected)
		}
	}
}

func TestCloudInitGeneration(t *testing.T) {
	cfg := &CloudInitConfig{
		PodName:        "test-pod",
		PodNamespace:   "default",
		RayHeadAddress: "100.64.0.1:6379",
		RayVersion:     "2.44.0",
		UseNativeDLAMI: true,
		NumGPUs:        1,
		Region:         "us-east-1",
		InstanceType:   "g5.xlarge",
	}

	script, err := GenerateCloudInit(cfg)
	if err != nil {
		t.Fatalf("GenerateCloudInit failed: %v", err)
	}

	if !bytes.Contains([]byte(script), []byte("token-ttl-seconds: 21600")) {
		t.Errorf("cloud-init missing IMDSv2 token retrieval")
	}
	if !bytes.Contains([]byte(script), []byte("/opt/pytorch/bin/ray start")) {
		t.Errorf("cloud-init missing native DLAMI ray start command")
	}
	if !bytes.Contains([]byte(script), []byte("/dev/console")) {
		t.Errorf("cloud-init missing serial console telemetry logging")
	}
}

func TestCloudInit_ContainerModeWithMicroRunner(t *testing.T) {
	testToken := fmt.Sprintf("auth-token-%d", time.Now().UnixNano())
	cfg := &CloudInitConfig{
		PodName:              "multi-tenant-pod",
		PodNamespace:         "ai-workloads",
		PodUID:               "pod-uid-456",
		RunnerAuthToken:      testToken,
		TargetContainerImage: "ghcr.io/org/worker:v1",
		Command:              []string{"python3", "worker.py"},
		UseNativeDLAMI:       false,
		NumGPUs:              2,
		AssignedGPUs:         []int{0, 1},
		CPUMillicores:        4000,
		MemoryBytes:          16 * 1024 * 1024 * 1024,
		Region:               "us-east-1",
		InstanceType:         "g5.12xlarge",
	}

	script, err := GenerateCloudInit(cfg)
	if err != nil {
		t.Fatalf("GenerateCloudInit failed: %v", err)
	}

	if bytes.Contains([]byte(script), []byte("cloudvm-workload")) {
		t.Errorf("cloud-init contains stale reference to cloudvm-workload")
	}
	if bytes.Contains([]byte(script), []byte("shell=True")) {
		t.Errorf("cloud-init runner must not execute commands with shell=True")
	}
	if bytes.Contains([]byte(script), []byte("--pid=host")) || bytes.Contains([]byte(script), []byte("--ipc=host")) {
		t.Errorf("cloud-init containers must not share pid or ipc namespace with host")
	}
	if !bytes.Contains([]byte(script), []byte(fmt.Sprintf(`AUTH_TOKEN = "%s"`, testToken))) {
		t.Errorf("cloud-init missing runner auth token configuration")
	}
	if !bytes.Contains([]byte(script), []byte("ThreadingHTTPServer")) {
		t.Errorf("cloud-init runner should use ThreadingHTTPServer for concurrent requests")
	}
	if cfg.GPUFlag() != `--gpus '"device=0,1"'` {
		t.Errorf("cloud-init missing discrete device slicing flags with CSV quotes, got: %s", cfg.GPUFlag())
	}
	if !bytes.Contains([]byte(script), []byte("--shm-size=16g")) {
		t.Errorf("cloud-init missing shared memory limit for PyTorch DataLoader")
	}
	if !bytes.Contains([]byte(script), []byte("pod-uid-456")) || !bytes.Contains([]byte(script), []byte("/containers/start")) {
		t.Errorf("cloud-init missing initial container launch via runner endpoint, got script:\n%s", script)
	}
	if !bytes.Contains([]byte(script), []byte(`docker ps -q --filter "name=cloudvm"`)) {
		t.Errorf("cloud-init missing multi-pod idle watchdog container check")
	}
}

func TestReconcilerZombieDetection(t *testing.T) {
	mock := NewMockProvider()
	server := NewServer(mock, "test-cluster")

	// Launch an instance via mock directly (pod not in server memory -> zombie)
	ctx := context.Background()
	inst, err := mock.Launch(ctx, &InstanceSpec{
		PodName:    "vanished-pod",
		ClusterTag: "test-cluster",
		Region:     "us-east-1",
	})
	if err != nil {
		t.Fatalf("failed to launch mock instance: %v", err)
	}

	// Create reconciler with zero graceTTL so it triggers immediately
	reconciler := NewReconciler(mock, server, mock, "test-cluster", 1*time.Millisecond)
	time.Sleep(5 * time.Millisecond)

	res, err := reconciler.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	if len(res.TerminatedZombies) != 1 || res.TerminatedZombies[0] != inst.ID {
		t.Fatalf("expected zombie %s to be terminated, got %+v", inst.ID, res.TerminatedZombies)
	}
}

func TestPodToInstanceSpec_PriceCapAndEgress(t *testing.T) {
	pod := &Pod{
		Metadata: PodMeta{
			Name:      "test-worker-price",
			Namespace: "default",
			Annotations: map[string]string{
				"cloudvm.vk.io/spot":             "false",
				"cloudvm.vk.io/max-price":        "2.50",
				"cloudvm.vk.io/price-policy":     "wait",
				"cloudvm.vk.io/egress-bandwidth": "100Mbps",
				"cloudvm.vk.io/egress-burst":     "15MB",
			},
		},
		Spec: PodSpecWrapper{
			Containers: []ContainerSpec{
				{
					Name:  "worker",
					Image: "test:latest",
				},
			},
		},
	}

	spec, err := PodToInstanceSpec(pod, "cluster-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spec.MarketType != "on-demand" {
		t.Errorf("expected marketType on-demand, got %s", spec.MarketType)
	}
	if spec.MaxPrice != "2.50" {
		t.Errorf("expected maxPrice 2.50, got %s", spec.MaxPrice)
	}
	if spec.PricePolicy != "wait" {
		t.Errorf("expected pricePolicy wait, got %s", spec.PricePolicy)
	}
	if spec.EgressBandwidth != "100Mbps" {
		t.Errorf("expected egressBandwidth 100Mbps, got %s", spec.EgressBandwidth)
	}
	if spec.EgressBurst != "15MB" {
		t.Errorf("expected egressBurst 15MB, got %s", spec.EgressBurst)
	}
}

func TestCloudInit_EgressRateLimiting(t *testing.T) {
	cfg := &CloudInitConfig{
		PodName:         "rate-limited-pod",
		PodNamespace:    "default",
		EgressBandwidth: "250Mbps",
		EgressBurst:     "20MB",
	}

	script, err := GenerateCloudInit(cfg)
	if err != nil {
		t.Fatalf("GenerateCloudInit failed: %v", err)
	}

	if !bytes.Contains([]byte(script), []byte("tc qdisc add dev \"$DEV\" parent 1:2 handle 20: tbf rate 250Mbps burst 20MB latency 50ms")) {
		t.Errorf("cloud-init missing expected tc rate limiting command, got:\n%s", script)
	}
}

func TestEgressRateToBandwidthCalculation(t *testing.T) {
	// $0.90/hr on AWS ($0.09/GB) should yield ~22mbit
	bwAWS := EgressRateToBandwidth(0.90, "aws")
	if bwAWS != "22mbit" {
		t.Errorf("expected 22mbit for $0.90/hr on AWS, got %s", bwAWS)
	}

	// $4.05/hr on AWS should yield ~100mbit
	bw100 := EgressRateToBandwidth(4.05, "aws")
	if bw100 != "100mbit" {
		t.Errorf("expected 100mbit for $4.05/hr on AWS, got %s", bw100)
	}

	// $0.85/hr on GCP ($0.085/GB) should yield ~22mbit
	bwGCP := EgressRateToBandwidth(0.85, "gcp")
	if bwGCP != "22mbit" {
		t.Errorf("expected 22mbit for $0.85/hr on GCP, got %s", bwGCP)
	}
}

func TestPodToInstanceSpec_MaxEgressRateDollarConversion(t *testing.T) {
	pod := &Pod{
		Metadata: PodMeta{
			Name:      "test-dollar-egress",
			Namespace: "default",
			Annotations: map[string]string{
				"cloudvm.vk.io/max-egress-rate": "$0.90",
			},
		},
		Spec: PodSpecWrapper{
			Containers: []ContainerSpec{
				{Name: "worker", Image: "test:latest"},
			},
		},
	}

	spec, err := PodToInstanceSpec(pod, "cluster-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if spec.MaxEgressRate != "$0.90" {
		t.Errorf("expected MaxEgressRate $0.90, got %s", spec.MaxEgressRate)
	}
	if spec.EgressBandwidth != "22mbit" {
		t.Errorf("expected calculated EgressBandwidth 22mbit, got %s", spec.EgressBandwidth)
	}
	if spec.EgressBurst != "100mb" {
		t.Errorf("expected default burst 100mb, got %s", spec.EgressBurst)
	}
}

func TestInterLinkAPIContract_NestedPodAndStatus(t *testing.T) {
	mock := NewMockProvider()
	server := NewServer(mock, "test-cluster")
	ts := httptest.NewServer(server.Routes())
	defer ts.Close()

	// 1. Test POST /create with InterLink RetrievedPodData format ({"pod": {...}})
	nestedPayload := `{
		"pod": {
			"metadata": {
				"name": "interlink-worker-1",
				"namespace": "production",
				"uid": "pod-uid-abcdef-12345"
			},
			"spec": {
				"containers": [
					{
						"name": "main",
						"image": "ubuntu:24.04",
						"resources": {
							"limits": {"nvidia.com/gpu": "1"}
						}
					}
				]
			}
		}
	}`

	resp, err := http.Post(ts.URL+"/create", "application/json", strings.NewReader(nestedPayload))
	if err != nil {
		t.Fatalf("POST /create failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /create, got %d", resp.StatusCode)
	}

	var created CreateStruct
	if err := json.NewDecoder(resp.Body).Decode(&created); err != nil {
		t.Fatalf("failed to decode CreateStruct response: %v", err)
	}

	if created.PodUID != "pod-uid-abcdef-12345" {
		t.Errorf("expected PodUID pod-uid-abcdef-12345, got %s", created.PodUID)
	}
	if created.PodJID == "" {
		t.Errorf("expected non-empty PodJID")
	}

	// 2. Test GET /status (standard InterLink method)
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/status", nil)
	if err != nil {
		t.Fatalf("failed to create GET /status request: %v", err)
	}
	statusResp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /status failed: %v", err)
	}
	defer statusResp.Body.Close()

	if statusResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from GET /status, got %d", statusResp.StatusCode)
	}

	rawStatus, err := io.ReadAll(statusResp.Body)
	if err != nil {
		t.Fatalf("failed to read status body: %v", err)
	}

	// Verify exact JSON field names matching CNCF InterLink api/status.go and types.go (MUST be "UID", not "PodUID")
	var rawMaps []map[string]interface{}
	if err := json.Unmarshal(rawStatus, &rawMaps); err != nil {
		t.Fatalf("failed to decode raw status JSON: %v", err)
	}
	if len(rawMaps) != 1 {
		t.Fatalf("expected 1 status map, got %d", len(rawMaps))
	}
	if uidVal, ok := rawMaps[0]["UID"].(string); !ok || uidVal != "pod-uid-abcdef-12345" {
		t.Errorf("interLink contract violation: expected top-level field \"UID\": \"pod-uid-abcdef-12345\", got %+v", rawMaps[0])
	}
	if _, present := rawMaps[0]["PodUID"]; present {
		t.Errorf("interLink contract violation: \"PodUID\" field must not be emitted in PodStatusResponse")
	}

	var statuses []PodStatusResponse
	if err := json.Unmarshal(rawStatus, &statuses); err != nil {
		t.Fatalf("failed to decode PodStatusResponse: %v", err)
	}

	if len(statuses) != 1 {
		t.Fatalf("expected 1 status response, got %d", len(statuses))
	}

	st := statuses[0]
	if st.PodUID != "pod-uid-abcdef-12345" || st.Name != "interlink-worker-1" || st.Namespace != "production" {
		t.Errorf("status mismatch: %+v", st)
	}
	if st.JID != created.PodJID {
		t.Errorf("expected JID %s, got %s", created.PodJID, st.JID)
	}

	// 3. Test POST /delete with nested format
	delResp, err := http.Post(ts.URL+"/delete", "application/json", strings.NewReader(nestedPayload))
	if err != nil {
		t.Fatalf("POST /delete failed: %v", err)
	}
	defer delResp.Body.Close()

	if delResp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 from /delete, got %d", delResp.StatusCode)
	}
}

func TestServer_RecoverActiveInstances(t *testing.T) {
	t.Setenv("TOMBSTONE_STORE_PATH", filepath.Join(t.TempDir(), "tombstones.json"))
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))
	mock := NewMockProvider()
	ctx := context.Background()

	// Seed cloud instance
	recToken := fmt.Sprintf("token-%d", time.Now().UnixNano())
	inst, err := mock.Launch(ctx, &InstanceSpec{
		PodName:         "recovered-pod",
		PodNamespace:    "custom-ns",
		PodUID:          "recovered-uid-999",
		RunnerAuthToken: recToken,
		ClusterTag:      "test-cluster",
		Region:          "us-east-1",
	})
	if err != nil {
		t.Fatalf("failed to launch: %v", err)
	}

	// Create new server instance simulating provider restart
	server := NewServer(mock, "test-cluster")
	if len(server.ActiveInstances()) != 0 {
		t.Fatalf("expected empty server instances on cold start")
	}

	// Recover state from cloud inventory
	if err := server.RecoverActiveInstances(ctx, mock); err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	active := server.ActiveInstances()
	key := "custom-ns/recovered-pod"
	if rec, ok := active[key]; !ok || rec.ID != inst.ID {
		t.Fatalf("expected recovered instance %s under %s, got %+v", inst.ID, key, active)
	}

	node, _, found := server.NodePool().FindPodNode("recovered-uid-999")
	if !found || !node.Cordoned {
		t.Fatalf("expected recovered node to be cordoned for safe restart")
	}
	if node.RunnerAuthToken != recToken {
		t.Fatalf("expected recovered node to retain RunnerAuthToken %s, got %s", recToken, node.RunnerAuthToken)
	}

	// Verify that Allocate skips this cordoned recovered node
	req, _ := NewResourceVector("1", "2Gi", 0, 0)
	_, _, _, err = server.NodePool().Allocate(req, "us-east-1", "new-pod", "custom-ns", "new-worker")
	if err == nil {
		t.Fatalf("expected Allocate to skip cordoned recovered node")
	}
}

func TestServer_HandleStatus_InFlightAndFinished(t *testing.T) {
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	mock := NewMockProvider()
	server := NewServer(mock, "test-cluster")
	ts := httptest.NewServer(server.Routes())
	defer ts.Close()

	// 1. In-flight / unknown pod status query returns Waiting (ContainerCreating)
	queryPayload := `[{"pod": {"metadata": {"name": "inflight-pod", "namespace": "default", "uid": "uid-inflight-1"}}}]`
	resp, err := http.Post(ts.URL+"/status", "application/json", strings.NewReader(queryPayload))
	if err != nil {
		t.Fatalf("POST /status failed: %v", err)
	}
	defer resp.Body.Close()

	var statuses []PodStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&statuses); err != nil {
		t.Fatalf("failed to decode status response: %v", err)
	}
	if len(statuses) != 1 {
		t.Fatalf("expected 1 status, got %d", len(statuses))
	}
	st := statuses[0]
	if len(st.Containers) != 1 || st.Containers[0].State.Waiting == nil {
		t.Fatalf("expected Waiting state for in-flight pod, got: %+v", st)
	}
	if st.Containers[0].State.Waiting.Reason != "ContainerCreating" {
		t.Fatalf("expected Reason ContainerCreating, got %s", st.Containers[0].State.Waiting.Reason)
	}

	// 2. Pod container completed returns Terminated and frees node allocations
	runner := NewMockContainerRunner()
	runner.SetStatus("uid-done-1", &ContainerStatusResult{
		Running:    false,
		ExitCode:   0,
		Reason:     "Completed",
		FinishedAt: time.Now(),
	})
	server.SetContainerRunner(runner)

	capVec, _ := NewResourceVector("4", "16Gi", 1, 0)
	node := NewCloudVMNode("i-done-node", ProviderMock, "us-east-1", "100.64.0.10", capVec)
	reqVec, _ := NewResourceVector("1", "4Gi", 1, 0)
	server.NodePool().RegisterAndAllocate(node, reqVec, "uid-done-1", "default", "done-pod")

	server.mu.Lock()
	server.instances["uid-done-1"] = &Instance{
		ID:        "i-done-node",
		Provider:  ProviderMock,
		Region:    "us-east-1",
		State:     StateRunning,
		CreatedAt: time.Now(),
	}
	server.mu.Unlock()

	queryDonePayload := `[{"pod": {"metadata": {"name": "done-pod", "namespace": "default", "uid": "uid-done-1"}}}]`
	resp2, err := http.Post(ts.URL+"/status", "application/json", strings.NewReader(queryDonePayload))
	if err != nil {
		t.Fatalf("POST /status for done pod failed: %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()

	var statuses2 []PodStatusResponse
	if err := json.NewDecoder(resp2.Body).Decode(&statuses2); err != nil {
		t.Fatalf("failed to decode status response: %v", err)
	}
	if len(statuses2) != 1 {
		t.Fatalf("expected 1 status, got %d", len(statuses2))
	}
	st2 := statuses2[0]
	if len(st2.Containers) != 1 || st2.Containers[0].State.Terminated == nil {
		t.Fatalf("expected Terminated state for finished pod, got: %+v", st2)
	}
	if st2.Containers[0].State.Terminated.Reason != "Completed" {
		t.Fatalf("expected Reason Completed, got %s", st2.Containers[0].State.Terminated.Reason)
	}

	// Verify that nodePool has marked the allocation completed without prematurely evicting it
	node, alloc, found := server.NodePool().FindPodNode("uid-done-1")
	if !found || !alloc.Completed {
		t.Fatalf("expected uid-done-1 to be marked completed in node pool")
	}
	if node.ActivePodCount() != 0 {
		t.Fatalf("expected 0 active pods on node, got %d", node.ActivePodCount())
	}
}

func TestRegression_FinishedPodDoesNotRevertToRunning(t *testing.T) {
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	mock := NewMockProvider()
	server := NewServer(mock, "test-cluster")
	runner := NewMockContainerRunner()
	server.SetContainerRunner(runner)
	ts := httptest.NewServer(server.Routes())
	defer ts.Close()

	capVec, _ := NewResourceVector("4", "16Gi", 1, 0)
	node := NewCloudVMNode("i-revert-node", ProviderMock, "us-east-1", "100.64.0.10", capVec)
	reqVec, _ := NewResourceVector("1", "4Gi", 1, 0)
	server.NodePool().RegisterAndAllocate(node, reqVec, "uid-revert-1", "default", "revert-pod")

	server.mu.Lock()
	server.instances["uid-revert-1"] = &Instance{
		ID:        "i-revert-node",
		Provider:  ProviderMock,
		Region:    "us-east-1",
		State:     StateRunning,
		CreatedAt: time.Now(),
	}
	server.mu.Unlock()

	// Simulate container exiting
	runner.SetStatus("uid-revert-1", &ContainerStatusResult{
		Running:    false,
		ExitCode:   0,
		Reason:     "Completed",
		FinishedAt: time.Now(),
	})

	queryPayload := `[{"pod": {"metadata": {"name": "revert-pod", "namespace": "default", "uid": "uid-revert-1"}}}]`

	// Poll 1: Must return Terminated (Completed)
	resp1, err := http.Post(ts.URL+"/status", "application/json", strings.NewReader(queryPayload))
	if err != nil {
		t.Fatalf("Poll 1 failed: %v", err)
	}
	var statuses1 []PodStatusResponse
	_ = json.NewDecoder(resp1.Body).Decode(&statuses1)
	_ = resp1.Body.Close()

	if len(statuses1) != 1 || statuses1[0].Containers[0].State.Terminated == nil {
		t.Fatalf("Poll 1 expected Terminated state, got %+v", statuses1)
	}
	if statuses1[0].Containers[0].State.Terminated.Reason != "Completed" {
		t.Fatalf("Poll 1 expected Reason Completed, got %s", statuses1[0].Containers[0].State.Terminated.Reason)
	}

	// Poll 2: Must STILL return Terminated (Completed), NOT revert to Running
	resp2, err := http.Post(ts.URL+"/status", "application/json", strings.NewReader(queryPayload))
	if err != nil {
		t.Fatalf("Poll 2 failed: %v", err)
	}
	var statuses2 []PodStatusResponse
	_ = json.NewDecoder(resp2.Body).Decode(&statuses2)
	_ = resp2.Body.Close()

	if len(statuses2) != 1 || statuses2[0].Containers[0].State.Terminated == nil {
		t.Fatalf("Poll 2 REGRESSION: pod reverted to non-terminated state on second poll: %+v", statuses2)
	}
	if statuses2[0].Containers[0].State.Terminated.Reason != "Completed" {
		t.Fatalf("Poll 2 expected Reason Completed, got %s", statuses2[0].Containers[0].State.Terminated.Reason)
	}
}

func TestRegression_DeleteFinishedPodDoesNotKillCoTenants(t *testing.T) {
	mock := NewMockProvider()
	server := NewServer(mock, "test-cluster")
	runner := NewMockContainerRunner()
	server.SetContainerRunner(runner)
	ts := httptest.NewServer(server.Routes())
	defer ts.Close()

	// 1. Launch node with 2 co-tenant pods
	capVec, _ := NewResourceVector("8", "32Gi", 2, 0)
	node := NewCloudVMNode("i-shared-node", ProviderMock, "us-east-1", "100.64.0.20", capVec)
	req1, _ := NewResourceVector("2", "8Gi", 1, 0)
	req2, _ := NewResourceVector("2", "8Gi", 1, 0)

	server.NodePool().RegisterAndAllocate(node, req1, "uid-cotenant-1", "default", "worker-1")
	_, _, _, err := server.NodePool().Allocate(req2, "us-east-1", "uid-cotenant-2", "default", "worker-2")
	if err != nil {
		t.Fatalf("failed to allocate co-tenant 2: %v", err)
	}

	server.mu.Lock()
	sharedInst := &Instance{
		ID:        "i-shared-node",
		Provider:  ProviderMock,
		Region:    "us-east-1",
		State:     StateRunning,
		CreatedAt: time.Now(),
	}
	server.instances["uid-cotenant-1"] = sharedInst
	server.instances["default/worker-1"] = sharedInst
	server.instances["uid-cotenant-2"] = sharedInst
	server.instances["default/worker-2"] = sharedInst
	server.mu.Unlock()

	// 2. Pod 1 finishes
	runner.SetStatus("uid-cotenant-1", &ContainerStatusResult{
		Running:    false,
		ExitCode:   0,
		Reason:     "Completed",
		FinishedAt: time.Now(),
	})
	// Pod 2 is still running
	runner.SetStatus("uid-cotenant-2", &ContainerStatusResult{
		Running:   true,
		StartedAt: time.Now(),
	})

	// Poll status for pod 1 to record completion
	queryPayload := `[{"pod": {"metadata": {"name": "worker-1", "namespace": "default", "uid": "uid-cotenant-1"}}}]`
	resp1, err := http.Post(ts.URL+"/status", "application/json", strings.NewReader(queryPayload))
	if err != nil {
		t.Fatalf("POST /status failed: %v", err)
	}
	_ = resp1.Body.Close()

	// 3. Delete finished pod 1
	delPayload1 := `{"pod": {"metadata": {"name": "worker-1", "namespace": "default", "uid": "uid-cotenant-1"}}}`
	delResp1, err := http.Post(ts.URL+"/delete", "application/json", strings.NewReader(delPayload1))
	if err != nil {
		t.Fatalf("POST /delete for pod 1 failed: %v", err)
	}
	_ = delResp1.Body.Close()

	// 4. REGRESSION CHECK: Verify the underlying VM was NOT terminated! Pod 2 is still running on it!
	if server.NodePool().ActiveNodeCount() != 1 {
		t.Fatalf("REGRESSION: deleting finished co-tenant killed shared VM while pod 2 was still active! ActiveNodeCount=%d", server.NodePool().ActiveNodeCount())
	}
	if _, _, found := server.NodePool().FindPodNode("uid-cotenant-2"); !found {
		t.Fatalf("REGRESSION: co-tenant pod 2 was evicted from node pool when pod 1 was deleted")
	}

	// 5. Now delete pod 2; since no more active pods remain, the VM can be terminated
	delPayload2 := `{"pod": {"metadata": {"name": "worker-2", "namespace": "default", "uid": "uid-cotenant-2"}}}`
	delResp2, err := http.Post(ts.URL+"/delete", "application/json", strings.NewReader(delPayload2))
	if err != nil {
		t.Fatalf("POST /delete for pod 2 failed: %v", err)
	}
	_ = delResp2.Body.Close()

	if server.NodePool().ActiveNodeCount() != 0 {
		t.Fatalf("expected node pool to be empty after last pod deleted, got %d", server.NodePool().ActiveNodeCount())
	}
}

func TestServer_CreateIdempotency(t *testing.T) {
	mock := NewMockProvider()
	server := NewServer(mock, "test-cluster")
	ts := httptest.NewServer(server.Routes())
	defer ts.Close()

	payload := `{"pod": {"metadata": {"name": "idempotent-worker", "namespace": "default", "uid": "uid-idem-1"}, "spec": {"containers": [{"name": "worker", "image": "ubuntu:24.04"}]}}}`

	resp1, err := http.Post(ts.URL+"/create", "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("first POST /create failed: %v", err)
	}
	var res1 CreateStruct
	_ = json.NewDecoder(resp1.Body).Decode(&res1)
	_ = resp1.Body.Close()

	// Second POST /create with identical UID
	resp2, err := http.Post(ts.URL+"/create", "application/json", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("second POST /create failed: %v", err)
	}
	var res2 CreateStruct
	_ = json.NewDecoder(resp2.Body).Decode(&res2)
	_ = resp2.Body.Close()

	if res1.PodUID != res2.PodUID || res1.PodJID != res2.PodJID {
		t.Fatalf("expected idempotent response, got res1=%+v res2=%+v", res1, res2)
	}
	if server.NodePool().ActiveNodeCount() != 1 {
		t.Fatalf("expected exactly 1 node launched, got %d", server.NodePool().ActiveNodeCount())
	}
}

func TestRegression_CompletedPodPreservesStatusAfterIdleVMTermination(t *testing.T) {
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	mock := NewMockProvider()
	server := NewServer(mock, "test-cluster")
	runner := NewMockContainerRunner()
	server.SetContainerRunner(runner)
	ts := httptest.NewServer(server.Routes())
	defer ts.Close()

	// 1. Seed launched instance in mock provider so termination succeeds
	inst, err := mock.Launch(context.Background(), &InstanceSpec{
		PodName:      "batch-job",
		PodNamespace: "default",
		PodUID:       "uid-ephem-1",
		Region:       "us-east-1",
	})
	if err != nil {
		t.Fatalf("failed to launch mock instance: %v", err)
	}

	capVec, _ := NewResourceVector("4", "16Gi", 1, 0)
	node := NewCloudVMNode(inst.ID, ProviderMock, "us-east-1", "100.64.0.30", capVec)
	reqVec, _ := NewResourceVector("2", "8Gi", 1, 0)
	server.NodePool().RegisterAndAllocate(node, reqVec, "uid-ephem-1", "default", "batch-job")

	server.mu.Lock()
	server.instances["uid-ephem-1"] = inst
	server.instances["default/batch-job"] = inst
	server.mu.Unlock()

	// 2. Pod finishes
	runner.SetStatus("uid-ephem-1", &ContainerStatusResult{
		Running:    false,
		ExitCode:   0,
		Reason:     "Completed",
		FinishedAt: time.Now(),
	})

	queryPayload := `[{"pod": {"metadata": {"name": "batch-job", "namespace": "default", "uid": "uid-ephem-1"}}}]`
	resp1, err := http.Post(ts.URL+"/status", "application/json", strings.NewReader(queryPayload))
	if err != nil {
		t.Fatalf("Poll 1 failed: %v", err)
	}
	var statuses1 []PodStatusResponse
	_ = json.NewDecoder(resp1.Body).Decode(&statuses1)
	_ = resp1.Body.Close()

	if len(statuses1) != 1 || statuses1[0].Containers[0].State.Terminated == nil {
		t.Fatalf("expected Terminated state on poll 1, got: %+v", statuses1)
	}

	// 3. VM becomes idle and is terminated by idle collector
	server.TerminateIdleNode(context.Background(), node)

	// Verify instances map no longer has the VM instance
	server.mu.RLock()
	_, instExists := server.instances["uid-ephem-1"]
	server.mu.RUnlock()
	if instExists {
		t.Fatalf("expected instance to be removed after idle node termination")
	}

	// 4. Poll /status again: REGRESSION CHECK: Must STILL report Terminated (Completed), NOT Waiting(ContainerCreating)!
	resp2, err := http.Post(ts.URL+"/status", "application/json", strings.NewReader(queryPayload))
	if err != nil {
		t.Fatalf("Poll 2 failed: %v", err)
	}
	var statuses2 []PodStatusResponse
	_ = json.NewDecoder(resp2.Body).Decode(&statuses2)
	_ = resp2.Body.Close()

	if len(statuses2) != 1 || statuses2[0].Containers[0].State.Terminated == nil {
		t.Fatalf("REGRESSION: completed pod lost its terminal status after VM shutdown and reverted to: %+v", statuses2)
	}
	if statuses2[0].Containers[0].State.Terminated.Reason != "Completed" {
		t.Fatalf("expected Reason Completed, got %s", statuses2[0].Containers[0].State.Terminated.Reason)
	}

	// 5. Explicit /delete cleans up the terminal record
	delPayload := `{"pod": {"metadata": {"name": "batch-job", "namespace": "default", "uid": "uid-ephem-1"}}}`
	delResp, err := http.Post(ts.URL+"/delete", "application/json", strings.NewReader(delPayload))
	if err != nil {
		t.Fatalf("DELETE failed: %v", err)
	}
	_ = delResp.Body.Close()

	server.mu.RLock()
	_, termExists := server.completedPods["uid-ephem-1"]
	server.mu.RUnlock()
	if termExists {
		t.Fatalf("expected completedPods to be cleaned up after explicit /delete")
	}
}

func TestCloudInit_RunnerPythonSyntaxAndImports(t *testing.T) {
	cfg := &CloudInitConfig{
		PodName:              "syntax-test",
		PodNamespace:         "default",
		PodUID:               "uid-syntax-1",
		RunnerAuthToken:      "test-token",
		TargetContainerImage: "rayproject/ray:2.44.0",
		RayHeadAddress:       "10.0.0.1:6379",
	}

	script, err := GenerateCloudInit(cfg)
	if err != nil {
		t.Fatalf("failed to generate cloud-init: %v", err)
	}

	// Verify Python runner imports time and threading
	if !strings.Contains(script, "import time") || !strings.Contains(script, "import threading") {
		t.Fatalf("cloud-init runner missing import time or import threading")
	}

	// Verify Ray auto-start command restored in JSON
	if !strings.Contains(script, "ray") || !strings.Contains(script, "10.0.0.1:6379") {
		t.Fatalf("cloud-init runner missing ray auto-start command in initial pod JSON")
	}

	// Verify IMDS protection and runner port protection
	if !strings.Contains(script, "169.254.169.254") || !strings.Contains(script, "--dport 9090") {
		t.Fatalf("cloud-init missing IMDS or runner port firewall rules")
	}

	// Verify container capability isolation
	if !strings.Contains(script, "--cap-drop=SETUID") || !strings.Contains(script, "--security-opt=no-new-privileges") || !strings.Contains(script, "--user=65532:65532") {
		t.Fatalf("cloud-init missing container privilege isolation flags")
	}
}

func TestServer_ConcurrentCreateSingleFlight(t *testing.T) {
	mock := NewMockProvider()
	server := NewServer(mock, "test-singleflight")
	ts := httptest.NewServer(server.Routes())
	defer ts.Close()

	payload := `{"pod": {"metadata": {"name": "singleflight-worker", "namespace": "default", "uid": "uid-sf-1"}, "spec": {"containers": [{"name": "worker", "image": "ubuntu:24.04"}]}}}`

	const concurrency = 8
	var wg sync.WaitGroup
	results := make([]CreateStruct, concurrency)
	errs := make([]error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			resp, err := http.Post(ts.URL+"/create", "application/json", strings.NewReader(payload))
			if err != nil {
				errs[idx] = err
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				errs[idx] = fmt.Errorf("unexpected status %d", resp.StatusCode)
				return
			}
			var res CreateStruct
			if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
				errs[idx] = err
				return
			}
			results[idx] = res
		}(i)
	}

	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("concurrent request %d failed: %v", i, err)
		}
	}

	// Verify all concurrent callers received the exact same response
	expectedUID := "uid-sf-1"
	expectedJID := results[0].PodJID
	if expectedJID == "" {
		t.Fatalf("expected non-empty PodJID in response")
	}

	for i, res := range results {
		if res.PodUID != expectedUID {
			t.Errorf("request %d: expected UID %s, got %s", i, expectedUID, res.PodUID)
		}
		if res.PodJID != expectedJID {
			t.Errorf("request %d: expected JID %s, got %s", i, expectedJID, res.PodJID)
		}
	}

	// Verify exactly 1 instance was launched by MockProvider
	if mock.LaunchCount() != 1 {
		t.Fatalf("expected exactly 1 instance launch, got %d launches across %d concurrent creates", mock.LaunchCount(), concurrency)
	}
	if server.NodePool().ActiveNodeCount() != 1 {
		t.Fatalf("expected exactly 1 active node in pool, got %d", server.NodePool().ActiveNodeCount())
	}
}

func TestCompletedStore_PersistenceAcrossReload(t *testing.T) {
	tmpFile := filepath.Join(t.TempDir(), "completed-test.json")
	store1, _ := NewCompletedStore(tmpFile)

	rec := &CompletedPodRecord{
		PodUID:     "uid-persist-1",
		ExitCode:   0,
		Reason:     "Completed",
		Message:    "job succeeded",
		FinishedAt: time.Now().Round(time.Millisecond),
	}

	if err := store1.Set("uid-persist-1", rec); err != nil {
		t.Fatalf("failed to set completed record: %v", err)
	}

	// Verify second store reloads the record from disk
	store2, _ := NewCompletedStore(tmpFile)
	loaded := store2.Get("uid-persist-1")
	if loaded == nil {
		t.Fatalf("expected record to survive reload from disk")
	}
	if loaded.PodUID != rec.PodUID || loaded.ExitCode != rec.ExitCode || loaded.Reason != rec.Reason {
		t.Fatalf("mismatched record after reload: got %+v, expected %+v", loaded, rec)
	}

	// Test deletion persistence
	if err := store2.Delete("uid-persist-1"); err != nil {
		t.Fatalf("failed to delete record: %v", err)
	}

	store3, _ := NewCompletedStore(tmpFile)
	if store3.Get("uid-persist-1") != nil {
		t.Fatalf("expected record to remain deleted after reload")
	}
}

func TestServer_ContainerStates_NotPrematurelyCompleted(t *testing.T) {
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	mock := NewMockProvider()
	server := NewServer(mock, "test-states")
	runner := NewMockContainerRunner()
	server.SetContainerRunner(runner)
	ts := httptest.NewServer(server.Routes())
	defer ts.Close()

	inst, err := mock.Launch(context.Background(), &InstanceSpec{
		PodName:      "state-test",
		PodNamespace: "default",
		PodUID:       "uid-state-1",
		Region:       "us-east-1",
	})
	if err != nil {
		t.Fatalf("failed to launch mock instance: %v", err)
	}

	capVec, _ := NewResourceVector("4", "16Gi", 1, 0)
	node := NewCloudVMNode(inst.ID, ProviderMock, "us-east-1", "100.64.0.10", capVec)
	reqVec, _ := NewResourceVector("1", "4Gi", 1, 0)
	server.NodePool().RegisterAndAllocate(node, reqVec, "uid-state-1", "default", "state-test")

	server.mu.Lock()
	server.instances["uid-state-1"] = inst
	server.mu.Unlock()

	// 1. Container in restarting / paused state must be Waiting, NOT Terminated or Completed!
	runner.SetStatus("uid-state-1", &ContainerStatusResult{
		Running: false,
		Status:  "restarting",
		Reason:  "Restarting",
	})

	queryPayload := `[{"pod": {"metadata": {"name": "state-test", "namespace": "default", "uid": "uid-state-1"}}}]`
	resp, err := http.Post(ts.URL+"/status", "application/json", strings.NewReader(queryPayload))
	if err != nil {
		t.Fatalf("POST /status failed: %v", err)
	}
	var statuses []PodStatusResponse
	_ = json.NewDecoder(resp.Body).Decode(&statuses)
	_ = resp.Body.Close()

	if len(statuses) != 1 || statuses[0].Containers[0].State.Waiting == nil {
		t.Fatalf("expected Waiting state for restarting container, got: %+v", statuses)
	}
	if statuses[0].Containers[0].State.Terminated != nil {
		t.Fatalf("restarting container must NOT be marked Terminated!")
	}

	// Verify node allocation is NOT freed
	_, alloc, found := server.NodePool().FindPodNode("uid-state-1")
	if !found || alloc.Completed {
		t.Fatalf("expected pod to remain active during container restart")
	}

	// 2. Container in exited state with code 0 is Completed
	runner.SetStatus("uid-state-1", &ContainerStatusResult{
		Running:    false,
		Status:     "exited",
		ExitCode:   0,
		Reason:     "Completed",
		FinishedAt: time.Now(),
	})

	resp2, err := http.Post(ts.URL+"/status", "application/json", strings.NewReader(queryPayload))
	if err != nil {
		t.Fatalf("POST /status 2 failed: %v", err)
	}
	var statuses2 []PodStatusResponse
	_ = json.NewDecoder(resp2.Body).Decode(&statuses2)
	_ = resp2.Body.Close()

	if len(statuses2) != 1 || statuses2[0].Containers[0].State.Terminated == nil {
		t.Fatalf("expected Terminated state for exited container, got: %+v", statuses2)
	}
	if statuses2[0].Containers[0].State.Terminated.Reason != "Completed" {
		t.Fatalf("expected Reason Completed, got %s", statuses2[0].Containers[0].State.Terminated.Reason)
	}
}

func TestServer_CompletedStatusSurvivesServerRestart(t *testing.T) {
	tmpDir := t.TempDir()
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(tmpDir, "completed.json"))
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(tmpDir, "tokens.json"))
	clusterTag := fmt.Sprintf("restart-test-%d", time.Now().UnixNano())
	mock := NewMockProvider()
	server1 := NewServer(mock, clusterTag)
	runner1 := NewMockContainerRunner()
	server1.SetContainerRunner(runner1)
	ts1 := httptest.NewServer(server1.Routes())
	defer ts1.Close()

	inst, err := mock.Launch(context.Background(), &InstanceSpec{
		PodName:      "survive-pod",
		PodNamespace: "default",
		PodUID:       "uid-survive-1",
		Region:       "us-east-1",
	})
	if err != nil {
		t.Fatalf("failed to launch mock instance: %v", err)
	}

	capVec, _ := NewResourceVector("4", "16Gi", 1, 0)
	node := NewCloudVMNode(inst.ID, ProviderMock, "us-east-1", "100.64.0.10", capVec)
	reqVec, _ := NewResourceVector("1", "4Gi", 1, 0)
	server1.NodePool().RegisterAndAllocate(node, reqVec, "uid-survive-1", "default", "survive-pod")

	server1.mu.Lock()
	server1.instances["uid-survive-1"] = inst
	server1.mu.Unlock()

	runner1.SetStatus("uid-survive-1", &ContainerStatusResult{
		Running:    false,
		Status:     "exited",
		ExitCode:   0,
		Reason:     "Completed",
		FinishedAt: time.Now(),
	})

	queryPayload := `[{"pod": {"metadata": {"name": "survive-pod", "namespace": "default", "uid": "uid-survive-1"}}}]`
	resp1, err := http.Post(ts1.URL+"/status", "application/json", strings.NewReader(queryPayload))
	if err != nil {
		t.Fatalf("POST /status failed: %v", err)
	}
	var st1 []PodStatusResponse
	_ = json.NewDecoder(resp1.Body).Decode(&st1)
	_ = resp1.Body.Close()
	if len(st1) != 1 || st1[0].Containers[0].State.Terminated == nil {
		t.Fatalf("expected Terminated on server1, got: %+v", st1)
	}

	// Terminate node so VM is gone
	server1.TerminateIdleNode(context.Background(), node)

	// Spin up server2 with default configuration using the same clusterTag (simulating pod restart)
	server2 := NewServer(mock, clusterTag)
	ts2 := httptest.NewServer(server2.Routes())
	defer ts2.Close()

	// Query status on server2: must survive restart and report Terminated(Completed)!
	resp2, err := http.Post(ts2.URL+"/status", "application/json", strings.NewReader(queryPayload))
	if err != nil {
		t.Fatalf("POST /status on server2 failed: %v", err)
	}
	var st2 []PodStatusResponse
	_ = json.NewDecoder(resp2.Body).Decode(&st2)
	_ = resp2.Body.Close()

	if len(st2) != 1 || st2[0].Containers[0].State.Terminated == nil {
		t.Fatalf("expected completed status to survive server restart, got: %+v", st2)
	}
	if st2[0].Containers[0].State.Terminated.Reason != "Completed" {
		t.Fatalf("expected Reason Completed, got %s", st2[0].Containers[0].State.Terminated.Reason)
	}

	// Re-creating completed pod on server2 returns identical PodJID
	createPayload := `{"pod": {"metadata": {"name": "survive-pod", "namespace": "default", "uid": "uid-survive-1"}}}`
	cResp, err := http.Post(ts2.URL+"/create", "application/json", strings.NewReader(createPayload))
	if err != nil {
		t.Fatalf("POST /create on server2 failed: %v", err)
	}
	var cRes CreateStruct
	_ = json.NewDecoder(cResp.Body).Decode(&cRes)
	_ = cResp.Body.Close()
	if cRes.PodJID != fmt.Sprintf("%s:uid-survive-1", inst.ID) {
		t.Fatalf("expected matching PodJID %s:uid-survive-1, got %s", inst.ID, cRes.PodJID)
	}
}

func TestServer_FirstCallerCancelDoesNotFailDuplicate(t *testing.T) {
	mock := NewMockProvider()
	server := NewServer(mock, fmt.Sprintf("cancel-test-%d", time.Now().UnixNano()))
	ts := httptest.NewServer(server.Routes())
	defer ts.Close()

	payload := `{"pod": {"metadata": {"name": "cancel-worker", "namespace": "default", "uid": "uid-cancel-1"}, "spec": {"containers": [{"name": "worker", "image": "ubuntu:24.04"}]}}}`

	ctx1, cancel1 := context.WithCancel(context.Background())
	req1, err := http.NewRequestWithContext(ctx1, http.MethodPost, ts.URL+"/create", strings.NewReader(payload))
	if err != nil {
		t.Fatalf("failed to create req1: %v", err)
	}
	req1.Header.Set("Content-Type", "application/json")

	var res2 CreateStruct
	var err2 error
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond) // Ensure caller 1 hits server first
		resp2, err := http.Post(ts.URL+"/create", "application/json", strings.NewReader(payload))
		if err != nil {
			err2 = err
			return
		}
		defer resp2.Body.Close()
		if resp2.StatusCode != http.StatusOK {
			err2 = fmt.Errorf("caller 2 got status %d", resp2.StatusCode)
			return
		}
		_ = json.NewDecoder(resp2.Body).Decode(&res2)
	}()

	// Cancel caller 1
	go func() {
		time.Sleep(10 * time.Millisecond)
		cancel1()
	}()

	resp1, _ := http.DefaultClient.Do(req1)
	if resp1 != nil && resp1.Body != nil {
		_ = resp1.Body.Close()
	}

	wg.Wait()

	if err2 != nil {
		t.Fatalf("caller 2 should have succeeded despite caller 1 cancellation, but got: %v", err2)
	}
	if res2.PodUID != "uid-cancel-1" || res2.PodJID == "" {
		t.Fatalf("caller 2 received invalid response: %+v", res2)
	}

	if mock.LaunchCount() != 1 {
		t.Fatalf("expected exactly 1 launch, got %d", mock.LaunchCount())
	}
}

func TestPodToInstanceSpec_RootUIDRejected(t *testing.T) {
	rootUID := int64(0)
	pod := &Pod{
		Metadata: PodMeta{Name: "root-pod", Namespace: "default"},
		Spec: PodSpecWrapper{
			SecurityContext: &PodSecurityContext{
				RunAsUser: &rootUID,
			},
			Containers: []ContainerSpec{
				{Name: "main", Image: "alpine"},
			},
		},
	}

	_, err := PodToInstanceSpec(pod, "test-cluster")
	if err == nil {
		t.Fatal("expected error when pod specifies runAsUser: 0, got nil")
	}

	nonRootUID := int64(1000)
	pod.Spec.SecurityContext.RunAsUser = &nonRootUID
	pod.Spec.Containers[0].SecurityContext = &SecurityContext{
		RunAsUser: &rootUID,
	}

	_, err = PodToInstanceSpec(pod, "test-cluster")
	if err == nil {
		t.Fatal("expected error when container specifies runAsUser: 0, got nil")
	}
}

func TestCompletedStore_CorruptFilePreserved(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "completed.json")

	// Write corrupt JSON
	if err := os.WriteFile(storePath, []byte(`{"corrupt": [truncated...`), 0600); err != nil {
		t.Fatalf("failed to write corrupt file: %v", err)
	}

	store, err := NewCompletedStore(storePath)
	if err == nil {
		t.Fatal("expected error loading corrupt store, got nil")
	}

	// Verify a .corrupt file was created
	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("failed to read dir: %v", err)
	}
	var corruptFileFound bool
	for _, f := range files {
		if strings.Contains(f.Name(), ".corrupt.") {
			corruptFileFound = true
			data, _ := os.ReadFile(filepath.Join(tmpDir, f.Name()))
			if !strings.Contains(string(data), "truncated") {
				t.Fatalf("corrupt file preserved incorrect data: %s", string(data))
			}
		}
	}
	if !corruptFileFound {
		t.Fatal("expected .corrupt file backup to be created")
	}

	// Write a new record to verify store can continue functioning
	if err := store.Set("uid-new", &CompletedPodRecord{PodUID: "uid-new", ExitCode: 0}); err != nil {
		t.Fatalf("failed to set record in new store: %v", err)
	}

	// Verify the original corrupt backup is still present and was not clobbered
	filesAfter, _ := os.ReadDir(tmpDir)
	var backupStillPresent bool
	for _, f := range filesAfter {
		if strings.Contains(f.Name(), ".corrupt.") {
			backupStillPresent = true
		}
	}
	if !backupStillPresent {
		t.Fatal("corrupt backup was clobbered by subsequent Set")
	}
}

func TestServer_LaunchTokenPreSavedToTokenStore(t *testing.T) {
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))

	mock := NewMockProvider()
	srv := NewServer(mock, "cluster-test")

	pod := &Pod{
		Metadata: PodMeta{Name: "pod-tok-1", Namespace: "default", UID: "uid-tok-1"},
		Spec: PodSpecWrapper{
			Containers: []ContainerSpec{
				{Name: "main", Image: "alpine"},
			},
		},
	}
	body, _ := json.Marshal(pod)

	req := httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(body))
	w := httptest.NewRecorder()
	srv.handleCreate(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}

	// Token must be stored under pod UID in tokenStore
	savedToken := srv.tokenStore.Get("uid-tok-1")
	if savedToken == "" {
		t.Fatal("expected token to be saved under pod UID in tokenStore, but was empty")
	}
}

func TestTokenStore_CorruptFilePreserved(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "tokens.json")

	if err := os.WriteFile(storePath, []byte(`{"corrupt": [truncated...`), 0600); err != nil {
		t.Fatalf("failed to write corrupt file: %v", err)
	}

	store, err := NewTokenStore(storePath)
	if err == nil {
		t.Fatal("expected error loading corrupt token store, got nil")
	}

	files, err := os.ReadDir(tmpDir)
	if err != nil {
		t.Fatalf("failed to read dir: %v", err)
	}
	var corruptFileFound bool
	for _, f := range files {
		if strings.Contains(f.Name(), ".corrupt.") {
			corruptFileFound = true
			data, _ := os.ReadFile(filepath.Join(tmpDir, f.Name()))
			if !strings.Contains(string(data), "truncated") {
				t.Fatalf("corrupt file preserved incorrect data: %s", string(data))
			}
		}
	}
	if !corruptFileFound {
		t.Fatal("expected .corrupt file backup to be created")
	}

	if err := store.Set("inst-1", "tok-1"); err != nil {
		t.Fatalf("failed to set token in new store: %v", err)
	}

	// Verify backup was not clobbered
	filesAfter, _ := os.ReadDir(tmpDir)
	var backupStillPresent bool
	for _, f := range filesAfter {
		if strings.Contains(f.Name(), ".corrupt.") {
			backupStillPresent = true
		}
	}
	if !backupStillPresent {
		t.Fatal("corrupt backup was clobbered by subsequent Set")
	}
}

func TestPodToInstanceSpec_LimitsOnly(t *testing.T) {
	pod := &Pod{
		Metadata: PodMeta{Name: "req-pod", Namespace: "default"},
		Spec: PodSpecWrapper{
			Containers: []ContainerSpec{
				{
					Name:  "main",
					Image: "alpine",
					Resources: ContainerResourceRequirements{
						Requests: map[string]string{
							"cpu":    "2",
							"memory": "4Gi",
						},
					},
				},
			},
		},
	}

	spec, err := PodToInstanceSpec(pod, "cluster-1")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if spec.CPUMillicores != 0 {
		t.Fatalf("expected CPUMillicores to be 0 when only requests are set, got %d", spec.CPUMillicores)
	}
	if spec.MemoryBytes != 0 {
		t.Fatalf("expected MemoryBytes to be 0 when only requests are set, got %d", spec.MemoryBytes)
	}
}

func TestServer_RecoverActiveInstances_FallbackToPodUIDToken(t *testing.T) {
	tmpDir := t.TempDir()
	tokPath := filepath.Join(tmpDir, "tokens.json")
	store, _ := NewTokenStore(tokPath)
	_ = store.Set("uid-crash-1", "secret-token-123")

	mock := NewMockProvider()
	srv := NewServer(mock, "cluster-1")
	srv.tokenStore = store

	inst := &Instance{
		ID:           "i-crash-123",
		Provider:     "aws",
		Region:       "us-east-1",
		InstanceType: "g5.xlarge",
		Metadata: map[string]string{
			"podName":   "worker-crash",
			"namespace": "default",
			"podUID":    "uid-crash-1",
		},
	}
	inv := &mockCloudInventory{instances: []*Instance{inst}}

	if err := srv.RecoverActiveInstances(context.Background(), inv); err != nil {
		t.Fatalf("failed to recover instances: %v", err)
	}

	srv.mu.RLock()
	node := srv.nodePool.nodes["i-crash-123"]
	srv.mu.RUnlock()

	if node == nil {
		t.Fatal("expected node to be recovered")
	}
	if node.RunnerAuthToken != "secret-token-123" {
		t.Fatalf("expected token secret-token-123 to be recovered from podUID, got %q", node.RunnerAuthToken)
	}
	if store.Get("i-crash-123") != "secret-token-123" {
		t.Fatalf("expected token to be linked to instance ID in store, got %q", store.Get("i-crash-123"))
	}
}

type mockCloudInventory struct {
	instances []*Instance
}

func (m *mockCloudInventory) ListActiveInstances(_ context.Context, _ string) ([]*Instance, error) {
	return m.instances, nil
}

func TestTokenStore_NullFileDoesNotPanic(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "tokens-null.json")

	if err := os.WriteFile(storePath, []byte("null"), 0600); err != nil {
		t.Fatalf("failed to write null file: %v", err)
	}

	ts, err := NewTokenStore(storePath)
	if err != nil {
		t.Fatalf("unexpected error loading null token store: %v", err)
	}

	// Must not panic with assignment to entry in nil map
	if err := ts.Set("inst-null", "tok-null"); err != nil {
		t.Fatalf("failed to set token on previously null store: %v", err)
	}
	if tok := ts.Get("inst-null"); tok != "tok-null" {
		t.Fatalf("expected tok-null, got %s", tok)
	}
}

func TestCompletedStore_NullFileDoesNotPanic(t *testing.T) {
	tmpDir := t.TempDir()
	storePath := filepath.Join(tmpDir, "completed-null.json")

	if err := os.WriteFile(storePath, []byte("null"), 0600); err != nil {
		t.Fatalf("failed to write null file: %v", err)
	}

	cs, err := NewCompletedStore(storePath)
	if err != nil {
		t.Fatalf("unexpected error loading null completed store: %v", err)
	}

	// Must not panic with assignment to entry in nil map
	rec := &CompletedPodRecord{PodUID: "uid-null", ExitCode: 0}
	if err := cs.Set("uid-null", rec); err != nil {
		t.Fatalf("failed to set completed record on previously null store: %v", err)
	}
	if got := cs.Get("uid-null"); got == nil || got.PodUID != "uid-null" {
		t.Fatalf("expected record to be retrievable from previously null store")
	}
}

func TestReconciler_SpotInterruptionMarksPodFailed(t *testing.T) {
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))

	mock := NewMockProvider()
	srv := NewServer(mock, "cluster-spot")

	pod := &Pod{
		Metadata: PodMeta{Name: "spot-worker", Namespace: "default", UID: "uid-spot-1"},
		Spec: PodSpecWrapper{
			Containers: []ContainerSpec{
				{Name: "main", Image: "alpine"},
			},
		},
	}
	body, _ := json.Marshal(pod)

	req := httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(body))
	w := httptest.NewRecorder()
	srv.handleCreate(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from create, got %d: %s", w.Code, w.Body.String())
	}

	var res CreateStruct
	_ = json.NewDecoder(w.Body).Decode(&res)
	parts := strings.Split(res.PodJID, ":")
	instID := parts[0]

	// Terminate the instance out-of-band in cloud inventory (simulating spot reclamation)
	if err := mock.Terminate(context.Background(), instID); err != nil {
		t.Fatalf("failed to terminate mock instance: %v", err)
	}

	// Run Reconciler
	reconciler := NewReconciler(mock, srv, mock, "cluster-spot", 10*time.Minute)
	reconciler.SetLaunchGracePeriod(0)
	result, err := reconciler.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("reconciler failed: %v", err)
	}

	if len(result.EvictedPods) == 0 {
		t.Fatalf("expected reconciler to evict pod on vanished VM, got none")
	}

	// Now check /status
	statusReq := httptest.NewRequest(http.MethodPost, "/status", bytes.NewReader(body))
	statusW := httptest.NewRecorder()
	srv.handleStatus(statusW, statusReq)

	if statusW.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from status, got %d", statusW.Code)
	}

	var responses []PodStatusResponse
	_ = json.NewDecoder(statusW.Body).Decode(&responses)

	if len(responses) == 0 || len(responses[0].Containers) == 0 {
		t.Fatalf("expected valid status response, got %+v", responses)
	}

	cStatus := responses[0].Containers[0]
	if cStatus.State.Terminated == nil {
		t.Fatalf("expected container to be Terminated after spot preemption, got: %+v", cStatus.State)
	}
	if cStatus.State.Terminated.Reason != "SpotInterruption" {
		t.Fatalf("expected Terminated reason SpotInterruption, got %s", cStatus.State.Terminated.Reason)
	}
	if cStatus.State.Terminated.ExitCode != 137 {
		t.Fatalf("expected exit code 137, got %d", cStatus.State.Terminated.ExitCode)
	}
}

func TestReconciler_CompletedCotenantNotOverwritten(t *testing.T) {
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))

	mock := NewMockProvider()
	srv := NewServer(mock, "cluster-cotenant")

	// Pod 1
	p1 := &Pod{
		Metadata: PodMeta{Name: "cotenant-1", Namespace: "default", UID: "uid-cot-1"},
		Spec: PodSpecWrapper{
			Containers: []ContainerSpec{{Name: "main", Image: "alpine"}},
		},
	}
	body1, _ := json.Marshal(p1)
	w1 := httptest.NewRecorder()
	srv.handleCreate(w1, httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(body1)))
	if w1.Code != http.StatusOK {
		t.Fatalf("failed to create pod 1: %d", w1.Code)
	}

	var res1 CreateStruct
	_ = json.NewDecoder(w1.Body).Decode(&res1)
	instID := strings.Split(res1.PodJID, ":")[0]

	// Mark pod 1 as completed successfully
	term1 := &ContainerTerminatedState{
		ExitCode:   0,
		Reason:     "Completed",
		Message:    "job succeeded",
		FinishedAt: time.Now().Add(-10 * time.Minute),
	}
	srv.mu.Lock()
	srv.completedPods["uid-cot-1"] = term1
	srv.mu.Unlock()
	_ = srv.completedStore.Set("uid-cot-1", &CompletedPodRecord{
		PodUID:     "uid-cot-1",
		ExitCode:   0,
		Reason:     "Completed",
		Message:    "job succeeded",
		FinishedAt: term1.FinishedAt,
	})

	// Pod 2 on the same instance
	p2 := &Pod{
		Metadata: PodMeta{Name: "cotenant-2", Namespace: "default", UID: "uid-cot-2"},
		Spec: PodSpecWrapper{
			Containers: []ContainerSpec{{Name: "main", Image: "alpine"}},
		},
	}
	body2, _ := json.Marshal(p2)
	w2 := httptest.NewRecorder()
	srv.handleCreate(w2, httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(body2)))
	if w2.Code != http.StatusOK {
		t.Fatalf("failed to create pod 2: %d", w2.Code)
	}

	// Terminate the shared VM in cloud provider
	if err := mock.Terminate(context.Background(), instID); err != nil {
		t.Fatalf("failed to terminate instance: %v", err)
	}

	// Run ReconcileOnce
	reconciler := NewReconciler(mock, srv, mock, "cluster-cotenant", 10*time.Minute)
	reconciler.SetLaunchGracePeriod(0)
	result, err := reconciler.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("reconciler failed: %v", err)
	}

	// Succeeded pod 1 must NOT be in EvictedPods
	for _, evicted := range result.EvictedPods {
		if evicted == "uid-cot-1" {
			t.Fatalf("succeeded pod uid-cot-1 was erroneously evicted!")
		}
	}

	// Verify pod 1 status is STILL Completed
	rec1 := srv.completedStore.Get("uid-cot-1")
	if rec1 == nil || rec1.Reason != "Completed" || rec1.ExitCode != 0 {
		t.Fatalf("completed record for pod 1 was overwritten: %+v", rec1)
	}

	// Verify pod 2 status is SpotInterruption
	rec2 := srv.completedStore.Get("uid-cot-2")
	if rec2 == nil || rec2.Reason != "SpotInterruption" || rec2.ExitCode != 137 {
		t.Fatalf("expected pod 2 to be marked SpotInterruption, got: %+v", rec2)
	}

	// Run a second reconcile pass: verify it is idempotent and does not re-evict
	result2, err := reconciler.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	if len(result2.EvictedPods) != 0 {
		t.Fatalf("second reconcile pass re-evicted pods: %v", result2.EvictedPods)
	}
}

type mockEC2FleetClient struct {
	createFleetFn           func(ctx context.Context, params *ec2.CreateFleetInput) (*ec2.CreateFleetOutput, error)
	describeInstancesFn     func(ctx context.Context, params *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error)
	terminateInstancesFn    func(ctx context.Context, params *ec2.TerminateInstancesInput) (*ec2.TerminateInstancesOutput, error)
	describeInstanceTypesFn func(ctx context.Context, params *ec2.DescribeInstanceTypesInput) (*ec2.DescribeInstanceTypesOutput, error)
}

func (m *mockEC2FleetClient) CreateFleet(ctx context.Context, params *ec2.CreateFleetInput, _ ...func(*ec2.Options)) (*ec2.CreateFleetOutput, error) {
	if m.createFleetFn != nil {
		return m.createFleetFn(ctx, params)
	}
	return &ec2.CreateFleetOutput{}, nil
}

func (m *mockEC2FleetClient) DescribeInstances(ctx context.Context, params *ec2.DescribeInstancesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstancesOutput, error) {
	if m.describeInstancesFn != nil {
		return m.describeInstancesFn(ctx, params)
	}
	return &ec2.DescribeInstancesOutput{}, nil
}

func (m *mockEC2FleetClient) TerminateInstances(ctx context.Context, params *ec2.TerminateInstancesInput, _ ...func(*ec2.Options)) (*ec2.TerminateInstancesOutput, error) {
	if m.terminateInstancesFn != nil {
		return m.terminateInstancesFn(ctx, params)
	}
	return &ec2.TerminateInstancesOutput{}, nil
}

func (m *mockEC2FleetClient) DescribeInstanceTypes(ctx context.Context, params *ec2.DescribeInstanceTypesInput, _ ...func(*ec2.Options)) (*ec2.DescribeInstanceTypesOutput, error) {
	if m.describeInstanceTypesFn != nil {
		return m.describeInstanceTypesFn(ctx, params)
	}
	return &ec2.DescribeInstanceTypesOutput{}, nil
}

func TestReconciler_LaunchGracePeriodPreventsPrematureEviction(t *testing.T) {
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))

	mock := NewMockProvider()
	srv := NewServer(mock, "cluster-grace")

	podJSON := `{
		"metadata": {
			"name": "pod-grace-1",
			"namespace": "default",
			"uid": "uid-grace-1",
			"annotations": {
				"cloudvm.vk.io/instance-type": "g5.xlarge",
				"cloudvm.vk.io/capacity-type": "spot"
			}
		},
		"spec": {
			"containers": [{"name": "app", "image": "alpine"}]
		}
	}`

	req := httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader([]byte(podJSON)))
	w := httptest.NewRecorder()
	srv.handleCreate(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("create failed with %d: %s", w.Code, w.Body.String())
	}

	var res CreateStruct
	_ = json.NewDecoder(w.Body).Decode(&res)
	instID := strings.Split(res.PodJID, ":")[0]

	// Simulate EC2 eventual consistency: instance exists on server tracker but is not yet returned in DescribeInstances
	_ = mock.Terminate(context.Background(), instID)

	// Reconciler with default 2m launch grace period
	reconciler := NewReconciler(mock, srv, mock, "cluster-grace", 10*time.Minute)
	result1, err := reconciler.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("reconcile pass 1 failed: %v", err)
	}
	if len(result1.EvictedPods) != 0 {
		t.Fatalf("expected 0 evicted pods during launch grace period, got %v", result1.EvictedPods)
	}

	// Now disable launch grace period to simulate aged instance
	reconciler.SetLaunchGracePeriod(0)
	result2, err := reconciler.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("reconcile pass 2 failed: %v", err)
	}
	if len(result2.EvictedPods) != 1 || result2.EvictedPods[0] != "uid-grace-1" {
		t.Fatalf("expected pod uid-grace-1 evicted after grace period elapsed, got %v", result2.EvictedPods)
	}
}

func TestAWSProvider_ListActiveInstances_PartialRegionFailureDoesNotBlockOtherRegions(t *testing.T) {
	t.Setenv("REGION_STORE_PATH", filepath.Join(t.TempDir(), "regions.json"))

	awsProv := NewAWSProvider("us-east-1", "us-west-2")

	// us-east-1 returns 1 healthy running instance
	eastClient := &mockEC2FleetClient{
		describeInstancesFn: func(_ context.Context, _ *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
			return &ec2.DescribeInstancesOutput{
				Reservations: []ec2types.Reservation{
					{
						Instances: []ec2types.Instance{
							{
								InstanceId:   aws.String("i-east-1"),
								InstanceType: ec2types.InstanceTypeG5Xlarge,
								State: &ec2types.InstanceState{
									Name: ec2types.InstanceStateNameRunning,
								},
								PrivateIpAddress: aws.String("10.0.1.10"),
								LaunchTime:       aws.Time(time.Now()),
								Tags: []ec2types.Tag{
									{Key: aws.String("ClusterTag"), Value: aws.String("test-cluster")},
									{Key: aws.String("PodUID"), Value: aws.String("uid-east-1")},
									{Key: aws.String("PodName"), Value: aws.String("pod-east-1")},
									{Key: aws.String("Namespace"), Value: aws.String("default")},
								},
							},
						},
					},
				},
			}, nil
		},
	}
	awsProv.SetRegionClient("us-east-1", eastClient)

	// us-west-2 returns throttling error (RequestLimitExceeded)
	westClient := &mockEC2FleetClient{
		describeInstancesFn: func(_ context.Context, _ *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
			return nil, fmt.Errorf("RequestLimitExceeded: EC2 API rate limit exceeded")
		},
	}
	awsProv.SetRegionClient("us-west-2", westClient)

	// 1. Direct ListActiveInstances check: returns healthy region instances instead of failing completely
	instances, err := awsProv.ListActiveInstances(context.Background(), "test-cluster")
	if err != nil {
		t.Fatalf("expected successful response with instances from healthy regions, got error: %v", err)
	}
	if len(instances) != 1 || instances[0].ID != "i-east-1" {
		t.Fatalf("expected 1 instance i-east-1 from us-east-1, got %v", instances)
	}

	// 2. Reconciler test: healthy region reconciles, failed region skips without false evictions
	mockTracker := &mockTrackerWrapper{
		activePods: map[string]string{
			"uid-east-1": "i-east-1",
			"uid-east-2": "i-east-vanished",
			"uid-west-1": "i-west-healthy-pod",
		},
		podRegions: map[string]string{
			"uid-east-1": "us-east-1",
			"uid-east-2": "us-east-1",
			"uid-west-1": "us-west-2",
		},
	}
	reconciler := NewReconciler(awsProv, mockTracker, nil, "test-cluster", 10*time.Minute)
	reconciler.SetLaunchGracePeriod(0)
	result, recErr := reconciler.ReconcileOnce(context.Background())
	if recErr != nil {
		t.Fatalf("reconciler failed: %v", recErr)
	}

	// uid-east-1 is alive -> not evicted
	// uid-east-2 is vanished from us-east-1 -> evicted
	// uid-west-1 in failing region us-west-2 -> SKIPPED (protected from false eviction)
	if len(result.EvictedPods) != 1 || result.EvictedPods[0] != "uid-east-2" {
		t.Fatalf("expected only uid-east-2 to be evicted, got: %v", result.EvictedPods)
	}
	for _, f := range mockTracker.failedCalls {
		if f == "uid-west-1" {
			t.Fatalf("pod in failing region uid-west-1 was erroneously evicted!")
		}
	}
}

func TestAWSProvider_ListActiveInstances_AllRegionsFailureReturnsError(t *testing.T) {
	t.Setenv("REGION_STORE_PATH", filepath.Join(t.TempDir(), "regions.json"))
	awsProv := NewAWSProvider("us-west-2")
	westClient := &mockEC2FleetClient{
		describeInstancesFn: func(_ context.Context, _ *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
			return nil, fmt.Errorf("AuthFailure: AWS credentials invalid")
		},
	}
	awsProv.SetRegionClient("us-west-2", westClient)

	instances, err := awsProv.ListActiveInstances(context.Background(), "test-cluster")
	if err == nil {
		t.Fatalf("expected error when all regions fail, got nil with instances: %v", instances)
	}
}

func TestAWSProvider_StaleSpotReclaimedInstancesPrunedFromRegionStore(t *testing.T) {
	t.Setenv("REGION_STORE_PATH", filepath.Join(t.TempDir(), "regions.json"))
	awsProv := NewAWSProvider("us-east-1")

	// Pre-populate an instance in region store
	if err := awsProv.regionStore.Set("i-spot-reclaimed", "us-east-1"); err != nil {
		t.Fatalf("failed to set initial mapping: %v", err)
	}
	awsProv.instanceRegions["i-spot-reclaimed"] = "us-east-1"

	// us-east-1 returns 0 instances (spot instance was reclaimed and terminated)
	eastClient := &mockEC2FleetClient{
		describeInstancesFn: func(_ context.Context, _ *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
			return &ec2.DescribeInstancesOutput{}, nil
		},
	}
	awsProv.SetRegionClient("us-east-1", eastClient)

	// Pass 1: lagging instance is NOT pruned immediately
	_, _ = awsProv.ListActiveInstancesByRegion(context.Background(), "test-cluster")
	if reg := awsProv.instanceRegions["i-spot-reclaimed"]; reg != "us-east-1" {
		t.Fatalf("expected instance not pruned on pass 1, but got empty")
	}

	// Pass 2: still retained
	_, _ = awsProv.ListActiveInstancesByRegion(context.Background(), "test-cluster")
	if reg := awsProv.instanceRegions["i-spot-reclaimed"]; reg != "us-east-1" {
		t.Fatalf("expected instance not pruned on pass 2, but got empty")
	}

	// Pass 3: confirmed missing for 3 consecutive passes -> pruned!
	_, _ = awsProv.ListActiveInstancesByRegion(context.Background(), "test-cluster")
	if reg := awsProv.instanceRegions["i-spot-reclaimed"]; reg != "" {
		t.Fatalf("expected reclaimed instance pruned from instanceRegions after 3 misses, got %s", reg)
	}
	if reg := awsProv.regionStore.Get("i-spot-reclaimed"); reg != "" {
		t.Fatalf("expected reclaimed instance pruned from regionStore after 3 misses, got %s", reg)
	}
}

func TestAWSProvider_LaggingInstanceNotPrunedAndRegionRetained(t *testing.T) {
	t.Setenv("REGION_STORE_PATH", filepath.Join(t.TempDir(), "regions.json"))
	awsProv := NewAWSProvider("us-east-1") // us-west-2 is NOT in default configured regions

	// Simulate launching an instance in candidate region us-west-2
	awsProv.AddConfiguredRegions("us-west-2")
	awsProv.instanceRegions["i-new-lagging"] = "us-west-2"
	_ = awsProv.regionStore.Set("i-new-lagging", "us-west-2")

	eastClient := &mockEC2FleetClient{
		describeInstancesFn: func(_ context.Context, _ *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
			return &ec2.DescribeInstancesOutput{}, nil
		},
	}
	awsProv.SetRegionClient("us-east-1", eastClient)

	// Pass 1: us-west-2 returns empty list (EC2 eventual consistency lagging)
	westClient := &mockEC2FleetClient{
		describeInstancesFn: func(_ context.Context, _ *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
			return &ec2.DescribeInstancesOutput{}, nil
		},
	}
	awsProv.SetRegionClient("us-west-2", westClient)

	byReg, _ := awsProv.ListActiveInstancesByRegion(context.Background(), "test-cluster")
	// Verify us-west-2 WAS queried
	if _, queried := byReg["us-west-2"]; !queried {
		t.Fatalf("expected us-west-2 to be queried in pass 1")
	}
	// Verify mapping was NOT dropped
	if reg := awsProv.instanceRegions["i-new-lagging"]; reg != "us-west-2" {
		t.Fatalf("expected i-new-lagging mapping retained, got %s", reg)
	}

	// Pass 2: EC2 indexes the instance and returns it
	westClient.describeInstancesFn = func(_ context.Context, _ *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
		return &ec2.DescribeInstancesOutput{
			Reservations: []ec2types.Reservation{
				{
					Instances: []ec2types.Instance{
						{
							InstanceId:   aws.String("i-new-lagging"),
							InstanceType: ec2types.InstanceTypeG5Xlarge,
							State: &ec2types.InstanceState{
								Name: ec2types.InstanceStateNameRunning,
							},
							PrivateIpAddress: aws.String("10.0.2.20"),
							LaunchTime:       aws.Time(time.Now()),
							Tags: []ec2types.Tag{
								{Key: aws.String("ClusterTag"), Value: aws.String("test-cluster")},
								{Key: aws.String("PodUID"), Value: aws.String("uid-lagging")},
								{Key: aws.String("PodName"), Value: aws.String("pod-lagging")},
							},
						},
					},
				},
			},
		}, nil
	}

	byReg2, _ := awsProv.ListActiveInstancesByRegion(context.Background(), "test-cluster")
	if len(byReg2["us-west-2"]) != 1 || byReg2["us-west-2"][0].ID != "i-new-lagging" {
		t.Fatalf("expected i-new-lagging visible in pass 2, got: %v", byReg2["us-west-2"])
	}
	if awsProv.missingCount["i-new-lagging"] != 0 {
		t.Fatalf("expected missingCount reset to 0, got %d", awsProv.missingCount["i-new-lagging"])
	}
}

func TestReconciler_RegionThrottledAtStartupAdoptsVMsInsteadOfKilling(t *testing.T) {
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))
	t.Setenv("REGION_STORE_PATH", filepath.Join(t.TempDir(), "regions.json"))

	awsProv := NewAWSProvider("us-east-1", "us-west-2")
	srv := NewServer(awsProv, "cluster-recovery-throttle")

	// 1. Simulate startup recovery when us-west-2 is throttled/failing
	eastClient := &mockEC2FleetClient{
		describeInstancesFn: func(_ context.Context, _ *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
			return &ec2.DescribeInstancesOutput{}, nil
		},
	}
	awsProv.SetRegionClient("us-east-1", eastClient)

	westClient := &mockEC2FleetClient{
		describeInstancesFn: func(_ context.Context, _ *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
			return nil, fmt.Errorf("RequestLimitExceeded: API throttled during startup")
		},
	}
	awsProv.SetRegionClient("us-west-2", westClient)

	// Startup recovery runs
	_ = srv.RecoverActiveInstances(context.Background(), awsProv)

	// Server state has NOT recovered i-west because us-west-2 failed
	srv.mu.RLock()
	_, hasWest := srv.instances["uid-west-active"]
	srv.mu.RUnlock()
	if hasWest {
		t.Fatalf("expected uid-west-active not recovered during throttled startup")
	}

	// 2. Later in steady state, us-west-2 recovers and answers with the running VM
	westClient.describeInstancesFn = func(_ context.Context, _ *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
		return &ec2.DescribeInstancesOutput{
			Reservations: []ec2types.Reservation{
				{
					Instances: []ec2types.Instance{
						{
							InstanceId:        aws.String("i-west-active"),
							InstanceType:      ec2types.InstanceTypeG5Xlarge,
							InstanceLifecycle: ec2types.InstanceLifecycleTypeSpot,
							State: &ec2types.InstanceState{
								Name: ec2types.InstanceStateNameRunning,
							},
							PrivateIpAddress: aws.String("10.0.2.15"),
							LaunchTime:       aws.Time(time.Now().Add(-15 * time.Minute)), // older than graceTTL!
							Tags: []ec2types.Tag{
								{Key: aws.String("ClusterTag"), Value: aws.String("cluster-recovery-throttle")},
								{Key: aws.String("PodUID"), Value: aws.String("uid-west-active")},
								{Key: aws.String("PodName"), Value: aws.String("pod-west-active")},
								{Key: aws.String("Namespace"), Value: aws.String("default")},
							},
						},
					},
				},
			},
		}, nil
	}

	// InterLink polls status for pod uid-west-active, providing active evidence
	statusBody := `[{"pod":{"metadata":{"name":"pod-west-active","namespace":"default","uid":"uid-west-active"}}}]`
	req := httptest.NewRequest(http.MethodPost, "/status", bytes.NewBufferString(statusBody))
	srv.handleStatus(httptest.NewRecorder(), req)

	reconciler := NewReconciler(awsProv, srv, awsProv, "cluster-recovery-throttle", 10*time.Minute)
	result, err := reconciler.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("reconciler failed: %v", err)
	}

	// CRITICAL CHECK: i-west-active MUST NOT be terminated as a zombie!
	if len(result.TerminatedZombies) != 0 {
		t.Fatalf("CRITICAL BUG: active VM i-west-active was terminated as a zombie: %v", result.TerminatedZombies)
	}

	// Verify i-west-active was re-adopted into server tracked instances
	srv.mu.RLock()
	inst := srv.instances["uid-west-active"]
	srv.mu.RUnlock()
	if inst == nil {
		t.Fatalf("expected i-west-active re-adopted into server instances")
	}
	if inst.ID != "i-west-active" {
		t.Fatalf("expected re-adopted instance ID i-west-active, got %s", inst.ID)
	}
}

func TestReconciler_EC2StateReasonQueriedFromStatus(t *testing.T) {
	t.Setenv("REGION_STORE_PATH", filepath.Join(t.TempDir(), "regions.json"))

	awsProv := NewAWSProvider("us-east-1")
	mockTracker := &mockTrackerWrapper{
		activePods: map[string]string{
			"uid-spot-reclaimed": "i-spot-reclaimed",
		},
		podRegions: map[string]string{
			"uid-spot-reclaimed": "us-east-1",
		},
	}

	// ListActiveInstances returns empty (instance is terminated/missing from pending/running)
	eastClient := &mockEC2FleetClient{
		describeInstancesFn: func(_ context.Context, params *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
			if len(params.InstanceIds) > 0 && params.InstanceIds[0] == "i-spot-reclaimed" {
				// By-ID describe returns terminated instance with StateReason
				return &ec2.DescribeInstancesOutput{
					Reservations: []ec2types.Reservation{
						{
							Instances: []ec2types.Instance{
								{
									InstanceId: aws.String("i-spot-reclaimed"),
									State: &ec2types.InstanceState{
										Name: ec2types.InstanceStateNameTerminated,
									},
									StateReason: &ec2types.StateReason{
										Code:    aws.String("Server.SpotInstanceTermination"),
										Message: aws.String("Spot instance termination initiated by EC2"),
									},
								},
							},
						},
					},
				}, nil
			}
			return &ec2.DescribeInstancesOutput{}, nil
		},
	}
	awsProv.SetRegionClient("us-east-1", eastClient)

	reconciler := NewReconciler(awsProv, mockTracker, nil, "test-cluster", 10*time.Minute)
	reconciler.SetLaunchGracePeriod(0)
	result, err := reconciler.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("reconciler failed: %v", err)
	}
	if len(result.EvictedPods) != 1 || result.EvictedPods[0] != "uid-spot-reclaimed" {
		t.Fatalf("expected uid-spot-reclaimed evicted, got: %v", result.EvictedPods)
	}
}

func TestServer_RecoveredOnDemandVMReportsNodeLost(t *testing.T) {
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))
	t.Setenv("REGION_STORE_PATH", filepath.Join(t.TempDir(), "regions.json"))

	awsProv := NewAWSProvider("us-east-1")
	srv := NewServer(awsProv, "cluster-ondemand-recover")

	eastClient := &mockEC2FleetClient{
		describeInstancesFn: func(_ context.Context, params *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
			if len(params.InstanceIds) > 0 {
				// By-ID describe when vanished returns terminated on-demand instance
				return &ec2.DescribeInstancesOutput{
					Reservations: []ec2types.Reservation{
						{
							Instances: []ec2types.Instance{
								{
									InstanceId:        aws.String("i-od-recovered"),
									InstanceLifecycle: "", // empty means on-demand!
									State: &ec2types.InstanceState{
										Name: ec2types.InstanceStateNameTerminated,
									},
								},
							},
						},
					},
				}, nil
			}
			// Bulk inventory returns running on-demand instance
			return &ec2.DescribeInstancesOutput{
				Reservations: []ec2types.Reservation{
					{
						Instances: []ec2types.Instance{
							{
								InstanceId:        aws.String("i-od-recovered"),
								InstanceType:      ec2types.InstanceTypeG5Xlarge,
								InstanceLifecycle: "", // on-demand!
								State: &ec2types.InstanceState{
									Name: ec2types.InstanceStateNameRunning,
								},
								PrivateIpAddress: aws.String("10.0.1.55"),
								LaunchTime:       aws.Time(time.Now()),
								Tags: []ec2types.Tag{
									{Key: aws.String("ClusterTag"), Value: aws.String("cluster-ondemand-recover")},
									{Key: aws.String("PodUID"), Value: aws.String("uid-od-recovered")},
									{Key: aws.String("PodName"), Value: aws.String("pod-od-recovered")},
									{Key: aws.String("Namespace"), Value: aws.String("default")},
								},
							},
						},
					},
				},
			}, nil
		},
	}
	awsProv.SetRegionClient("us-east-1", eastClient)

	// 1. Recover active instances on restart
	if err := srv.RecoverActiveInstances(context.Background(), awsProv); err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	srv.mu.RLock()
	recInst := srv.instances["uid-od-recovered"]
	srv.mu.RUnlock()
	if recInst == nil {
		t.Fatalf("expected recovered instance entry")
	}
	if recInst.Metadata["marketType"] != "on-demand" {
		t.Fatalf("expected recovered instance marketType on-demand, got %s", recInst.Metadata["marketType"])
	}

	// 2. Simulate instance vanishing from bulk active inventory
	eastClient.describeInstancesFn = func(_ context.Context, params *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
		if len(params.InstanceIds) > 0 {
			// By-ID status query
			return &ec2.DescribeInstancesOutput{
				Reservations: []ec2types.Reservation{
					{
						Instances: []ec2types.Instance{
							{
								InstanceId:        aws.String("i-od-recovered"),
								InstanceLifecycle: "",
								State: &ec2types.InstanceState{
									Name: ec2types.InstanceStateNameTerminated,
								},
							},
						},
					},
				},
			}, nil
		}
		return &ec2.DescribeInstancesOutput{}, nil
	}

	reconciler := NewReconciler(awsProv, srv, awsProv, "cluster-ondemand-recover", 10*time.Minute)
	reconciler.SetLaunchGracePeriod(0)
	result, err := reconciler.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if len(result.EvictedPods) != 1 {
		t.Fatalf("expected 1 evicted pod, got %v", result.EvictedPods)
	}

	rec := srv.completedStore.Get("uid-od-recovered")
	if rec == nil || rec.Reason != "NodeLost" {
		t.Fatalf("expected recovered on-demand VM failure reason NodeLost, got: %+v", rec)
	}
}

func TestNodePool_OnDemandPodNeverPacksOntoSpotVM(t *testing.T) {
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))

	mock := NewMockProvider()
	srv := NewServer(mock, "cluster-market-separation")

	// Launch spot Pod 1
	spotPodJSON := `{
		"metadata": {
			"name": "pod-spot-1",
			"namespace": "default",
			"uid": "uid-spot-1",
			"annotations": {
				"cloudvm.vk.io/instance-type": "g5.12xlarge",
				"cloudvm.vk.io/capacity-type": "spot",
				"cloudvm.vk.io/cpu": "4",
				"cloudvm.vk.io/memory": "16Gi"
			}
		},
		"spec": {
			"containers": [{"name": "app", "image": "pytorch"}]
		}
	}`
	req1 := httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader([]byte(spotPodJSON)))
	w1 := httptest.NewRecorder()
	srv.handleCreate(w1, req1)
	if w1.Code != http.StatusOK {
		t.Fatalf("spot pod create failed: %d: %s", w1.Code, w1.Body.String())
	}
	var res1 CreateStruct
	_ = json.NewDecoder(w1.Body).Decode(&res1)
	spotInstID := strings.Split(res1.PodJID, ":")[0]

	// Launch on-demand Pod 2 with small resources that would fit on spotInstID if market were ignored
	odPodJSON := `{
		"metadata": {
			"name": "pod-od-1",
			"namespace": "default",
			"uid": "uid-od-1",
			"annotations": {
				"cloudvm.vk.io/instance-type": "g5.12xlarge",
				"cloudvm.vk.io/capacity-type": "on-demand",
				"cloudvm.vk.io/cpu": "4",
				"cloudvm.vk.io/memory": "16Gi"
			}
		},
		"spec": {
			"containers": [{"name": "app", "image": "pytorch"}]
		}
	}`
	req2 := httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader([]byte(odPodJSON)))
	w2 := httptest.NewRecorder()
	srv.handleCreate(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("on-demand pod create failed: %d: %s", w2.Code, w2.Body.String())
	}
	var res2 CreateStruct
	_ = json.NewDecoder(w2.Body).Decode(&res2)
	odInstID := strings.Split(res2.PodJID, ":")[0]

	if spotInstID == odInstID {
		t.Fatalf("CRITICAL BUG: on-demand pod was packed onto spot VM %s!", spotInstID)
	}

	srv.mu.RLock()
	instSpot := srv.instances["uid-spot-1"]
	instOD := srv.instances["uid-od-1"]
	srv.mu.RUnlock()

	if instSpot.Metadata["marketType"] != "spot" {
		t.Fatalf("expected spot VM metadata marketType spot, got %s", instSpot.Metadata["marketType"])
	}
	if instOD.Metadata["marketType"] != "on-demand" {
		t.Fatalf("expected on-demand VM metadata marketType on-demand, got %s", instOD.Metadata["marketType"])
	}

	// Launch a second on-demand pod: verify it CAN bin-pack onto the on-demand VM!
	odPod2JSON := `{
		"metadata": {
			"name": "pod-od-2",
			"namespace": "default",
			"uid": "uid-od-2",
			"annotations": {
				"cloudvm.vk.io/instance-type": "g5.12xlarge",
				"cloudvm.vk.io/capacity-type": "on-demand",
				"cloudvm.vk.io/cpu": "4",
				"cloudvm.vk.io/memory": "16Gi"
			}
		},
		"spec": {
			"containers": [{"name": "app", "image": "pytorch"}]
		}
	}`
	req3 := httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader([]byte(odPod2JSON)))
	w3 := httptest.NewRecorder()
	srv.handleCreate(w3, req3)
	if w3.Code != http.StatusOK {
		t.Fatalf("on-demand pod 2 create failed: %d: %s", w3.Code, w3.Body.String())
	}
	var res3 CreateStruct
	_ = json.NewDecoder(w3.Body).Decode(&res3)
	odInstID2 := strings.Split(res3.PodJID, ":")[0]

	if odInstID2 != odInstID {
		t.Fatalf("expected on-demand pod 2 to bin-pack on on-demand VM %s, got %s", odInstID, odInstID2)
	}

	srv.mu.RLock()
	instOD2 := srv.instances["uid-od-2"]
	srv.mu.RUnlock()
	if instOD2.Metadata["marketType"] != "on-demand" {
		t.Fatalf("expected packed on-demand pod to inherit marketType on-demand, got %s", instOD2.Metadata["marketType"])
	}
}

type mockTrackerWrapper struct {
	activePods  map[string]string
	podRegions  map[string]string
	failedCalls []string
}

func (m *mockTrackerWrapper) GetActivePods(_ context.Context) (map[string]string, error) {
	return m.activePods, nil
}

func (m *mockTrackerWrapper) GetPodRegion(_ context.Context, podKey string) (string, bool) {
	if m.podRegions != nil {
		reg, ok := m.podRegions[podKey]
		return reg, ok
	}
	return "", false
}

func (m *mockTrackerWrapper) MarkPodFailed(_ context.Context, podName string, _ string) error {
	m.failedCalls = append(m.failedCalls, podName)
	return nil
}

func TestAWSProvider_RegionPersistenceAcrossReload(t *testing.T) {
	storePath := filepath.Join(t.TempDir(), "instance-regions.json")
	t.Setenv("REGION_STORE_PATH", storePath)

	p1 := NewAWSProvider("us-east-1")
	regStore, err := NewRegionStore(storePath)
	if err != nil {
		t.Fatalf("failed to create region store: %v", err)
	}
	p1.SetRegionStore(regStore)

	// Save instance region mapping
	if err := p1.regionStore.Set("i-west-persist", "us-west-2"); err != nil {
		t.Fatalf("failed to set region: %v", err)
	}

	// Recreate provider from scratch (simulating daemon restart)
	p2 := NewAWSProvider("us-east-1")
	if reg := p2.instanceRegions["i-west-persist"]; reg != "us-west-2" {
		t.Fatalf("expected restored region us-west-2, got %s", reg)
	}

	// Terminate should remove from store
	westClient := &mockEC2FleetClient{
		terminateInstancesFn: func(_ context.Context, _ *ec2.TerminateInstancesInput) (*ec2.TerminateInstancesOutput, error) {
			return &ec2.TerminateInstancesOutput{}, nil
		},
	}
	p2.SetRegionClient("us-west-2", westClient)
	if err := p2.Terminate(context.Background(), "i-west-persist"); err != nil {
		t.Fatalf("terminate failed: %v", err)
	}

	p3 := NewAWSProvider("us-east-1")
	if reg := p3.instanceRegions["i-west-persist"]; reg != "" {
		t.Fatalf("expected empty region after termination reload, got %s", reg)
	}
}

func TestServer_WarmNodeCotenantMetadataAndOnDemandNodeLost(t *testing.T) {
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))

	mock := NewMockProvider()
	srv := NewServer(mock, "cluster-ondemand")

	// Pod 1: on-demand
	pod1JSON := `{
		"metadata": {
			"name": "pod-od-1",
			"namespace": "default",
			"uid": "uid-od-1",
			"annotations": {
				"cloudvm.vk.io/instance-type": "g5.12xlarge",
				"cloudvm.vk.io/capacity-type": "on-demand",
				"cloudvm.vk.io/cpu": "4",
				"cloudvm.vk.io/memory": "16Gi"
			}
		},
		"spec": {
			"containers": [{"name": "app", "image": "pytorch"}]
		}
	}`
	req1 := httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader([]byte(pod1JSON)))
	w1 := httptest.NewRecorder()
	srv.handleCreate(w1, req1)
	if w1.Code != http.StatusOK {
		t.Fatalf("pod 1 create failed: %d: %s", w1.Code, w1.Body.String())
	}
	var res1 CreateStruct
	_ = json.NewDecoder(w1.Body).Decode(&res1)
	instID := strings.Split(res1.PodJID, ":")[0]

	// Pod 2: on-demand co-tenant bin-packing onto same warm node
	pod2JSON := `{
		"metadata": {
			"name": "pod-od-2",
			"namespace": "default",
			"uid": "uid-od-2",
			"annotations": {
				"cloudvm.vk.io/instance-type": "g5.12xlarge",
				"cloudvm.vk.io/capacity-type": "on-demand",
				"cloudvm.vk.io/cpu": "4",
				"cloudvm.vk.io/memory": "16Gi"
			}
		},
		"spec": {
			"containers": [{"name": "app", "image": "pytorch"}]
		}
	}`
	req2 := httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader([]byte(pod2JSON)))
	w2 := httptest.NewRecorder()
	srv.handleCreate(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("pod 2 create failed: %d: %s", w2.Code, w2.Body.String())
	}
	var res2 CreateStruct
	_ = json.NewDecoder(w2.Body).Decode(&res2)
	instID2 := strings.Split(res2.PodJID, ":")[0]

	if instID != instID2 {
		t.Fatalf("expected pod 2 to bin-pack on warm node %s, got %s", instID, instID2)
	}

	// Verify co-tenant instance metadata is fully populated
	srv.mu.RLock()
	inst2 := srv.instances["uid-od-2"]
	srv.mu.RUnlock()
	if inst2 == nil {
		t.Fatalf("expected instance entry for uid-od-2")
	}
	if inst2.Metadata["marketType"] != "on-demand" {
		t.Fatalf("expected marketType on-demand, got %s", inst2.Metadata["marketType"])
	}
	if inst2.Metadata["podUID"] != "uid-od-2" {
		t.Fatalf("expected podUID uid-od-2, got %s", inst2.Metadata["podUID"])
	}
	if inst2.Metadata["podName"] != "pod-od-2" {
		t.Fatalf("expected podName pod-od-2, got %s", inst2.Metadata["podName"])
	}

	// Verify GetActivePods does NOT produce duplicate namespace keys
	activePods, err := srv.GetActivePods(context.Background())
	if err != nil {
		t.Fatalf("GetActivePods failed: %v", err)
	}
	if _, hasJunkKey := activePods["default/pod-od-2"]; hasJunkKey {
		t.Fatalf("GetActivePods contained duplicate namespace key default/pod-od-2")
	}
	if activePods["uid-od-1"] != instID || activePods["uid-od-2"] != instID {
		t.Fatalf("unexpected activePods mapping: %+v", activePods)
	}

	// Terminate VM in cloud inventory (simulating lost on-demand VM)
	_ = mock.Terminate(context.Background(), instID)

	reconciler := NewReconciler(mock, srv, mock, "cluster-ondemand", 10*time.Minute)
	reconciler.SetLaunchGracePeriod(0)
	result, err := reconciler.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if len(result.EvictedPods) != 2 {
		t.Fatalf("expected 2 evicted pods, got %d: %v", len(result.EvictedPods), result.EvictedPods)
	}

	// Verify both pods were recorded with reason NodeLost (NOT SpotInterruption)
	rec1 := srv.completedStore.Get("uid-od-1")
	if rec1 == nil || rec1.Reason != "NodeLost" {
		t.Fatalf("expected pod 1 reason NodeLost, got %+v", rec1)
	}
	rec2 := srv.completedStore.Get("uid-od-2")
	if rec2 == nil || rec2.Reason != "NodeLost" {
		t.Fatalf("expected pod 2 reason NodeLost, got %+v", rec2)
	}

	// Verify no junk record was written under namespace key
	if junk := srv.completedStore.Get("default/pod-od-2"); junk != nil {
		t.Fatalf("junk record found under namespace key: %+v", junk)
	}

	// Idempotent second pass
	result2, err := reconciler.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("second reconcile failed: %v", err)
	}
	if len(result2.EvictedPods) != 0 {
		t.Fatalf("expected 0 evicted pods on second pass, got %d", len(result2.EvictedPods))
	}
}

func TestServer_DeletedPodVMWithFailedTerminationIsTerminatedAsZombie(t *testing.T) {
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))
	t.Setenv("TOMBSTONE_STORE_PATH", filepath.Join(t.TempDir(), "tombstones.json"))

	mock := NewMockProvider()
	srv := NewServer(mock, "test-cluster")

	podJSON := `{
		"metadata": {
			"name": "u1",
			"namespace": "default",
			"uid": "uid-u1"
		},
		"spec": {
			"containers": [
				{
					"name": "c1",
					"resources": {
						"limits": {
							"cpu": "2",
							"memory": "4Gi"
						}
					}
				}
			]
		}
	}`

	// Create pod u1
	req := httptest.NewRequest(http.MethodPost, "/create", bytes.NewBufferString(podJSON))
	rec := httptest.NewRecorder()
	srv.handleCreate(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("create failed with code %d: %s", rec.Code, rec.Body.String())
	}

	// Verify u1 is active
	active, err := srv.GetActivePods(context.Background())
	if err != nil || active["uid-u1"] == "" {
		t.Fatalf("expected u1 active, got: %v", active)
	}
	instID := active["uid-u1"]

	// Simulate provider Terminate failure (e.g. EC2 throttling or permission denied)
	mock.SetTerminateFn(func(_ context.Context, id string) error {
		if id == instID {
			return fmt.Errorf("API throttled: unable to terminate")
		}
		return nil
	})

	// Delete pod u1
	delReq := httptest.NewRequest(http.MethodPost, "/delete", bytes.NewBufferString(podJSON))
	delRec := httptest.NewRecorder()
	srv.handleDelete(delRec, delReq)
	if delRec.Code != http.StatusOK {
		t.Fatalf("delete returned %d", delRec.Code)
	}

	// Verify instance is still running in cloud inventory (because termination failed)
	activeInsts, err := mock.ListActiveInstances(context.Background(), "test-cluster")
	if err != nil || len(activeInsts) != 1 || activeInsts[0].ID != instID {
		t.Fatalf("expected instance %s still in cloud inventory, got: %v", instID, activeInsts)
	}

	// Now provider termination succeeds
	mock.SetTerminateFn(nil)

	// Run reconciler with 0 grace period
	reconciler := NewReconciler(mock, srv, mock, "test-cluster", 0)
	result, err := reconciler.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("reconciler failed: %v", err)
	}

	// Instance MUST be terminated as a zombie!
	if len(result.TerminatedZombies) != 1 || result.TerminatedZombies[0] != instID {
		t.Fatalf("expected %s terminated as zombie, got: %v", instID, result.TerminatedZombies)
	}

	// Pod MUST NOT reappear in active pods or be re-adopted!
	activeAfter, _ := srv.GetActivePods(context.Background())
	if _, exists := activeAfter["uid-u1"]; exists {
		t.Fatalf("expected uid-u1 NOT in active pods after zombie termination, got: %v", activeAfter)
	}
}

func TestNodePool_AdoptedVMIsCordonedAndRejectsNewPodPacking(t *testing.T) {
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))
	t.Setenv("TOMBSTONE_STORE_PATH", filepath.Join(t.TempDir(), "tombstones.json"))

	mock := NewMockProvider()
	srv := NewServer(mock, "test-cluster")

	// Set up runner token in tokenStore
	if err := srv.tokenStore.Set("i-adopted-01", "adopted-secret-token"); err != nil {
		t.Fatalf("failed to store token: %v", err)
	}

	inst := &Instance{
		ID:           "i-adopted-01",
		Provider:     ProviderMock,
		Region:       "us-east-1",
		InstanceType: "g5.12xlarge",
		IP:           "10.0.1.50",
		State:        StateRunning,
		CreatedAt:    time.Now().Add(-5 * time.Minute),
		Metadata: map[string]string{
			"podUID":     "uid-adopted-1",
			"podName":    "pod-adopted-1",
			"namespace":  "default",
			"marketType": "on-demand",
		},
	}

	// InterLink polls status for pod uid-adopted-1, providing active evidence
	statusBody := `[{"pod":{"metadata":{"name":"pod-adopted-1","namespace":"default","uid":"uid-adopted-1"}}}]`
	reqStatus := httptest.NewRequest(http.MethodPost, "/status", bytes.NewBufferString(statusBody))
	srv.handleStatus(httptest.NewRecorder(), reqStatus)

	// Adopt instance
	ctx := context.Background()
	adopted := srv.AdoptInstance(ctx, inst)
	if !adopted {
		t.Fatalf("expected AdoptInstance to succeed")
	}

	// 1. Verify adopted node is cordoned
	node, alloc, found := srv.nodePool.FindPodNode("uid-adopted-1")
	if !found {
		t.Fatalf("expected node found in nodePool for uid-adopted-1")
	}
	if !node.Cordoned {
		t.Fatalf("CRITICAL: adopted node must be cordoned to prevent packing collisions")
	}
	if alloc.PodUID != "uid-adopted-1" {
		t.Fatalf("expected allocation reconstructed for uid-adopted-1")
	}
	if node.RunnerAuthToken != "adopted-secret-token" {
		t.Fatalf("expected runner token loaded from tokenStore, got %q", node.RunnerAuthToken)
	}

	// 2. Launch new pod u2 requesting resources
	pod2JSON := `{
		"metadata": {
			"name": "pod-new-2",
			"namespace": "default",
			"uid": "uid-new-2"
		},
		"spec": {
			"containers": [
				{
					"name": "c1",
					"resources": {
						"limits": {
							"cpu": "4",
							"memory": "8Gi",
							"nvidia.com/gpu": "1"
						}
					}
				}
			]
		}
	}`

	req := httptest.NewRequest(http.MethodPost, "/create", bytes.NewBufferString(pod2JSON))
	rec := httptest.NewRecorder()
	srv.handleCreate(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("create for pod 2 failed: %s", rec.Body.String())
	}

	// Verify pod 2 did NOT pack onto adopted node i-adopted-01
	node2, _, found2 := srv.nodePool.FindPodNode("uid-new-2")
	if !found2 {
		t.Fatalf("expected pod 2 registered in nodePool")
	}
	if node2.InstanceID == "i-adopted-01" {
		t.Fatalf("CRITICAL: new pod packed onto cordoned adopted VM %s!", node2.InstanceID)
	}
}

func TestServer_RestartWithPersistentTombstoneFile(t *testing.T) {
	tombPath := filepath.Join(t.TempDir(), "tombstones.json")
	tokPath := filepath.Join(t.TempDir(), "tokens.json")
	compPath := filepath.Join(t.TempDir(), "completed.json")

	t.Setenv("TOMBSTONE_STORE_PATH", tombPath)
	t.Setenv("TOKEN_STORE_PATH", tokPath)
	t.Setenv("COMPLETED_STORE_PATH", compPath)

	mock := NewMockProvider()
	srv1 := NewServer(mock, "test-cluster")

	podJSON := `{
		"metadata": {
			"name": "u1",
			"namespace": "default",
			"uid": "uid-u1"
		},
		"spec": {
			"containers": [
				{
					"name": "c1",
					"resources": {
						"limits": {
							"cpu": "2",
							"memory": "4Gi"
						}
					}
				}
			]
		}
	}`

	// Create pod u1 in Server 1
	req := httptest.NewRequest(http.MethodPost, "/create", bytes.NewBufferString(podJSON))
	rec := httptest.NewRecorder()
	srv1.handleCreate(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("create failed with code %d: %s", rec.Code, rec.Body.String())
	}

	active, err := srv1.GetActivePods(context.Background())
	if err != nil || active["uid-u1"] == "" {
		t.Fatalf("expected u1 active, got: %v", active)
	}
	instID := active["uid-u1"]

	// Fail provider Terminate
	mock.SetTerminateFn(func(_ context.Context, id string) error {
		if id == instID {
			return fmt.Errorf("API throttled: unable to terminate")
		}
		return nil
	})

	// Delete pod u1
	delReq := httptest.NewRequest(http.MethodPost, "/delete", bytes.NewBufferString(podJSON))
	delRec := httptest.NewRecorder()
	srv1.handleDelete(delRec, delReq)
	if delRec.Code != http.StatusOK {
		t.Fatalf("delete returned %d", delRec.Code)
	}

	// Verify instance remains in cloud inventory
	activeInsts, err := mock.ListActiveInstances(context.Background(), "test-cluster")
	if err != nil || len(activeInsts) != 1 || activeInsts[0].ID != instID {
		t.Fatalf("expected instance %s still in cloud inventory, got: %v", instID, activeInsts)
	}

	// Simulate daemon restart: create Server 2 with the same tombstone file path
	srv2 := NewServer(mock, "test-cluster")

	// Allow provider terminate to succeed now
	mock.SetTerminateFn(nil)

	// Run recovery on Server 2
	if err := srv2.RecoverActiveInstances(context.Background(), mock); err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	// CRITICAL CHECK: srv2 must NOT have recovered uid-u1
	activeAfterRecover, _ := srv2.GetActivePods(context.Background())
	if _, exists := activeAfterRecover["uid-u1"]; exists {
		t.Fatalf("CRITICAL: tombstoned pod uid-u1 was resurrected during recovery!")
	}

	// Run reconciler on Server 2
	reconciler := NewReconciler(mock, srv2, mock, "test-cluster", 0)
	res, err := reconciler.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("reconciler failed: %v", err)
	}

	// CRITICAL CHECK: instID must be terminated as zombie immediately!
	if len(res.TerminatedZombies) != 1 || res.TerminatedZombies[0] != instID {
		t.Fatalf("expected %s terminated as zombie on restart, got: %v", instID, res.TerminatedZombies)
	}
}

func TestNodePool_ConcurrentIsTombstonedAndRegisterNode(t *testing.T) {
	t.Setenv("TOMBSTONE_STORE_PATH", filepath.Join(t.TempDir(), "tombstones.json"))
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))
	mock := NewMockProvider()
	srv := NewServer(mock, "test-cluster")

	const iterations = 500
	var wg sync.WaitGroup
	wg.Add(3)

	inst := &Instance{
		ID:           "i-concurrent",
		Provider:     ProviderMock,
		Region:       "us-east-1",
		InstanceType: "g5.xlarge",
		Metadata: map[string]string{
			"podUID":  "uid-concurrent",
			"podName": "pod-concurrent",
		},
	}

	// Goroutine 1: IsTombstoned calls
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			_ = srv.IsTombstoned(inst)
		}
	}()

	// Goroutine 2: Node registration and allocations
	go func() {
		defer wg.Done()
		vec, _ := NewResourceVector("2", "4Gi", 0, 0)
		for i := 0; i < iterations; i++ {
			node := NewCloudVMNode("i-concurrent", ProviderMock, "us-east-1", "10.0.0.1", vec)
			srv.nodePool.RegisterNode(node)
			srv.nodePool.RegisterAndAllocate(node, vec, "uid-concurrent", "default", "pod-concurrent")
		}
	}()

	// Goroutine 3: Node unregistration
	go func() {
		defer wg.Done()
		for i := 0; i < iterations; i++ {
			srv.nodePool.UnregisterNode("i-concurrent")
		}
	}()

	wg.Wait()
}

type failingContainerRunner struct {
	mu         sync.RWMutex
	shouldFail bool
	containers []*ContainerStatusResult
}

func (f *failingContainerRunner) StartContainer(_ context.Context, _ *CloudVMNode, _ *StartContainerRequest) error {
	return nil
}
func (f *failingContainerRunner) StopContainer(_ context.Context, _ *CloudVMNode, _ string) error {
	return nil
}
func (f *failingContainerRunner) GetStatus(_ context.Context, _ *CloudVMNode, _ string) (*ContainerStatusResult, error) {
	return nil, nil
}
func (f *failingContainerRunner) ListContainers(_ context.Context, _ *CloudVMNode) ([]*ContainerStatusResult, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.shouldFail {
		return nil, fmt.Errorf("connection refused: in-VM runner unreachable during mesh initialization")
	}
	return f.containers, nil
}

func TestServer_RecoverActiveInstances_UnknownCoTenantsRetainedAndRetried(t *testing.T) {
	t.Setenv("TOMBSTONE_STORE_PATH", filepath.Join(t.TempDir(), "tombstones.json"))
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))

	mock := NewMockProvider()
	srv := NewServer(mock, "test-cluster")

	failingRunner := &failingContainerRunner{
		shouldFail: true, // Runner fails during initial recovery
		containers: []*ContainerStatusResult{
			{PodUID: "uid-u1", Running: true, Status: "running"},
			{PodUID: "uid-u2", Running: true, Status: "running"},
		},
	}
	srv.SetContainerRunner(failingRunner)

	// Launch VM hosting pod u1
	ctx := context.Background()
	inst, err := mock.Launch(ctx, &InstanceSpec{
		PodName:      "u1",
		PodNamespace: "default",
		PodUID:       "uid-u1",
		ClusterTag:   "test-cluster",
		Region:       "us-east-1",
		InstanceType: "g5.12xlarge",
	})
	if err != nil {
		t.Fatalf("launch failed: %v", err)
	}

	// 1. Recover active instances while runner is unreachable
	if err := srv.RecoverActiveInstances(ctx, mock); err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	// Verify node is marked with UnknownCoTenants
	if !srv.nodePool.HasUnknownCoTenants(inst.ID) {
		t.Fatalf("expected node %s marked with UnknownCoTenants when runner listing fails", inst.ID)
	}

	// 2. Delete pod u1 while UnknownCoTenants is true
	pod1JSON := `{"metadata":{"name":"u1","namespace":"default","uid":"uid-u1"}}`
	delReq := httptest.NewRequest(http.MethodPost, "/delete", bytes.NewBufferString(pod1JSON))
	delRec := httptest.NewRecorder()
	srv.handleDelete(delRec, delReq)
	if delRec.Code != http.StatusOK {
		t.Fatalf("delete returned %d", delRec.Code)
	}

	// CRITICAL CHECK: Shared VM must NOT be terminated while UnknownCoTenants is true!
	activeInsts, err := mock.ListActiveInstances(ctx, "test-cluster")
	if err != nil || len(activeInsts) != 1 || activeInsts[0].ID != inst.ID {
		t.Fatalf("CRITICAL BUG: shared VM was terminated while UnknownCoTenants was true!")
	}

	// 3. In-VM runner and WireGuard mesh come up
	failingRunner.mu.Lock()
	failingRunner.shouldFail = false
	failingRunner.mu.Unlock()

	// 4. Background Reconciler runs
	reconciler := NewReconciler(mock, srv, mock, "test-cluster", 0)
	_, err = reconciler.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	// Verify UnknownCoTenants was cleared and pod u2 was discovered
	if srv.nodePool.HasUnknownCoTenants(inst.ID) {
		t.Fatalf("expected UnknownCoTenants cleared after successful reconciliation")
	}
	if _, _, found := srv.nodePool.FindPodNode("uid-u2"); !found {
		t.Fatalf("expected co-tenant pod uid-u2 registered in nodePool after reconciliation")
	}

	// 5. Delete remaining pod u2 -> VM should now terminate cleanly
	pod2JSON := `{"metadata":{"name":"u2","namespace":"default","uid":"uid-u2"}}`
	delReq2 := httptest.NewRequest(http.MethodPost, "/delete", bytes.NewBufferString(pod2JSON))
	delRec2 := httptest.NewRecorder()
	srv.handleDelete(delRec2, delReq2)
	if delRec2.Code != http.StatusOK {
		t.Fatalf("delete u2 returned %d", delRec2.Code)
	}

	// Verify VM is now terminated
	activeAfterAll, err := mock.ListActiveInstances(ctx, "test-cluster")
	if err != nil || len(activeAfterAll) != 0 {
		t.Fatalf("expected VM terminated after last co-tenant was deleted, got: %v", activeAfterAll)
	}
}

func TestNodePool_UnknownCoTenantsProtectedFromIdleCollectorAndReconcilerWhileDown(t *testing.T) {
	t.Setenv("TOMBSTONE_STORE_PATH", filepath.Join(t.TempDir(), "tombstones.json"))
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))

	mock := NewMockProvider()
	srv := NewServer(mock, "test-cluster")

	failingRunner := &failingContainerRunner{
		shouldFail: true, // Runner remains down
	}
	srv.SetContainerRunner(failingRunner)

	ctx := context.Background()
	inst, err := mock.Launch(ctx, &InstanceSpec{
		PodName:      "u1",
		PodNamespace: "default",
		PodUID:       "uid-u1",
		ClusterTag:   "test-cluster",
		Region:       "us-east-1",
		InstanceType: "g5.12xlarge",
	})
	if err != nil {
		t.Fatalf("launch failed: %v", err)
	}

	// Recover active instances while runner is down -> flags UnknownCoTenants
	if err := srv.RecoverActiveInstances(ctx, mock); err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	// Delete pod u1
	pod1JSON := `{"metadata":{"name":"u1","namespace":"default","uid":"uid-u1"}}`
	delReq := httptest.NewRequest(http.MethodPost, "/delete", bytes.NewBufferString(pod1JSON))
	delRec := httptest.NewRecorder()
	srv.handleDelete(delRec, delReq)
	if delRec.Code != http.StatusOK {
		t.Fatalf("delete returned %d", delRec.Code)
	}

	// 1. Verify idle collector does NOT collect this node even with 0 active pods
	expired := srv.nodePool.CollectExpiredIdleNodes()
	if len(expired) != 0 {
		t.Fatalf("CRITICAL: idle collector collected node %s while UnknownCoTenants is true!", expired[0].InstanceID)
	}

	// 2. Verify IsTombstoned returns false while UnknownCoTenants is true
	if srv.IsTombstoned(inst) {
		t.Fatalf("CRITICAL: IsTombstoned returned true for node %s while UnknownCoTenants is true!", inst.ID)
	}

	// 3. Verify ReconcileOnce does NOT terminate this node as a zombie
	reconciler := NewReconciler(mock, srv, mock, "test-cluster", 0)
	res, err := reconciler.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if len(res.TerminatedZombies) != 0 {
		t.Fatalf("CRITICAL: reconciler killed VM %s as a zombie while runner was down!", res.TerminatedZombies)
	}

	// VM must still be running in cloud inventory
	activeVMs, err := mock.ListActiveInstances(ctx, "test-cluster")
	if err != nil || len(activeVMs) != 1 || activeVMs[0].ID != inst.ID {
		t.Fatalf("expected VM %s still active in cloud inventory, got: %v", inst.ID, activeVMs)
	}
}

func TestNodePool_UnknownCoTenantsExceededRetriesClearedForGC(t *testing.T) {
	t.Setenv("TOMBSTONE_STORE_PATH", filepath.Join(t.TempDir(), "tombstones.json"))
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))

	mock := NewMockProvider()
	srv := NewServer(mock, "test-cluster")

	failingRunner := &failingContainerRunner{
		shouldFail: true, // Runner permanently broken
	}
	srv.SetContainerRunner(failingRunner)

	ctx := context.Background()
	inst, err := mock.Launch(ctx, &InstanceSpec{
		PodName:      "u1",
		PodNamespace: "default",
		PodUID:       "uid-u1",
		ClusterTag:   "test-cluster",
		Region:       "us-east-1",
		InstanceType: "g5.12xlarge",
	})
	if err != nil {
		t.Fatalf("launch failed: %v", err)
	}

	if err := srv.RecoverActiveInstances(ctx, mock); err != nil {
		t.Fatalf("recovery failed: %v", err)
	}

	// Delete u1
	pod1JSON := `{"metadata":{"name":"u1","namespace":"default","uid":"uid-u1"}}`
	delReq := httptest.NewRequest(http.MethodPost, "/delete", bytes.NewBufferString(pod1JSON))
	srv.handleDelete(httptest.NewRecorder(), delReq)

	// Simulate retries up to MaxUnknownCoTenantRetries
	reconciler := NewReconciler(mock, srv, mock, "test-cluster", 0)
	for i := 0; i < MaxUnknownCoTenantRetries-1; i++ {
		_, _ = reconciler.ReconcileOnce(ctx)
		if !srv.nodePool.HasUnknownCoTenants(inst.ID) {
			t.Fatalf("expected UnknownCoTenants still true on retry %d", i+1)
		}
	}

	// Final retry: limit exceeded -> flag cleared -> VM collected as zombie
	res, err := reconciler.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("reconciler failed: %v", err)
	}
	if srv.nodePool.HasUnknownCoTenants(inst.ID) {
		t.Fatalf("expected UnknownCoTenants cleared after exceeding retry limit")
	}
	if len(res.TerminatedZombies) != 1 || res.TerminatedZombies[0] != inst.ID {
		t.Fatalf("expected VM %s terminated as zombie after retry limit exceeded, got: %v", inst.ID, res.TerminatedZombies)
	}
}
