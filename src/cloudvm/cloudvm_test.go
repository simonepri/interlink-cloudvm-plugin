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
)

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
