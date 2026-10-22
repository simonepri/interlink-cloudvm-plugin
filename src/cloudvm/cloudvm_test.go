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
	"strings"
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
	if !bytes.Contains([]byte(script), []byte(`--gpus '"device=0,1"'`)) {
		t.Errorf("cloud-init missing discrete device slicing flags with CSV quotes")
	}
	if !bytes.Contains([]byte(script), []byte("--shm-size=16g")) {
		t.Errorf("cloud-init missing shared memory limit for PyTorch DataLoader")
	}
	if !bytes.Contains([]byte(script), []byte(`docker run -d --name="cloudvm-pod-uid-456"`)) {
		t.Errorf("cloud-init missing detached initial container launch with pod UID name, got script:\n%s", script)
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

	spec := PodToInstanceSpec(pod, "cluster-1")
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

	spec := PodToInstanceSpec(pod, "cluster-1")
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
