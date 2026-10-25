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

	"github.com/DATA-DOG/go-sqlmock"
	"k8s.io/apimachinery/pkg/api/resource"
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
	defer func() { _ = resp.Body.Close() }()

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
	defer func() { _ = resp.Body.Close() }()

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
			defer func() { _ = resp.Body.Close() }()
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

func TestServer_WarmNodeReuseAcrossPodDeletions(t *testing.T) {
	mock := NewMockProvider()
	srv := NewServer(mock, "cluster-warm-reuse")
	srv.SetIdleGracePeriod(100 * time.Millisecond)

	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// 1. Launch Pod 1
	p1Payload := `{
		"pod": {
			"metadata": {"name": "worker-1", "namespace": "default", "uid": "uid-p1"},
			"spec": {
				"containers": [{
					"name": "c1",
					"image": "rayproject/ray:2.38.0",
					"resources": {
						"requests": {"cpu": "4", "memory": "16Gi", "nvidia.com/gpu": "2"},
						"limits": {"nvidia.com/gpu": "2"}
					}
				}]
			}
		}
	}`

	resp1, err := http.Post(ts.URL+"/create", "application/json", strings.NewReader(p1Payload))
	if err != nil {
		t.Fatalf("POST /create for pod 1 failed: %v", err)
	}
	_ = resp1.Body.Close()

	if srv.NodePool().ActiveNodeCount() != 1 {
		t.Fatalf("expected 1 active node in pool, got %d", srv.NodePool().ActiveNodeCount())
	}
	node, _, found := srv.NodePool().FindPodNode("uid-p1")
	if !found || node == nil {
		t.Fatalf("pod 1 not found in node pool")
	}
	originalInstanceID := node.InstanceID

	// 2. Delete Pod 1
	delPayload1 := `{"pod": {"metadata": {"name": "worker-1", "namespace": "default", "uid": "uid-p1"}}}`
	delResp1, err := http.Post(ts.URL+"/delete", "application/json", strings.NewReader(delPayload1))
	if err != nil {
		t.Fatalf("POST /delete for pod 1 failed: %v", err)
	}
	_ = delResp1.Body.Close()

	// CRITICAL CHECK: Under IdleGracePeriod, the VM must NOT be terminated!
	if srv.NodePool().ActiveNodeCount() != 1 {
		t.Fatalf("REGRESSION: VM was eagerly terminated on delete instead of holding warm! ActiveNodeCount=%d", srv.NodePool().ActiveNodeCount())
	}
	if node.ActivePodCount() != 0 {
		t.Fatalf("expected 0 active pods on warm node, got %d", node.ActivePodCount())
	}
	if node.IdleSince == nil {
		t.Fatalf("expected IdleSince timestamp recorded on warm node")
	}
	if !node.Cordoned {
		t.Fatalf("expected warm node to be cordoned upon reaching 0 active pods")
	}

	// Explicitly uncordon node to re-enable pod allocation
	srv.NodePool().Uncordon(originalInstanceID)

	// 3. Launch Pod 2 within idle grace period -> Should reuse the existing warm VM!
	p2Payload := `{
		"pod": {
			"metadata": {"name": "worker-2", "namespace": "default", "uid": "uid-p2"},
			"spec": {
				"containers": [{
					"name": "c2",
					"image": "rayproject/ray:2.38.0",
					"resources": {
						"requests": {"cpu": "4", "memory": "16Gi", "nvidia.com/gpu": "2"},
						"limits": {"nvidia.com/gpu": "2"}
					}
				}]
			}
		}
	}`

	resp2, err := http.Post(ts.URL+"/create", "application/json", strings.NewReader(p2Payload))
	if err != nil {
		t.Fatalf("POST /create for pod 2 failed: %v", err)
	}
	_ = resp2.Body.Close()

	if srv.NodePool().ActiveNodeCount() != 1 {
		t.Fatalf("expected Pod 2 to reuse warm VM, but new node was launched! ActiveNodeCount=%d", srv.NodePool().ActiveNodeCount())
	}
	node2, _, found2 := srv.NodePool().FindPodNode("uid-p2")
	if !found2 || node2 == nil {
		t.Fatalf("pod 2 not found in node pool")
	}
	if node2.InstanceID != originalInstanceID {
		t.Fatalf("expected pod 2 to bin-pack onto original VM %s, got %s", originalInstanceID, node2.InstanceID)
	}
	if node2.IdleSince != nil {
		t.Fatalf("expected IdleSince reset to nil after assigning pod 2")
	}
	if node2.ActivePodCount() != 1 {
		t.Fatalf("expected 1 active pod on node, got %d", node2.ActivePodCount())
	}

	// 4. Delete Pod 2
	delPayload2 := `{"pod": {"metadata": {"name": "worker-2", "namespace": "default", "uid": "uid-p2"}}}`
	delResp2, err := http.Post(ts.URL+"/delete", "application/json", strings.NewReader(delPayload2))
	if err != nil {
		t.Fatalf("POST /delete for pod 2 failed: %v", err)
	}
	_ = delResp2.Body.Close()

	// Wait for idle grace period to expire
	time.Sleep(150 * time.Millisecond)

	// Collect and terminate expired idle node
	expired := srv.NodePool().CollectExpiredIdleNodes()
	if len(expired) != 1 || expired[0].InstanceID != originalInstanceID {
		t.Fatalf("expected 1 expired node (%s), got: %+v", originalInstanceID, expired)
	}
	srv.TerminateIdleNode(context.Background(), expired[0])

	if srv.NodePool().ActiveNodeCount() != 0 {
		t.Fatalf("expected 0 active nodes after idle termination, got %d", srv.NodePool().ActiveNodeCount())
	}
}

func TestServer_GenericDaemonOptionsPropagation(t *testing.T) {
	mockProv := NewMockProvider()
	srv := NewServer(mockProv, "test-cluster")

	srv.SetRegistryMirrors([]string{"mirror1.example.com", "mirror2.example.com"})
	srv.SetOtelEndpoint("otel-collector:4317")
	srv.SetEnableNPD(true)
	srv.SetCustomBootstrapScript("echo global-bootstrap")
	srv.SetRunnerConcurrency(16)

	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// 1. Pod without annotations should inherit server generic daemon options
	payload1 := `{"pod": {"metadata": {"name": "daemon-pod-1", "namespace": "default", "uid": "uid-daemon-1"}}}`
	resp1, err := http.Post(ts.URL+"/create", "application/json", strings.NewReader(payload1))
	if err != nil {
		t.Fatalf("POST /create failed: %v", err)
	}
	_ = resp1.Body.Close()

	spec1 := mockProv.LastSpec()
	if spec1 == nil {
		t.Fatalf("expected LastSpec to be populated")
	}
	if len(spec1.RegistryMirrors) != 2 || spec1.RegistryMirrors[0] != "mirror1.example.com" {
		t.Errorf("expected inherited RegistryMirrors, got: %v", spec1.RegistryMirrors)
	}
	if spec1.OtelEndpoint != "otel-collector:4317" {
		t.Errorf("expected inherited OtelEndpoint, got: %s", spec1.OtelEndpoint)
	}
	if !spec1.NodeProblemDetector {
		t.Errorf("expected NodeProblemDetector to be true")
	}
	if spec1.CustomBootstrapScript != "echo global-bootstrap" {
		t.Errorf("expected inherited CustomBootstrapScript, got: %s", spec1.CustomBootstrapScript)
	}
	if spec1.RunnerConcurrency != 16 {
		t.Errorf("expected inherited RunnerConcurrency 16, got: %d", spec1.RunnerConcurrency)
	}

	// 2. Pod with custom annotations should override server generic daemon options
	mockProv2 := NewMockProvider()
	srv2 := NewServer(mockProv2, "test-cluster")
	srv2.SetRegistryMirrors([]string{"mirror1.example.com", "mirror2.example.com"})
	srv2.SetOtelEndpoint("otel-collector:4317")
	srv2.SetEnableNPD(false)
	srv2.SetCustomBootstrapScript("echo global-bootstrap")
	srv2.SetRunnerConcurrency(16)

	ts2 := httptest.NewServer(srv2.Routes())
	defer ts2.Close()

	payload2 := `{
		"pod": {
			"metadata": {
				"name": "daemon-pod-2",
				"namespace": "default",
				"uid": "uid-daemon-2",
				"annotations": {
					"cloudvm.vk.io/registry-mirrors": "override.mirror.io,backup.mirror.io",
					"cloudvm.vk.io/otel-endpoint": "custom-otel:4317",
					"cloudvm.vk.io/enable-npd": "true",
					"cloudvm.vk.io/custom-bootstrap-script": "echo pod-bootstrap",
					"cloudvm.vk.io/runner-concurrency": "32"
				}
			}
		}
	}`
	resp2, err := http.Post(ts2.URL+"/create", "application/json", strings.NewReader(payload2))
	if err != nil {
		t.Fatalf("POST /create for pod 2 failed: %v", err)
	}
	_ = resp2.Body.Close()

	spec2 := mockProv2.LastSpec()
	if spec2 == nil {
		t.Fatalf("expected LastSpec to be populated for pod 2")
	}
	if len(spec2.RegistryMirrors) != 2 || spec2.RegistryMirrors[0] != "override.mirror.io" {
		t.Errorf("expected overridden RegistryMirrors, got: %v", spec2.RegistryMirrors)
	}
	if spec2.OtelEndpoint != "custom-otel:4317" {
		t.Errorf("expected overridden OtelEndpoint, got: %s", spec2.OtelEndpoint)
	}
	if spec2.CustomBootstrapScript != "echo pod-bootstrap" {
		t.Errorf("expected overridden CustomBootstrapScript, got: %s", spec2.CustomBootstrapScript)
	}
	if spec2.RunnerConcurrency != 32 {
		t.Errorf("expected overridden RunnerConcurrency 32, got: %d", spec2.RunnerConcurrency)
	}
}

func TestServer_RunnerConcurrency_EnvAndAnnotation(t *testing.T) {
	// 1. Set MAX_RUNNER_CONCURRENCY in environment and verify server reads and propagates to pod
	t.Setenv("MAX_RUNNER_CONCURRENCY", "8")
	mockProv := NewMockProvider()
	srv := NewServer(mockProv, "test-cluster")
	if srv.RunnerConcurrency() != 8 {
		t.Fatalf("expected runner concurrency 8 from env, got: %d", srv.RunnerConcurrency())
	}

	ts := httptest.NewServer(srv.Routes())
	defer ts.Close()

	// Pod without annotation inherits MAX_RUNNER_CONCURRENCY
	payload1 := `{"pod": {"metadata": {"name": "pod-default-concurrency", "namespace": "default", "uid": "uid-rc-1"}}}`
	resp1, err := http.Post(ts.URL+"/create", "application/json", strings.NewReader(payload1))
	if err != nil {
		t.Fatalf("POST /create failed: %v", err)
	}
	_ = resp1.Body.Close()

	spec1 := mockProv.LastSpec()
	if spec1 == nil {
		t.Fatalf("expected LastSpec to be populated")
	}
	if spec1.RunnerConcurrency != 8 {
		t.Errorf("expected spec.RunnerConcurrency to be 8, got: %d", spec1.RunnerConcurrency)
	}

	cfg1 := srv.BuildCloudInitConfig(spec1)
	script1, err := GenerateCloudInit(cfg1)
	if err != nil {
		t.Fatalf("failed to generate cloud-init: %v", err)
	}
	if !strings.Contains(script1, "RUNNER_CONCURRENCY = 8") {
		t.Errorf("expected cloud-init to contain RUNNER_CONCURRENCY = 8")
	}
	if !strings.Contains(script1, "DOCKER_SEMAPHORE = threading.Semaphore(RUNNER_CONCURRENCY)") {
		t.Errorf("expected cloud-init to contain DOCKER_SEMAPHORE = threading.Semaphore(RUNNER_CONCURRENCY)")
	}

	// 2. Pod with annotation "cloudvm.vk.io/runner-concurrency" overrides environment default
	mockProv2 := NewMockProvider()
	srv2 := NewServer(mockProv2, "test-cluster")
	ts2 := httptest.NewServer(srv2.Routes())
	defer ts2.Close()

	payload2 := `{
		"pod": {
			"metadata": {
				"name": "pod-annotated-concurrency",
				"namespace": "default",
				"uid": "uid-rc-2",
				"annotations": {
					"cloudvm.vk.io/runner-concurrency": "24"
				}
			}
		}
	}`
	resp2, err := http.Post(ts2.URL+"/create", "application/json", strings.NewReader(payload2))
	if err != nil {
		t.Fatalf("POST /create failed: %v", err)
	}
	_ = resp2.Body.Close()

	spec2 := mockProv2.LastSpec()
	if spec2 == nil {
		t.Fatalf("expected LastSpec to be populated")
	}
	if spec2.RunnerConcurrency != 24 {
		t.Errorf("expected spec.RunnerConcurrency to be 24, got: %d", spec2.RunnerConcurrency)
	}

	cfg2 := srv2.BuildCloudInitConfig(spec2)
	script2, err := GenerateCloudInit(cfg2)
	if err != nil {
		t.Fatalf("failed to generate cloud-init: %v", err)
	}
	if !strings.Contains(script2, "RUNNER_CONCURRENCY = 24") {
		t.Errorf("expected cloud-init to contain RUNNER_CONCURRENCY = 24")
	}
	if !strings.Contains(script2, "DOCKER_SEMAPHORE = threading.Semaphore(RUNNER_CONCURRENCY)") {
		t.Errorf("expected cloud-init to contain DOCKER_SEMAPHORE = threading.Semaphore(RUNNER_CONCURRENCY)")
	}

	// 3. When neither env var nor annotation is set, defaults to auto/cpu_count
	t.Setenv("MAX_RUNNER_CONCURRENCY", "")
	mockProv3 := NewMockProvider()
	srv3 := NewServer(mockProv3, "test-cluster")
	ts3 := httptest.NewServer(srv3.Routes())
	defer ts3.Close()

	payload3 := `{"pod": {"metadata": {"name": "pod-auto-concurrency", "namespace": "default", "uid": "uid-rc-3"}}}`
	resp3, err := http.Post(ts3.URL+"/create", "application/json", strings.NewReader(payload3))
	if err != nil {
		t.Fatalf("POST /create failed: %v", err)
	}
	_ = resp3.Body.Close()

	spec3 := mockProv3.LastSpec()
	if spec3 == nil {
		t.Fatalf("expected LastSpec to be populated")
	}
	if spec3.RunnerConcurrency != 0 {
		t.Errorf("expected spec.RunnerConcurrency to be 0 (auto), got: %d", spec3.RunnerConcurrency)
	}

	cfg3 := srv3.BuildCloudInitConfig(spec3)
	script3, err := GenerateCloudInit(cfg3)
	if err != nil {
		t.Fatalf("failed to generate cloud-init: %v", err)
	}
	if !strings.Contains(script3, "DEFAULT_RUNNER_CONCURRENCY = max(4, os.cpu_count() or 4)") {
		t.Errorf("expected cloud-init to contain DEFAULT_RUNNER_CONCURRENCY definition")
	}
	if !strings.Contains(script3, "RUNNER_CONCURRENCY = DEFAULT_RUNNER_CONCURRENCY") {
		t.Errorf("expected cloud-init to contain RUNNER_CONCURRENCY = DEFAULT_RUNNER_CONCURRENCY")
	}
	if !strings.Contains(script3, "DOCKER_SEMAPHORE = threading.Semaphore(RUNNER_CONCURRENCY)") {
		t.Errorf("expected cloud-init to contain DOCKER_SEMAPHORE = threading.Semaphore(RUNNER_CONCURRENCY)")
	}
}

func TestServer_PostgresHAStoresWiring(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("failed to create sqlmock: %v", err)
	}
	defer db.Close()

	pg := NewPGStoreWithDB(db)
	mockProv := NewMockProvider()
	srv := NewServer(mockProv, "test-cluster")

	// SetTombstoneStore populates initial tombstones cache via GetAll
	mock.ExpectQuery("SELECT id, created_at FROM cloudvm_tombstones").
		WillReturnRows(sqlmock.NewRows([]string{"id", "created_at"}))

	srv.SetTokenStore(pg.TokenStore())
	srv.SetCompletedStore(pg.CompletedStore())
	srv.SetTombstoneStore(pg.TombstoneStore())
	srv.SetRegionStore(pg.RegionStore())

	// Verify token operations through server wired store
	mock.ExpectExec("INSERT INTO cloudvm_tokens").
		WithArgs("inst-123", "tok-secret", sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := srv.tokenStore.Set("inst-123", "tok-secret"); err != nil {
		t.Fatalf("tokenStore.Set failed: %v", err)
	}

	// Verify tombstone operations through server wired store
	mock.ExpectQuery("SELECT 1 FROM cloudvm_tombstones WHERE id = \\$1").
		WithArgs("tomb-123").
		WillReturnRows(sqlmock.NewRows([]string{"1"}).AddRow(1))

	if !srv.tombstoneStore.Has("tomb-123") {
		t.Fatalf("expected tombstoneStore.Has to return true")
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unfulfilled sqlmock expectations: %v", err)
	}
}

func TestServerPinglinkAndHealthz(t *testing.T) {
	mock := NewMockProvider()
	server := NewServer(mock, "test-cluster")
	ts := httptest.NewServer(server.Routes())
	defer ts.Close()

	for _, path := range []string{"/healthz", "/pinglink"} {
		res, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatalf("failed to GET %s: %v", path, err)
		}
		if res.StatusCode != http.StatusOK {
			t.Errorf("GET %s status = %d, want 200", path, res.StatusCode)
		}
		_ = res.Body.Close()

		postRes, err := http.Post(ts.URL+path, "application/json", strings.NewReader("{}"))
		if err != nil {
			t.Fatalf("failed to POST %s: %v", path, err)
		}
		if postRes.StatusCode != http.StatusOK {
			t.Errorf("POST %s status = %d, want 200", path, postRes.StatusCode)
		}
		_ = postRes.Body.Close()
	}
}

func TestServer_RetryStartContainerDiscoversIP(t *testing.T) {
	mockProv := NewMockProvider()
	srv := NewServer(mockProv, "test-cluster")
	runner := NewMockContainerRunner()
	srv.SetContainerRunner(runner)

	node := NewCloudVMNode("i-retry-ip", ProviderMock, "us-west-2", "", ResourceVector{})
	srv.nodePool.RegisterNode(node)

	mockProv.mu.Lock()
	mockProv.instances["i-retry-ip"] = &Instance{
		ID:    "i-retry-ip",
		IP:    "10.136.5.99",
		State: StateRunning,
	}
	mockProv.mu.Unlock()

	req := &StartContainerRequest{
		PodUID:    "uid-retry-ip",
		Namespace: "default",
		PodName:   "test-retry",
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go srv.retryStartContainer(ctx, node, req)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if node.GetMeshIP() == "10.136.5.99" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if node.GetMeshIP() != "10.136.5.99" {
		t.Fatalf("expected node.MeshIP to be discovered as 10.136.5.99, got %q", node.GetMeshIP())
	}
}

type failOnceRunner struct {
	*MockContainerRunner
	failedOnce sync.Map
}

func (f *failOnceRunner) StartContainer(ctx context.Context, node *CloudVMNode, req *StartContainerRequest) error {
	if _, loaded := f.failedOnce.LoadOrStore(req.PodUID, true); !loaded {
		return fmt.Errorf("runner not ready yet")
	}
	return f.MockContainerRunner.StartContainer(ctx, node, req)
}

func TestServer_BinPackHitRetriesStartContainerWhenBooting(t *testing.T) {
	mockProv := NewMockProvider()
	srv := NewServer(mockProv, "test-cluster")
	runner := &failOnceRunner{MockContainerRunner: NewMockContainerRunner()}
	srv.SetContainerRunner(runner)

	nodeCap := ResourceVector{
		CPU:    resource.MustParse("8"),
		Memory: resource.MustParse("16Gi"),
	}
	warmNode := NewCloudVMNode("i-warm-slice", ProviderMock, "us-west-2", "10.136.1.10", nodeCap)
	warmNode.Spot = true
	srv.nodePool.RegisterNode(warmNode)

	podJSON := `{
		"metadata": {
			"name": "test-slice-pod",
			"namespace": "default",
			"uid": "uid-slice-retry",
			"annotations": {
				"cloudvm.vk.io/region": "us-west-2",
				"cloudvm.vk.io/instance-type": "c6i.xlarge",
				"cloudvm.vk.io/capacity-type": "spot",
				"cloudvm.vk.io/cpu": "1",
				"cloudvm.vk.io/memory": "2Gi"
			}
		},
		"spec": {
			"containers": [
				{
					"name": "worker",
					"image": "alpine:latest"
				}
			]
		}
	}`

	w := httptest.NewRecorder()
	r := httptest.NewRequest(http.MethodPost, "/create", strings.NewReader(podJSON))
	r.Header.Set("Content-Type", "application/json")

	srv.handleCreate(w, r)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from handleCreate, got %d: %s", w.Code, w.Body.String())
	}

	deadline := time.Now().Add(5 * time.Second)
	var succeeded bool
	for time.Now().Before(deadline) {
		if req := runner.GetStartRequest("uid-slice-retry"); req != nil {
			succeeded = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	if !succeeded {
		t.Fatalf("expected background retry worker to start container for sliced pod, but request was not found")
	}
}

func TestServer_HandleStatus_3000Pods(t *testing.T) {
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))

	mock := NewMockProvider()
	server := NewServer(mock, "test-cluster")
	ts := httptest.NewServer(server.Routes())
	defer ts.Close()

	// 1. Setup a running instance for pod 100
	server.mu.Lock()
	server.instances["uid-100"] = &Instance{
		ID:        "i-scale-100",
		Provider:  ProviderMock,
		Region:    "us-east-1",
		State:     StateRunning,
		CreatedAt: time.Now(),
	}
	// 2. Setup a completed pod record in completedPods for pod 200
	server.completedPods["uid-200"] = &ContainerTerminatedState{
		ExitCode:   0,
		Reason:     "Completed",
		Message:    "job completed successfully",
		FinishedAt: time.Now(),
	}
	server.mu.Unlock()

	// 3. Setup a completed store record for pod 300
	if server.completedStore != nil {
		_ = server.completedStore.Set("uid-300", &CompletedPodRecord{
			PodUID:     "uid-300",
			ExitCode:   0,
			Reason:     "Completed",
			FinishedAt: time.Now(),
		})
	}

	// 4. Construct a batch payload containing 3,000 pods with specs
	var buf bytes.Buffer
	buf.WriteString("[")
	for i := 0; i < 3000; i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		fmt.Fprintf(&buf, `{"pod":{"metadata":{"name":"scale-pod-%d","namespace":"default","uid":"uid-%d","labels":{"app":"scale","batch":"test"}},"spec":{"containers":[{"name":"worker","image":"ubuntu:24.04","resources":{"limits":{"cpu":"1","memory":"2Gi"}}}]}}}`, i, i)
	}
	buf.WriteString("]")

	resp, err := http.Post(ts.URL+"/status", "application/json", bytes.NewReader(buf.Bytes()))
	if err != nil {
		t.Fatalf("POST /status for 3,000 pods failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from /status, got %d", resp.StatusCode)
	}

	var statuses []PodStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&statuses); err != nil {
		t.Fatalf("failed to decode 3,000 pod status responses: %v", err)
	}

	if len(statuses) != 3000 {
		t.Fatalf("expected 3,000 status responses, got %d", len(statuses))
	}

	// Verify pod 100 is Running
	st100 := statuses[100]
	if len(st100.Containers) != 1 || st100.Containers[0].State.Running == nil || !st100.Containers[0].Ready {
		t.Errorf("expected pod 100 to be Running and Ready, got: %+v", st100)
	}

	// Verify pod 200 is Terminated (Completed) from server.completedPods
	st200 := statuses[200]
	if len(st200.Containers) != 1 || st200.Containers[0].State.Terminated == nil || st200.Containers[0].State.Terminated.Reason != "Completed" {
		t.Errorf("expected pod 200 to be Completed, got: %+v", st200)
	}

	// Verify pod 300 is Terminated (Completed) from completedStore
	st300 := statuses[300]
	if len(st300.Containers) != 1 || st300.Containers[0].State.Terminated == nil || st300.Containers[0].State.Terminated.Reason != "Completed" {
		t.Errorf("expected pod 300 to be Completed, got: %+v", st300)
	}

	// Verify pod 500 is Waiting (ContainerCreating)
	st500 := statuses[500]
	if len(st500.Containers) != 1 || st500.Containers[0].State.Waiting == nil || st500.Containers[0].State.Waiting.Reason != "ContainerCreating" {
		t.Errorf("expected pod 500 to be ContainerCreating, got: %+v", st500)
	}

	// Verify sightings map recorded all pod UIDs
	server.mu.RLock()
	seen100 := server.activeSightings["uid-100"]
	seen500 := server.activeSightings["uid-500"]
	server.mu.RUnlock()
	if seen100.IsZero() || seen500.IsZero() {
		t.Errorf("expected active sightings to be recorded for queried pods")
	}
}

func BenchmarkServer_HandleStatus_3000Pods(b *testing.B) {
	b.ReportAllocs()
	mock := NewMockProvider()
	server := NewServer(mock, "bench-cluster")
	ts := httptest.NewServer(server.Routes())
	defer ts.Close()

	var buf bytes.Buffer
	buf.WriteString("[")
	for i := 0; i < 3000; i++ {
		if i > 0 {
			buf.WriteString(",")
		}
		fmt.Fprintf(&buf, `{"pod":{"metadata":{"name":"scale-pod-%d","namespace":"default","uid":"uid-%d"}}}`, i, i)
	}
	buf.WriteString("]")
	payload := buf.Bytes()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := http.Post(ts.URL+"/status", "application/json", bytes.NewReader(payload))
		if err != nil {
			b.Fatalf("POST /status failed: %v", err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}
}

func TestServer_HandleStatus_NetworkMetrics(t *testing.T) {
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	mock := NewMockProvider()
	server := NewServer(mock, "test-cluster")
	ts := httptest.NewServer(server.Routes())
	defer ts.Close()

	runner := NewMockContainerRunner()
	runner.SetStatus("uid-net-pod", &ContainerStatusResult{
		Running:        true,
		Status:         "running",
		StartedAt:      time.Now(),
		NetworkRxBytes: 1048576,
		NetworkTxBytes: 2097152,
	})
	server.SetContainerRunner(runner)

	capVec, _ := NewResourceVector("4", "16Gi", 1, 0)
	node := NewCloudVMNode("i-net-node", ProviderAWS, "us-west-2", "100.64.0.50", capVec)
	node.InstanceType = "g5.xlarge"
	reqVec, _ := NewResourceVector("1", "4Gi", 1, 0)
	server.NodePool().RegisterAndAllocate(node, reqVec, "uid-net-pod", "default", "net-pod")

	server.mu.Lock()
	server.instances["uid-net-pod"] = &Instance{
		ID:           "i-net-node",
		Provider:     ProviderAWS,
		Region:       "us-west-2",
		InstanceType: "g5.xlarge",
		State:        StateRunning,
		CreatedAt:    time.Now(),
	}
	server.mu.Unlock()

	queryPayload := `[{"pod": {"metadata": {"name": "net-pod", "namespace": "default", "uid": "uid-net-pod"}}}]`
	resp, err := http.Post(ts.URL+"/status", "application/json", strings.NewReader(queryPayload))
	if err != nil {
		t.Fatalf("POST /status failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// Verify metrics were updated in GlobalMetrics
	rec := httptest.NewRecorder()
	GlobalMetrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := rec.Body.String()

	if !strings.Contains(body, "cloudvm_network_receive_bytes_total") {
		t.Errorf("expected cloudvm_network_receive_bytes_total in metrics")
	}
	if !strings.Contains(body, "cloudvm_network_transmit_bytes_total") {
		t.Errorf("expected cloudvm_network_transmit_bytes_total in metrics")
	}
}

func TestServer_HandleStatus_RunnerStartFailureIsTerminal(t *testing.T) {
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	server := NewServer(NewMockProvider(), "test-cluster")
	ts := httptest.NewServer(server.Routes())
	defer ts.Close()

	const startErr = `could not select device driver "" with capabilities: [[gpu]]`
	runner := NewMockContainerRunner()
	// A failed docker run leaves the container in "created"; the start failure must still end the pod.
	runner.SetStatus("uid-gpu-pod", &ContainerStatusResult{
		Status:  "created",
		Reason:  "StartError",
		Message: startErr,
	})
	server.SetContainerRunner(runner)

	capVec, _ := NewResourceVector("4", "16Gi", 1, 0)
	node := NewCloudVMNode("i-gpu-node", ProviderAWS, "us-west-2", "100.64.0.51", capVec)
	reqVec, _ := NewResourceVector("1", "4Gi", 1, 0)
	server.NodePool().RegisterAndAllocate(node, reqVec, "uid-gpu-pod", "default", "gpu-pod")

	server.mu.Lock()
	server.instances["uid-gpu-pod"] = &Instance{
		ID:        "i-gpu-node",
		Provider:  ProviderAWS,
		Region:    "us-west-2",
		State:     StateRunning,
		CreatedAt: time.Now(),
	}
	server.mu.Unlock()

	queryPayload := `[{"pod": {"metadata": {"name": "gpu-pod", "namespace": "default", "uid": "uid-gpu-pod"}}}]`
	resp, err := http.Post(ts.URL+"/status", "application/json", strings.NewReader(queryPayload))
	if err != nil {
		t.Fatalf("POST /status failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var statuses []PodStatusResponse
	if err := json.NewDecoder(resp.Body).Decode(&statuses); err != nil {
		t.Fatalf("decode status: %v", err)
	}
	if len(statuses) != 1 || len(statuses[0].Containers) != 1 {
		t.Fatalf("unexpected status payload: %+v", statuses)
	}
	term := statuses[0].Containers[0].State.Terminated
	if term == nil {
		t.Fatalf("expected terminated state for start failure, got %+v", statuses[0].Containers[0].State)
	}
	if term.Reason != "StartError" || term.Message != startErr || term.ExitCode == 0 {
		t.Fatalf("unexpected terminated state: %+v", term)
	}
}
