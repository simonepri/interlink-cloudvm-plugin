package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

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

func TestReconciler_TerminatesStoppedVM(t *testing.T) {
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))

	mock := NewMockProvider()
	srv := NewServer(mock, "cluster-stop")
	body, _ := json.Marshal(&Pod{
		Metadata: PodMeta{Name: "exited-worker", Namespace: "default", UID: "uid-stop-1"},
		Spec:     PodSpecWrapper{Containers: []ContainerSpec{{Name: "main", Image: "alpine"}}},
	})
	w := httptest.NewRecorder()
	srv.handleCreate(w, httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(body)))
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK from create, got %d: %s", w.Code, w.Body.String())
	}
	var res CreateStruct
	_ = json.NewDecoder(w.Body).Decode(&res)
	instID := strings.Split(res.PodJID, ":")[0]

	// The guest shuts down: the provider reports the VM stopped, but it still exists.
	mock.mu.Lock()
	mock.instances[instID].State = StateTerminated
	mock.mu.Unlock()
	var terminated []string
	mock.SetTerminateFn(func(_ context.Context, id string) error {
		terminated = append(terminated, id)
		return nil
	})

	reconciler := NewReconciler(mock, srv, mock, "cluster-stop", 10*time.Minute)
	reconciler.SetLaunchGracePeriod(0)
	result, err := reconciler.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("reconciler failed: %v", err)
	}
	if len(result.EvictedPods) != 1 {
		t.Fatalf("expected the pod on the stopped VM to be evicted, got %v", result.EvictedPods)
	}
	if len(terminated) != 1 || terminated[0] != instID {
		t.Fatalf("expected stopped VM %s to be terminated, got %v", instID, terminated)
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

type mockTrackerWrapper struct {
	activePods  map[string]string
	podRegions  map[string]string
	mu          sync.Mutex
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
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failedCalls = append(m.failedCalls, podName)
	return nil
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

func TestReconciler_KeepAlivePingsDispatchedToActiveNodes(t *testing.T) {
	var pingReceived atomic.Int32
	var lastAuthHeader string
	var lastPath string
	var mu sync.Mutex

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		lastAuthHeader = r.Header.Get("Authorization")
		lastPath = r.URL.Path
		mu.Unlock()
		pingReceived.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer ts.Close()

	hostPort := ts.Listener.Addr().String()

	np := NewNodePool(60 * time.Second)

	// Node 1: active, non-cordoned, with IP and token -> should receive ping
	node1 := NewCloudVMNode("i-active-1", ProviderAWS, "us-east-1", hostPort, ResourceVector{})
	node1.RunnerAuthToken = "secret-token-test-1" //nolint:gosec // test token
	np.RegisterNode(node1)

	// Node 2: cordoned node -> must NOT receive ping
	node2 := NewCloudVMNode("i-cordoned-2", ProviderAWS, "us-east-1", hostPort, ResourceVector{})
	node2.RunnerAuthToken = "secret-token-test-2" //nolint:gosec // test token
	node2.Cordoned = true
	np.RegisterNode(node2)

	// Node 3: active node without IP -> must NOT receive ping
	node3 := NewCloudVMNode("i-no-ip-3", ProviderAWS, "us-east-1", "", ResourceVector{})
	node3.RunnerAuthToken = "secret-token-test-3" //nolint:gosec // test token
	np.RegisterNode(node3)

	mock := NewMockProvider()
	reconciler := NewReconciler(mock, nil, mock, "test-cluster", 10*time.Minute)
	reconciler.SetNodePool(np)

	reconciler.PingActiveNodes(context.Background())

	if got := pingReceived.Load(); got != 1 {
		t.Fatalf("expected exactly 1 keep-alive ping, got %d", got)
	}

	mu.Lock()
	defer mu.Unlock()
	if lastPath != "/healthz" {
		t.Errorf("expected ping path /healthz, got %s", lastPath)
	}
	expectedAuth := "Bearer secret-token-test-1"
	if lastAuthHeader != expectedAuth {
		t.Errorf("expected auth header %q, got %q", expectedAuth, lastAuthHeader)
	}
}

func TestReconciler_KeepAlivePingsUnreachableNodeGraceful(_ *testing.T) {
	np := NewNodePool(60 * time.Second)

	// Node with unreachable IP
	node1 := NewCloudVMNode("i-unreachable-1", ProviderAWS, "us-east-1", "127.0.0.1:1", ResourceVector{})
	node1.RunnerAuthToken = "token-unreachable" //nolint:gosec // test token
	np.RegisterNode(node1)

	// Node that returns 500 Internal Server Error
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	node2 := NewCloudVMNode("i-err-2", ProviderAWS, "us-east-1", ts.Listener.Addr().String(), ResourceVector{})
	node2.RunnerAuthToken = "token-500" //nolint:gosec // test token
	np.RegisterNode(node2)

	mock := NewMockProvider()
	reconciler := NewReconciler(mock, nil, mock, "test-cluster", 10*time.Minute)
	reconciler.SetNodePool(np)
	reconciler.SetHTTPClient(&http.Client{Timeout: 100 * time.Millisecond})

	// Must not crash or panic
	reconciler.PingActiveNodes(context.Background())
}

func TestReconciler_KeepAliveStartTickerLoop(t *testing.T) {
	var pingCount atomic.Int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		pingCount.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer ts.Close()

	np := NewNodePool(60 * time.Second)
	node := NewCloudVMNode("i-ticker-1", ProviderAWS, "us-east-1", ts.Listener.Addr().String(), ResourceVector{})
	node.RunnerAuthToken = "test-token" //nolint:gosec // test token
	np.RegisterNode(node)

	mock := NewMockProvider()
	reconciler := NewReconciler(mock, nil, mock, "test-cluster", 10*time.Minute)
	reconciler.SetNodePool(np)
	reconciler.SetPingInterval(25 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})

	go func() {
		reconciler.Start(ctx)
		close(done)
	}()

	// Wait for at least 2 pings (initial + 1 ticker tick)
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if pingCount.Load() >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	cancel()

	select {
	case <-done:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("reconciler.Start did not terminate after context cancellation")
	}

	if got := pingCount.Load(); got < 2 {
		t.Fatalf("expected at least 2 pings in ticker loop, got %d", got)
	}
}

func TestReconciler_KeepAlivePingsSkipBusyNodes(t *testing.T) {
	var pingReceivedTokens sync.Map
	var pingCount atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		token := strings.TrimPrefix(auth, "Bearer ")
		pingReceivedTokens.Store(token, true)
		pingCount.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer ts.Close()

	hostPort := ts.Listener.Addr().String()
	np := NewNodePool(60 * time.Second)

	// Node 1: Idle node (0 allocated pods) -> MUST be pinged
	nodeIdle := NewCloudVMNode("i-idle-1", ProviderAWS, "us-east-1", hostPort, ResourceVector{})
	nodeIdle.RunnerAuthToken = "token-idle-1" //nolint:gosec // test token
	np.RegisterNode(nodeIdle)

	// Node 2: Busy node with AllocatedPods > 0 -> MUST be skipped
	nodeBusyAlloc := NewCloudVMNode("i-busy-alloc-2", ProviderAWS, "us-east-1", hostPort, ResourceVector{})
	nodeBusyAlloc.RunnerAuthToken = "token-busy-alloc" //nolint:gosec // test token
	nodeBusyAlloc.AllocatedPods = 1
	np.RegisterNode(nodeBusyAlloc)

	// Node 3: Busy node with Pods slice non-empty -> MUST be skipped
	nodeBusyPods := NewCloudVMNode("i-busy-pods-3", ProviderAWS, "us-east-1", hostPort, ResourceVector{})
	nodeBusyPods.RunnerAuthToken = "token-busy-pods" //nolint:gosec // test token
	nodeBusyPods.Pods = []string{"pod-test-1"}
	np.RegisterNode(nodeBusyPods)

	// Node 4: Busy node with active assigned pod -> MUST be skipped
	nodeBusyActive := NewCloudVMNode("i-busy-active-4", ProviderAWS, "us-east-1", hostPort, ResourceVector{})
	nodeBusyActive.RunnerAuthToken = "token-busy-active" //nolint:gosec // test token
	nodeBusyActive.AssignPod(&PodAllocation{
		PodUID: "pod-uid-4",
	})
	np.RegisterNode(nodeBusyActive)

	mock := NewMockProvider()
	reconciler := NewReconciler(mock, nil, mock, "test-cluster", 10*time.Minute)
	reconciler.SetNodePool(np)
	reconciler.SetPingJitter(0)

	reconciler.PingActiveNodes(context.Background())

	if got := pingCount.Load(); got != 1 {
		t.Fatalf("expected exactly 1 ping to idle node, got %d", got)
	}

	if _, ok := pingReceivedTokens.Load("token-idle-1"); !ok {
		t.Errorf("expected idle node to receive ping")
	}
	if _, ok := pingReceivedTokens.Load("token-busy-alloc"); ok {
		t.Errorf("expected busy node with AllocatedPods > 0 to be skipped")
	}
	if _, ok := pingReceivedTokens.Load("token-busy-pods"); ok {
		t.Errorf("expected busy node with len(Pods) > 0 to be skipped")
	}
	if _, ok := pingReceivedTokens.Load("token-busy-active"); ok {
		t.Errorf("expected busy node with active assigned pod to be skipped")
	}
}

func TestReconciler_KeepAlivePingsConcurrencyClamped(t *testing.T) {
	var currentConcurrent atomic.Int32
	var peakConcurrent atomic.Int32
	var totalPings atomic.Int32

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		curr := currentConcurrent.Add(1)
		defer currentConcurrent.Add(-1)

		for {
			peak := peakConcurrent.Load()
			if curr <= peak || peakConcurrent.CompareAndSwap(peak, curr) {
				break
			}
		}

		totalPings.Add(1)
		time.Sleep(20 * time.Millisecond)

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer ts.Close()

	hostPort := ts.Listener.Addr().String()

	t.Run("DefaultBound20", func(t *testing.T) {
		np := NewNodePool(60 * time.Second)
		totalNodes := 50
		for i := 0; i < totalNodes; i++ {
			node := NewCloudVMNode(fmt.Sprintf("i-node-%d", i), ProviderAWS, "us-east-1", hostPort, ResourceVector{})
			node.RunnerAuthToken = fmt.Sprintf("token-%d", i) //nolint:gosec // test token
			np.RegisterNode(node)
		}

		mock := NewMockProvider()
		reconciler := NewReconciler(mock, nil, mock, "test-cluster", 10*time.Minute)
		reconciler.SetNodePool(np)
		reconciler.SetPingJitter(0)

		currentConcurrent.Store(0)
		peakConcurrent.Store(0)
		totalPings.Store(0)

		reconciler.PingActiveNodes(context.Background())

		if got := totalPings.Load(); got != int32(totalNodes) {
			t.Fatalf("expected %d total pings, got %d", totalNodes, got)
		}

		peak := peakConcurrent.Load()
		if peak > 20 {
			t.Fatalf("peak concurrency %d exceeded semaphore bound of 20", peak)
		}
		if peak < 2 {
			t.Fatalf("expected concurrent pings, got peak concurrency %d", peak)
		}
	})

	t.Run("CustomBoundClamped", func(t *testing.T) {
		np := NewNodePool(60 * time.Second)
		totalNodes := 20
		for i := 0; i < totalNodes; i++ {
			node := NewCloudVMNode(fmt.Sprintf("i-custom-%d", i), ProviderAWS, "us-east-1", hostPort, ResourceVector{})
			node.RunnerAuthToken = fmt.Sprintf("token-custom-%d", i) //nolint:gosec // test token
			np.RegisterNode(node)
		}

		mock := NewMockProvider()
		reconciler := NewReconciler(mock, nil, mock, "test-cluster", 10*time.Minute)
		reconciler.SetNodePool(np)
		reconciler.SetPingConcurrency(5)
		reconciler.SetPingJitter(0)

		currentConcurrent.Store(0)
		peakConcurrent.Store(0)
		totalPings.Store(0)

		reconciler.PingActiveNodes(context.Background())

		if got := totalPings.Load(); got != int32(totalNodes) {
			t.Fatalf("expected %d total pings, got %d", totalNodes, got)
		}

		peak := peakConcurrent.Load()
		if peak > 5 {
			t.Fatalf("peak concurrency %d exceeded custom semaphore bound of 5", peak)
		}
		if peak < 2 {
			t.Fatalf("expected concurrent pings, got peak concurrency %d", peak)
		}
	})
}

func TestReconciler_ConcurrentWatchdogReaper(t *testing.T) {
	var concurrentScans atomic.Int32
	var peakScans atomic.Int32

	createMockClient := func(reg string, instID string) *mockEC2FleetClient {
		return &mockEC2FleetClient{
			describeInstancesFn: func(_ context.Context, input *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
				curr := concurrentScans.Add(1)
				defer concurrentScans.Add(-1)
				for {
					p := peakScans.Load()
					if curr <= p || peakScans.CompareAndSwap(p, curr) {
						break
					}
				}
				time.Sleep(20 * time.Millisecond)

				// Verify input filter by ClusterTag
				hasClusterTag := false
				for _, filter := range input.Filters {
					if *filter.Name == "tag:ClusterTag" && len(filter.Values) > 0 && filter.Values[0] == "multi-cluster" {
						hasClusterTag = true
						break
					}
				}
				if !hasClusterTag {
					return &ec2.DescribeInstancesOutput{}, nil
				}

				return &ec2.DescribeInstancesOutput{
					Reservations: []ec2types.Reservation{
						{
							Instances: []ec2types.Instance{
								{
									InstanceId:   aws.String(instID),
									InstanceType: ec2types.InstanceTypeG5Xlarge,
									State: &ec2types.InstanceState{
										Name: ec2types.InstanceStateNameRunning,
									},
									PrivateIpAddress: aws.String("10.0.1.5"),
									LaunchTime:       aws.Time(time.Now().Add(-15 * time.Minute)), // expired zombie
									Tags: []ec2types.Tag{
										{Key: aws.String("ClusterTag"), Value: aws.String("multi-cluster")},
										{Key: aws.String("PodName"), Value: aws.String("zombie-pod-" + reg)},
										{Key: aws.String("Namespace"), Value: aws.String("default")},
									},
								},
							},
						},
					},
				}, nil
			},
		}
	}

	awsProv := NewAWSProvider("us-east-1", "us-west-2", "eu-central-1")
	awsProv.SetRegionClient("us-east-1", createMockClient("us-east-1", "i-east"))
	awsProv.SetRegionClient("us-west-2", createMockClient("us-west-2", "i-west"))
	awsProv.SetRegionClient("eu-central-1", createMockClient("eu-central-1", "i-eu"))

	mockTracker := &mockTrackerWrapper{
		activePods: map[string]string{}, // All pods vanished, VMs are zombies
	}

	terminated := make(map[string]bool)
	var termMu sync.Mutex
	reconciler := NewReconciler(awsProv, mockTracker, nil, "multi-cluster", 5*time.Minute)
	reconciler.SetConfiguredRegions("us-east-1", "us-west-2", "eu-central-1")
	reconciler.SetOnTerminated(func(id string) {
		termMu.Lock()
		defer termMu.Unlock()
		terminated[id] = true
	})

	res, err := reconciler.ReapIdleWatchdog(context.Background())
	if err != nil {
		t.Fatalf("ReapIdleWatchdog failed: %v", err)
	}

	if len(res.TerminatedZombies) != 3 {
		t.Fatalf("expected 3 terminated zombies across 3 regions, got %d: %v", len(res.TerminatedZombies), res.TerminatedZombies)
	}

	if peak := peakScans.Load(); peak < 2 {
		t.Fatalf("expected concurrent region scans, got peak concurrency %d", peak)
	}
}
