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
