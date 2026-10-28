package main

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
)

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
	_, _, err = allocate(server.NodePool(), req, "us-east-1", "new-pod", "custom-ns")
	if err == nil {
		t.Fatalf("expected Allocate to skip cordoned recovered node")
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

func (f *failingContainerRunner) GetHostStats(_ context.Context, _ *CloudVMNode) (*HostStatsResult, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if f.shouldFail {
		return nil, fmt.Errorf("connection refused: in-VM runner unreachable during mesh initialization")
	}
	return &HostStatsResult{}, nil
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

	reconciler := NewReconciler(mock, srv, mock, "test-cluster", 0)
	// First reconcile: the node is flagged for far less than the timeout, so it stays flagged however slow the host is.
	_, _ = reconciler.ReconcileOnce(ctx)
	if !srv.nodePool.HasUnknownCoTenants(inst.ID) {
		t.Fatalf("expected UnknownCoTenants still true before timeout")
	}

	// Move the flagging time past the timeout instead of sleeping, so the test does not depend on the clock rate.
	srv.nodePool.mu.Lock()
	srv.nodePool.nodes[inst.ID].UnknownSince = time.Now().Add(-2 * UnknownCoTenantTimeout)
	srv.nodePool.mu.Unlock()

	// Reconcile after timeout: flag cleared -> VM collected as zombie
	res, err := reconciler.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("reconciler failed: %v", err)
	}
	if srv.nodePool.HasUnknownCoTenants(inst.ID) {
		t.Fatalf("expected UnknownCoTenants cleared after timeout elapsed")
	}
	if len(res.TerminatedZombies) != 1 || res.TerminatedZombies[0] != inst.ID {
		t.Fatalf("expected VM %s terminated as zombie after retry limit exceeded, got: %v", inst.ID, res.TerminatedZombies)
	}
}

func TestNodePool_AdoptedVMWithRunnerUnreachableFlagsUnknownCoTenantsAndProtectsCoTenants(t *testing.T) {
	t.Setenv("TOMBSTONE_STORE_PATH", filepath.Join(t.TempDir(), "tombstones.json"))
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))

	mock := NewMockProvider()
	srv := NewServer(mock, "test-cluster")

	failingRunner := &failingContainerRunner{
		shouldFail: true, // Runner unreachable during adoption
		containers: []*ContainerStatusResult{
			{PodUID: "uid-u1", Running: true, Status: "running"},
			{PodUID: "uid-u2", Running: true, Status: "running"},
		},
	}
	srv.SetContainerRunner(failingRunner)

	inst := &Instance{
		ID:           "i-adopted-shared",
		Provider:     ProviderMock,
		Region:       "us-east-1",
		InstanceType: "g5.12xlarge",
		IP:           "10.0.1.60",
		State:        StateRunning,
		CreatedAt:    time.Now().Add(-5 * time.Minute),
		Metadata: map[string]string{
			"podUID":     "uid-u1",
			"podName":    "u1",
			"namespace":  "default",
			"marketType": "on-demand",
			"clusterTag": "test-cluster",
		},
	}
	// Track instance in mock provider
	mock.mu.Lock()
	mock.instances["i-adopted-shared"] = inst
	mock.mu.Unlock()

	// 1. InterLink sends /status for u1 (providing active evidence)
	statusBody := `[{"pod":{"metadata":{"name":"u1","namespace":"default","uid":"uid-u1"}}}]`
	reqStatus := httptest.NewRequest(http.MethodPost, "/status", bytes.NewBufferString(statusBody))
	srv.handleStatus(httptest.NewRecorder(), reqStatus)

	// 2. Adopt instance while runner is unreachable
	ctx := context.Background()
	adopted := srv.AdoptInstance(ctx, inst)
	if !adopted {
		t.Fatalf("expected AdoptInstance to succeed")
	}

	// CRITICAL CHECK: adopted node must be marked with UnknownCoTenants
	if !srv.nodePool.HasUnknownCoTenants("i-adopted-shared") {
		t.Fatalf("CRITICAL: adopted node must be flagged with UnknownCoTenants when runner listing fails")
	}

	// 3. Delete u1 while UnknownCoTenants is true
	pod1JSON := `{"metadata":{"name":"u1","namespace":"default","uid":"uid-u1"}}`
	delReq := httptest.NewRequest(http.MethodPost, "/delete", bytes.NewBufferString(pod1JSON))
	srv.handleDelete(httptest.NewRecorder(), delReq)

	// Verify VM is NOT terminated by /delete
	activeVMs, err := mock.ListActiveInstances(ctx, "test-cluster")
	if err != nil || len(activeVMs) != 1 || activeVMs[0].ID != "i-adopted-shared" {
		t.Fatalf("CRITICAL BUG: adopted shared VM was killed by /delete while UnknownCoTenants was true!")
	}

	// 4. Verify idle collector does NOT kill it
	expired := srv.nodePool.CollectExpiredIdleNodes()
	if len(expired) != 0 {
		t.Fatalf("CRITICAL BUG: idle collector killed adopted node while UnknownCoTenants was true!")
	}

	// 5. Verify reconciler does NOT kill it as a zombie
	reconciler := NewReconciler(mock, srv, mock, "test-cluster", 0)
	res, err := reconciler.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}
	if len(res.TerminatedZombies) != 0 {
		t.Fatalf("CRITICAL BUG: reconciler killed adopted VM as a zombie while UnknownCoTenants was true: %v", res.TerminatedZombies)
	}

	// 6. Runner comes back up
	failingRunner.mu.Lock()
	failingRunner.shouldFail = false
	failingRunner.mu.Unlock()

	// 7. Background reconciler runs and discovers u2
	_, err = reconciler.ReconcileOnce(ctx)
	if err != nil {
		t.Fatalf("reconcile failed: %v", err)
	}

	if srv.nodePool.HasUnknownCoTenants("i-adopted-shared") {
		t.Fatalf("expected UnknownCoTenants cleared after runner reconnected")
	}
	if _, _, found := srv.nodePool.FindPodNode("uid-u2"); !found {
		t.Fatalf("expected co-tenant pod uid-u2 registered in nodePool after reconciliation")
	}

	// 8. Delete remaining pod u2 -> VM should now terminate cleanly
	pod2JSON := `{"metadata":{"name":"u2","namespace":"default","uid":"uid-u2"}}`
	delReq2 := httptest.NewRequest(http.MethodPost, "/delete", bytes.NewBufferString(pod2JSON))
	srv.handleDelete(httptest.NewRecorder(), delReq2)

	activeAfterAll, err := mock.ListActiveInstances(ctx, "test-cluster")
	if err != nil || len(activeAfterAll) != 0 {
		t.Fatalf("expected VM terminated after last co-tenant was deleted, got: %v", activeAfterAll)
	}
}
