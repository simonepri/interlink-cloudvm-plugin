// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
)

// allocate bin-packs a pod into pool and returns its node and GPUs.
func allocate(pool *NodePool, req ResourceVector, region, podUID, namespace string) (*CloudVMNode, []int, error) {
	alloc, err := pool.AllocatePod(AllocationRequest{Resources: req, Region: region, PodUID: podUID, Namespace: namespace, PodName: podUID})
	if err != nil {
		return nil, nil, err
	}
	return alloc.Node, alloc.GPUs, nil
}

func TestNodePool_GPUSlicing(t *testing.T) {
	pool := NewNodePool(60 * time.Second)

	// Register an 8-GPU VM (e.g. g5.48xlarge or p4d.24xlarge)
	capacity := ResourceVector{
		CPU:    resource.MustParse("96"),
		Memory: resource.MustParse("384Gi"),
		GPUs:   8,
	}
	node := NewCloudVMNode("i-8gpu-node-01", ProviderAWS, "us-west-2", "10.42.0.10", capacity)
	node.Namespace = "default"
	pool.RegisterNode(node)

	if count := pool.ActiveNodeCount(); count != 1 {
		t.Fatalf("expected 1 active node, got %d", count)
	}

	// 1. First 4-GPU pod requests bin-packing
	pod1Req := ResourceVector{
		CPU:    resource.MustParse("32"),
		Memory: resource.MustParse("128Gi"),
		GPUs:   4,
	}
	allocatedNode, gpus1, err := allocate(pool, pod1Req, "us-west-2", "pod-uid-1", "default")
	if err != nil {
		t.Fatalf("expected pod 1 to allocate successfully: %v", err)
	}
	if allocatedNode.InstanceID != "i-8gpu-node-01" {
		t.Fatalf("expected node i-8gpu-node-01, got %s", allocatedNode.InstanceID)
	}
	expectedGPUs1 := []int{0, 1, 2, 3}
	if len(gpus1) != 4 || gpus1[0] != 0 || gpus1[1] != 1 || gpus1[2] != 2 || gpus1[3] != 3 {
		t.Fatalf("expected GPUs %v, got %v", expectedGPUs1, gpus1)
	}

	// 2. Second 4-GPU pod arrives - should pack into remaining 4 GPU slots on the same warm VM
	pod2Req := ResourceVector{
		CPU:    resource.MustParse("32"),
		Memory: resource.MustParse("128Gi"),
		GPUs:   4,
	}
	allocatedNode2, gpus2, err := allocate(pool, pod2Req, "us-west-2", "pod-uid-2", "default")
	if err != nil {
		t.Fatalf("expected pod 2 to bin-pack into warm node: %v", err)
	}
	if allocatedNode2.InstanceID != "i-8gpu-node-01" {
		t.Fatalf("expected node i-8gpu-node-01, got %s", allocatedNode2.InstanceID)
	}
	expectedGPUs2 := []int{4, 5, 6, 7}
	if len(gpus2) != 4 || gpus2[0] != 4 || gpus2[1] != 5 || gpus2[2] != 6 || gpus2[3] != 7 {
		t.Fatalf("expected GPUs %v, got %v", expectedGPUs2, gpus2)
	}

	// 3. Third 1-GPU pod arrives - must miss bin-pack because node has 0 GPUs left
	pod3Req := ResourceVector{
		CPU:    resource.MustParse("4"),
		Memory: resource.MustParse("16Gi"),
		GPUs:   1,
	}
	_, _, err = allocate(pool, pod3Req, "us-west-2", "pod-uid-3", "default")
	if err == nil {
		t.Fatalf("expected pod 3 allocation to fail due to GPU exhaustion")
	}
}

func TestNodePool_CPUMemoryFittingAndScoring(t *testing.T) {
	pool := NewNodePool(60 * time.Second)

	// Node 1: Small capacity
	node1 := NewCloudVMNode("i-cpu-small", ProviderAWS, "us-east-1", "10.42.0.1", ResourceVector{
		CPU:    resource.MustParse("8"),
		Memory: resource.MustParse("16Gi"),
	})
	// Node 2: Large capacity
	node2 := NewCloudVMNode("i-cpu-large", ProviderAWS, "us-east-1", "10.42.0.2", ResourceVector{
		CPU:    resource.MustParse("32"),
		Memory: resource.MustParse("64Gi"),
	})

	node1.Namespace = "prod"
	pool.RegisterNode(node1)
	node2.Namespace = "prod"
	pool.RegisterNode(node2)

	// Pod requesting 6 CPUs and 12GiB
	req := ResourceVector{
		CPU:    resource.MustParse("6"),
		Memory: resource.MustParse("12Gi"),
	}

	// NodeResourcesMostAllocated favors node1 (6/8 = 75%) over node2 (6/32 = 18.75%)
	targetNode, _, err := allocate(pool, req, "us-east-1", "pod-cpu-1", "prod")
	if err != nil {
		t.Fatalf("unexpected allocation error: %v", err)
	}
	if targetNode.InstanceID != "i-cpu-small" {
		t.Errorf("MostAllocated scoring should prefer tighter bin-pack on i-cpu-small, got %s", targetNode.InstanceID)
	}
}

func TestNodePool_PodReleaseAndIdleCooldown(t *testing.T) {
	pool := NewNodePool(100 * time.Millisecond)

	node := NewCloudVMNode("i-cooldown-node", ProviderAWS, "us-west-2", "10.42.0.5", ResourceVector{
		CPU:    resource.MustParse("16"),
		Memory: resource.MustParse("32Gi"),
		GPUs:   2,
	})
	node.Namespace = "default"
	pool.RegisterNode(node)

	// Allocate Pod A and Pod B
	req := ResourceVector{
		CPU:    resource.MustParse("4"),
		Memory: resource.MustParse("8Gi"),
		GPUs:   1,
	}
	_, _, _ = allocate(pool, req, "us-west-2", "pod-a", "default")
	_, _, _ = allocate(pool, req, "us-west-2", "pod-b", "default")

	// Release Pod A: Node still has Pod B running -> not idle
	pool.Release("pod-a")
	if node.IdleSince != nil {
		t.Errorf("node should not be idle while Pod B is still running")
	}
	if node.Cordoned {
		t.Errorf("node should not be cordoned while Pod B is still running")
	}

	// Release Pod B: Node has 0 active pods -> IdleSince recorded, and it stays allocatable for reuse
	pool.Release("pod-b")
	if node.IdleSince == nil {
		t.Fatalf("node should record IdleSince timestamp once all pods finish")
	}
	if node.Cordoned {
		t.Fatalf("idle node should stay allocatable during its grace period")
	}

	// Immediate collection should return empty because grace period (100ms) has not elapsed
	expired := pool.CollectExpiredIdleNodes()
	if len(expired) != 0 {
		t.Fatalf("expected 0 expired nodes immediately, got %d", len(expired))
	}

	// Wait for grace period to expire
	time.Sleep(150 * time.Millisecond)
	expired = pool.CollectExpiredIdleNodes()
	if len(expired) != 1 || expired[0].InstanceID != "i-cooldown-node" {
		t.Fatalf("expected i-cooldown-node to be expired and collected, got %+v", expired)
	}

	// Active node count should now be 0
	if pool.ActiveNodeCount() != 0 {
		t.Errorf("expected active node count 0 after cleanup, got %d", pool.ActiveNodeCount())
	}
}

func TestEndToEnd_Two4GPUPodsBinPackIntoOne8GPUVM(t *testing.T) {
	mock := NewMockProvider()
	server := NewServer(mock, "test-cluster")
	handler := server.Routes()

	make4GPUPod := func(name, uid string) []byte {
		pod := Pod{
			Metadata: PodMeta{
				Name:      name,
				Namespace: "default",
				UID:       uid,
				Annotations: map[string]string{
					"cloudvm.vk.io/capacity-type": "spot",
					"cloudvm.vk.io/region":        "us-east-1",
					"cloudvm.vk.io/instance-type": "g5.48xlarge", // 8-GPU shape
				},
			},
			Spec: PodSpecWrapper{
				Containers: []ContainerSpec{
					{
						Name:  "worker",
						Image: "rayproject/ray:2.44.0",
						Resources: ContainerResourceRequirements{
							Limits: map[string]string{
								"cpu":            "16",
								"memory":         "64Gi",
								"nvidia.com/gpu": "4",
							},
						},
					},
				},
			},
		}
		b, _ := json.Marshal(pod)
		return b
	}

	pod1Body := make4GPUPod("ray-worker-1", "uid-pod-1")
	pod2Body := make4GPUPod("ray-worker-2", "uid-pod-2")

	// 1. Create Pod 1 (4 GPUs on g5.48xlarge) -> Launches 1 VM (i-mock-0001)
	rec1 := httptest.NewRecorder()
	handler.ServeHTTP(rec1, httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(pod1Body)))
	if rec1.Code != http.StatusOK {
		t.Fatalf("pod 1 create failed: %d %s", rec1.Code, rec1.Body.String())
	}

	// 2. Create Pod 2 (4 GPUs on g5.48xlarge) -> Bin-packs into warm VM i-mock-0001!
	rec2 := httptest.NewRecorder()
	handler.ServeHTTP(rec2, httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(pod2Body)))
	if rec2.Code != http.StatusOK {
		t.Fatalf("pod 2 create failed: %d %s", rec2.Code, rec2.Body.String())
	}

	// Verify only 1 cloud VM was launched for both 4-GPU pods
	activeVMs, _ := mock.ListActiveInstances(context.Background(), "test-cluster")
	if len(activeVMs) != 1 {
		t.Fatalf("expected both 4-GPU pods to share 1 8-GPU VM, but %d VMs were launched", len(activeVMs))
	}

	// Verify GPU slot isolation: Pod 1 -> [0,1,2,3], Pod 2 -> [4,5,6,7]
	_, alloc1, ok1 := server.NodePool().FindPodNode("uid-pod-1")
	_, alloc2, ok2 := server.NodePool().FindPodNode("uid-pod-2")
	if !ok1 || !ok2 {
		t.Fatalf("expected both pods to be tracked in NodePool")
	}
	if len(alloc1.AssignedGPUs) != 4 || alloc1.AssignedGPUs[0] != 0 || alloc1.AssignedGPUs[3] != 3 {
		t.Errorf("unexpected GPUs for pod 1: %v", alloc1.AssignedGPUs)
	}
	if len(alloc2.AssignedGPUs) != 4 || alloc2.AssignedGPUs[0] != 4 || alloc2.AssignedGPUs[3] != 7 {
		t.Errorf("unexpected GPUs for pod 2: %v", alloc2.AssignedGPUs)
	}

	// 3. Delete Pod 1 -> Must release Pod 1's 4 GPUs WITHOUT terminating the VM for Pod 2
	recDel1 := httptest.NewRecorder()
	handler.ServeHTTP(recDel1, httptest.NewRequest(http.MethodPost, "/delete", bytes.NewReader(pod1Body)))
	if recDel1.Code != http.StatusOK {
		t.Fatalf("pod 1 delete failed: %d", recDel1.Code)
	}

	activeVMsAfterDel1, _ := mock.ListActiveInstances(context.Background(), "test-cluster")
	if len(activeVMsAfterDel1) != 1 {
		t.Fatalf("deleting co-tenant Pod 1 must NOT terminate shared VM while Pod 2 is still running; active VMs: %d", len(activeVMsAfterDel1))
	}

	// 3b. Run ReconcileOnce after Pod 1 was deleted while Pod 2 is still running -> VM must NOT be treated as a zombie
	reconciler := NewReconciler(mock, server, mock, "test-cluster", 0)
	res, err := reconciler.ReconcileOnce(context.Background())
	if err != nil {
		t.Fatalf("reconciler failed: %v", err)
	}
	if len(res.TerminatedZombies) != 0 {
		t.Fatalf("reconciler erroneously killed shared VM while Pod 2 was still running: %v", res.TerminatedZombies)
	}

	// 4. Delete Pod 2 the way the virtual kubelet does -> Last pod on VM finishes -> VM is terminated
	recDel2 := httptest.NewRecorder()
	handler.ServeHTTP(recDel2, httptest.NewRequest(http.MethodDelete, "/delete", bytes.NewReader(pod2Body)))
	if recDel2.Code != http.StatusOK {
		t.Fatalf("pod 2 delete failed: %d", recDel2.Code)
	}

	activeVMsAfterDel2, _ := mock.ListActiveInstances(context.Background(), "test-cluster")
	if len(activeVMsAfterDel2) != 0 {
		t.Fatalf("expected shared VM to be terminated after last pod exited, got %d active VMs", len(activeVMsAfterDel2))
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

func TestNodePool_MaxPodsPerNodeDensity(t *testing.T) {
	pool := NewNodePool(60 * time.Second)
	if pool.MaxPodsPerNode() != 100 {
		t.Fatalf("expected default MaxPodsPerNode to be 100, got %d", pool.MaxPodsPerNode())
	}

	// Set a tight limit to test density cap
	pool.SetMaxPodsPerNode(2)
	if pool.MaxPodsPerNode() != 2 {
		t.Fatalf("expected MaxPodsPerNode to be 2, got %d", pool.MaxPodsPerNode())
	}

	// Node 1 with abundant capacity
	capacity := ResourceVector{
		CPU:    resource.MustParse("64"),
		Memory: resource.MustParse("256Gi"),
	}
	node1 := NewCloudVMNode("i-density-node-01", ProviderAWS, "us-east-1", "10.42.0.11", capacity)
	node1.Namespace = "default"
	pool.RegisterNode(node1)

	req := ResourceVector{
		CPU:    resource.MustParse("1"),
		Memory: resource.MustParse("2Gi"),
	}

	// 1. Allocate Pod 1
	n1, _, err := allocate(pool, req, "us-east-1", "pod-d-1", "default")
	if err != nil || n1.InstanceID != "i-density-node-01" {
		t.Fatalf("failed to allocate pod 1: %v", err)
	}
	if node1.ActivePodCount() != 1 {
		t.Fatalf("expected 1 active pod on node 1, got %d", node1.ActivePodCount())
	}

	// 2. Allocate Pod 2 (reaches density limit of 2)
	n2, _, err := allocate(pool, req, "us-east-1", "pod-d-2", "default")
	if err != nil || n2.InstanceID != "i-density-node-01" {
		t.Fatalf("failed to allocate pod 2: %v", err)
	}
	if node1.ActivePodCount() != 2 {
		t.Fatalf("expected 2 active pods on node 1, got %d", node1.ActivePodCount())
	}

	// 3. Allocate Pod 3: must be rejected even though node 1 has plenty of CPU/memory
	_, _, err = allocate(pool, req, "us-east-1", "pod-d-3", "default")
	if err == nil {
		t.Fatalf("expected pod 3 allocation to be rejected due to maxPodsPerNode limit")
	}

	// 4. Register Node 2: Pod 3 should now succeed by fanning out onto Node 2
	node2 := NewCloudVMNode("i-density-node-02", ProviderAWS, "us-east-1", "10.42.0.12", capacity)
	node2.Namespace = "default"
	pool.RegisterNode(node2)

	n3, _, err := allocate(pool, req, "us-east-1", "pod-d-3", "default")
	if err != nil || n3.InstanceID != "i-density-node-02" {
		t.Fatalf("expected pod 3 to fan out to node 2, got node %v, err: %v", n3, err)
	}
	if node2.ActivePodCount() != 1 {
		t.Fatalf("expected 1 active pod on node 2, got %d", node2.ActivePodCount())
	}
}

func TestNodePool_IdleNodeStaysAllocatableUntilSpinDown(t *testing.T) {
	pool := NewNodePool(60 * time.Second)

	capacity := ResourceVector{
		CPU:    resource.MustParse("32"),
		Memory: resource.MustParse("128Gi"),
	}
	node := NewCloudVMNode("i-cordon-test-01", ProviderAWS, "us-east-1", "10.42.0.21", capacity)
	node.Namespace = "default"
	pool.RegisterNode(node)

	req := ResourceVector{
		CPU:    resource.MustParse("2"),
		Memory: resource.MustParse("4Gi"),
	}

	if _, _, err := allocate(pool, req, "us-east-1", "uid-cordon-1", "default"); err != nil {
		t.Fatalf("failed to allocate pod 1: %v", err)
	}

	// Releasing the last pod leaves the node idle but allocatable.
	if remaining := pool.Release("uid-cordon-1"); remaining != 0 {
		t.Fatalf("expected remaining active pods 0, got %d", remaining)
	}
	if node.Cordoned {
		t.Fatalf("expected idle node to stay allocatable")
	}

	// A completed pod that is not deleted yet keeps its container on the VM, so the node takes no new pod.
	if _, _, err := allocate(pool, req, "us-east-1", "uid-cordon-2", "default"); err != nil {
		t.Fatalf("expected the idle node to be reused: %v", err)
	}
	pool.MarkPodCompleted("uid-cordon-2", 0, "Completed", "Done", time.Now())
	if node.ActivePodCount() != 0 || node.Cordoned {
		t.Fatalf("expected an idle, uncordoned node after MarkPodCompleted (active %d, cordoned %v)", node.ActivePodCount(), node.Cordoned)
	}
	if _, _, err := allocate(pool, req, "us-east-1", "uid-cordon-3", "default"); err == nil {
		t.Fatalf("expected no reuse while the completed pod's container is still on the VM")
	}
	pool.Release("uid-cordon-2")

	// A node marked for spin-down takes no pod.
	pool.Cordon(node.InstanceID, SpinDownInterruption)
	if node.SpinDownReason != SpinDownInterruption {
		t.Fatalf("expected spin-down reason %q, got %q", SpinDownInterruption, node.SpinDownReason)
	}
	if _, _, err := allocate(pool, req, "us-east-1", "uid-cordon-3", "default"); err == nil {
		t.Fatalf("expected allocation to fail when the only warm node is marked for spin-down")
	}

	pool.Uncordon(node.InstanceID)
	if nAlloc, _, err := allocate(pool, req, "us-east-1", "uid-cordon-3", "default"); err != nil || nAlloc.InstanceID != "i-cordon-test-01" {
		t.Fatalf("expected allocation to succeed on uncordoned node, got: %v", err)
	}
}

func TestNodePool_ServerMaxPodsPerNodeEnv(t *testing.T) {
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))

	t.Run("configured_valid_env", func(t *testing.T) {
		t.Setenv("MAX_PODS_PER_NODE", "42")
		mock := NewMockProvider()
		srv := NewServer(mock, "cluster-env-density")
		if srv.NodePool().MaxPodsPerNode() != 42 {
			t.Fatalf("expected MaxPodsPerNode 42 from env, got %d", srv.NodePool().MaxPodsPerNode())
		}
	})

	t.Run("default_when_unset", func(t *testing.T) {
		t.Setenv("MAX_PODS_PER_NODE", "")
		mock := NewMockProvider()
		srv := NewServer(mock, "cluster-env-default")
		if srv.NodePool().MaxPodsPerNode() != 100 {
			t.Fatalf("expected default MaxPodsPerNode 100 when unset, got %d", srv.NodePool().MaxPodsPerNode())
		}
	})

	t.Run("default_when_zero_or_negative", func(t *testing.T) {
		t.Setenv("MAX_PODS_PER_NODE", "-5")
		mock := NewMockProvider()
		srv := NewServer(mock, "cluster-env-neg")
		if srv.NodePool().MaxPodsPerNode() != 100 {
			t.Fatalf("expected default MaxPodsPerNode 100 when negative, got %d", srv.NodePool().MaxPodsPerNode())
		}
	})
}

func TestNodePool_InstanceByIPMatchesLaunchAddress(t *testing.T) {
	pool := NewNodePool(60 * time.Second)
	pool.RegisterNode(NewCloudVMNode("i-launch", ProviderAWS, "eu-west-1", "10.0.0.5", ResourceVector{}))

	if id, ok := pool.InstanceByIP("10.0.0.5"); !ok || id != "i-launch" {
		t.Fatalf("InstanceByIP(10.0.0.5) = %q, %v; want i-launch, true", id, ok)
	}
	if _, ok := pool.InstanceByIP(""); ok {
		t.Fatal("InstanceByIP matched an empty address")
	}
	if _, ok := pool.InstanceByIP("10.0.0.6"); ok {
		t.Fatal("InstanceByIP matched an unknown address")
	}
}

func TestNodePool_NodeKeepsOneNamespace(t *testing.T) {
	pool := NewNodePool(time.Minute)
	capacity, _ := NewResourceVector("8", "32Gi", 0, 0)
	req, _ := NewResourceVector("1", "1Gi", 0, 0)
	pool.RegisterNode(NewCloudVMNode("i-shared", ProviderAWS, "eu-west-1", "10.0.0.5", capacity))
	if _, ok := pool.InstanceNamespace("i-shared"); ok {
		t.Fatal("a node without pods has a namespace")
	}
	if _, _, err := allocate(pool, req, "eu-west-1", "uid-a", "team-a"); err != nil {
		t.Fatalf("first pod: %v", err)
	}
	if _, _, err := allocate(pool, req, "eu-west-1", "uid-b", "team-b"); err == nil {
		t.Fatal("a pod of another namespace joined the node")
	}
	if _, _, err := allocate(pool, req, "eu-west-1", "uid-a2", "team-a"); err != nil {
		t.Fatalf("pod of the same namespace: %v", err)
	}
	if ns, ok := pool.InstanceNamespace("i-shared"); !ok || ns != "team-a" {
		t.Fatalf("InstanceNamespace = %q, %v; want team-a, true", ns, ok)
	}
	if _, ok := pool.InstanceNamespace("i-missing"); ok {
		t.Fatal("an unknown node has a namespace")
	}
}

func TestNodePool_DaemonSetPodsNeitherBindNorHoldTheNode(t *testing.T) {
	pool := NewNodePool(time.Minute)
	capacity, _ := NewResourceVector("8", "32Gi", 0, 0)
	req, _ := NewResourceVector("1", "1Gi", 0, 0)
	node := NewCloudVMNode("i-ds", ProviderAWS, "eu-west-1", "10.0.0.6", capacity)
	pool.RegisterNode(node)
	daemon := func(uid, namespace string) error {
		_, err := pool.AllocatePod(AllocationRequest{Resources: req, Region: "eu-west-1", PodUID: uid, Namespace: namespace, PodName: uid, DaemonSet: true})
		return err
	}

	if err := daemon("uid-proxy", "system"); err != nil {
		t.Fatalf("DaemonSet pod on an unbound node: %v", err)
	}
	if ns, ok := pool.InstanceNamespace("i-ds"); ok || node.Namespace != "" {
		t.Fatalf("a DaemonSet pod bound the node to %q", ns)
	}
	if node.isIdle() || node.IdleSince != nil {
		t.Fatal("a node that ran only DaemonSet pods is idle")
	}
	if _, _, err := allocate(pool, req, "eu-west-1", "uid-w1", "team-a"); err != nil {
		t.Fatalf("workload pod: %v", err)
	}
	if err := daemon("uid-exporter", "monitoring"); err != nil {
		t.Fatalf("DaemonSet pod of another namespace on a bound node: %v", err)
	}
	if ns, ok := pool.InstanceNamespace("i-ds"); !ok || ns != "team-a" {
		t.Fatalf("InstanceNamespace = %q, %v; want team-a", ns, ok)
	}
	if _, _, err := allocate(pool, req, "eu-west-1", "uid-w2", "team-b"); err == nil {
		t.Fatal("a workload pod of another namespace joined the node")
	}
	if !node.Allocated.CPU.Equal(resource.MustParse("3")) {
		t.Fatalf("allocated CPU = %s, want 3 counting the DaemonSet pods", node.Allocated.CPU.String())
	}

	// With only DaemonSet pods left, the node is idle; the pool keeps the DaemonSet pods and reuses the node for the
	// namespace it is bound to.
	pool.Release("uid-w1")
	if !node.isIdle() || pool.IdleNodeCount() != 1 || node.ActivePodCount() != 2 {
		t.Fatalf("after the workload left: idle %v, idle nodes %d, active pods %d", node.isIdle(), pool.IdleNodeCount(), node.ActivePodCount())
	}
	idleSince := *node.IdleSince
	pool.Release("uid-proxy")
	if !node.IdleSince.Equal(idleSince) {
		t.Fatal("a DaemonSet pod leaving restarted the idle grace period")
	}
	if _, _, err := allocate(pool, req, "eu-west-1", "uid-w3", "team-b"); err == nil {
		t.Fatal("an idle node bound to team-a took a team-b workload pod")
	}
	if got, _, err := allocate(pool, req, "eu-west-1", "uid-w3", "team-a"); err != nil || got.InstanceID != "i-ds" {
		t.Fatalf("reuse of the idle node: %v", err)
	}
	if node.isIdle() {
		t.Fatal("a reused node is still idle")
	}
}

func TestNodePool_CollectsNodesWithOnlyDaemonSetPods(t *testing.T) {
	pool := NewNodePool(0)
	capacity, _ := NewResourceVector("8", "32Gi", 0, 0)
	req, _ := NewResourceVector("1", "1Gi", 0, 0)
	pool.RegisterNode(NewCloudVMNode("i-ds", ProviderAWS, "eu-west-1", "10.0.0.6", capacity))
	if _, _, err := allocate(pool, req, "eu-west-1", "uid-w1", "team-a"); err != nil {
		t.Fatalf("workload pod: %v", err)
	}
	if _, err := pool.AllocatePod(AllocationRequest{Resources: req, Region: "eu-west-1", PodUID: "uid-proxy", Namespace: "system", PodName: "proxy", DaemonSet: true}); err != nil {
		t.Fatalf("DaemonSet pod: %v", err)
	}
	if expired := pool.CollectExpiredIdleNodes(); len(expired) != 0 {
		t.Fatalf("collected a node with a workload pod: %d", len(expired))
	}
	pool.Release("uid-w1")
	time.Sleep(time.Millisecond)
	if expired := pool.CollectExpiredIdleNodes(); len(expired) != 1 {
		t.Fatalf("collected %d nodes, want the one with only a DaemonSet pod", len(expired))
	}
}

func TestServer_PodNamespaceOnWorker(t *testing.T) {
	s := NewServer(NewMockProvider(), "pod-namespace")
	capacity, _ := NewResourceVector("8", "32Gi", 0, 0)
	req, _ := NewResourceVector("1", "1Gi", 0, 0)
	s.nodePool.RegisterNode(NewCloudVMNode("i-w", ProviderAWS, "eu-west-1", "10.0.0.7", capacity))
	s.nodePool.RegisterNode(NewCloudVMNode("i-other", ProviderAWS, "us-east-1", "10.0.0.8", capacity))
	if _, _, err := allocate(s.nodePool, req, "eu-west-1", "uid-w", "team-a"); err != nil {
		t.Fatalf("workload pod: %v", err)
	}
	if _, err := s.nodePool.AllocatePod(AllocationRequest{Resources: req, Region: "eu-west-1", PodUID: "uid-ds", Namespace: "monitoring", PodName: "exporter", DaemonSet: true}); err != nil {
		t.Fatalf("DaemonSet pod: %v", err)
	}
	for _, tc := range []struct {
		instance, uid, want string
		ok                  bool
	}{
		{"i-w", "uid-w", "team-a", true},
		{"i-w", "uid-ds", "monitoring", true},
		{"i-other", "uid-w", "", false},
		{"i-w", "uid-missing", "", false},
		{"i-missing", "uid-w", "", false},
	} {
		if got, ok := s.PodNamespaceOnWorker(tc.instance, tc.uid); got != tc.want || ok != tc.ok {
			t.Errorf("PodNamespaceOnWorker(%s, %s) = %q, %v; want %q, %v", tc.instance, tc.uid, got, ok, tc.want, tc.ok)
		}
	}
	if ns, ok := s.WorkerNamespace("i-w"); !ok || ns != "team-a" {
		t.Errorf("WorkerNamespace = %q, %v; want the workload's team-a", ns, ok)
	}
}
