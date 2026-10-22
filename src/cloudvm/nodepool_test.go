// Package main implements the interlink-provider-cloudvm daemon.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
)

func TestNodePool_GPUSlicing(t *testing.T) {
	pool := NewNodePool(60 * time.Second)

	// Register an 8-GPU VM (e.g. g5.48xlarge or p4d.24xlarge)
	capacity := ResourceVector{
		CPU:    resource.MustParse("96"),
		Memory: resource.MustParse("384Gi"),
		GPUs:   8,
	}
	node := NewCloudVMNode("i-8gpu-node-01", ProviderAWS, "us-west-2", "10.42.0.10", capacity)
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
	allocatedNode, gpus1, _, err := pool.Allocate(pod1Req, "us-west-2", "pod-uid-1", "default", "worker-1")
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
	allocatedNode2, gpus2, _, err := pool.Allocate(pod2Req, "us-west-2", "pod-uid-2", "default", "worker-2")
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
	_, _, _, err = pool.Allocate(pod3Req, "us-west-2", "pod-uid-3", "default", "worker-3")
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

	pool.RegisterNode(node1)
	pool.RegisterNode(node2)

	// Pod requesting 6 CPUs and 12GiB
	req := ResourceVector{
		CPU:    resource.MustParse("6"),
		Memory: resource.MustParse("12Gi"),
	}

	// NodeResourcesMostAllocated favors node1 (6/8 = 75%) over node2 (6/32 = 18.75%)
	targetNode, _, _, err := pool.Allocate(req, "us-east-1", "pod-cpu-1", "prod", "task-1")
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
	pool.RegisterNode(node)

	// Allocate Pod A and Pod B
	req := ResourceVector{
		CPU:    resource.MustParse("4"),
		Memory: resource.MustParse("8Gi"),
		GPUs:   1,
	}
	_, _, _, _ = pool.Allocate(req, "us-west-2", "pod-a", "default", "worker-a")
	_, _, _, _ = pool.Allocate(req, "us-west-2", "pod-b", "default", "worker-b")

	// Release Pod A: Node still has Pod B running -> not idle
	pool.Release("pod-a")
	if node.IdleSince != nil {
		t.Errorf("node should not be idle while Pod B is still running")
	}

	// Release Pod B: Node has 0 active pods -> IdleSince recorded
	pool.Release("pod-b")
	if node.IdleSince == nil {
		t.Fatalf("node should record IdleSince timestamp once all pods finish")
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

	// 4. Delete Pod 2 -> Last pod on VM finishes -> VM is terminated
	recDel2 := httptest.NewRecorder()
	handler.ServeHTTP(recDel2, httptest.NewRequest(http.MethodPost, "/delete", bytes.NewReader(pod2Body)))
	if recDel2.Code != http.StatusOK {
		t.Fatalf("pod 2 delete failed: %d", recDel2.Code)
	}

	activeVMsAfterDel2, _ := mock.ListActiveInstances(context.Background(), "test-cluster")
	if len(activeVMsAfterDel2) != 0 {
		t.Fatalf("expected shared VM to be terminated after last pod exited, got %d active VMs", len(activeVMsAfterDel2))
	}
}
