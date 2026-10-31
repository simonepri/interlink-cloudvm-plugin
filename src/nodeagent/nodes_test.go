// Tests that served Nodes carry what Karpenter expects (providerID, labels, the unregistered taint, GPU capacity,
// Ready once the VM is up) and that Nodes follow their claims' lifecycle without reasserting taints.

package main

import (
	"context"
	"testing"
	"time"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

func gpuClaim(name string, state nodeclaim.State) nodeclaim.NodeClaim {
	return nodeclaim.NodeClaim{
		Name:         name,
		ProviderID:   nodeclaim.FormatProviderID("aws", "eu-west-1", "i-"+name),
		InstanceType: "g5.xlarge",
		Region:       "eu-west-1",
		Zone:         "eu-west-1a",
		CapacityType: nodeclaim.CapacityTypeSpot,
		State:        state,
		Labels:       map[string]string{"gpu-class": "a10g", "karpenter.sh/nodepool": "gpu"},
		Taints:       []nodeclaim.Taint{{Key: "virtual-node.interlink/provider", Value: "cloudvm", Effect: "NoSchedule"}},
		Capacity:     map[string]string{"cpu": "4", "memory": "16Gi", nodeclaim.ResourceGPU: "1"},
	}
}

func testManager(claims *fakeClaims, kube *fake.Clientset) *NodeManager {
	return NewNodeManager(NodeConfig{
		Shard: Shard{Index: 0, Count: 1}, Claims: claims, Kube: kube, NodeIP: "10.0.0.7", KubeletPort: 10250,
		LeaseRenewInterval: 50 * time.Millisecond, LeaseDurationSeconds: 40, StatusInterval: 50 * time.Millisecond,
		OrphanSweepInterval: time.Hour,
	})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func getNode(t *testing.T, kube *fake.Clientset, name string) *corev1.Node {
	t.Helper()
	n, err := kube.CoreV1().Nodes().Get(context.Background(), name, metav1.GetOptions{})
	if err != nil {
		return nil
	}
	return n
}

func readyStatus(n *corev1.Node) corev1.ConditionStatus {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status
		}
	}
	return corev1.ConditionUnknown
}

func TestNodeRegistersForKarpenter(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kube := fake.NewClientset()
	claims := newFakeClaims(gpuClaim("vm-a", nodeclaim.StateLaunching))
	m := testManager(claims, kube)
	m.Sync(ctx)
	defer m.stopAll()

	waitFor(t, "node creation", func() bool { return getNode(t, kube, "vm-a") != nil })
	n := getNode(t, kube, "vm-a")
	if n.Spec.ProviderID != "cloudvm://aws/eu-west-1/i-vm-a/vm" {
		t.Errorf("providerID = %q", n.Spec.ProviderID)
	}
	for k, v := range map[string]string{
		"gpu-class": "a10g", labelInstanceType: "g5.xlarge", labelRegion: "eu-west-1", labelZone: "eu-west-1a",
		labelCapacityType: "spot", labelNodeType: nodeTypeValue, nodeclaim.LabelNodeAgent: "true", labelHostname: "vm-a",
	} {
		if n.Labels[k] != v {
			t.Errorf("label %s = %q, want %q", k, n.Labels[k], v)
		}
	}
	hasTaint := func(n *corev1.Node, key string) bool {
		for _, tt := range n.Spec.Taints {
			if tt.Key == key {
				return true
			}
		}
		return false
	}
	if !hasTaint(n, nodeclaim.TaintUnregistered) || !hasTaint(n, "virtual-node.interlink/provider") {
		t.Errorf("taints = %v, want the claim taint and %s", n.Spec.Taints, nodeclaim.TaintUnregistered)
	}
	if gpu := n.Status.Allocatable[nodeclaim.ResourceGPU]; gpu.Cmp(resource.MustParse("1")) != 0 {
		t.Errorf("allocatable %s = %s, want 1", nodeclaim.ResourceGPU, gpu.String())
	}
	if pods := n.Status.Capacity[corev1.ResourcePods]; pods.Cmp(resource.MustParse("110")) != 0 {
		t.Errorf("capacity pods = %s, want the default 110", pods.String())
	}
	if readyStatus(n) != corev1.ConditionFalse {
		t.Errorf("Ready = %s while the VM launches, want False", readyStatus(n))
	}
	if n.Status.DaemonEndpoints.KubeletEndpoint.Port != 10250 || n.Status.Addresses[0].Address != "10.0.0.7" {
		t.Errorf("endpoint = %v %v", n.Status.DaemonEndpoints, n.Status.Addresses)
	}

	// Karpenter removes the unregistered taint and adds its own label; the agent must not undo either.
	n.Spec.Taints = n.Spec.Taints[:1]
	n.Labels["karpenter.sh/registered"] = "true"
	if _, err := kube.CoreV1().Nodes().Update(ctx, n, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	claims.set(gpuClaim("vm-a", nodeclaim.StateRunning))
	m.Sync(ctx)
	waitFor(t, "Ready=True", func() bool { return readyStatus(getNode(t, kube, "vm-a")) == corev1.ConditionTrue })
	time.Sleep(200 * time.Millisecond) // several status updates
	n = getNode(t, kube, "vm-a")
	if hasTaint(n, nodeclaim.TaintUnregistered) {
		t.Error("the agent reasserted the unregistered taint after Karpenter removed it")
	}
	if n.Labels["karpenter.sh/registered"] != "true" {
		t.Error("the agent dropped a label Karpenter added")
	}
	if _, err := kube.CoordinationV1().Leases(corev1.NamespaceNodeLease).Get(ctx, "vm-a", metav1.GetOptions{}); err != nil {
		t.Errorf("node lease: %v", err)
	}
}

// TestNodeReportsEveryClaimResource checks that the Node reports each resource of its claim's capacity as capacity and
// allocatable, including one that appears only after the Node registered.
func TestNodeReportsEveryClaimResource(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kube := fake.NewClientset()
	c := gpuClaim("vm-r", nodeclaim.StateRunning)
	c.Capacity = nodeclaim.Hardware{CPU: "8", Memory: "32Gi", GPUs: 1, TPUs: 4, Pods: 58}.Capacity()
	claims := newFakeClaims(c)
	m := testManager(claims, kube)
	m.Sync(ctx)
	defer m.stopAll()
	waitFor(t, "node creation", func() bool { return getNode(t, kube, "vm-r") != nil })

	// The plugin reads the root volume size after the VM launched.
	c.Capacity = nodeclaim.Hardware{CPU: "8", Memory: "32Gi", EphemeralStorage: "200Gi", GPUs: 1, TPUs: 4, Pods: 58}.Capacity()
	claims.set(c)
	m.Sync(ctx)
	reports := func(n *corev1.Node) bool {
		for name, q := range c.Capacity {
			want := resource.MustParse(q)
			got, gotAlloc := n.Status.Capacity[corev1.ResourceName(name)], n.Status.Allocatable[corev1.ResourceName(name)]
			if got.Cmp(want) != 0 || gotAlloc.Cmp(want) != 0 {
				return false
			}
		}
		return true
	}
	waitFor(t, "every claim resource on the node", func() bool { return reports(getNode(t, kube, "vm-r")) })
}

func TestNodeFollowsClaimLifecycle(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kube := fake.NewClientset()
	claims := newFakeClaims(gpuClaim("vm-a", nodeclaim.StateRunning), gpuClaim("vm-b", nodeclaim.StateRunning))
	m := testManager(claims, kube)
	m.Sync(ctx)
	defer m.stopAll()
	waitFor(t, "both nodes", func() bool { return getNode(t, kube, "vm-a") != nil && getNode(t, kube, "vm-b") != nil })

	// A failing list changes nothing.
	claims.setFail(true)
	m.Sync(ctx)
	m.Sync(ctx)
	if m.ServedCount() != 2 || getNode(t, kube, "vm-a") == nil {
		t.Fatal("a failed claim list stopped serving nodes")
	}
	claims.setFail(false)

	// A terminating claim stops being served and is never served again; Karpenter deletes its Node.
	claims.set(gpuClaim("vm-a", nodeclaim.StateTerminating))
	m.Sync(ctx)
	if m.Serves("vm-a") {
		t.Fatal("still serving a terminating claim")
	}
	claims.set(gpuClaim("vm-a", nodeclaim.StateRunning))
	m.Sync(ctx)
	if m.Serves("vm-a") {
		t.Fatal("served a claim again after it was terminating")
	}

	// A claim missing from two successful lists has its Node deleted.
	claims.remove("vm-b")
	m.Sync(ctx)
	if getNode(t, kube, "vm-b") == nil {
		t.Fatal("deleted the node after one missing list")
	}
	m.Sync(ctx)
	if _, err := kube.CoreV1().Nodes().Get(ctx, "vm-b", metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatalf("node vm-b after its claim vanished: %v, want not found", err)
	}
	if m.Serves("vm-b") {
		t.Fatal("still serving vm-b")
	}
}

func TestNodeDeletedExternallyIsNotRecreated(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kube := fake.NewClientset()
	claims := newFakeClaims(gpuClaim("vm-a", nodeclaim.StateRunning))
	m := testManager(claims, kube)
	m.Sync(ctx)
	defer m.stopAll()
	waitFor(t, "node", func() bool { return getNode(t, kube, "vm-a") != nil })
	if err := kube.CoreV1().Nodes().Delete(ctx, "vm-a", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "retirement", func() bool { return !m.Serves("vm-a") })
	m.Sync(ctx)
	time.Sleep(200 * time.Millisecond)
	if getNode(t, kube, "vm-a") != nil {
		t.Fatal("the agent recreated a node someone deleted")
	}
}

func TestNodeBeingDeletedIsNotServed(t *testing.T) {
	ctx := context.Background()
	now := metav1.Now()
	kube := fake.NewClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{
		Name: "vm-a", DeletionTimestamp: &now, Finalizers: []string{"karpenter.sh/termination"},
	}})
	m := testManager(newFakeClaims(gpuClaim("vm-a", nodeclaim.StateRunning)), kube)
	m.Sync(ctx)
	defer m.stopAll()
	if m.Serves("vm-a") {
		t.Fatal("served a node that is being deleted")
	}
}

func TestShardServesOnlyItsNodes(t *testing.T) {
	ctx := context.Background()
	var claims []nodeclaim.NodeClaim
	for _, name := range []string{"a", "b", "c", "d", "e", "f", "g", "h"} {
		claims = append(claims, gpuClaim("vm-"+name, nodeclaim.StateLaunching))
	}
	total := 0
	for i := range 3 {
		kube := fake.NewClientset()
		m := testManager(newFakeClaims(claims...), kube)
		m.cfg.Shard = Shard{Index: i, Count: 3}
		m.Sync(ctx)
		for _, c := range claims {
			if m.Serves(c.Name) != (OwnerShard(c.Name, 3) == i) {
				t.Errorf("shard %d serves %s = %v", i, c.Name, m.Serves(c.Name))
			}
		}
		total += m.ServedCount()
		m.stopAll()
	}
	if total != len(claims) {
		t.Fatalf("shards served %d nodes in total, want %d", total, len(claims))
	}
}

func TestNodeIsNotReadyWhenClaimIsUnhealthy(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kube := fake.NewClientset()
	claims := newFakeClaims(gpuClaim("vm-u", nodeclaim.StateRunning))
	m := testManager(claims, kube)
	m.Sync(ctx)
	defer m.stopAll()
	waitFor(t, "ready node", func() bool { n := getNode(t, kube, "vm-u"); return n != nil && readyStatus(n) == corev1.ConditionTrue })

	claims.set(gpuClaim("vm-u", nodeclaim.StateUnhealthy))
	m.Sync(ctx)
	waitFor(t, "NotReady node", func() bool { return readyStatus(getNode(t, kube, "vm-u")) == corev1.ConditionFalse })
	for _, c := range getNode(t, kube, "vm-u").Status.Conditions {
		if c.Type == corev1.NodeReady && c.Reason != reasonVMUnhealthy {
			t.Fatalf("Ready reason = %q, want %q", c.Reason, reasonVMUnhealthy)
		}
	}
}
