// Tests that the agent deletes from a claim's VM the pods that left its Node, both those deleted while the agent was
// down and those a refused create finds, so the VM takes the pods the scheduler fits in their place.

package main

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// runPlainAgent starts a shard serving the claim whose claim list and departed-pod sweep run only at startup.
func runPlainAgent(t *testing.T, kube *fake.Clientset, claims *fakeClaims, plugin *fakePlugin) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	agent := NewAgent(AgentConfig{
		Kube: kube, Claims: claims, Pods: NewPodAPI(plugin.server.URL, nil),
		Shard: Shard{Count: 1}, NodeIP: "10.0.0.7", KubeletPort: 10250, ClaimPoll: time.Hour, PodStatus: time.Hour,
		LeaseSeconds: 40, LeaseRenew: time.Second, NodeStatus: time.Second, OrphanSweep: time.Hour,
	})
	done := make(chan error, 1)
	go func() { done <- agent.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Error(err)
		}
	})
}

func plainPod(name, nodeName string) *corev1.Pod {
	pod := testPod(name, nodeName)
	pod.Spec.ImagePullSecrets, pod.Spec.Containers[0].Env = nil, nil
	return pod
}

func (p *fakePlugin) deletedUIDs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.deleted)
}

func (p *fakePlugin) wasCreated(uid string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.created[uid]
	return ok
}

func TestAgentDeletesPodsThatLeftWhileItWasDown(t *testing.T) {
	ctx := context.Background()
	// The VM holds two pods: "kept" is still bound to the Node, "gone" was deleted while the agent was down.
	kube := fake.NewClientset(plainPod("kept", "vm-a"))
	plugin := newFakePlugin(t)
	plugin.slots = 2
	plugin.holding["uid-kept"], plugin.holding["uid-gone"] = true, true
	claim := gpuClaim("vm-a", nodeclaim.StateRunning)
	claim.Pods = []nodeclaim.PodRef{{Namespace: "team-a", Name: "gone", UID: "uid-gone"}, {Namespace: "team-a", Name: "kept", UID: "uid-kept"}}
	runPlainAgent(t, kube, newFakeClaims(claim), plugin)

	waitFor(t, "delete of the departed pod", func() bool { return slices.Contains(plugin.deletedUIDs(), "uid-gone") })
	if slices.Contains(plugin.deletedUIDs(), "uid-kept") {
		t.Fatalf("the agent deleted a pod still bound to the node: %v", plugin.deletedUIDs())
	}

	if _, err := kube.CoreV1().Pods("team-a").Create(ctx, plainPod("next", "vm-a"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "create of the next pod", func() bool { return plugin.wasCreated("uid-next") })
	if p, err := kube.CoreV1().Pods("team-a").Get(ctx, "next", metav1.GetOptions{}); err != nil || p.Status.Phase == corev1.PodFailed {
		t.Fatalf("next pod = %+v, %v", p.Status, err)
	}
}

func TestAgentRetriesARefusedPodAfterDeletingDepartedPods(t *testing.T) {
	ctx := context.Background()
	kube := fake.NewClientset()
	plugin := newFakePlugin(t)
	plugin.slots = 1
	claims := newFakeClaims(gpuClaim("vm-a", nodeclaim.StateRunning))
	runPlainAgent(t, kube, claims, plugin)
	waitFor(t, "node", func() bool { return getNode(t, kube, "vm-a") != nil })

	// The plugin reports a pod the agent's last claim list did not show, which holds the VM's only slot.
	plugin.mu.Lock()
	plugin.holding["uid-gone"] = true
	plugin.mu.Unlock()
	claim := gpuClaim("vm-a", nodeclaim.StateRunning)
	claim.Pods = []nodeclaim.PodRef{{Namespace: "team-a", Name: "gone", UID: "uid-gone"}}
	claims.set(claim)

	if _, err := kube.CoreV1().Pods("team-a").Create(ctx, plainPod("p1", "vm-a"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "create of p1", func() bool { return plugin.wasCreated("uid-p1") })
	if got := plugin.deletedUIDs(); !slices.Equal(got, []string{"uid-gone"}) {
		t.Fatalf("deleted = %v, want only the departed pod", got)
	}
	if p, err := kube.CoreV1().Pods("team-a").Get(ctx, "p1", metav1.GetOptions{}); err != nil || p.Status.Phase == corev1.PodFailed {
		t.Fatalf("p1 = %+v, %v", p.Status, err)
	}
}
