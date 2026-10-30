// End-to-end tests of one shard against a fake clientset and a fake plugin: pods bound to served Nodes start through
// /create, report status from /status, and stop through /delete, while DaemonSet pods stay Pending off the plugin.

package main

import (
	"context"
	"io"
	"slices"
	"testing"
	"time"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func testPod(name, nodeName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team-a", UID: types.UID("uid-" + name)},
		Spec: corev1.PodSpec{
			NodeName:         nodeName,
			ImagePullSecrets: []corev1.LocalObjectReference{{Name: "pull"}},
			Containers: []corev1.Container{{
				Name: "worker", Image: "busybox",
				Env: []corev1.EnvVar{{Name: "TOKEN", ValueFrom: &corev1.EnvVarSource{
					SecretKeyRef: &corev1.SecretKeySelector{LocalObjectReference: corev1.LocalObjectReference{Name: "app"}, Key: "token"},
				}}},
			}},
		},
	}
}

func TestAgentRunsPodsOnServedNodes(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kube := fake.NewClientset(
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "pull", Namespace: "team-a"}, Type: corev1.SecretTypeDockerConfigJson,
			Data: map[string][]byte{corev1.DockerConfigJsonKey: []byte(`{"auths":{}}`)}},
		&corev1.Secret{ObjectMeta: metav1.ObjectMeta{Name: "app", Namespace: "team-a"}, Data: map[string][]byte{"token": []byte("s3cret")}},
	)
	calls := countCalls(kube)
	plugin := newFakePlugin(t)
	agent := NewAgent(AgentConfig{
		Kube: kube, Claims: newFakeClaims(gpuClaim("vm-a", nodeclaim.StateRunning)), Pods: NewPodAPI(plugin.server.URL, nil),
		Shard: Shard{Count: 1}, NodeIP: "10.0.0.7", KubeletPort: 10250, ClaimPoll: time.Hour, PodStatus: 50 * time.Millisecond,
		LeaseSeconds: 40, LeaseRenew: time.Second, NodeStatus: time.Second, OrphanSweep: time.Hour,
	})
	done := make(chan error, 1)
	go func() { done <- agent.Run(ctx) }()
	waitFor(t, "node", func() bool { return getNode(t, kube, "vm-a") != nil })

	if _, err := kube.CoreV1().Pods("team-a").Create(ctx, testPod("p1", "vm-a"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := kube.CoreV1().Pods("team-a").Create(ctx, testPod("other", "real-kubelet-node"), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "plugin create", func() bool {
		plugin.mu.Lock()
		defer plugin.mu.Unlock()
		_, ok := plugin.created["uid-p1"]
		return ok
	})
	plugin.mu.Lock()
	req := plugin.created["uid-p1"]
	_, otherCreated := plugin.created["uid-other"]
	plugin.mu.Unlock()
	if otherCreated {
		t.Fatal("the agent ran a pod bound to a node it does not serve")
	}
	if len(req.Secrets) != 1 || req.Secrets[0].Name != "pull" {
		t.Errorf("create secrets = %v, want the pull secret", req.Secrets)
	}
	if env := req.Pod.Spec.Containers[0].Env; len(env) != 1 || env[0].Value != "s3cret" {
		t.Errorf("create env = %v, want the resolved secret value", env)
	}

	plugin.mu.Lock()
	plugin.running["uid-p1"] = true
	plugin.mu.Unlock()
	waitFor(t, "pod Running", func() bool {
		p, err := kube.CoreV1().Pods("team-a").Get(ctx, "p1", metav1.GetOptions{})
		return err == nil && p.Status.Phase == corev1.PodRunning && p.Status.PodIP == "10.9.0.1"
	})

	logs, err := agent.Pods.ContainerLogs(ctx, "team-a", "p1", "worker", api.ContainerLogOpts{Tail: 10})
	if err != nil {
		t.Fatal(err)
	}
	text, _ := io.ReadAll(logs)
	_ = logs.Close()
	if string(text) != "hello from p1/worker\n" {
		t.Errorf("logs = %q", text)
	}

	if err := kube.CoreV1().Pods("team-a").Delete(ctx, "p1", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "plugin delete", func() bool {
		plugin.mu.Lock()
		defer plugin.mu.Unlock()
		return len(plugin.deleted) == 1 && plugin.deleted[0] == "uid-p1"
	})

	for call := range calls.snapshot() {
		switch call {
		case "list secrets", "watch secrets", "list configmaps", "watch configmaps":
			t.Errorf("the agent made a cluster-wide %s call", call)
		}
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestAgentHoldsDaemonSetPodsPending(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kube := fake.NewClientset()
	plugin := newFakePlugin(t)
	agent := NewAgent(AgentConfig{
		Kube: kube, Claims: newFakeClaims(gpuClaim("vm-a", nodeclaim.StateRunning)), Pods: NewPodAPI(plugin.server.URL, nil),
		Shard: Shard{Count: 1}, NodeIP: "10.0.0.7", KubeletPort: 10250, ClaimPoll: time.Hour, PodStatus: 20 * time.Millisecond,
		LeaseSeconds: 40, LeaseRenew: time.Second, NodeStatus: time.Second, OrphanSweep: time.Hour,
	})
	done := make(chan error, 1)
	go func() { done <- agent.Run(ctx) }()
	waitFor(t, "node", func() bool { return getNode(t, kube, "vm-a") != nil })

	daemon := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "node-proxy-x1", Namespace: "system", UID: "uid-daemon",
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: "node-proxy", UID: "ds-1"}}},
		Spec: corev1.PodSpec{NodeName: "vm-a", Containers: []corev1.Container{{Name: "proxy", Image: "proxy"}}},
	}
	if _, err := kube.CoreV1().Pods("system").Create(ctx, daemon, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	var held *corev1.Pod
	waitFor(t, "daemonset pod held Pending", func() bool {
		p, err := kube.CoreV1().Pods("system").Get(ctx, daemon.Name, metav1.GetOptions{})
		held = p
		return err == nil && p.Status.Phase == corev1.PodPending && p.Status.Reason == ReasonDaemonSetPod
	})

	// The workload pod of another namespace still runs on the VM the DaemonSet pod is bound to.
	workload := testPod("w1", "vm-a")
	workload.Spec.ImagePullSecrets, workload.Spec.Containers[0].Env = nil, nil
	if _, err := kube.CoreV1().Pods("team-a").Create(ctx, workload, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "workload create and status poll", func() bool {
		plugin.mu.Lock()
		defer plugin.mu.Unlock()
		_, created := plugin.created["uid-w1"]
		return created && plugin.polled["uid-w1"]
	})
	time.Sleep(100 * time.Millisecond) // several status intervals
	plugin.mu.Lock()
	_, daemonCreated := plugin.created["uid-daemon"]
	daemonPolled := plugin.polled["uid-daemon"]
	plugin.mu.Unlock()
	if daemonCreated || daemonPolled {
		t.Fatalf("the plugin saw the DaemonSet pod: created %v, polled %v", daemonCreated, daemonPolled)
	}
	if p, err := kube.CoreV1().Pods("system").Get(ctx, daemon.Name, metav1.GetOptions{}); err != nil || p.ResourceVersion != held.ResourceVersion {
		t.Fatalf("the DaemonSet pod changed after it was held: %v", err)
	}

	if err := kube.CoreV1().Pods("system").Delete(ctx, daemon.Name, metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "daemonset pod forgotten", func() bool {
		_, err := agent.Pods.GetPod(ctx, "system", daemon.Name)
		return err != nil
	})
	plugin.mu.Lock()
	deleted := slices.Clone(plugin.deleted)
	plugin.mu.Unlock()
	if slices.Contains(deleted, "uid-daemon") {
		t.Fatalf("the agent sent the DaemonSet pod to the plugin's /delete: %v", deleted)
	}
	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}
