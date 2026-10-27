// Package main implements the interlink-cloudvm-plugin daemon.
// This file tests that spot notices on a worker log stream cordon the VM, post pod events, and create KubeRay drain requests once.
package main

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
)

func testPod(name, uid string, labels map[string]string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ray", UID: types.UID(uid), Labels: labels}}
}

// countingKubeRecorder drains through the fake clientset but counts events itself, since the fake ignores GenerateName.
type countingKubeRecorder struct {
	*KubePodEventRecorder
	fakeEventRecorder
}

func (r *countingKubeRecorder) RecordPodEvent(ctx context.Context, namespace, name, uid, eventType, reason, message string) error {
	if eventType != corev1.EventTypeWarning {
		reason = "unexpected type " + eventType
	}
	return r.fakeEventRecorder.RecordPodEvent(ctx, namespace, name, uid, eventType, reason, message)
}

func TestSpotInterruption_LogStreamAsksKubeRayToDrainWorkers(t *testing.T) {
	cs := fake.NewClientset(
		testPod("rc-worker-a", "uid-worker", map[string]string{rayClusterLabel: "rc", rayGroupLabel: "gpu", rayNodeTypeLabel: "worker"}),
		testPod("rc-head", "uid-head", map[string]string{rayClusterLabel: "rc", rayGroupLabel: "headgroup", rayNodeTypeLabel: "head"}),
		testPod("plain", "uid-plain", nil),
	)
	s := NewServer(NewMockProvider(), "test-cluster")
	rec := &countingKubeRecorder{KubePodEventRecorder: NewKubePodEventRecorder(cs)}
	s.SetPodEventRecorder(rec)
	capVec, _ := NewResourceVector("8", "32Gi", 0, 0)
	reqVec, _ := NewResourceVector("1", "1Gi", 0, 0)
	node := NewCloudVMNode("i-spot", ProviderMock, "us-east-1", "", capVec)
	s.NodePool().RegisterAndAllocate(node, reqVec, "uid-worker", "ray", "rc-worker-a")
	node.AssignPod(&PodAllocation{PodUID: "uid-head", PodNamespace: "ray", PodName: "rc-head", Allocated: reqVec})
	node.AssignPod(&PodAllocation{PodUID: "uid-plain", PodNamespace: "ray", PodName: "plain", Allocated: reqVec})

	notice := `{"src":"spot","msg":"{\"event\": \"spot-interruption\", \"action\": \"terminate\", \"time\": \"2026-10-27T10:02:00Z\"}"}` + "\n"
	stream := `{"src":"spot","msg":"[cloudvm-spot-watcher] watching"}` + "\n" + notice + notice +
		// A runner line that looks like a notice must not count as one.
		`{"src":"runner","msg":"{\"event\": \"spot-interruption\"}"}` + "\n"
	s.ConsumeWorkerLogs("i-spot", strings.NewReader(stream))
	// A reconnecting agent can resend the notice on a new stream.
	s.ConsumeWorkerLogs("i-spot", strings.NewReader(notice))

	if !node.IsCordoned() {
		t.Fatal("interrupted node was not cordoned")
	}
	ctx := context.Background()
	cms, err := cs.CoreV1().ConfigMaps("ray").List(ctx, metav1.ListOptions{})
	if err != nil || len(cms.Items) != 1 {
		t.Fatalf("listed %d ConfigMaps (err %v), want one for the Ray worker", len(cms.Items), err)
	}
	cm := cms.Items[0]
	if cm.Name != "spot-interrupted-uid-worker" || cm.Labels[managedByLabel] != managedByValue || cm.Labels[kubeRaySpotEventLabel] != kubeRaySpotEventValue || cm.Labels[kubeRayTargetClusterLabel] != "rc" {
		t.Fatalf("unexpected ConfigMap metadata %+v", cm.ObjectMeta)
	}
	wantData := map[string]string{kubeRayWorkerGroupKey: "gpu", kubeRayInterruptedPodKey: "rc-worker-a", kubeRayReplicasKey: "1"}
	if len(cm.Data) != len(wantData) {
		t.Fatalf("ConfigMap data %v, want %v", cm.Data, wantData)
	}
	for k, v := range wantData {
		if cm.Data[k] != v {
			t.Fatalf("ConfigMap data %v, want %v", cm.Data, wantData)
		}
	}
	if refs := cm.OwnerReferences; len(refs) != 1 || refs[0].Kind != "Pod" || refs[0].UID != "uid-worker" {
		t.Fatalf("ConfigMap owner references %+v, want the worker pod", refs)
	}

	perPod := map[string]int{}
	for _, ev := range rec.events {
		if ev.reason != "CloudVMSpotInterruption" || !strings.Contains(ev.message, "terminate at 2026-10-27T10:02:00Z") {
			t.Fatalf("unexpected event %+v", ev)
		}
		perPod[ev.name]++
	}
	// One event per pod for each stream that carried the notice.
	for _, name := range []string{"rc-worker-a", "rc-head", "plain"} {
		if perPod[name] != 2 {
			t.Fatalf("events per pod %v, want 2 for %s", perPod, name)
		}
	}
}

func TestRequestRayWorkerDrain_IsIdempotent(t *testing.T) {
	cs := fake.NewClientset(testPod("rc-worker-a", "uid-worker", map[string]string{rayClusterLabel: "rc", rayGroupLabel: "gpu"}))
	rec := NewKubePodEventRecorder(cs)
	ctx := context.Background()
	for i, want := range []bool{true, false, false} {
		got, err := rec.RequestRayWorkerDrain(ctx, "ray", "rc-worker-a", "uid-worker")
		if err != nil || got != want {
			t.Fatalf("request %d: got (%v, %v), want (%v, nil)", i, got, err, want)
		}
	}
	if got, err := rec.RequestRayWorkerDrain(ctx, "ray", "gone", "uid-gone"); err != nil || got {
		t.Fatalf("request for a deleted pod: got (%v, %v), want (false, nil)", got, err)
	}
}
