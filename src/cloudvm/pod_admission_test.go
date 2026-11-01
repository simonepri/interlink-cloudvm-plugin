// Tests that create refuses pods with several regular containers or a sandbox runtime class, and that NVIDIA runtime
// class DaemonSet pods of allowed namespaces see the GPUs their NVIDIA_VISIBLE_DEVICES names while GPU pods keep theirs.
package main

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
)

func strp(s string) *string { return &s }

// reasonEvents counts the events of each reason the server posts, from any goroutine.
type reasonEvents struct {
	mu     sync.Mutex
	counts map[string]int
}

func (e *reasonEvents) RecordPodEvent(_ context.Context, _, _, _, _, reason, _ string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.counts == nil {
		e.counts = map[string]int{}
	}
	e.counts[reason]++
	return nil
}

// take returns how many events of the reason were posted since the last take.
func (e *reasonEvents) take(reason string) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := e.counts[reason]
	delete(e.counts, reason)
	return n
}

func TestPodAdmissionRefusals(t *testing.T) {
	one := []ContainerSpec{{Name: "main", Image: "busybox"}}
	cases := []struct {
		name       string
		classes    []string
		containers []ContainerSpec
		class      *string
		reason     string
		inMessage  []string
	}{
		{name: "one container", containers: one},
		{name: "no runtime class", containers: one, class: strp("")},
		{name: "default NVIDIA runtime class", containers: one, class: strp("nvidia-cdi")},
		{name: "configured NVIDIA runtime class", classes: []string{"gpu"}, containers: one, class: strp("gpu")},
		{
			name:       "several regular containers",
			containers: []ContainerSpec{{Name: "main"}, {Name: "sidecar"}, {Name: "proxy"}},
			reason:     reasonMultipleContainersUnsupported, inMessage: []string{`"main"`, `"sidecar", "proxy"`},
		},
		{name: "sandbox runtime class", containers: one, class: strp("gvisor"), reason: reasonRuntimeClassUnsupported, inMessage: []string{`"gvisor"`}},
		{name: "default class outside a configured list", classes: []string{"gpu"}, containers: one, class: strp("nvidia"), reason: reasonRuntimeClassUnsupported},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod := &Pod{Metadata: PodMeta{Name: "p", Namespace: "ns"}, Spec: PodSpecWrapper{Containers: tc.containers, RuntimeClassName: tc.class}}
			got := NewPodAdmission(tc.classes, nil).Refusal(pod)
			if tc.reason == "" {
				if got != nil {
					t.Fatalf("Refusal = %v, want none", got)
				}
				return
			}
			if got == nil || got.Reason != tc.reason {
				t.Fatalf("Refusal = %v, want reason %s", got, tc.reason)
			}
			for _, want := range tc.inMessage {
				if !strings.Contains(got.Message, want) {
					t.Errorf("message %q lacks %s", got.Message, want)
				}
			}
		})
	}
}

func TestCreateRefusesPodsItCannotRunAsAsked(t *testing.T) {
	for _, tc := range []struct {
		name, reason, spec string
	}{
		{name: "multi", reason: reasonMultipleContainersUnsupported, spec: `"containers": [{"name": "main", "image": "busybox"}, {"name": "sidecar", "image": "envoy"}]`},
		{name: "kata", reason: reasonRuntimeClassUnsupported, spec: `"runtimeClassName": "kata", "containers": [{"name": "main", "image": "busybox"}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockProvider()
			server := NewServer(mock, "test-cluster")
			server.SetDefaultRegion("us-west-2")
			events := &reasonEvents{}
			server.SetPodEventRecorder(events)
			ts := httptest.NewServer(server.Routes())
			defer ts.Close()

			payload := `{"pod": {"metadata": {"name": "p", "namespace": "default", "uid": "uid-` + tc.name + `", "annotations": {"cloudvm.vk.io/region": "us-west-2"}}, "spec": {` + tc.spec + `}}}`
			resp, err := http.Post(ts.URL+"/create", "application/json", strings.NewReader(payload))
			if err != nil {
				t.Fatal(err)
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest || !strings.HasPrefix(string(body), tc.reason+": ") {
				t.Fatalf("create: HTTP %d %q, want 400 starting with %s", resp.StatusCode, body, tc.reason)
			}
			if mock.LastSpec() != nil {
				t.Fatal("a refused pod launched a VM")
			}
			if n := events.take(tc.reason); n != 1 {
				t.Fatalf("%d %s events, want one", n, tc.reason)
			}
		})
	}
}

// nvidiaPod returns a pod bound to node nc-gpu with the nvidia runtime class whose container sets NVIDIA_VISIBLE_DEVICES to
// visible and requests gpus GPUs, owned by a DaemonSet when daemonSet is set.
func nvidiaPod(namespace, name, visible string, gpus int, daemonSet bool) Pod {
	pod := Pod{
		Metadata: PodMeta{Name: name, Namespace: namespace, UID: "uid-" + name},
		Spec: PodSpecWrapper{NodeName: "nc-gpu", RuntimeClassName: strp("nvidia"), Containers: []ContainerSpec{{
			Name: "exporter", Image: "dcgm-exporter", Env: []EnvVar{{Name: envNVIDIAVisibleDevices, Value: visible}},
		}}},
	}
	if gpus > 0 {
		pod.Spec.Containers[0].Resources.Limits = map[string]string{"nvidia.com/gpu": "1"}
	}
	if daemonSet {
		pod.Metadata.OwnerReferences = []OwnerReference{{APIVersion: "apps/v1", Kind: "DaemonSet", Name: "dcgm-exporter"}}
	}
	return pod
}

func TestClaimNVIDIARuntimeDaemonSetSeesTheGPUsItNames(t *testing.T) {
	s, client, url := newClaimServer(t, NewMockProvider())
	s.SetPodAdmission(NewPodAdmission(nil, []string{"gpu-monitoring"}))
	events := &reasonEvents{}
	s.SetPodEventRecorder(events)
	if _, err := client.Create(context.Background(), claimRequest("nc-gpu", "g5.12xlarge")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	runner := s.runner.(*MockContainerRunner)

	for _, tc := range []struct {
		pod    Pod
		gpus   []int
		any    int // with gpus nil, how many assigned GPUs, whichever they are
		warned bool
	}{
		{pod: nvidiaPod("gpu-monitoring", "requests", "all", 1, true), any: 1},
		{pod: nvidiaPod("gpu-monitoring", "dcgm", "all", 0, true), gpus: []int{0, 1, 2, 3}},
		{pod: nvidiaPod("gpu-monitoring", "dcgm-two", "0,2", 0, true), gpus: []int{0, 2}},
		{pod: nvidiaPod("gpu-monitoring", "none", "none", 0, true)},
		{pod: nvidiaPod("gpu-monitoring", "uuid", "GPU-8f2c", 0, true), warned: true},
		{pod: nvidiaPod("gpu-monitoring", "out-of-range", "4", 0, true), warned: true},
		{pod: nvidiaPod("other", "outside", "all", 0, true), warned: true},
		{pod: nvidiaPod("gpu-monitoring", "workload", "all", 0, false), warned: true},
	} {
		t.Run(tc.pod.Metadata.Name, func(t *testing.T) {
			if code := postClaimPod(t, url, tc.pod); code != http.StatusOK {
				t.Fatalf("create: HTTP %d", code)
			}
			req := runner.GetStartRequest(tc.pod.Metadata.UID)
			if req == nil {
				t.Fatal("the pod did not start")
			}
			if tc.any > 0 && len(req.AssignedGPUs) != tc.any || tc.any == 0 && !slices.Equal(req.AssignedGPUs, tc.gpus) {
				t.Errorf("GPUs = %v, want %v", req.AssignedGPUs, tc.gpus)
			}
			if warned := events.take(reasonNVIDIAVisibleDevicesIgnored) == 1; warned != tc.warned {
				t.Errorf("%s warning posted: %v, want %v", reasonNVIDIAVisibleDevicesIgnored, warned, tc.warned)
			}
		})
	}
}
