// Tests that pods launch in the region of the node they are bound to, and that a pod with no determinable region is refused.
package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

type stubNodeRegions map[string]string

func (s stubNodeRegions) NodeRegion(name string) string { return s[name] }

type recordedPodEvent struct{ reason, message string }

type captureEvents struct{ events []recordedPodEvent }

func (c *captureEvents) RecordPodEvent(_ context.Context, _, _, _, _, reason, message string) error {
	c.events = append(c.events, recordedPodEvent{reason, message})
	return nil
}

func TestCreateLaunchesInBoundNodeRegion(t *testing.T) {
	t.Setenv("NODE_REGIONS", `{"virtual-node-region-a":"eu-west-1","virtual-node-both":"eu-west-1"}`)
	labels := stubNodeRegions{"virtual-node-labeled": "us-east-1", "virtual-node-both": "us-east-2"}

	cases := []struct {
		name        string
		nodeName    string
		annotations string
		selector    string
		want        string // empty means the pod is refused
	}{
		{name: "labeled node outside the map", nodeName: "virtual-node-labeled", want: "us-east-1"},
		{name: "unlabeled node in the map", nodeName: "virtual-node-region-a", want: "eu-west-1"},
		{name: "label beats the map", nodeName: "virtual-node-both", want: "us-east-2"},
		{name: "annotation overrides label", nodeName: "virtual-node-labeled", annotations: `"cloudvm.vk.io/region": "ap-south-1"`, want: "ap-south-1"},
		{name: "annotation overrides map", nodeName: "virtual-node-region-a", annotations: `"cloudvm.vk.io/region": "ap-south-1"`, want: "ap-south-1"},
		{name: "node selector when the node names no region", nodeName: "virtual-node-other", selector: `"topology.kubernetes.io/region": "eu-central-1"`, want: "eu-central-1"},
		{name: "unknown node is refused", nodeName: "virtual-node-other"},
		{name: "unbound pod is refused"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockProvider()
			server := NewServer(mock, "test-cluster")
			server.SetDefaultRegion("us-west-2")
			server.SetNodeRegionLookup(labels)
			events := &captureEvents{}
			server.SetPodEventRecorder(events)
			ts := httptest.NewServer(server.Routes())
			defer ts.Close()

			payload := `{"pod": {"metadata": {"name": "worker", "namespace": "default", "uid": "uid-` + strings.ReplaceAll(tc.name, " ", "-") + `", "annotations": {` + tc.annotations + `}},
				"spec": {"nodeName": "` + tc.nodeName + `", "nodeSelector": {` + tc.selector + `}, "containers": [{"name": "main", "image": "ubuntu:24.04"}]}}}`
			resp, err := http.Post(ts.URL+"/create", "application/json", strings.NewReader(payload))
			if err != nil {
				t.Fatalf("POST /create failed: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()
			if tc.want == "" {
				if resp.StatusCode != http.StatusBadRequest {
					t.Fatalf("expected 400 from /create, got %d", resp.StatusCode)
				}
				if mock.LastSpec() != nil {
					t.Fatalf("refused pod launched in %q", mock.LastSpec().Region)
				}
				if len(events.events) != 1 || events.events[0].reason != "CloudVMRegionUnknown" {
					t.Errorf("events = %+v, want one CloudVMRegionUnknown", events.events)
				}
				return
			}
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("expected 200 from /create, got %d", resp.StatusCode)
			}
			spec := mock.LastSpec()
			if spec == nil {
				t.Fatal("expected a launch")
			}
			if spec.Region != tc.want {
				t.Errorf("region = %q, want %q", spec.Region, tc.want)
			}
		})
	}
}

func TestNodeWatchReadsRegionLabel(t *testing.T) {
	client := fake.NewSimpleClientset(
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "virtual-node-use1-l4-spot", Labels: map[string]string{regionLabel: "us-east-1", "other": "x"}}},
		&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "unlabeled"}},
	)
	watch := NewNodeWatch(client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watch.Start(ctx, 5*time.Second)
	if got := watch.NodeRegion("virtual-node-use1-l4-spot"); got != "us-east-1" {
		t.Errorf("labeled node region = %q, want us-east-1", got)
	}
	if got := watch.NodeRegion("unlabeled"); got != "" {
		t.Errorf("unlabeled node region = %q, want empty", got)
	}
	if got := watch.NodeRegion("missing"); got != "" {
		t.Errorf("missing node region = %q, want empty", got)
	}
}

func TestNodeWatchStartGivesUpWhenListFails(t *testing.T) {
	client := fake.NewSimpleClientset(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "n", Labels: map[string]string{regionLabel: "us-east-1"}}})
	client.PrependReactor("list", "nodes", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("forbidden")
	})
	watch := NewNodeWatch(client)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	if watch.Start(ctx, 200*time.Millisecond) {
		t.Fatal("Start reported a sync although every list fails")
	}
	if time.Since(start) > 5*time.Second {
		t.Errorf("Start blocked for %s", time.Since(start))
	}
	if got := watch.NodeRegion("n"); got != "" {
		t.Errorf("unsynced watch answered %q, want empty", got)
	}
}
