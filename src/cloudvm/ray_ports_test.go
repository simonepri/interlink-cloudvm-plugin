// Tests that KubeRay pods start with their fixed Ray agent ports set to 0, so several Ray nodes can share one VM.
package main

import (
	"context"
	"net/http"
	"slices"
	"testing"
)

// kubeRayWorkerArgs is the worker command KubeRay generates, with its fixed metrics and dashboard agent ports.
const kubeRayWorkerArgs = "ulimit -n 65536; ray start  --address=head-svc.ns.svc.cluster.local:6379  --block  --dashboard-agent-listen-port=52365  --memory=12884901888  --metrics-export-port=8080  --node-ip-address=$(hostname -I | awk '{print $1}')  --num-cpus=3  --num-gpus=1 "

const freedKubeRayWorkerArgs = "ulimit -n 65536; ray start  --address=head-svc.ns.svc.cluster.local:6379  --block  --dashboard-agent-listen-port=0  --memory=12884901888  --metrics-export-port=0  --node-ip-address=$(hostname -I | awk '{print $1}')  --num-cpus=3  --num-gpus=1 "

func kubeRayWorkerPod(name, node string) Pod {
	return Pod{
		Metadata: PodMeta{Name: name, Namespace: "team-a", UID: "uid-" + name, Labels: map[string]string{rayNodeTypeLabel: "worker"}},
		Spec: PodSpecWrapper{NodeName: node, Containers: []ContainerSpec{{
			Name: "ray-worker", Image: "rayproject/ray", Command: []string{"/bin/bash", "-c", "--"}, Args: []string{kubeRayWorkerArgs},
			Resources: ContainerResourceRequirements{Limits: map[string]string{"cpu": "1", "memory": "1Gi", "nvidia.com/gpu": "1"}},
		}}},
	}
}

func TestFreeRayFixedPorts(t *testing.T) {
	cases := []struct {
		name   string
		labels map[string]string
		args   []string
		want   []string
	}{
		{"KubeRay command", map[string]string{rayNodeTypeLabel: "worker"}, []string{kubeRayWorkerArgs}, []string{freedKubeRayWorkerArgs}},
		{"separate words", map[string]string{rayNodeTypeLabel: "head"},
			[]string{"ray", "start", "--metrics-export-port", "8080", "--dashboard-agent-listen-port 52365", "--port=6379"},
			[]string{"ray", "start", "--metrics-export-port", "0", "--dashboard-agent-listen-port=0", "--port=6379"}},
		{"other flags", map[string]string{rayNodeTypeLabel: "worker"},
			[]string{"--metrics-export-port-x=1 --dashboard-port=8265 --node-manager-port=6380"},
			[]string{"--metrics-export-port-x=1 --dashboard-port=8265 --node-manager-port=6380"}},
		{"not a Ray pod", nil, []string{kubeRayWorkerArgs}, []string{kubeRayWorkerArgs}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pod := &Pod{
				Metadata: PodMeta{Labels: tc.labels},
				Spec:     PodSpecWrapper{Containers: []ContainerSpec{{Name: "c", Command: slices.Clone(tc.args), Args: slices.Clone(tc.args)}}},
			}
			freeRayFixedPorts(pod)
			if c := pod.Spec.Containers[0]; !slices.Equal(c.Command, tc.want) || !slices.Equal(c.Args, tc.want) {
				t.Errorf("command = %q, args = %q, want %q", c.Command, c.Args, tc.want)
			}
		})
	}
}

func TestKubeRayWorkersOnOneClaimVMStartWithFreePorts(t *testing.T) {
	s, client, url := newClaimServer(t, NewMockProvider())
	if _, err := client.Create(context.Background(), claimRequest("nc-ray", "g5.12xlarge")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	runner := s.runner.(*MockContainerRunner)
	for _, name := range []string{"w1", "w2"} {
		if code := postClaimPod(t, url, kubeRayWorkerPod(name, "nc-ray")); code != http.StatusOK {
			t.Fatalf("create %s: HTTP %d", name, code)
		}
		req := runner.GetStartRequest("uid-" + name)
		if req == nil {
			t.Fatalf("%s did not start", name)
		}
		if !slices.Equal(req.Args, []string{freedKubeRayWorkerArgs}) {
			t.Errorf("%s args = %q, want the fixed agent ports set to 0", name, req.Args)
		}
	}
}
