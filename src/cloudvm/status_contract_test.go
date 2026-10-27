// Package main tests that /status responses carry container statuses in the shape interLink 0.6.2 applies to pods.
// The tests decode responses into interLink's own status type and replay its by-name status merge.
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8stypes "k8s.io/apimachinery/pkg/types"
)

// interLinkPodStatus mirrors types.PodStatus in interLink 0.6.2 pkg/interlink/types.go.
type interLinkPodStatus struct {
	PodName        string                   `json:"name"`
	PodUID         string                   `json:"UID"`
	PodNamespace   string                   `json:"namespace"`
	JobID          string                   `json:"JID"`
	Containers     []corev1.ContainerStatus `json:"containers"`
	InitContainers []corev1.ContainerStatus `json:"initContainers"`
}

// applyInterLinkStatus replays how interLink 0.6.2 handleContainersUpdate merges remote statuses into the
// in-cluster pod: it replaces the status with the same container name and appends any name it does not know.
func applyInterLinkStatus(inCluster, remote []corev1.ContainerStatus) []corev1.ContainerStatus {
	for _, r := range remote {
		found := false
		for i := range inCluster {
			if inCluster[i].Name == r.Name {
				inCluster[i] = r
				found = true
				break
			}
		}
		if !found {
			inCluster = append(inCluster, r)
		}
	}
	return inCluster
}

// submittedStatuses returns the statuses the virtual kubelet sets on a pod when it submits it to the plugin.
func submittedStatuses(containers []corev1.Container) []corev1.ContainerStatus {
	out := make([]corev1.ContainerStatus, 0, len(containers))
	for _, c := range containers {
		out = append(out, corev1.ContainerStatus{
			Name:  c.Name,
			Image: c.Image,
			State: corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: "Remote pod submitted"}},
		})
	}
	return out
}

func newStatusContractServer(t *testing.T, uid string, result *ContainerStatusResult) *httptest.Server {
	t.Helper()
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	server := NewServer(NewMockProvider(), "test-cluster")

	runner := NewMockContainerRunner()
	runner.SetStatus(uid, result)
	server.SetContainerRunner(runner)

	capVec, _ := NewResourceVector("4", "16Gi", 1, 0)
	node := NewCloudVMNode("i-contract", ProviderAWS, "us-west-2", "100.64.0.60", capVec)
	reqVec, _ := NewResourceVector("1", "4Gi", 1, 0)
	server.NodePool().RegisterAndAllocate(node, reqVec, uid, "default", "contract-pod")

	server.mu.Lock()
	server.instances[uid] = &Instance{
		ID:        "i-contract",
		Provider:  ProviderAWS,
		Region:    "us-west-2",
		State:     StateRunning,
		CreatedAt: time.Now(),
	}
	server.mu.Unlock()

	ts := httptest.NewServer(server.Routes())
	t.Cleanup(ts.Close)
	return ts
}

// queryInterLinkStatus sends the pod list the way interLink's status handler does and decodes interLink's type.
func queryInterLinkStatus(t *testing.T, ts *httptest.Server, pod *corev1.Pod) interLinkPodStatus {
	t.Helper()
	body, err := json.Marshal([]*corev1.Pod{pod})
	if err != nil {
		t.Fatalf("marshal pods: %v", err)
	}
	req, err := http.NewRequest(http.MethodGet, ts.URL+"/status", strings.NewReader(string(body)))
	if err != nil {
		t.Fatalf("build request: %v", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("GET /status: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var statuses []interLinkPodStatus
	if err := json.NewDecoder(resp.Body).Decode(&statuses); err != nil {
		t.Fatalf("decode into interLink PodStatus: %v", err)
	}
	if len(statuses) != 1 {
		t.Fatalf("expected one pod status, got %d", len(statuses))
	}
	return statuses[0]
}

func contractPod(uid string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "contract-pod", Namespace: "default", UID: k8stypes.UID(uid)},
		Spec: corev1.PodSpec{
			InitContainers: []corev1.Container{{Name: "setup", Image: "busybox:1.37"}},
			Containers: []corev1.Container{
				{Name: "probe", Image: "nvidia/cuda:12.6.3-base-ubuntu24.04"},
				{Name: "sidecar", Image: "busybox:1.37"},
			},
		},
	}
}

func assertStatusNames(t *testing.T, kind string, got []corev1.ContainerStatus, want []corev1.Container) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s: expected %d statuses, got %d: %+v", kind, len(want), len(got), got)
	}
	for i := range want {
		if got[i].Name != want[i].Name {
			t.Errorf("%s[%d]: expected name %q, got %q", kind, i, want[i].Name, got[i].Name)
		}
	}
}

func TestStatusContract_RunningPodReportsEverySpecContainer(t *testing.T) {
	const uid = "uid-contract-running"
	startedAt := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	ts := newStatusContractServer(t, uid, &ContainerStatusResult{Running: true, Status: "running", StartedAt: startedAt})
	pod := contractPod(uid)

	st := queryInterLinkStatus(t, ts, pod)
	if st.PodUID != uid || st.PodName != pod.Name || st.PodNamespace != pod.Namespace || st.JobID == "" {
		t.Fatalf("unexpected pod identity: %+v", st)
	}
	assertStatusNames(t, "containers", st.Containers, pod.Spec.Containers)
	assertStatusNames(t, "initContainers", st.InitContainers, pod.Spec.InitContainers)
	for _, c := range st.Containers {
		if c.State.Running == nil || !c.State.Running.StartedAt.Time.Equal(startedAt) || !c.Ready {
			t.Errorf("container %s: expected ready running state started at %v, got %+v", c.Name, startedAt, c.State)
		}
	}
	for _, c := range st.InitContainers {
		if c.State.Terminated == nil || c.State.Terminated.ExitCode != 0 {
			t.Errorf("init container %s: expected successful terminated state, got %+v", c.Name, c.State)
		}
	}

	merged := applyInterLinkStatus(submittedStatuses(pod.Spec.Containers), st.Containers)
	assertStatusNames(t, "merged containers", merged, pod.Spec.Containers)
	for _, c := range merged {
		if c.State.Waiting != nil {
			t.Errorf("container %s still waiting after merge: %+v", c.Name, c.State.Waiting)
		}
	}
}

func TestStatusContract_FailedPodReportsTerminatedForEveryContainer(t *testing.T) {
	cases := []struct {
		name     string
		result   *ContainerStatusResult
		exitCode int32
		reason   string
		message  string
	}{
		{
			name:     "exited with error",
			result:   &ContainerStatusResult{Status: "exited", ExitCode: 1, Reason: "Error", Message: "worker exited"},
			exitCode: 1,
			reason:   "Error",
			message:  "worker exited",
		},
		{
			name: "start error",
			result: &ContainerStatusResult{
				Status:  "created",
				Reason:  "StartError",
				Message: `could not select device driver "" with capabilities: [[gpu]]`,
			},
			exitCode: 1,
			reason:   "StartError",
			message:  `could not select device driver "" with capabilities: [[gpu]]`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			const uid = "uid-contract-failed"
			ts := newStatusContractServer(t, uid, tc.result)
			pod := contractPod(uid)

			st := queryInterLinkStatus(t, ts, pod)
			assertStatusNames(t, "containers", st.Containers, pod.Spec.Containers)
			for _, c := range st.Containers {
				term := c.State.Terminated
				if term == nil {
					t.Fatalf("container %s: expected terminated state, got %+v", c.Name, c.State)
				}
				if term.ExitCode != tc.exitCode || term.Reason != tc.reason || term.Message != tc.message || term.FinishedAt.IsZero() {
					t.Errorf("container %s: unexpected terminated state %+v", c.Name, term)
				}
				if c.Ready || c.State.Running != nil || c.State.Waiting != nil {
					t.Errorf("container %s: terminated status must carry no other state: %+v", c.Name, c)
				}
			}

			// interLink sets Terminated.Reason on every in-cluster status once all remote containers terminate
			// with an error, so each in-cluster status must have been replaced by a terminated one.
			merged := applyInterLinkStatus(submittedStatuses(pod.Spec.Containers), st.Containers)
			assertStatusNames(t, "merged containers", merged, pod.Spec.Containers)
			for _, c := range merged {
				if c.State.Terminated == nil {
					t.Errorf("container %s has no terminated state after merge: %+v", c.Name, c.State)
				}
			}
		})
	}
}

func TestStatusContract_PodWithoutSpecFallsBackToDefaultContainerName(t *testing.T) {
	const uid = "uid-contract-nospec"
	ts := newStatusContractServer(t, uid, &ContainerStatusResult{Running: true, Status: "running"})
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "contract-pod", Namespace: "default", UID: uid}}

	st := queryInterLinkStatus(t, ts, pod)
	if len(st.Containers) != 1 || st.Containers[0].Name != defaultContainerName {
		t.Fatalf("expected one %q status, got %+v", defaultContainerName, st.Containers)
	}
	if len(st.InitContainers) != 0 {
		t.Fatalf("expected no init container statuses, got %+v", st.InitContainers)
	}
}

func TestStatusContract_ImagePullFailureWaits(t *testing.T) {
	for _, reason := range []string{"ErrImagePull", "ImagePullBackOff"} {
		t.Run(reason, func(t *testing.T) {
			const uid = "uid-contract-pull"
			ts := newStatusContractServer(t, uid, &ContainerStatusResult{Status: "created", Reason: reason, Message: "pull access denied"})
			pod := contractPod(uid)

			st := queryInterLinkStatus(t, ts, pod)
			assertStatusNames(t, "containers", st.Containers, pod.Spec.Containers)
			for _, c := range st.Containers {
				w := c.State.Waiting
				if w == nil || w.Reason != reason || w.Message != "pull access denied" {
					t.Fatalf("container %s: want waiting %s, got %+v", c.Name, reason, c.State)
				}
				if c.Ready || c.State.Terminated != nil || c.State.Running != nil {
					t.Errorf("container %s: a failing pull must neither run nor terminate: %+v", c.Name, c)
				}
			}
		})
	}
}
