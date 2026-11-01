// Runs the embedded in-VM runner script against a fake nerdctl CLI to verify how it reports container start failures.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

const fakeNerdctlStartError = `nerdctl: could not select device driver "" with capabilities: [[gpu]]`

// fakeNerdctl mimics a nerdctl CLI whose `run` creates the container but fails to start it, as a missing GPU runtime does.
const fakeNerdctl = `#!/usr/bin/env bash
state="$FAKE_NERDCTL_STATE"
case "$1" in
  pull) exit 0 ;;
  run) touch "$state/created"; echo '` + fakeNerdctlStartError + `' >&2; exit 125 ;;
  inspect)
    if [ -f "$state/created" ]; then
      printf '[{"State":{"Status":"created","Running":false,"ExitCode":128,"Error":"could not select device driver \\"\\" with capabilities: [[gpu]]","StartedAt":"0001-01-01T00:00:00Z","FinishedAt":"0001-01-01T00:00:00Z"}}]'
      exit 0
    fi
    echo "Error: No such object: $2" >&2; exit 1 ;;
  *) exit 0 ;;
esac
`

// extractRunnerScript returns the runner Python source rendered into the cloud-init script.
func extractRunnerScript(t *testing.T) string {
	t.Helper()
	script, err := RenderCloudInit(&CloudInitConfig{PodName: "p", PodUID: "uid", RunnerAuthToken: "tok"})
	if err != nil {
		t.Fatalf("render cloud-init: %v", err)
	}
	const begin = "cat << 'RUNNER_EOF' > /usr/local/bin/cloudvm-runner.py\n"
	start := strings.Index(script, begin)
	if start < 0 {
		t.Fatal("runner script not found in cloud-init")
	}
	body := script[start+len(begin):]
	end := strings.Index(body, "\nRUNNER_EOF\n")
	if end < 0 {
		t.Fatal("runner script terminator not found in cloud-init")
	}
	return body[:end]
}

type runnerHarness struct {
	t      *testing.T
	url    string
	token  string
	script string
	env    []string
}

func newRunnerHarness(t *testing.T) *runnerHarness {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 not available")
	}
	dir := t.TempDir()
	binDir := filepath.Join(dir, "bin")
	stateDir := filepath.Join(dir, "state")
	for _, d := range []string{binDir, stateDir} {
		if err := os.MkdirAll(d, 0o750); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(binDir, "nerdctl"), []byte(fakeNerdctl), 0o755); err != nil { //nolint:gosec // test fixture must be executable
		t.Fatal(err)
	}
	tokenPath := filepath.Join(dir, "runner-token")
	if err := os.WriteFile(tokenPath, []byte("tok"), 0o600); err != nil {
		t.Fatal(err)
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	_ = l.Close()

	src := extractRunnerScript(t)
	src = strings.Replace(src, "PORT = 9090", fmt.Sprintf("PORT = %d", port), 1)
	src = strings.ReplaceAll(src, "/etc/cloudvm/runner-token", tokenPath)
	src = strings.ReplaceAll(src, "/tmp/cloudvm-heartbeat", filepath.Join(dir, "heartbeat"))
	scriptPath := filepath.Join(dir, "runner.py")
	if err := os.WriteFile(scriptPath, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	return &runnerHarness{
		t:      t,
		url:    fmt.Sprintf("http://127.0.0.1:%d", port),
		token:  "tok",
		script: scriptPath,
		env:    append(os.Environ(), "PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"), "FAKE_NERDCTL_STATE="+stateDir),
	}
}

// start launches a fresh runner process, which begins with no in-memory start failures.
func (h *runnerHarness) start() func() {
	h.t.Helper()
	cmd := exec.Command("python3", h.script) //nolint:gosec // script rendered by the test
	cmd.Env = h.env
	if err := cmd.Start(); err != nil {
		h.t.Fatalf("start runner: %v", err)
	}
	stop := func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if resp, err := http.Get(h.url + "/healthz"); err == nil {
			_ = resp.Body.Close()
			return stop
		}
		time.Sleep(50 * time.Millisecond)
	}
	stop()
	h.t.Fatal("runner did not become healthy")
	return nil
}

func (h *runnerHarness) do(method, path, body string) *http.Response {
	h.t.Helper()
	req, err := http.NewRequest(method, h.url+path, strings.NewReader(body))
	if err != nil {
		h.t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+h.token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		h.t.Fatalf("%s %s: %v", method, path, err)
	}
	return resp
}

func (h *runnerHarness) status(podUID string) ContainerStatusResult {
	h.t.Helper()
	resp := h.do(http.MethodGet, "/containers/status?podUID="+podUID, "")
	defer func() { _ = resp.Body.Close() }()
	var res ContainerStatusResult
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		h.t.Fatalf("decode status (HTTP %d): %v", resp.StatusCode, err)
	}
	return res
}

func TestRunnerScript_ReportsStartFailure(t *testing.T) {
	h := newRunnerHarness(t)
	stop := h.start()

	resp := h.do(http.MethodPost, "/containers/start", `{"podUID":"gpu","image":"busybox","assignedGPUs":[0]}`)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("start returned HTTP %d", resp.StatusCode)
	}

	var res ContainerStatusResult
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		res = h.status("gpu")
		if res.Reason != "ContainerCreating" {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if res.Reason != "StartError" || res.Status != "dead" || res.ExitCode == 0 {
		t.Fatalf("expected a terminal StartError, got %+v", res)
	}
	if !strings.Contains(res.Message, "could not select device driver") {
		t.Fatalf("expected the nerdctl error in the message, got %q", res.Message)
	}
	stop()

	// A restarted runner has lost its in-memory failure record and must read the failure from nerdctl inspect.
	stop = h.start()
	defer stop()
	res = h.status("gpu")
	if res.Reason != "StartError" || res.Status != "dead" || res.ExitCode == 0 {
		t.Fatalf("expected StartError from nerdctl inspect after restart, got %+v", res)
	}
	if !strings.Contains(res.Message, "could not select device driver") {
		t.Fatalf("expected the nerdctl state error in the message, got %q", res.Message)
	}
}

// fakePullingNerdctl logs each pull with the credential directory it saw and fails pulls of refs listed in $state/fail.
const fakePullingNerdctl = `#!/usr/bin/env bash
state="$FAKE_NERDCTL_STATE"
case "$1" in
  --snapshotter=stargz)
    if [ "$2" = pull ]; then
      echo "stargz $3" >> "$state/pulls"
      echo 'level=fatal msg="unable to initialize unpacker: no unpack platforms defined: invalid argument"' >&2; exit 1
    fi
    echo "$@" > "$state/run"; touch "$state/created"; exit 0 ;;
  pull)
    echo "$2 $DOCKER_CONFIG" >> "$state/pulls"
    cat "$DOCKER_CONFIG/config.json" > "$state/config-at-pull" 2>/dev/null
    if grep -qxF "$2" "$state/fail" 2>/dev/null; then echo "pull access denied for $2: no basic auth credentials" >&2; exit 1; fi
    exit 0 ;;
  run) echo "$@" > "$state/run"; touch "$state/created"; exit 0 ;;
  inspect)
    if [ -f "$state/created" ]; then
      printf '[{"State":{"Status":"running","Running":true,"ExitCode":0,"StartedAt":"2026-01-01T00:00:00Z","FinishedAt":"0001-01-01T00:00:00Z"}}]'
      exit 0
    fi
    exit 1 ;;
  *) exit 0 ;;
esac
`

func newPullingRunnerHarness(t *testing.T, failRefs ...string) (*runnerHarness, string) {
	t.Helper()
	h := newRunnerHarness(t)
	dir := filepath.Dir(h.script)
	if err := os.WriteFile(filepath.Join(dir, "bin", "nerdctl"), []byte(fakePullingNerdctl), 0o755); err != nil { //nolint:gosec // test fixture must be executable
		t.Fatal(err)
	}
	state := filepath.Join(dir, "state")
	if err := os.WriteFile(filepath.Join(state, "fail"), []byte(strings.Join(failRefs, "\n")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	mirrors := filepath.Join(dir, "mirrors.json")
	if err := os.WriteFile(mirrors, []byte(`{"docker.io":"123456789012.dkr.ecr.eu-west-1.amazonaws.com/docker-hub"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	h.env = append(h.env, "CLOUDVM_IMAGE_MIRRORS="+mirrors, "CLOUDVM_DOCKER_CONFIG="+filepath.Join(dir, "docker"),
		"CLOUDVM_PULL_BACKOFF_START=0.2", "CLOUDVM_PULL_BACKOFF_MAX=0.4")
	return h, state
}

func waitStatus(t *testing.T, h *runnerHarness, uid string, ok func(ContainerStatusResult) bool) {
	t.Helper()
	var res ContainerStatusResult
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if res = h.status(uid); ok(res) {
			return
		}
	}
	t.Fatalf("status never matched, last %+v", res)
}

const mirroredRay = "123456789012.dkr.ecr.eu-west-1.amazonaws.com/docker-hub/rayproject/ray:2.9"

func TestRunnerScript_PullFailureBacksOffThenRecovers(t *testing.T) {
	h, state := newPullingRunnerHarness(t, mirroredRay, "rayproject/ray:2.9")
	defer h.start()()

	resp := h.do(http.MethodPost, "/containers/start", `{"podUID":"pull","image":"rayproject/ray:2.9"}`)
	_ = resp.Body.Close()

	seen := map[string]bool{}
	waitStatus(t, h, "pull", func(r ContainerStatusResult) bool {
		if r.Running || r.Status == "dead" || r.ExitCode != 0 {
			t.Fatalf("a failing pull must not terminate the container: %+v", r)
		}
		seen[r.Reason] = true
		return seen["ErrImagePull"] && seen["ImagePullBackOff"]
	})
	if r := h.status("pull"); !strings.Contains(r.Message, "no basic auth credentials") {
		t.Errorf("status should carry the pull error, got %+v", r)
	}

	if err := os.WriteFile(filepath.Join(state, "fail"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, h, "pull", func(r ContainerStatusResult) bool { return r.Running })
	pulls, _ := os.ReadFile(filepath.Join(state, "pulls"))
	if n := strings.Count(string(pulls), "\n"); n < 3 {
		t.Errorf("want the mirror and upstream tried, then the mirror again, got %q", pulls)
	}
}

func TestRunnerScript_PullFallsBackToUpstreamWhenMirrorFails(t *testing.T) {
	h, state := newPullingRunnerHarness(t, mirroredRay)
	defer h.start()()

	resp := h.do(http.MethodPost, "/containers/start", `{"podUID":"fallback","image":"rayproject/ray:2.9"}`)
	_ = resp.Body.Close()
	waitStatus(t, h, "fallback", func(r ContainerStatusResult) bool { return r.Running })

	run, _ := os.ReadFile(filepath.Join(state, "run"))
	if !strings.Contains(string(run), " rayproject/ray:2.9") || strings.Contains(string(run), "ecr") {
		t.Errorf("container must run the upstream reference after the mirror failed, got %q", run)
	}
}

func TestRunnerScript_ECRPullUsesCredentialHelper(t *testing.T) {
	h, state := newPullingRunnerHarness(t)
	defer h.start()()

	for _, body := range []string{
		`{"podUID":"plain","image":"rayproject/ray:2.9"}`,
		`{"podUID":"secret","image":"rayproject/ray:2.9","dockerConfigJSON":"{\"auths\":{\"ghcr.io\":{\"auth\":\"eDp5\"}}}"}`,
	} {
		resp := h.do(http.MethodPost, "/containers/start", body)
		_ = resp.Body.Close()
	}
	waitStatus(t, h, "plain", func(r ContainerStatusResult) bool { return r.Running })
	// Both pods share the fake's container state, so wait for the second pull itself.
	waitStatus(t, h, "secret", func(ContainerStatusResult) bool {
		pulls, _ := os.ReadFile(filepath.Join(state, "pulls")) //nolint:gosec // test-controlled path
		return strings.Count(string(pulls), "\n") >= 2
	})

	hostCfg, _ := os.ReadFile(filepath.Join(filepath.Dir(h.script), "docker", "config.json"))
	if !strings.Contains(string(hostCfg), `"123456789012.dkr.ecr.eu-west-1.amazonaws.com": "ecr-login"`) && !strings.Contains(string(hostCfg), `"123456789012.dkr.ecr.eu-west-1.amazonaws.com":"ecr-login"`) {
		t.Errorf("host config must map the ECR host to the helper for nerdctl and the snapshotter, got %s", hostCfg)
	}
	pulls, _ := os.ReadFile(filepath.Join(state, "pulls"))
	lines := strings.Split(strings.TrimSpace(string(pulls)), "\n")
	perPod := 0
	for _, l := range lines {
		if strings.Contains(l, "docker-cfg-") {
			perPod++
		}
	}
	if len(lines) != 2 || perPod != 1 {
		t.Errorf("one pull must use the host config and one a per-pod directory, got %q", pulls)
	}
}

func TestRunnerScript_StargzUnpackErrorFallsBackToDefaultSnapshotter(t *testing.T) {
	h, state := newPullingRunnerHarness(t)
	sock := filepath.Join(filepath.Dir(h.script), "stargz.sock")
	if err := os.WriteFile(sock, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	h.env = append(h.env, "CLOUDVM_STARGZ_SOCKET="+sock)
	defer h.start()()

	resp := h.do(http.MethodPost, "/containers/start", `{"podUID":"lazy","image":"ghcr.io/example/app:1"}`)
	_ = resp.Body.Close()
	waitStatus(t, h, "lazy", func(r ContainerStatusResult) bool { return r.Running })

	pulls, _ := os.ReadFile(filepath.Join(state, "pulls"))
	if !strings.HasPrefix(string(pulls), "stargz ghcr.io/example/app:1\nghcr.io/example/app:1 ") {
		t.Errorf("want a stargz pull then a default-snapshotter pull, got %q", pulls)
	}
	run, _ := os.ReadFile(filepath.Join(state, "run"))
	if strings.Contains(string(run), "--snapshotter") || !strings.Contains(string(run), "ghcr.io/example/app:1") {
		t.Errorf("run must use the default snapshotter the image was pulled into, got %q", run)
	}
}

// fakeRunningNerdctl reports a running container the way nerdctl does: no finish time, and a start time with sub-second digits.
const fakeRunningNerdctl = `#!/usr/bin/env bash
case "$1" in
  inspect) printf '[{"State":{"Status":"running","Running":true,"Pid":42,"ExitCode":0,"StartedAt":"2026-01-02T03:04:05.123456789Z","FinishedAt":""}}]'; exit 0 ;;
  *) exit 0 ;;
esac
`

func TestRunnerScript_RunningContainerStatusDecodesInGoClient(t *testing.T) {
	h := newRunnerHarness(t)
	if err := os.WriteFile(filepath.Join(filepath.Dir(h.script), "bin", "nerdctl"), []byte(fakeRunningNerdctl), 0o755); err != nil { //nolint:gosec // test fixture must be executable
		t.Fatal(err)
	}
	defer h.start()()

	var port int
	if _, err := fmt.Sscanf(h.url, "http://127.0.0.1:%d", &port); err != nil {
		t.Fatal(err)
	}
	node := &CloudVMNode{InstanceID: "i-running", MeshIP: "127.0.0.1", RunnerAuthToken: h.token}
	res, err := NewHTTPContainerRunner(port).GetStatus(context.Background(), node, "running-pod")
	if err != nil {
		t.Fatalf("GetStatus must decode the runner's reply for a running container: %v", err)
	}
	if !res.Running || res.Status != "running" || res.StartedAt.IsZero() {
		t.Errorf("want a running container with its start time, got %+v", res)
	}
}

// fakeRemovingNerdctl creates $state/created on run and deletes it on rm unless $state/stuck exists.
const fakeRemovingNerdctl = `#!/usr/bin/env bash
state="$FAKE_NERDCTL_STATE"
case "$1" in
  run) touch "$state/created"; exit 0 ;;
  rm) [ -f "$state/stuck" ] || rm -f "$state/created"; exit 0 ;;
  inspect)
    if [ -f "$state/created" ]; then
      printf '[{"State":{"Status":"exited","Running":false,"ExitCode":0}}]'
      exit 0
    fi
    echo "Error: No such object: $2" >&2; exit 1 ;;
  *) exit 0 ;;
esac
`

func TestRunnerScript_StopRemovesContainer(t *testing.T) {
	h := newRunnerHarness(t)
	stateDir := strings.TrimPrefix(h.env[len(h.env)-1], "FAKE_NERDCTL_STATE=")
	binDir := filepath.Join(filepath.Dir(stateDir), "bin")
	if err := os.WriteFile(filepath.Join(binDir, "nerdctl"), []byte(fakeRemovingNerdctl), 0o755); err != nil { //nolint:gosec // test fixture must be executable
		t.Fatal(err)
	}
	defer h.start()()

	created := filepath.Join(stateDir, "created")
	startAndWait := func() {
		t.Helper()
		resp := h.do(http.MethodPost, "/containers/start", `{"podUID":"p","image":"busybox"}`)
		_ = resp.Body.Close()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := os.Stat(created); err == nil {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatal("container never ran")
	}

	startAndWait()
	resp := h.do(http.MethodPost, "/containers/stop?podUID=p", "")
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("stop returned HTTP %d", resp.StatusCode)
	}
	if _, err := os.Stat(created); !os.IsNotExist(err) {
		t.Fatalf("expected stop to remove the container, stat: %v", err)
	}

	// A container that survives removal fails the stop, so the plugin never reuses the VM.
	startAndWait()
	if err := os.WriteFile(filepath.Join(stateDir, "stuck"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	resp = h.do(http.MethodPost, "/containers/stop?podUID=p", "")
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("expected HTTP 500 when the container remains, got %d", resp.StatusCode)
	}
}

func TestRunnerScript_StopWaitsForTheAgentToShipTheContainersOutput(t *testing.T) {
	h := newRunnerHarness(t)
	stateDir := strings.TrimPrefix(h.env[len(h.env)-1], "FAKE_NERDCTL_STATE=")
	binDir := filepath.Join(filepath.Dir(stateDir), "bin")
	if err := os.WriteFile(filepath.Join(binDir, "nerdctl"), []byte(fakeRemovingNerdctl), 0o755); err != nil { //nolint:gosec // test fixture must be executable
		t.Fatal(err)
	}
	drainDir := filepath.Join(filepath.Dir(stateDir), "log-drain")
	if err := os.MkdirAll(drainDir, 0o750); err != nil {
		t.Fatal(err)
	}
	src, err := os.ReadFile(h.script)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(h.script, []byte(strings.ReplaceAll(string(src), "/run/cloudvm/log-drain", drainDir)), 0o600); err != nil { //nolint:gosec // test-controlled script path
		t.Fatal(err)
	}
	defer h.start()()

	created := filepath.Join(stateDir, "created")
	resp := h.do(http.MethodPost, "/containers/start", `{"podUID":"p","image":"busybox"}`)
	_ = resp.Body.Close()
	waitForWithin(t, 10*time.Second, "container run", func() bool { _, err := os.Stat(created); return err == nil })
	drainFile := filepath.Join(drainDir, "cloudvm-p") // the agent follows the container
	if err := os.WriteFile(drainFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}

	stopped := make(chan int, 1)
	go func() {
		resp := h.do(http.MethodPost, "/containers/stop?podUID=p", "")
		_ = resp.Body.Close()
		stopped <- resp.StatusCode
	}()
	time.Sleep(500 * time.Millisecond)
	if _, err := os.Stat(created); err != nil {
		t.Fatal("the runner removed the container, and its output, before the agent shipped it")
	}
	if err := os.Remove(drainFile); err != nil { // the agent shipped the last line
		t.Fatal(err)
	}
	select {
	case code := <-stopped:
		if code != http.StatusOK {
			t.Fatalf("stop returned HTTP %d", code)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("stop kept waiting after the agent shipped the output")
	}
	if _, err := os.Stat(created); !os.IsNotExist(err) {
		t.Fatalf("expected stop to remove the container, stat: %v", err)
	}
}

func TestRunnerScript_ReportsStartTimings(t *testing.T) {
	h := newRunnerHarness(t)
	defer h.start()()

	before := time.Now().Add(-time.Second)
	resp := h.do(http.MethodPost, "/containers/start", `{"podUID":"timed","image":"busybox"}`)
	_ = resp.Body.Close()
	var res ContainerStatusResult
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if res = h.status("timed"); res.Timings != nil && !res.Timings.PullFinishedAt.IsZero() {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	tm := res.Timings
	if tm == nil || tm.AcceptedAt.Before(before) || tm.PullStartedAt.Before(tm.AcceptedAt) || tm.PullFinishedAt.Before(tm.PullStartedAt) {
		t.Fatalf("want ordered accept, pull start and pull end times, got %+v", tm)
	}
	if tm.Image != "busybox" || tm.PullAttempts != 1 || tm.LazyPull {
		t.Errorf("want one full pull of busybox, got %+v", tm)
	}
}

func TestRunnerScript_GivesContainersTheNamespaceSearchList(t *testing.T) {
	for _, tc := range []struct {
		name, resolv, body, want string
	}{
		{"the tunnel's cluster domain gets the pod's namespace first", "nameserver 127.0.0.153\nsearch svc.cluster.local cluster.local ec2.internal\n",
			`{"podUID":"dns","namespace":"team-a","image":"busybox"}`,
			"--dns-search team-a.svc.cluster.local --dns-search svc.cluster.local --dns-search cluster.local --dns-search ec2.internal"},
		{"a host without the tunnel's DNS keeps its own list", "nameserver 10.0.0.2\nsearch ec2.internal\n",
			`{"podUID":"dns","namespace":"team-a","image":"busybox"}`, ""},
		{"a pod without a namespace keeps the host's list", "nameserver 127.0.0.153\nsearch svc.cluster.local cluster.local\n",
			`{"podUID":"dns","image":"busybox"}`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h, state := newPullingRunnerHarness(t)
			resolv := filepath.Join(filepath.Dir(h.script), "resolv.conf")
			if err := os.WriteFile(resolv, []byte(tc.resolv), 0o600); err != nil {
				t.Fatal(err)
			}
			h.env = append(h.env, "CLOUDVM_RESOLV_CONF="+resolv)
			defer h.start()()
			resp := h.do(http.MethodPost, "/containers/start", tc.body)
			_ = resp.Body.Close()
			waitStatus(t, h, "dns", func(r ContainerStatusResult) bool { return r.Running })
			run, _ := os.ReadFile(filepath.Join(state, "run"))
			got := strings.Join(regexpDNSSearch.FindAllString(string(run), -1), " ")
			if got != tc.want {
				t.Fatalf("dns search flags = %q, want %q (run: %s)", got, tc.want, run)
			}
		})
	}
}

var regexpDNSSearch = regexp.MustCompile(`--dns-search \S+`)

// fakeStargzNerdctl pulls with the stargz snapshotter and reports two layers; fakeCtr labels the snapshot named in
// $state/remote as one the snapshotter mounted remotely.
const fakeStargzNerdctl = `#!/usr/bin/env bash
state="$FAKE_NERDCTL_STATE"
[ "$1" = --snapshotter=stargz ] && shift
case "$1" in
  pull) exit 0 ;;
  image)
    # Like nerdctl, inspect finds no image by a name@digest reference.
    case "${@: -1}" in *@*) echo "level=fatal msg=\"no such image: ${@: -1}\"" >&2; exit 1 ;; esac
    printf '["sha256:aaaa","sha256:bbbb"]\n'; exit 0 ;;
  run) touch "$state/created"; exit 0 ;;
  inspect)
    if [ -f "$state/created" ]; then
      printf '[{"State":{"Status":"running","Running":true,"ExitCode":0,"StartedAt":"2026-01-01T00:00:00Z","FinishedAt":"0001-01-01T00:00:00Z"}}]'
      exit 0
    fi
    exit 1 ;;
  *) exit 0 ;;
esac
`

const fakeCtr = `#!/usr/bin/env bash
state="$FAKE_NERDCTL_STATE"
name="${@: -1}"
if grep -qxF "$name" "$state/remote" 2>/dev/null; then
  printf '{"Name":"%s","Labels":{"containerd.io/snapshot/remote":"remote snapshot"}}\n' "$name"
else
  printf '{"Name":"%s","Labels":{}}\n' "$name"
fi
`

func TestRunnerScript_LazyPullReportsOnlyRemoteMounts(t *testing.T) {
	// Layer snapshots are named by chain ID: the second is the SHA-256 of "<first chain ID> <second diff ID>".
	const secondChain = "sha256:2c4d8760379f05d0ad1ed610712309f0114e7345e6e6a351494cff030ffdf197"
	for _, tc := range []struct {
		name, image, remote string
		want                bool
	}{
		{"remote mount", "ghcr.io/example/app:1", secondChain, true},
		{"local unpack", "ghcr.io/example/app:1", "", false},
		{"remote mount of an image pinned by digest", "ghcr.io/example/app@sha256:" + strings.Repeat("80d4", 16), secondChain, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newRunnerHarness(t)
			dir := filepath.Dir(h.script)
			for name, body := range map[string]string{"nerdctl": fakeStargzNerdctl, "ctr": fakeCtr} {
				if err := os.WriteFile(filepath.Join(dir, "bin", name), []byte(body), 0o755); err != nil { //nolint:gosec // test fixture must be executable
					t.Fatal(err)
				}
			}
			if err := os.WriteFile(filepath.Join(dir, "state", "remote"), []byte(tc.remote+"\n"), 0o600); err != nil {
				t.Fatal(err)
			}
			sock := filepath.Join(dir, "stargz.sock")
			if err := os.WriteFile(sock, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			h.env = append(h.env, "CLOUDVM_STARGZ_SOCKET="+sock)
			defer h.start()()

			resp := h.do(http.MethodPost, "/containers/start", `{"podUID":"lazy","image":"`+tc.image+`"}`)
			_ = resp.Body.Close()
			waitStatus(t, h, "lazy", func(r ContainerStatusResult) bool { return r.Timings != nil && !r.Timings.PullFinishedAt.IsZero() })
			if got := h.status("lazy").Timings.LazyPull; got != tc.want {
				t.Errorf("lazyPull = %v, want %v", got, tc.want)
			}
		})
	}
}
