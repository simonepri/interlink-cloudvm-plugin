// Runs the embedded in-VM runner script against a fake nerdctl CLI to verify how it reports container start failures.
package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
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
