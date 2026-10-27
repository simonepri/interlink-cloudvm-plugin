// Runs the in-VM runner's credential endpoint with a fake nerdctl and checks the credentials and env it gives containers.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// fakeRecordingNerdctl writes the arguments of each `run` to $state/run-<container name>, one per line.
const fakeRecordingNerdctl = `#!/usr/bin/env bash
state="$FAKE_NERDCTL_STATE"
case "$1" in
  run)
    name=""; prev=""
    for a in "$@"; do [ "$prev" = "--name" ] && name="$a"; prev="$a"; done
    printf '%s\n' "$@" > "$state/run-$name.tmp" && mv "$state/run-$name.tmp" "$state/run-$name"
    exit 0 ;;
  inspect) echo "Error: No such object: $2" >&2; exit 1 ;;
  *) exit 0 ;;
esac
`

func freeLoopbackAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	return addr
}

// runArgs waits for the fake nerdctl to record the run of the pod's container and returns its -e values.
func runEnv(t *testing.T, stateDir, podUID string) map[string]string {
	t.Helper()
	path := filepath.Join(stateDir, "run-cloudvm-"+podUID)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		raw, err := os.ReadFile(path) //nolint:gosec // path built by the test
		if err == nil {
			env := map[string]string{}
			args := strings.Split(strings.TrimSpace(string(raw)), "\n")
			for i := 0; i+1 < len(args); i++ {
				if args[i] == "-e" {
					k, v, _ := strings.Cut(args[i+1], "=")
					env[k] = v
				}
			}
			return env
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("container of %s never ran", podUID)
	return nil
}

type credentialResponse struct {
	AccessKeyID     string `json:"AccessKeyId"`
	SecretAccessKey string `json:"SecretAccessKey"`
	Token           string `json:"Token"`
	Expiration      string `json:"Expiration"`
}

func getCredentials(t *testing.T, uri, secret string) (int, credentialResponse) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, uri, nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", secret)
	var resp *http.Response
	deadline := time.Now().Add(10 * time.Second)
	for {
		resp, err = http.DefaultClient.Do(req)
		if err == nil || time.Now().After(deadline) {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("GET %s: %v", uri, err)
	}
	defer func() { _ = resp.Body.Close() }()
	var out credentialResponse
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp.StatusCode, out
}

func TestRunnerScript_ServesPodRoleCredentials(t *testing.T) {
	h := newRunnerHarness(t)
	binDir := filepath.Join(t.TempDir(), "bin")
	stateDir := t.TempDir()
	if err := os.MkdirAll(binDir, 0o750); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "nerdctl"), []byte(fakeRecordingNerdctl), 0o755); err != nil { //nolint:gosec // test fixture must be executable
		t.Fatal(err)
	}
	credAddr := freeLoopbackAddr(t)
	h.env = append(h.env,
		"PATH="+binDir+string(os.PathListSeparator)+os.Getenv("PATH"),
		"FAKE_NERDCTL_STATE="+stateDir,
		"CLOUDVM_CREDENTIALS_ADDR="+credAddr,
	)
	defer h.start()()

	post := func(path, body string) {
		t.Helper()
		resp := h.do(http.MethodPost, path, body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("%s returned HTTP %d", path, resp.StatusCode)
		}
	}
	credsJSON := func(key string, expiration int64) string {
		return fmt.Sprintf(`{"accessKeyId":%q,"secretAccessKey":"secret-%s","sessionToken":"session-%s","expiration":%d}`, key, key, key, expiration)
	}
	exp := time.Now().Add(time.Hour).Unix()
	post("/containers/start", `{"podUID":"pi","image":"busybox","env":{"FOO":"bar"},"awsCredentials":`+credsJSON("ASIA1", exp)+`}`)
	post("/containers/start", `{"podUID":"late","image":"busybox","awsCredentials":{}}`)
	post("/containers/start", `{"podUID":"plain","image":"busybox","env":{"FOO":"bar"}}`)

	piEnv := runEnv(t, stateDir, "pi")
	uri := piEnv["AWS_CONTAINER_CREDENTIALS_FULL_URI"]
	piSecret := piEnv["AWS_CONTAINER_AUTHORIZATION_TOKEN"]
	if uri != "http://"+credAddr+"/v1/credentials" || len(piSecret) < 32 || piEnv["FOO"] != "bar" {
		t.Fatalf("unexpected container env %v", piEnv)
	}
	for k := range runEnv(t, stateDir, "plain") {
		if strings.HasPrefix(k, "AWS_") {
			t.Fatalf("pod without a role got %s", k)
		}
	}

	if code, _ := getCredentials(t, uri, "not-a-pod-secret"); code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for an unknown secret, got %d", code)
	}
	code, creds := getCredentials(t, uri, piSecret)
	if code != http.StatusOK || creds.AccessKeyID != "ASIA1" || creds.SecretAccessKey != "secret-ASIA1" || creds.Token != "session-ASIA1" {
		t.Fatalf("unexpected credentials (HTTP %d): %+v", code, creds)
	}
	if got, err := time.Parse(time.RFC3339, creds.Expiration); err != nil || got.Unix() != exp {
		t.Fatalf("expiration %q is not %d in RFC 3339: %v", creds.Expiration, exp, err)
	}

	// Pushed credentials replace the old ones, and older credentials never replace newer ones.
	post("/containers/credentials?podUID=pi", credsJSON("ASIA2", exp+1800))
	post("/containers/credentials?podUID=pi", credsJSON("ASIA-STALE", exp))
	if _, creds = getCredentials(t, uri, piSecret); creds.AccessKeyID != "ASIA2" {
		t.Fatalf("expected the pushed credentials, got %+v", creds)
	}

	// A pod whose credentials have not arrived, or have expired, gets 503 until the plugin sends current ones.
	lateSecret := runEnv(t, stateDir, "late")["AWS_CONTAINER_AUTHORIZATION_TOKEN"]
	if lateSecret == "" || lateSecret == piSecret {
		t.Fatalf("expected a distinct secret per pod, got %q", lateSecret)
	}
	if code, _ = getCredentials(t, uri, lateSecret); code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 before credentials arrive, got %d", code)
	}
	post("/containers/credentials?podUID=late", credsJSON("ASIA-OLD", time.Now().Add(-time.Minute).Unix()))
	if code, _ = getCredentials(t, uri, lateSecret); code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503 for expired credentials, got %d", code)
	}
	post("/containers/credentials?podUID=late", credsJSON("ASIA3", exp))
	if code, creds = getCredentials(t, uri, lateSecret); code != http.StatusOK || creds.AccessKeyID != "ASIA3" {
		t.Fatalf("expected ASIA3 once delivered (HTTP %d): %+v", code, creds)
	}

	// Stopping the pod revokes its secret.
	post("/containers/stop?podUID=pi", "")
	if code, _ = getCredentials(t, uri, piSecret); code != http.StatusUnauthorized {
		t.Fatalf("expected 401 after stop, got %d", code)
	}
}
