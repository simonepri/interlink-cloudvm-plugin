// Package main implements the interlink-cloudvm-plugin daemon.
// This file runs the embedded Python spot watcher against fake AWS and GCP metadata services.
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// startSpotWatcher runs the watcher with env and returns its stdout, which the test reads while it runs.
func startSpotWatcher(t *testing.T, env ...string) *syncBuffer {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	script := filepath.Join(t.TempDir(), "spot_watcher.py")
	if err := os.WriteFile(script, []byte(spotWatcherScript), 0o600); err != nil {
		t.Fatalf("write watcher: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var stdout, stderr syncBuffer
	cmd := exec.CommandContext(ctx, python, "-I", script) //nolint:gosec // test-controlled script path
	cmd.Env = append(os.Environ(), append([]string{"CLOUDVM_SPOT_POLL_SECONDS=0.05"}, env...)...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Start(); err != nil {
		cancel()
		t.Fatalf("start watcher: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("watcher log:\n%s", stderr.String())
		}
	})
	return &stdout
}

// spotNotices decodes the notice lines a watcher printed.
func spotNotices(t *testing.T, out string) []SpotInterruptionNotice {
	t.Helper()
	var notices []SpotInterruptionNotice
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if line == "" {
			continue
		}
		var n SpotInterruptionNotice
		if err := json.Unmarshal([]byte(line), &n); err != nil {
			t.Fatalf("watcher printed a non-JSON line %q: %v", line, err)
		}
		notices = append(notices, n)
	}
	return notices
}

func TestSpotWatcher_AWSPrintsEachNoticeOnce(t *testing.T) {
	var polls, unauthorized atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /latest/api/token", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-aws-ec2-metadata-token-ttl-seconds") == "" {
			http.Error(w, "missing ttl", http.StatusBadRequest)
			return
		}
		_, _ = w.Write([]byte("imds-token"))
	})
	mux.HandleFunc("GET /latest/meta-data/spot/instance-action", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-aws-ec2-metadata-token") != "imds-token" {
			unauthorized.Add(1)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		if polls.Add(1) <= 3 {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"action": "terminate", "time": "2026-10-27T10:02:00Z"}`))
	})
	imds := httptest.NewServer(mux)
	t.Cleanup(imds.Close)

	out := startSpotWatcher(t, "CLOUDVM_AWS_METADATA_URL="+imds.URL, "CLOUDVM_GCP_METADATA_URL=http://127.0.0.1:1")
	waitForWithin(t, 60*time.Second, "AWS notice", func() bool { return strings.Contains(out.String(), "terminate") })
	// Keep polling past the notice: the watcher must not print it again.
	start := polls.Load()
	waitForWithin(t, 30*time.Second, "repeated polls", func() bool { return polls.Load() >= start+5 })

	notices := spotNotices(t, out.String())
	want := SpotInterruptionNotice{Event: spotInterruptionEvent, Action: "terminate", Time: "2026-10-27T10:02:00Z"}
	if len(notices) != 1 || notices[0] != want {
		t.Fatalf("watcher printed %+v, want exactly %+v", notices, want)
	}
	if unauthorized.Load() != 0 {
		t.Fatalf("watcher polled %d times without its IMDSv2 token", unauthorized.Load())
	}
}

func TestSpotWatcher_GCPWaitsForPreemption(t *testing.T) {
	var preempted, waits atomic.Bool
	var badFlavor atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("GET /computeMetadata/v1/instance/preempted", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" {
			badFlavor.Add(1)
			http.Error(w, "missing Metadata-Flavor", http.StatusForbidden)
			return
		}
		etag := func() string {
			if preempted.Load() {
				return "etag-true"
			}
			return "etag-false"
		}
		if q := r.URL.Query(); q.Get("wait_for_change") == "true" {
			waits.Store(true)
			// Hold the request open like the metadata server does until the value changes.
			deadline := time.Now().Add(time.Second)
			for etag() == q.Get("last_etag") && time.Now().Before(deadline) {
				time.Sleep(10 * time.Millisecond)
			}
		}
		if preempted.Load() {
			w.Header().Set("ETag", "etag-true")
			_, _ = w.Write([]byte("TRUE"))
			return
		}
		w.Header().Set("ETag", "etag-false")
		_, _ = w.Write([]byte("FALSE"))
	})
	mux.HandleFunc("GET /computeMetadata/v1/instance/id", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("1234567890"))
	})
	metadata := httptest.NewServer(mux)
	t.Cleanup(metadata.Close)

	// No AWS metadata service answers, so the watcher detects GCP.
	out := startSpotWatcher(t, "CLOUDVM_AWS_METADATA_URL=http://127.0.0.1:1", "CLOUDVM_GCP_METADATA_URL="+metadata.URL)
	waitForWithin(t, 60*time.Second, "watcher waiting on the preempted value", waits.Load)
	if out.String() != "" {
		t.Fatalf("watcher printed %q before preemption", out.String())
	}
	preempted.Store(true)
	waitForWithin(t, 30*time.Second, "GCP notice", func() bool { return strings.Contains(out.String(), "preempt") })
	time.Sleep(300 * time.Millisecond)

	notices := spotNotices(t, out.String())
	if len(notices) != 1 || notices[0].Event != spotInterruptionEvent || notices[0].Action != "preempt" {
		t.Fatalf("watcher printed %+v, want one preempt notice", notices)
	}
	if _, err := time.Parse(time.RFC3339Nano, notices[0].Time); err != nil {
		t.Fatalf("notice time %q is not RFC 3339: %v", notices[0].Time, err)
	}
	if badFlavor.Load() != 0 {
		t.Fatalf("watcher sent %d requests without Metadata-Flavor", badFlavor.Load())
	}
}
