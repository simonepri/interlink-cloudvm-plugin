// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPContainerRunner_GetStatusAndHostStats(t *testing.T) {
	expectedToken := "test-runner-token-123"
	statusServed := false
	hostStatsServed := false

	mux := http.NewServeMux()
	mux.HandleFunc("/containers/status", func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth != "Bearer "+expectedToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		podUID := r.URL.Query().Get("podUID")
		if podUID != "test-pod-uid" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		statusServed = true
		res := &ContainerStatusResult{
			PodUID:         podUID,
			Running:        true,
			Status:         "running",
			StartedAt:      time.Now(),
			NetworkRxBytes: 1048576,
			NetworkTxBytes: 2097152,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(res)
	})

	mux.HandleFunc("/host/stats", func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		if auth != "Bearer "+expectedToken {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		hostStatsServed = true
		res := &HostStatsResult{
			NetworkRxBytes: 5242880,
			NetworkTxBytes: 10485760,
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(res)
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	host, portStr, err := net.SplitHostPort(ts.Listener.Addr().String())
	if err != nil {
		t.Fatalf("failed to split host port: %v", err)
	}
	port, _ := strconv.Atoi(portStr)

	runner := NewHTTPContainerRunner(port)
	node := &CloudVMNode{
		InstanceID:      "i-test-node",
		Provider:        ProviderAWS,
		Region:          "us-west-2",
		InstanceType:    "g5.xlarge",
		MeshIP:          host,
		RunnerAuthToken: expectedToken,
	}

	// 1. Test GetStatus
	statusRes, err := runner.GetStatus(context.Background(), node, "test-pod-uid")
	if err != nil {
		t.Fatalf("GetStatus failed: %v", err)
	}
	if !statusServed {
		t.Errorf("status endpoint was not invoked")
	}
	if statusRes.NetworkRxBytes != 1048576 || statusRes.NetworkTxBytes != 2097152 {
		t.Errorf("unexpected network bytes in status: rx=%d, tx=%d", statusRes.NetworkRxBytes, statusRes.NetworkTxBytes)
	}

	// 2. Test GetHostStats
	hostStats, err := runner.GetHostStats(context.Background(), node)
	if err != nil {
		t.Fatalf("GetHostStats failed: %v", err)
	}
	if !hostStatsServed {
		t.Errorf("host stats endpoint was not invoked")
	}
	if hostStats.NetworkRxBytes != 5242880 || hostStats.NetworkTxBytes != 10485760 {
		t.Errorf("unexpected network bytes in host stats: rx=%d, tx=%d", hostStats.NetworkRxBytes, hostStats.NetworkTxBytes)
	}
}

func TestMockContainerRunner_HostStats(t *testing.T) {
	mockRunner := NewMockContainerRunner()
	node := &CloudVMNode{InstanceID: "i-mock"}

	// Default empty stats
	stats, err := mockRunner.GetHostStats(context.Background(), node)
	if err != nil {
		t.Fatalf("GetHostStats error: %v", err)
	}
	if stats.NetworkRxBytes != 0 || stats.NetworkTxBytes != 0 {
		t.Errorf("expected 0 stats, got rx=%d, tx=%d", stats.NetworkRxBytes, stats.NetworkTxBytes)
	}

	// Custom set stats
	mockRunner.SetHostStats(&HostStatsResult{
		NetworkRxBytes: 4096,
		NetworkTxBytes: 8192,
	})
	stats, err = mockRunner.GetHostStats(context.Background(), node)
	if err != nil {
		t.Fatalf("GetHostStats error: %v", err)
	}
	if stats.NetworkRxBytes != 4096 || stats.NetworkTxBytes != 8192 {
		t.Errorf("expected configured stats, got rx=%d, tx=%d", stats.NetworkRxBytes, stats.NetworkTxBytes)
	}
}

func TestSpotInterruptionWatcher_IMDSPolling(t *testing.T) {
	var imdsHit atomic.Bool
	var imdsInterrupted atomic.Bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/latest/api/token":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte("mock-imds-token"))
		case "/latest/meta-data/spot/instance-action":
			imdsHit.Store(true)
			if imdsInterrupted.Load() {
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write([]byte(`{"action": "terminate", "time": "2026-10-25T02:00:00Z"}`))
			} else {
				w.WriteHeader(http.StatusNotFound)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()

	var pushedFrames []*StatusFrame
	var framesMu sync.Mutex

	pusher := StatusFramePusherFunc(func(_ context.Context, frame *StatusFrame) error {
		framesMu.Lock()
		defer framesMu.Unlock()
		pushedFrames = append(pushedFrames, frame)
		return nil
	})

	watcher := NewSpotInterruptionWatcher(SpotInterruptionWatcherConfig{
		IMDSEndpoint:      server.URL + "/latest/meta-data/spot/instance-action",
		IMDSTokenEndpoint: server.URL + "/latest/api/token",
		PollInterval:      10 * time.Millisecond,
		Pusher:            pusher,
	})
	watcher.TrackPod("pod-spot-1")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	watcher.Start(ctx)

	// Initially not interrupted
	time.Sleep(30 * time.Millisecond)
	if watcher.IsInterrupted() {
		t.Fatalf("expected watcher not interrupted initially")
	}

	// Trigger interruption on IMDS
	imdsInterrupted.Store(true)

	// Wait for watcher to detect interruption
	for i := 0; i < 50; i++ {
		if watcher.IsInterrupted() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !watcher.IsInterrupted() {
		t.Fatalf("expected watcher to detect spot interruption")
	}

	watcher.Stop()

	framesMu.Lock()
	defer framesMu.Unlock()

	if len(pushedFrames) == 0 {
		t.Fatalf("expected PodInterrupted frame pushed down tunnel, got 0 frames")
	}

	foundGeneral := false
	foundPod := false
	for _, f := range pushedFrames {
		if f.Type == FrameTypePodInterrupted && f.Status == "PodInterrupted" && f.Reason == "SpotInterruption" {
			switch f.PodUID {
			case "":
				foundGeneral = true
			case "pod-spot-1":
				foundPod = true
			}
		}
	}

	if !foundGeneral || !foundPod {
		t.Fatalf("expected both general and pod-scoped PodInterrupted frames: general=%v, pod=%v", foundGeneral, foundPod)
	}
}

func TestSpotInterruptionWatcher_ACPIEvent(t *testing.T) {
	tmpDir := t.TempDir()
	acpiFile := tmpDir + "/acpi_events"

	// Initially benign event
	if err := os.WriteFile(acpiFile, []byte(`ac_adapter AC 00000080 00000001
`), 0600); err != nil {
		t.Fatalf("failed to write acpi file: %v", err)
	}

	var pushedFrames []*StatusFrame
	var framesMu sync.Mutex

	pusher := StatusFramePusherFunc(func(_ context.Context, frame *StatusFrame) error {
		framesMu.Lock()
		defer framesMu.Unlock()
		pushedFrames = append(pushedFrames, frame)
		return nil
	})

	watcher := NewSpotInterruptionWatcher(SpotInterruptionWatcherConfig{
		ACPIEventPath: acpiFile,
		PollInterval:  10 * time.Millisecond,
		Pusher:        pusher,
	})
	watcher.TrackPod("pod-acpi-1")

	detected, _, err := watcher.CheckACPIOnce(context.Background())
	if err != nil || detected {
		t.Fatalf("expected no detection on ac adapter event, got detected=%v, err=%v", detected, err)
	}

	// Write ACPI power button event
	if err := os.WriteFile(acpiFile, []byte(`button/power PBTN 00000080 00000000
`), 0600); err != nil {
		t.Fatalf("failed to write power event: %v", err)
	}

	detected, msg, err := watcher.CheckACPIOnce(context.Background())
	if err != nil || !detected {
		t.Fatalf("expected ACPI detection, got detected=%v, err=%v", detected, err)
	}
	if !strings.Contains(msg, "button/power") {
		t.Errorf("unexpected ACPI message: %s", msg)
	}

	// Trigger via watcher
	if err := watcher.TriggerInterruption(context.Background(), "SpotInterruption", "ACPI shutdown event: "+msg); err != nil {
		t.Fatalf("TriggerInterruption failed: %v", err)
	}

	framesMu.Lock()
	defer framesMu.Unlock()
	if len(pushedFrames) == 0 {
		t.Fatalf("expected frames pushed on ACPI event")
	}
	if pushedFrames[0].Type != FrameTypePodInterrupted || pushedFrames[0].Reason != "SpotInterruption" {
		t.Errorf("unexpected frame: %+v", pushedFrames[0])
	}
}

func TestHTTPFramePusher(t *testing.T) {
	var receivedAuth string
	var receivedFrame StatusFrame
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedAuth = r.Header.Get("Authorization")
		if err := json.NewDecoder(r.Body).Decode(&receivedFrame); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	pusher := NewHTTPFramePusher(server.URL, "secret-token", server.Client())
	frame := &StatusFrame{
		Type:      FrameTypePodInterrupted,
		Status:    "PodInterrupted",
		Reason:    "SpotInterruption",
		PodUID:    "uid-push-1",
		Timestamp: time.Now().UTC(),
	}

	if err := pusher.PushStatusFrame(context.Background(), frame); err != nil {
		t.Fatalf("PushStatusFrame failed: %v", err)
	}

	if receivedAuth != "Bearer secret-token" {
		t.Errorf("expected Bearer secret-token, got %s", receivedAuth)
	}
	if receivedFrame.Type != FrameTypePodInterrupted || receivedFrame.PodUID != "uid-push-1" {
		t.Errorf("unexpected received frame: %+v", receivedFrame)
	}
}
