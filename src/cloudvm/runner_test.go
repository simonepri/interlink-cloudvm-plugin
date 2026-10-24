// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
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
