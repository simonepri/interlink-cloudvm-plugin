// Package main implements the interlink-cloudvm-plugin daemon.
// This file tests the OTLP log forwarder: endpoint handling, OTLP JSON grouping, bounded queueing, and dropping on a slow collector.
package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNewOTLPLogForwarder_Endpoint(t *testing.T) {
	cases := map[string]string{
		"http://collector:4318":            "http://collector:4318/v1/logs",
		"http://collector:4318/":           "http://collector:4318/v1/logs",
		"https://collector/custom/v1/logs": "https://collector/custom/v1/logs",
	}
	for in, want := range cases {
		f, err := NewOTLPLogForwarder(in)
		if err != nil || f.endpoint != want {
			t.Errorf("NewOTLPLogForwarder(%q) = %v, %v; want %s", in, f, err, want)
		}
	}
	for _, bad := range []string{"collector:4318", "grpc://collector:4317", ""} {
		if _, err := NewOTLPLogForwarder(bad); err == nil {
			t.Errorf("NewOTLPLogForwarder(%q) must fail", bad)
		}
	}
}

func TestEncodeOTLPLogs_GroupsByResource(t *testing.T) {
	a := LogResource{InstanceID: "i-a", Region: "eu-west-1", PodName: "p", Namespace: "ns"}
	b := LogResource{InstanceID: "i-b", Region: "eu-west-1"}
	ts := time.Unix(0, 42)
	req := encodeOTLPLogs([]LogEntry{
		{Resource: a, Time: ts, Body: "a1", Attributes: map[string]string{"z": "1", "a": "2"}},
		{Resource: b, Time: ts, Body: "b1"},
		{Resource: a, Time: ts, Body: "a2"},
	})
	if len(req.ResourceLogs) != 2 {
		t.Fatalf("resourceLogs = %d, want 2", len(req.ResourceLogs))
	}
	first := req.ResourceLogs[0]
	if res := kvMap(first.Resource.Attributes); res["cloudvm.instance_id"] != "i-a" || res["k8s.pod.name"] != "p" || res["k8s.namespace.name"] != "ns" || res["cloudvm.region"] != "eu-west-1" {
		t.Errorf("resource = %v", res)
	}
	records := first.ScopeLogs[0].LogRecords
	if len(records) != 2 || records[0].Body.StringValue != "a1" || records[1].Body.StringValue != "a2" || records[0].TimeUnixNano != "42" {
		t.Errorf("records = %+v", records)
	}
	if attrs := records[0].Attributes; attrs[0].Key != "a" || attrs[1].Key != "z" {
		t.Errorf("attributes must be sorted, got %+v", attrs)
	}
	if res := kvMap(req.ResourceLogs[1].Resource.Attributes); len(res) != 2 {
		t.Errorf("empty pod attributes must be omitted, got %v", res)
	}
}

func TestOTLPLogForwarder_DropsInsteadOfBlockingWhenCollectorIsSlow(t *testing.T) {
	release := make(chan struct{})
	collector := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer collector.Close()
	defer close(release)

	f, err := NewOTLPLogForwarder(collector.URL)
	if err != nil {
		t.Fatal(err)
	}
	f.maxBytes = 64 * 1024
	f.batchSize = 10
	f.flushInterval = 10 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.Run(ctx)

	start := time.Now()
	accepted := 0
	for range 10000 {
		if f.Enqueue(LogEntry{Body: "0123456789", Time: time.Now()}) {
			accepted++
		}
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Enqueue blocked for %v behind a stalled collector", elapsed)
	}
	if f.Dropped() == 0 || accepted == 0 {
		t.Errorf("accepted %d, dropped %d; want both non-zero", accepted, f.Dropped())
	}
	f.mu.Lock()
	queued := f.queued
	f.mu.Unlock()
	if queued > f.maxBytes {
		t.Errorf("queue holds %d bytes, above the %d byte bound", queued, f.maxBytes)
	}
}

func TestOTLPLogForwarder_CountsFailedExportsAsDropped(t *testing.T) {
	collector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer collector.Close()
	f, err := NewOTLPLogForwarder(collector.URL)
	if err != nil {
		t.Fatal(err)
	}
	f.Enqueue(LogEntry{Body: "x"})
	f.Enqueue(LogEntry{Body: "y"})
	f.drain(context.Background())
	if f.Dropped() != 2 || f.Exported() != 0 {
		t.Errorf("dropped %d exported %d; want 2 and 0", f.Dropped(), f.Exported())
	}
}

func TestOTLPLogForwarder_ExportsBatches(t *testing.T) {
	c := &otlpCollector{}
	collector := httptest.NewServer(c)
	defer collector.Close()
	f, err := NewOTLPLogForwarder(collector.URL)
	if err != nil {
		t.Fatal(err)
	}
	f.batchSize = 2
	for _, body := range []string{"a", "b", "c"} {
		f.Enqueue(LogEntry{Body: body, Resource: LogResource{InstanceID: "i-1"}})
	}
	f.drain(context.Background())
	if f.Exported() != 3 {
		t.Fatalf("exported %d, want 3", f.Exported())
	}
	if rec, n := c.find("c"); n != 1 || rec.resource["cloudvm.instance_id"] != "i-1" {
		t.Errorf("collector record = %+v (%d)", rec, n)
	}
}

func TestEncodeOTLPLogs_CarriesTheTraceID(t *testing.T) {
	req := encodeOTLPLogs([]LogEntry{{Time: time.Unix(0, 1), Body: "x", TraceID: "0102030405060708090a0b0c0d0e0f10"}, {Time: time.Unix(0, 2), Body: "y"}})
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(string(raw), `"traceId":"0102030405060708090a0b0c0d0e0f10"`); got != 1 {
		t.Errorf("want the trace ID on the one record that has it, got %s", raw)
	}
}
