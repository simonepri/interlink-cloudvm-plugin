// Package main implements the interlink-cloudvm-plugin daemon.
// This file batches worker log records in a bounded queue and exports them as OTLP/HTTP JSON logs, dropping records when the collector falls behind.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Forwarder defaults: an 8 MiB queue flushed every second or every 512 records, with a 10 second export timeout.
const (
	defaultLogQueueBytes    = 8 * 1024 * 1024
	defaultLogBatchSize     = 512
	defaultLogFlushInterval = time.Second
	defaultLogExportTimeout = 10 * time.Second
	logEntryOverheadBytes   = 256
)

// LogResource identifies where a log record came from; it becomes the OTLP resource.
type LogResource struct {
	InstanceID string
	Region     string
	PodName    string
	Namespace  string
	// PodUID and Container name the pod and init container that wrote a container record. Carrying the pod UID on the
	// resource also keeps a Kubernetes attributes processor from matching the record to the plugin's own pod.
	PodUID    string
	Container string
}

// LogEntry is one log record waiting for export.
type LogEntry struct {
	Resource   LogResource
	Time       time.Time
	Body       string
	Attributes map[string]string
	// TraceID is the hex ID of the lifecycle trace the record belongs to, or empty.
	TraceID string
}

func (e *LogEntry) size() int {
	n := len(e.Body) + logEntryOverheadBytes
	for k, v := range e.Attributes {
		n += len(k) + len(v)
	}
	return n
}

// OTLPLogForwarder exports log entries to an OTLP/HTTP logs endpoint without ever blocking its producers.
type OTLPLogForwarder struct {
	endpoint      string
	client        *http.Client
	maxBytes      int
	batchSize     int
	flushInterval time.Duration

	mu     sync.Mutex
	queue  []LogEntry
	queued int
	wake   chan struct{}

	dropped  atomic.Uint64
	exported atomic.Uint64
}

// NewOTLPLogForwarder exports to endpoint; an endpoint without a path gets the OTLP default /v1/logs.
func NewOTLPLogForwarder(endpoint string) (*OTLPLogForwarder, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("invalid OTLP logs endpoint %q: want an http(s) URL", endpoint)
	}
	if u.Path == "" || u.Path == "/" {
		u.Path = "/v1/logs"
	}
	return &OTLPLogForwarder{
		endpoint:      u.String(),
		client:        &http.Client{Timeout: defaultLogExportTimeout},
		maxBytes:      defaultLogQueueBytes,
		batchSize:     defaultLogBatchSize,
		flushInterval: defaultLogFlushInterval,
		wake:          make(chan struct{}, 1),
	}, nil
}

// Enqueue queues an entry for export and reports false when the queue is full and the entry was dropped.
func (f *OTLPLogForwarder) Enqueue(e LogEntry) bool {
	size := e.size()
	f.mu.Lock()
	if f.queued+size > f.maxBytes {
		f.mu.Unlock()
		f.dropped.Add(1)
		return false
	}
	f.queue = append(f.queue, e)
	f.queued += size
	full := len(f.queue) >= f.batchSize
	f.mu.Unlock()
	if full {
		select {
		case f.wake <- struct{}{}:
		default:
		}
	}
	return true
}

// Dropped returns how many entries were discarded because the queue was full or an export failed.
func (f *OTLPLogForwarder) Dropped() uint64 { return f.dropped.Load() }

// Exported returns how many entries the collector accepted.
func (f *OTLPLogForwarder) Exported() uint64 { return f.exported.Load() }

// Run exports queued entries until ctx ends, then makes one last bounded attempt to flush.
func (f *OTLPLogForwarder) Run(ctx context.Context) {
	ticker := time.NewTicker(f.flushInterval)
	defer ticker.Stop()
	var reportedDrops uint64
	for {
		select {
		case <-ctx.Done():
			flushCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), defaultLogExportTimeout)
			f.drain(flushCtx)
			cancel()
			return
		case <-ticker.C:
		case <-f.wake:
		}
		f.drain(ctx)
		if d := f.dropped.Load(); d != reportedDrops {
			log.Printf("[cloudvm-logs] Dropped %d worker log records so far (collector slow or unreachable)", d) //nolint:gosec // counter value
			reportedDrops = d
		}
	}
}

// drain exports batches until the queue is empty, an export fails, or ctx ends.
func (f *OTLPLogForwarder) drain(ctx context.Context) {
	for more := true; more; {
		more = f.flushOnce(ctx)
	}
}

// flushOnce exports up to one batch and reports whether more entries remain.
func (f *OTLPLogForwarder) flushOnce(ctx context.Context) bool {
	f.mu.Lock()
	n := min(len(f.queue), f.batchSize)
	if n == 0 {
		f.mu.Unlock()
		return false
	}
	batch := f.queue[:n:n]
	f.queue = append([]LogEntry(nil), f.queue[n:]...)
	for i := range batch {
		f.queued -= batch[i].size()
	}
	more := len(f.queue) > 0
	f.mu.Unlock()

	if err := f.export(ctx, batch); err != nil {
		f.dropped.Add(uint64(len(batch)))
		log.Printf("[cloudvm-logs] OTLP export of %d records failed: %v", len(batch), err) //nolint:gosec // count and transport error
		return false
	}
	f.exported.Add(uint64(len(batch)))
	return more && ctx.Err() == nil
}

func (f *OTLPLogForwarder) export(ctx context.Context, batch []LogEntry) error {
	body, err := json.Marshal(encodeOTLPLogs(batch))
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, f.endpoint, bytes.NewReader(body)) //nolint:gosec // operator-configured collector endpoint
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := f.client.Do(req) //nolint:gosec // operator-configured collector endpoint
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return fmt.Errorf("collector answered %d", resp.StatusCode)
	}
	return nil
}

// OTLP/HTTP JSON encoding of ExportLogsServiceRequest (opentelemetry-proto logs/v1).
type otlpLogsRequest struct {
	ResourceLogs []otlpResourceLogs `json:"resourceLogs"`
}

type otlpResourceLogs struct {
	Resource  otlpResource    `json:"resource"`
	ScopeLogs []otlpScopeLogs `json:"scopeLogs"`
}

type otlpResource struct {
	Attributes []otlpKeyValue `json:"attributes"`
}

type otlpScopeLogs struct {
	Scope      otlpScope       `json:"scope"`
	LogRecords []otlpLogRecord `json:"logRecords"`
}

type otlpScope struct {
	Name string `json:"name"`
}

type otlpLogRecord struct {
	TimeUnixNano         string         `json:"timeUnixNano"`
	ObservedTimeUnixNano string         `json:"observedTimeUnixNano"`
	Body                 otlpAnyValue   `json:"body"`
	Attributes           []otlpKeyValue `json:"attributes,omitempty"`
	TraceID              string         `json:"traceId,omitempty"`
}

type otlpKeyValue struct {
	Key   string       `json:"key"`
	Value otlpAnyValue `json:"value"`
}

type otlpAnyValue struct {
	StringValue string `json:"stringValue"`
}

// encodeOTLPLogs groups entries by resource, keeping first-seen resource order and record order within each.
func encodeOTLPLogs(batch []LogEntry) otlpLogsRequest {
	observed := strconv.FormatInt(time.Now().UnixNano(), 10)
	index := make(map[LogResource]int)
	var req otlpLogsRequest
	for i := range batch {
		e := &batch[i]
		idx, ok := index[e.Resource]
		if !ok {
			idx = len(req.ResourceLogs)
			index[e.Resource] = idx
			req.ResourceLogs = append(req.ResourceLogs, otlpResourceLogs{
				Resource:  otlpResource{Attributes: resourceAttributes(e.Resource)},
				ScopeLogs: []otlpScopeLogs{{Scope: otlpScope{Name: "interlink-cloudvm-plugin/worker-logs"}}},
			})
		}
		scope := &req.ResourceLogs[idx].ScopeLogs[0]
		scope.LogRecords = append(scope.LogRecords, otlpLogRecord{
			TimeUnixNano:         strconv.FormatInt(e.Time.UnixNano(), 10),
			ObservedTimeUnixNano: observed,
			Body:                 otlpAnyValue{StringValue: e.Body},
			Attributes:           sortedAttributes(e.Attributes),
			TraceID:              e.TraceID,
		})
	}
	return req
}

func resourceAttributes(r LogResource) []otlpKeyValue {
	var attrs []otlpKeyValue
	for _, kv := range [][2]string{
		{"cloudvm.instance_id", r.InstanceID},
		{"cloudvm.region", r.Region},
		{"k8s.pod.name", r.PodName},
		{"k8s.namespace.name", r.Namespace},
		{"k8s.pod.uid", r.PodUID},
		{"k8s.container.name", r.Container},
	} {
		if kv[1] != "" {
			attrs = append(attrs, otlpKeyValue{Key: kv[0], Value: otlpAnyValue{StringValue: kv[1]}})
		}
	}
	return attrs
}

func sortedAttributes(m map[string]string) []otlpKeyValue {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	attrs := make([]otlpKeyValue, 0, len(keys))
	for _, k := range keys {
		attrs = append(attrs, otlpKeyValue{Key: k, Value: otlpAnyValue{StringValue: m[k]}})
	}
	return attrs
}
