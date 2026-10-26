// Package main implements the interlink-cloudvm-plugin daemon.
// This file consumes the log stream workers send over their tunnel session, keeps recent container output for kubectl logs, and feeds the OTLP forwarder.
package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"
)

// maxWorkerLogLine bounds one record on a worker log stream; longer lines are dropped.
const maxWorkerLogLine = 64 * 1024

// Defaults for the kubectl logs buffer: 128 KiB of recent output for each of the last 256 pods (32 MiB at most).
const (
	defaultPodLogPods  = 256
	defaultPodLogBytes = 128 * 1024
)

// Worker log sources.
const (
	WorkerLogSourceBootstrap = "bootstrap"
	WorkerLogSourceRunner    = "runner"
	WorkerLogSourceContainer = "container"
)

// WorkerLogRecord is one line on a worker log stream.
type WorkerLogRecord struct {
	// Time is an RFC 3339 timestamp; the plugin uses its receive time when it is missing or invalid.
	Time string `json:"time,omitempty"`
	// Source is bootstrap, runner, or container.
	Source string `json:"src"`
	// PodUID identifies the pod whose container wrote a container record.
	PodUID string `json:"pod,omitempty"`
	// Stream is stdout or stderr for container records.
	Stream  string `json:"fd,omitempty"`
	Message string `json:"msg"`
}

// SetLogForwarder exports every worker log record through forwarder.
func (s *Server) SetLogForwarder(forwarder *OTLPLogForwarder) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logForwarder = forwarder
}

// ConsumeWorkerLogs reads one worker's log stream until it ends; the instance ID comes from the authenticated session.
func (s *Server) ConsumeWorkerLogs(instanceID string, r io.Reader) {
	s.mu.RLock()
	forwarder := s.logForwarder
	s.mu.RUnlock()
	reader, ok := r.(*bufio.Reader)
	if !ok {
		reader = bufio.NewReaderSize(r, maxWorkerLogLine)
	}
	pods := make(map[string]PodAllocation)
	for {
		line, err := readWorkerLogLine(reader)
		if len(line) > 0 {
			var rec WorkerLogRecord
			if json.Unmarshal(line, &rec) == nil {
				s.recordWorkerLog(forwarder, instanceID, &rec, pods)
			}
		}
		if err != nil {
			return
		}
	}
}

// readWorkerLogLine returns the next line, skipping any line longer than the reader's buffer.
func readWorkerLogLine(reader *bufio.Reader) ([]byte, error) {
	for {
		line, err := reader.ReadSlice('\n')
		if !errors.Is(err, bufio.ErrBufferFull) {
			return line, err
		}
		for errors.Is(err, bufio.ErrBufferFull) {
			_, err = reader.ReadSlice('\n')
		}
		if err != nil {
			return nil, err
		}
	}
}

func (s *Server) recordWorkerLog(forwarder *OTLPLogForwarder, instanceID string, rec *WorkerLogRecord, pods map[string]PodAllocation) {
	ts, err := time.Parse(time.RFC3339Nano, rec.Time)
	if err != nil {
		ts = time.Now()
	}
	var pod PodAllocation
	if rec.PodUID != "" {
		alloc, ok := pods[rec.PodUID]
		if !ok {
			if _, found, exists := s.nodePool.FindPodNode(rec.PodUID); exists {
				alloc = found
				pods[rec.PodUID] = alloc
			}
		}
		pod = alloc
	} else {
		pod = s.solePod(instanceID)
	}
	if rec.Source == WorkerLogSourceContainer && rec.PodUID != "" {
		s.podLogs.Append(rec.PodUID, PodLogLine{Time: ts, Stream: rec.Stream, Text: rec.Message})
	}
	if forwarder == nil {
		return
	}
	attrs := map[string]string{"cloudvm.log.source": rec.Source}
	if rec.Stream != "" {
		attrs["log.iostream"] = rec.Stream
	}
	if rec.PodUID != "" {
		attrs["k8s.pod.uid"] = rec.PodUID
	}
	forwarder.Enqueue(LogEntry{
		Resource: LogResource{
			InstanceID: instanceID,
			Region:     s.nodeRegion(instanceID),
			PodName:    pod.PodName,
			Namespace:  pod.PodNamespace,
		},
		Time:       ts,
		Body:       rec.Message,
		Attributes: attrs,
	})
}

// solePod returns the pod a worker's host logs belong to: the only pod on the node, or none when it hosts several.
func (s *Server) solePod(instanceID string) PodAllocation {
	node, ok := s.nodePool.Node(instanceID)
	if !ok {
		return PodAllocation{}
	}
	node.mu.RLock()
	defer node.mu.RUnlock()
	if len(node.ActivePods) != 1 {
		return PodAllocation{}
	}
	for _, alloc := range node.ActivePods {
		return *alloc
	}
	return PodAllocation{}
}

// PodLogLine is one line of container output kept for kubectl logs.
type PodLogLine struct {
	Time   time.Time
	Stream string
	Text   string
}

type podLog struct {
	lines []PodLogLine
	bytes int
}

// PodLogBuffer keeps the most recent container output of the most recently seen pods within fixed bounds.
type PodLogBuffer struct {
	mu       sync.Mutex
	maxPods  int
	maxBytes int
	pods     map[string]*podLog
	order    []string // pod UIDs in first-seen order, oldest first
}

// NewPodLogBuffer keeps up to maxBytes of output for each of the last maxPods pods.
func NewPodLogBuffer(maxPods, maxBytes int) *PodLogBuffer {
	return &PodLogBuffer{maxPods: maxPods, maxBytes: maxBytes, pods: make(map[string]*podLog)}
}

// Append adds a line to a pod's output, discarding its oldest lines and the oldest pods beyond the bounds.
func (b *PodLogBuffer) Append(podUID string, line PodLogLine) {
	if len(line.Text) > b.maxBytes {
		line.Text = line.Text[:b.maxBytes]
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	pl, ok := b.pods[podUID]
	if !ok {
		if len(b.order) >= b.maxPods {
			delete(b.pods, b.order[0])
			b.order = b.order[1:]
		}
		pl = &podLog{}
		b.pods[podUID] = pl
		b.order = append(b.order, podUID)
	}
	pl.lines = append(pl.lines, line)
	pl.bytes += len(line.Text)
	drop := 0
	for pl.bytes > b.maxBytes {
		pl.bytes -= len(pl.lines[drop].Text)
		drop++
	}
	if drop > 0 {
		pl.lines = append([]PodLogLine(nil), pl.lines[drop:]...)
	}
}

// Lines returns a copy of the output kept for a pod.
func (b *PodLogBuffer) Lines(podUID string) []PodLogLine {
	b.mu.Lock()
	defer b.mu.Unlock()
	pl, ok := b.pods[podUID]
	if !ok {
		return nil
	}
	return append([]PodLogLine(nil), pl.lines...)
}

// GetLogsRequest is the interLink LogStruct body the virtual kubelet sends to /getLogs.
type GetLogsRequest struct {
	Namespace     string         `json:"Namespace"`
	PodUID        string         `json:"PodUID"`
	PodName       string         `json:"PodName"`
	ContainerName string         `json:"ContainerName"`
	Opts          GetLogsOptions `json:"Opts"`
}

// GetLogsOptions mirrors interLink's ContainerLogOpts.
type GetLogsOptions struct {
	Tail         int       `json:"Tail"`
	LimitBytes   int       `json:"Bytes"`
	Timestamps   bool      `json:"Timestamps"`
	Follow       bool      `json:"Follow"`
	Previous     bool      `json:"Previous"`
	SinceSeconds int       `json:"SinceSeconds"`
	SinceTime    time.Time `json:"SinceTime"`
}

// handleGetLogs serves kubectl logs from the container output workers streamed; it returns what is buffered and never follows.
func (s *Server) handleGetLogs(w http.ResponseWriter, r *http.Request) {
	var req GetLogsRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 64*1024)).Decode(&req); err != nil {
		http.Error(w, "invalid logs request: "+err.Error(), http.StatusBadRequest)
		return
	}
	if req.PodUID == "" {
		http.Error(w, "PodUID is required", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	if req.Opts.Previous {
		return
	}
	_, _ = io.WriteString(w, renderPodLogs(s.podLogs.Lines(req.PodUID), req.Opts, time.Now()))
}

// renderPodLogs applies kubectl's since, tail, timestamps, and byte-limit options to buffered lines.
func renderPodLogs(lines []PodLogLine, opts GetLogsOptions, now time.Time) string {
	since := opts.SinceTime
	if opts.SinceSeconds > 0 {
		since = now.Add(-time.Duration(opts.SinceSeconds) * time.Second)
	}
	if !since.IsZero() {
		kept := lines[:0:0]
		for _, l := range lines {
			if !l.Time.Before(since) {
				kept = append(kept, l)
			}
		}
		lines = kept
	}
	if opts.Tail > 0 && len(lines) > opts.Tail {
		lines = lines[len(lines)-opts.Tail:]
	}
	var out strings.Builder
	for _, l := range lines {
		if opts.Timestamps {
			out.WriteString(l.Time.UTC().Format(time.RFC3339Nano))
			out.WriteByte(' ')
		}
		out.WriteString(l.Text)
		out.WriteByte('\n')
	}
	text := out.String()
	if opts.LimitBytes > 0 && len(text) > opts.LimitBytes {
		text = text[:opts.LimitBytes]
	}
	return text
}
