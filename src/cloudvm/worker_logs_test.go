// Package main implements the interlink-cloudvm-plugin daemon.
// This file tests the worker log stream end to end: dialer and Python agent to gateway, OTLP export, and kubectl logs.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/hashicorp/yamux"
)

// otlpCollector records the log records an OTLP/HTTP collector receives.
type otlpCollector struct {
	mu      sync.Mutex
	records []collectedRecord
}

type collectedRecord struct {
	resource map[string]string
	attrs    map[string]string
	body     string
}

func kvMap(kvs []otlpKeyValue) map[string]string {
	m := make(map[string]string, len(kvs))
	for _, kv := range kvs {
		m[kv.Key] = kv.Value.StringValue
	}
	return m
}

func (c *otlpCollector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var req otlpLogsRequest
	if r.URL.Path != "/v1/logs" || json.NewDecoder(r.Body).Decode(&req) != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, rl := range req.ResourceLogs {
		for _, sl := range rl.ScopeLogs {
			for _, lr := range sl.LogRecords {
				c.records = append(c.records, collectedRecord{kvMap(rl.Resource.Attributes), kvMap(lr.Attributes), lr.Body.StringValue})
			}
		}
	}
}

func (c *otlpCollector) find(body string) (collectedRecord, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var found collectedRecord
	n := 0
	for _, rec := range c.records {
		if rec.body == body {
			found = rec
			n++
		}
	}
	return found, n
}

// withCollector attaches a fast-flushing OTLP forwarder to the fixture's server.
func withCollector(t *testing.T, f *tunnelFixture) *otlpCollector {
	t.Helper()
	collector := &otlpCollector{}
	srv := httptest.NewServer(collector)
	t.Cleanup(srv.Close)
	forwarder, err := NewOTLPLogForwarder(srv.URL)
	if err != nil {
		t.Fatalf("forwarder: %v", err)
	}
	forwarder.flushInterval = 20 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go forwarder.Run(ctx)
	f.server.SetLogForwarder(forwarder)
	return collector
}

func (f *tunnelFixture) addNodeWithPod(id, region, token, podUID, namespace, podName string) {
	capacity, _ := NewResourceVector("4", "16Gi", 0, 0)
	node := NewCloudVMNode(id, ProviderMock, region, "", capacity)
	node.RunnerAuthToken = token
	req, _ := NewResourceVector("1", "1Gi", 0, 0)
	f.server.NodePool().RegisterAndAllocate(node, req, podUID, namespace, podName)
}

func getLogs(t *testing.T, s *Server, req GetLogsRequest) string {
	t.Helper()
	body, _ := json.Marshal(req)
	rec := httptest.NewRecorder()
	s.handleGetLogs(rec, httptest.NewRequest(http.MethodGet, "/getLogs", strings.NewReader(string(body))))
	if rec.Code != http.StatusOK {
		t.Fatalf("getLogs: %d %s", rec.Code, rec.Body.String())
	}
	return rec.Body.String()
}

func TestWorkerLogs_DialerStreamReachesCollectorAndKubectlLogs(t *testing.T) {
	f := newTunnelFixture(t, time.Minute)
	collector := withCollector(t, f)
	const id, token = "i-logs", "logs-token"
	f.addNodeWithPod(id, "eu-west-1", token, "uid-1", "team-a", "job-1")

	d := f.dialer(t, id, token)
	d.cfg.LogSource = func(ctx context.Context, w io.Writer) error {
		enc := json.NewEncoder(w)
		_ = enc.Encode(WorkerLogRecord{Source: WorkerLogSourceBootstrap, Message: "bootstrap step 1"})
		_, _ = fmt.Fprintf(w, "%s\n", strings.Repeat("x", maxWorkerLogLine+10)) // overlong lines are skipped
		_ = enc.Encode(WorkerLogRecord{Time: "2026-10-26T09:59:00Z", Source: WorkerLogSourceContainer, PodUID: "uid-1", Container: "wait-gcs-ready", Stream: "stdout", Message: "waiting for GCS"})
		_ = enc.Encode(WorkerLogRecord{Time: "2026-10-26T10:00:00.5Z", Source: WorkerLogSourceContainer, PodUID: "uid-1", Stream: "stderr", Message: "hello from the container"})
		<-ctx.Done()
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = d.Run(ctx, NewRunnerHTTPMux(&RunnerHandlerAdapter{Runner: NewMockContainerRunner()}, token))
	}()

	waitFor(t, "container record at collector", func() bool { _, n := collector.find("hello from the container"); return n == 1 })
	rec, _ := collector.find("hello from the container")
	want := map[string]string{"cloudvm.instance_id": id, "cloudvm.region": "eu-west-1", "k8s.pod.name": "job-1", "k8s.namespace.name": "team-a", "k8s.pod.uid": "uid-1"}
	for k, v := range want {
		if rec.resource[k] != v {
			t.Errorf("resource %s = %q, want %q", k, rec.resource[k], v)
		}
	}
	if rec.attrs["log.iostream"] != "stderr" || rec.attrs["cloudvm.log.source"] != "container" || rec.resource["k8s.container.name"] != "" {
		t.Errorf("record attributes = %v", rec.attrs)
	}
	host, n := collector.find("bootstrap step 1")
	if n != 1 || host.resource["k8s.pod.name"] != "job-1" || host.attrs["cloudvm.log.source"] != "bootstrap" {
		t.Errorf("bootstrap record = %+v (seen %d times)", host, n)
	}

	out := getLogs(t, f.server, GetLogsRequest{Namespace: "team-a", PodName: "job-1", PodUID: "uid-1", Opts: GetLogsOptions{Timestamps: true}})
	if out != "2026-10-26T10:00:00.5Z hello from the container\n" {
		t.Errorf("kubectl logs = %q", out)
	}
	if out := getLogs(t, f.server, GetLogsRequest{PodUID: "uid-1", ContainerName: "worker"}); out != "hello from the container\n" {
		t.Errorf("kubectl logs -c worker = %q, want only the main container's output", out)
	}
	if out := getLogs(t, f.server, GetLogsRequest{PodUID: "uid-1", ContainerName: "wait-gcs-ready"}); out != "waiting for GCS\n" {
		t.Errorf("kubectl logs -c wait-gcs-ready = %q, want the init container's output", out)
	}
	if rec, _ := collector.find("waiting for GCS"); rec.resource["k8s.container.name"] != "wait-gcs-ready" || rec.resource["k8s.pod.uid"] != "uid-1" {
		t.Errorf("init container record resource = %v", rec.resource)
	}
}

func TestWorkerLogs_DeletedPodsLateLinesKeepTheirPod(t *testing.T) {
	f := newTunnelFixture(t, time.Minute)
	collector := withCollector(t, f)
	const id, token = "i-late", "late-token"
	f.addNodeWithPod(id, "eu-west-1", token, "uid-gone", "team-a", "job-gone")
	f.addNodeWithPod("i-other", "eu-west-1", "other-token", "uid-elsewhere", "team-b", "job-elsewhere")

	// The pod's first lines on this stream arrive after the plugin released it, as after a reconnect while it stopped.
	released := make(chan struct{})
	d := f.dialer(t, id, token)
	d.cfg.LogSource = func(ctx context.Context, w io.Writer) error {
		<-released
		enc := json.NewEncoder(w)
		_ = enc.Encode(WorkerLogRecord{Source: WorkerLogSourceContainer, PodUID: "uid-gone", Stream: "stderr", Message: "last words"})
		_ = enc.Encode(WorkerLogRecord{Source: WorkerLogSourceContainer, PodUID: "uid-elsewhere", Stream: "stdout", Message: "claims another node's pod"})
		<-ctx.Done()
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_ = d.Run(ctx, NewRunnerHTTPMux(&RunnerHandlerAdapter{Runner: NewMockContainerRunner()}, token))
	}()
	waitFor(t, "session", func() bool { return f.registry.Has(id) })
	f.server.NodePool().Release("uid-gone")
	close(released)

	waitFor(t, "late record at collector", func() bool { _, n := collector.find("claims another node's pod"); return n == 1 })
	rec, _ := collector.find("last words")
	for k, v := range map[string]string{"k8s.pod.name": "job-gone", "k8s.namespace.name": "team-a", "k8s.pod.uid": "uid-gone"} {
		if rec.resource[k] != v {
			t.Errorf("deleted pod's record: resource %s = %q, want %q", k, rec.resource[k], v)
		}
	}
	if foreign, _ := collector.find("claims another node's pod"); foreign.resource["k8s.pod.name"] != "" || foreign.resource["k8s.namespace.name"] != "" || foreign.resource["k8s.pod.uid"] != "" {
		t.Errorf("a worker must not name a pod of another node: resource = %v", foreign.resource)
	}
}

func TestTunnelAgent_ContainerOutputSurvivesASessionDropAfterItExits(t *testing.T) {
	dir := t.TempDir()
	cli := filepath.Join(dir, "nerdctl")
	// The fake CLI lists every file in containers/ with the ID in ids/ and prints a stopped container's whole output.
	script := "#!/usr/bin/env bash\ncase \"$1\" in\n" +
		"  ps) for n in $(ls \"$FAKE_STATE/containers\"); do echo \"$(cat \"$FAKE_STATE/ids/$n\") $n\"; done ;;\n" +
		"  logs) cat \"$FAKE_STATE/containers/${@: -1}\" ;;\nesac\n"
	if err := os.WriteFile(cli, []byte(script), 0o755); err != nil { //nolint:gosec // test fixture must be executable
		t.Fatal(err)
	}
	t.Setenv("FAKE_STATE", dir)
	out := runAgentPython(t, fmt.Sprintf(`
import asyncio, json, os
state, cli = %q, %q
os.makedirs(os.path.join(state, "containers"))
os.makedirs(os.path.join(state, "ids"))
name = "cloudvm-uid-1"

def create(container_id):
    with open(os.path.join(state, "ids", name), "w") as f:
        f.write(container_id)
drain = os.path.join(state, "drain")
agent.CONTAINER_POLL_SECONDS = 0.05

def write_output(first, last):
    with open(os.path.join(state, "containers", name), "a") as f:
        for i in range(first, last + 1):
            f.write("2026-01-01T00:00:%%02d.000000000Z line %%d\n" %% (i, i))

class Stream(object):
    def __init__(self, fail_after=None):
        self.lines, self.fail_after = [], fail_after
    async def write(self, data):
        if self.fail_after is not None and len(self.lines) > self.fail_after:
            await asyncio.sleep(0.3)  # every record is queued by now
            raise EOFError("gateway closed the log stream")
        self.lines.append(data)

def messages(stream):
    return [json.loads(l)["msg"] for l in stream.lines[1:]]

async def ship_until(collector, stream, done):
    task = asyncio.ensure_future(collector.ship(stream))
    for _ in range(200):
        if done() or task.done():
            break
        await asyncio.sleep(0.05)
    task.cancel()
    await asyncio.wait([task])

async def main():
    collector = agent.LogCollector([], cli, drain)
    create("id-1")
    write_output(1, 5)
    dropped = Stream(fail_after=2)
    await ship_until(collector, dropped, lambda: False)
    print("drain file kept after the drop:", os.path.exists(os.path.join(drain, name)))
    resumed = Stream()
    await ship_until(collector, resumed, lambda: not os.path.exists(os.path.join(drain, name)))
    print("delivered:", messages(dropped) + messages(resumed))
    print("drain file removed:", not os.path.exists(os.path.join(drain, name)))
    # The runner runs a retried init container again under the same name, as a new container.
    create("id-2")
    write_output(6, 6)
    rerun = Stream()
    await ship_until(collector, rerun, lambda: messages(rerun) and not os.path.exists(os.path.join(drain, name)))
    print("rerun delivered:", messages(rerun))

asyncio.new_event_loop().run_until_complete(main())
`, dir, cli))
	want := "drain file kept after the drop: True\n" +
		"delivered: ['line 1', 'line 2', 'line 3', 'line 4', 'line 5']\n" +
		"drain file removed: True\n" +
		"rerun delivered: ['line 6']\n"
	var got strings.Builder
	for _, line := range strings.SplitAfter(out, "\n") {
		if !strings.HasPrefix(line, "[cloudvm-tunnel-agent]") {
			got.WriteString(line)
		}
	}
	if got.String() != want {
		t.Fatalf("agent output:\n%s\nwant:\n%s", out, want)
	}
}

func TestWorkerLogs_GatewayClosesUnknownStreams(t *testing.T) {
	f := newTunnelFixture(t, time.Minute)
	const id, token = "i-unknown-stream", "tok"
	f.addNode(id, "eu-west-1", token)

	conn, err := net.Dial("tcp", f.gateway.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer conn.Close()
	hello, _ := json.Marshal(TunnelHandshakeRequest{InstanceID: id, AuthToken: token}) //nolint:gosec // test token
	_, _ = conn.Write(append(hello, '\n'))
	if line, err := bufio.NewReader(conn).ReadString('\n'); err != nil || !strings.Contains(line, `"ok"`) {
		t.Fatalf("handshake: %q %v", line, err)
	}
	session, err := yamux.Client(conn, DefaultYamuxConfig())
	if err != nil {
		t.Fatalf("yamux: %v", err)
	}
	defer session.Close()
	stream, err := session.OpenStream()
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	_, _ = stream.Write([]byte(`{"stream":"metrics","version":1}` + "\n"))
	_ = stream.SetReadDeadline(time.Now().Add(5 * time.Second))
	if _, err := stream.Read(make([]byte, 1)); !errors.Is(err, io.EOF) {
		t.Fatalf("gateway must close streams of unknown kinds, read returned %v", err)
	}
}

func TestPodLogBuffer_BoundsBytesAndPods(t *testing.T) {
	b := NewPodLogBuffer(2, 10)
	for _, text := range []string{"aaaa", "bbbb", "cccc"} {
		b.Append("p1", PodLogLine{Text: text})
	}
	if got := b.Lines("p1"); len(got) != 2 || got[0].Text != "bbbb" {
		t.Errorf("p1 lines = %+v, want the last two", got)
	}
	b.Append("p2", PodLogLine{Text: strings.Repeat("z", 50)})
	if got := b.Lines("p2"); len(got) != 1 || len(got[0].Text) != 10 {
		t.Errorf("an overlong line must be truncated to the pod budget, got %+v", got)
	}
	b.Append("p3", PodLogLine{Text: "x"})
	if b.Lines("p1") != nil || b.Lines("p3") == nil {
		t.Error("the oldest pod must be evicted beyond maxPods")
	}
}

func TestRenderPodLogs_AppliesOptions(t *testing.T) {
	now := time.Date(2026, 10, 26, 12, 0, 0, 0, time.UTC)
	lines := []PodLogLine{
		{Time: now.Add(-time.Hour), Text: "old"},
		{Time: now.Add(-time.Minute), Text: "recent-1"},
		{Time: now.Add(-time.Second), Text: "recent-2"},
	}
	cases := []struct {
		opts GetLogsOptions
		want string
	}{
		{GetLogsOptions{}, "old\nrecent-1\nrecent-2\n"},
		{GetLogsOptions{Tail: 1}, "recent-2\n"},
		{GetLogsOptions{SinceSeconds: 120}, "recent-1\nrecent-2\n"},
		{GetLogsOptions{SinceTime: now.Add(-30 * time.Second)}, "recent-2\n"},
		{GetLogsOptions{LimitBytes: 5}, "old\nr"},
		{GetLogsOptions{Tail: 1, Timestamps: true}, "2026-10-26T11:59:59Z recent-2\n"},
	}
	for _, c := range cases {
		if got := renderPodLogs(lines, c.opts, now); got != c.want {
			t.Errorf("renderPodLogs(%+v) = %q, want %q", c.opts, got, c.want)
		}
	}
}

func TestHandleGetLogs_RejectsMissingPodUID(t *testing.T) {
	s := NewServer(NewMockProvider(), "test-cluster")
	rec := httptest.NewRecorder()
	s.handleGetLogs(rec, httptest.NewRequest(http.MethodGet, "/getLogs", strings.NewReader(`{"PodName":"x"}`)))
	if rec.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", rec.Code)
	}
}

func TestTunnelAgent_PythonAgentStreamsLogFiles(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("python3 not available")
	}
	f := newTunnelFixture(t, time.Minute)
	collector := withCollector(t, f)
	const instanceID, token = "i-python-logs", "agent-token"
	f.addNodeWithPod(instanceID, "us-east-2", token, "uid-py", "team-b", "job-py")

	endpoint, caFile := startTLSTerminator(t, f.gateway.Addr().String())
	dir := t.TempDir()
	tokenFile := filepath.Join(dir, "runner-token")
	bootstrapLog := filepath.Join(dir, "bootstrap.log")
	agentFile := filepath.Join(dir, "agent.py")
	for path, content := range map[string]string{tokenFile: token, bootstrapLog: "line one\nline two\n", agentFile: tunnelAgentScript} {
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatalf("write %s: %v", path, err)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var agentLog syncBuffer
	cmd := exec.CommandContext(ctx, python, "-I", agentFile) //nolint:gosec // test-controlled script path
	cmd.Env = append(os.Environ(),
		"CLOUDVM_TUNNEL_CA_FILE="+caFile,
		"CLOUDVM_TUNNEL_ENDPOINT="+endpoint,
		"CLOUDVM_TCP_PROXY_PORT=0",
		"CLOUDVM_INSTANCE_ID="+instanceID,
		"CLOUDVM_RUNNER_TOKEN_FILE="+tokenFile,
		"CLOUDVM_ATTESTATION_FILE="+filepath.Join(dir, "absent"),
		"CLOUDVM_TUNNEL_MAX_BACKOFF=0.2",
		"CLOUDVM_LOG_FILES=bootstrap="+bootstrapLog,
		"CLOUDVM_CONTAINER_LOGS=0",
	)
	cmd.Stderr = &agentLog
	if err := cmd.Start(); err != nil {
		t.Fatalf("start agent: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("agent log:\n%s", agentLog.String())
		}
	})

	waitForWithin(t, 60*time.Second, "existing lines", func() bool { _, n := collector.find("line two"); return n == 1 })
	rec, _ := collector.find("line one")
	if rec.resource["cloudvm.instance_id"] != instanceID || rec.resource["cloudvm.region"] != "us-east-2" || rec.resource["k8s.pod.name"] != "job-py" || rec.attrs["cloudvm.log.source"] != "bootstrap" {
		t.Errorf("record = %+v", rec)
	}

	appendLine := func(text string) {
		file, err := os.OpenFile(bootstrapLog, os.O_APPEND|os.O_WRONLY, 0o600) //nolint:gosec // test temp file
		if err != nil {
			t.Fatalf("open log: %v", err)
		}
		_, _ = file.WriteString(text + "\n")
		_ = file.Close()
	}
	appendLine("line three")
	waitForWithin(t, 30*time.Second, "appended line", func() bool { _, n := collector.find("line three"); return n == 1 })

	// After a reconnect the agent resumes where it stopped instead of resending the file.
	session, _ := f.registry.Get(instanceID)
	_ = f.registry.Close(instanceID)
	waitForWithin(t, 60*time.Second, "reconnect", func() bool { s, ok := f.registry.Get(instanceID); return ok && s != session })
	appendLine("line four")
	waitForWithin(t, 30*time.Second, "line after reconnect", func() bool { _, n := collector.find("line four"); return n == 1 })
	if _, n := collector.find("line one"); n != 1 {
		t.Errorf("line one delivered %d times, want once", n)
	}
}
