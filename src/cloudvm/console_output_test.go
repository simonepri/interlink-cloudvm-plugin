// Tests console output decoding, tail truncation and failure-message enrichment on the launch and dead-VM paths.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
)

type consoleEC2Client struct {
	*mockEC2FleetClient
	output string
	err    error
	latest bool
	// posted is returned for requests without Latest when set, as for a shut-down instance.
	posted *string
}

func (c *consoleEC2Client) GetConsoleOutput(_ context.Context, in *ec2.GetConsoleOutputInput, _ ...func(*ec2.Options)) (*ec2.GetConsoleOutputOutput, error) {
	c.latest = aws.ToBool(in.Latest)
	if !c.latest && c.posted != nil {
		return &ec2.GetConsoleOutputOutput{Output: aws.String(base64.StdEncoding.EncodeToString([]byte(*c.posted)))}, nil
	}
	if c.err != nil {
		return nil, c.err
	}
	return &ec2.GetConsoleOutputOutput{Output: aws.String(base64.StdEncoding.EncodeToString([]byte(c.output)))}, nil
}

type consoleProvider struct {
	*MockProvider
	out string
	err error
}

func (c *consoleProvider) ConsoleOutput(context.Context, string, string) (string, error) {
	return c.out, c.err
}

func TestAWSProviderConsoleOutputDecodes(t *testing.T) {
	cl := &consoleEC2Client{mockEC2FleetClient: &mockEC2FleetClient{}, output: "boot failed: no docker"}
	p := &AWSProvider{Region: "us-east-1", client: cl}
	got, err := p.ConsoleOutput(context.Background(), "us-east-1", "i-1")
	if err != nil || got != "boot failed: no docker" || !cl.latest {
		t.Fatalf("got %q err %v latest %v", got, err, cl.latest)
	}
}

func TestTailBytesKeepsLastBytes(t *testing.T) {
	in := strings.Repeat("a", 5000) + "END"
	got := tailBytes(in, consoleTailBytes)
	if len(got) != consoleTailBytes || !strings.HasSuffix(got, "END") {
		t.Fatalf("len %d", len(got))
	}
	if got := tailBytes(strings.Repeat("é", 3000), 4095); len(got) > 4095 || !strings.HasPrefix(got, "é") {
		t.Fatalf("broken utf-8 boundary")
	}
}

func TestWithConsoleTail(t *testing.T) {
	node := NewCloudVMNode("i-1", ProviderMock, "us-east-1", "", ResourceVector{})
	cp := &consoleProvider{MockProvider: NewMockProvider(), out: "line1\nFATAL bootstrap"}
	s := NewServer(cp, "c")
	msg := s.withConsoleTail(context.Background(), node, "tunnel timeout")
	if !strings.HasPrefix(msg, "tunnel timeout") || !strings.Contains(msg, "FATAL bootstrap") {
		t.Fatalf("msg %q", msg)
	}
	cp.err = errors.New("denied")
	if got := s.withConsoleTail(context.Background(), node, "x"); got != "x" {
		t.Fatalf("error must leave message unchanged: %q", got)
	}
}

func TestAWSProviderConsoleOutputFallsBackToPostedOutput(t *testing.T) {
	posted := "posted after shutdown"
	for name, cl := range map[string]*consoleEC2Client{
		"empty latest": {mockEC2FleetClient: &mockEC2FleetClient{}, posted: &posted},
		"latest error": {mockEC2FleetClient: &mockEC2FleetClient{}, posted: &posted, err: errors.New("unsupported")},
	} {
		p := &AWSProvider{Region: "us-east-1", client: cl}
		got, err := p.ConsoleOutput(context.Background(), "us-east-1", "i-1")
		if err != nil || got != posted || cl.latest {
			t.Fatalf("%s: got %q err %v latest %v", name, got, err, cl.latest)
		}
	}
}

// captureLog redirects the standard logger for the rest of the test.
func captureLog(t *testing.T) *syncBuffer {
	t.Helper()
	buf := &syncBuffer{}
	prev := log.Writer()
	log.SetOutput(buf)
	t.Cleanup(func() { log.SetOutput(prev) })
	return buf
}

func postStatus(t *testing.T, srv *Server, body []byte) ContainerTerminatedState {
	t.Helper()
	w := httptest.NewRecorder()
	srv.handleStatus(w, httptest.NewRequest(http.MethodPost, "/status", bytes.NewReader(body)))
	var responses []PodStatusResponse
	if err := json.NewDecoder(w.Body).Decode(&responses); err != nil || len(responses) != 1 || len(responses[0].Containers) == 0 {
		t.Fatalf("status %d: %s", w.Code, w.Body.String())
	}
	term := responses[0].Containers[0].State.Terminated
	if term == nil {
		t.Fatalf("expected terminated container, got %+v", responses[0].Containers[0].State)
	}
	return *term
}

func TestReconcilerAttachesConsoleTailToDeadVM(t *testing.T) {
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))
	logs := captureLog(t)

	cp := &consoleProvider{MockProvider: NewMockProvider(), out: "cloud-init: pulling image\nFATAL: disk full\n"}
	srv := NewServer(cp, "cluster-console")
	body, _ := json.Marshal(&Pod{
		Metadata: PodMeta{Name: "boot-fail", Namespace: "default", UID: "uid-boot-fail"},
		Spec:     PodSpecWrapper{Containers: []ContainerSpec{{Name: "main", Image: "alpine"}}},
	})
	w := httptest.NewRecorder()
	srv.handleCreate(w, httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(body)))
	var created CreateStruct
	if err := json.NewDecoder(w.Body).Decode(&created); err != nil || w.Code != http.StatusOK {
		t.Fatalf("create %d: %s", w.Code, w.Body.String())
	}
	instID := strings.Split(created.PodJID, ":")[0]
	if err := cp.Terminate(context.Background(), instID); err != nil {
		t.Fatalf("terminate: %v", err)
	}

	reconciler := NewReconciler(cp, srv, cp, "cluster-console", 10*time.Minute)
	reconciler.SetLaunchGracePeriod(0)
	result, err := reconciler.ReconcileOnce(context.Background())
	if err != nil || len(result.EvictedPods) != 1 {
		t.Fatalf("evicted %v err %v", result.EvictedPods, err)
	}

	term := postStatus(t, srv, body)
	if !strings.HasPrefix(term.Message, "Underlying cloud VM terminated") || !strings.Contains(term.Message, "FATAL: disk full") {
		t.Fatalf("message lacks console tail: %q", term.Message)
	}
	if rec := srv.completedStore.Get("uid-boot-fail"); rec == nil || !strings.Contains(rec.Message, "FATAL: disk full") {
		t.Fatalf("persisted record lacks console tail: %+v", rec)
	}
	if out := logs.String(); !strings.Contains(out, instID+" in us-east-1") || !strings.Contains(out, "FATAL: disk full") {
		t.Fatalf("log lacks instance %s or console tail: %s", instID, out)
	}
}

func TestStatusReportsFailureMessageForFailedInstance(t *testing.T) {
	t.Setenv("TOKEN_STORE_PATH", filepath.Join(t.TempDir(), "tokens.json"))
	t.Setenv("COMPLETED_STORE_PATH", filepath.Join(t.TempDir(), "completed.json"))
	srv := NewServer(NewMockProvider(), "cluster-console")
	srv.mu.Lock()
	srv.instances["uid-cotenant"] = &Instance{
		ID: "i-dead", Provider: ProviderMock, Region: "us-east-1", State: StateFailed,
		FailureMessage: "Underlying cloud VM terminated (NodeLost)\n--- console output (last 4 KiB) ---\nkernel panic",
	}
	srv.instances["uid-legacy"] = &Instance{ID: "i-old", Provider: ProviderMock, Region: "us-east-1", State: StateFailed}
	srv.mu.Unlock()

	for uid, want := range map[string]string{"uid-cotenant": "kernel panic", "uid-legacy": "Cloud instance failed"} {
		body, _ := json.Marshal([]*Pod{{Metadata: PodMeta{Name: uid, Namespace: "default", UID: uid}}})
		if term := postStatus(t, srv, body); !strings.Contains(term.Message, want) {
			t.Fatalf("%s: message %q lacks %q", uid, term.Message, want)
		}
	}
}

// blockingTracker reports a fixed set of active pods and blocks MarkPodFailed until all of them are in flight.
type blockingTracker struct {
	mockTrackerWrapper
	inFlight atomic.Int32
	release  chan struct{}
}

func (b *blockingTracker) MarkPodFailed(ctx context.Context, podName, reason string) error {
	if int(b.inFlight.Add(1)) == len(b.activePods) {
		close(b.release)
	}
	select {
	case <-b.release:
	case <-time.After(5 * time.Second):
		return errors.New("MarkPodFailed calls ran serially")
	}
	return b.mockTrackerWrapper.MarkPodFailed(ctx, podName, reason)
}

func TestReconcilerMarksDeadPodsConcurrently(t *testing.T) {
	tracker := &blockingTracker{release: make(chan struct{})}
	tracker.activePods = map[string]string{"uid-a": "i-a", "uid-b": "i-b", "uid-c": "i-c"}
	mock := NewMockProvider()
	reconciler := NewReconciler(mock, tracker, mock, "cluster-console", 10*time.Minute)
	reconciler.SetLaunchGracePeriod(0)
	result, err := reconciler.ReconcileOnce(context.Background())
	if err != nil || len(result.EvictedPods) != 3 {
		t.Fatalf("evicted %v err %v", result.EvictedPods, err)
	}
}
