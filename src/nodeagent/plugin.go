// PodAPI speaks the plugin's interLink pod endpoints (/create, /delete, /status, /getLogs) with the payload shapes the
// plugin parses, so pods on agent-served nodes take the same path as pods on interLink virtual nodes.

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
)

// maxStatusBody bounds a /status response; it holds one small entry per pod of the shard.
const maxStatusBody = 64 << 20

// PodAPI calls the plugin's per-pod endpoints.
type PodAPI struct {
	baseURL string
	http    *http.Client
}

// NewPodAPI returns a client for the plugin at baseURL.
func NewPodAPI(baseURL string, httpClient *http.Client) *PodAPI {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &PodAPI{baseURL: strings.TrimRight(baseURL, "/"), http: httpClient}
}

// createRequest is the body of /create: the pod and the Secrets its imagePullSecrets name.
type createRequest struct {
	Pod     *corev1.Pod     `json:"pod"`
	Secrets []corev1.Secret `json:"secrets,omitempty"`
}

// WaitingState mirrors the plugin's waiting container state.
type WaitingState struct {
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message,omitempty"`
}

// RunningState mirrors the plugin's running container state.
type RunningState struct {
	StartedAt time.Time `json:"startedAt"`
}

// TerminatedState mirrors the plugin's terminated container state.
type TerminatedState struct {
	ExitCode    int32     `json:"exitCode"`
	Reason      string    `json:"reason,omitempty"`
	Message     string    `json:"message,omitempty"`
	FinishedAt  time.Time `json:"finishedAt"`
	ContainerID string    `json:"containerID,omitempty"`
}

// ContainerState mirrors the plugin's container state.
type ContainerState struct {
	Waiting    *WaitingState    `json:"waiting,omitempty"`
	Running    *RunningState    `json:"running,omitempty"`
	Terminated *TerminatedState `json:"terminated,omitempty"`
}

// ContainerStatus mirrors one container entry of a /status response.
type ContainerStatus struct {
	Name  string         `json:"name"`
	Image string         `json:"image,omitempty"`
	State ContainerState `json:"state"`
	Ready bool           `json:"ready"`
}

// PodStatus mirrors one entry of a /status response.
type PodStatus struct {
	UID            string            `json:"UID"`
	Name           string            `json:"name"`
	Namespace      string            `json:"namespace"`
	JID            string            `json:"JID"`
	Containers     []ContainerStatus `json:"containers"`
	InitContainers []ContainerStatus `json:"initContainers"`
	PodIP          string            `json:"podIP,omitempty"`
}

// logsRequest is the body of /getLogs.
type logsRequest struct {
	Namespace     string      `json:"Namespace"`
	PodUID        string      `json:"PodUID"`
	PodName       string      `json:"PodName"`
	ContainerName string      `json:"ContainerName"`
	Opts          logsOptions `json:"Opts"`
}

// logsOptions mirrors interLink's ContainerLogOpts.
type logsOptions struct {
	Tail         int       `json:"Tail"`
	LimitBytes   int       `json:"Bytes"`
	Timestamps   bool      `json:"Timestamps"`
	Follow       bool      `json:"Follow"`
	Previous     bool      `json:"Previous"`
	SinceSeconds int       `json:"SinceSeconds"`
	SinceTime    time.Time `json:"SinceTime"`
}

// Create starts the pod on the VM of its node.
func (a *PodAPI) Create(ctx context.Context, pod *corev1.Pod, secrets []corev1.Secret) error {
	return a.call(ctx, "/create", createRequest{Pod: pod, Secrets: secrets})
}

// Delete stops the pod and releases its share of the VM.
func (a *PodAPI) Delete(ctx context.Context, pod *corev1.Pod) error {
	return a.call(ctx, "/delete", createRequest{Pod: pod})
}

// Status returns the status of every listed pod in one call.
func (a *PodAPI) Status(ctx context.Context, pods []*corev1.Pod) ([]PodStatus, error) {
	if len(pods) == 0 {
		return nil, nil
	}
	resp, err := a.post(ctx, "/status", pods)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, statusError("/status", resp)
	}
	var out []PodStatus
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxStatusBody)).Decode(&out); err != nil {
		return nil, fmt.Errorf("decode /status: %w", err)
	}
	return out, nil
}

// Logs returns the buffered output of a pod's container.
func (a *PodAPI) Logs(ctx context.Context, pod *corev1.Pod, container string, opts logsOptions) (io.ReadCloser, error) {
	resp, err := a.post(ctx, "/getLogs", logsRequest{
		Namespace: pod.Namespace, PodUID: string(pod.UID), PodName: pod.Name, ContainerName: container, Opts: opts,
	})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		defer func() { _ = resp.Body.Close() }()
		return nil, statusError("/getLogs", resp)
	}
	return resp.Body, nil
}

func (a *PodAPI) post(ctx context.Context, path string, body any) (*http.Response, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("encode %s: %w", path, err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.baseURL+path, bytes.NewReader(raw))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := a.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("POST %s: %w", path, err)
	}
	return resp, nil
}

// call posts the body and discards a successful response.
func (a *PodAPI) call(ctx context.Context, path string, body any) error {
	resp, err := a.post(ctx, path, body)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return statusError(path, resp)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

// StatusError is a non-200 answer of the plugin.
type StatusError struct {
	Path    string
	Code    int
	Message string
}

func (e *StatusError) Error() string {
	return fmt.Sprintf("plugin %s: HTTP %d: %s", e.Path, e.Code, e.Message)
}

// isRefusal reports whether the plugin refused a pod its Node's VM cannot take.
func isRefusal(err error) bool {
	var se *StatusError
	return errors.As(err, &se) && se.Code == http.StatusConflict
}

func statusError(path string, resp *http.Response) error {
	msg, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return &StatusError{Path: path, Code: resp.StatusCode, Message: strings.TrimSpace(string(msg))}
}
