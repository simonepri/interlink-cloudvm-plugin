// Delivery of pod role credentials to the in-VM runner, over the reverse tunnel or directly over the mesh.
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
)

// PodCredentialsPusher is a ContainerRunner that can replace the role credentials of a running pod.
type PodCredentialsPusher interface {
	PushPodCredentials(ctx context.Context, node *CloudVMNode, podUID string, creds *PodAWSCredentials) error
}

func postPodCredentials(ctx context.Context, client *http.Client, url, runnerToken string, creds *PodAWSCredentials) error {
	payload, err := json.Marshal(creds) //nolint:gosec // the runner needs the credentials; the channel is authenticated
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(payload)) //nolint:gosec // runner endpoint on the private mesh or the reverse tunnel
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if runnerToken != "" {
		req.Header.Set("Authorization", "Bearer "+runnerToken)
	}
	resp, err := client.Do(req) //nolint:gosec // runner endpoint on the private mesh or the reverse tunnel
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		return fmt.Errorf("in-VM runner error (%d): %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}

// PushPodCredentials sends the pod's role credentials to the runner over the mesh.
func (h *HTTPContainerRunner) PushPodCredentials(ctx context.Context, node *CloudVMNode, podUID string, creds *PodAWSCredentials) error {
	url, err := h.endpoint(node, podCredentialsPath(podUID))
	if err != nil {
		return err
	}
	return postPodCredentials(ctx, h.client, url, node.RunnerAuthToken, creds)
}

// PushPodCredentials sends the pod's role credentials down the worker's reverse tunnel session.
func (t *TunnelContainerRunner) PushPodCredentials(ctx context.Context, node *CloudVMNode, podUID string, creds *PodAWSCredentials) error {
	if node == nil || node.InstanceID == "" {
		return errors.New("node or instanceID is missing")
	}
	session, ok := t.registry.Get(node.InstanceID)
	if !ok {
		if pusher, isPusher := t.fallback.(PodCredentialsPusher); isPusher {
			return pusher.PushPodCredentials(ctx, node, podUID, creds)
		}
		return fmt.Errorf("no active reverse tunnel session for instance %s", node.InstanceID)
	}
	return postPodCredentials(ctx, t.clientForSession(session), "http://clvm-tunnel"+podCredentialsPath(podUID), node.RunnerAuthToken, creds)
}

// PushPodCredentials records the credentials pushed for the pod.
func (m *MockContainerRunner) PushPodCredentials(_ context.Context, _ *CloudVMNode, podUID string, creds *PodAWSCredentials) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pushedCredentials == nil {
		m.pushedCredentials = make(map[string][]PodAWSCredentials)
	}
	m.pushedCredentials[podUID] = append(m.pushedCredentials[podUID], *creds)
	return nil
}

// PushedCredentials returns the credentials pushed for the pod, oldest first.
func (m *MockContainerRunner) PushedCredentials(podUID string) []PodAWSCredentials {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return append([]PodAWSCredentials(nil), m.pushedCredentials[podUID]...)
}
