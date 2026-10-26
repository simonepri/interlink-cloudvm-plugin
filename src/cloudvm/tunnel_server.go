// Package main implements the interlink-cloudvm-plugin daemon.
// This file connects the Server to the reverse tunnel gateway: worker authentication, launch settings, and the connect deadline.
package main

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// DefaultTunnelConnectTimeout bounds how long a launched worker may take to open its reverse tunnel.
const DefaultTunnelConnectTimeout = 10 * time.Minute

// DefaultMeshTags is the tailnet ACL tag workers advertise when joining the mesh.
const DefaultMeshTags = "tag:clvm-worker"

// TunnelSettings configures how launched workers reach the plugin through the reverse tunnel.
type TunnelSettings struct {
	// Registry holds the worker sessions accepted by the gateway.
	Registry *SessionRegistry
	// Endpoint is the host:port tailnet and router workers dial, verified against the public CA.
	Endpoint string
	// Transports maps each region to the path its workers take to the gateway; unlisted regions use tailnet.
	Transports TunnelTransports
	// GatewayIP is the gateway ClusterIP router workers pin the Endpoint host to.
	GatewayIP string
	// PublicEndpoint is the host:port of the public listener public workers dial.
	PublicEndpoint string
	// IdentityVerifier checks the signed instance identity document of public workers; nil accepts every document.
	IdentityVerifier AttestationVerifier
	// MeshAuthKey is the tailnet auth key workers use to join the mesh at boot.
	MeshAuthKey string
	// MeshTags is the comma-separated list of tailnet tags workers advertise.
	MeshTags string
	// ConnectTimeout bounds how long a new worker may take to connect before it is failed and terminated.
	ConnectTimeout time.Duration
	// DirectFallback keeps the legacy private-IP HTTP runner path for same-VPC workers.
	DirectFallback bool
}

type tunnelState struct {
	TunnelSettings
	metrics *TunnelMetrics
}

// EnableTunnel routes every runner call through worker tunnel sessions and configures launches to open one.
func (s *Server) EnableTunnel(settings TunnelSettings) *TunnelMetrics {
	if settings.Registry == nil {
		settings.Registry = NewSessionRegistry()
	}
	if settings.MeshTags == "" {
		settings.MeshTags = DefaultMeshTags
	}
	if settings.ConnectTimeout <= 0 {
		settings.ConnectTimeout = DefaultTunnelConnectTimeout
	}
	runner := NewTunnelContainerRunner(settings.Registry)
	if settings.DirectFallback {
		runner.SetFallback(NewHTTPContainerRunner(9090))
	}
	metrics := NewTunnelMetrics(settings.Registry, s.nodeRegion)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.runner = runner
	s.tunnel = &tunnelState{TunnelSettings: settings, metrics: metrics}
	return metrics
}

func (s *Server) tunnelConfig() *tunnelState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.tunnel
}

func (s *Server) nodeRegion(instanceID string) string {
	if node, ok := s.nodePool.Node(instanceID); ok {
		return node.Region
	}
	return ""
}

// ValidateTunnelToken accepts a worker only if it is a tracked instance presenting the runner token issued at its launch.
func (s *Server) ValidateTunnelToken(_ context.Context, instanceID, token string) error {
	expected := ""
	if node, ok := s.nodePool.Node(instanceID); ok {
		expected = node.RunnerAuthToken
	}
	if expected == "" && s.tokenStore != nil {
		expected = s.tokenStore.Get(instanceID)
	}
	if expected == "" {
		return errors.New("unknown instance")
	}
	if subtle.ConstantTimeCompare([]byte(expected), []byte(token)) != 1 {
		return errors.New("token mismatch")
	}
	return nil
}

// applyTunnelSpec stamps the endpoint, transport and mesh credentials of the spec's region onto a launch spec.
func (s *Server) applyTunnelSpec(spec *InstanceSpec) error {
	t := s.tunnelConfig()
	if t == nil || spec == nil {
		return nil
	}
	transport := t.Transports.For(spec.Region)
	spec.TunnelTransport = string(transport)
	spec.RunnerDirectHTTP = t.DirectFallback
	switch transport {
	case TunnelTransportRouter:
		spec.TunnelEndpoint = t.Endpoint
		spec.TunnelGatewayIP = t.GatewayIP
	case TunnelTransportPublic:
		spec.TunnelEndpoint = t.PublicEndpoint
	default:
		spec.TunnelEndpoint = t.Endpoint
		spec.MeshTags = t.MeshTags
		if t.MeshAuthKey != "" {
			spec.MeshAuthKey = t.MeshAuthKey
		}
		if spec.MeshAuthKey == "" {
			return fmt.Errorf("region %q uses the tailnet tunnel transport but TAILSCALE_AUTHKEY is not set", spec.Region)
		}
		return nil
	}
	// Router and public workers never join the tailnet, so a pod-supplied key must not reach their user data.
	spec.MeshAuthKey = ""
	spec.MeshTags = ""
	return nil
}

// encryptMeshAuthKey moves the mesh auth key into a KMS envelope so only ciphertext reaches user data.
func encryptMeshAuthKey(ctx context.Context, client KMSClient, keyID string, spec *InstanceSpec) error {
	if spec.MeshAuthKey == "" {
		return nil
	}
	ciphertext, err := EncryptRunnerToken(ctx, client, keyID, spec.MeshAuthKey)
	if err != nil {
		return fmt.Errorf("failed to encrypt mesh auth key: %w", err)
	}
	spec.MeshAuthKeyCiphertext = ciphertext
	spec.MeshAuthKey = ""
	return nil
}

// tunnelDeadlineExceeded reports whether node was launched longer ago than the connect timeout without ever opening a tunnel.
func (s *Server) tunnelDeadlineExceeded(node *CloudVMNode, launchedAt time.Time) bool {
	t := s.tunnelConfig()
	if t == nil || node == nil {
		return false
	}
	return time.Since(launchedAt) >= t.ConnectTimeout && !t.Registry.Has(node.InstanceID)
}

// startRetryTimeout returns how long a new node's first container start is retried.
func (s *Server) startRetryTimeout() time.Duration {
	timeout := 5 * time.Minute
	if t := s.tunnelConfig(); t != nil {
		timeout += t.ConnectTimeout
	}
	return timeout
}

// failTunnelDeadline fails the pod and terminates a worker whose reverse tunnel never connected.
func (s *Server) failTunnelDeadline(ctx context.Context, node *CloudVMNode, req *StartContainerRequest) {
	msg := fmt.Sprintf("worker %s did not open its reverse tunnel within %v", node.InstanceID, s.tunnelConfig().ConnectTimeout)
	log.Printf("[cloudvm] %s; terminating it", sanitizeLog(msg)) //nolint:gosec // sanitized
	msg = s.withConsoleTail(ctx, node, msg)
	term := &ContainerTerminatedState{ExitCode: 1, Reason: "TunnelConnectTimeout", Message: msg, FinishedAt: time.Now()}
	s.mu.Lock()
	s.completedPods[req.PodUID] = term
	if s.completedStore != nil {
		_ = s.completedStore.Set(req.PodUID, &CompletedPodRecord{
			PodUID: req.PodUID, ExitCode: term.ExitCode, Reason: term.Reason, Message: term.Message, FinishedAt: term.FinishedAt,
		})
	}
	s.mu.Unlock()
	s.nodePool.MarkPodCompleted(req.PodUID, term.ExitCode, term.Reason, term.Message, term.FinishedAt)
	s.nodePool.UnregisterNode(node.InstanceID)
	s.TerminateIdleNode(ctx, node)
}

// handleMetrics serves tunnel metrics together with the embedded spot ticker's metrics when either is enabled.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	ticker := s.spotTicker
	tunnel := s.tunnel
	s.mu.RUnlock()
	var gatherers prometheus.Gatherers
	if ticker != nil && ticker.Metrics() != nil {
		gatherers = append(gatherers, ticker.Metrics().Registry)
	}
	if tunnel != nil {
		gatherers = append(gatherers, tunnel.metrics.Registry)
	}
	if len(gatherers) == 0 {
		http.Error(w, "no metrics enabled", http.StatusNotFound)
		return
	}
	promhttp.HandlerFor(gatherers, promhttp.HandlerOpts{}).ServeHTTP(w, r)
}
