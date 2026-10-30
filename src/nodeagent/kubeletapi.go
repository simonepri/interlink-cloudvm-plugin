// The shard's kubelet API: one HTTPS server for every Node the shard serves, routing /containerLogs and /pods by pod
// and admitting only callers whose client certificate a trusted CA signed, as the API server's kubelet client is.

package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/virtual-kubelet/virtual-kubelet/node/api"
	corev1 "k8s.io/api/core/v1"
)

// KubeletAPIConfig configures the kubelet API server.
type KubeletAPIConfig struct {
	// Addr is the listen address, such as :10250.
	Addr string
	// ServingCertificate returns the certificate to present; it is read on every handshake, so a renewed one applies to
	// the next connection.
	ServingCertificate func() *tls.Certificate
	// ClientCAFile holds the CAs whose client certificates may call the API.
	ClientCAFile string
}

// NewKubeletAPIServer returns the HTTPS server for the provider's pods.
func NewKubeletAPIServer(cfg KubeletAPIConfig, pods *PodProvider) (*http.Server, error) {
	caPEM, err := os.ReadFile(cfg.ClientCAFile)
	if err != nil {
		return nil, fmt.Errorf("reading client CA bundle: %w", err)
	}
	clientCAs := x509.NewCertPool()
	if !clientCAs.AppendCertsFromPEM(caPEM) {
		return nil, fmt.Errorf("client CA bundle %s holds no PEM certificate", cfg.ClientCAFile)
	}
	mux := http.NewServeMux()
	api.AttachPodRoutes(api.PodHandlerConfig{
		GetContainerLogs: pods.ContainerLogs,
		GetPods:          pods.GetPods,
		GetPodsFromKubernetes: func(ctx context.Context) ([]*corev1.Pod, error) {
			return pods.GetPods(ctx)
		},
	}, mux, true)
	return &http.Server{
		Addr:              cfg.Addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		TLSConfig: &tls.Config{
			MinVersion: tls.VersionTLS12,
			GetCertificate: func(*tls.ClientHelloInfo) (*tls.Certificate, error) {
				if cert := cfg.ServingCertificate(); cert != nil {
					return cert, nil
				}
				return nil, errors.New("no serving certificate available")
			},
			ClientAuth: tls.RequireAndVerifyClientCert,
			ClientCAs:  clientCAs,
		},
	}, nil
}
