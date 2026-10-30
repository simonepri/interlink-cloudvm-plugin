// The shard's kubelet API: one HTTPS server for every Node the shard serves, routing /containerLogs and /pods by pod
// and admitting only callers whose client certificate a trusted CA signed, as the API server's kubelet client is.

package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"fmt"
	"math/big"
	"net"
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
	// CertFile and KeyFile hold the serving certificate; when both are empty a self-signed one for NodeIP is made.
	CertFile, KeyFile string
	// NodeIP is the address the self-signed certificate names.
	NodeIP string
	// ClientCAFile holds the CAs whose client certificates may call the API.
	ClientCAFile string
}

// NewKubeletAPIServer returns the HTTPS server for the provider's pods.
func NewKubeletAPIServer(cfg KubeletAPIConfig, pods *PodProvider) (*http.Server, error) {
	cert, err := servingCertificate(cfg)
	if err != nil {
		return nil, err
	}
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
			MinVersion:   tls.VersionTLS12,
			Certificates: []tls.Certificate{cert},
			ClientAuth:   tls.RequireAndVerifyClientCert,
			ClientCAs:    clientCAs,
		},
	}, nil
}

func servingCertificate(cfg KubeletAPIConfig) (tls.Certificate, error) {
	if cfg.CertFile != "" || cfg.KeyFile != "" {
		if cfg.CertFile == "" || cfg.KeyFile == "" {
			return tls.Certificate{}, errors.New("set both the serving certificate and key files, or neither")
		}
		return tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
	}
	return selfSignedCertificate(cfg.NodeIP)
}

// selfSignedCertificate makes a one-year ECDSA certificate for the IP, as the kubelet does without a serving cert.
func selfSignedCertificate(ip string) (tls.Certificate, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return tls.Certificate{}, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		return tls.Certificate{}, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: "node-agent"},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(365 * 24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	if parsed := net.ParseIP(ip); parsed != nil {
		tmpl.IPAddresses = []net.IP{parsed}
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return tls.Certificate{}, err
	}
	return tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key}, nil
}
