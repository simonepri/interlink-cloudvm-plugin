// The kubelet API serving certificate: a mounted key pair, a self-signed certificate, or one the cluster signs through
// a CertificateSigningRequest for the shard's pod IP and renews before it expires.

package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"sync"
	"time"

	certificatesv1 "k8s.io/api/certificates/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/util/certificate"
)

// servingCertUsages are the key usages of every requested serving certificate; the key is ECDSA, so no key encipherment.
var servingCertUsages = []certificatesv1.KeyUsage{certificatesv1.UsageDigitalSignature, certificatesv1.UsageServerAuth}

// ServingCertConfig selects and configures the source of the serving certificate.
type ServingCertConfig struct {
	// CertFile and KeyFile hold a mounted serving key pair; they take precedence over a signed certificate.
	CertFile, KeyFile string
	// SignerName is the CertificateSigningRequest signer; empty serves a self-signed certificate.
	SignerName string
	// PodName names the certificate; PodIP is its only IP address, the address every served Node advertises.
	PodName, PodIP string
	// Duration is the certificate lifetime requested from the signer.
	Duration time.Duration
	// Kube creates the CertificateSigningRequests.
	Kube kubernetes.Interface
}

// servingCertSubject is the subject the agent requests and the approver requires. The kubelet-serving signer signs
// only system:node: names in the system:nodes group; other signers get the pod name.
func servingCertSubject(signer, podName string) pkix.Name {
	if signer == certificatesv1.KubeletServingSignerName {
		return pkix.Name{CommonName: "system:node:" + podName, Organization: []string{"system:nodes"}}
	}
	return pkix.Name{CommonName: podName}
}

// StartServingCertificate returns the function the kubelet API reads its certificate from on every TLS handshake.
// With a signer it serves a self-signed certificate until the first signed one arrives, and requests a new one at
// 70 to 90 percent of each certificate's lifetime until ctx ends.
func StartServingCertificate(ctx context.Context, cfg ServingCertConfig) (func() *tls.Certificate, error) {
	if cfg.CertFile != "" || cfg.KeyFile != "" {
		if cfg.CertFile == "" || cfg.KeyFile == "" {
			return nil, errors.New("set both the serving certificate and key files, or neither")
		}
		cert, err := tls.LoadX509KeyPair(cfg.CertFile, cfg.KeyFile)
		if err != nil {
			return nil, err
		}
		return func() *tls.Certificate { return &cert }, nil
	}
	certPEM, keyPEM, err := selfSignedCertificate(cfg.PodIP)
	if err != nil {
		return nil, err
	}
	if cfg.SignerName == "" {
		cert, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return nil, err
		}
		return func() *tls.Certificate { return &cert }, nil
	}
	ip := net.ParseIP(cfg.PodIP)
	if ip == nil {
		return nil, fmt.Errorf("pod IP %q is not an IP address", cfg.PodIP)
	}
	manager, err := certificate.NewManager(&certificate.Config{
		ClientsetFn: func(*tls.Certificate) (kubernetes.Interface, error) { return cfg.Kube, nil },
		Template: &x509.CertificateRequest{
			Subject:     servingCertSubject(cfg.SignerName, cfg.PodName),
			IPAddresses: []net.IP{ip},
		},
		SignerName:                   cfg.SignerName,
		RequestedCertificateLifetime: &cfg.Duration,
		Usages:                       servingCertUsages,
		CertificateStore:             &memoryStore{},
		BootstrapCertificatePEM:      certPEM,
		BootstrapKeyPEM:              keyPEM,
		Name:                         "kubelet API serving",
		Ctx:                          &ctx,
	})
	if err != nil {
		return nil, err
	}
	manager.Start()
	return manager.Current, nil
}

// memoryStore keeps the current key pair in memory only, so every agent restart requests a fresh certificate.
type memoryStore struct {
	mu   sync.Mutex
	cert *tls.Certificate
}

func (s *memoryStore) Current() (*tls.Certificate, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cert == nil {
		err := certificate.NoCertKeyError("no serving certificate yet")
		return nil, &err
	}
	return s.cert, nil
}

func (s *memoryStore) Update(certPEM, keyPEM []byte) (*tls.Certificate, error) {
	cert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	logf("serving certificate %s issued by %q valid until %s", cert.Leaf.SerialNumber, cert.Leaf.Issuer,
		cert.Leaf.NotAfter.UTC().Format(time.RFC3339))
	s.cert = &cert
	return s.cert, nil
}

// selfSignedCertificate makes a one-year ECDSA certificate for the IP, as the kubelet does without a serving cert.
func selfSignedCertificate(ip string) (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 62))
	if err != nil {
		return nil, nil, err
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
		return nil, nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), nil
}
