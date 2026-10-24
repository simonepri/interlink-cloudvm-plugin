// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.mozilla.org/pkcs7"
)

// AWSInstanceIdentity represents the verified payload of an EC2 instance identity document.
type AWSInstanceIdentity struct {
	InstanceID       string    `json:"instanceId"`
	AccountID        string    `json:"accountId"`
	Region           string    `json:"region"`
	AvailabilityZone string    `json:"availabilityZone"`
	InstanceType     string    `json:"instanceType"`
	PrivateIP        string    `json:"privateIp"`
	ImageID          string    `json:"imageId"`
	Architecture     string    `json:"architecture"`
	PendingTime      time.Time `json:"pendingTime"`
	Version          string    `json:"version,omitempty"`
}

// GCPInstanceIdentity represents the verified identity claims from a GCP VM identity token.
type GCPInstanceIdentity struct {
	Subject       string `json:"sub"`
	Email         string `json:"email"`
	Audience      string `json:"aud"`
	Issuer        string `json:"iss"`
	ExpiresAt     int64  `json:"exp"`
	IssuedAt      int64  `json:"iat"`
	InstanceID    string `json:"instanceId"`
	InstanceName  string `json:"instanceName"`
	ProjectID     string `json:"projectId"`
	ProjectNumber int64  `json:"projectNumber"`
	Zone          string `json:"zone"`
}

// AttestationVerifier verifies cloud instance identity attestation documents.
type AttestationVerifier interface {
	VerifyAWS(ctx context.Context, pkcs7Data []byte) (*AWSInstanceIdentity, error)
	VerifyGCP(ctx context.Context, token string, audience string) (*GCPInstanceIdentity, error)
}

// DefaultAttestationVerifier provides standard AWS PKCS#7 and GCP VM identity JWT verification.
type DefaultAttestationVerifier struct {
	awsTrustStore *x509.CertPool
	gcpCertURL    string
	gcpKeys       map[string]*rsa.PublicKey
	gcpKeysMu     sync.RWMutex
	httpClient    *http.Client
}

// NewDefaultAttestationVerifier initializes a DefaultAttestationVerifier with standard defaults.
func NewDefaultAttestationVerifier() *DefaultAttestationVerifier {
	return &DefaultAttestationVerifier{
		gcpCertURL: "https://www.googleapis.com/oauth2/v1/certs",
		gcpKeys:    make(map[string]*rsa.PublicKey),
		httpClient: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

// SetAWSTrustStore configures an optional x509.CertPool to verify the AWS signing certificate chain.
func (v *DefaultAttestationVerifier) SetAWSTrustStore(pool *x509.CertPool) {
	v.awsTrustStore = pool
}

// SetGCPCertURL configures the URL from which GCP public certificates are fetched.
func (v *DefaultAttestationVerifier) SetGCPCertURL(url string) {
	v.gcpCertURL = url
}

// SetGCPPublicKey registers a pre-configured RSA public key for a key ID (useful for tests and isolated nets).
func (v *DefaultAttestationVerifier) SetGCPPublicKey(kid string, key *rsa.PublicKey) {
	v.gcpKeysMu.Lock()
	defer v.gcpKeysMu.Unlock()
	v.gcpKeys[kid] = key
}

// VerifyAWS verifies an AWS PKCS#7 signed instance identity document and returns the identity payload.
func (v *DefaultAttestationVerifier) VerifyAWS(_ context.Context, pkcs7Data []byte) (*AWSInstanceIdentity, error) {
	if len(pkcs7Data) == 0 {
		return nil, errors.New("empty PKCS#7 attestation data")
	}

	raw := pkcs7Data
	trimmed := strings.TrimSpace(string(pkcs7Data))

	// Check if data is PEM encoded
	if strings.HasPrefix(trimmed, "-----BEGIN") {
		block, _ := pem.Decode([]byte(trimmed))
		if block == nil {
			return nil, errors.New("failed to decode PEM block from PKCS#7 attestation data")
		}
		raw = block.Bytes
	} else {
		// Attempt standard or raw base64 decode if string looks base64-encoded
		cleaned := strings.ReplaceAll(strings.ReplaceAll(trimmed, "\n", ""), "\r", "")
		if decoded, err := base64.StdEncoding.DecodeString(cleaned); err == nil && len(decoded) > 0 {
			raw = decoded
		}
	}

	p7, err := pkcs7.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("failed to parse PKCS#7 structure: %w", err)
	}

	if v.awsTrustStore != nil {
		if err := p7.VerifyWithChain(v.awsTrustStore); err != nil {
			return nil, fmt.Errorf("PKCS#7 signature verification failed against trust store: %w", err)
		}
	} else {
		if err := p7.Verify(); err != nil {
			return nil, fmt.Errorf("PKCS#7 signature verification failed: %w", err)
		}
	}

	var identity AWSInstanceIdentity
	if err := json.Unmarshal(p7.Content, &identity); err != nil {
		return nil, fmt.Errorf("failed to parse AWS instance identity JSON: %w", err)
	}

	if identity.InstanceID == "" {
		return nil, errors.New("AWS instance identity document missing instanceId")
	}
	if identity.Region == "" {
		return nil, errors.New("AWS instance identity document missing region")
	}

	return &identity, nil
}

type gcpJWTHeader struct {
	Alg string `json:"alg"`
	Kid string `json:"kid"`
	Typ string `json:"typ"`
}

type gcpComputeEngineClaims struct {
	InstanceID    string `json:"instance_id"`
	InstanceName  string `json:"instance_name"`
	ProjectID     string `json:"project_id"`
	ProjectNumber int64  `json:"project_number"`
	Zone          string `json:"zone"`
}

type gcpGoogleClaims struct {
	ComputeEngine gcpComputeEngineClaims `json:"compute_engine"`
}

type gcpIdentityClaims struct {
	Iss    string          `json:"iss"`
	Sub    string          `json:"sub"`
	Aud    string          `json:"aud"`
	Exp    int64           `json:"exp"`
	Iat    int64           `json:"iat"`
	Email  string          `json:"email"`
	Google gcpGoogleClaims `json:"google"`
}

// VerifyGCP verifies a Google Cloud VM identity JWT and extracts the instance identity.
func (v *DefaultAttestationVerifier) VerifyGCP(ctx context.Context, token string, expectedAudience string) (*GCPInstanceIdentity, error) {
	token = strings.TrimSpace(token)
	if token == "" {
		return nil, errors.New("empty GCP identity token")
	}

	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("invalid JWT format: expected 3 dot-separated segments, got %d", len(parts))
	}

	headerBytes, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return nil, fmt.Errorf("failed to decode JWT header: %w", err)
	}

	var header gcpJWTHeader
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return nil, fmt.Errorf("failed to parse JWT header: %w", err)
	}

	if header.Alg != "RS256" {
		return nil, fmt.Errorf("unsupported JWT algorithm %q, expected RS256", header.Alg)
	}

	payloadBytes, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, fmt.Errorf("failed to decode JWT payload: %w", err)
	}

	var claims gcpIdentityClaims
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return nil, fmt.Errorf("failed to parse JWT claims: %w", err)
	}

	// Validate standard claims
	if claims.Iss != "https://accounts.google.com" && claims.Iss != "accounts.google.com" {
		return nil, fmt.Errorf("invalid token issuer: %q", claims.Iss)
	}

	if expectedAudience != "" && claims.Aud != expectedAudience {
		return nil, fmt.Errorf("token audience mismatch: expected %q, got %q", expectedAudience, claims.Aud)
	}

	now := time.Now().Unix()
	const clockSkew = 60 // 1 minute tolerance
	if claims.Exp != 0 && claims.Exp < now-clockSkew {
		return nil, fmt.Errorf("token expired at %s (current time: %s)",
			time.Unix(claims.Exp, 0).UTC().Format(time.RFC3339),
			time.Unix(now, 0).UTC().Format(time.RFC3339))
	}
	if claims.Iat != 0 && claims.Iat > now+clockSkew {
		return nil, fmt.Errorf("token issued in the future: iat %s > current %s",
			time.Unix(claims.Iat, 0).UTC().Format(time.RFC3339),
			time.Unix(now, 0).UTC().Format(time.RFC3339))
	}

	// Signature verification
	pubKey, err := v.getGCPPublicKey(ctx, header.Kid)
	if err != nil {
		return nil, fmt.Errorf("failed to obtain public key for kid %q: %w", header.Kid, err)
	}

	signedContent := parts[0] + "." + parts[1]
	sigBytes, err := base64.RawURLEncoding.DecodeString(parts[2])
	if err != nil {
		return nil, fmt.Errorf("failed to decode signature: %w", err)
	}

	hash := sha256.Sum256([]byte(signedContent))
	if err := rsa.VerifyPKCS1v15(pubKey, crypto.SHA256, hash[:], sigBytes); err != nil {
		return nil, fmt.Errorf("GCP JWT signature verification failed: %w", err)
	}

	return &GCPInstanceIdentity{
		Subject:       claims.Sub,
		Email:         claims.Email,
		Audience:      claims.Aud,
		Issuer:        claims.Iss,
		ExpiresAt:     claims.Exp,
		IssuedAt:      claims.Iat,
		InstanceID:    claims.Google.ComputeEngine.InstanceID,
		InstanceName:  claims.Google.ComputeEngine.InstanceName,
		ProjectID:     claims.Google.ComputeEngine.ProjectID,
		ProjectNumber: claims.Google.ComputeEngine.ProjectNumber,
		Zone:          claims.Google.ComputeEngine.Zone,
	}, nil
}

func (v *DefaultAttestationVerifier) getGCPPublicKey(ctx context.Context, kid string) (*rsa.PublicKey, error) {
	v.gcpKeysMu.RLock()
	key, ok := v.gcpKeys[kid]
	v.gcpKeysMu.RUnlock()
	if ok {
		return key, nil
	}

	if v.gcpCertURL == "" {
		return nil, fmt.Errorf("public key not found for kid %q and no cert URL configured", kid)
	}

	if err := v.fetchGCPCertificates(ctx); err != nil {
		return nil, fmt.Errorf("failed to fetch GCP certificates: %w", err)
	}

	v.gcpKeysMu.RLock()
	key, ok = v.gcpKeys[kid]
	v.gcpKeysMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("public key for kid %q not found in fetched certificates", kid)
	}
	return key, nil
}

func (v *DefaultAttestationVerifier) fetchGCPCertificates(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.gcpCertURL, nil)
	if err != nil {
		return err
	}

	resp, err := v.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code fetching GCP certs: %d", resp.StatusCode)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1024*1024))
	if err != nil {
		return err
	}

	var certsMap map[string]string
	if err := json.Unmarshal(body, &certsMap); err != nil {
		return fmt.Errorf("failed to parse certificate JSON map: %w", err)
	}

	v.gcpKeysMu.Lock()
	defer v.gcpKeysMu.Unlock()
	for kid, pemStr := range certsMap {
		block, _ := pem.Decode([]byte(pemStr))
		if block == nil {
			continue
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			continue
		}
		if rsaKey, ok := cert.PublicKey.(*rsa.PublicKey); ok {
			v.gcpKeys[kid] = rsaKey
		}
	}
	return nil
}
