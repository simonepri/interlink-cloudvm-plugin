// Gateway-side verification of AWS EC2 instance identity documents against embedded regional certificates and the launch record.
package main

import (
	"context"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"strings"

	"go.mozilla.org/pkcs7"
)

// TunnelAllowedAccountsEnv lists the comma-separated AWS account IDs whose instances may attest.
const TunnelAllowedAccountsEnv = "TUNNEL_ALLOWED_ACCOUNTS"

// LaunchRecord is what the plugin recorded when it launched an instance.
type LaunchRecord struct {
	InstanceID string
	Region     string
}

// LaunchRecordLookup returns the launch record for an instance, or false when the plugin never launched it.
type LaunchRecordLookup func(ctx context.Context, instanceID string) (LaunchRecord, bool)

// LaunchRecord returns the launch record of a tracked instance.
func (s *Server) LaunchRecord(_ context.Context, instanceID string) (LaunchRecord, bool) {
	node, ok := s.nodePool.Node(instanceID)
	if !ok {
		return LaunchRecord{}, false
	}
	return LaunchRecord{InstanceID: node.InstanceID, Region: node.Region}, true
}

// AWSIdentityVerifier verifies the RSA-2048 PKCS#7 instance identity document with the signing certificate
// AWS publishes for the document's region, then requires its instance ID, region and account to match the
// launch record and the account allowlist. It implements AttestationVerifier and rejects GCP tokens.
type AWSIdentityVerifier struct {
	certs           map[string]*x509.Certificate
	allowedAccounts map[string]struct{}
	lookup          LaunchRecordLookup
}

var _ AttestationVerifier = (*AWSIdentityVerifier)(nil)

// NewAWSIdentityVerifier builds a verifier from the embedded AWS regional certificates.
// An empty allowedAccounts set rejects every document.
func NewAWSIdentityVerifier(allowedAccounts []string, lookup LaunchRecordLookup) (*AWSIdentityVerifier, error) {
	if lookup == nil {
		return nil, errors.New("launch record lookup is required")
	}
	certs, err := parseAWSIdentityCertificates(awsIdentityCertificates)
	if err != nil {
		return nil, err
	}
	allowed := make(map[string]struct{}, len(allowedAccounts))
	for _, a := range allowedAccounts {
		if a = strings.TrimSpace(a); a != "" {
			allowed[a] = struct{}{}
		}
	}
	return &AWSIdentityVerifier{certs: certs, allowedAccounts: allowed, lookup: lookup}, nil
}

// AllowedAccountsFromEnv reads TUNNEL_ALLOWED_ACCOUNTS.
func AllowedAccountsFromEnv() []string {
	return strings.Split(os.Getenv(TunnelAllowedAccountsEnv), ",")
}

func parseAWSIdentityCertificates(pemByRegion map[string]string) (map[string]*x509.Certificate, error) {
	certs := make(map[string]*x509.Certificate, len(pemByRegion))
	for region, p := range pemByRegion {
		block, _ := pem.Decode([]byte(p))
		if block == nil {
			return nil, fmt.Errorf("embedded AWS certificate for %s is not PEM", region)
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("embedded AWS certificate for %s: %w", region, err)
		}
		certs[region] = cert
	}
	return certs, nil
}

// VerifyAWS checks the signature and the launch record, and returns the verified identity.
func (v *AWSIdentityVerifier) VerifyAWS(ctx context.Context, data []byte) (*AWSInstanceIdentity, error) {
	raw, err := decodePKCS7(data)
	if err != nil {
		return nil, err
	}
	p7, err := pkcs7.Parse(raw)
	if err != nil {
		return nil, fmt.Errorf("parse PKCS#7: %w", err)
	}

	// The region selects which certificate must verify the signature; a forged region only selects a
	// certificate the forger cannot sign for.
	var identity AWSInstanceIdentity
	if err := json.Unmarshal(p7.Content, &identity); err != nil {
		return nil, fmt.Errorf("parse identity document: %w", err)
	}
	cert, ok := v.certs[identity.Region]
	if !ok {
		return nil, fmt.Errorf("no AWS identity certificate for region %q", identity.Region)
	}
	p7.Certificates = []*x509.Certificate{cert}
	if err := p7.Verify(); err != nil {
		return nil, fmt.Errorf("identity document signature invalid: %w", err)
	}

	if _, ok := v.allowedAccounts[identity.AccountID]; !ok || identity.AccountID == "" {
		return nil, fmt.Errorf("account %q is not allowed", identity.AccountID)
	}
	rec, ok := v.lookup(ctx, identity.InstanceID)
	if !ok {
		return nil, fmt.Errorf("instance %q has no launch record", identity.InstanceID)
	}
	if rec.InstanceID != identity.InstanceID || rec.Region != identity.Region {
		return nil, fmt.Errorf("identity document (%s, %s) does not match launch record (%s, %s)",
			identity.InstanceID, identity.Region, rec.InstanceID, rec.Region)
	}
	return &identity, nil
}

// VerifyGCP always fails: this verifier covers AWS only.
func (v *AWSIdentityVerifier) VerifyGCP(context.Context, string, string) (*GCPInstanceIdentity, error) {
	return nil, errors.New("GCP attestation is not supported by the AWS identity verifier")
}

// decodePKCS7 accepts raw DER, PEM, or base64 (with optional line breaks) PKCS#7 data.
func decodePKCS7(data []byte) ([]byte, error) {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" {
		return nil, errors.New("empty attestation")
	}
	if strings.HasPrefix(trimmed, "-----BEGIN") {
		block, _ := pem.Decode([]byte(trimmed))
		if block == nil {
			return nil, errors.New("attestation is not valid PEM")
		}
		return block.Bytes, nil
	}
	cleaned := strings.NewReplacer("\n", "", "\r", "").Replace(trimmed)
	if decoded, err := base64.StdEncoding.DecodeString(cleaned); err == nil && len(decoded) > 0 {
		return decoded, nil
	}
	return data, nil
}
