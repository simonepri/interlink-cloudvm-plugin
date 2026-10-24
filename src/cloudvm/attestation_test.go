// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"crypto"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.mozilla.org/pkcs7"
)

func generateTestCertificate(t *testing.T) (*rsa.PrivateKey, *x509.Certificate) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate RSA key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(100),
		Subject: pkix.Name{
			CommonName: "AWS EC2 Identity",
		},
		NotBefore:             time.Now().Add(-1 * time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}

	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &priv.PublicKey, priv)
	if err != nil {
		t.Fatalf("failed to create certificate: %v", err)
	}

	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("failed to parse certificate: %v", err)
	}
	return priv, cert
}

func TestVerifyAWS_Valid(t *testing.T) {
	priv, cert := generateTestCertificate(t)
	identityJSON := `{
		"instanceId": "i-0123456789abcdef0",
		"accountId": "112233445566",
		"region": "us-east-1",
		"availabilityZone": "us-east-1a",
		"instanceType": "c5.large",
		"privateIp": "10.0.1.5"
	}`

	sd, err := pkcs7.NewSignedData([]byte(identityJSON))
	if err != nil {
		t.Fatalf("failed to init signed data: %v", err)
	}
	if err := sd.AddSigner(cert, priv, pkcs7.SignerInfoConfig{}); err != nil {
		t.Fatalf("failed to add signer: %v", err)
	}
	der, err := sd.Finish()
	if err != nil {
		t.Fatalf("failed to finish PKCS#7: %v", err)
	}

	verifier := NewDefaultAttestationVerifier()

	// 1. Raw DER
	res, err := verifier.VerifyAWS(context.Background(), der)
	if err != nil {
		t.Fatalf("expected VerifyAWS to succeed on raw DER, got: %v", err)
	}
	if res.InstanceID != "i-0123456789abcdef0" || res.Region != "us-east-1" {
		t.Errorf("unexpected verified identity: %+v", res)
	}

	// 2. Base64 encoded string
	b64 := base64.StdEncoding.EncodeToString(der)
	resB64, err := verifier.VerifyAWS(context.Background(), []byte(b64))
	if err != nil {
		t.Fatalf("expected VerifyAWS to succeed on base64, got: %v", err)
	}
	if resB64.InstanceID != "i-0123456789abcdef0" {
		t.Errorf("unexpected instance ID from base64: %s", resB64.InstanceID)
	}

	// 3. With Trust Store
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	verifier.SetAWSTrustStore(pool)
	resPool, err := verifier.VerifyAWS(context.Background(), der)
	if err != nil {
		t.Fatalf("expected VerifyAWS to succeed with valid trust store, got: %v", err)
	}
	if resPool.InstanceID != "i-0123456789abcdef0" {
		t.Errorf("unexpected instance ID: %s", resPool.InstanceID)
	}
}

func TestVerifyAWS_Errors(t *testing.T) {
	verifier := NewDefaultAttestationVerifier()

	// Empty data
	if _, err := verifier.VerifyAWS(context.Background(), nil); err == nil {
		t.Error("expected error on empty data")
	}

	// Malformed PKCS#7
	if _, err := verifier.VerifyAWS(context.Background(), []byte("not-pkcs7-data")); err == nil {
		t.Error("expected error on malformed data")
	}

	// Missing required fields
	priv, cert := generateTestCertificate(t)
	sd, _ := pkcs7.NewSignedData([]byte(`{"accountId":"123"}`)) // missing instanceId and region
	_ = sd.AddSigner(cert, priv, pkcs7.SignerInfoConfig{})
	der, _ := sd.Finish()
	if _, err := verifier.VerifyAWS(context.Background(), der); err == nil {
		t.Error("expected error when instanceId is missing")
	}
}

func generateTestJWT(t *testing.T, priv *rsa.PrivateKey, kid string, claims map[string]any) string {
	t.Helper()
	headerJSON, err := json.Marshal(map[string]string{
		"alg": "RS256",
		"kid": kid,
		"typ": "JWT",
	})
	if err != nil {
		t.Fatalf("failed to marshal header: %v", err)
	}
	claimsJSON, err := json.Marshal(claims)
	if err != nil {
		t.Fatalf("failed to marshal claims: %v", err)
	}

	headerB64 := base64.RawURLEncoding.EncodeToString(headerJSON)
	claimsB64 := base64.RawURLEncoding.EncodeToString(claimsJSON)
	signingInput := headerB64 + "." + claimsB64

	hashed := sha256.Sum256([]byte(signingInput))
	sig, err := rsa.SignPKCS1v15(rand.Reader, priv, crypto.SHA256, hashed[:])
	if err != nil {
		t.Fatalf("failed to sign JWT: %v", err)
	}
	sigB64 := base64.RawURLEncoding.EncodeToString(sig)

	return signingInput + "." + sigB64
}

func TestVerifyGCP_Valid(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	kid := "test-key-1"
	expectedAud := "https://clvm-tunnel.cell-us.c.corp.boltz.com"
	claims := map[string]any{
		"iss":   "https://accounts.google.com",
		"sub":   "service-account-id-123",
		"aud":   expectedAud,
		"exp":   time.Now().Add(1 * time.Hour).Unix(),
		"iat":   time.Now().Add(-1 * time.Minute).Unix(),
		"email": "runner@test-project.iam.gserviceaccount.com",
		"google": map[string]any{
			"compute_engine": map[string]any{
				"instance_id":    "123456789012345",
				"instance_name":  "worker-instance-1",
				"project_id":     "test-project",
				"project_number": 998877,
				"zone":           "us-central1-b",
			},
		},
	}

	token := generateTestJWT(t, priv, kid, claims)

	verifier := NewDefaultAttestationVerifier()
	verifier.SetGCPPublicKey(kid, &priv.PublicKey)

	id, err := verifier.VerifyGCP(context.Background(), token, expectedAud)
	if err != nil {
		t.Fatalf("expected VerifyGCP to succeed, got: %v", err)
	}

	if id.InstanceID != "123456789012345" {
		t.Errorf("unexpected InstanceID: %s", id.InstanceID)
	}
	if id.InstanceName != "worker-instance-1" {
		t.Errorf("unexpected InstanceName: %s", id.InstanceName)
	}
	if id.ProjectID != "test-project" {
		t.Errorf("unexpected ProjectID: %s", id.ProjectID)
	}
	if id.Zone != "us-central1-b" {
		t.Errorf("unexpected Zone: %s", id.Zone)
	}
}

func TestVerifyGCP_FetchFromHTTP(t *testing.T) {
	priv, cert := generateTestCertificate(t)
	kid := "server-kid-1"

	certPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "CERTIFICATE",
		Bytes: cert.Raw,
	})

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			kid: string(certPEM),
		})
	}))
	defer ts.Close()

	claims := map[string]any{
		"iss": "https://accounts.google.com",
		"sub": "sa-1",
		"aud": "my-aud",
		"exp": time.Now().Add(10 * time.Minute).Unix(),
		"iat": time.Now().Add(-10 * time.Second).Unix(),
		"google": map[string]any{
			"compute_engine": map[string]any{
				"instance_id": "inst-fetched-1",
				"project_id":  "p-1",
				"zone":        "europe-west1-b",
			},
		},
	}
	token := generateTestJWT(t, priv, kid, claims)

	verifier := NewDefaultAttestationVerifier()
	verifier.SetGCPCertURL(ts.URL)

	id, err := verifier.VerifyGCP(context.Background(), token, "my-aud")
	if err != nil {
		t.Fatalf("expected VerifyGCP to succeed with remote cert fetch, got: %v", err)
	}
	if id.InstanceID != "inst-fetched-1" {
		t.Errorf("expected inst-fetched-1, got: %s", id.InstanceID)
	}
}

func TestVerifyGCP_ValidationErrors(t *testing.T) {
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}
	kid := "test-kid"

	verifier := NewDefaultAttestationVerifier()
	verifier.SetGCPPublicKey(kid, &priv.PublicKey)

	// Audience mismatch
	claimsBadAud := map[string]any{
		"iss": "https://accounts.google.com",
		"aud": "wrong-audience",
		"exp": time.Now().Add(time.Hour).Unix(),
	}
	tokenBadAud := generateTestJWT(t, priv, kid, claimsBadAud)
	if _, err := verifier.VerifyGCP(context.Background(), tokenBadAud, "expected-aud"); err == nil {
		t.Error("expected error on audience mismatch")
	}

	// Expired token
	claimsExpired := map[string]any{
		"iss": "https://accounts.google.com",
		"aud": "my-aud",
		"exp": time.Now().Add(-10 * time.Minute).Unix(),
	}
	tokenExpired := generateTestJWT(t, priv, kid, claimsExpired)
	if _, err := verifier.VerifyGCP(context.Background(), tokenExpired, "my-aud"); err == nil {
		t.Error("expected error on expired token")
	}

	// Invalid issuer
	claimsBadIss := map[string]any{
		"iss": "https://evil.com",
		"aud": "my-aud",
		"exp": time.Now().Add(time.Hour).Unix(),
	}
	tokenBadIss := generateTestJWT(t, priv, kid, claimsBadIss)
	if _, err := verifier.VerifyGCP(context.Background(), tokenBadIss, "my-aud"); err == nil {
		t.Error("expected error on bad issuer")
	}
}
