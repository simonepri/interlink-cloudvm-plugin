// Tests for AWSIdentityVerifier using a real EC2 RSA-2048 instance identity document from us-west-2.
package main

import (
	"context"
	"os"
	"strings"
	"testing"
)

// The sample document in testdata was produced by a real instance:
// instanceId i-05bef8a081f307783, accountId 975426262029, region us-west-2.
const (
	sampleInstance = "i-05bef8a081f307783"
	sampleAccount  = "975426262029"
	sampleRegion   = "us-west-2"
)

func sampleDoc(t *testing.T) []byte {
	t.Helper()
	b, err := os.ReadFile("testdata/aws_rsa2048_us_west_2.b64")
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func lookupFor(id, region string) LaunchRecordLookup {
	return func(_ context.Context, instance string) (LaunchRecord, bool) {
		if instance != id {
			return LaunchRecord{}, false
		}
		return LaunchRecord{InstanceID: id, Region: region}, true
	}
}

func newTestVerifier(t *testing.T, accounts []string, lookup LaunchRecordLookup) *AWSIdentityVerifier {
	t.Helper()
	v, err := NewAWSIdentityVerifier(accounts, lookup)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func TestAWSIdentityVerifier_ValidDocument(t *testing.T) {
	v := newTestVerifier(t, []string{"1", sampleAccount}, lookupFor(sampleInstance, sampleRegion))
	id, err := v.VerifyAWS(context.Background(), sampleDoc(t))
	if err != nil {
		t.Fatal(err)
	}
	if id.InstanceID != sampleInstance || id.AccountID != sampleAccount || id.Region != sampleRegion {
		t.Fatalf("unexpected identity %+v", id)
	}
}

func TestAWSIdentityVerifier_Rejections(t *testing.T) {
	good := []string{sampleAccount}
	tests := []struct {
		name     string
		accounts []string
		lookup   LaunchRecordLookup
		doc      func([]byte) []byte
		want     string
	}{
		{"account not allowed", []string{"111111111111"}, lookupFor(sampleInstance, sampleRegion), nil, "not allowed"},
		{"empty allowlist", nil, lookupFor(sampleInstance, sampleRegion), nil, "not allowed"},
		{"unknown instance", good, lookupFor("i-other", sampleRegion), nil, "no launch record"},
		{"region mismatch", good, lookupFor(sampleInstance, "us-east-1"), nil, "does not match launch record"},
		{"tampered document", good, lookupFor(sampleInstance, sampleRegion), func(b []byte) []byte {
			// Flip a character inside the signed JSON payload.
			c := []byte(strings.TrimSpace(string(b)))
			if c[200] == 'A' {
				c[200] = 'B'
			} else {
				c[200] = 'A'
			}
			return c
		}, ""},
		{"garbage", good, lookupFor(sampleInstance, sampleRegion), func([]byte) []byte { return []byte("not a document") }, ""},
		{"empty", good, lookupFor(sampleInstance, sampleRegion), func([]byte) []byte { return nil }, "empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			v := newTestVerifier(t, tt.accounts, tt.lookup)
			doc := sampleDoc(t)
			if tt.doc != nil {
				doc = tt.doc(doc)
			}
			_, err := v.VerifyAWS(context.Background(), doc)
			if err == nil {
				t.Fatal("expected rejection")
			}
			if tt.want != "" && !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("error %q does not contain %q", err, tt.want)
			}
		})
	}
}

func TestAWSIdentityVerifier_WrongRegionCertificateRejectsSignature(t *testing.T) {
	v := newTestVerifier(t, []string{sampleAccount}, lookupFor(sampleInstance, sampleRegion))
	// Swap in another region's certificate for us-west-2: the real signature must no longer verify.
	v.certs[sampleRegion] = v.certs["us-east-1"]
	_, err := v.VerifyAWS(context.Background(), sampleDoc(t))
	if err == nil || !strings.Contains(err.Error(), "signature invalid") {
		t.Fatalf("expected signature failure, got %v", err)
	}
}

func TestAWSIdentityVerifier_GCPUnsupported(t *testing.T) {
	v := newTestVerifier(t, nil, lookupFor("", ""))
	if _, err := v.VerifyGCP(context.Background(), "token", ""); err == nil {
		t.Fatal("expected error")
	}
}

func TestEmbeddedAWSIdentityCertificatesParse(t *testing.T) {
	certs, err := parseAWSIdentityCertificates(awsIdentityCertificates)
	if err != nil {
		t.Fatal(err)
	}
	if len(certs) < 25 {
		t.Fatalf("expected the regional certificates, got %d", len(certs))
	}
}
