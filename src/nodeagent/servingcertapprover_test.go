// Tests of the serving certificate approver: it approves only the agent pod's own serving requests, denies the agent's
// requests that ask for more, and leaves every other user's and signer's requests alone.

package main

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"net"
	"testing"
	"time"

	certificatesv1 "k8s.io/api/certificates/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// agentCSR returns the request the agent pod makes, after edit changes it.
func agentCSR(t *testing.T, edit func(req *x509.CertificateRequest, csr *certificatesv1.CertificateSigningRequest)) *certificatesv1.CertificateSigningRequest {
	t.Helper()
	user, extra := agentIdentity()
	expiration := int32(testCertTTL.Seconds())
	csr := &certificatesv1.CertificateSigningRequest{
		ObjectMeta: metav1.ObjectMeta{Name: "csr-1"},
		Spec: certificatesv1.CertificateSigningRequestSpec{
			SignerName: testSigner, Usages: servingCertUsages, ExpirationSeconds: &expiration, Username: user, Extra: extra,
		},
	}
	req := &x509.CertificateRequest{
		Subject: servingCertSubject(testSigner, testAgentPod), IPAddresses: []net.IP{net.ParseIP(testAgentPodIP)},
	}
	if edit != nil {
		edit(req, csr)
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	der, err := x509.CreateCertificateRequest(rand.Reader, req, key)
	if err != nil {
		t.Fatal(err)
	}
	if csr.Spec.Request == nil {
		csr.Spec.Request = pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: der})
	}
	return csr
}

// review runs the approver once over the request and returns its resulting condition type, or "" for none.
func review(t *testing.T, csr *certificatesv1.CertificateSigningRequest) certificatesv1.RequestConditionType {
	t.Helper()
	ctx := context.Background()
	kube := fake.NewClientset(agentPod(), csr)
	approver := NewServingCertApprover(ServingCertApproverConfig{
		Kube: kube, SignerName: testSigner, AgentNamespace: testAgentNS, AgentServiceAccount: testAgentSA,
		MaxDuration: testCertTTL,
	})
	if err := approver.sync(ctx, csr); err != nil {
		t.Fatal(err)
	}
	got, err := kube.CertificatesV1().CertificateSigningRequests().Get(ctx, csr.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got.Status.Conditions) == 0 {
		return ""
	}
	return got.Status.Conditions[len(got.Status.Conditions)-1].Type
}

func TestApproverApprovesTheAgentPodsOwnRequest(t *testing.T) {
	if got := review(t, agentCSR(t, nil)); got != certificatesv1.CertificateApproved {
		t.Fatalf("condition = %q, want Approved", got)
	}
}

func TestApproverApprovesKubeletServingSubject(t *testing.T) {
	csr := agentCSR(t, func(req *x509.CertificateRequest, csr *certificatesv1.CertificateSigningRequest) {
		req.Subject = servingCertSubject(certificatesv1.KubeletServingSignerName, testAgentPod)
		csr.Spec.SignerName = certificatesv1.KubeletServingSignerName
	})
	kube := fake.NewClientset(agentPod(), csr)
	approver := NewServingCertApprover(ServingCertApproverConfig{
		Kube: kube, SignerName: certificatesv1.KubeletServingSignerName, AgentNamespace: testAgentNS,
		AgentServiceAccount: testAgentSA, MaxDuration: testCertTTL,
	})
	if err := approver.sync(context.Background(), csr); err != nil {
		t.Fatal(err)
	}
	got, err := kube.CertificatesV1().CertificateSigningRequests().Get(context.Background(), csr.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if c := got.Status.Conditions; len(c) != 1 || c[0].Type != certificatesv1.CertificateApproved {
		t.Fatalf("conditions = %v, want Approved", c)
	}
}

func TestApproverLeavesOtherRequestsAlone(t *testing.T) {
	for name, edit := range map[string]func(*x509.CertificateRequest, *certificatesv1.CertificateSigningRequest){
		"other user": func(_ *x509.CertificateRequest, csr *certificatesv1.CertificateSigningRequest) {
			csr.Spec.Username = "system:serviceaccount:" + testAgentNS + ":other"
		},
		"other signer": func(_ *x509.CertificateRequest, csr *certificatesv1.CertificateSigningRequest) {
			csr.Spec.SignerName = certificatesv1.KubeletServingSignerName
		},
		"already decided": func(_ *x509.CertificateRequest, csr *certificatesv1.CertificateSigningRequest) {
			csr.Status.Conditions = []certificatesv1.CertificateSigningRequestCondition{{Type: certificatesv1.CertificateFailed}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			csr := agentCSR(t, edit)
			want := certificatesv1.RequestConditionType("")
			if len(csr.Status.Conditions) > 0 {
				want = csr.Status.Conditions[0].Type
			}
			if got := review(t, csr); got != want {
				t.Fatalf("condition = %q, want %q", got, want)
			}
		})
	}
}

func TestApproverDeniesRequestsBeyondThePodsOwnCertificate(t *testing.T) {
	for name, edit := range map[string]func(*x509.CertificateRequest, *certificatesv1.CertificateSigningRequest){
		"another IP": func(req *x509.CertificateRequest, _ *certificatesv1.CertificateSigningRequest) {
			req.IPAddresses = append(req.IPAddresses, net.ParseIP("192.0.2.1"))
		},
		"no IP": func(req *x509.CertificateRequest, _ *certificatesv1.CertificateSigningRequest) {
			req.IPAddresses = nil
		},
		"DNS name": func(req *x509.CertificateRequest, _ *certificatesv1.CertificateSigningRequest) {
			req.DNSNames = []string{"kubernetes.default.svc"}
		},
		"other subject": func(req *x509.CertificateRequest, _ *certificatesv1.CertificateSigningRequest) {
			req.Subject = pkix.Name{CommonName: "node-agent-1"}
		},
		"client auth usage": func(_ *x509.CertificateRequest, csr *certificatesv1.CertificateSigningRequest) {
			csr.Spec.Usages = append(csr.Spec.Usages, certificatesv1.UsageClientAuth)
		},
		"no server auth usage": func(_ *x509.CertificateRequest, csr *certificatesv1.CertificateSigningRequest) {
			csr.Spec.Usages = []certificatesv1.KeyUsage{certificatesv1.UsageDigitalSignature}
		},
		"no expiration": func(_ *x509.CertificateRequest, csr *certificatesv1.CertificateSigningRequest) {
			csr.Spec.ExpirationSeconds = nil
		},
		"long expiration": func(_ *x509.CertificateRequest, csr *certificatesv1.CertificateSigningRequest) {
			long := int32((testCertTTL + time.Second).Seconds())
			csr.Spec.ExpirationSeconds = &long
		},
		"not pod-bound token": func(_ *x509.CertificateRequest, csr *certificatesv1.CertificateSigningRequest) {
			csr.Spec.Extra = nil
		},
		"other pod UID": func(_ *x509.CertificateRequest, csr *certificatesv1.CertificateSigningRequest) {
			csr.Spec.Extra = map[string]certificatesv1.ExtraValue{podNameExtra: {testAgentPod}, podUIDExtra: {"stale"}}
		},
		"missing pod": func(_ *x509.CertificateRequest, csr *certificatesv1.CertificateSigningRequest) {
			csr.Spec.Extra = map[string]certificatesv1.ExtraValue{podNameExtra: {"gone"}, podUIDExtra: {"uid-gone"}}
		},
		"malformed request": func(_ *x509.CertificateRequest, csr *certificatesv1.CertificateSigningRequest) {
			csr.Spec.Request = []byte("not a request")
		},
	} {
		t.Run(name, func(t *testing.T) {
			if got := review(t, agentCSR(t, edit)); got != certificatesv1.CertificateDenied {
				t.Fatalf("condition = %q, want Denied", got)
			}
		})
	}
}
