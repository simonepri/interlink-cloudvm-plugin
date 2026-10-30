// Tests of the serving certificate: the request names only the pod IP, the approver approves it, and the kubelet API
// presents each renewed certificate while still requiring a client certificate from its client CA.

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
	"math/big"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	certificatesv1 "k8s.io/api/certificates/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

const (
	testSigner      = "example.com/serving"
	testAgentNS     = "cloudvm"
	testAgentSA     = "node-agent"
	testAgentPod    = "node-agent-0"
	testAgentPodIP  = "127.0.0.1"
	testCertTTL     = 24 * time.Hour
	testAgentPodUID = "uid-node-agent-0"
)

func agentPod() *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: testAgentPod, Namespace: testAgentNS, UID: testAgentPodUID},
		Spec:       corev1.PodSpec{ServiceAccountName: testAgentSA},
		Status:     corev1.PodStatus{PodIP: testAgentPodIP, PodIPs: []corev1.PodIP{{IP: testAgentPodIP}}},
	}
}

// agentIdentity is the user the API server records on a request made with the agent pod's token.
func agentIdentity() (string, map[string]certificatesv1.ExtraValue) {
	return "system:serviceaccount:" + testAgentNS + ":" + testAgentSA, map[string]certificatesv1.ExtraValue{
		podNameExtra: {testAgentPod}, podUIDExtra: {testAgentPodUID},
	}
}

var csrResource = certificatesv1.SchemeGroupVersion.WithResource("certificatesigningrequests")

// serveCSRs makes the fake clientset behave like the API server for CertificateSigningRequests: it names created
// requests, records the requesting user, and honors metadata.name and spec.signerName field selectors.
func serveCSRs(kube *fake.Clientset) {
	var n atomic.Int64
	user, extra := agentIdentity()
	csrFields := func(csr *certificatesv1.CertificateSigningRequest) fields.Set {
		return fields.Set{"metadata.name": csr.Name, "spec.signerName": csr.Spec.SignerName}
	}
	kube.PrependReactor("create", "certificatesigningrequests", func(a k8stesting.Action) (bool, runtime.Object, error) {
		csr := a.(k8stesting.CreateAction).GetObject().(*certificatesv1.CertificateSigningRequest)
		if csr.Name == "" {
			csr.Name = csr.GenerateName + strconv.FormatInt(n.Add(1), 10)
		}
		csr.UID = types.UID("uid-" + csr.Name)
		csr.Spec.Username, csr.Spec.Extra = user, extra
		return false, nil, nil
	})
	kube.PrependReactor("list", "certificatesigningrequests", func(a k8stesting.Action) (bool, runtime.Object, error) {
		selector := a.(k8stesting.ListAction).GetListRestrictions().Fields
		obj, err := kube.Tracker().List(csrResource, certificatesv1.SchemeGroupVersion.WithKind("CertificateSigningRequest"), "")
		if err != nil {
			return true, nil, err
		}
		list := obj.(*certificatesv1.CertificateSigningRequestList)
		list.Items = slices.DeleteFunc(list.Items, func(csr certificatesv1.CertificateSigningRequest) bool {
			return !selector.Matches(csrFields(&csr))
		})
		return true, list, nil
	})
	kube.PrependWatchReactor("certificatesigningrequests", func(a k8stesting.Action) (bool, watch.Interface, error) {
		selector := a.(k8stesting.WatchAction).GetWatchRestrictions().Fields
		w, err := kube.Tracker().Watch(csrResource, "")
		if err != nil {
			return true, nil, err
		}
		return true, watch.Filter(w, func(e watch.Event) (watch.Event, bool) {
			csr, ok := e.Object.(*certificatesv1.CertificateSigningRequest)
			return e, !ok || selector.Matches(csrFields(csr))
		}), nil
	})
}

// testCA is a certificate authority that issues certificates of a fixed lifetime.
type testCA struct {
	cert *x509.Certificate
	key  *ecdsa.PrivateKey
	pool *x509.CertPool
}

func newTestCA(t *testing.T, name string) *testCA {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: name}, IsCA: true, BasicConstraintsValid: true,
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageCertSign,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AddCert(cert)
	return &testCA{cert: cert, key: key, pool: pool}
}

var testSerial atomic.Int64

func (ca *testCA) issue(t *testing.T, tmpl *x509.Certificate, pub any, lifetime time.Duration) []byte {
	t.Helper()
	tmpl.SerialNumber = big.NewInt(100 + testSerial.Add(1))
	tmpl.NotBefore, tmpl.NotAfter = time.Now(), time.Now().Add(lifetime)
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, pub, ca.key)
	if err != nil {
		t.Error(err)
		return nil
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})
}

// clientCertificate returns a client key pair the CA signed.
func (ca *testCA) clientCertificate(t *testing.T) tls.Certificate {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	certPEM := ca.issue(t, &x509.Certificate{
		Subject: pkix.Name{CommonName: "kube-apiserver-kubelet-client"}, KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}, &key.PublicKey, time.Hour)
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := tls.X509KeyPair(certPEM, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}))
	if err != nil {
		t.Fatal(err)
	}
	return cert
}

// signApproved plays the signer: it issues every approved request of testSigner with the lifetime until ctx ends.
func (ca *testCA) signApproved(ctx context.Context, t *testing.T, kube *fake.Clientset, lifetime time.Duration) {
	t.Helper()
	for ctx.Err() == nil {
		list, err := kube.CertificatesV1().CertificateSigningRequests().List(ctx, metav1.ListOptions{})
		if err != nil {
			return
		}
		for i := range list.Items {
			csr := &list.Items[i]
			approved := slices.ContainsFunc(csr.Status.Conditions, func(c certificatesv1.CertificateSigningRequestCondition) bool {
				return c.Type == certificatesv1.CertificateApproved
			})
			if !approved || len(csr.Status.Certificate) > 0 || csr.Spec.SignerName != testSigner {
				continue
			}
			block, _ := pem.Decode(csr.Spec.Request)
			req, err := x509.ParseCertificateRequest(block.Bytes)
			if err != nil {
				t.Error(err)
				return
			}
			csr.Status.Certificate = ca.issue(t, &x509.Certificate{
				Subject: req.Subject, IPAddresses: req.IPAddresses, KeyUsage: x509.KeyUsageDigitalSignature,
				ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
			}, req.PublicKey, lifetime)
			if _, err := kube.CertificatesV1().CertificateSigningRequests().UpdateStatus(ctx, csr, metav1.UpdateOptions{}); err != nil {
				t.Error(err)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func listCSRs(t *testing.T, kube *fake.Clientset) []certificatesv1.CertificateSigningRequest {
	t.Helper()
	list, err := kube.CertificatesV1().CertificateSigningRequests().List(context.Background(), metav1.ListOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return list.Items
}

func TestServingCertRequestNamesOnlyThePodIP(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kube := fake.NewClientset()
	serveCSRs(kube)
	current, err := StartServingCertificate(ctx, ServingCertConfig{
		SignerName: testSigner, PodName: testAgentPod, PodIP: "10.0.0.7", Duration: testCertTTL, Kube: kube,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "certificate request", func() bool { return len(listCSRs(t, kube)) > 0 })

	csr := listCSRs(t, kube)[0]
	if csr.Spec.SignerName != testSigner {
		t.Errorf("signer = %q, want %q", csr.Spec.SignerName, testSigner)
	}
	if !slices.Equal(csr.Spec.Usages, servingCertUsages) {
		t.Errorf("usages = %v, want %v", csr.Spec.Usages, servingCertUsages)
	}
	if csr.Spec.ExpirationSeconds == nil || *csr.Spec.ExpirationSeconds != int32(testCertTTL.Seconds()) {
		t.Errorf("expirationSeconds = %v, want %d", csr.Spec.ExpirationSeconds, int32(testCertTTL.Seconds()))
	}
	block, _ := pem.Decode(csr.Spec.Request)
	req, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	if req.Subject.String() != "CN="+testAgentPod {
		t.Errorf("subject = %q, want CN=%s", req.Subject, testAgentPod)
	}
	if len(req.IPAddresses) != 1 || !req.IPAddresses[0].Equal(net.ParseIP("10.0.0.7")) || len(req.DNSNames) > 0 {
		t.Errorf("SANs = %v %v, want only 10.0.0.7", req.IPAddresses, req.DNSNames)
	}
	// Until the request is signed, the self-signed certificate for the pod IP serves.
	if cert := current(); cert == nil || cert.Leaf.Subject.CommonName != "node-agent" {
		t.Errorf("certificate before signing = %v, want the self-signed one", cert)
	}
}

func TestServingCertKubeletSigningUsesNodeSubject(t *testing.T) {
	got := servingCertSubject(certificatesv1.KubeletServingSignerName, testAgentPod)
	if want := "CN=system:node:" + testAgentPod + ",O=system:nodes"; got.String() != want {
		t.Errorf("subject = %q, want %q", got, want)
	}
}

func TestServingCertRotatesAndServerPresentsRenewal(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	kube := fake.NewClientset(agentPod())
	serveCSRs(kube)
	approver := NewServingCertApprover(ServingCertApproverConfig{
		Kube: kube, SignerName: testSigner, AgentNamespace: testAgentNS, AgentServiceAccount: testAgentSA,
		MaxDuration: testCertTTL,
	})
	go func() { _ = approver.Run(ctx) }()
	servingCA := newTestCA(t, "cluster CA")
	go servingCA.signApproved(ctx, t, kube, 3*time.Second)

	clientCA := newTestCA(t, "kubelet client CA")
	caFile := filepath.Join(t.TempDir(), "client-ca.crt")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: clientCA.cert.Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	current, err := StartServingCertificate(ctx, ServingCertConfig{
		SignerName: testSigner, PodName: testAgentPod, PodIP: testAgentPodIP, Duration: testCertTTL, Kube: kube,
	})
	if err != nil {
		t.Fatal(err)
	}
	server, err := NewKubeletAPIServer(KubeletAPIConfig{ServingCertificate: current, ClientCAFile: caFile}, nil)
	if err != nil {
		t.Fatal(err)
	}
	ln, err := net.Listen("tcp", testAgentPodIP+":0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = server.ServeTLS(ln, "", "") }()
	defer server.Close()

	// servedSerial is the serial of the certificate the server presents, once it verifies against the cluster CA.
	clientCert := clientCA.clientCertificate(t)
	servedSerial := func() string {
		conn, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{
			RootCAs: servingCA.pool, Certificates: []tls.Certificate{clientCert}, MinVersion: tls.VersionTLS12,
		})
		if err != nil {
			return ""
		}
		defer conn.Close()
		return conn.ConnectionState().PeerCertificates[0].SerialNumber.String()
	}
	var first string
	waitFor(t, "a signed serving certificate", func() bool { first = servedSerial(); return first != "" })
	waitFor(t, "a renewed serving certificate", func() bool { s := servedSerial(); return s != "" && s != first })

	csrs := listCSRs(t, kube)
	if len(csrs) < 2 {
		t.Fatalf("got %d certificate requests, want one per certificate", len(csrs))
	}
	for i := range csrs {
		if c := csrs[i].Status.Conditions; len(c) != 1 || c[0].Type != certificatesv1.CertificateApproved {
			t.Errorf("request %s conditions = %v, want approved", csrs[i].Name, c)
		}
	}

	// A caller without a client certificate from the client CA is still refused.
	insecure := &http.Client{Transport: &http.Transport{TLSClientConfig: &tls.Config{
		RootCAs: servingCA.pool, MinVersion: tls.VersionTLS12,
	}}}
	if resp, err := insecure.Get("https://" + ln.Addr().String() + "/pods"); err == nil {
		_ = resp.Body.Close()
		t.Fatalf("request without a client certificate got %s, want a TLS error", resp.Status)
	}
}
