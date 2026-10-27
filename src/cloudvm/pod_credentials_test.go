// Tests role detection, token minting and refresh, and how the server hands pod credentials to the runner.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

var podIdentityEnv = map[string]string{ //nolint:gosec // env names and paths, not credentials
	"AWS_CONTAINER_CREDENTIALS_FULL_URI":     "http://169.254.170.23/v1/credentials",
	"AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE": "/var/run/secrets/pods.eks.amazonaws.com/serviceaccount/eks-pod-identity-token",
	"AWS_REGION":                             "us-west-2",
}

var irsaEnv = map[string]string{ //nolint:gosec // env names and paths, not credentials
	"AWS_ROLE_ARN":                "arn:aws:iam::111122223333:role/team-role",
	"AWS_WEB_IDENTITY_TOKEN_FILE": "/var/run/secrets/eks.amazonaws.com/serviceaccount/token",
}

type tokenRequestRecord struct {
	namespace      string
	serviceAccount string
	spec           authenticationv1.TokenRequestSpec
}

// fakeTokenAPI answers TokenRequests with numbered tokens that expire after the requested lifetime.
type fakeTokenAPI struct {
	mu       sync.Mutex
	now      func() time.Time
	requests []tokenRequestRecord
	fail     bool
}

func (f *fakeTokenAPI) clientset() *fake.Clientset {
	cs := fake.NewClientset()
	cs.PrependReactor("create", "serviceaccounts", func(a k8stesting.Action) (bool, runtime.Object, error) {
		ca, ok := a.(k8stesting.CreateActionImpl)
		if !ok || ca.GetSubresource() != "token" {
			return false, nil, nil
		}
		f.mu.Lock()
		defer f.mu.Unlock()
		if f.fail {
			return true, nil, errors.New("forbidden")
		}
		tr := ca.GetObject().(*authenticationv1.TokenRequest).DeepCopy()
		f.requests = append(f.requests, tokenRequestRecord{namespace: ca.GetNamespace(), serviceAccount: ca.Name, spec: tr.Spec})
		tr.Status = authenticationv1.TokenRequestStatus{
			Token:               fmt.Sprintf("jwt-%d", len(f.requests)),
			ExpirationTimestamp: metav1.NewTime(f.now().Add(time.Duration(*tr.Spec.ExpirationSeconds) * time.Second)),
		}
		return true, tr, nil
	})
	return cs
}

func (f *fakeTokenAPI) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.requests)
}

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

type exchangeRecord struct {
	role        PodAWSRole
	token       string
	sessionName string
}

// fakeExchanger returns numbered credentials that expire lifetime after the exchange.
type fakeExchanger struct {
	mu       sync.Mutex
	now      func() time.Time
	lifetime time.Duration
	calls    []exchangeRecord
	fail     bool
}

func (f *fakeExchanger) Exchange(_ context.Context, role PodAWSRole, token, sessionName string) (PodAWSCredentials, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fail {
		return PodAWSCredentials{}, errors.New("access denied")
	}
	f.calls = append(f.calls, exchangeRecord{role: role, token: token, sessionName: sessionName})
	n := len(f.calls)
	return PodAWSCredentials{
		AccessKeyID:     fmt.Sprintf("ASIA%d", n),
		SecretAccessKey: fmt.Sprintf("secret-%d", n),
		SessionToken:    fmt.Sprintf("session-%d", n),
		Expiration:      f.now().Add(f.lifetime).Unix(),
	}, nil
}

func newTestPodCredentials(t *testing.T) (*PodCredentials, *fakeTokenAPI, *fakeExchanger, *fakeClock) {
	t.Helper()
	clock := &fakeClock{t: time.Unix(1_800_000_000, 0)}
	api := &fakeTokenAPI{now: clock.now}
	ex := &fakeExchanger{now: clock.now, lifetime: 6 * time.Hour}
	pc := NewPodCredentials(api.clientset(), ex)
	pc.now = clock.now
	return pc, api, ex, clock
}

func TestDetectPodAWSRole(t *testing.T) {
	both := map[string]string{}
	for k, v := range podIdentityEnv {
		both[k] = v
	}
	for k, v := range irsaEnv {
		both[k] = v
	}
	cases := []struct {
		name      string
		env       map[string]string
		mechanism string
		roleARN   string
	}{
		{name: "pod identity", env: podIdentityEnv, mechanism: AWSMechanismPodIdentity},
		{name: "pod identity over IPv6", env: map[string]string{"AWS_CONTAINER_CREDENTIALS_FULL_URI": "http://[fd00:ec2::23]/v1/credentials"}, mechanism: AWSMechanismPodIdentity}, //nolint:gosec // an endpoint URL, not a credential
		{name: "irsa", env: irsaEnv, mechanism: AWSMechanismIRSA, roleARN: irsaEnv["AWS_ROLE_ARN"]},
		{name: "irsa wins like the SDK chain", env: both, mechanism: AWSMechanismIRSA, roleARN: irsaEnv["AWS_ROLE_ARN"]},
		{name: "no role", env: map[string]string{"AWS_REGION": "us-west-2"}},
		{name: "role ARN without token file", env: map[string]string{"AWS_ROLE_ARN": "arn:aws:iam::111122223333:role/x"}},
		{name: "own credential endpoint", env: map[string]string{"AWS_CONTAINER_CREDENTIALS_FULL_URI": "http://127.0.0.1:8080/creds"}}, //nolint:gosec // an endpoint URL, not a credential
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			role := DetectPodAWSRole(tc.env)
			if tc.mechanism == "" {
				if role != nil {
					t.Fatalf("expected no role, got %+v", role)
				}
				return
			}
			if role == nil || role.Mechanism != tc.mechanism || role.RoleARN != tc.roleARN {
				t.Fatalf("expected %s %q, got %+v", tc.mechanism, tc.roleARN, role)
			}
		})
	}
}

func TestPodCredentials_TrackExchangesPodBoundToken(t *testing.T) {
	pc, api, ex, clock := newTestPodCredentials(t)
	pod := PodRef{Namespace: "team-a", Name: "ray-worker", UID: "uid-1", ServiceAccount: "ray"}

	creds := pc.Track(context.Background(), pod, podIdentityEnv, true)
	if creds == nil || creds.AccessKeyID != "ASIA1" || creds.SecretAccessKey != "secret-1" || creds.SessionToken != "session-1" {
		t.Fatalf("unexpected credentials %v", creds)
	}
	if want := clock.now().Add(6 * time.Hour).Unix(); creds.Expiration != want {
		t.Fatalf("expected expiry %d, got %d", want, creds.Expiration)
	}
	if api.count() != 1 {
		t.Fatalf("expected one TokenRequest, got %d", api.count())
	}
	req := api.requests[0]
	if req.namespace != "team-a" || req.serviceAccount != "ray" {
		t.Fatalf("token minted for %s/%s", req.namespace, req.serviceAccount)
	}
	if len(req.spec.Audiences) != 1 || req.spec.Audiences[0] != "pods.eks.amazonaws.com" {
		t.Fatalf("unexpected audiences %v", req.spec.Audiences)
	}
	ref := req.spec.BoundObjectRef
	if ref == nil || ref.Kind != "Pod" || ref.Name != "ray-worker" || string(ref.UID) != "uid-1" {
		t.Fatalf("token not bound to the pod: %+v", ref)
	}
	if len(ex.calls) != 1 || ex.calls[0].token != "jwt-1" || ex.calls[0].role.Mechanism != AWSMechanismPodIdentity || ex.calls[0].sessionName != "cloudvm-uid-1" {
		t.Fatalf("unexpected exchanges %+v", ex.calls)
	}

	// Tracking the same pod again reuses its credentials.
	if again := pc.Track(context.Background(), pod, podIdentityEnv, true); again == nil || again.AccessKeyID != "ASIA1" || api.count() != 1 {
		t.Fatalf("expected the tracked credentials to be reused, got %v after %d requests", again, api.count())
	}
}

func TestPodCredentials_TrackIRSAUsesSTSAudienceAndDefaultServiceAccount(t *testing.T) {
	pc, api, ex, _ := newTestPodCredentials(t)
	if creds := pc.Track(context.Background(), PodRef{Namespace: "team-b", Name: "job", UID: "uid-2"}, irsaEnv, true); creds == nil || creds.AccessKeyID == "" {
		t.Fatalf("unexpected credentials %v", creds)
	}
	req := api.requests[0]
	if req.serviceAccount != "default" || req.spec.Audiences[0] != "sts.amazonaws.com" {
		t.Fatalf("unexpected TokenRequest %+v", req)
	}
	if role := ex.calls[0].role; role.Mechanism != AWSMechanismIRSA || role.RoleARN != irsaEnv["AWS_ROLE_ARN"] {
		t.Fatalf("unexpected exchange role %+v", role)
	}
}

func TestPodCredentials_TrackSkipsPodsWithoutRole(t *testing.T) {
	pc, api, _, _ := newTestPodCredentials(t)
	if creds := pc.Track(context.Background(), PodRef{Namespace: "ns", Name: "p", UID: "uid-3"}, map[string]string{"FOO": "bar"}, true); creds != nil {
		t.Fatalf("expected no credentials, got %v", creds)
	}
	if api.count() != 0 || pc.Tracked("uid-3") {
		t.Fatalf("expected no tokens and no tracked pod, got %d tokens", api.count())
	}
}

func TestPodAWSCredentials_StringRedactsSecrets(t *testing.T) {
	c := PodAWSCredentials{AccessKeyID: "ASIA1", SecretAccessKey: "very-secret", SessionToken: "very-session", Expiration: 1}
	for _, out := range []string{fmt.Sprint(c), fmt.Sprintf("%+v", c), fmt.Sprintf("%#v", c), fmt.Sprintf("%v", &c)} {
		if strings.Contains(out, "very-secret") || strings.Contains(out, "very-session") {
			t.Fatalf("credentials leaked into %q", out)
		}
	}
}

func TestPodCredentials_RefreshOnceRefreshesAndRedelivers(t *testing.T) {
	pc, api, ex, clock := newTestPodCredentials(t)
	ctx := context.Background()

	// The first exchange fails, so the pod is tracked without credentials until the refresher obtains them.
	ex.fail = true
	creds := pc.Track(ctx, PodRef{Namespace: "ns", Name: "p", UID: "uid-5"}, podIdentityEnv, true)
	if creds == nil || creds.AccessKeyID != "" {
		t.Fatalf("expected tracked credentials without keys, got %v", creds)
	}
	ex.fail = false

	var delivered []PodAWSCredentials
	deliverErr := errors.New("runner unreachable")
	deliver := func(_ context.Context, uid string, c *PodAWSCredentials) error {
		if uid != "uid-5" {
			t.Fatalf("delivered credentials of %s", uid)
		}
		delivered = append(delivered, *c)
		return deliverErr
	}

	pc.RefreshOnce(ctx, deliver)
	if len(delivered) != 1 || delivered[0].AccessKeyID != "ASIA1" {
		t.Fatalf("expected ASIA1 obtained and delivered, got %v", delivered)
	}

	// A failed delivery is retried with the same credentials.
	deliverErr = nil
	pc.RefreshOnce(ctx, deliver)
	if len(ex.calls) != 1 || len(delivered) != 2 || delivered[1].AccessKeyID != "ASIA1" {
		t.Fatalf("expected ASIA1 redelivered, got %d exchanges and %v", len(ex.calls), delivered)
	}

	// Delivered credentials that are still fresh are left alone.
	clock.advance(2 * time.Hour)
	pc.RefreshOnce(ctx, deliver)
	if len(ex.calls) != 1 || len(delivered) != 2 {
		t.Fatalf("expected no refresh, got %d exchanges and %d deliveries", len(ex.calls), len(delivered))
	}

	// Past half their lifetime the credentials are replaced with ones from a fresh token.
	clock.advance(90 * time.Minute)
	pc.RefreshOnce(ctx, deliver)
	if len(delivered) != 3 || delivered[2].AccessKeyID != "ASIA2" || ex.calls[1].token != fmt.Sprintf("jwt-%d", api.count()) {
		t.Fatalf("expected ASIA2 from a new token, got %v", delivered)
	}

	// Short-lived credentials are renewed before fewer than podCredentialsMinRemaining remain.
	ex.lifetime = 30 * time.Minute
	clock.advance(4 * time.Hour)
	pc.RefreshOnce(ctx, deliver)
	clock.advance(11 * time.Minute)
	pc.RefreshOnce(ctx, deliver)
	if len(delivered) != 5 || delivered[4].AccessKeyID != "ASIA4" {
		t.Fatalf("expected short-lived credentials renewed, got %v", delivered)
	}

	// Once the pod is gone the refresher forgets it.
	clock.advance(time.Hour)
	deliverErr = errPodCredentialsGone
	pc.RefreshOnce(ctx, deliver)
	if pc.Tracked("uid-5") {
		t.Fatal("expected the finished pod to be forgotten")
	}
}

func credentialPodJSON(uid string, env map[string]string) []byte {
	envList := make([]EnvVar, 0, len(env))
	for k, v := range env {
		envList = append(envList, EnvVar{Name: k, Value: v})
	}
	pod := Pod{
		Metadata: PodMeta{
			Name:      "pod-" + uid,
			Namespace: "team-a",
			UID:       uid,
			Annotations: map[string]string{
				"cloudvm.vk.io/region":        "us-west-2",
				"cloudvm.vk.io/instance-type": "c6i.xlarge",
				"cloudvm.vk.io/capacity-type": "spot",
			},
		},
		Spec: PodSpecWrapper{
			ServiceAccountName: "ray",
			Containers:         []ContainerSpec{{Name: "worker", Image: "busybox", Env: envList}},
		},
	}
	body, _ := json.Marshal(pod)
	return body
}

func TestServer_CreateHandsRoleCredentialsToRunner(t *testing.T) {
	srv := NewServer(NewMockProvider(), "test-cluster")
	runner := NewMockContainerRunner()
	srv.SetContainerRunner(runner)
	pc, api, _, _ := newTestPodCredentials(t)
	srv.SetPodCredentials(pc)
	handler := srv.Routes()

	post := func(path string, body []byte) {
		t.Helper()
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body)))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s returned %d: %s", path, rec.Code, rec.Body.String())
		}
	}

	post("/create", credentialPodJSON("uid-role", podIdentityEnv))
	req := runner.GetStartRequest("uid-role")
	if req == nil || req.AWSCredentials == nil || req.AWSCredentials.AccessKeyID != "ASIA1" {
		t.Fatalf("expected role credentials in the start request, got %+v", req)
	}
	if api.requests[0].serviceAccount != "ray" || api.requests[0].namespace != "team-a" {
		t.Fatalf("token minted for %+v", api.requests[0])
	}
	for _, k := range webhookCredentialEnv {
		if _, ok := req.Env[k]; ok {
			t.Fatalf("expected %s stripped from the container env, got %v", k, req.Env)
		}
	}
	if req.Env["AWS_REGION"] != "us-west-2" {
		t.Fatalf("expected the other webhook env kept, got %v", req.Env)
	}

	post("/create", credentialPodJSON("uid-plain", map[string]string{"FOO": "bar"}))
	plain := runner.GetStartRequest("uid-plain")
	if plain == nil || plain.AWSCredentials != nil || plain.Env["FOO"] != "bar" {
		t.Fatalf("expected no credentials for a pod without a role, got %+v", plain)
	}
	if pc.Tracked("uid-plain") || api.count() != 1 {
		t.Fatalf("expected no token for the pod without a role, got %d tokens", api.count())
	}

	// The refresher delivers through the runner of the pod's node.
	pc.RefreshOnce(context.Background(), srv.deliverPodCredentials)
	if pushed := runner.PushedCredentials("uid-role"); len(pushed) != 1 || pushed[0].AccessKeyID != "ASIA1" {
		t.Fatalf("expected ASIA1 pushed to the runner, got %v", pushed)
	}

	// After a restart forgets the pod, the next status poll resumes refreshing it.
	pc.Forget("uid-role")
	post("/status", []byte("["+string(credentialPodJSON("uid-role", podIdentityEnv))+"]"))
	if !pc.Tracked("uid-role") {
		t.Fatal("expected the status poll to resume tracking the pod")
	}

	post("/delete", credentialPodJSON("uid-role", podIdentityEnv))
	if pc.Tracked("uid-role") {
		t.Fatal("expected deletion to stop tracking the pod")
	}
}
