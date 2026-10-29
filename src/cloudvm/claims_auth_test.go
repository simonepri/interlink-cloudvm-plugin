// Tests that the NodeClaim API admits only allow-listed callers whose tokens a TokenReview accepts for its audience.
package main

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	authnv1 "k8s.io/api/authentication/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
)

const testCaller = "system:serviceaccount:karpenter:cloudvm-karpenter"

// fakeTokenReviewer authenticates "good" as testCaller, "other" as another account and "wrong-aud" for another
// audience; it rejects every other token and counts the reviews.
func fakeTokenReviewer() (*fake.Clientset, *atomic.Int32) {
	cs := fake.NewClientset()
	var reviews atomic.Int32
	cs.PrependReactor("create", "tokenreviews", func(action k8stesting.Action) (bool, runtime.Object, error) {
		reviews.Add(1)
		review := action.(k8stesting.CreateAction).GetObject().(*authnv1.TokenReview).DeepCopy()
		switch review.Spec.Token {
		case "good":
			review.Status = authnv1.TokenReviewStatus{Authenticated: true, Audiences: review.Spec.Audiences, User: authnv1.UserInfo{Username: testCaller}}
		case "other":
			review.Status = authnv1.TokenReviewStatus{Authenticated: true, Audiences: review.Spec.Audiences, User: authnv1.UserInfo{Username: "system:serviceaccount:team-a:default"}}
		case "wrong-aud":
			review.Status = authnv1.TokenReviewStatus{Authenticated: true, Audiences: []string{"https://kubernetes.default.svc"}, User: authnv1.UserInfo{Username: testCaller}}
		}
		return true, review, nil
	})
	return cs, &reviews
}

// bearer returns a transport that sends token as the bearer token.
func bearer(token string) http.RoundTripper {
	return roundTripFunc(func(r *http.Request) (*http.Response, error) {
		r = r.Clone(r.Context())
		r.Header.Set("Authorization", "Bearer "+token)
		return http.DefaultTransport.RoundTrip(r)
	})
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestClaimAPIAuthentication(t *testing.T) {
	t.Setenv("CLAIM_STORE_PATH", t.TempDir()+"/claims.json")
	s := NewServer(NewMockProvider(), "claims-auth")
	srv := httptest.NewServer(s.Routes())
	defer srv.Close()
	ctx := context.Background()
	client := func(token string) *nodeclaim.Client {
		if token == "" {
			return nodeclaim.NewClient(srv.URL, nil)
		}
		return nodeclaim.NewClient(srv.URL, &http.Client{Transport: bearer(token)})
	}

	if _, err := client("good").List(ctx); !nodeclaim.IsUnauthorized(err) {
		t.Fatalf("List without an authenticator: %v, want refused", err)
	}
	reviewer, reviews := fakeTokenReviewer()
	s.SetClaimAuthenticator(newClaimAuthenticator(reviewer, nodeclaim.DefaultTokenAudience, []string{testCaller}))

	cases := map[string]int{"": http.StatusUnauthorized, "bad": http.StatusUnauthorized, "wrong-aud": http.StatusUnauthorized, "other": http.StatusForbidden}
	for token, want := range cases {
		_, err := client(token).List(ctx)
		var apiErr *nodeclaim.APIError
		if !nodeclaim.IsUnauthorized(err) || !errors.As(err, &apiErr) || apiErr.Status != want {
			t.Errorf("token %q: %v, want HTTP %d", token, err, want)
		}
	}
	before := reviews.Load()
	for range 3 {
		if _, err := client("good").List(ctx); err != nil {
			t.Fatalf("List with an allowed token: %v", err)
		}
	}
	if got := reviews.Load() - before; got != 1 {
		t.Fatalf("reviews for 3 calls with one token = %d, want 1 (cached)", got)
	}
	resp, err := http.Get(srv.URL + "/healthz")
	if err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("/healthz: %v %v", resp, err)
	}
	_ = resp.Body.Close()

	empty := newClaimAuthenticator(reviewer, nodeclaim.DefaultTokenAudience, nil)
	s.SetClaimAuthenticator(empty)
	if _, err := client("good").List(ctx); !nodeclaim.IsUnauthorized(err) {
		t.Fatalf("List with an empty allow-list: %v, want refused", err)
	}
}
