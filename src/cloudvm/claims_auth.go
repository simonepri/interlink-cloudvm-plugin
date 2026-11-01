// Caller authentication of the NodeClaim API: every request carries a service account token that a TokenReview
// verifies for the API's audience, and only allow-listed service accounts may call; anything else is refused.
package main

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	authnv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
)

const (
	// claimAuthCacheTTL is how long a verified token is trusted before it is reviewed again.
	claimAuthCacheTTL = 30 * time.Second
	// claimAuthReviewTimeout bounds one TokenReview call.
	claimAuthReviewTimeout = 5 * time.Second
	// claimAuthCacheMax bounds the cached reviews; expired ones are pruned when it is reached.
	claimAuthCacheMax = 1024
)

// claimAuthenticator verifies bearer tokens with TokenReview and admits the allowed users.
type claimAuthenticator struct {
	client   kubernetes.Interface
	audience string
	allowed  map[string]bool
	now      func() time.Time

	mu    sync.Mutex
	cache map[[sha256.Size]byte]cachedReview
}

type cachedReview struct {
	username string
	expires  time.Time
}

// newClaimAuthenticator returns an authenticator that admits the users in allowed, such as
// system:serviceaccount:karpenter:cloudvm-karpenter, presenting tokens for audience.
func newClaimAuthenticator(client kubernetes.Interface, audience string, allowed []string) *claimAuthenticator {
	a := &claimAuthenticator{client: client, audience: audience, allowed: map[string]bool{}, now: time.Now, cache: map[[sha256.Size]byte]cachedReview{}}
	for _, u := range allowed {
		if u = strings.TrimSpace(u); u != "" {
			a.allowed[u] = true
		}
	}
	return a
}

// claimAuthenticatorFromEnv builds the authenticator from NODECLAIM_API_AUDIENCE (default nodeclaim.DefaultTokenAudience)
// and NODECLAIM_API_ALLOWED_USERS with the in-cluster client; outside a cluster it returns nil, which refuses every call.
func claimAuthenticatorFromEnv() *claimAuthenticator {
	cfg, err := kubeConfig(false)
	if err != nil {
		log.Printf("[cloudvm] NodeClaim API refuses every call: no in-cluster config to review tokens: %v", err)
		return nil
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Printf("[cloudvm] NodeClaim API refuses every call: %v", err)
		return nil
	}
	audience := os.Getenv("NODECLAIM_API_AUDIENCE")
	if audience == "" {
		audience = nodeclaim.DefaultTokenAudience
	}
	allowed := parseCommaList(os.Getenv("NODECLAIM_API_ALLOWED_USERS"))
	if len(allowed) == 0 {
		log.Printf("[cloudvm] NODECLAIM_API_ALLOWED_USERS is empty, so the NodeClaim API refuses every call")
	}
	return newClaimAuthenticator(cs, audience, allowed)
}

// SetClaimAuthenticator sets how NodeClaim API callers are verified; nil refuses every call.
func (s *Server) SetClaimAuthenticator(a *claimAuthenticator) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.claimAuth = a
}

// authenticate returns the caller's username, or an API error saying why the request is refused.
func (a *claimAuthenticator) authenticate(ctx context.Context, header string) (string, error) {
	token, ok := strings.CutPrefix(header, "Bearer ")
	if token = strings.TrimSpace(token); !ok || token == "" {
		return "", apiError(http.StatusUnauthorized, nodeclaim.ReasonUnauthorized, "missing bearer token")
	}
	key := sha256.Sum256([]byte(token))
	now := a.now()
	a.mu.Lock()
	hit, cached := a.cache[key]
	a.mu.Unlock()
	if cached && now.Before(hit.expires) {
		return a.authorize(hit.username)
	}

	reviewCtx, cancel := context.WithTimeout(ctx, claimAuthReviewTimeout)
	defer cancel()
	review, err := a.client.AuthenticationV1().TokenReviews().Create(reviewCtx, &authnv1.TokenReview{
		Spec: authnv1.TokenReviewSpec{Token: token, Audiences: []string{a.audience}},
	}, metav1.CreateOptions{})
	if err != nil {
		return "", fmt.Errorf("token review: %w", err)
	}
	if !review.Status.Authenticated || !slices.Contains(review.Status.Audiences, a.audience) {
		return "", apiError(http.StatusUnauthorized, nodeclaim.ReasonUnauthorized, "token is not valid for audience "+a.audience)
	}
	username := review.Status.User.Username
	a.mu.Lock()
	if len(a.cache) >= claimAuthCacheMax {
		for k, v := range a.cache {
			if !now.Before(v.expires) {
				delete(a.cache, k)
			}
		}
	}
	if len(a.cache) < claimAuthCacheMax {
		a.cache[key] = cachedReview{username: username, expires: now.Add(claimAuthCacheTTL)}
	}
	a.mu.Unlock()
	return a.authorize(username)
}

func (a *claimAuthenticator) authorize(username string) (string, error) {
	if !a.allowed[username] {
		return "", apiError(http.StatusForbidden, nodeclaim.ReasonForbidden, username+" may not call the NodeClaim API")
	}
	return username, nil
}

// requireClaimCaller serves next only for a caller the authenticator admits.
func (s *Server) requireClaimCaller(next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		s.mu.RLock()
		auth := s.claimAuth
		s.mu.RUnlock()
		if auth == nil {
			writeClaimError(w, apiError(http.StatusForbidden, nodeclaim.ReasonForbidden, "NodeClaim API authentication is not configured"))
			return
		}
		if _, err := auth.authenticate(r.Context(), r.Header.Get("Authorization")); err != nil {
			writeClaimError(w, err)
			return
		}
		next(w, r)
	}
}
