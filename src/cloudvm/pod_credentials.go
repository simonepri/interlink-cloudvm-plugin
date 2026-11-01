// Pod AWS credentials: detects whether a pod's service account has an IAM role, exchanges the pod's projected service
// account token for that role's credentials, and keeps the worker that runs the pod supplied with fresh credentials.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// The mechanisms through which a pod's service account receives an IAM role on EKS.
const (
	AWSMechanismPodIdentity = "eks-pod-identity"
	AWSMechanismIRSA        = "irsa"
)

// The audiences the token exchange of each mechanism accepts.
const (
	podIdentityAudience = "pods.eks.amazonaws.com"
	irsaAudience        = "sts.amazonaws.com"
)

// podTokenLifetime is the lifetime requested for each minted token; the API server's minimum is ten minutes, and the
// token only has to outlive the exchange that follows.
const podTokenLifetime = 10 * time.Minute

// podCredentialsMinRemaining is the shortest validity the worker's credentials may have left before they are renewed;
// SDKs renew theirs 15 minutes before expiry, so the worker always holds credentials that outlast that window.
const podCredentialsMinRemaining = 20 * time.Minute

// errPodCredentialsGone tells the refresher that the pod no longer runs, so it stops refreshing its credentials.
var errPodCredentialsGone = errors.New("pod no longer runs")

// podIdentityAgentHosts are the EKS Pod Identity Agent addresses that the EKS webhook writes into
// AWS_CONTAINER_CREDENTIALS_FULL_URI of a pod whose service account has a Pod Identity association.
var podIdentityAgentHosts = map[string]bool{"169.254.170.23": true, "fd00:ec2::23": true}

// webhookCredentialEnv lists the variables the EKS webhook injects to point SDKs at in-cluster credential sources that
// a worker VM lacks; the plugin drops them and the runner sets its own credential endpoint instead.
var webhookCredentialEnv = []string{
	"AWS_CONTAINER_AUTHORIZATION_TOKEN",
	"AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
	"AWS_CONTAINER_CREDENTIALS_FULL_URI",
	"AWS_ROLE_ARN",
	"AWS_WEB_IDENTITY_TOKEN_FILE",
}

// PodAWSRole is the IAM role a pod's service account receives, as the EKS webhook recorded it in the pod's env.
type PodAWSRole struct {
	Mechanism string
	RoleARN   string
}

// DetectPodAWSRole returns the role the EKS webhook bound to the pod's service account, or nil when it bound none.
// IRSA wins over Pod Identity when both are present, as in the AWS SDK credential chain.
func DetectPodAWSRole(env map[string]string) *PodAWSRole {
	if env["AWS_ROLE_ARN"] != "" && env["AWS_WEB_IDENTITY_TOKEN_FILE"] != "" {
		return &PodAWSRole{Mechanism: AWSMechanismIRSA, RoleARN: env["AWS_ROLE_ARN"]}
	}
	if u, err := url.Parse(env["AWS_CONTAINER_CREDENTIALS_FULL_URI"]); err == nil && podIdentityAgentHosts[u.Hostname()] {
		return &PodAWSRole{Mechanism: AWSMechanismPodIdentity}
	}
	return nil
}

// stripWebhookCredentialEnv returns env without the variables that point at in-cluster credential sources.
func stripWebhookCredentialEnv(env map[string]string) map[string]string {
	out := make(map[string]string, len(env))
	for k, v := range env {
		out[k] = v
	}
	for _, k := range webhookCredentialEnv {
		delete(out, k)
	}
	return out
}

// PodAWSCredentials are temporary credentials of a pod's role. Empty keys tell the runner that the pod has a role
// whose credentials have not arrived yet.
type PodAWSCredentials struct {
	AccessKeyID     string `json:"accessKeyId,omitempty"`
	SecretAccessKey string `json:"secretAccessKey,omitempty"`
	SessionToken    string `json:"sessionToken,omitempty"`
	Expiration      int64  `json:"expiration,omitempty"`
}

// String redacts the secrets so credentials never reach a log.
func (c PodAWSCredentials) String() string {
	return fmt.Sprintf("{accessKeyId:%s secretAccessKey:<redacted> sessionToken:<redacted> expiration:%d}", c.AccessKeyID, c.Expiration)
}

// GoString redacts the secrets for %#v as well.
func (c PodAWSCredentials) GoString() string { return c.String() }

// PodRoleExchanger exchanges a pod's projected service account token for its role's credentials.
type PodRoleExchanger interface {
	Exchange(ctx context.Context, role PodAWSRole, token, sessionName string) (PodAWSCredentials, error)
}

// PodRef names the pod credentials are minted for.
type PodRef struct {
	Namespace      string
	Name           string
	UID            string
	ServiceAccount string
}

type podCredentialEntry struct {
	pod       PodRef
	role      PodAWSRole
	creds     PodAWSCredentials
	issuedAt  time.Time
	delivered bool
}

// PodCredentialDeliverer sends a pod's credentials to the worker that runs it.
type PodCredentialDeliverer func(ctx context.Context, podUID string, creds *PodAWSCredentials) error

// PodCredentials obtains and refreshes the role credentials of the pods whose service account has a role.
type PodCredentials struct {
	client    kubernetes.Interface
	exchanger PodRoleExchanger
	now       func() time.Time

	mu   sync.Mutex
	pods map[string]*podCredentialEntry
}

// NewPodCredentials creates a PodCredentials that mints tokens through client and exchanges them through exchanger.
func NewPodCredentials(client kubernetes.Interface, exchanger PodRoleExchanger) *PodCredentials {
	return &PodCredentials{
		client:    client,
		exchanger: exchanger,
		now:       time.Now,
		pods:      make(map[string]*podCredentialEntry),
	}
}

// NewInClusterPodCredentials creates a PodCredentials from the in-cluster service account.
func NewInClusterPodCredentials(exchanger PodRoleExchanger) (*PodCredentials, error) {
	cfg, err := kubeConfig(false)
	if err != nil {
		return nil, fmt.Errorf("unable to load in-cluster config: %w", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to create kubernetes client: %w", err)
	}
	return NewPodCredentials(cs, exchanger), nil
}

// Track registers the pod for credential refreshes and returns its credentials, or nil when its service account has
// no role. When fetch is true it obtains the first credentials now; on failure the keys stay empty for the refresher.
func (p *PodCredentials) Track(ctx context.Context, pod PodRef, env map[string]string, fetch bool) *PodAWSCredentials {
	role := DetectPodAWSRole(env)
	if role == nil || pod.UID == "" {
		return nil
	}
	if pod.ServiceAccount == "" {
		pod.ServiceAccount = "default"
	}

	p.mu.Lock()
	entry, ok := p.pods[pod.UID]
	if !ok {
		entry = &podCredentialEntry{pod: pod, role: *role}
		p.pods[pod.UID] = entry
	}
	missing := entry.creds.AccessKeyID == ""
	p.mu.Unlock()

	if fetch && missing {
		if err := p.refresh(ctx, pod.UID); err != nil {
			log.Printf("[cloudvm] Cannot get the AWS credentials of pod %s/%s yet: %v", pod.Namespace, pod.Name, err)
		}
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if entry, ok = p.pods[pod.UID]; !ok {
		return nil
	}
	creds := entry.creds
	return &creds
}

// Forget stops refreshing the pod's credentials.
func (p *PodCredentials) Forget(podUID string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.pods, podUID)
}

// Tracked reports whether the pod's credentials are being refreshed.
func (p *PodCredentials) Tracked(podUID string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	_, ok := p.pods[podUID]
	return ok
}

// refresh mints a pod-bound token, exchanges it for the role's credentials, and records them as undelivered.
func (p *PodCredentials) refresh(ctx context.Context, podUID string) error {
	p.mu.Lock()
	entry, ok := p.pods[podUID]
	if !ok {
		p.mu.Unlock()
		return nil
	}
	pod, role := entry.pod, entry.role
	p.mu.Unlock()

	audience := podIdentityAudience
	if role.Mechanism == AWSMechanismIRSA {
		audience = irsaAudience
	}
	lifetime := int64(podTokenLifetime / time.Second)
	tr, err := p.client.CoreV1().ServiceAccounts(pod.Namespace).CreateToken(ctx, pod.ServiceAccount, &authenticationv1.TokenRequest{
		Spec: authenticationv1.TokenRequestSpec{
			Audiences:         []string{audience},
			ExpirationSeconds: &lifetime,
			// Binding the token to the pod invalidates it as soon as the pod is deleted, as the kubelet's projected tokens are.
			BoundObjectRef: &authenticationv1.BoundObjectReference{
				Kind:       "Pod",
				APIVersion: "v1",
				Name:       pod.Name,
				UID:        types.UID(pod.UID),
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create token for service account %s/%s: %w", pod.Namespace, pod.ServiceAccount, err)
	}
	issuedAt := p.now()
	creds, err := p.exchanger.Exchange(ctx, role, tr.Status.Token, "cloudvm-"+pod.UID)
	if err != nil {
		return fmt.Errorf("exchange the token of service account %s/%s: %w", pod.Namespace, pod.ServiceAccount, err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if entry, ok = p.pods[podUID]; ok {
		entry.creds = creds
		entry.issuedAt = issuedAt
		entry.delivered = false
	}
	return nil
}

// needsRefresh reports whether the entry's credentials are missing, past half their lifetime, or closer to expiry
// than podCredentialsMinRemaining.
func (e *podCredentialEntry) needsRefresh(now time.Time) bool {
	if e.creds.AccessKeyID == "" {
		return true
	}
	expiresAt := time.Unix(e.creds.Expiration, 0)
	return now.After(e.issuedAt.Add(expiresAt.Sub(e.issuedAt)/2)) || expiresAt.Sub(now) < podCredentialsMinRemaining
}

// RefreshOnce renews the credentials of every pod that needs it and delivers each undelivered set. A pod whose
// delivery reports errPodCredentialsGone stops being tracked.
func (p *PodCredentials) RefreshOnce(ctx context.Context, deliver PodCredentialDeliverer) {
	now := p.now()
	p.mu.Lock()
	var stale, uids []string
	for uid, entry := range p.pods {
		if entry.needsRefresh(now) {
			stale = append(stale, uid)
		}
		uids = append(uids, uid)
	}
	p.mu.Unlock()

	for _, uid := range stale {
		if err := p.refresh(ctx, uid); err != nil {
			log.Printf("[cloudvm] Cannot refresh the AWS credentials of pod %s: %v", uid, err)
		}
	}
	for _, uid := range uids {
		p.mu.Lock()
		entry, ok := p.pods[uid]
		if !ok || entry.delivered || entry.creds.AccessKeyID == "" {
			p.mu.Unlock()
			continue
		}
		creds := entry.creds
		p.mu.Unlock()

		err := deliver(ctx, uid, &creds)
		p.mu.Lock()
		switch {
		case errors.Is(err, errPodCredentialsGone):
			delete(p.pods, uid)
		case err != nil:
			log.Printf("[cloudvm] Cannot deliver the AWS credentials of pod %s: %v", uid, err)
		default:
			if entry, ok = p.pods[uid]; ok && entry.creds.AccessKeyID == creds.AccessKeyID {
				entry.delivered = true
			}
		}
		p.mu.Unlock()
	}
}

// Run refreshes and delivers credentials every interval until ctx ends.
func (p *PodCredentials) Run(ctx context.Context, interval time.Duration, deliver PodCredentialDeliverer) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			p.RefreshOnce(ctx, deliver)
		}
	}
}

// podCredentialsPath is the runner route that replaces a pod's credentials.
func podCredentialsPath(podUID string) string {
	return "/containers/credentials?podUID=" + url.QueryEscape(podUID)
}

// envFromContainers merges the env of every container, the way PodToInstanceSpec builds the workload env.
func envFromContainers(envs ...[]EnvVar) map[string]string {
	out := map[string]string{}
	for _, list := range envs {
		for _, e := range list {
			out[e.Name] = e.Value
		}
	}
	return out
}
