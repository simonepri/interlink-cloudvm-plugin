// Reads the Kubernetes objects a pod's volumes and environment name besides Secrets: ConfigMaps, the API server's
// default/kubernetes Service and the pod's ServiceAccount, and mints the pod's projected service account tokens.
package main

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// ConfigMapResolver returns a ConfigMap's data, binary keys included.
type ConfigMapResolver interface {
	GetConfigMap(ctx context.Context, namespace, name string) (map[string][]byte, error)
}

// ServiceAccountTokenIssuer mints a token for the pod's service account that the API server invalidates with the pod.
type ServiceAccountTokenIssuer interface {
	CreatePodToken(ctx context.Context, pod PodRef, audience string, expirationSeconds int64) (token string, expiresAt time.Time, err error)
}

// ServiceAccountAutomount reports the automountServiceAccountToken of a ServiceAccount, nil when it sets none.
type ServiceAccountAutomount interface {
	ServiceAccountAutomount(ctx context.Context, namespace, name string) (*bool, error)
}

// APIServerService returns the ClusterIP and port of the default/kubernetes Service.
type APIServerService interface {
	APIServerService(ctx context.Context) (host string, port int32, err error)
}

// GetConfigMap reads the ConfigMap's data and binaryData.
func (r *KubeSecretResolver) GetConfigMap(ctx context.Context, namespace, name string) (map[string][]byte, error) {
	if r.client == nil {
		return nil, errors.New("kubernetes client not initialized")
	}
	cm, err := r.client.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get configmap %s/%s: %w", namespace, name, err)
	}
	data := make(map[string][]byte, len(cm.Data)+len(cm.BinaryData))
	for k, v := range cm.Data {
		data[k] = []byte(v)
	}
	for k, v := range cm.BinaryData {
		data[k] = v
	}
	return data, nil
}

// CreatePodToken mints a token bound to the pod through the TokenRequest API; an empty audience gets the API server's.
func (r *KubeSecretResolver) CreatePodToken(ctx context.Context, pod PodRef, audience string, expirationSeconds int64) (string, time.Time, error) {
	if r.client == nil {
		return "", time.Time{}, errors.New("kubernetes client not initialized")
	}
	sa := pod.ServiceAccount
	if sa == "" {
		sa = "default"
	}
	spec := authenticationv1.TokenRequestSpec{
		ExpirationSeconds: &expirationSeconds,
		BoundObjectRef:    &authenticationv1.BoundObjectReference{Kind: "Pod", APIVersion: "v1", Name: pod.Name, UID: types.UID(pod.UID)},
	}
	if audience != "" {
		spec.Audiences = []string{audience}
	}
	tr, err := r.client.CoreV1().ServiceAccounts(pod.Namespace).CreateToken(ctx, sa, &authenticationv1.TokenRequest{Spec: spec}, metav1.CreateOptions{})
	if err != nil {
		return "", time.Time{}, fmt.Errorf("create token for service account %s/%s: %w", pod.Namespace, sa, err)
	}
	return tr.Status.Token, tr.Status.ExpirationTimestamp.Time, nil
}

// ServiceAccountAutomount reads the ServiceAccount's automountServiceAccountToken.
func (r *KubeSecretResolver) ServiceAccountAutomount(ctx context.Context, namespace, name string) (*bool, error) {
	if r.client == nil {
		return nil, errors.New("kubernetes client not initialized")
	}
	sa, err := r.client.CoreV1().ServiceAccounts(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to get service account %s/%s: %w", namespace, name, err)
	}
	return sa.AutomountServiceAccountToken, nil
}

// apiServerServiceTTL is how long the default/kubernetes Service address stays cached; it changes only when the
// cluster's Service network does.
const apiServerServiceTTL = 10 * time.Minute

// apiServerServiceCache holds the last default/kubernetes Service address read.
type apiServerServiceCache struct {
	sync.Mutex
	host    string
	port    int32
	err     error
	fetched time.Time
}

// apiServerServiceRetry is how long a failed default/kubernetes Service read is reported before it is tried again.
const apiServerServiceRetry = time.Minute

// APIServerService reads the default/kubernetes Service's ClusterIP and HTTPS port, caching them.
func (r *KubeSecretResolver) APIServerService(ctx context.Context) (string, int32, error) {
	if r.client == nil {
		return "", 0, errors.New("kubernetes client not initialized")
	}
	c := &r.apiServer
	c.Lock()
	defer c.Unlock()
	if c.host != "" && time.Since(c.fetched) < apiServerServiceTTL {
		return c.host, c.port, nil
	}
	if c.err != nil && time.Since(c.fetched) < apiServerServiceRetry {
		return "", 0, c.err
	}
	c.fetched = time.Now()
	svc, err := r.client.CoreV1().Services(metav1.NamespaceDefault).Get(ctx, "kubernetes", metav1.GetOptions{})
	if err == nil && (svc.Spec.ClusterIP == "" || svc.Spec.ClusterIP == corev1.ClusterIPNone || len(svc.Spec.Ports) == 0) {
		err = errors.New("it has no ClusterIP or port")
	}
	if err != nil {
		c.host, c.err = "", fmt.Errorf("service default/kubernetes: %w", err)
		return "", 0, c.err
	}
	port := svc.Spec.Ports[0].Port
	for _, p := range svc.Spec.Ports {
		if p.Name == "https" {
			port = p.Port
		}
	}
	c.host, c.port, c.err = svc.Spec.ClusterIP, port, nil
	return c.host, c.port, nil
}
