// Secret and ConfigMap listers that read each object with one API GET when a pod's environment needs it, so the agent
// never caches Secrets or ConfigMaps of the whole cluster.

package main

import (
	"context"
	"errors"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

// getTimeout bounds one Secret or ConfigMap read.
const getTimeout = 10 * time.Second

// errListUnsupported is returned by List: the virtual kubelet reads Secrets and ConfigMaps only by name.
var errListUnsupported = errors.New("listing is not supported; objects are read by name")

// getSecretInformer satisfies the virtual kubelet's SecretInformer with GET-backed reads. The virtual kubelet calls
// only Lister on it.
type getSecretInformer struct{ client kubernetes.Interface }

func (getSecretInformer) Informer() cache.SharedIndexInformer { return nil }

func (i getSecretInformer) Lister() corev1listers.SecretLister { return getSecretLister(i) }

type getSecretLister struct{ client kubernetes.Interface }

func (getSecretLister) List(labels.Selector) ([]*corev1.Secret, error) {
	return nil, errListUnsupported
}

func (l getSecretLister) Secrets(namespace string) corev1listers.SecretNamespaceLister {
	return getSecretNamespaceLister{client: l.client, namespace: namespace}
}

type getSecretNamespaceLister struct {
	client    kubernetes.Interface
	namespace string
}

func (getSecretNamespaceLister) List(labels.Selector) ([]*corev1.Secret, error) {
	return nil, errListUnsupported
}

func (l getSecretNamespaceLister) Get(name string) (*corev1.Secret, error) {
	ctx, cancel := context.WithTimeout(context.Background(), getTimeout)
	defer cancel()
	return l.client.CoreV1().Secrets(l.namespace).Get(ctx, name, metav1.GetOptions{})
}

// getConfigMapInformer satisfies the virtual kubelet's ConfigMapInformer with GET-backed reads.
type getConfigMapInformer struct{ client kubernetes.Interface }

func (getConfigMapInformer) Informer() cache.SharedIndexInformer { return nil }

func (i getConfigMapInformer) Lister() corev1listers.ConfigMapLister { return getConfigMapLister(i) }

type getConfigMapLister struct{ client kubernetes.Interface }

func (getConfigMapLister) List(labels.Selector) ([]*corev1.ConfigMap, error) {
	return nil, errListUnsupported
}

func (l getConfigMapLister) ConfigMaps(namespace string) corev1listers.ConfigMapNamespaceLister {
	return getConfigMapNamespaceLister{client: l.client, namespace: namespace}
}

type getConfigMapNamespaceLister struct {
	client    kubernetes.Interface
	namespace string
}

func (getConfigMapNamespaceLister) List(labels.Selector) ([]*corev1.ConfigMap, error) {
	return nil, errListUnsupported
}

func (l getConfigMapNamespaceLister) Get(name string) (*corev1.ConfigMap, error) {
	ctx, cancel := context.WithTimeout(context.Background(), getTimeout)
	defer cancel()
	return l.client.CoreV1().ConfigMaps(l.namespace).Get(ctx, name, metav1.GetOptions{})
}

// imagePullSecrets reads the Secrets a pod's imagePullSecrets name; like the kubelet, it skips one it cannot read.
func imagePullSecrets(ctx context.Context, client kubernetes.Interface, pod *corev1.Pod) []corev1.Secret {
	var out []corev1.Secret
	for _, ref := range pod.Spec.ImagePullSecrets {
		if ref.Name == "" {
			continue
		}
		s, err := client.CoreV1().Secrets(pod.Namespace).Get(ctx, ref.Name, metav1.GetOptions{})
		if err != nil {
			logf("pod %s/%s: skipping image pull secret %s: %v", pod.Namespace, pod.Name, ref.Name, err)
			continue
		}
		out = append(out, *s)
	}
	return out
}
