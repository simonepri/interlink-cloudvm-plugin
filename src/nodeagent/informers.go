// List options and cache transforms of the shard's two informers: scheduled pods, trimmed to a skeleton unless their
// node belongs to this shard, and Services, trimmed to what service-link environment variables need; and the pod
// informer view the pod controller reads, without the pod and host IP fieldRefs it cannot resolve.

package main

import (
	"slices"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	corev1informers "k8s.io/client-go/informers/core/v1"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

// scheduledPods restricts the pod watch to pods bound to a node, the set the apiserver indexes by spec.nodeName.
func scheduledPods(opts *metav1.ListOptions) {
	opts.FieldSelector = "spec.nodeName!="
}

// podTransform keeps full pods of this shard's nodes and only identity fields of other pods, so the shared cache
// costs little for pods the shard never runs.
func podTransform(shard Shard) cache.TransformFunc {
	return func(obj any) (any, error) {
		pod, ok := obj.(*corev1.Pod)
		if !ok {
			return obj, nil
		}
		pod.ManagedFields = nil
		if shard.Owns(pod.Spec.NodeName) {
			return pod, nil
		}
		return &corev1.Pod{
			TypeMeta: pod.TypeMeta,
			ObjectMeta: metav1.ObjectMeta{
				Name: pod.Name, Namespace: pod.Namespace, UID: pod.UID, ResourceVersion: pod.ResourceVersion,
				DeletionTimestamp: pod.DeletionTimestamp,
			},
			Spec: corev1.PodSpec{NodeName: pod.Spec.NodeName},
		}, nil
	}
}

// serviceTransform keeps the fields the kubelet's service-link environment variables read.
func serviceTransform(obj any) (any, error) {
	svc, ok := obj.(*corev1.Service)
	if !ok {
		return obj, nil
	}
	return &corev1.Service{
		TypeMeta:   svc.TypeMeta,
		ObjectMeta: metav1.ObjectMeta{Name: svc.Name, Namespace: svc.Namespace, UID: svc.UID, ResourceVersion: svc.ResourceVersion},
		Spec:       corev1.ServiceSpec{ClusterIP: svc.Spec.ClusterIP, ClusterIPs: svc.Spec.ClusterIPs, Ports: svc.Spec.Ports},
	}, nil
}

// controllerPodInformer is the pod informer the pod controller reads. The controller resolves every env fieldRef
// itself and refuses the pod and host IP fields, which a pod has no value for before it starts, so a pod that names
// one would never reach the plugin; its pods lack those variables, which CreatePod restores from the informer's copy.
type controllerPodInformer struct{ corev1informers.PodInformer }

func (i controllerPodInformer) Lister() corev1listers.PodLister {
	return controllerPodLister{i.PodInformer.Lister()}
}

type controllerPodLister struct{ corev1listers.PodLister }

func (l controllerPodLister) List(selector labels.Selector) ([]*corev1.Pod, error) {
	return withoutHostIPFieldRefsAll(l.PodLister.List(selector))
}

func (l controllerPodLister) Pods(namespace string) corev1listers.PodNamespaceLister {
	return controllerPodNamespaceLister{l.PodLister.Pods(namespace)}
}

type controllerPodNamespaceLister struct {
	corev1listers.PodNamespaceLister
}

func (l controllerPodNamespaceLister) List(selector labels.Selector) ([]*corev1.Pod, error) {
	return withoutHostIPFieldRefsAll(l.PodNamespaceLister.List(selector))
}

func (l controllerPodNamespaceLister) Get(name string) (*corev1.Pod, error) {
	pod, err := l.PodNamespaceLister.Get(name)
	if err != nil {
		return nil, err
	}
	return withoutHostIPFieldRefs(pod), nil
}

func withoutHostIPFieldRefsAll(pods []*corev1.Pod, err error) ([]*corev1.Pod, error) {
	if err != nil {
		return nil, err
	}
	out := make([]*corev1.Pod, len(pods))
	for i, pod := range pods {
		out[i] = withoutHostIPFieldRefs(pod)
	}
	return out, nil
}

// withoutHostIPFieldRefs returns the pod, or a copy of it without its env variables that reference the pod or host IP.
func withoutHostIPFieldRefs(pod *corev1.Pod) *corev1.Pod {
	refersToHostIP := func(e corev1.EnvVar) bool {
		return e.ValueFrom != nil && e.ValueFrom.FieldRef != nil && hostIPFields[e.ValueFrom.FieldRef.FieldPath]
	}
	has := func(cs []corev1.Container) bool {
		return slices.ContainsFunc(cs, func(c corev1.Container) bool { return slices.ContainsFunc(c.Env, refersToHostIP) })
	}
	if !has(pod.Spec.InitContainers) && !has(pod.Spec.Containers) {
		return pod
	}
	out := pod.DeepCopy()
	for _, cs := range [][]corev1.Container{out.Spec.InitContainers, out.Spec.Containers} {
		for i := range cs {
			cs[i].Env = slices.DeleteFunc(cs[i].Env, refersToHostIP)
		}
	}
	return out
}
