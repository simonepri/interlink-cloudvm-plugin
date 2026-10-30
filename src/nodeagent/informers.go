// List options and cache transforms of the shard's two informers: scheduled pods, trimmed to a skeleton unless their
// node belongs to this shard, and Services, trimmed to what service-link environment variables need.

package main

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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
