// Kubernetes pod event recorder: posts core/v1 Events on pods so users see launch decisions, such as the chosen
// region, in `kubectl describe pod`.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// PodEventRecorder posts an event on a pod.
type PodEventRecorder interface {
	RecordPodEvent(ctx context.Context, namespace, name, uid, eventType, reason, message string) error
}

// KubePodEventRecorder posts pod events through the Kubernetes API.
type KubePodEventRecorder struct {
	client kubernetes.Interface
}

// NewKubePodEventRecorder creates a recorder backed by client.
func NewKubePodEventRecorder(client kubernetes.Interface) *KubePodEventRecorder {
	return &KubePodEventRecorder{client: client}
}

// NewInClusterPodEventRecorder creates a recorder from the in-cluster service account. It never reads a kubeconfig,
// so a plugin running outside a cluster posts no events.
func NewInClusterPodEventRecorder() (*KubePodEventRecorder, error) {
	cfg, err := kubeConfig(false)
	if err != nil {
		return nil, fmt.Errorf("unable to load in-cluster config: %w", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to create kubernetes client: %w", err)
	}
	return NewKubePodEventRecorder(cs), nil
}

// RecordPodEvent creates one event on the pod.
func (r *KubePodEventRecorder) RecordPodEvent(ctx context.Context, namespace, name, uid, eventType, reason, message string) error {
	now := metav1.NewTime(time.Now())
	_, err := r.client.CoreV1().Events(namespace).Create(ctx, &corev1.Event{
		ObjectMeta: metav1.ObjectMeta{GenerateName: name + ".", Namespace: namespace},
		InvolvedObject: corev1.ObjectReference{
			APIVersion: "v1",
			Kind:       "Pod",
			Namespace:  namespace,
			Name:       name,
			UID:        types.UID(uid),
		},
		Type:           eventType,
		Reason:         reason,
		Message:        message,
		Source:         corev1.EventSource{Component: "interlink-cloudvm-plugin"},
		FirstTimestamp: now,
		LastTimestamp:  now,
		Count:          1,
	}, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("create event %s on pod %s/%s: %w", reason, namespace, name, err)
	}
	return nil
}

// recordLaunchRegionEvent posts a CloudVMRegionSelected event naming the region a new VM launched in and any regions
// that failed before it; launches that report no region post nothing.
func (s *Server) recordLaunchRegionEvent(ctx context.Context, spec *InstanceSpec, inst *Instance) {
	s.mu.RLock()
	recorder := s.eventRecorder
	s.mu.RUnlock()
	region := inst.Metadata["launchRegion"]
	if recorder == nil || region == "" {
		return
	}
	msg := fmt.Sprintf("Launched cloud VM %s (%s) in %s", inst.ID, inst.InstanceType, region)
	if fallbacks := inst.Metadata["regionFallbacks"]; fallbacks != "" {
		msg = fmt.Sprintf("%s after falling back from %s: %s", msg, inst.Metadata["requestedRegion"], fallbacks)
	}
	if len(msg) > maxPodEventMessage {
		msg = msg[:maxPodEventMessage]
	}
	eventCtx, cancel := context.WithTimeout(ctx, podEventTimeout)
	defer cancel()
	if err := recorder.RecordPodEvent(eventCtx, spec.PodNamespace, spec.PodName, spec.PodUID, corev1.EventTypeNormal, "CloudVMRegionSelected", msg); err != nil {
		log.Printf("[cloudvm] Warning: %v", err)
	}
}

// maxPodEventMessage keeps event messages within the 1 KiB that Kubernetes event consumers accept.
const maxPodEventMessage = 1024

// podEventTimeout bounds one event post so an unreachable API server cannot delay a launch response.
const podEventTimeout = 5 * time.Second
