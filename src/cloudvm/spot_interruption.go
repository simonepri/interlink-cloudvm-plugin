// Package main implements the interlink-cloudvm-plugin daemon.
// This file turns the spot interruption notices a worker's watcher reports into pod events and KubeRay drain requests.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// spotInterruptionEvent is the event field of the watcher's notice records.
const spotInterruptionEvent = "spot-interruption"

// KubeRay pod labels that identify a Ray worker's cluster and worker group.
const (
	rayClusterLabel = "ray.io/cluster"
	rayGroupLabel   = "ray.io/group"
)

// The cluster admits the plugin's ConfigMap writes only on objects that carry this label.
const (
	managedByLabel = "app.kubernetes.io/managed-by"
	managedByValue = "interlink-cloudvm-plugin"
)

// Labels and data keys of the ConfigMap that the KubeRay operator's spot interruption controller consumes.
const (
	kubeRaySpotEventLabel     = "test.k8s.io/event"
	kubeRaySpotEventValue     = "spot-interrupted"
	kubeRayTargetClusterLabel = "target-cluster"
	kubeRayWorkerGroupKey     = "workerGroup"
	kubeRayInterruptedPodKey  = "interruptedPod"
	kubeRayReplicasKey        = "interruptedReplicas"
)

// SpotInterruptionNotice is one record the in-VM spot watcher prints when the cloud schedules the VM's interruption.
type SpotInterruptionNotice struct {
	Event string `json:"event"`
	// Action is what the cloud will do: terminate, stop, or hibernate on AWS and preempt on GCP.
	Action string `json:"action"`
	// Time is when AWS will act, or when the watcher saw the GCP preemption begin.
	Time string `json:"time"`
}

// parseSpotInterruptionNotice returns the notice a spot watcher record carries.
func parseSpotInterruptionNotice(rec *WorkerLogRecord) (SpotInterruptionNotice, bool) {
	var notice SpotInterruptionNotice
	if rec.Source != WorkerLogSourceSpot || json.Unmarshal([]byte(rec.Message), &notice) != nil {
		return SpotInterruptionNotice{}, false
	}
	return notice, notice.Event == spotInterruptionEvent
}

// RayDrainRequester asks KubeRay to drain a Ray worker pod before its VM goes away.
type RayDrainRequester interface {
	// RequestRayWorkerDrain reports whether it asked for a drain; pods that are not Ray workers, or already asked for, report false.
	RequestRayWorkerDrain(ctx context.Context, namespace, name, uid string) (bool, error)
}

// spotInterruptionConfigMapName names the drain request of one pod, so repeated notices for the pod find the same object.
func spotInterruptionConfigMapName(podUID string) string {
	return "spot-interrupted-" + podUID
}

// RequestRayWorkerDrain creates the ConfigMap that tells KubeRay a worker pod's VM is being interrupted; the pod owns it, so
// Kubernetes deletes it with the pod.
func (r *KubePodEventRecorder) RequestRayWorkerDrain(ctx context.Context, namespace, name, uid string) (bool, error) {
	pod, err := r.client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
	if apierrors.IsNotFound(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("get pod %s/%s: %w", namespace, name, err)
	}
	cluster, group := pod.Labels[rayClusterLabel], pod.Labels[rayGroupLabel]
	if cluster == "" || group == "" || pod.Labels[rayNodeTypeLabel] == "head" {
		return false, nil
	}
	_, err = r.client.CoreV1().ConfigMaps(namespace).Create(ctx, &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      spotInterruptionConfigMapName(uid),
			Namespace: namespace,
			Labels: map[string]string{
				managedByLabel:            managedByValue,
				kubeRaySpotEventLabel:     kubeRaySpotEventValue,
				kubeRayTargetClusterLabel: cluster,
			},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "v1", Kind: "Pod", Name: name, UID: types.UID(uid)}},
		},
		Data: map[string]string{
			kubeRayWorkerGroupKey:    group,
			kubeRayInterruptedPodKey: name,
			kubeRayReplicasKey:       "1",
		},
	}, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("create spot interruption ConfigMap for pod %s/%s: %w", namespace, name, err)
	}
	return true, nil
}

// handleSpotInterruption stops placing pods on the interrupted VM, posts a warning on each of its pods, and asks KubeRay to
// drain the Ray workers among them.
func (s *Server) handleSpotInterruption(ctx context.Context, instanceID string, notice SpotInterruptionNotice) {
	s.nodePool.Cordon(instanceID)
	pods := s.runningPods(instanceID)
	log.Printf("[cloudvm] Spot interruption notice for %s (action %q at %s) affects %d pods", instanceID, notice.Action, notice.Time, len(pods))
	s.mu.RLock()
	recorder := s.eventRecorder
	s.mu.RUnlock()
	if recorder == nil {
		return
	}
	drainer, _ := recorder.(RayDrainRequester)
	msg := fmt.Sprintf("Cloud VM %s received a spot interruption notice: %s at %s", instanceID, notice.Action, notice.Time)
	if len(msg) > maxPodEventMessage {
		msg = msg[:maxPodEventMessage]
	}
	for _, pod := range pods {
		callCtx, cancel := context.WithTimeout(ctx, podEventTimeout)
		if err := recorder.RecordPodEvent(callCtx, pod.PodNamespace, pod.PodName, pod.PodUID, corev1.EventTypeWarning, "CloudVMSpotInterruption", msg); err != nil {
			log.Printf("[cloudvm] Warning: %v", err)
		}
		if drainer != nil {
			if requested, err := drainer.RequestRayWorkerDrain(callCtx, pod.PodNamespace, pod.PodName, pod.PodUID); err != nil {
				log.Printf("[cloudvm] Warning: %v", err)
			} else if requested {
				log.Printf("[cloudvm] Asked KubeRay to drain Ray worker %s/%s on %s", pod.PodNamespace, pod.PodName, instanceID)
			}
		}
		cancel()
	}
}

// runningPods returns copies of the allocations of the pods still running on a node.
func (s *Server) runningPods(instanceID string) []PodAllocation {
	node, ok := s.nodePool.Node(instanceID)
	if !ok {
		return nil
	}
	node.mu.RLock()
	defer node.mu.RUnlock()
	pods := make([]PodAllocation, 0, len(node.ActivePods))
	for _, alloc := range node.ActivePods {
		if !alloc.Completed {
			pods = append(pods, *alloc)
		}
	}
	return pods
}
