// Kubernetes pod watch: a shared informer that tells the reconciler which pods still exist and tells the tunnel gateway
// which namespace owns a pod IP.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const (
	podUIDIndex = "pod-uid"
	podIPIndex  = "pod-ip"
)

// PodExistence answers whether the pod with a given UID still exists in Kubernetes.
type PodExistence interface {
	// HasSynced reports whether the watch holds a complete pod list. Callers must not act on absence before it does.
	HasSynced() bool
	// PodExists reports whether a pod with the UID is present.
	PodExists(uid string) bool
}

// OrphanedPodDeleter processes the deletion of a pod whose delete call the plugin never received.
type OrphanedPodDeleter interface {
	DeleteOrphanedPod(ctx context.Context, namespace, name, uid string)
}

// PodWatch caches the UIDs and IPs of all pods in the cluster through a shared informer.
type PodWatch struct {
	informer cache.SharedIndexInformer
	factory  informers.SharedInformerFactory
}

// NewPodWatch creates a watch backed by client. Only pod metadata and the fields that decide who owns a pod IP stay in
// the cache.
func NewPodWatch(client kubernetes.Interface) *PodWatch {
	factory := informers.NewSharedInformerFactory(client, 0)
	inf := factory.Core().V1().Pods().Informer()
	_ = inf.SetTransform(func(obj any) (any, error) {
		if pod, ok := obj.(*corev1.Pod); ok {
			return &corev1.Pod{
				ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace, UID: pod.UID},
				Spec:       corev1.PodSpec{HostNetwork: pod.Spec.HostNetwork},
				Status:     corev1.PodStatus{Phase: pod.Status.Phase, PodIP: pod.Status.PodIP, PodIPs: pod.Status.PodIPs},
			}, nil
		}
		return obj, nil
	})
	// GCP labels lowercase and rewrite UIDs, so both sides compare in label form.
	_ = inf.AddIndexers(cache.Indexers{podUIDIndex: func(obj any) ([]string, error) {
		if pod, ok := obj.(*corev1.Pod); ok {
			return []string{sanitizeGCPLabel(string(pod.UID))}, nil
		}
		return nil, nil
	}, podIPIndex: podIPs})
	return &PodWatch{informer: inf, factory: factory}
}

// NewInClusterPodWatch creates a watch from the in-cluster service account. It never reads a kubeconfig, so a plugin
// running outside a cluster has no watch and the reconciler never judges pod existence.
func NewInClusterPodWatch() (*PodWatch, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to load in-cluster config: %w", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to create kubernetes client: %w", err)
	}
	return NewPodWatch(cs), nil
}

// Start runs the informer until ctx is cancelled. It returns immediately.
func (w *PodWatch) Start(ctx context.Context) {
	w.factory.Start(ctx.Done())
}

// HasSynced reports whether the informer completed its initial list.
func (w *PodWatch) HasSynced() bool {
	return w.informer.HasSynced()
}

// PodExists reports whether a cached pod has the UID.
func (w *PodWatch) PodExists(uid string) bool {
	objs, err := w.informer.GetIndexer().ByIndex(podUIDIndex, sanitizeGCPLabel(uid))
	return err != nil || len(objs) > 0
}

// PodNamespace returns the namespace of the pods that hold ip; false when no pod holds it or pods of several namespaces
// do.
func (w *PodWatch) PodNamespace(ip string) (string, bool) {
	objs, err := w.informer.GetIndexer().ByIndex(podIPIndex, ip)
	if err != nil {
		return "", false
	}
	namespace := ""
	for _, obj := range objs {
		pod, ok := obj.(*corev1.Pod)
		if !ok || (namespace != "" && pod.Namespace != namespace) {
			return "", false
		}
		namespace = pod.Namespace
	}
	return namespace, namespace != ""
}

// podIPs indexes the IPs a pod owns. A host-network pod shares its node's IP and a finished pod may have handed its IP
// to a new pod, so neither owns one.
func podIPs(obj any) ([]string, error) {
	pod, ok := obj.(*corev1.Pod)
	if !ok || pod.Spec.HostNetwork || pod.Status.Phase == corev1.PodSucceeded || pod.Status.Phase == corev1.PodFailed {
		return nil, nil
	}
	ips := make([]string, 0, len(pod.Status.PodIPs)+1)
	for _, podIP := range pod.Status.PodIPs {
		ips = append(ips, podIP.IP)
	}
	if len(ips) == 0 && pod.Status.PodIP != "" {
		ips = append(ips, pod.Status.PodIP)
	}
	return ips, nil
}

// SetPodWatch enables termination of VMs whose pod no longer exists.
func (r *Reconciler) SetPodWatch(pw PodExistence) {
	r.podWatch = pw
}

// DeleteOrphanedPod runs the same path as a virtual kubelet delete call for a pod that no longer exists.
func (s *Server) DeleteOrphanedPod(ctx context.Context, namespace, name, uid string) {
	body, err := json.Marshal(&Pod{Metadata: PodMeta{Name: name, Namespace: namespace, UID: uid}})
	if err != nil {
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, "/delete", strings.NewReader(string(body)))
	if err != nil {
		return
	}
	s.handleDelete(httptest.NewRecorder(), req)
}

// reapOrphanedVMs terminates every settled VM whose pod UID is absent from Kubernetes and returns the IDs it
// terminated. It does nothing until the pod watch has synced.
func (r *Reconciler) reapOrphanedVMs(ctx context.Context, instances []*Instance) []string {
	if r.podWatch == nil || !r.podWatch.HasSynced() {
		return nil
	}
	var reaped []string
	for _, inst := range instances {
		if inst == nil || inst.State == StateTerminated || inst.State == StateFailed {
			continue
		}
		podUID, podName, ns, _ := extractInstanceIdentity(inst)
		if podUID == "" || r.podWatch.PodExists(podUID) {
			continue
		}
		if r.launchGracePeriod > 0 && !inst.CreatedAt.IsZero() && time.Since(inst.CreatedAt) < r.launchGracePeriod {
			continue
		}
		if checker, ok := r.tracker.(CoTenantReconciler); ok && checker.HasUnknownCoTenants(inst.ID) {
			continue
		}
		cleanID := sanitizeLog(inst.ID)
		log.Printf("[reconciler] Pod %s/%s (uid %s) no longer exists. Releasing VM %s...", sanitizeLog(ns), sanitizeLog(podName), sanitizeLog(podUID), cleanID) //nolint:gosec // sanitized
		if d, ok := r.tracker.(OrphanedPodDeleter); ok {
			d.DeleteOrphanedPod(ctx, ns, podName, podUID)
		}
		// Co-tenant pods or the idle cooldown keep a pooled node alive; the idle collector terminates it later.
		if np := r.getNodePool(); np != nil && (np.HasActivePods(inst.ID) || np.HasNode(inst.ID)) {
			continue
		}
		reaped = append(reaped, inst.ID)
		if r.terminator != nil {
			if err := r.terminator.Terminate(ctx, inst.ID); err != nil {
				log.Printf("[reconciler] Error terminating orphaned VM %s: %v", cleanID, err) //nolint:gosec // sanitized
			} else if r.onTerminate != nil {
				r.onTerminate(inst.ID)
			}
		}
	}
	return reaped
}
