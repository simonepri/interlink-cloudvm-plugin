// Package main implements the interlink-provider-cloudvm daemon.
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"
)

// PodStateTracker abstracts queries to the running InterLink server.
type PodStateTracker interface {
	GetActivePods(ctx context.Context) (map[string]string, error)
	MarkPodFailed(ctx context.Context, podName string, reason string) error
}

// InstanceTerminator abstracts VM termination for the reconciler.
type InstanceTerminator interface {
	Terminate(ctx context.Context, instanceID string) error
}

// Reconciler handles background garbage collection for opportunistic VMs.
type Reconciler struct {
	inventory   CloudInventory
	tracker     PodStateTracker
	terminator  InstanceTerminator
	clusterTag  string
	graceTTL    time.Duration
	onTerminate func(instanceID string)
}

// SetOnTerminated registers a callback invoked when a zombie instance is terminated.
func (r *Reconciler) SetOnTerminated(fn func(instanceID string)) {
	r.onTerminate = fn
}

// ReconcileResult summarizes the actions taken by one reconciliation pass.
type ReconcileResult struct {
	TerminatedZombies []string
	EvictedPods       []string
}

// NewReconciler creates a background garbage collector.
func NewReconciler(inventory CloudInventory, tracker PodStateTracker, terminator InstanceTerminator, clusterTag string, graceTTL time.Duration) *Reconciler {
	if graceTTL <= 0 {
		graceTTL = 10 * time.Minute
	}
	return &Reconciler{
		inventory:  inventory,
		tracker:    tracker,
		terminator: terminator,
		clusterTag: clusterTag,
		graceTTL:   graceTTL,
	}
}

// ReconcileOnce performs a single pass of bidirectional synchronization.
func (r *Reconciler) ReconcileOnce(ctx context.Context) (*ReconcileResult, error) {
	result := &ReconcileResult{}

	activeInstances, err := r.inventory.ListActiveInstances(ctx, r.clusterTag)
	if err != nil {
		return nil, fmt.Errorf("failed to query cloud inventory: %w", err)
	}

	activePods, err := r.tracker.GetActivePods(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to query active pods: %w", err)
	}

	cloudInstances := make(map[string]*Instance, len(activeInstances))
	for _, inst := range activeInstances {
		if inst != nil {
			cloudInstances[inst.ID] = inst
		}
	}

	// 1. Forward check: detect pods whose cloud VM vanished or terminated
	for podKey, instID := range activePods {
		if instID == "" {
			continue
		}
		inst, alive := cloudInstances[instID]
		if !alive || (inst != nil && (inst.State == StateTerminated || inst.State == StateFailed)) {
			cleanKey := strings.ReplaceAll(strings.ReplaceAll(podKey, "\n", ""), "\r", "")
			cleanID := strings.ReplaceAll(strings.ReplaceAll(instID, "\n", ""), "\r", "")
			log.Printf("[reconciler] Detected vanished/interrupted cloud VM %s for pod %s. Marking pod failed...", cleanID, cleanKey)
			if err := r.tracker.MarkPodFailed(ctx, podKey, "SpotInterruption"); err != nil {
				log.Printf("[reconciler] Error marking pod %s failed: %v", cleanKey, err)
			} else {
				result.EvictedPods = append(result.EvictedPods, podKey)
			}
		}
	}

	// 2. Reverse check: detect zombie VMs whose pods vanished from tracker
	activeInstanceIDs := make(map[string]bool, len(activePods))
	for _, id := range activePods {
		if id != "" {
			activeInstanceIDs[id] = true
		}
	}

	now := time.Now()
	for _, inst := range activeInstances {
		if activeInstanceIDs[inst.ID] {
			continue
		}

		podName := inst.Metadata["podName"]
		if podName == "" {
			podName = inst.Metadata["PodName"]
		}
		if podName == "" {
			continue
		}

		ns := inst.Metadata["namespace"]
		if ns == "" {
			ns = inst.Metadata["Namespace"]
		}
		if ns == "" {
			ns = "default"
		}

		fullKey := fmt.Sprintf("%s/%s", ns, podName)
		_, existsFull := activePods[fullKey]
		_, existsBare := activePods[podName]

		if !existsFull && !existsBare {
			if now.Sub(inst.CreatedAt) > r.graceTTL {
				cleanID := strings.ReplaceAll(strings.ReplaceAll(inst.ID, "\n", ""), "\r", "")
				cleanKey := strings.ReplaceAll(strings.ReplaceAll(fullKey, "\n", ""), "\r", "")
				log.Printf("[reconciler] Detected zombie instance %s for vanished pod %s. Terminating...", cleanID, cleanKey)
				result.TerminatedZombies = append(result.TerminatedZombies, inst.ID)
				if r.terminator != nil {
					if err := r.terminator.Terminate(ctx, inst.ID); err != nil {
						log.Printf("[reconciler] Error terminating zombie instance %s: %v", cleanID, err)
					} else if r.onTerminate != nil {
						r.onTerminate(inst.ID)
					}
				}
			}
		}
	}

	return result, nil
}
