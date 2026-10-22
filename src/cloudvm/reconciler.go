// Package main implements the interlink-provider-cloudvm daemon.
package main

import (
	"context"
	"fmt"
	"log"
	"time"
)

// PodStateTracker abstracts queries to the running InterLink server.
type PodStateTracker interface {
	GetActivePods(ctx context.Context) (map[string]string, error)
	MarkPodFailed(ctx context.Context, podName string, reason string) error
}

// Reconciler handles background garbage collection for opportunistic VMs.
type Reconciler struct {
	inventory  CloudInventory
	tracker    PodStateTracker
	clusterTag string
	graceTTL   time.Duration
}

// ReconcileResult summarizes the actions taken by one reconciliation pass.
type ReconcileResult struct {
	TerminatedZombies []string
	EvictedPods       []string
}

// NewReconciler creates a background garbage collector.
func NewReconciler(inventory CloudInventory, tracker PodStateTracker, clusterTag string, graceTTL time.Duration) *Reconciler {
	if graceTTL == 0 {
		graceTTL = 5 * time.Minute
	}
	return &Reconciler{
		inventory:  inventory,
		tracker:    tracker,
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

	now := time.Now()
	for _, inst := range activeInstances {
		podName := inst.Metadata["podName"]
		if podName == "" {
			continue
		}

		_, podExists := activePods[podName]
		if !podExists {
			if now.Sub(inst.CreatedAt) > r.graceTTL {
				log.Printf("[reconciler] Detected zombie instance for vanished pod. Terminating...")
				result.TerminatedZombies = append(result.TerminatedZombies, inst.ID)
			}
		}
	}

	return result, nil
}
