// Package main implements the interlink-provider-cloudvm daemon.
package main

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"
)

// UnknownCoTenantTimeout defines the elapsed duration before an unverified node is released for garbage collection.
var UnknownCoTenantTimeout = 30 * time.Minute

func extractInstanceIdentity(inst *Instance) (podUID, podName, ns, marketType string) {
	if inst == nil || inst.Metadata == nil {
		return "", "", "", ""
	}
	podUID = inst.Metadata["podUID"]
	podName = inst.Metadata["podName"]
	if podName == "" {
		podName = inst.Metadata["PodName"]
	}
	ns = inst.Metadata["namespace"]
	if ns == "" {
		ns = inst.Metadata["Namespace"]
	}
	if ns == "" {
		ns = "default"
	}
	marketType = inst.Metadata["marketType"]
	if marketType == "" {
		marketType = "on-demand"
	}
	inst.Metadata["marketType"] = marketType
	return podUID, podName, ns, marketType
}

// buildRecoveredNode constructs a cordoned CloudVMNode and resolves its runner auth token.
func (s *Server) buildRecoveredNode(inst *Instance, podUID, ns, marketType string) (*CloudVMNode, ResourceVector) {
	defaultVec, _ := NewResourceVector("2", "4Gi", 0, 0)
	capVec := CapacityForInstanceType(inst.InstanceType, defaultVec)
	recNode := NewCloudVMNode(inst.ID, inst.Provider, inst.Region, inst.IP, capVec)
	recNode.Namespace = ns
	recNode.Spot = (marketType == "spot")
	recNode.Cordoned = true

	tok := ""
	if s.tokenStore != nil {
		tok = s.tokenStore.Get(inst.ID)
		if tok == "" && podUID != "" {
			tok = s.tokenStore.Get(podUID)
		}
		if tok != "" && s.tokenStore.Get(inst.ID) == "" {
			_ = s.tokenStore.Set(inst.ID, tok)
		}
	}
	if tok == "" {
		tok = inst.Metadata["runnerToken"]
	}
	if tok == "" {
		tok = inst.Metadata["cloudvm:runner-token"]
	}
	if tok != "" && s.tokenStore != nil {
		_ = s.tokenStore.Set(inst.ID, tok)
	}
	recNode.RunnerAuthToken = tok
	return recNode, defaultVec
}

// AdoptInstance re-adopts an active cloud VM into the server tracked state if it has valid pod tags
// and has not been explicitly deleted or completed.
func (s *Server) AdoptInstance(ctx context.Context, inst *Instance) bool {
	podUID, podName, ns, marketType := extractInstanceIdentity(inst)
	if podUID == "" || podName == "" {
		return false
	}

	s.mu.RLock()
	if s.isTombstonedLocked(podUID) || s.isTombstonedLocked(inst.ID) {
		s.mu.RUnlock()
		return false
	}
	if s.completedPods[podUID] != nil || (s.completedStore != nil && s.completedStore.Get(podUID) != nil) {
		s.mu.RUnlock()
		return false
	}
	if !s.statusPolled {
		s.mu.RUnlock()
		return false
	}
	sighting, sighted := s.activeSightings[podUID]
	if !sighted || time.Since(sighting) > 10*time.Minute {
		s.mu.RUnlock()
		return false
	}
	if s.instances[podUID] != nil || s.instances[inst.ID] != nil {
		s.mu.RUnlock()
		return true
	}
	s.mu.RUnlock()

	var recNode *CloudVMNode
	var defaultVec ResourceVector
	if s.nodePool != nil {
		recNode, defaultVec = s.buildRecoveredNode(inst, podUID, ns, marketType)
	}

	// Query containers outside s.mu lock to avoid stalling HTTP handlers on unreachable nodes
	var containers []*ContainerStatusResult
	if s.runner != nil && recNode != nil {
		var listErr error
		containers, listErr = s.runner.ListContainers(ctx, recNode)
		if listErr != nil {
			recNode.UnknownCoTenants = true
			recNode.UnknownSince = time.Now()
			log.Printf("[cloudvm] Warning: failed to list containers on adopted VM %s: %v. Flagged with UnknownCoTenants.", sanitizeLog(inst.ID), listErr) //nolint:gosec // sanitized
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isTombstonedLocked(podUID) || s.isTombstonedLocked(inst.ID) {
		return false
	}
	if s.completedPods[podUID] != nil || (s.completedStore != nil && s.completedStore.Get(podUID) != nil) {
		return false
	}
	if s.instances[podUID] != nil || s.instances[inst.ID] != nil {
		return true
	}

	key := fmt.Sprintf("%s/%s", ns, podName)
	s.instances[key] = inst
	s.instances[podUID] = inst
	s.instances[inst.ID] = inst

	if s.nodePool != nil && recNode != nil {
		s.nodePool.RegisterAndAllocate(recNode, defaultVec, podUID, ns, podName)
		for _, c := range containers {
			if c != nil && c.PodUID != "" && c.PodUID != podUID && !s.isTombstonedLocked(c.PodUID) {
				s.instances[c.PodUID] = inst
				s.nodePool.RegisterAndAllocate(recNode, defaultVec, c.PodUID, ns, c.PodUID)
			}
		}
	}
	return true
}

// RecoverActiveInstances queries the cloud inventory to rebuild in-memory state on startup.
func (s *Server) RecoverActiveInstances(ctx context.Context, inv CloudInventory) error {
	if inv == nil {
		return nil
	}
	instances, err := inv.ListActiveInstances(ctx, s.clusterTag)
	if err != nil {
		return fmt.Errorf("failed to recover cloud instances: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, inst := range instances {
		podUID, podName, ns, marketType := extractInstanceIdentity(inst)
		if s.isTombstonedLocked(podUID) || s.isTombstonedLocked(inst.ID) {
			log.Printf("[cloudvm] Skipping tombstoned instance %s (pod %s) during startup recovery", sanitizeLog(inst.ID), sanitizeLog(podUID)) //nolint:gosec // sanitized
			continue
		}
		if podName == "" {
			continue
		}

		key := fmt.Sprintf("%s/%s", ns, podName)
		s.instances[key] = inst
		if podUID == "" {
			podUID = key
		} else {
			s.instances[podUID] = inst
		}
		s.instances[inst.ID] = inst

		recNode, defaultVec := s.buildRecoveredNode(inst, podUID, ns, marketType)
		s.nodePool.RegisterAndAllocate(recNode, defaultVec, podUID, ns, podName)

		if s.runner != nil {
			containers, err := s.runner.ListContainers(ctx, recNode)
			if err == nil {
				for _, c := range containers {
					if c.PodUID != "" && c.PodUID != podUID && !s.isTombstonedLocked(c.PodUID) {
						s.instances[c.PodUID] = inst
						s.nodePool.RegisterAndAllocate(recNode, defaultVec, c.PodUID, ns, c.PodUID)
					}
				}
			} else {
				recNode.UnknownCoTenants = true
				recNode.UnknownSince = time.Now()
				log.Printf("[cloudvm] Warning: failed to list containers on recovered VM %s: %v. Flagged with UnknownCoTenants.", sanitizeLog(inst.ID), err) //nolint:gosec // sanitized
			}
		}
		log.Printf("[cloudvm] Recovered existing cloud VM %s for pod %s", sanitizeLog(inst.ID), sanitizeLog(key)) //nolint:gosec // sanitized
	}
	return nil
}

// ActiveInstances returns a snapshot of known active instances.
func (s *Server) ActiveInstances() map[string]*Instance {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]*Instance, len(s.instances))
	for k, v := range s.instances {
		out[k] = v
	}
	return out
}

// GetPodRegion returns the cloud region associated with podKey.
func (s *Server) GetPodRegion(_ context.Context, podKey string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	inst, ok := s.instances[podKey]
	if !ok || inst == nil {
		return "", false
	}
	return inst.Region, true
}

// GetPodCreatedAt returns the creation timestamp of the instance associated with podKey.
func (s *Server) GetPodCreatedAt(_ context.Context, podKey string) (time.Time, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	inst, ok := s.instances[podKey]
	if !ok || inst == nil {
		return time.Time{}, false
	}
	return inst.CreatedAt, true
}

// GetActivePods returns a map of genuinely active (running/pending) pod UIDs to their cloud instance IDs.
func (s *Server) GetActivePods(_ context.Context) (map[string]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	res := make(map[string]string)
	for k, inst := range s.instances {
		if inst == nil {
			continue
		}
		uid := k
		if inst.Metadata != nil && inst.Metadata["podUID"] != "" {
			uid = inst.Metadata["podUID"]
		}
		if s.isTombstonedLocked(uid) {
			continue
		}
		if s.completedPods[uid] != nil || (s.completedStore != nil && s.completedStore.Get(uid) != nil) {
			continue
		}
		if s.nodePool != nil {
			if _, alloc, found := s.nodePool.FindPodNode(uid); found && alloc.Completed {
				continue
			}
		}
		if inst.State == StateRunning || inst.State == StateProvisioning {
			res[uid] = inst.ID
		}
	}
	return res, nil
}

// MarkPodFailed marks a pod instance state as failed and commits terminal execution telemetry.
func (s *Server) MarkPodFailed(_ context.Context, podKey string, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	var uid string
	var jid string
	var inst *Instance
	if i, ok := s.instances[podKey]; ok {
		inst = i
		if inst.Metadata != nil {
			uid = inst.Metadata["podUID"]
		}
	}
	if uid == "" {
		uid = podKey
	}

	if existing := s.completedPods[uid]; existing != nil && existing.Reason == "Completed" {
		return nil
	}
	if s.completedStore != nil {
		if rec := s.completedStore.Get(uid); rec != nil && rec.Reason == "Completed" {
			return nil
		}
	}
	if existing := s.completedPods[uid]; existing != nil && (existing.Reason == "SpotInterruption" || existing.Reason == "NodeLost") {
		return nil
	}

	if inst != nil && inst.Metadata != nil {
		if code := inst.Metadata["stateReasonCode"]; code != "" {
			if strings.Contains(code, "SpotInstanceTermination") {
				reason = "SpotInterruption"
			} else {
				reason = code
			}
		} else if reason == "SpotInterruption" {
			if inst.Metadata["marketType"] == "on-demand" {
				reason = "NodeLost"
			}
		}
	}

	if inst != nil {
		inst.State = StateFailed
		if inst.Metadata == nil {
			inst.Metadata = make(map[string]string)
		}
		inst.Metadata["failureReason"] = reason
		jid = fmt.Sprintf("%s:%s", inst.ID, uid)
	} else {
		jid = fmt.Sprintf("cvm-%s", uid)
	}

	exitCode := int32(137)
	finAt := time.Now()
	term := &ContainerTerminatedState{
		ExitCode:   exitCode,
		Reason:     reason,
		Message:    fmt.Sprintf("Underlying cloud VM terminated (%s)", reason),
		FinishedAt: finAt,
	}
	s.completedPods[uid] = term

	if s.nodePool != nil {
		s.nodePool.MarkPodCompleted(uid, exitCode, reason, term.Message, finAt)
		if inst != nil {
			if node, _, found := s.nodePool.FindPodNode(uid); !found || node.ActivePodCount() == 0 {
				s.nodePool.UnregisterNode(inst.ID)
			}
		}
	}
	if s.completedStore != nil {
		_ = s.completedStore.Set(uid, &CompletedPodRecord{
			PodUID:     uid,
			PodJID:     jid,
			ExitCode:   exitCode,
			Reason:     reason,
			Message:    term.Message,
			FinishedAt: finAt,
		})
	}
	return nil
}

// RetryUnknownCoTenants attempts to query the in-VM runner for nodes whose startup listing failed.
func (s *Server) RetryUnknownCoTenants(ctx context.Context) {
	if s.nodePool == nil || s.runner == nil {
		return
	}
	nodes := s.nodePool.NodesWithUnknownCoTenants()
	for _, node := range nodes {
		containers, err := s.runner.ListContainers(ctx, node)
		if err == nil {
			s.mu.Lock()
			defaultVec, _ := NewResourceVector("2", "4Gi", 0, 0)
			for _, c := range containers {
				if c != nil && c.PodUID != "" && !s.isTombstonedLocked(c.PodUID) {
					if _, _, exists := s.nodePool.FindPodNode(c.PodUID); !exists {
						if inst := s.instances[node.InstanceID]; inst != nil {
							s.instances[c.PodUID] = inst
						}
						s.nodePool.RegisterAndAllocate(node, defaultVec, c.PodUID, node.Namespace, c.PodUID)
					}
				}
			}
			s.mu.Unlock()
			s.nodePool.SetUnknownCoTenants(node.InstanceID, false)
			log.Printf("[cloudvm] Successfully resolved co-tenants for node %s via background reconciliation", sanitizeLog(node.InstanceID)) //nolint:gosec // sanitized
		} else {
			retries, exceeded := s.nodePool.IncrementUnknownCoTenantRetries(node.InstanceID, UnknownCoTenantTimeout)
			if exceeded {
				log.Printf("[cloudvm] ERROR: node %s exceeded %v co-tenant reconciliation timeout (%d attempts, err: %v). Clearing unknown flag to allow garbage collection.", sanitizeLog(node.InstanceID), UnknownCoTenantTimeout, retries, err) //nolint:gosec // sanitized
			} else {
				log.Printf("[cloudvm] Retrying co-tenant discovery for node %s (attempt %d, elapsed %v): %v", sanitizeLog(node.InstanceID), retries, time.Since(node.UnknownSince).Truncate(time.Second), err) //nolint:gosec // sanitized
			}
		}
	}
}
