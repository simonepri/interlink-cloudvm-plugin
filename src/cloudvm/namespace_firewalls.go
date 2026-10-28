// Namespace firewalls: reads regions.<region>.namespaceFirewalls from CLUSTER_FACTS, resolves the firewall each pod's
// namespace joins at launch, and posts a warning event on pods whose namespace has none.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

// errNamespaceFirewallMissing reports a launch refused because the region isolates namespaces but lists no firewall
// for the pod's namespace.
var errNamespaceFirewallMissing = errors.New("no namespace firewall for the pod's namespace")

var (
	// namespaceNamePattern matches a Kubernetes namespace name (an RFC 1123 label).
	namespaceNamePattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?$`)
	// securityGroupIDPattern matches an EC2 security group ID.
	securityGroupIDPattern = regexp.MustCompile(`^sg-[0-9a-f]{8,17}$`)
	// networkTagPattern matches a Compute Engine network tag.
	networkTagPattern = regexp.MustCompile(`^[a-z]([-a-z0-9]{0,61}[a-z0-9])?$`)
)

// NamespaceFirewallsFromClusterFacts reads regions.<region>.namespaceFirewalls from the CLUSTER_FACTS JSON, keyed by
// region and then namespace, rejecting a key that is not a namespace name and a value that is neither an EC2 security
// group ID nor a Compute Engine network tag. Regions without entries are absent from the result.
func NamespaceFirewallsFromClusterFacts(raw string) (map[string]map[string]string, error) {
	firewalls := map[string]map[string]string{}
	if strings.TrimSpace(raw) == "" {
		return firewalls, nil
	}
	var facts ClusterFacts
	if err := json.Unmarshal([]byte(raw), &facts); err != nil {
		return nil, fmt.Errorf("invalid CLUSTER_FACTS: %w", err)
	}
	for region, fact := range facts.Regions {
		for namespace, firewall := range fact.NamespaceFirewalls {
			if !namespaceNamePattern.MatchString(namespace) {
				return nil, fmt.Errorf("CLUSTER_FACTS region %s: namespaceFirewalls: %q is not a namespace name", region, namespace)
			}
			if !securityGroupIDPattern.MatchString(firewall) && !networkTagPattern.MatchString(firewall) {
				return nil, fmt.Errorf("CLUSTER_FACTS region %s: namespaceFirewalls[%s]: %q is neither a security group ID nor a network tag", region, namespace, firewall)
			}
		}
		if len(fact.NamespaceFirewalls) > 0 {
			firewalls[region] = fact.NamespaceFirewalls
		}
	}
	return firewalls, nil
}

// namespaceFirewall returns the firewall a pod of namespace joins in region. It returns "" when the region lists no
// namespace firewalls, which leaves launches unchanged, and errNamespaceFirewallMissing when the region lists some
// but none for namespace.
func namespaceFirewall(firewalls map[string]string, region, namespace string) (string, error) {
	if len(firewalls) == 0 {
		return "", nil
	}
	if fw := firewalls[namespace]; fw != "" {
		return fw, nil
	}
	return "", fmt.Errorf("%w: region %s isolates namespaces but CLUSTER_FACTS regions.%s.namespaceFirewalls has no entry for namespace %q",
		errNamespaceFirewallMissing, region, region, namespace)
}

// recordNamespaceFirewallEvent posts a CloudVMNamespaceFirewallMissing warning on the pod when err is a launch the
// provider refused for lack of a namespace firewall; other errors post nothing.
func (s *Server) recordNamespaceFirewallEvent(ctx context.Context, spec *InstanceSpec, err error) {
	if !errors.Is(err, errNamespaceFirewallMissing) || spec == nil {
		return
	}
	s.mu.RLock()
	recorder := s.eventRecorder
	s.mu.RUnlock()
	if recorder == nil {
		return
	}
	msg := err.Error()
	if len(msg) > maxPodEventMessage {
		msg = msg[:maxPodEventMessage]
	}
	eventCtx, cancel := context.WithTimeout(ctx, podEventTimeout)
	defer cancel()
	if recErr := recorder.RecordPodEvent(eventCtx, spec.PodNamespace, spec.PodName, spec.PodUID, corev1.EventTypeWarning, "CloudVMNamespaceFirewallMissing", msg); recErr != nil {
		log.Printf("[cloudvm] Warning: %v", recErr)
	}
}
