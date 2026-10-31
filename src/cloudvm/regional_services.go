// Regional services: Services of the regional namespaces that stand in for another namespace's Service for the
// workers of one region, and the regional VMs that only the pods of those namespaces run on.
package main

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
)

const (
	// AnnotationRegionalServiceFor names, as namespace/name, the Service a Service of a regional namespace stands in
	// for.
	AnnotationRegionalServiceFor = "cloudvm.vk.io/regional-service-for"
	// LabelRegion is the region whose workers a stand-in Service serves.
	LabelRegion = "topology.kubernetes.io/region"
	// TaintRegional marks the NodeClaims of regional VMs, which run only the pods of the regional namespaces.
	TaintRegional = "cloudvm.vk.io/regional"

	standInIndex       = "stand-in"
	standInSourceIndex = "stand-in-source"
	// standInTTL keeps workers asking again soon, so they fall back to the real Service soon after a stand-in's last
	// endpoint goes away.
	standInTTL = 5
)

// RegionalServices finds the Services of regional namespaces that stand in for another Service.
type RegionalServices interface {
	// StandIn returns the stand-in Service for namespace/name in region.
	StandIn(namespace, name, region string) (ClusterService, bool)
	// StandsInFor reports whether a Service of standInNamespace stands in for namespace/name in some region.
	StandsInFor(standInNamespace, namespace, name string) bool
}

// ParseRegionalNamespaces parses the comma-separated REGIONAL_NAMESPACES list.
func ParseRegionalNamespaces(raw string) (map[string]bool, error) {
	set := map[string]bool{}
	for _, ns := range strings.Split(raw, ",") {
		ns = strings.TrimSpace(ns)
		if ns == "" {
			continue
		}
		if !namespaceNamePattern.MatchString(ns) {
			return nil, fmt.Errorf("REGIONAL_NAMESPACES: %q is not a namespace name", ns)
		}
		set[ns] = true
	}
	return set, nil
}

// standInOf returns the namespace/name target and region of a kept stand-in Service.
func standInOf(obj any) (target, region string, ok bool) {
	svc, isService := obj.(*corev1.Service)
	if !isService {
		return "", "", false
	}
	target, region = svc.Annotations[AnnotationRegionalServiceFor], svc.Labels[LabelRegion]
	namespace, name, cut := strings.Cut(target, "/")
	if !cut || namespace == "" || name == "" || strings.Contains(name, "/") || region == "" {
		return "", "", false
	}
	return target, region, true
}

// StandIn returns the oldest-named stand-in for namespace/name in region; a second stand-in for the same Service and
// region is ignored.
func (w *ServiceWatch) StandIn(namespace, name, region string) (ClusterService, bool) {
	objs, err := w.informer.GetIndexer().ByIndex(standInIndex, namespace+"/"+name+"|"+region)
	if err != nil || len(objs) == 0 {
		return ClusterService{}, false
	}
	var best *corev1.Service
	for _, obj := range objs {
		if svc, ok := obj.(*corev1.Service); ok && (best == nil || svc.Namespace+"/"+svc.Name < best.Namespace+"/"+best.Name) {
			best = svc
		}
	}
	if best == nil {
		return ClusterService{}, false
	}
	return ClusterService{Namespace: best.Namespace, Name: best.Name}, true
}

// StandsInFor reports whether a Service of standInNamespace stands in for namespace/name.
func (w *ServiceWatch) StandsInFor(standInNamespace, namespace, name string) bool {
	objs, err := w.informer.GetIndexer().ByIndex(standInSourceIndex, standInNamespace+"|"+namespace+"/"+name)
	return err == nil && len(objs) > 0
}
