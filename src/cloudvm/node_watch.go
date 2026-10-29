// Caches the region label of every node through a shared informer so pods launch in the region of the node they bind
// to, and remembers which nodes the node agent serves so their pods never launch a VM of their own.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
)

// regionLabel is the well-known node label that names the region a node runs in.
const regionLabel = "topology.kubernetes.io/region"

// NodeRegionLookup reports the region a node's labels name.
type NodeRegionLookup interface {
	// NodeRegion returns the region label of the node; empty when the node is unknown or carries no label.
	NodeRegion(nodeName string) string
}

// NodeAgentLookup reports whether the node agent serves a node, which makes it the Node of a NodeClaim.
type NodeAgentLookup interface {
	ServedByNodeAgent(nodeName string) bool
}

// NodeWatch caches node names and region labels. It needs get, list, and watch on nodes.
type NodeWatch struct {
	informer cache.SharedIndexInformer
	factory  informers.SharedInformerFactory
}

// NewNodeWatch creates a watch backed by client. Only the name, the region label and the node agent label stay in the
// cache.
func NewNodeWatch(client kubernetes.Interface) *NodeWatch {
	factory := informers.NewSharedInformerFactory(client, 0)
	inf := factory.Core().V1().Nodes().Informer()
	_ = inf.SetTransform(func(obj any) (any, error) {
		if node, ok := obj.(*corev1.Node); ok {
			trimmed := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: node.Name}}
			for _, key := range []string{regionLabel, nodeclaim.LabelNodeAgent} {
				if v := node.Labels[key]; v != "" {
					if trimmed.Labels == nil {
						trimmed.Labels = map[string]string{}
					}
					trimmed.Labels[key] = v
				}
			}
			return trimmed, nil
		}
		return obj, nil
	})
	return &NodeWatch{informer: inf, factory: factory}
}

// NewInClusterNodeWatch creates a watch from the in-cluster service account.
func NewInClusterNodeWatch() (*NodeWatch, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to load in-cluster config: %w", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to create kubernetes client: %w", err)
	}
	return NewNodeWatch(cs), nil
}

// Start runs the informer until ctx is cancelled and waits up to wait for its first full list. On timeout, such as
// when the service account cannot list nodes, it logs a warning and returns false; the informer keeps retrying, and
// NodeRegion answers only once the list has succeeded.
func (w *NodeWatch) Start(ctx context.Context, wait time.Duration) bool {
	w.factory.Start(ctx.Done())
	waitCtx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	if cache.WaitForCacheSync(waitCtx.Done(), w.informer.HasSynced) {
		return true
	}
	log.Printf("[cloudvm] Warning: node watch not synced after %s, so node region labels are ignored until it syncs; check that the service account can get, list, and watch nodes", wait)
	return false
}

// NodeRegion returns the region label of the cached node, or an empty string.
func (w *NodeWatch) NodeRegion(nodeName string) string {
	if !w.informer.HasSynced() {
		return ""
	}
	obj, exists, err := w.informer.GetStore().GetByKey(nodeName)
	if err != nil || !exists {
		return ""
	}
	if node, ok := obj.(*corev1.Node); ok {
		return node.Labels[regionLabel]
	}
	return ""
}

// ServedByNodeAgent reports whether the cached node carries the node agent label.
func (w *NodeWatch) ServedByNodeAgent(nodeName string) bool {
	if !w.informer.HasSynced() {
		return false
	}
	obj, exists, err := w.informer.GetStore().GetByKey(nodeName)
	if err != nil || !exists {
		return false
	}
	node, ok := obj.(*corev1.Node)
	return ok && node.Labels[nodeclaim.LabelNodeAgent] == "true"
}
