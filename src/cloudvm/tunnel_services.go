// Kubernetes Service watch: a shared informer that tells the tunnel gateway which Service owns a ClusterIP, so worker
// streams reach Services of their own namespace and the shared Services the plugin names.
package main

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const serviceIPIndex = "service-ip"

// ClusterService is the Service that holds a ClusterIP.
type ClusterService struct {
	Namespace string
	Name      string
	// Ports are the TCP ports the Service exposes.
	Ports []int32
}

// exposes reports whether the Service exposes TCP port.
func (s ClusterService) exposes(port int) bool {
	for _, p := range s.Ports {
		if int(p) == port {
			return true
		}
	}
	return false
}

// ClusterServices maps a Service ClusterIP to its Service.
type ClusterServices interface {
	// HasSynced reports whether the mapping covers every Service. The gateway refuses lookups before it does.
	HasSynced() bool
	// ServiceByIP returns the Service holding ip.
	ServiceByIP(ip string) (ClusterService, bool)
}

// ServiceWatch caches the ClusterIPs and TCP ports of all Services in the cluster through a shared informer.
type ServiceWatch struct {
	informer cache.SharedIndexInformer
	factory  informers.SharedInformerFactory
}

// NewServiceWatch creates a watch backed by client that keeps only what decides who owns a ClusterIP.
func NewServiceWatch(client kubernetes.Interface) *ServiceWatch {
	factory := informers.NewSharedInformerFactory(client, 0)
	inf := factory.Core().V1().Services().Informer()
	_ = inf.SetTransform(func(obj any) (any, error) {
		if svc, ok := obj.(*corev1.Service); ok {
			return &corev1.Service{
				ObjectMeta: metav1.ObjectMeta{Name: svc.Name, Namespace: svc.Namespace},
				Spec:       corev1.ServiceSpec{ClusterIPs: serviceClusterIPs(svc), Ports: svc.Spec.Ports},
			}, nil
		}
		return obj, nil
	})
	_ = inf.AddIndexers(cache.Indexers{serviceIPIndex: func(obj any) ([]string, error) {
		if svc, ok := obj.(*corev1.Service); ok {
			return serviceClusterIPs(svc), nil
		}
		return nil, nil
	}})
	return &ServiceWatch{informer: inf, factory: factory}
}

// NewInClusterServiceWatch creates a watch from the in-cluster service account.
func NewInClusterServiceWatch() (*ServiceWatch, error) {
	cfg, err := rest.InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to load in-cluster config: %w", err)
	}
	cs, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("unable to create kubernetes client: %w", err)
	}
	return NewServiceWatch(cs), nil
}

// Start runs the informer until done closes. It returns immediately.
func (w *ServiceWatch) Start(done <-chan struct{}) {
	w.factory.Start(done)
}

// HasSynced reports whether the informer completed its initial list.
func (w *ServiceWatch) HasSynced() bool {
	return w.informer.HasSynced()
}

// ServiceByIP returns the Service whose ClusterIPs include ip.
func (w *ServiceWatch) ServiceByIP(ip string) (ClusterService, bool) {
	objs, err := w.informer.GetIndexer().ByIndex(serviceIPIndex, ip)
	if err != nil || len(objs) != 1 {
		return ClusterService{}, false
	}
	svc, ok := objs[0].(*corev1.Service)
	if !ok {
		return ClusterService{}, false
	}
	found := ClusterService{Namespace: svc.Namespace, Name: svc.Name}
	for _, p := range svc.Spec.Ports {
		if p.Protocol == "" || p.Protocol == corev1.ProtocolTCP {
			found.Ports = append(found.Ports, p.Port)
		}
	}
	return found, true
}

// serviceClusterIPs returns the virtual IPs of a Service; headless and ExternalName Services have none.
func serviceClusterIPs(svc *corev1.Service) []string {
	ips := svc.Spec.ClusterIPs
	if len(ips) == 0 && svc.Spec.ClusterIP != "" {
		ips = []string{svc.Spec.ClusterIP}
	}
	var out []string
	for _, ip := range ips {
		if ip != "" && ip != corev1.ClusterIPNone {
			out = append(out, ip)
		}
	}
	return out
}

// ParseSharedServices parses a comma-separated list of namespace/name Services that workers of every namespace may
// reach.
func ParseSharedServices(raw string) (map[string]bool, error) {
	shared := make(map[string]bool)
	for _, entry := range strings.Split(raw, ",") {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		namespace, name, ok := strings.Cut(entry, "/")
		if !ok || namespace == "" || name == "" || strings.Contains(name, "/") {
			return nil, fmt.Errorf("invalid shared service %q: want namespace/name", entry)
		}
		shared[entry] = true
	}
	return shared, nil
}
