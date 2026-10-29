// Runs this provider and a stub "other" provider as two full Karpenter controller sets against one envtest API
// server, and checks that each launches only for its own NodePools and never touches the other's NodeClaims.

// Package integration holds envtest-based tests that run Karpenter core controllers with the CloudVM provider.
package integration

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/awslabs/operatorpkg/object"
	"github.com/awslabs/operatorpkg/status"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	ctrlconfig "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	karpapis "sigs.k8s.io/karpenter/pkg/apis"
	karpv1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	karpcp "sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers"
	"sigs.k8s.io/karpenter/pkg/controllers/nodeoverlay"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/prediction"
	"sigs.k8s.io/karpenter/pkg/test"
	testv1alpha1 "sigs.k8s.io/karpenter/pkg/test/v1alpha1"

	"github.com/simonepri/interlink-cloudvm-plugin/karpenter/apis/v1alpha1"
	"github.com/simonepri/interlink-cloudvm-plugin/karpenter/cloudprovider"
	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
)

const (
	cloudVMTaintKey     = "virtual-node.interlink/provider"
	partitionLabel      = "capacity-provider"
	partitionCloudVM    = "cloudvm"
	settleWindow        = 15 * time.Second // longer than Karpenter's 10 s maximum provisioning batch
	provisioningTimeout = 60 * time.Second
)

// startControllers registers one full Karpenter controller set for a provider on its own manager.
func startControllers(ctx context.Context, t *testing.T, cfg *rest.Config, name string, cp karpcp.CloudProvider) {
	t.Helper()
	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme.Scheme,
		Logger:                 log.FromContext(ctx).WithName(name),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		LeaderElection:         false,
		Controller:             ctrlconfig.Controller{SkipNameValidation: new(true)},
		BaseContext:            func() context.Context { return ctx },
	})
	if err != nil {
		t.Fatal(err)
	}
	setupIndexers(ctx, t, mgr)
	clk := clock.RealClock{}
	kube := mgr.GetClient()
	cluster := state.NewCluster(clk, kube, cp)
	// Each controller set registers its metrics at construction; in production the two sets are separate processes.
	crmetrics.Registry = prometheus.NewRegistry()
	for _, c := range controllers.NewControllers(ctx, mgr, clk, kube, events.NewRecorder(mgr.GetEventRecorderFor(name)), //nolint:staticcheck // the recorder type Karpenter core v1.15 accepts
		cp, cp, cluster, nodeoverlay.NewInstanceTypeStore(), prediction.NewStore()) {
		if err := c.Register(ctx, mgr); err != nil {
			t.Fatal(err)
		}
	}
	go func() {
		if err := mgr.Start(ctx); err != nil {
			t.Errorf("%s manager: %v", name, err)
		}
	}()
}

// setupIndexers mirrors the field indexes Karpenter's operator registers before starting controllers.
func setupIndexers(ctx context.Context, t *testing.T, mgr manager.Manager) {
	t.Helper()
	idx := mgr.GetFieldIndexer()
	must := func(err error) {
		if err != nil {
			t.Fatal(err)
		}
	}
	must(idx.IndexField(ctx, &corev1.Pod{}, "spec.nodeName", func(o client.Object) []string { return []string{o.(*corev1.Pod).Spec.NodeName} }))
	must(idx.IndexField(ctx, &corev1.Node{}, "spec.providerID", func(o client.Object) []string { return []string{o.(*corev1.Node).Spec.ProviderID} }))
	must(idx.IndexField(ctx, &storagev1.VolumeAttachment{}, "spec.nodeName", func(o client.Object) []string {
		return []string{o.(*storagev1.VolumeAttachment).Spec.NodeName}
	}))
	must(idx.IndexField(ctx, &karpv1.NodeClaim{}, "status.providerID", func(o client.Object) []string { return []string{o.(*karpv1.NodeClaim).Status.ProviderID} }))
	ref := func(get func(*karpv1.NodeClassReference) string) func(*karpv1.NodeClassReference) []string {
		return func(r *karpv1.NodeClassReference) []string {
			if r == nil {
				return nil
			}
			return []string{get(r)}
		}
	}
	for field, get := range map[string]func(*karpv1.NodeClassReference) []string{
		"group": ref(func(r *karpv1.NodeClassReference) string { return r.Group }),
		"kind":  ref(func(r *karpv1.NodeClassReference) string { return r.Kind }),
		"name":  ref(func(r *karpv1.NodeClassReference) string { return r.Name }),
	} {
		must(idx.IndexField(ctx, &karpv1.NodeClaim{}, "spec.nodeClassRef."+field, func(o client.Object) []string { return get(o.(*karpv1.NodeClaim).Spec.NodeClassRef) }))
		must(idx.IndexField(ctx, &karpv1.NodePool{}, "spec.template.spec.nodeClassRef."+field, func(o client.Object) []string {
			return get(o.(*karpv1.NodePool).Spec.Template.Spec.NodeClassRef)
		}))
	}
}

func cloudVMNodeClassCRD(t *testing.T) *apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join("..", "..", "charts", "cloudvm-karpenter", "crds", "karpenter.cloudvm.vk.io_cloudvmnodeclasses.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	return object.Unmarshal[apiextensionsv1.CustomResourceDefinition](raw)
}

func nodePool(name string, ref *karpv1.NodeClassReference, labels map[string]string, taints []corev1.Taint) *karpv1.NodePool {
	return &karpv1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: karpv1.NodePoolSpec{Template: karpv1.NodeClaimTemplate{
			ObjectMeta: karpv1.ObjectMeta{Labels: labels},
			Spec: karpv1.NodeClaimTemplateSpec{
				NodeClassRef: ref,
				Taints:       taints,
				Requirements: []karpv1.NodeSelectorRequirementWithMinValues{{
					Key: corev1.LabelOSStable, Operator: corev1.NodeSelectorOpIn, Values: []string{string(corev1.Linux)},
				}},
				ExpireAfter: karpv1.MustParseNillableDuration("Never"),
			},
		}},
	}
}

// pendingPod creates a pod and marks it unschedulable, as kube-scheduler would, so both provisioners see it.
func pendingPod(ctx context.Context, t *testing.T, kube client.Client, name string, mutate func(*corev1.Pod)) {
	t.Helper()
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: corev1.PodSpec{Containers: []corev1.Container{{
			Name: "main", Image: "registry.example.com/app:1",
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}},
		}}},
	}
	mutate(pod)
	if err := kube.Create(ctx, pod); err != nil {
		t.Fatal(err)
	}
	pod.Status.Conditions = []corev1.PodCondition{{
		Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: corev1.PodReasonUnschedulable, LastTransitionTime: metav1.Now(),
	}}
	if err := kube.Status().Update(ctx, pod); err != nil {
		t.Fatal(err)
	}
}

func nodeClaimsFor(ctx context.Context, t *testing.T, kube client.Client, kind string) []karpv1.NodeClaim {
	t.Helper()
	list := &karpv1.NodeClaimList{}
	if err := kube.List(ctx, list); err != nil {
		t.Fatal(err)
	}
	return lo.Filter(list.Items, func(nc karpv1.NodeClaim, _ int) bool { return nc.Spec.NodeClassRef.Kind == kind })
}

// recordingProvider records the NodeClass kind of every NodeClaim the stub provider is asked to create or delete.
type recordingProvider struct {
	*fake.CloudProvider

	mu      sync.Mutex
	creates []string
	deletes []string
}

func (r *recordingProvider) Create(ctx context.Context, nc *karpv1.NodeClaim) (*karpv1.NodeClaim, error) {
	r.mu.Lock()
	r.creates = append(r.creates, nc.Spec.NodeClassRef.Kind)
	r.mu.Unlock()
	return r.CloudProvider.Create(ctx, nc)
}

func (r *recordingProvider) Delete(ctx context.Context, nc *karpv1.NodeClaim) error {
	r.mu.Lock()
	r.deletes = append(r.deletes, nc.Name)
	r.mu.Unlock()
	return r.CloudProvider.Delete(ctx, nc)
}

func (r *recordingProvider) calls() (creates, deletes []string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return slices.Clone(r.creates), slices.Clone(r.deletes)
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(provisioningTimeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestTwoProvidersPartitionNodePoolsAndNodeClaims(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		if os.Getenv("CI") != "" {
			t.Fatal("KUBEBUILDER_ASSETS must point at envtest binaries in CI")
		}
		t.Skip("KUBEBUILDER_ASSETS is unset; run through `mise run test`")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	ctx = log.IntoContext(ctx, zap.New(zap.UseDevMode(true), zap.Level(zap.Options{}.Level)))
	ctx = options.ToContext(ctx, test.Options(test.OptionsFields{BatchIdleDuration: new(time.Second), BatchMaxDuration: new(5 * time.Second)}))

	crds := append(append(append([]*apiextensionsv1.CustomResourceDefinition{}, karpapis.CRDs...), testv1alpha1.CRDs...), cloudVMNodeClassCRD(t))
	env := &envtest.Environment{CRDs: crds, ErrorIfCRDPathMissing: true}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cancel()
		_ = env.Stop()
	})
	kube, err := client.New(cfg, client.Options{Scheme: scheme.Scheme})
	if err != nil {
		t.Fatal(err)
	}

	plugin := newPluginServer(gpuCatalog())
	t.Cleanup(plugin.Close)
	providerCfg, err := cloudprovider.LoadConfig("")
	if err != nil {
		t.Fatal(err)
	}
	providerCfg.GPUClasses = []cloudprovider.GPUClass{{Class: "l4", Models: []string{"L4 24GB"}}}
	cloudVM := cloudprovider.New(kube, nodeclaim.NewClient(plugin.URL, nil), providerCfg, clock.RealClock{})
	other := &recordingProvider{CloudProvider: fake.NewCloudProvider()}

	// The CloudVMNodeClass must start Ready from its CRD default; the stub's class is made Ready explicitly.
	cvmClass := &v1alpha1.CloudVMNodeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu"},
		Spec:       v1alpha1.CloudVMNodeClassSpec{Footprint: []v1alpha1.FootprintRegion{{Provider: "aws", Region: "us-east-1"}, {Provider: "aws", Region: "eu-west-1"}}},
	}
	if err := kube.Create(ctx, cvmClass); err != nil {
		t.Fatal(err)
	}
	stored := &v1alpha1.CloudVMNodeClass{}
	if err := kube.Get(ctx, client.ObjectKeyFromObject(cvmClass), stored); err != nil {
		t.Fatal(err)
	}
	if !stored.StatusConditions().Root().IsTrue() {
		t.Fatalf("CloudVMNodeClass is not Ready on creation: %+v", stored.Status.Conditions)
	}
	otherClass := &testv1alpha1.TestNodeClass{ObjectMeta: metav1.ObjectMeta{Name: "default"}}
	otherKind := object.GVK(otherClass).Kind
	if err := kube.Create(ctx, otherClass); err != nil {
		t.Fatal(err)
	}
	otherClass.StatusConditions().SetTrue(status.ConditionReady)
	if err := kube.Status().Update(ctx, otherClass); err != nil {
		t.Fatal(err)
	}

	cloudVMTaint := corev1.Taint{Key: cloudVMTaintKey, Value: partitionCloudVM, Effect: corev1.TaintEffectNoSchedule}
	pools := []*karpv1.NodePool{
		nodePool("cloudvm-gpu", &karpv1.NodeClassReference{Group: v1alpha1.Group, Kind: "CloudVMNodeClass", Name: "gpu"},
			map[string]string{partitionLabel: partitionCloudVM}, []corev1.Taint{cloudVMTaint}),
		nodePool("cluster-default", &karpv1.NodeClassReference{Group: object.GVK(otherClass).Group, Kind: object.GVK(otherClass).Kind, Name: "default"}, nil, nil),
	}
	for _, np := range pools {
		if err := kube.Create(ctx, np); err != nil {
			t.Fatal(err)
		}
	}

	startControllers(ctx, t, cfg, "cloudvm", cloudVM)
	startControllers(ctx, t, cfg, "other", other)

	// A GPU pod that only the CloudVM pool can serve: it selects the partition label and tolerates the taint.
	pendingPod(ctx, t, kube, "cloudvm-gpu-pod", func(p *corev1.Pod) {
		p.Spec.NodeSelector = map[string]string{partitionLabel: partitionCloudVM, cloudprovider.LabelGPUClass: "l4"}
		p.Spec.Tolerations = []corev1.Toleration{{Key: cloudVMTaintKey, Operator: corev1.TolerationOpEqual, Value: partitionCloudVM, Effect: corev1.TaintEffectNoSchedule}}
		p.Spec.Containers[0].Resources.Requests[nodeclaim.ResourceGPU] = resource.MustParse("1")
		p.Spec.Containers[0].Resources.Limits = corev1.ResourceList{nodeclaim.ResourceGPU: resource.MustParse("1")}
	})
	// A plain pod that only the other provider's pool can serve.
	pendingPod(ctx, t, kube, "cluster-pod", func(*corev1.Pod) {})

	waitFor(t, "a CloudVM NodeClaim to launch", func() bool { return len(plugin.launches()) > 0 })
	waitFor(t, "an other-provider NodeClaim", func() bool { return len(nodeClaimsFor(ctx, t, kube, otherKind)) > 0 })
	time.Sleep(settleWindow)

	cvmClaims := nodeClaimsFor(ctx, t, kube, "CloudVMNodeClass")
	if len(cvmClaims) != 1 {
		t.Fatalf("got %d CloudVM NodeClaims, want exactly 1", len(cvmClaims))
	}
	if got := plugin.launches(); len(got) != 1 || got[0] != cvmClaims[0].Name {
		t.Fatalf("plugin launched %v, want only %s", got, cvmClaims[0].Name)
	}
	launched := cvmClaims[0]
	if launched.Labels[corev1.LabelTopologyRegion] != "eu-west-1" || launched.Labels[cloudprovider.LabelGPUClass] != "l4" {
		t.Errorf("CloudVM NodeClaim labels = %v, want eu-west-1 (cheapest) and gpu-class l4", launched.Labels)
	}
	if launched.Status.ProviderID == "" {
		t.Error("CloudVM NodeClaim has no providerID")
	}

	otherClaims := nodeClaimsFor(ctx, t, kube, otherKind)
	if len(otherClaims) != 1 {
		t.Fatalf("got %d other-provider NodeClaims, want exactly 1", len(otherClaims))
	}
	otherCreates, otherDeletes := other.calls()
	for _, kind := range otherCreates {
		if kind != otherKind {
			t.Errorf("other provider was asked to create a %s NodeClaim", kind)
		}
	}
	if len(otherDeletes) != 0 {
		t.Errorf("other provider received deletes %v", otherDeletes)
	}
	if got := plugin.deleted(); len(got) != 0 {
		t.Errorf("plugin received deletes %v", got)
	}
	for _, c := range plugin.launches() {
		if c == otherClaims[0].Name {
			t.Errorf("plugin launched the other provider's NodeClaim %s", c)
		}
	}

	// Act as the node agent: register a Node for the CloudVM claim, then delete the claim and expect only this
	// provider to terminate it.
	registerNode(ctx, t, kube, &launched)
	waitFor(t, "the CloudVM NodeClaim to register", func() bool {
		nc := &karpv1.NodeClaim{}
		return kube.Get(ctx, client.ObjectKeyFromObject(&launched), nc) == nil && nc.StatusConditions().Get(karpv1.ConditionTypeRegistered).IsTrue()
	})
	if err := kube.Delete(ctx, &launched); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the plugin to terminate the CloudVM VM", func() bool { return slices.Contains(plugin.deleted(), launched.Name) })
	if _, deletes := other.calls(); len(deletes) != 0 {
		t.Errorf("other provider received deletes %v", deletes)
	}
	if got := nodeClaimsFor(ctx, t, kube, otherKind); len(got) != 1 || got[0].DeletionTimestamp != nil {
		t.Errorf("other provider's NodeClaim was disturbed: %d left", len(got))
	}
}

// registerNode creates the Node the node agent would create for a claim, Ready and carrying the unregistered taint.
func registerNode(ctx context.Context, t *testing.T, kube client.Client, nc *karpv1.NodeClaim) {
	t.Helper()
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: nc.Name, Labels: nc.Labels},
		Spec:       corev1.NodeSpec{ProviderID: nc.Status.ProviderID, Taints: []corev1.Taint{karpv1.UnregisteredNoExecuteTaint}},
	}
	if err := kube.Create(ctx, node); err != nil {
		t.Fatal(err)
	}
	node.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue, Reason: "AgentReady", LastHeartbeatTime: metav1.Now(), LastTransitionTime: metav1.Now()}}
	node.Status.Capacity = nc.Status.Capacity
	node.Status.Allocatable = nc.Status.Allocatable
	if err := kube.Status().Update(ctx, node); err != nil {
		t.Fatal(err)
	}
}
