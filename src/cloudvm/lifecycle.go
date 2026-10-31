// Records how pods and VMs start as traces, stage-duration histograms and pod events: a pod's trace runs from its
// creation through its image pull to its running container, and a VM's stages from launch request to a connected worker.

package main

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"slices"
	"sync"
	"time"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim/lifecycletrace"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	corev1 "k8s.io/api/core/v1"
)

// lifecycleTracerName is the instrumentation scope of the plugin's lifecycle spans.
const lifecycleTracerName = "github.com/simonepri/interlink-cloudvm-plugin/cloudvm"

// Pod stages: each ends where the next begins, and stageReady spans them all.
const (
	stagePending        = "pending"         // pod created -> the plugin accepted its create
	stageRunnerWait     = "runner_wait"     // create accepted -> the VM's runner accepted the container
	stageImagePull      = "image_pull"      // the runner pulled the image
	stageContainerStart = "container_start" // image pulled -> container running
	stageStatusReport   = "status_report"   // container running -> the plugin saw it running
	stageReady          = "ready"           // pod: created -> running; VM: launch requested -> worker connected
)

// VM stages besides the boot phases the worker reports.
const (
	stageLaunch        = "launch"         // the cloud launch call
	stageTunnelConnect = "tunnel_connect" // the worker's last boot phase -> its tunnel handshake
)

// Lifecycle bounds: records idle this long are dropped, and a worker reports at most maxBootMarks boot phases.
const (
	lifecycleRecordTTL = 6 * time.Hour
	lifecycleGCPeriod  = 10 * time.Minute
	maxBootMarks       = 16
)

// bootPhasePattern is the shape of a boot phase name a worker reports, which becomes a span name and a label value.
var bootPhasePattern = regexp.MustCompile(`^[a-z][a-z0-9_]{0,31}$`)

// Lifecycle follows the pods and VMs of this plugin process. A pod or VM it first sees after a restart records only
// the stages that end after it was seen.
type Lifecycle struct {
	metrics *LifecycleMetrics
	events  func() PodEventRecorder
	now     func() time.Time

	mu     sync.Mutex
	tracer trace.Tracer
	pods   map[string]*podLifecycle // pod UID ->
	vms    map[string]*vmLifecycle  // instance ID ->
	lastGC time.Time
}

// podLifecycle is what the plugin knows of one pod's start.
type podLifecycle struct {
	trace                lifecycletrace.Trace
	uid, namespace, name string
	container, image     string
	claim                string
	createdAt            time.Time
	acceptedAt           time.Time
	offering             lifecycleOffering
	instanceID           string
	seen                 time.Time

	runnerAccepted, pullStarted, pullFailed, pulled, started, done bool
}

// vmLifecycle is what the plugin knows of one VM's start.
type vmLifecycle struct {
	trace       lifecycletrace.Trace
	claim       string
	offering    lifecycleOffering
	attrs       []attribute.KeyValue
	requestedAt time.Time
	launchedAt  time.Time
	connected   bool
	seen        time.Time
}

// NewLifecycle returns a Lifecycle that records spans nowhere until SetTracerProvider and posts pod events through
// the recorder events returns, when it returns one.
func NewLifecycle(metrics *LifecycleMetrics, events func() PodEventRecorder) *Lifecycle {
	return &Lifecycle{
		metrics: metrics,
		events:  events,
		now:     time.Now,
		tracer:  noop.NewTracerProvider().Tracer(lifecycleTracerName),
		pods:    make(map[string]*podLifecycle),
		vms:     make(map[string]*vmLifecycle),
	}
}

// SetTracerProvider records lifecycle spans through tp.
func (l *Lifecycle) SetTracerProvider(tp trace.TracerProvider) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.tracer = tp.Tracer(lifecycleTracerName)
}

// PodAccepted starts following a pod whose create the plugin accepted; claim names the NodeClaim whose Node the pod
// is bound to, if any, and node is that claim's VM when the plugin knows it.
func (l *Lifecycle) PodAccepted(pod *Pod, spec *InstanceSpec, claim string, node *CloudVMNode) {
	if spec.PodUID == "" {
		return
	}
	now := l.now()
	p := &podLifecycle{
		trace:      lifecycletrace.ForPod(spec.PodUID),
		uid:        spec.PodUID,
		namespace:  spec.PodNamespace,
		name:       spec.PodName,
		image:      spec.TargetContainerImage,
		container:  defaultContainerName,
		claim:      claim,
		createdAt:  pod.Metadata.CreationTimestamp,
		acceptedAt: now,
		seen:       now,
		offering:   lifecycleOffering{provider: spec.Provider, region: spec.Region, instanceType: spec.InstanceType, capacityType: capacityType(spec.MarketType)},
	}
	if node != nil {
		p.offering, p.instanceID = nodeOffering(node), node.InstanceID
	}
	if len(pod.Spec.Containers) > 0 && pod.Spec.Containers[0].Name != "" {
		p.container = pod.Spec.Containers[0].Name
	}
	l.mu.Lock()
	l.gcLocked(now)
	if _, ok := l.pods[p.uid]; ok {
		l.mu.Unlock()
		return
	}
	l.pods[p.uid] = p
	tracer := l.tracer
	l.mu.Unlock()
	if !p.createdAt.IsZero() {
		l.podStage(tracer, p, stagePending, p.createdAt, now)
	}
}

// RunnerStatus advances a pod's lifecycle from one status its VM's runner reported.
func (l *Lifecycle) RunnerStatus(node *CloudVMNode, uid string, res *ContainerStatusResult) {
	if node == nil || uid == "" || res == nil {
		return
	}
	now := l.now()
	l.mu.Lock()
	p, ok := l.pods[uid]
	if !ok {
		// A pod created before this process started: what already happened is not recorded again.
		p = &podLifecycle{trace: lifecycletrace.ForPod(uid), uid: uid, container: defaultContainerName}
		l.pods[uid] = p
		p.seen = now
		p.offering, p.instanceID = nodeOffering(node), node.InstanceID
		l.advanceLocked(p, res, now)
		l.mu.Unlock()
		return
	}
	p.seen = now
	if p.instanceID == "" {
		p.offering, p.instanceID = nodeOffering(node), node.InstanceID
	}
	if p.claim == "" {
		p.claim = node.ClaimName
	}
	tracer := l.tracer
	work := l.advanceLocked(p, res, now)
	snapshot := *p
	l.mu.Unlock()
	for _, w := range work {
		w(tracer, &snapshot)
	}
}

// lifecycleWork is a span, metric or event a status transition produced, run outside the lock.
type lifecycleWork func(trace.Tracer, *podLifecycle)

// advanceLocked marks the stages res shows as done and returns what records them. A pod adopted after a restart
// (no acceptedAt) only has its flags set.
func (l *Lifecycle) advanceLocked(p *podLifecycle, res *ContainerStatusResult, now time.Time) []lifecycleWork {
	adopted := p.acceptedAt.IsZero()
	tm := res.Timings
	if tm == nil {
		tm = &RunnerTimings{}
	}
	if tm.Image != "" {
		p.image = tm.Image
	}
	var work []lifecycleWork
	add := func(w lifecycleWork) {
		if !adopted {
			work = append(work, w)
		}
	}
	if !p.runnerAccepted && !tm.AcceptedAt.IsZero() {
		p.runnerAccepted = true
		add(func(tr trace.Tracer, p *podLifecycle) {
			l.podStage(tr, p, stageRunnerWait, p.acceptedAt, tm.AcceptedAt)
		})
	}
	if !p.pullStarted && !tm.PullStartedAt.IsZero() {
		p.pullStarted = true
		add(func(_ trace.Tracer, p *podLifecycle) {
			l.event(p, corev1.EventTypeNormal, "Pulling", fmt.Sprintf("Pulling image %q", p.image))
		})
	}
	if !p.pullFailed && !p.pulled && (res.Reason == "ErrImagePull" || res.Reason == "ImagePullBackOff") {
		p.pullFailed = true
		msg := res.Message
		add(func(_ trace.Tracer, p *podLifecycle) {
			l.event(p, corev1.EventTypeWarning, "Failed", fmt.Sprintf("Failed to pull image %q: %s", p.image, msg))
		})
	}
	if !p.pulled && !tm.PullFinishedAt.IsZero() {
		p.pulled = true
		add(func(tr trace.Tracer, p *podLifecycle) {
			attrs := []attribute.KeyValue{
				attribute.String("container.image.name", p.image),
				attribute.Int("cloudvm.pull.attempts", tm.PullAttempts),
				attribute.Bool("cloudvm.pull.lazy", tm.LazyPull),
			}
			l.podStage(tr, p, stageImagePull, tm.PullStartedAt, tm.PullFinishedAt, attrs...)
			l.event(p, corev1.EventTypeNormal, "Pulled", fmt.Sprintf("Successfully pulled image %q in %s", p.image, tm.PullFinishedAt.Sub(tm.PullStartedAt).Round(time.Millisecond)))
		})
	}
	started := res.Running || (!res.StartedAt.IsZero() && res.StartedAt.Year() > 1)
	if !p.started && started {
		p.started, p.done = true, true
		startedAt := res.StartedAt
		add(func(tr trace.Tracer, p *podLifecycle) {
			from := tm.PullFinishedAt
			if from.IsZero() {
				from = tm.AcceptedAt
			}
			if !from.IsZero() {
				l.podStage(tr, p, stageContainerStart, from, startedAt)
			}
			l.podStage(tr, p, stageStatusReport, startedAt, now)
			l.event(p, corev1.EventTypeNormal, "Created", "Created container: "+p.container)
			l.event(p, corev1.EventTypeNormal, "Started", "Started container "+p.container)
			l.podRoot(tr, p, now, nil)
		})
	}
	if !p.done && !started && (res.Reason == "StartError" || res.Status == "dead" || res.Status == "exited") {
		p.done = true
		cause := fmt.Errorf("%s: %s", res.Reason, res.Message)
		add(func(tr trace.Tracer, p *podLifecycle) { l.podRoot(tr, p, now, cause) })
	}
	return work
}

// PodDeleted stops following a pod, recording its trace's root when the pod never ran.
func (l *Lifecycle) PodDeleted(uid string) {
	l.mu.Lock()
	p, ok := l.pods[uid]
	delete(l.pods, uid)
	tracer := l.tracer
	l.mu.Unlock()
	if ok && !p.done && !p.acceptedAt.IsZero() {
		l.podRoot(tracer, p, l.now(), fmt.Errorf("pod deleted before its container ran"))
	}
}

func (l *Lifecycle) podAttrs(p *podLifecycle) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.String("k8s.namespace.name", p.namespace),
		attribute.String("k8s.pod.name", p.name),
		attribute.String("k8s.pod.uid", p.uid),
	}
	if p.claim != "" {
		attrs = append(attrs, attribute.String("cloudvm.nodeclaim", p.claim))
	}
	if p.instanceID != "" {
		attrs = append(attrs, attribute.String("cloudvm.instance_id", p.instanceID))
	}
	return append(attrs, p.offering.attrs()...)
}

func (l *Lifecycle) podStage(tracer trace.Tracer, p *podLifecycle, stage string, start, end time.Time, extra ...attribute.KeyValue) {
	lifecycletrace.RecordStage(context.Background(), tracer, p.trace, "cloudvm.pod."+stage, start, end, nil, append(l.podAttrs(p), extra...))
	l.metrics.observePodStage(stage, p.offering, end.Sub(start).Seconds())
}

// podRoot records the root span of a pod's trace, from its creation, or from the accepted create when the payload
// had no creation time, until end.
func (l *Lifecycle) podRoot(tracer trace.Tracer, p *podLifecycle, end time.Time, err error) {
	start := p.createdAt
	if start.IsZero() {
		start = p.acceptedAt
	}
	var links []trace.Link
	if p.claim != "" {
		links = append(links, lifecycletrace.ForNodeClaim(p.claim).Link())
	}
	lifecycletrace.RecordRoot(context.Background(), tracer, p.trace, "cloudvm.pod", start, end, err, l.podAttrs(p), links...)
	if err == nil {
		l.metrics.observePodStage(stageReady, p.offering, end.Sub(start).Seconds())
	}
}

// event posts a pod event without delaying the status call that observed it.
func (l *Lifecycle) event(p *podLifecycle, eventType, reason, message string) {
	if l.events == nil || p.name == "" {
		return
	}
	recorder := l.events()
	if recorder == nil {
		return
	}
	if len(message) > maxPodEventMessage {
		message = message[:maxPodEventMessage]
	}
	ns, name, uid := p.namespace, p.name, p.uid
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), podEventTimeout)
		defer cancel()
		if err := recorder.RecordPodEvent(ctx, ns, name, uid, eventType, reason, message); err != nil {
			log.Printf("[cloudvm] Warning: %v", err)
		}
	}()
}

// VMLaunched records the cloud launch call of a VM whose stages belong to tr: the NodeClaim's trace for a claim VM,
// the pod's for a VM launched for one pod. A failed claim launch also ends the claim's trace.
func (l *Lifecycle) VMLaunched(tr lifecycletrace.Trace, spec *InstanceSpec, claim string, requestedAt time.Time, inst *Instance, err error) {
	now := l.now()
	vm := &vmLifecycle{trace: tr, claim: claim, requestedAt: requestedAt, launchedAt: now, seen: now}
	vm.offering = lifecycleOffering{provider: spec.Provider, region: spec.Region, instanceType: spec.InstanceType, capacityType: capacityType(spec.MarketType)}
	if inst != nil {
		vm.offering.provider, vm.offering.region = string(inst.Provider), inst.Region
		if inst.InstanceType != "" {
			vm.offering.instanceType = inst.InstanceType
		}
		vm.attrs = append(vm.attrs, attribute.String("cloudvm.instance_id", inst.ID))
		if zone := inst.Metadata["availabilityZone"]; zone != "" {
			vm.attrs = append(vm.attrs, attribute.String("cloudvm.zone", zone))
		}
	}
	if claim != "" {
		vm.attrs = append(vm.attrs, attribute.String("cloudvm.nodeclaim", claim))
	} else {
		vm.attrs = append(vm.attrs, attribute.String("k8s.namespace.name", spec.PodNamespace), attribute.String("k8s.pod.name", spec.PodName))
	}
	vm.attrs = append(vm.attrs, vm.offering.attrs()...)

	l.mu.Lock()
	l.gcLocked(now)
	if inst != nil && err == nil {
		l.vms[inst.ID] = vm
	}
	tracer := l.tracer
	l.mu.Unlock()
	l.vmStage(tracer, vm, stageLaunch, requestedAt, now, err)
	if err != nil && claim != "" {
		lifecycletrace.RecordRoot(context.Background(), tracer, tr, "cloudvm.nodeclaim", requestedAt, now, err, vm.attrs)
	}
}

// WorkerConnected records the boot phases a launched VM's worker reported in its first tunnel handshake and, for a
// claim VM, the root of the claim's trace, which ends when the claim's Node can turn Ready.
func (l *Lifecycle) WorkerConnected(req *TunnelHandshakeRequest) {
	now := l.now()
	l.mu.Lock()
	vm, ok := l.vms[req.InstanceID]
	if !ok || vm.connected {
		l.mu.Unlock()
		return
	}
	vm.connected, vm.seen = true, now
	tracer := l.tracer
	l.mu.Unlock()

	marks := req.BootMarks()
	if len(marks) > maxBootMarks {
		marks = marks[:maxBootMarks]
	}
	slices.SortStableFunc(marks, func(a, b BootMark) int { return a.At.Compare(b.At) })
	prev := vm.launchedAt
	for _, m := range marks {
		if !bootPhasePattern.MatchString(m.Phase) || m.At.IsZero() {
			continue
		}
		l.vmStage(tracer, vm, m.Phase, prev, m.At, nil)
		prev = m.At
	}
	l.vmStage(tracer, vm, stageTunnelConnect, prev, now, nil)
	l.metrics.observeVMStage(stageReady, vm.offering, now.Sub(vm.requestedAt).Seconds())
	if vm.claim != "" {
		lifecycletrace.RecordRoot(context.Background(), tracer, vm.trace, "cloudvm.nodeclaim", vm.requestedAt, now, nil, vm.attrs)
	}
}

func (l *Lifecycle) vmStage(tracer trace.Tracer, vm *vmLifecycle, stage string, start, end time.Time, err error) {
	lifecycletrace.RecordStage(context.Background(), tracer, vm.trace, "cloudvm.vm."+stage, start, end, err, vm.attrs)
	l.metrics.observeVMStage(stage, vm.offering, end.Sub(start).Seconds())
}

// LogTraceID returns the hex trace ID a worker log record belongs to: its pod's trace, or for host logs the trace of
// the VM's start; empty when neither is known.
func (l *Lifecycle) LogTraceID(instanceID, podUID string) string {
	if podUID != "" {
		return lifecycletrace.ForPod(podUID).ID.String()
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if vm, ok := l.vms[instanceID]; ok {
		return vm.trace.ID.String()
	}
	return ""
}

// gcLocked drops records idle longer than lifecycleRecordTTL, at most once per lifecycleGCPeriod.
func (l *Lifecycle) gcLocked(now time.Time) {
	if now.Sub(l.lastGC) < lifecycleGCPeriod {
		return
	}
	l.lastGC = now
	for uid, p := range l.pods {
		if now.Sub(p.seen) > lifecycleRecordTTL {
			delete(l.pods, uid)
		}
	}
	for id, vm := range l.vms {
		if now.Sub(vm.seen) > lifecycleRecordTTL {
			delete(l.vms, id)
		}
	}
}

func (o lifecycleOffering) attrs() []attribute.KeyValue {
	var attrs []attribute.KeyValue
	for _, kv := range [][2]string{
		{"cloud.provider", o.provider},
		{"cloud.region", o.region},
		{"cloudvm.instance_type", o.instanceType},
		{"cloudvm.capacity_type", o.capacityType},
	} {
		if kv[1] != "" {
			attrs = append(attrs, attribute.String(kv[0], kv[1]))
		}
	}
	return attrs
}

func nodeOffering(node *CloudVMNode) lifecycleOffering {
	ct := "on-demand"
	if node.Spot {
		ct = "spot"
	}
	return lifecycleOffering{provider: string(node.Provider), region: node.Region, instanceType: node.InstanceType, capacityType: ct}
}

// capacityType names a launch spec's market type the way NodeClaims and Node labels do.
func capacityType(marketType string) string {
	if marketType == "on-demand" {
		return "on-demand"
	}
	return "spot"
}
