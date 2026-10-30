// Package main implements the CloudVM node agent: one process per shard that registers the Nodes of many NodeClaim
// VMs, keeps them Ready, runs their pods through the plugin, and serves their kubelet API; `approve-serving-certs` runs
// the approver of the agents' serving certificate requests instead.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/simonepri/interlink-cloudvm-plugin/nodeclaim"
	"github.com/virtual-kubelet/virtual-kubelet/node"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	typedcorev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/tools/record"
)

// serviceAccountCA is the cluster CA bundle mounted into every pod, the default trust for kubelet API callers.
const serviceAccountCA = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"

// defaultServingCertDuration is the lifetime requested for, and the longest approved for, a signed serving certificate.
const defaultServingCertDuration = 24 * time.Hour

// defaultAPITokenFile is where the chart projects the service account token the NodeClaim API admits.
const defaultAPITokenFile = "/var/run/secrets/cloudvm-plugin/token" //nolint:gosec // a file path, not a credential

func logf(format string, args ...any) {
	log.Printf("[node-agent] "+format, args...) //nolint:gosec // callers pass sanitized names and errors
}

// Config is the agent's environment.
type Config struct {
	PluginURL           string
	Shard               Shard
	NodeIP              string
	KubeletPort         int32
	PodName             string
	CertFile            string
	KeyFile             string
	ServingSigner       string        // CertificateSigningRequest signer of the serving certificate; empty self-signs
	ServingCertDuration time.Duration // lifetime requested for the signed serving certificate
	ClientCAFile        string
	APITokenFile        string // projected token sent to the NodeClaim API
	ClaimPoll           time.Duration
	PodStatus           time.Duration
	LeaseSeconds        int32
	LeaseRenew          time.Duration
	NodeStatus          time.Duration
	PodSyncWorkers      int
	KubeQPS             float32
	KubeBurst           int
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	d, err := time.ParseDuration(envOr(key, def.String()))
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a positive duration", key)
	}
	return d, nil
}

func envInt(key string, def int) (int, error) {
	n, err := strconv.Atoi(envOr(key, strconv.Itoa(def)))
	if err != nil || n <= 0 {
		return 0, fmt.Errorf("%s must be a positive integer", key)
	}
	return n, nil
}

// LoadConfig reads the configuration from the environment.
func LoadConfig() (*Config, error) {
	cfg := &Config{
		PluginURL:     os.Getenv("PLUGIN_URL"),
		NodeIP:        os.Getenv("POD_IP"),
		PodName:       envOr("POD_NAME", "node-agent-0"),
		CertFile:      os.Getenv("KUBELET_TLS_CERT_FILE"),
		KeyFile:       os.Getenv("KUBELET_TLS_KEY_FILE"),
		ServingSigner: os.Getenv("KUBELET_SERVING_SIGNER"),
		ClientCAFile:  envOr("KUBELET_CLIENT_CA_FILE", serviceAccountCA),
		APITokenFile:  envOr("PLUGIN_API_TOKEN_FILE", defaultAPITokenFile),
	}
	if cfg.PluginURL == "" {
		return nil, errors.New("PLUGIN_URL is required")
	}
	if cfg.NodeIP == "" {
		return nil, errors.New("POD_IP is required: every served Node advertises it")
	}
	shard, err := ParseShard(os.Getenv("SHARD_INDEX"), envOr("SHARD_COUNT", "1"), cfg.PodName)
	if err != nil {
		return nil, err
	}
	cfg.Shard = shard
	var errs []error
	collect := func(err error) { errs = append(errs, err) }
	port, err := envInt("KUBELET_PORT", 10250)
	collect(err)
	cfg.KubeletPort = int32(min(port, 65535)) //nolint:gosec // bounded above
	cfg.ServingCertDuration, err = envDuration("KUBELET_SERVING_CERT_DURATION", defaultServingCertDuration)
	collect(err)
	cfg.ClaimPoll, err = envDuration("CLAIM_POLL_INTERVAL", 10*time.Second)
	collect(err)
	cfg.PodStatus, err = envDuration("POD_STATUS_INTERVAL", 5*time.Second)
	collect(err)
	lease, err := envInt("LEASE_DURATION_SECONDS", 40)
	collect(err)
	cfg.LeaseSeconds = int32(min(lease, 3600)) //nolint:gosec // bounded above
	cfg.LeaseRenew, err = envDuration("LEASE_RENEW_INTERVAL", 10*time.Second)
	collect(err)
	cfg.NodeStatus, err = envDuration("NODE_STATUS_INTERVAL", time.Minute)
	collect(err)
	cfg.PodSyncWorkers, err = envInt("POD_SYNC_WORKERS", 20)
	collect(err)
	qps, err := envInt("KUBE_API_QPS", 500)
	collect(err)
	cfg.KubeQPS = float32(qps)
	cfg.KubeBurst, err = envInt("KUBE_API_BURST", 1000)
	collect(err)
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	return cfg, nil
}

func kubeConfig() (*rest.Config, error) {
	if path := os.Getenv("KUBECONFIG"); path != "" {
		return clientcmd.BuildConfigFromFlags("", path)
	}
	return rest.InClusterConfig()
}

func main() {
	run := runAgent
	if len(os.Args) > 1 && os.Args[1] == "approve-serving-certs" {
		run = runApprover
	}
	if err := run(); err != nil {
		logf("exiting: %v", err)
		os.Exit(1)
	}
}

// runApprover approves the serving certificate requests of the node agent pods named by the environment.
func runApprover() error {
	cfg := ServingCertApproverConfig{
		SignerName:          os.Getenv("SIGNER_NAME"),
		AgentNamespace:      os.Getenv("AGENT_NAMESPACE"),
		AgentServiceAccount: os.Getenv("AGENT_SERVICE_ACCOUNT"),
	}
	if cfg.SignerName == "" || cfg.AgentNamespace == "" || cfg.AgentServiceAccount == "" {
		return errors.New("SIGNER_NAME, AGENT_NAMESPACE and AGENT_SERVICE_ACCOUNT are required")
	}
	maxDuration, err := envDuration("MAX_SERVING_CERT_DURATION", defaultServingCertDuration)
	if err != nil {
		return err
	}
	cfg.MaxDuration = maxDuration
	restCfg, err := kubeConfig()
	if err != nil {
		return fmt.Errorf("kubernetes client config: %w", err)
	}
	restCfg.UserAgent = "cloudvm-serving-cert-approver"
	if cfg.Kube, err = kubernetes.NewForConfig(restCfg); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return NewServingCertApprover(cfg).Run(ctx)
}

func runAgent() error {
	cfg, err := LoadConfig()
	if err != nil {
		return err
	}
	restCfg, err := kubeConfig()
	if err != nil {
		return fmt.Errorf("kubernetes client config: %w", err)
	}
	restCfg.QPS, restCfg.Burst = cfg.KubeQPS, cfg.KubeBurst
	restCfg.UserAgent = "cloudvm-node-agent"
	kube, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	httpClient := &http.Client{Timeout: 60 * time.Second}
	agent := NewAgent(AgentConfig{
		Kube: kube,
		Claims: nodeclaim.NewClient(cfg.PluginURL, &http.Client{
			Timeout:   httpClient.Timeout,
			Transport: &nodeclaim.TokenFileTransport{Path: cfg.APITokenFile},
		}),
		Pods:           NewPodAPI(cfg.PluginURL, httpClient),
		Shard:          cfg.Shard,
		NodeIP:         cfg.NodeIP,
		KubeletPort:    cfg.KubeletPort,
		ClaimPoll:      cfg.ClaimPoll,
		PodStatus:      cfg.PodStatus,
		LeaseSeconds:   cfg.LeaseSeconds,
		LeaseRenew:     cfg.LeaseRenew,
		NodeStatus:     cfg.NodeStatus,
		PodSyncWorkers: cfg.PodSyncWorkers,
	})
	servingCert, err := StartServingCertificate(ctx, ServingCertConfig{
		CertFile: cfg.CertFile, KeyFile: cfg.KeyFile, SignerName: cfg.ServingSigner, PodName: cfg.PodName,
		PodIP: cfg.NodeIP, Duration: cfg.ServingCertDuration, Kube: kube,
	})
	if err != nil {
		return fmt.Errorf("kubelet API serving certificate: %w", err)
	}
	server, err := NewKubeletAPIServer(KubeletAPIConfig{
		Addr: fmt.Sprintf(":%d", cfg.KubeletPort), ServingCertificate: servingCert, ClientCAFile: cfg.ClientCAFile,
	}, agent.Pods)
	if err != nil {
		return err
	}
	go func() {
		if err := server.ListenAndServeTLS("", ""); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logf("kubelet API server: %v", err)
			stop()
		}
	}()
	logf("serving shard %d of %d on %s:%d", cfg.Shard.Index, cfg.Shard.Count, cfg.NodeIP, cfg.KubeletPort)
	runErr := agent.Run(ctx)
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdownCtx)
	return runErr
}

// AgentConfig wires an Agent.
type AgentConfig struct {
	Kube           kubernetes.Interface
	Claims         nodeclaim.API
	Pods           *PodAPI
	Shard          Shard
	NodeIP         string
	KubeletPort    int32
	ClaimPoll      time.Duration
	PodStatus      time.Duration
	LeaseSeconds   int32
	LeaseRenew     time.Duration
	NodeStatus     time.Duration
	OrphanSweep    time.Duration
	PodSyncWorkers int
}

// Agent is one shard: its NodeManager, its pod provider, and the shared pod informer and controller.
type Agent struct {
	cfg   AgentConfig
	Nodes *NodeManager
	Pods  *PodProvider
}

// NewAgent builds the shard's components without starting them.
func NewAgent(cfg AgentConfig) *Agent {
	nodes := NewNodeManager(NodeConfig{
		Shard: cfg.Shard, Claims: cfg.Claims, Kube: cfg.Kube, NodeIP: cfg.NodeIP, KubeletPort: cfg.KubeletPort,
		PollInterval: cfg.ClaimPoll, LeaseDurationSeconds: cfg.LeaseSeconds, LeaseRenewInterval: cfg.LeaseRenew,
		StatusInterval: cfg.NodeStatus, OrphanSweepInterval: cfg.OrphanSweep,
	})
	return &Agent{cfg: cfg, Nodes: nodes, Pods: NewPodProvider(cfg.Pods, cfg.Kube, nodes, cfg.PodStatus)}
}

// Run serves Nodes and pods until ctx ends.
func (a *Agent) Run(ctx context.Context) error {
	go a.Nodes.Run(ctx)
	select {
	case <-ctx.Done():
		return nil
	case <-a.Nodes.Synced():
	}

	podFactory := informers.NewSharedInformerFactoryWithOptions(a.cfg.Kube, 0,
		informers.WithTweakListOptions(scheduledPods), informers.WithTransform(podTransform(a.cfg.Shard)))
	podInformer := podFactory.Core().V1().Pods()
	serviceFactory := informers.NewSharedInformerFactoryWithOptions(a.cfg.Kube, 0, informers.WithTransform(serviceTransform))
	serviceInformer := serviceFactory.Core().V1().Services()
	podInformer.Informer()
	serviceInformer.Informer()
	podFactory.Start(ctx.Done())
	serviceFactory.Start(ctx.Done())
	defer podFactory.Shutdown()
	defer serviceFactory.Shutdown()

	broadcaster := record.NewBroadcaster(record.WithContext(ctx))
	broadcaster.StartRecordingToSink(&typedcorev1.EventSinkImpl{Interface: a.cfg.Kube.CoreV1().Events("")})
	defer broadcaster.Shutdown()

	pc, err := node.NewPodController(node.PodControllerConfig{
		PodClient:         a.cfg.Kube.CoreV1(),
		PodInformer:       podInformer,
		EventRecorder:     broadcaster.NewRecorder(scheme.Scheme, corev1.EventSource{Component: "cloudvm-node-agent"}),
		Provider:          a.Pods,
		SecretInformer:    getSecretInformer{client: a.cfg.Kube},
		ConfigMapInformer: getConfigMapInformer{client: a.cfg.Kube},
		ServiceInformer:   serviceInformer,
		PodEventFilterFunc: func(_ context.Context, pod *corev1.Pod) bool {
			return a.cfg.Shard.Owns(pod.Spec.NodeName) && a.Nodes.Serves(pod.Spec.NodeName)
		},
	})
	if err != nil {
		return err
	}
	workers := a.cfg.PodSyncWorkers
	if workers <= 0 {
		workers = 20
	}
	return pc.Run(ctx, workers)
}
