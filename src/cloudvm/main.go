// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/pricing"
	"github.com/aws/aws-sdk-go-v2/service/servicequotas"
)

func parseCommaList(val string) []string {
	var out []string
	for _, item := range strings.Split(val, ",") {
		if t := strings.TrimSpace(item); t != "" {
			out = append(out, t)
		}
	}
	return out
}

func parseTickerRegions(val string) []string {
	trimmed := strings.TrimSpace(val)
	if trimmed == "" || strings.EqualFold(trimmed, "auto") {
		return []string{"auto"}
	}
	regions := parseCommaList(trimmed)
	if len(regions) == 0 {
		return []string{"auto"}
	}
	for _, r := range regions {
		if strings.EqualFold(r, "auto") {
			return []string{"auto"}
		}
	}
	return regions
}

// awsPricingRegion hosts the AWS Price List API endpoint; prices for every region are read from it.
const awsPricingRegion = "us-east-1"

// newConfiguredTicker builds the spot price ticker from TICKER_* and SCRAPE_INTERVAL_SECONDS and connects its AWS market APIs.
func newConfiguredTicker(ctx context.Context, awsProv *AWSProvider) *SpotPriceTicker {
	tickerRegions := parseTickerRegions(os.Getenv("TICKER_REGIONS"))
	interval := 10 * time.Minute
	if intervalSec := os.Getenv("SCRAPE_INTERVAL_SECONDS"); intervalSec != "" {
		if s, err := strconv.Atoi(intervalSec); err == nil && s > 0 {
			interval = time.Duration(s) * time.Second
		}
	}
	tickerTypes := parseCommaList(os.Getenv("TICKER_INSTANCE_TYPES"))
	spotTicker := NewSpotPriceTicker(tickerRegions, tickerTypes, interval, awsProv.ConfiguredRegions()...)
	spotTicker.GCPMarket().SetRegions(parseCommaList(os.Getenv("TICKER_GCP_REGIONS")))
	if raw := os.Getenv("TICKER_PROBE_MAX_INSTANCES"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			spotTicker.SetProbeMaxInstances(n)
		}
	}
	configureAWSMarketAPIs(ctx, spotTicker)
	return spotTicker
}

// configureAWSMarketAPIs gives the shared on-demand price book a Price List API client and the ticker, when present, per-region Service Quotas and CloudWatch clients.
func configureAWSMarketAPIs(ctx context.Context, spotTicker *SpotPriceTicker) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(awsPricingRegion))
	if err != nil {
		log.Printf("[cloudvm] Warning: AWS on-demand prices and quotas unavailable: %v", err)
		return
	}
	GlobalOnDemandPrices.SetClient(pricing.NewFromConfig(cfg))
	if spotTicker != nil {
		spotTicker.SetServiceQuotasFactory(func(region string) AWSServiceQuotasAPI {
			return servicequotas.NewFromConfig(cfg, func(o *servicequotas.Options) { o.Region = region })
		})
		spotTicker.SetQuotaUsageFactory(func(region string) AWSUsageAPI {
			return cloudwatch.NewFromConfig(cfg, func(o *cloudwatch.Options) { o.Region = region })
		})
	}
}

func main() {
	if err := run(); err != nil {
		log.Fatalf("[cloudvm] %v", err)
	}
}

func run() error {
	port := os.Getenv("PORT")
	if port == "" {
		port = "4040"
	}

	clusterTag := os.Getenv("CLUSTER_TAG")
	if clusterTag == "" {
		clusterTag = "interlink-cloudvm"
	}

	providerType := os.Getenv("PROVIDER_TYPE")
	if providerType == "" {
		providerType = "mock"
	}

	log.Printf("[interlink-cloudvm-plugin] Starting server...")

	var defaultProvider Provider
	mockProv := NewMockProvider()
	awsRegion := os.Getenv("AWS_REGION")
	if awsRegion == "" {
		awsRegion = "us-east-1"
	}
	var additionalRegions []string
	if raw := os.Getenv("AWS_REGIONS"); raw != "" {
		for _, r := range strings.Split(raw, ",") {
			r = strings.TrimSpace(r)
			if r != "" {
				additionalRegions = append(additionalRegions, r)
			}
		}
	}
	ConfigureAWSLaunchPacingFromEnv()
	awsProv := NewAWSProvider(awsRegion, additionalRegions...)

	// Standalone spot price ticker microservice mode
	if os.Getenv("SERVICE_MODE") == "ticker" {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		spotTicker := newConfiguredTicker(ctx, awsProv)
		go spotTicker.Start(ctx)
		log.Printf("[cloudvm-ticker] Running standalone spot ticker on port %s for %d regions", port, len(spotTicker.Regions())) //nolint:gosec // length safe

		tickerServer := &http.Server{
			Addr:         ":" + port,
			Handler:      spotTicker.Routes(),
			ReadTimeout:  15 * time.Second,
			WriteTimeout: 15 * time.Second,
		}

		stopChan := make(chan os.Signal, 1)
		signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

		tickerErrChan := make(chan error, 1)
		go func() {
			if err := tickerServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				tickerErrChan <- err
			}
		}()

		select {
		case <-stopChan:
			log.Printf("[cloudvm-ticker] Shutting down gracefully...")
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = tickerServer.Shutdown(shutdownCtx)
			shutdownCancel()
			log.Printf("[cloudvm-ticker] Server stopped.")
			return nil
		case err := <-tickerErrChan:
			return fmt.Errorf("ticker server failed: %w", err)
		}
	}

	switch providerType {
	case "aws":
		defaultProvider = awsProv
	case "gcp":
		gcpProject := os.Getenv("GCP_PROJECT")
		gcpZone := os.Getenv("GCP_ZONE")
		defaultProvider = NewGCPProvider(gcpProject, gcpZone)
	default:
		defaultProvider = mockProv
	}

	server := NewServer(defaultProvider, clusterTag)
	server.SetDefaultRegion(awsRegion)

	if dbURL := os.Getenv("DATABASE_URL"); dbURL != "" {
		pgStore, err := NewPGStore(dbURL)
		if err != nil {
			return fmt.Errorf("failed to initialize PostgreSQL store: %w", err)
		}
		defer pgStore.Close()
		server.SetTokenStore(pgStore.TokenStore())
		server.SetCompletedStore(pgStore.CompletedStore())
		server.SetTombstoneStore(pgStore.TombstoneStore())
		server.SetRegionStore(pgStore.RegionStore())
		awsProv.SetRegionStore(pgStore.RegionStore())
		log.Printf("[cloudvm] Initialized PostgreSQL HA stores")
	}

	if mirrors := os.Getenv("REGISTRY_MIRRORS"); mirrors != "" {
		server.SetRegistryMirrors(parseCommaList(mirrors))
	}
	if otel := os.Getenv("OTEL_ENDPOINT"); otel != "" {
		server.SetOtelEndpoint(otel)
	}
	if npd := os.Getenv("ENABLE_NPD"); npd == "true" || npd == "1" {
		server.SetEnableNPD(true)
	}
	if script := os.Getenv("CUSTOM_BOOTSTRAP_SCRIPT"); script != "" {
		server.SetCustomBootstrapScript(script)
	}
	if raw := os.Getenv("MAX_RUNNER_CONCURRENCY"); raw != "" {
		if c, err := strconv.Atoi(strings.TrimSpace(raw)); err == nil && c > 0 {
			server.SetRunnerConcurrency(c)
		}
	}

	idleGrace := 60 * time.Second
	if raw := os.Getenv("IDLE_GRACE_PERIOD"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d >= 0 {
			idleGrace = d
		} else if sec, err := strconv.Atoi(raw); err == nil && sec >= 0 {
			idleGrace = time.Duration(sec) * time.Second
		}
	}
	server.SetIdleGracePeriod(idleGrace)
	if raw := os.Getenv("VM_LAUNCH_TIMEOUT"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			server.SetLaunchTimeout(d)
		} else if sec, err := strconv.Atoi(raw); err == nil && sec > 0 {
			server.SetLaunchTimeout(time.Duration(sec) * time.Second)
		}
	}
	if raw := os.Getenv("POD_SIGHTING_TIMEOUT"); raw != "" {
		if d, err := time.ParseDuration(raw); err == nil && d > 0 {
			server.SetPodSightingTimeout(d)
		} else if sec, err := strconv.Atoi(raw); err == nil && sec > 0 {
			server.SetPodSightingTimeout(time.Duration(sec) * time.Second)
		}
	}
	log.Printf("[cloudvm] Node pool warm idle grace period configured to %v", idleGrace) //nolint:gosec // duration is safe
	server.RegisterProvider(ProviderAWS, awsProv)
	server.RegisterProvider(ProviderGCP, NewGCPProvider("", "us-central1-a"))
	server.RegisterProvider(ProviderMock, mockProv)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if providerType == "aws" || providerType == "gcp" {
		gateway, err := startTunnelGateway(ctx, server)
		if err != nil {
			return err
		}
		defer func() { _ = gateway.Close() }()
	}

	// Embedded spot price ticker
	if os.Getenv("ENABLE_TICKER") == "true" {
		spotTicker := newConfiguredTicker(ctx, awsProv)
		go spotTicker.Start(ctx)
		server.SetSpotTicker(spotTicker)
		GlobalFleetTracker.SetSpotPrices(spotTicker.SpotPrice)
		awsProv.SetRegionScorer(spotTicker)
		log.Printf("[cloudvm-ticker] Embedded spot price poller started for %d regions", len(spotTicker.Regions())) //nolint:gosec // log length is safe
	} else if providerType == "aws" {
		configureAWSMarketAPIs(ctx, nil)
	}

	// Background reconciler
	reconcileInterval := 60 * time.Second
	if intervalSec := os.Getenv("RECONCILE_INTERVAL_SECONDS"); intervalSec != "" {
		if s, err := strconv.Atoi(intervalSec); err == nil && s > 0 {
			reconcileInterval = time.Duration(s) * time.Second
		}
	}

	var inventory CloudInventory
	if inv, ok := defaultProvider.(CloudInventory); ok {
		inventory = inv
	}
	var terminator InstanceTerminator
	if term, ok := defaultProvider.(InstanceTerminator); ok {
		terminator = term
	}

	if inventory != nil {
		if err := server.RecoverActiveInstances(ctx, inventory); err != nil {
			log.Printf("[cloudvm] Warning: failed to recover active instances: %v", err)
		}

		graceTTL := 10 * time.Minute
		if sec := os.Getenv("RECONCILE_GRACE_TTL_SECONDS"); sec != "" {
			if s, err := strconv.Atoi(sec); err == nil && s > 0 {
				graceTTL = time.Duration(s) * time.Second
			}
		}
		reconciler := NewReconciler(inventory, server, terminator, clusterTag, graceTTL)
		launchGrace := 2 * time.Minute
		if sec := os.Getenv("RECONCILE_LAUNCH_GRACE_SECONDS"); sec != "" {
			if s, err := strconv.Atoi(sec); err == nil && s >= 0 {
				launchGrace = time.Duration(s) * time.Second
			}
		}
		if sec := os.Getenv("UNKNOWN_COTENANT_TIMEOUT_SECONDS"); sec != "" {
			if s, err := strconv.Atoi(sec); err == nil && s > 0 {
				UnknownCoTenantTimeout = time.Duration(s) * time.Second
			}
		}
		reconciler.SetLaunchGracePeriod(launchGrace)
		if pw, err := NewInClusterPodWatch(); err == nil {
			pw.Start(ctx)
			reconciler.SetPodWatch(pw)
		}
		server.SetReconciler(reconciler)
		go reconciler.Start(ctx)

		go func() {
			ticker := time.NewTicker(reconcileInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					res, err := reconciler.ReconcileOnce(ctx)
					if err != nil {
						log.Printf("[reconciler] Error during reconciliation: %v", err)
					} else if len(res.TerminatedZombies) > 0 || len(res.EvictedPods) > 0 {
						log.Printf("[reconciler] Cleaned %d zombies, evicted %d vanished pods", //nolint:gosec // counts only
							len(res.TerminatedZombies), len(res.EvictedPods))
					}
				}
			}
		}()
	}

	// Background idle node collector (terminates warm VMs after idle grace period)
	go func() {
		idleInterval := 10 * time.Second
		if ig := server.NodePool().IdleGracePeriod(); ig > 0 && ig < 10*time.Second {
			idleInterval = ig / 2
			if idleInterval < time.Second {
				idleInterval = time.Second
			}
		}
		idleTicker := time.NewTicker(idleInterval)
		defer idleTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-idleTicker.C:
				for _, node := range server.NodePool().CollectExpiredIdleNodes() {
					server.TerminateIdleNode(ctx, node)
				}
			}
		}
	}()

	httpServer := &http.Server{
		Addr:        ":" + port,
		Handler:     server.Routes(),
		ReadTimeout: 15 * time.Second,
		// A create call launches the VM before it responds, so the response deadline covers the launch timeout.
		WriteTimeout: server.LaunchTimeout() + time.Minute,
	}

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	httpErrChan := make(chan error, 1)
	go func() {
		portNum, _ := strconv.Atoi(port)
		if portNum <= 0 || portNum > 65535 {
			portNum = 4040
		}
		log.Printf("[interlink-cloudvm-plugin] Listening on port %d", portNum)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			httpErrChan <- err
		}
	}()

	select {
	case <-stopChan:
		log.Printf("[interlink-cloudvm-plugin] Shutting down gracefully...")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		err := httpServer.Shutdown(shutdownCtx)
		shutdownCancel()
		if err != nil {
			log.Printf("[interlink-cloudvm-plugin] Shutdown error: %v", err)
		}
		log.Printf("[interlink-cloudvm-plugin] Server stopped.")
		return nil
	case err := <-httpErrChan:
		return fmt.Errorf("http server failed: %w", err)
	}
}

// startTunnelGateway listens for worker reverse tunnels and routes every runner call through them.
func startTunnelGateway(ctx context.Context, server *Server) (*TunnelGateway, error) {
	endpoint := strings.TrimSpace(os.Getenv("TUNNEL_ENDPOINT"))
	transports, err := TunnelTransportsFromClusterFacts(os.Getenv("CLUSTER_FACTS"))
	if err != nil {
		return nil, err
	}
	directCIDRs, err := DirectCIDRsFromClusterFacts(os.Getenv("CLUSTER_FACTS"))
	if err != nil {
		return nil, err
	}
	imageMirrors, err := ImageMirrorsFromClusterFacts(os.Getenv("CLUSTER_FACTS"))
	if err != nil {
		return nil, err
	}
	meshKey := os.Getenv("TAILSCALE_AUTHKEY")
	if meshKey == "" && transports.meshKeyRequired() {
		return nil, fmt.Errorf("TAILSCALE_AUTHKEY is required so tailnet-transport workers can join the tailnet")
	}
	connectTimeout := DefaultTunnelConnectTimeout
	if raw := os.Getenv("TUNNEL_CONNECT_TIMEOUT"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d <= 0 {
			return nil, fmt.Errorf("invalid TUNNEL_CONNECT_TIMEOUT %q", raw)
		}
		connectTimeout = d
	}
	listenAddr := os.Getenv("TUNNEL_GATEWAY_LISTEN")
	if listenAddr == "" {
		listenAddr = ":7443"
	}

	settings := TunnelSettings{
		Endpoint:       endpoint,
		Transports:     transports,
		DirectCIDRs:    directCIDRs,
		ClusterCIDRs:   splitCIDRList(os.Getenv("TUNNEL_TCP_DIRECT_CIDRS")),
		ImageMirrors:   imageMirrors,
		GatewayIP:      strings.TrimSpace(os.Getenv("TUNNEL_GATEWAY_IP")),
		PublicEndpoint: strings.TrimSpace(os.Getenv("TUNNEL_PUBLIC_ENDPOINT")),
		MeshAuthKey:    meshKey,
		MeshTags:       os.Getenv("TAILSCALE_TAGS"),
		ConnectTimeout: connectTimeout,
		DirectFallback: os.Getenv("RUNNER_DIRECT_FALLBACK") == "true",
	}
	if err := settings.validateTransports(); err != nil {
		return nil, err
	}
	if transports.Uses(TunnelTransportPublic) {
		verifier, err := publicIdentityVerifier(server, os.Getenv(TunnelIdentityVerificationEnv), AllowedAccountsFromEnv())
		if err != nil {
			return nil, err
		}
		settings.IdentityVerifier = verifier
	}
	registry := NewSessionRegistry()
	settings.Registry = registry
	metrics := server.EnableTunnel(settings)
	listener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed to listen for reverse tunnels on %s: %w", listenAddr, err)
	}
	if endpoint := strings.TrimSpace(os.Getenv("CLOUDVM_OTLP_LOGS_ENDPOINT")); endpoint != "" {
		forwarder, err := NewOTLPLogForwarder(endpoint)
		if err != nil {
			_ = listener.Close()
			return nil, err
		}
		server.SetLogForwarder(forwarder)
		go forwarder.Run(ctx)
		log.Printf("[cloudvm-logs] Forwarding worker logs to %s", endpoint) //nolint:gosec // configuration value
	}
	directTCPNetworks, err := ParseDirectTCPNetworks(os.Getenv("TUNNEL_TCP_DIRECT_CIDRS"))
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	gateway, err := NewTunnelGateway(TunnelGatewayConfig{
		Listener:           listener,
		Registry:           registry,
		TokenValidator:     server.ValidateTunnelToken,
		IdentityValidator:  server.ValidateTunnelIdentity,
		OnReject:           metrics.RecordRejection,
		OnBootstrapRequest: metrics.RecordBootstrapRequest,
		LogSink:            server,
		Resolver:           server.ResolveInstanceAddress,
		DirectTCPNetworks:  directTCPNetworks,
	})
	if err != nil {
		_ = listener.Close()
		return nil, err
	}
	gateway.Start(ctx)
	if proxyAddr := strings.TrimSpace(os.Getenv("TUNNEL_PROXY_LISTEN")); proxyAddr != "" {
		proxyListener, err := (&net.ListenConfig{}).Listen(ctx, "tcp", proxyAddr)
		if err != nil {
			return nil, fmt.Errorf("failed to listen for tunnel CONNECT requests on %s: %w", proxyAddr, err)
		}
		go gateway.ServeConnectProxy(ctx, proxyListener)
		log.Printf("[cloudvm-tunnel] Serving CONNECT to worker addresses on %s", proxyAddr) //nolint:gosec // configuration value
	}
	log.Printf("[cloudvm-tunnel] Gateway listening on %s; workers dial %s (connect timeout %v)", listenAddr, endpoint, connectTimeout) //nolint:gosec // configuration values
	return gateway, nil
}
