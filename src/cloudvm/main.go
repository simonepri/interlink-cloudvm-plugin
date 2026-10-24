// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
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
	awsProv := NewAWSProvider(awsRegion, additionalRegions...)

	// Standalone spot price ticker microservice mode
	if os.Getenv("SERVICE_MODE") == "ticker" {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tickerRegions := strings.Split(os.Getenv("TICKER_REGIONS"), ",")
		if len(tickerRegions) == 1 && tickerRegions[0] == "" {
			tickerRegions = []string{awsRegion}
		}
		interval := 10 * time.Minute
		if intervalSec := os.Getenv("SCRAPE_INTERVAL_SECONDS"); intervalSec != "" {
			if s, err := strconv.Atoi(intervalSec); err == nil && s > 0 {
				interval = time.Duration(s) * time.Second
			}
		}
		tickerTypes := parseCommaList(os.Getenv("TICKER_INSTANCE_TYPES"))
		spotTicker := NewSpotPriceTicker(tickerRegions, tickerTypes, interval)
		go spotTicker.Start(ctx)
		log.Printf("[cloudvm-ticker] Running standalone spot ticker on port %s for %d regions", port, len(tickerRegions)) //nolint:gosec // length safe

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
	if providerType == "aws" || providerType == "gcp" {
		server.SetContainerRunner(NewHTTPContainerRunner(9090))
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Embedded spot price ticker
	if os.Getenv("ENABLE_TICKER") == "true" {
		tickerRegions := strings.Split(os.Getenv("TICKER_REGIONS"), ",")
		if len(tickerRegions) == 1 && tickerRegions[0] == "" {
			tickerRegions = []string{awsRegion}
		}
		interval := 10 * time.Minute
		if intervalSec := os.Getenv("SCRAPE_INTERVAL_SECONDS"); intervalSec != "" {
			if s, err := strconv.Atoi(intervalSec); err == nil && s > 0 {
				interval = time.Duration(s) * time.Second
			}
		}
		tickerTypes := parseCommaList(os.Getenv("TICKER_INSTANCE_TYPES"))
		spotTicker := NewSpotPriceTicker(tickerRegions, tickerTypes, interval)
		go spotTicker.Start(ctx)
		server.SetSpotTicker(spotTicker)
		log.Printf("[cloudvm-ticker] Embedded spot price poller started for %d regions", len(tickerRegions)) //nolint:gosec // log length is safe
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
						log.Printf("[reconciler] Cleaned %d zombies, evicted %d vanished pods",
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
		Addr:         ":" + port,
		Handler:      server.Routes(),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
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
