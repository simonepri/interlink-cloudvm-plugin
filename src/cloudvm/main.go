// Package main implements the interlink-provider-cloudvm daemon.
package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
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

	log.Printf("[interlink-provider-cloudvm] Starting server...")

	var defaultProvider Provider
	mockProv := NewMockProvider()
	awsRegion := os.Getenv("AWS_REGION")
	if awsRegion == "" {
		awsRegion = "us-east-1"
	}
	awsProv := NewAWSProvider(awsRegion)

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
		spotTicker := NewSpotPriceTicker(tickerRegions, nil, interval)
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

		go func() {
			if err := tickerServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				log.Fatalf("[cloudvm-ticker] Server failed: %v", err)
			}
		}()

		<-stopChan
		log.Printf("[cloudvm-ticker] Shutting down gracefully...")
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
		_ = tickerServer.Shutdown(shutdownCtx)
		shutdownCancel()
		log.Printf("[cloudvm-ticker] Server stopped.")
		return
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
	server.RegisterProvider(ProviderAWS, awsProv)
	server.RegisterProvider(ProviderGCP, NewGCPProvider("", "us-central1-a"))
	server.RegisterProvider(ProviderMock, mockProv)

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
		spotTicker := NewSpotPriceTicker(tickerRegions, nil, interval)
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

		reconciler := NewReconciler(inventory, server, terminator, clusterTag, 10*time.Minute)
		server.SetReconciler(reconciler)

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
		idleTicker := time.NewTicker(30 * time.Second)
		defer idleTicker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-idleTicker.C:
				if terminator != nil {
					for _, node := range server.NodePool().CollectExpiredIdleNodes() {
						cleanID := strings.ReplaceAll(strings.ReplaceAll(node.InstanceID, "\n", ""), "\r", "")
						log.Printf("[cloudvm] Terminating expired idle node %s", cleanID) //nolint:gosec // instance ID is internal and sanitized
						_ = terminator.Terminate(ctx, node.InstanceID)
					}
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

	go func() {
		portNum, _ := strconv.Atoi(port)
		if portNum <= 0 || portNum > 65535 {
			portNum = 4040
		}
		log.Printf("[interlink-provider-cloudvm] Listening on port %d", portNum)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[interlink-provider-cloudvm] Server failed: %v", err)
		}
	}()

	<-stopChan
	log.Printf("[interlink-provider-cloudvm] Shutting down gracefully...")

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	err := httpServer.Shutdown(shutdownCtx)
	shutdownCancel()
	if err != nil {
		log.Printf("[interlink-provider-cloudvm] Shutdown error: %v", err)
	}

	log.Printf("[interlink-provider-cloudvm] Server stopped.")
}
