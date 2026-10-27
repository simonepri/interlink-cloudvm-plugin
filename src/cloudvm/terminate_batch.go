// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"sync"
	"time"
)

// BatchTerminator terminates many instances in as few cloud calls as the provider allows.
type BatchTerminator interface {
	// TerminateBatch terminates every instance and returns an error for each instance it failed to terminate.
	TerminateBatch(ctx context.Context, instanceIDs []string) map[string]error
}

// terminateConcurrency bounds parallel single-instance Terminate calls.
const terminateConcurrency = 32

// terminateCallTimeout bounds one cloud termination call so a hung call cannot stall the callers' loops.
const terminateCallTimeout = 30 * time.Second

// terminateInstances terminates every instance through term and returns an error for each instance it failed to
// terminate; instances absent from the result were terminated.
func terminateInstances(ctx context.Context, term InstanceTerminator, instanceIDs []string) map[string]error {
	if len(instanceIDs) == 0 {
		return nil
	}
	if batch, ok := term.(BatchTerminator); ok {
		return batch.TerminateBatch(ctx, instanceIDs)
	}
	return terminateEach(ctx, term.Terminate, instanceIDs)
}

// terminateEach calls terminate once per instance with bounded parallelism and a per-call timeout.
func terminateEach(ctx context.Context, terminate func(context.Context, string) error, instanceIDs []string) map[string]error {
	var (
		mu   sync.Mutex
		wg   sync.WaitGroup
		errs map[string]error
	)
	sem := make(chan struct{}, terminateConcurrency)
	for _, id := range instanceIDs {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			callCtx, cancel := context.WithTimeout(ctx, terminateCallTimeout)
			defer cancel()
			if err := terminate(callCtx, id); err != nil {
				mu.Lock()
				if errs == nil {
					errs = make(map[string]error)
				}
				errs[id] = err
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	return errs
}
