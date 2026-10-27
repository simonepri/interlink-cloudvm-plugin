// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"log"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/ec2"
)

// maxTerminateInstanceIDs is the most instance IDs one EC2 TerminateInstances call accepts.
const maxTerminateInstanceIDs = 1000

// TerminateBatch terminates instances with one TerminateInstances call per region and 1000 IDs. EC2 rejects a whole
// call when any ID in it is invalid, so a failed call falls back to one call per instance.
func (p *AWSProvider) TerminateBatch(ctx context.Context, instanceIDs []string) map[string]error {
	byRegion := make(map[string][]string)
	p.mu.RLock()
	for _, id := range instanceIDs {
		region := p.instanceRegions[id]
		if region == "" {
			region = p.Region
		}
		byRegion[region] = append(byRegion[region], id)
	}
	p.mu.RUnlock()

	var (
		mu   sync.Mutex
		wg   sync.WaitGroup
		errs map[string]error
	)
	for region, ids := range byRegion {
		for start := 0; start < len(ids); start += maxTerminateInstanceIDs {
			chunk := ids[start:min(start+maxTerminateInstanceIDs, len(ids))]
			wg.Add(1)
			go func() {
				defer wg.Done()
				chunkErrs := p.terminateChunk(ctx, region, chunk)
				if len(chunkErrs) == 0 {
					return
				}
				mu.Lock()
				defer mu.Unlock()
				if errs == nil {
					errs = make(map[string]error)
				}
				for id, err := range chunkErrs {
					errs[id] = err
				}
			}()
		}
	}
	wg.Wait()
	return errs
}

// terminateChunk terminates up to maxTerminateInstanceIDs instances of one region in one call.
func (p *AWSProvider) terminateChunk(ctx context.Context, region string, ids []string) map[string]error {
	callCtx, cancel := context.WithTimeout(ctx, terminateCallTimeout)
	defer cancel()
	client, err := p.getClient(callCtx, region)
	if err == nil {
		_, err = client.TerminateInstances(callCtx, &ec2.TerminateInstancesInput{InstanceIds: ids})
	}
	if err != nil {
		log.Printf("[cloudvm] Batch termination of %d instances in %s failed, retrying one by one: %v", len(ids), sanitizeLog(region), err) //nolint:gosec // sanitized
		return terminateEach(ctx, p.Terminate, ids)
	}
	p.mu.Lock()
	for _, id := range ids {
		delete(p.instanceRegions, id)
	}
	p.mu.Unlock()
	if p.regionStore != nil {
		for _, id := range ids {
			_ = p.regionStore.Delete(id)
		}
	}
	return nil
}
