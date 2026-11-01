// Idempotent EC2 Fleet launches: client tokens derived from the exact CreateFleet parameters, and reuse of a VM an
// earlier launch of the same pod or claim already started.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
)

// errFleetTokenReused reports that EC2 already saw the request's client token with other parameters, so nothing
// launched; the next launch of the same pod or claim uses another token.
var errFleetTokenReused = errors.New("CreateFleet client token was already used with other parameters")

// fleetTokenPrefix marks client tokens this plugin derives; a token is the prefix and a hex digest, at most 64 ASCII
// characters as EC2 requires.
const fleetTokenPrefix = "cvm-"

// fleetAttempts counts, per region and launch UID, the CreateFleet attempts known to have launched nothing. The count
// is part of the client token, so a retry after a known empty outcome asks EC2 again instead of replaying it, while a
// retry after an unknown outcome, such as a timeout or a restart, repeats the token and stays idempotent.
var fleetAttempts = &fleetAttemptCounter{counts: make(map[string]int)}

type fleetAttemptCounter struct {
	mu     sync.Mutex
	counts map[string]int
}

func fleetAttemptKey(region, uid string) string { return region + "/" + uid }

func (c *fleetAttemptCounter) current(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.counts[key]
}

func (c *fleetAttemptCounter) advance(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.counts[key]++
}

func (c *fleetAttemptCounter) forget(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.counts, key)
}

// fleetClientToken derives the client token of input from every parameter it carries and the attempt number, so two
// requests share a token only when EC2 would see them as identical.
func fleetClientToken(input *ec2.CreateFleetInput, attempt int) (string, error) {
	unsigned := *input
	unsigned.ClientToken = nil
	params, err := json.Marshal(&unsigned) //nolint:gosec // ClientToken is an idempotency key, not a secret, and is nil here
	if err != nil {
		return "", fmt.Errorf("encode CreateFleet parameters for the client token: %w", err)
	}
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%d\n", attempt)
	_, _ = h.Write(params)
	return fleetTokenPrefix + hex.EncodeToString(h.Sum(nil))[:56], nil
}

func isIdempotentParameterMismatch(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) && apiErr.ErrorCode() == "IdempotentParameterMismatch" {
		return true
	}
	return strings.Contains(err.Error(), "IdempotentParameterMismatch")
}

// findLaunchedInstance returns a pending or running instance in the client's region that an earlier launch of the
// same pod or claim started with one of instanceTypes, in zone when zone is set, or nil when there is none. A plugin
// that restarted mid-launch thereby takes over the VM instead of starting a second one.
func findLaunchedInstance(ctx context.Context, client EC2FleetClient, clusterTag, uid, zone string, instanceTypes []string) (*ec2types.Instance, error) {
	out, err := DescribeAllInstances(ctx, client, &ec2.DescribeInstancesInput{Filters: []ec2types.Filter{
		{Name: aws.String("tag:PodUID"), Values: []string{uid}},
		{Name: aws.String("tag:ClusterTag"), Values: []string{clusterTag}},
		{Name: aws.String("instance-state-name"), Values: []string{"pending", "running"}},
	}})
	if err != nil {
		return nil, fmt.Errorf("look up instances already launched for %s: %w", uid, err)
	}
	for _, r := range out.Reservations {
		for i := range r.Instances {
			inst := &r.Instances[i]
			if instanceTag(inst, "PodUID") != uid || instanceTag(inst, "ClusterTag") != clusterTag {
				continue
			}
			if state := inst.State; state == nil || (state.Name != ec2types.InstanceStateNamePending && state.Name != ec2types.InstanceStateNameRunning) {
				continue
			}
			if !slices.Contains(instanceTypes, string(inst.InstanceType)) || (zone != "" && instanceZone(*inst) != zone) {
				continue
			}
			return inst, nil
		}
	}
	return nil, nil
}

func instanceTag(inst *ec2types.Instance, key string) string {
	for _, t := range inst.Tags {
		if aws.ToString(t.Key) == key {
			return aws.ToString(t.Value)
		}
	}
	return ""
}
