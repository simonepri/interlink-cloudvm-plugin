// Tests per-region launch pacing configuration, queuing of launch bursts and the raised SDK retry quota.
package main

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"golang.org/x/time/rate"
)

// restoreLaunchPacing restores the package pacing settings when the test ends.
func restoreLaunchPacing(t *testing.T) {
	t.Helper()
	fleetRateLimitMu.RLock()
	r, b, q := fleetRateLimit, fleetBurstSize, awsRetryQuota
	fleetRateLimitMu.RUnlock()
	t.Cleanup(func() {
		fleetRateLimitMu.Lock()
		fleetRateLimit, fleetBurstSize, awsRetryQuota = r, b, q
		fleetRateLimiters = make(map[string]*rate.Limiter)
		fleetRateLimitMu.Unlock()
	})
}

func TestConfigureAWSLaunchPacingFromEnv(t *testing.T) {
	restoreLaunchPacing(t)
	t.Setenv("AWS_LAUNCH_RATE", "2.5")
	t.Setenv("AWS_LAUNCH_BURST", "7")
	t.Setenv("AWS_RETRY_QUOTA", "900")
	ConfigureAWSLaunchPacingFromEnv()
	l := getFleetRateLimiter("eu-west-1")
	if l.Limit() != 2.5 || l.Burst() != 7 || awsRetryQuota != 900 {
		t.Fatalf("limit %v burst %d quota %d", l.Limit(), l.Burst(), awsRetryQuota)
	}

	t.Setenv("AWS_LAUNCH_RATE", "-1")
	t.Setenv("AWS_LAUNCH_BURST", "lots")
	t.Setenv("AWS_RETRY_QUOTA", "0")
	fleetRateLimitMu.Lock()
	fleetRateLimit, fleetBurstSize, awsRetryQuota = defaultLaunchRate, defaultLaunchBurst, defaultRetryQuota
	fleetRateLimitMu.Unlock()
	ConfigureAWSLaunchPacingFromEnv()
	l = getFleetRateLimiter("eu-west-1")
	if l.Limit() != defaultLaunchRate || l.Burst() != defaultLaunchBurst || awsRetryQuota != defaultRetryQuota {
		t.Fatalf("invalid values must keep defaults: limit %v burst %d quota %d", l.Limit(), l.Burst(), awsRetryQuota)
	}
}

func TestLaunchBurstQueuesInsteadOfFailing(t *testing.T) {
	ResetFleetRateLimiters()
	defer ResetFleetRateLimiters()
	SetFleetRateLimiter("us-east-burst", rate.NewLimiter(rate.Limit(50), 2))

	var mu sync.Mutex
	var calls []time.Time
	client := &mockEC2FleetClient{
		createFleetFn: func(_ context.Context, _ *ec2.CreateFleetInput) (*ec2.CreateFleetOutput, error) {
			mu.Lock()
			calls = append(calls, time.Now())
			mu.Unlock()
			return &ec2.CreateFleetOutput{
				FleetId:   aws.String("fleet-burst"),
				Instances: []ec2types.CreateFleetInstance{{InstanceIds: []string{"i-burst"}, InstanceType: ec2types.InstanceTypeG5Xlarge}},
			}, nil
		},
	}

	const launches = 12
	start := time.Now()
	errs := make(chan error, launches)
	var wg sync.WaitGroup
	for range launches {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := LaunchFleetWithClient(context.Background(), client, &FleetRequest{
				Region: "us-east-burst", PodUID: "uid-burst", PodName: "pod-burst", PodNamespace: "default", InstanceTypes: []string{"g5.xlarge"},
			}, nil)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("queued launch failed: %v", err)
		}
	}
	// 2 burst tokens, then 10 more at 50/s: at least ~200ms.
	if len(calls) != launches || time.Since(start) < 150*time.Millisecond {
		t.Fatalf("calls %d in %v; launches were not paced", len(calls), time.Since(start))
	}
}

// drainRetryTokens takes retry tokens for a throttling error until the quota refuses one, up to limit.
func drainRetryTokens(t *testing.T, r aws.Retryer, limit int) int {
	t.Helper()
	opErr := errors.New("RequestLimitExceeded")
	for i := range limit {
		if _, err := r.GetRetryToken(context.Background(), opErr); err != nil {
			return i
		}
	}
	return limit
}

func TestEC2RetryerRaisesRetryQuota(t *testing.T) {
	restoreLaunchPacing(t)
	t.Setenv("AWS_NEW_RETRIES_2026", "")
	// The SDK default quota of 500 tokens allows 100 retries at 5 tokens each.
	if got := drainRetryTokens(t, retry.NewStandard(), 1000); got != 100 {
		t.Fatalf("SDK default allowed %d retries, want 100", got)
	}
	for _, mode := range []aws.RetryMode{aws.RetryModeStandard, aws.RetryModeAdaptive} {
		if got := drainRetryTokens(t, newEC2Retryer(mode), 1000); got != defaultRetryQuota/5 {
			t.Fatalf("%s mode allowed %d retries, want %d", mode, got, defaultRetryQuota/5)
		}
	}
	if _, ok := newEC2Retryer(aws.RetryModeAdaptive).(*retry.AdaptiveMode); !ok {
		t.Fatalf("adaptive retry mode was not kept")
	}
}

func TestAWSProviderClientUsesRaisedRetryQuota(t *testing.T) {
	restoreLaunchPacing(t)
	t.Setenv("AWS_NEW_RETRIES_2026", "")
	t.Setenv("REGION_STORE_PATH", t.TempDir()+"/regions.json")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	p := NewAWSProvider("us-east-1")
	if _, err := p.getClient(context.Background(), "ap-south-1"); err != nil {
		t.Fatalf("getClient: %v", err)
	}
	cl := p.ec2Clients["ap-south-1"]
	if got := drainRetryTokens(t, cl.Options().Retryer, 1000); got != defaultRetryQuota/5 {
		t.Fatalf("regional client allowed %d retries, want %d", got, defaultRetryQuota/5)
	}
}
