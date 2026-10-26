// Paces EC2 launch calls per region with a client-side token bucket and sizes the SDK retry quota, so a burst of pod
// creates queues for EC2 API capacity instead of failing with RequestLimitExceeded or "retry quota exceeded".
package main

import (
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"golang.org/x/time/rate"
)

// EC2 throttles CreateFleet in the mutating-actions request bucket: 50 tokens refilled at 5 per second, per account
// and region (https://docs.aws.amazon.com/ec2/latest/devguide/ec2-api-throttling.html). The defaults stay below
// that so other callers in the account keep headroom.
const (
	defaultLaunchRate  = rate.Limit(4)
	defaultLaunchBurst = 20
	// defaultRetryQuota is five times the SDK's 500-token retry quota, which a sustained burst of throttled calls
	// drains in 100 retries; once empty, the SDK fails every call with "retry quota exceeded" instead of retrying.
	defaultRetryQuota = 2500
)

var (
	fleetRateLimitMu  sync.RWMutex
	fleetRateLimiters = make(map[string]*rate.Limiter)
	fleetRateLimit    = defaultLaunchRate
	fleetBurstSize    = defaultLaunchBurst

	awsRetryQuota uint = defaultRetryQuota
)

// ConfigureAWSLaunchPacingFromEnv applies AWS_LAUNCH_RATE (launch calls per second per region), AWS_LAUNCH_BURST and
// AWS_RETRY_QUOTA, keeping the default for any value that is unset or not positive. Call it before any client exists.
func ConfigureAWSLaunchPacingFromEnv() {
	fleetRateLimitMu.Lock()
	defer fleetRateLimitMu.Unlock()
	if raw := strings.TrimSpace(os.Getenv("AWS_LAUNCH_RATE")); raw != "" {
		if v, err := strconv.ParseFloat(raw, 64); err == nil && v > 0 {
			fleetRateLimit = rate.Limit(v)
		} else {
			log.Print("[aws] Warning: ignoring AWS_LAUNCH_RATE, which is not a positive number")
		}
	}
	if raw := strings.TrimSpace(os.Getenv("AWS_LAUNCH_BURST")); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			fleetBurstSize = v
		} else {
			log.Print("[aws] Warning: ignoring AWS_LAUNCH_BURST, which is not a positive number")
		}
	}
	if raw := strings.TrimSpace(os.Getenv("AWS_RETRY_QUOTA")); raw != "" {
		if v, err := strconv.ParseUint(raw, 10, 32); err == nil && v > 0 {
			awsRetryQuota = uint(v)
		} else {
			log.Print("[aws] Warning: ignoring AWS_RETRY_QUOTA, which is not a positive number")
		}
	}
	fleetRateLimiters = make(map[string]*rate.Limiter)
}

// getFleetRateLimiter returns the launch rate limiter for a region (or global if region is empty).
func getFleetRateLimiter(region string) *rate.Limiter {
	fleetRateLimitMu.RLock()
	limiter, exists := fleetRateLimiters[region]
	fleetRateLimitMu.RUnlock()
	if exists && limiter != nil {
		return limiter
	}

	fleetRateLimitMu.Lock()
	defer fleetRateLimitMu.Unlock()
	if limiter, exists := fleetRateLimiters[region]; exists && limiter != nil {
		return limiter
	}

	limiter = rate.NewLimiter(fleetRateLimit, fleetBurstSize)
	fleetRateLimiters[region] = limiter
	return limiter
}

// SetFleetRateLimiter configures the rate limiter for a specific region (or empty string for global).
func SetFleetRateLimiter(region string, limiter *rate.Limiter) {
	fleetRateLimitMu.Lock()
	defer fleetRateLimitMu.Unlock()
	fleetRateLimiters[region] = limiter
}

// ResetFleetRateLimiters resets the regional rate limiters map.
func ResetFleetRateLimiters() {
	fleetRateLimitMu.Lock()
	defer fleetRateLimitMu.Unlock()
	fleetRateLimiters = make(map[string]*rate.Limiter)
}

// newEC2Retryer returns the SDK retryer for one regional EC2 client: the retry mode the SDK resolved from
// AWS_RETRY_MODE, with the retry quota raised to awsRetryQuota. Adaptive mode builds on the standard retryer and
// keeps the same quota, so it needs the raise as well.
func newEC2Retryer(mode aws.RetryMode) aws.Retryer {
	fleetRateLimitMu.RLock()
	quota := awsRetryQuota
	fleetRateLimitMu.RUnlock()
	withQuota := func(o *retry.StandardOptions) {
		o.RateLimiter = ratelimit.NewTokenRateLimit(quota)
	}
	if mode == aws.RetryModeAdaptive {
		return retry.NewAdaptiveMode(func(o *retry.AdaptiveModeOptions) {
			o.StandardOptions = append(o.StandardOptions, withQuota)
		})
	}
	return retry.NewStandard(withQuota)
}
