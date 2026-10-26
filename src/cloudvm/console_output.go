// Fetches the tail of a failed worker's boot console so launch, boot, tunnel and dead-VM failures carry the bootstrap log.
package main

import (
	"context"
	"encoding/base64"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
)

const (
	// consoleTailBytes caps the console output attached to failure messages.
	consoleTailBytes = 4096
	// consoleFetchTimeout bounds the best-effort console fetch.
	consoleFetchTimeout = 10 * time.Second
)

// ConsoleOutputProvider is implemented by providers that can return an instance's boot console.
type ConsoleOutputProvider interface {
	ConsoleOutput(ctx context.Context, region, instanceID string) (string, error)
}

type ec2ConsoleClient interface {
	GetConsoleOutput(ctx context.Context, params *ec2.GetConsoleOutputInput, optFns ...func(*ec2.Options)) (*ec2.GetConsoleOutputOutput, error)
}

// ConsoleOutput returns an instance's EC2 console output (requires ec2:GetConsoleOutput). It asks for the live serial
// buffer first (Latest, Nitro instances only) and falls back to the snapshot EC2 posts shortly after a state transition
// such as stop or terminate, which is all that a non-Nitro or shut-down instance may still have.
func (p *AWSProvider) ConsoleOutput(ctx context.Context, region, instanceID string) (string, error) {
	client, err := p.getClient(ctx, region)
	if err != nil {
		return "", err
	}
	cc, ok := client.(ec2ConsoleClient)
	if !ok {
		return "", nil
	}
	out, err := getConsoleOutput(ctx, cc, instanceID, true)
	if (err != nil || strings.TrimSpace(out) == "") && ctx.Err() == nil {
		if posted, postedErr := getConsoleOutput(ctx, cc, instanceID, false); postedErr == nil {
			return posted, nil
		}
	}
	return out, err
}

func getConsoleOutput(ctx context.Context, cc ec2ConsoleClient, instanceID string, latest bool) (string, error) {
	out, err := cc.GetConsoleOutput(ctx, &ec2.GetConsoleOutputInput{
		InstanceId: aws.String(instanceID),
		Latest:     aws.Bool(latest),
	})
	if err != nil {
		return "", err
	}
	return decodeConsoleOutput(aws.ToString(out.Output)), nil
}

// decodeConsoleOutput decodes the base64 payload EC2 returns, keeping the raw text when it is not base64.
func decodeConsoleOutput(s string) string {
	if b, err := base64.StdEncoding.DecodeString(s); err == nil {
		return string(b)
	}
	return s
}

// tailBytes returns the last n bytes of s, trimmed to a valid UTF-8 boundary.
func tailBytes(s string, n int) string {
	s = strings.TrimSpace(s)
	if len(s) <= n {
		return s
	}
	s = s[len(s)-n:]
	for len(s) > 0 && !utf8.RuneStart(s[0]) {
		s = s[1:]
	}
	return s
}

// withConsoleTail appends the last 4 KiB of the node's console output to msg, best effort.
func (s *Server) withConsoleTail(ctx context.Context, node *CloudVMNode, msg string) string {
	if node == nil {
		return msg
	}
	return appendConsoleTail(msg, s.consoleTail(ctx, node.Provider, node.Region, node.InstanceID))
}

// consoleTail returns the last 4 KiB of an instance's console output, or "" when the provider cannot return it
// within consoleFetchTimeout.
func (s *Server) consoleTail(ctx context.Context, provider ProviderType, region, instanceID string) string {
	prov, ok := s.ProviderForType(provider).(ConsoleOutputProvider)
	if !ok || instanceID == "" {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), consoleFetchTimeout)
	defer cancel()
	out, err := prov.ConsoleOutput(ctx, region, instanceID)
	if err != nil {
		return ""
	}
	return tailBytes(out, consoleTailBytes)
}

// appendConsoleTail appends a non-empty console tail to msg under a separator line.
func appendConsoleTail(msg, tail string) string {
	if tail == "" {
		return msg
	}
	return msg + "\n--- console output (last 4 KiB) ---\n" + tail
}
