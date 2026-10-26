// Fetches the tail of a failed worker's boot console so launch, boot and tunnel failures carry the bootstrap log.
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

// ConsoleOutput returns the latest EC2 console output of an instance (requires ec2:GetConsoleOutput).
func (p *AWSProvider) ConsoleOutput(ctx context.Context, region, instanceID string) (string, error) {
	client, err := p.getClient(ctx, region)
	if err != nil {
		return "", err
	}
	cc, ok := client.(ec2ConsoleClient)
	if !ok {
		return "", nil
	}
	out, err := cc.GetConsoleOutput(ctx, &ec2.GetConsoleOutputInput{
		InstanceId: aws.String(instanceID),
		Latest:     aws.Bool(true),
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
	prov, ok := s.ProviderForType(node.Provider).(ConsoleOutputProvider)
	if !ok {
		return msg
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), consoleFetchTimeout)
	defer cancel()
	out, err := prov.ConsoleOutput(ctx, node.Region, node.InstanceID)
	tail := tailBytes(out, consoleTailBytes)
	if err != nil || tail == "" {
		return msg
	}
	return msg + "\n--- console output (last 4 KiB) ---\n" + tail
}
