// Tests console output decoding, tail truncation and failure-message enrichment.
package main

import (
	"context"
	"encoding/base64"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
)

type consoleEC2Client struct {
	*mockEC2FleetClient
	output string
	err    error
	latest bool
}

func (c *consoleEC2Client) GetConsoleOutput(_ context.Context, in *ec2.GetConsoleOutputInput, _ ...func(*ec2.Options)) (*ec2.GetConsoleOutputOutput, error) {
	c.latest = aws.ToBool(in.Latest)
	if c.err != nil {
		return nil, c.err
	}
	return &ec2.GetConsoleOutputOutput{Output: aws.String(base64.StdEncoding.EncodeToString([]byte(c.output)))}, nil
}

type consoleProvider struct {
	*MockProvider
	out string
	err error
}

func (c *consoleProvider) ConsoleOutput(context.Context, string, string) (string, error) {
	return c.out, c.err
}

func TestAWSProviderConsoleOutputDecodes(t *testing.T) {
	cl := &consoleEC2Client{mockEC2FleetClient: &mockEC2FleetClient{}, output: "boot failed: no docker"}
	p := &AWSProvider{Region: "us-east-1", client: cl}
	got, err := p.ConsoleOutput(context.Background(), "us-east-1", "i-1")
	if err != nil || got != "boot failed: no docker" || !cl.latest {
		t.Fatalf("got %q err %v latest %v", got, err, cl.latest)
	}
}

func TestTailBytesKeepsLastBytes(t *testing.T) {
	in := strings.Repeat("a", 5000) + "END"
	got := tailBytes(in, consoleTailBytes)
	if len(got) != consoleTailBytes || !strings.HasSuffix(got, "END") {
		t.Fatalf("len %d", len(got))
	}
	if got := tailBytes(strings.Repeat("é", 3000), 4095); len(got) > 4095 || !strings.HasPrefix(got, "é") {
		t.Fatalf("broken utf-8 boundary")
	}
}

func TestWithConsoleTail(t *testing.T) {
	node := NewCloudVMNode("i-1", ProviderMock, "us-east-1", "", ResourceVector{})
	cp := &consoleProvider{MockProvider: NewMockProvider(), out: "line1\nFATAL bootstrap"}
	s := NewServer(cp, "c")
	msg := s.withConsoleTail(context.Background(), node, "tunnel timeout")
	if !strings.HasPrefix(msg, "tunnel timeout") || !strings.Contains(msg, "FATAL bootstrap") {
		t.Fatalf("msg %q", msg)
	}
	cp.err = errors.New("denied")
	if got := s.withConsoleTail(context.Background(), node, "x"); got != "x" {
		t.Fatalf("error must leave message unchanged: %q", got)
	}
}
