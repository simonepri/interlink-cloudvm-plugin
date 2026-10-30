// Package main implements the interlink-cloudvm-plugin daemon.
package main

import (
	"context"
	"encoding/base64"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
)

// KMSClient defines the subset of AWS KMS operations required for runner token encryption.
type KMSClient interface {
	Encrypt(ctx context.Context, params *kms.EncryptInput, optFns ...func(*kms.Options)) (*kms.EncryptOutput, error)
}

// NewKMSClient constructs a KMSClient from an existing AWS configuration.
func NewKMSClient(cfg aws.Config) KMSClient {
	return kms.NewFromConfig(cfg)
}

// NewKMSClientFromConfig loads the default AWS configuration for the specified region and constructs a KMSClient.
func NewKMSClientFromConfig(ctx context.Context, region string) (KMSClient, error) {
	cfg, err := workerAWSConfig.Load(ctx, region)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config for KMS in region %s: %w", region, err)
	}
	return kms.NewFromConfig(cfg), nil
}

// EncryptRunnerToken encrypts a plaintext runner authentication token using AWS KMS and returns the base64-encoded ciphertext.
func EncryptRunnerToken(ctx context.Context, client KMSClient, keyID string, plaintextToken string) (string, error) {
	if client == nil {
		return "", fmt.Errorf("kms client is required")
	}
	if keyID == "" {
		return "", fmt.Errorf("kms key ID is required")
	}
	if plaintextToken == "" {
		return "", fmt.Errorf("plaintext token is required")
	}

	out, err := client.Encrypt(ctx, &kms.EncryptInput{
		KeyId:     aws.String(keyID),
		Plaintext: []byte(plaintextToken),
	})
	if err != nil {
		return "", fmt.Errorf("failed to encrypt runner token with KMS key %s: %w", keyID, err)
	}
	if out == nil || len(out.CiphertextBlob) == 0 {
		return "", fmt.Errorf("kms encrypt returned empty ciphertext blob")
	}

	return base64.StdEncoding.EncodeToString(out.CiphertextBlob), nil
}
