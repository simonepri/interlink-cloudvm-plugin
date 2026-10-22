// Package main implements the interlink-provider-cloudvm daemon.
package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
)

type mockKMSClient struct {
	encryptFunc func(ctx context.Context, params *kms.EncryptInput, optFns ...func(*kms.Options)) (*kms.EncryptOutput, error)
	lastInput   *kms.EncryptInput
	calls       int
}

func (m *mockKMSClient) Encrypt(ctx context.Context, params *kms.EncryptInput, optFns ...func(*kms.Options)) (*kms.EncryptOutput, error) {
	m.calls++
	m.lastInput = params
	if m.encryptFunc != nil {
		return m.encryptFunc(ctx, params, optFns...)
	}
	return &kms.EncryptOutput{
		CiphertextBlob: []byte("encrypted-ciphertext-blob"),
		KeyId:          params.KeyId,
	}, nil
}

func TestEncryptRunnerToken_Success(t *testing.T) {
	ctx := context.Background()
	mock := &mockKMSClient{}
	keyID := "arn:aws:kms:us-east-1:123456789012:key/test-key-id"
	plaintext := "my-secret-runner-auth-token-12345"

	ciphertextB64, err := EncryptRunnerToken(ctx, mock, keyID, plaintext)
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	if mock.calls != 1 {
		t.Errorf("expected 1 call to Encrypt, got %d", mock.calls)
	}
	if mock.lastInput == nil {
		t.Fatal("expected EncryptInput to be captured")
	}
	if mock.lastInput.KeyId == nil || *mock.lastInput.KeyId != keyID {
		t.Errorf("expected keyId %q, got %v", keyID, mock.lastInput.KeyId)
	}
	if string(mock.lastInput.Plaintext) != plaintext {
		t.Errorf("expected plaintext %q, got %q", plaintext, string(mock.lastInput.Plaintext))
	}

	expectedB64 := base64.StdEncoding.EncodeToString([]byte("encrypted-ciphertext-blob"))
	if ciphertextB64 != expectedB64 {
		t.Errorf("expected ciphertext %q, got %q", expectedB64, ciphertextB64)
	}
}

func TestEncryptRunnerToken_ValidationAndErrors(t *testing.T) {
	ctx := context.Background()
	mock := &mockKMSClient{}

	tests := []struct {
		name        string
		client      KMSClient
		keyID       string
		plaintext   string
		setupMock   func(*mockKMSClient)
		errContains string
	}{
		{
			name:        "nil client",
			client:      nil,
			keyID:       "key-1",
			plaintext:   "token",
			errContains: "kms client is required",
		},
		{
			name:        "empty key ID",
			client:      mock,
			keyID:       "",
			plaintext:   "token",
			errContains: "kms key ID is required",
		},
		{
			name:        "empty plaintext token",
			client:      mock,
			keyID:       "key-1",
			plaintext:   "",
			errContains: "plaintext token is required",
		},
		{
			name:      "kms provider error",
			client:    mock,
			keyID:     "key-1",
			plaintext: "token",
			setupMock: func(m *mockKMSClient) {
				m.encryptFunc = func(_ context.Context, _ *kms.EncryptInput, _ ...func(*kms.Options)) (*kms.EncryptOutput, error) {
					return nil, errors.New("access denied to kms key")
				}
			},
			errContains: "access denied to kms key",
		},
		{
			name:      "kms returned nil output",
			client:    mock,
			keyID:     "key-1",
			plaintext: "token",
			setupMock: func(m *mockKMSClient) {
				m.encryptFunc = func(_ context.Context, _ *kms.EncryptInput, _ ...func(*kms.Options)) (*kms.EncryptOutput, error) {
					return nil, nil
				}
			},
			errContains: "empty ciphertext blob",
		},
		{
			name:      "kms returned empty ciphertext blob",
			client:    mock,
			keyID:     "key-1",
			plaintext: "token",
			setupMock: func(m *mockKMSClient) {
				m.encryptFunc = func(_ context.Context, _ *kms.EncryptInput, _ ...func(*kms.Options)) (*kms.EncryptOutput, error) {
					return &kms.EncryptOutput{CiphertextBlob: []byte{}}, nil
				}
			},
			errContains: "empty ciphertext blob",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.setupMock != nil {
				tc.setupMock(mock)
			}
			_, err := EncryptRunnerToken(ctx, tc.client, tc.keyID, tc.plaintext)
			if err == nil {
				t.Fatalf("expected error containing %q, got nil", tc.errContains)
			}
			if !strings.Contains(err.Error(), tc.errContains) {
				t.Errorf("expected error containing %q, got %q", tc.errContains, err.Error())
			}
		})
	}
}

func TestNewKMSClient(t *testing.T) {
	cfg := aws.Config{Region: "us-east-1"}
	client := NewKMSClient(cfg)
	if client == nil {
		t.Fatal("expected non-nil KMSClient from NewKMSClient")
	}
}

func TestCloudInit_KMSMode_ScriptGeneration(t *testing.T) {
	expectedCmdSnippet := "aws kms decrypt --ciphertext-blob fileb://<(echo \"$CIPHERTEXT\" | base64 -d)"

	t.Run("with KMSEncryptedToken true and KMSCiphertext", func(t *testing.T) {
		cfg := &CloudInitConfig{
			PodName:           "kms-worker",
			PodNamespace:      "default",
			Region:            "us-west-2",
			KMSCiphertext:     "base64-encrypted-blob-abc",
			KMSEncryptedToken: true,
		}

		script, err := GenerateCloudInit(cfg)
		if err != nil {
			t.Fatalf("GenerateCloudInit failed: %v", err)
		}

		if !strings.Contains(script, expectedCmdSnippet) {
			t.Errorf("cloud-init missing expected KMS decrypt command: %q\ngot script:\n%s", expectedCmdSnippet, script)
		}
		if !strings.Contains(script, "base64-encrypted-blob-abc") {
			t.Errorf("cloud-init missing KMSCiphertext string in script")
		}
		if !strings.Contains(script, "/etc/cloudvm/runner-token") {
			t.Errorf("cloud-init missing runner token destination path")
		}
		// In KMS mode, plaintext token must NOT appear in script
		if strings.Contains(script, "echo -n \"\" > /etc/cloudvm/runner-token") {
			t.Errorf("cloud-init leaked plaintext echo branch")
		}
	})

	t.Run("KMS encryption fails if ciphertext is empty", func(t *testing.T) {
		cfg := &CloudInitConfig{
			PodName:           "kms-missing-ct",
			PodNamespace:      "default",
			KMSEncryptedToken: true,
			KMSCiphertext:     "",
		}
		_, err := GenerateCloudInit(cfg)
		if err == nil {
			t.Fatal("expected error when KMSCiphertext is empty, got nil")
		}
	})
}

func TestCloudInit_PlaintextMode_ScriptGeneration(t *testing.T) {
	kmsCmdSnippet := "aws kms decrypt"

	cfg := &CloudInitConfig{
		PodName:           "plain-worker",
		PodNamespace:      "default",
		Region:            "us-east-1",
		RunnerAuthToken:   "test-plaintext-token-xyz", //nolint:gosec // test mock token
		KMSEncryptedToken: false,
		KMSKeyID:          "",
	}

	script, err := GenerateCloudInit(cfg)
	if err != nil {
		t.Fatalf("GenerateCloudInit failed: %v", err)
	}

	if strings.Contains(script, kmsCmdSnippet) {
		t.Errorf("cloud-init should NOT contain %q in plaintext mode", kmsCmdSnippet)
	}
	if !strings.Contains(script, `echo -n "test-plaintext-token-xyz" > /etc/cloudvm/runner-token`) {
		t.Errorf("cloud-init missing plaintext token file write")
	}
	// Verify runner Python source loads token dynamically without hardcoding it
	if !strings.Contains(script, `with open('/etc/cloudvm/runner-token', 'r') as f:`) {
		t.Errorf("cloud-init runner should load token from /etc/cloudvm/runner-token at runtime")
	}
}

func TestServer_KMS_EndToEnd(t *testing.T) {
	mockProv := NewMockProvider()
	server := NewServer(mockProv, "test-cluster")
	kmsMock := &mockKMSClient{}
	keyID := "arn:aws:kms:us-east-1:123456789012:key/test-runner-key"
	server.SetKMSConfig(kmsMock, keyID)

	pod := Pod{
		Metadata: PodMeta{
			Name:      "test-kms-pod",
			Namespace: "default",
			UID:       "pod-kms-1234",
			Annotations: map[string]string{
				"cloudvm.vk.io/provider": "aws",
			},
		},
		Spec: PodSpecWrapper{
			Containers: []ContainerSpec{
				{
					Name:  "worker",
					Image: "alpine:latest",
				},
			},
		},
	}

	body, err := json.Marshal(pod)
	if err != nil {
		t.Fatalf("marshal pod failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/create", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	server.Routes().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d: %s", rec.Code, rec.Body.String())
	}

	// 1. Verify Encrypt was called with keyID and captured plaintext
	if kmsMock.calls != 1 {
		t.Fatalf("expected 1 call to kmsClient.Encrypt, got %d", kmsMock.calls)
	}
	if kmsMock.lastInput == nil || *kmsMock.lastInput.KeyId != keyID {
		t.Fatalf("expected KMS KeyID %s, got: %v", keyID, kmsMock.lastInput.KeyId)
	}

	// 2. Verify InstanceSpec received ciphertext and KMSEncryptedToken = true
	lastSpec := mockProv.LastSpec()
	if lastSpec == nil {
		t.Fatal("expected non-nil lastSpec on mock provider")
	}
	if !lastSpec.KMSEncryptedToken {
		t.Errorf("expected lastSpec.KMSEncryptedToken to be true")
	}
	expectedB64 := base64.StdEncoding.EncodeToString([]byte("encrypted-ciphertext-blob"))
	if lastSpec.KMSCiphertext != expectedB64 {
		t.Errorf("expected KMSCiphertext %s, got: %s", expectedB64, lastSpec.KMSCiphertext)
	}

	// 3. Verify plaintext token is retained in tokenStore for daemon mesh calls
	storedTok := server.tokenStore.Get("pod-kms-1234")
	if storedTok == "" {
		t.Errorf("expected plaintext token retained in TokenStore, got empty")
	}
	if storedTok != string(kmsMock.lastInput.Plaintext) {
		t.Errorf("expected TokenStore to hold plaintext token %s, got %s", string(kmsMock.lastInput.Plaintext), storedTok)
	}
}
