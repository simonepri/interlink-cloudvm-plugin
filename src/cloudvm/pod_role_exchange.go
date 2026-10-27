// AWS token exchange for pod roles: eks-auth:AssumeRoleForPodIdentity for EKS Pod Identity and
// sts:AssumeRoleWithWebIdentity for IRSA, called from the plugin in the cluster's account.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/eksauth"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// EKSAuthAPI is the subset of the EKS Auth client the exchange uses.
type EKSAuthAPI interface {
	AssumeRoleForPodIdentity(ctx context.Context, in *eksauth.AssumeRoleForPodIdentityInput, optFns ...func(*eksauth.Options)) (*eksauth.AssumeRoleForPodIdentityOutput, error)
}

// STSWebIdentityAPI is the subset of the STS client the exchange uses.
type STSWebIdentityAPI interface {
	AssumeRoleWithWebIdentity(ctx context.Context, in *sts.AssumeRoleWithWebIdentityInput, optFns ...func(*sts.Options)) (*sts.AssumeRoleWithWebIdentityOutput, error)
}

// AWSPodRoleExchanger exchanges pod tokens through EKS Auth for the cluster clusterName and through STS.
type AWSPodRoleExchanger struct {
	clusterName string
	eksAuth     EKSAuthAPI
	sts         STSWebIdentityAPI
}

// NewAWSPodRoleExchanger creates an exchanger from the given clients. Without a cluster name, Pod Identity
// exchanges fail, since EKS Auth looks the association up by cluster.
func NewAWSPodRoleExchanger(clusterName string, eksAuth EKSAuthAPI, stsClient STSWebIdentityAPI) *AWSPodRoleExchanger {
	return &AWSPodRoleExchanger{clusterName: clusterName, eksAuth: eksAuth, sts: stsClient}
}

// NewAWSPodRoleExchangerFromConfig creates an exchanger with the plugin's own AWS credentials in the cluster's region.
func NewAWSPodRoleExchangerFromConfig(ctx context.Context, clusterName, region string) (*AWSPodRoleExchanger, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return nil, fmt.Errorf("load AWS config: %w", err)
	}
	return NewAWSPodRoleExchanger(clusterName, eksauth.NewFromConfig(cfg), sts.NewFromConfig(cfg)), nil
}

// Exchange returns the role's credentials for the pod token.
func (e *AWSPodRoleExchanger) Exchange(ctx context.Context, role PodAWSRole, token, sessionName string) (PodAWSCredentials, error) {
	switch role.Mechanism {
	case AWSMechanismPodIdentity:
		if e.clusterName == "" {
			return PodAWSCredentials{}, errors.New("EKS_CLUSTER_NAME is unset")
		}
		out, err := e.eksAuth.AssumeRoleForPodIdentity(ctx, &eksauth.AssumeRoleForPodIdentityInput{
			ClusterName: aws.String(e.clusterName),
			Token:       aws.String(token),
		})
		if err != nil {
			return PodAWSCredentials{}, err
		}
		c := out.Credentials
		if c == nil || c.Expiration == nil {
			return PodAWSCredentials{}, errors.New("EKS Auth returned no credentials")
		}
		return PodAWSCredentials{
			AccessKeyID:     aws.ToString(c.AccessKeyId),
			SecretAccessKey: aws.ToString(c.SecretAccessKey),
			SessionToken:    aws.ToString(c.SessionToken),
			Expiration:      c.Expiration.Unix(),
		}, nil
	case AWSMechanismIRSA:
		out, err := e.sts.AssumeRoleWithWebIdentity(ctx, &sts.AssumeRoleWithWebIdentityInput{
			RoleArn:          aws.String(role.RoleARN),
			RoleSessionName:  aws.String(sessionName),
			WebIdentityToken: aws.String(token),
		})
		if err != nil {
			return PodAWSCredentials{}, err
		}
		c := out.Credentials
		if c == nil || c.Expiration == nil {
			return PodAWSCredentials{}, errors.New("STS returned no credentials")
		}
		return PodAWSCredentials{
			AccessKeyID:     aws.ToString(c.AccessKeyId),
			SecretAccessKey: aws.ToString(c.SecretAccessKey),
			SessionToken:    aws.ToString(c.SessionToken),
			Expiration:      c.Expiration.Unix(),
		}, nil
	default:
		return PodAWSCredentials{}, fmt.Errorf("unknown mechanism %q", role.Mechanism)
	}
}

// NewPodCredentialsFromEnv creates the PodCredentials of an in-cluster plugin for the EKS cluster EKS_CLUSTER_NAME in
// EKS_CLUSTER_REGION, which defaults to defaultRegion.
func NewPodCredentialsFromEnv(ctx context.Context, defaultRegion string) (*PodCredentials, error) {
	region := os.Getenv("EKS_CLUSTER_REGION")
	if region == "" {
		region = defaultRegion
	}
	exchanger, err := NewAWSPodRoleExchangerFromConfig(ctx, os.Getenv("EKS_CLUSTER_NAME"), region)
	if err != nil {
		return nil, err
	}
	return NewInClusterPodCredentials(exchanger)
}
