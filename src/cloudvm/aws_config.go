// AWS configuration of the plugin's worker calls: EC2, KMS, pricing, quota and usage clients assume the role named by
// CLOUDVM_AWS_ROLE_ARN when it is set, while the pod token exchange keeps the plugin's own credentials.
package main

import (
	"context"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// awsRoleARNEnv names the role the plugin assumes for every AWS call except the pod token exchange, usually in the
// account that hosts the workers when that differs from the cluster's account.
const awsRoleARNEnv = "CLOUDVM_AWS_ROLE_ARN"

// awsRoleSessionName names the plugin's sessions of that role in CloudTrail.
const awsRoleSessionName = "interlink-cloudvm-plugin"

// awsConfigLoader loads the AWS configuration of the plugin's worker calls.
type awsConfigLoader struct {
	roleARN string

	once  sync.Once
	creds aws.CredentialsProvider
}

// workerAWSConfig loads the configuration of every AWS client except the pod token exchange.
var workerAWSConfig = &awsConfigLoader{roleARN: os.Getenv(awsRoleARNEnv)}

// Load returns the default AWS configuration in region, with the role's credentials when a role is set.
func (l *awsConfigLoader) Load(ctx context.Context, region string) (aws.Config, error) {
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		return aws.Config{}, err
	}
	return l.assumeRole(cfg), nil
}

// assumeRole returns cfg with the role's credentials, assumed with cfg's own credentials. Every configuration shares
// one credentials cache, so the clients of all regions reuse one session and renew it together.
func (l *awsConfigLoader) assumeRole(cfg aws.Config) aws.Config {
	if l.roleARN == "" {
		return cfg
	}
	l.once.Do(func() {
		l.creds = aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(sts.NewFromConfig(cfg), l.roleARN, func(o *stscreds.AssumeRoleOptions) {
			o.RoleSessionName = awsRoleSessionName
		}))
	})
	cfg.Credentials = l.creds
	return cfg
}
