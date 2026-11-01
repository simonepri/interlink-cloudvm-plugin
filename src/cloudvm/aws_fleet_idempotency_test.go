// Tests that CreateFleet client tokens never name two different requests and that a relaunch reuses a VM an earlier
// launch of the same claim already started.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ec2"
	ec2types "github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/aws/smithy-go"
)

// idempotentFleetEC2 answers CreateFleet like EC2 does for client tokens: a token seen with the same parameters
// replays the first answer, and a token seen with other parameters fails with IdempotentParameterMismatch. One fake
// serves every region, which is stricter than EC2's per-region scope: a token must never name two requests.
type idempotentFleetEC2 struct {
	mockEC2FleetClient
	mu      sync.Mutex
	seen    map[string]string // client token -> parameters
	answers map[string]*ec2.CreateFleetOutput
	tokens  []string
	// launch decides the answer to a first request: an instance ID to launch, or "" for no capacity.
	launch func(params *ec2.CreateFleetInput) string
}

func newIdempotentFleetEC2(launch func(params *ec2.CreateFleetInput) string) *idempotentFleetEC2 {
	return &idempotentFleetEC2{seen: map[string]string{}, answers: map[string]*ec2.CreateFleetOutput{}, launch: launch}
}

func (f *idempotentFleetEC2) CreateFleet(_ context.Context, params *ec2.CreateFleetInput, _ ...func(*ec2.Options)) (*ec2.CreateFleetOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	token := aws.ToString(params.ClientToken)
	f.tokens = append(f.tokens, token)
	unsigned := *params
	unsigned.ClientToken = nil
	encoded, _ := json.Marshal(&unsigned) //nolint:gosec // ClientToken is an idempotency key, not a secret, and is nil here
	if prev, ok := f.seen[token]; ok {
		if prev != string(encoded) {
			return nil, &smithy.GenericAPIError{Code: "IdempotentParameterMismatch", Message: "The client token you have provided is associated with a resource that is already deleted or was created with different parameters."}
		}
		return f.answers[token], nil
	}
	out := &ec2.CreateFleetOutput{FleetId: aws.String("fleet-" + token)}
	if id := f.launch(params); id != "" {
		itype := params.LaunchTemplateConfigs[0].Overrides[0].InstanceType
		out.Instances = []ec2types.CreateFleetInstance{{InstanceIds: []string{id}, InstanceType: itype}}
	} else {
		out.Errors = []ec2types.CreateFleetError{{ErrorCode: aws.String("UnfulfillableCapacity"), ErrorMessage: aws.String("Unable to fulfill capacity due to your request configuration.")}}
	}
	f.seen[token], f.answers[token] = string(encoded), out
	return out, nil
}

func fleetInstanceType(params *ec2.CreateFleetInput) string {
	return string(params.LaunchTemplateConfigs[0].Overrides[0].InstanceType)
}

func spotClaimSpec(uid, region, itype string) *InstanceSpec {
	return &InstanceSpec{
		PodName:          "cloudvm-spot-claim",
		PodUID:           uid,
		Region:           region,
		InstanceType:     itype,
		MarketType:       "spot",
		ClusterTag:       "test-cluster",
		LaunchTemplateID: "lt-1234",
		NodeClaim:        true,
	}
}

func TestLaunchRetryOfClaimInAnotherRegionUsesNewClientToken(t *testing.T) {
	t.Setenv("REGION_STORE_PATH", filepath.Join(t.TempDir(), "regions.json"))
	ResetFleetRateLimiters()
	defer ResetFleetRateLimiters()
	p := NewAWSProvider("eu-west-2", "ap-south-2")
	fake := newIdempotentFleetEC2(func(params *ec2.CreateFleetInput) string {
		if fleetInstanceType(params) == "g6.2xlarge" {
			return "i-second"
		}
		return ""
	})
	p.SetRegionClient("eu-west-2", fake)
	p.SetRegionClient("ap-south-2", fake)
	uid := claimPodUIDPrefix + "cloudvm-spot-region"

	if _, err := p.Launch(context.Background(), spotClaimSpec(uid, "eu-west-2", "g6.xlarge")); !errors.Is(err, errNoInstancesLaunched) {
		t.Fatalf("first launch err = %v, want no capacity", err)
	}
	inst, err := p.Launch(context.Background(), spotClaimSpec(uid, "ap-south-2", "g6.2xlarge"))
	if err != nil {
		t.Fatalf("second launch of the claim in another region: %v", err)
	}
	if inst.ID != "i-second" || inst.Region != "ap-south-2" {
		t.Fatalf("instance = %s in %s, want i-second in ap-south-2", inst.ID, inst.Region)
	}
}

func TestLaunchRetryOfClaimWithAnotherOfferingUsesNewClientToken(t *testing.T) {
	t.Setenv("REGION_STORE_PATH", filepath.Join(t.TempDir(), "regions.json"))
	ResetFleetRateLimiters()
	defer ResetFleetRateLimiters()
	p := NewAWSProvider("eu-west-2")
	fake := newIdempotentFleetEC2(func(params *ec2.CreateFleetInput) string {
		if fleetInstanceType(params) == "g6.2xlarge" {
			return "i-second"
		}
		return ""
	})
	p.SetRegionClient("eu-west-2", fake)
	uid := claimPodUIDPrefix + "cloudvm-spot-offering"

	if _, err := p.Launch(context.Background(), spotClaimSpec(uid, "eu-west-2", "g6.xlarge")); !errors.Is(err, errNoInstancesLaunched) {
		t.Fatalf("first launch err = %v, want no capacity", err)
	}
	spec := spotClaimSpec(uid, "eu-west-2", "g6.2xlarge")
	if _, err := p.Launch(context.Background(), spec); err != nil {
		t.Fatalf("launch of the claim's next offering in the same region: %v", err)
	}
}

func TestLaunchFleetAfterRestartWithNewUserDataUsesNewClientToken(t *testing.T) {
	ResetFleetRateLimiters()
	defer ResetFleetRateLimiters()
	fake := newIdempotentFleetEC2(func(*ec2.CreateFleetInput) string { return "" })
	req := func(userData string) *FleetRequest {
		return &FleetRequest{Region: "eu-west-2", PodUID: "nodeclaim-restart", PodName: "restart", InstanceTypes: []string{"g6.xlarge"}, UserData: userData}
	}
	overrides := BuildFleetOverrides([]string{"g6.xlarge"}, nil)
	if _, err := LaunchFleetWithClient(context.Background(), fake, req("ciphertext-before-restart"), overrides); err != nil {
		t.Fatal(err)
	}
	// A restarted plugin encrypts the runner token again, and a KMS ciphertext differs on every call.
	if _, err := LaunchFleetWithClient(context.Background(), fake, req("ciphertext-after-restart"), overrides); err != nil {
		t.Fatalf("launch with the user data of a restarted plugin: %v", err)
	}
}

func TestLaunchFleetRepeatsTokenOnlyWhileTheOutcomeIsUnknown(t *testing.T) {
	ResetFleetRateLimiters()
	defer ResetFleetRateLimiters()
	failNext := true
	var tokens []string
	client := &mockEC2FleetClient{createFleetFn: func(_ context.Context, params *ec2.CreateFleetInput) (*ec2.CreateFleetOutput, error) {
		tokens = append(tokens, aws.ToString(params.ClientToken))
		if failNext {
			failNext = false
			return nil, &smithy.GenericAPIError{Code: "InternalError", Message: "An internal error has occurred."}
		}
		return &ec2.CreateFleetOutput{}, nil
	}}
	req := &FleetRequest{Region: "eu-west-2", PodUID: "nodeclaim-outcome", PodName: "outcome", InstanceTypes: []string{"g6.xlarge"}}
	overrides := BuildFleetOverrides([]string{"g6.xlarge"}, nil)
	for range 3 {
		_, _ = LaunchFleetWithClient(context.Background(), client, req, overrides)
	}
	if tokens[0] != tokens[1] {
		t.Fatalf("token after an internal error = %s, want the same %s, since the first request may have launched", tokens[1], tokens[0])
	}
	if tokens[1] == tokens[2] {
		t.Fatalf("token after a fleet that launched nothing = %s, want a new one so EC2 tries again", tokens[2])
	}
	if len(tokens[0]) > 64 {
		t.Fatalf("token %q is longer than the 64 characters EC2 accepts", tokens[0])
	}
}

func TestLaunchFleetMovesOffAMismatchedToken(t *testing.T) {
	ResetFleetRateLimiters()
	defer ResetFleetRateLimiters()
	var tokens []string
	client := &mockEC2FleetClient{createFleetFn: func(_ context.Context, params *ec2.CreateFleetInput) (*ec2.CreateFleetOutput, error) {
		tokens = append(tokens, aws.ToString(params.ClientToken))
		return nil, &smithy.GenericAPIError{Code: "IdempotentParameterMismatch", Message: "different parameters"}
	}}
	req := &FleetRequest{Region: "eu-west-2", PodUID: "nodeclaim-mismatch", PodName: "mismatch", InstanceTypes: []string{"g6.xlarge"}}
	overrides := BuildFleetOverrides([]string{"g6.xlarge"}, nil)
	_, err := LaunchFleetWithClient(context.Background(), client, req, overrides)
	if !errors.Is(err, errFleetTokenReused) {
		t.Fatalf("err = %v, want errFleetTokenReused", err)
	}
	_, _ = LaunchFleetWithClient(context.Background(), client, req, overrides)
	if len(tokens) != 2 || tokens[0] == tokens[1] {
		t.Fatalf("tokens = %v, want one call per launch and a new token after the mismatch", tokens)
	}
}

func TestLaunchReusesTheVMAnEarlierLaunchOfTheClaimStarted(t *testing.T) {
	t.Setenv("REGION_STORE_PATH", filepath.Join(t.TempDir(), "regions.json"))
	ResetFleetRateLimiters()
	defer ResetFleetRateLimiters()
	p := NewAWSProvider("eu-west-2")
	uid := claimPodUIDPrefix + "cloudvm-spot-restart"
	running := ec2types.Instance{
		InstanceId:   aws.String("i-before-restart"),
		InstanceType: "g6.xlarge",
		State:        &ec2types.InstanceState{Name: ec2types.InstanceStateNameRunning},
		Placement:    &ec2types.Placement{AvailabilityZone: aws.String("eu-west-2a")},
		Tags: []ec2types.Tag{
			{Key: aws.String("PodUID"), Value: aws.String(uid)},
			{Key: aws.String("ClusterTag"), Value: aws.String("test-cluster")},
		},
	}
	creates := 0
	p.SetRegionClient("eu-west-2", &mockEC2FleetClient{
		createFleetFn: func(context.Context, *ec2.CreateFleetInput) (*ec2.CreateFleetOutput, error) {
			creates++
			return &ec2.CreateFleetOutput{}, nil
		},
		describeInstancesFn: func(context.Context, *ec2.DescribeInstancesInput) (*ec2.DescribeInstancesOutput, error) {
			return &ec2.DescribeInstancesOutput{Reservations: []ec2types.Reservation{{Instances: []ec2types.Instance{running}}}}, nil
		},
	})

	spec := spotClaimSpec(uid, "eu-west-2", "g6.xlarge")
	spec.Zone = "eu-west-2a"
	inst, err := p.Launch(context.Background(), spec)
	if err != nil {
		t.Fatal(err)
	}
	if inst.ID != "i-before-restart" || creates != 0 {
		t.Fatalf("instance = %s after %d CreateFleet calls, want i-before-restart and none", inst.ID, creates)
	}

	spec = spotClaimSpec(uid, "eu-west-2", "g6.2xlarge")
	if _, err := p.Launch(context.Background(), spec); creates != 1 {
		t.Fatalf("CreateFleet calls = %d (err %v), want a launch for an offering the running VM does not match", creates, err)
	}
}
