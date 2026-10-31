// Tests that the providerIDs the plugin issues round-trip, and that a cloud controller manager cannot read them as
// one of its own instances and delete the Node.
package nodeclaim

import (
	"net/url"
	"regexp"
	"strings"
	"testing"
)

// awsCCMInstanceID mirrors KubernetesInstanceID.MapToAWSInstanceID of the AWS cloud controller manager
// (https://github.com/kubernetes/cloud-provider-aws/blob/master/pkg/providers/v1/instances.go), which reads the last
// path segment of any providerID that does not start with aws:// as an EC2 instance ID in its own account. Its node
// lifecycle controller deletes a NotReady Node whose instance it cannot find, so a CloudVM providerID must fail here.
func awsCCMInstanceID(providerID string) (string, bool) {
	s := providerID
	if !strings.HasPrefix(s, "aws://") {
		s = "aws:///" + "/" + s
	}
	u, err := url.Parse(s)
	if err != nil || u.Scheme != "aws" {
		return "", false
	}
	tokens := strings.Split(strings.Trim(u.Path, "/"), "/")
	id := tokens[len(tokens)-1]
	if !regexp.MustCompile("^i-[^/]*$").MatchString(id) && !strings.HasPrefix(id, "fargate-") {
		return "", false
	}
	return id, true
}

func TestProviderIDIsNotAnAWSInstanceForTheCloudControllerManager(t *testing.T) {
	for _, tc := range []struct{ cloud, region, id string }{
		{"aws", "ap-southeast-5", "i-0116c2098e7c8e85a"},
		{"aws", "us-west-2", "i-1"},
		{"gcp", "us-central1", "i-1"},
	} {
		providerID := FormatProviderID(tc.cloud, tc.region, tc.id)
		if id, ok := awsCCMInstanceID(providerID); ok {
			t.Errorf("the AWS cloud controller manager reads %q as its own instance %s", providerID, id)
		}
	}
	if _, ok := awsCCMInstanceID("aws:///us-west-2a/i-0116c2098e7c8e85a"); !ok {
		t.Fatal("awsCCMInstanceID no longer recognizes an EC2 providerID")
	}
}

func TestParseProviderIDAcceptsLegacyProviderIDs(t *testing.T) {
	legacy := FormatLegacyProviderID("aws", "eu-west-1", "i-1")
	if legacy != "cloudvm://aws/eu-west-1/i-1" {
		t.Fatalf("FormatLegacyProviderID = %q", legacy)
	}
	cloud, region, id, err := ParseProviderID(legacy)
	if err != nil || cloud != "aws" || region != "eu-west-1" || id != "i-1" {
		t.Fatalf("ParseProviderID(%q) = %q %q %q %v", legacy, cloud, region, id, err)
	}
}
