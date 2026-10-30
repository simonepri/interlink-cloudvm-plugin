// Tests that the worker AWS clients assume the configured role through STS and share one session across regions.
package main

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
)

// fakeAssumeRoleAPI serves sts:AssumeRole and records each call.
type fakeAssumeRoleAPI struct {
	mu    sync.Mutex
	calls int
	form  url.Values
	auth  string
}

func (f *fakeAssumeRoleAPI) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	f.mu.Lock()
	f.calls++
	f.form, f.auth = r.PostForm, r.Header.Get("Authorization")
	f.mu.Unlock()
	w.Header().Set("Content-Type", "text/xml")
	_, _ = fmt.Fprintf(w, `<AssumeRoleResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleResult>
    <Credentials>
      <AccessKeyId>ASIAWORKER</AccessKeyId>
      <SecretAccessKey>worker-secret</SecretAccessKey>
      <SessionToken>worker-session</SessionToken>
      <Expiration>%s</Expiration>
    </Credentials>
  </AssumeRoleResult>
</AssumeRoleResponse>`, time.Now().Add(time.Hour).UTC().Format(time.RFC3339))
}

func pluginConfig(region, endpoint string) aws.Config {
	return aws.Config{
		Region:       region,
		BaseEndpoint: aws.String(endpoint),
		Credentials:  credentials.NewStaticCredentialsProvider("AKIDPLUGIN", "plugin-secret", "plugin-session"),
	}
}

func TestAWSConfigLoader_AssumesRoleOnceForEveryRegion(t *testing.T) {
	api := &fakeAssumeRoleAPI{}
	srv := httptest.NewServer(api)
	t.Cleanup(srv.Close)

	loader := &awsConfigLoader{roleARN: "arn:aws:iam::111122223333:role/workers"}
	west := loader.assumeRole(pluginConfig("us-west-2", srv.URL))
	east := loader.assumeRole(pluginConfig("us-east-1", srv.URL))
	if west.Region != "us-west-2" || east.Region != "us-east-1" {
		t.Fatalf("regions changed: %s, %s", west.Region, east.Region)
	}

	for _, cfg := range []aws.Config{west, east} {
		creds, err := cfg.Credentials.Retrieve(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if creds.AccessKeyID != "ASIAWORKER" || creds.SessionToken != "worker-session" {
			t.Fatalf("got credentials %s, want the role's", creds.AccessKeyID)
		}
	}
	if api.calls != 1 {
		t.Fatalf("assumed the role %d times, want once for every region", api.calls)
	}
	if got := api.form.Get("Action"); got != "AssumeRole" {
		t.Fatalf("got STS action %q", got)
	}
	if got := api.form.Get("RoleArn"); got != loader.roleARN {
		t.Fatalf("assumed %q, want %q", got, loader.roleARN)
	}
	if got := api.form.Get("RoleSessionName"); got != awsRoleSessionName {
		t.Fatalf("got session name %q", got)
	}
	if !strings.Contains(api.auth, "Credential=AKIDPLUGIN/") {
		t.Fatalf("AssumeRole not signed with the plugin's own credentials: %q", api.auth)
	}
}

func TestAWSConfigLoader_KeepsOwnCredentialsWithoutRole(t *testing.T) {
	loader := &awsConfigLoader{}
	cfg := loader.assumeRole(pluginConfig("us-west-2", "http://unused.invalid"))
	creds, err := cfg.Credentials.Retrieve(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if creds.AccessKeyID != "AKIDPLUGIN" {
		t.Fatalf("got credentials %s, want the plugin's own", creds.AccessKeyID)
	}
}
