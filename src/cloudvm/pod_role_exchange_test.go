// Tests the pod role exchange through the AWS SDK clients against fake EKS Auth and STS endpoints.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/eksauth"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// fakeRoleAPI serves eks-auth:AssumeRoleForPodIdentity and sts:AssumeRoleWithWebIdentity.
type fakeRoleAPI struct {
	mu       sync.Mutex
	eksPath  string
	eksAuth  string
	eksToken string
	stsForm  url.Values
}

func (f *fakeRoleAPI) handler(expiry time.Time) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/clusters/", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var in struct {
			Token string `json:"token"`
		}
		_ = json.Unmarshal(body, &in)
		f.mu.Lock()
		f.eksPath, f.eksAuth, f.eksToken = r.URL.Path, r.Header.Get("Authorization"), in.Token
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"credentials":{"accessKeyId":"ASIAPOD","secretAccessKey":"pod-secret","sessionToken":"pod-session","expiration":%d}}`, expiry.Unix())
	})
	mux.HandleFunc("/sts/", func(w http.ResponseWriter, r *http.Request) {
		_ = r.ParseForm()
		f.mu.Lock()
		f.stsForm = r.PostForm
		f.mu.Unlock()
		w.Header().Set("Content-Type", "text/xml")
		_, _ = fmt.Fprintf(w, `<AssumeRoleWithWebIdentityResponse xmlns="https://sts.amazonaws.com/doc/2011-06-15/">
  <AssumeRoleWithWebIdentityResult>
    <Credentials>
      <AccessKeyId>ASIAIRSA</AccessKeyId>
      <SecretAccessKey>irsa-secret</SecretAccessKey>
      <SessionToken>irsa-session</SessionToken>
      <Expiration>%s</Expiration>
    </Credentials>
  </AssumeRoleWithWebIdentityResult>
</AssumeRoleWithWebIdentityResponse>`, expiry.UTC().Format(time.RFC3339))
	})
	return mux
}

func TestAWSPodRoleExchanger_ExchangesThroughEKSAuthAndSTS(t *testing.T) {
	api := &fakeRoleAPI{}
	expiry := time.Now().Add(6 * time.Hour).Truncate(time.Second)
	srv := httptest.NewServer(api.handler(expiry))
	t.Cleanup(srv.Close)

	cfg := aws.Config{
		Region:      "us-west-2",
		Credentials: credentials.NewStaticCredentialsProvider("AKIDPLUGIN", "plugin-secret", "plugin-session"),
	}
	ex := NewAWSPodRoleExchanger("research",
		eksauth.NewFromConfig(cfg, func(o *eksauth.Options) { o.BaseEndpoint = aws.String(srv.URL) }),
		sts.NewFromConfig(cfg, func(o *sts.Options) { o.BaseEndpoint = aws.String(srv.URL + "/sts/") }))

	creds, err := ex.Exchange(context.Background(), PodAWSRole{Mechanism: AWSMechanismPodIdentity}, "pod-jwt", "cloudvm-uid")
	if err != nil {
		t.Fatal(err)
	}
	want := PodAWSCredentials{AccessKeyID: "ASIAPOD", SecretAccessKey: "pod-secret", SessionToken: "pod-session", Expiration: expiry.Unix()}
	if creds != want {
		t.Fatalf("got %v, want %v", creds, want)
	}
	if api.eksPath != "/clusters/research/assume-role-for-pod-identity" || api.eksToken != "pod-jwt" {
		t.Fatalf("unexpected EKS Auth call %s with token %q", api.eksPath, api.eksToken)
	}
	if !strings.Contains(api.eksAuth, "Credential=AKIDPLUGIN/") || !strings.Contains(api.eksAuth, "/us-west-2/eks-auth/aws4_request") {
		t.Fatalf("EKS Auth call not signed with the plugin's credentials: %q", api.eksAuth)
	}

	creds, err = ex.Exchange(context.Background(), PodAWSRole{Mechanism: AWSMechanismIRSA, RoleARN: "arn:aws:iam::111122223333:role/team-role"}, "web-jwt", "cloudvm-uid")
	if err != nil {
		t.Fatal(err)
	}
	if creds.AccessKeyID != "ASIAIRSA" || creds.SessionToken != "irsa-session" || creds.Expiration != expiry.Unix() {
		t.Fatalf("unexpected IRSA credentials %v", creds)
	}
	form := api.stsForm
	if form.Get("Action") != "AssumeRoleWithWebIdentity" || form.Get("RoleArn") != "arn:aws:iam::111122223333:role/team-role" ||
		form.Get("WebIdentityToken") != "web-jwt" || form.Get("RoleSessionName") != "cloudvm-uid" {
		t.Fatalf("unexpected STS request %v", form)
	}

	if _, err := NewAWSPodRoleExchanger("", nil, nil).Exchange(context.Background(), PodAWSRole{Mechanism: AWSMechanismPodIdentity}, "t", "s"); err == nil {
		t.Fatal("expected Pod Identity to fail without a cluster name")
	}
}
