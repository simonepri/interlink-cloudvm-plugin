// Tests that the HTTP client speaks the documented paths and maps error reasons and provider IDs as the contract says.

package nodeclaim

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
)

func TestClientRoundTrip(t *testing.T) {
	var gotCreate CreateRequest
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+PathNodeClaims, func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&gotCreate); err != nil {
			t.Errorf("decode create: %v", err)
		}
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(NodeClaim{Name: gotCreate.Name, ProviderID: FormatProviderID("aws", "eu-west-1", "i-1")})
	})
	mux.HandleFunc("GET "+PathNodeClaims, func(w http.ResponseWriter, r *http.Request) {
		items := []NodeClaim{{Name: "a", ProviderID: "cloudvm://aws/eu-west-1/i-1/vm"}}
		if id := r.URL.Query().Get("providerID"); id != "" && id != items[0].ProviderID {
			items = nil
		}
		_ = json.NewEncoder(w).Encode(List{Items: items})
	})
	mux.HandleFunc("GET "+PathNodeClaims+"/{name}", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(ErrorResponse{Reason: ReasonNotFound, Message: r.PathValue("name")})
	})
	mux.HandleFunc("DELETE "+PathNodeClaims+"/{name}", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	})
	mux.HandleFunc("GET "+PathCatalog, func(w http.ResponseWriter, r *http.Request) {
		if got := r.URL.Query().Get("regions"); got != "a,b" {
			t.Errorf("regions = %q", got)
		}
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(ErrorResponse{Reason: ReasonInsufficientCapacity})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()
	c := NewClient(srv.URL+"/", nil)
	ctx := context.Background()

	claim, err := c.Create(ctx, &CreateRequest{Name: "a", InstanceType: "g5.xlarge", Region: "eu-west-1", CapacityType: CapacityTypeSpot})
	if err != nil || claim.ProviderID != "cloudvm://aws/eu-west-1/i-1/vm" || gotCreate.InstanceType != "g5.xlarge" {
		t.Fatalf("Create = %+v, %v (sent %+v)", claim, err, gotCreate)
	}
	if items, err := c.List(ctx); err != nil || len(items) != 1 {
		t.Fatalf("List = %v, %v", items, err)
	}
	if got, err := c.GetByProviderID(ctx, "cloudvm://aws/eu-west-1/i-1/vm"); err != nil || got.Name != "a" {
		t.Fatalf("GetByProviderID = %+v, %v", got, err)
	}
	if _, err := c.GetByProviderID(ctx, "cloudvm://aws/eu-west-1/i-2/vm"); !IsNotFound(err) {
		t.Fatalf("GetByProviderID(missing) error = %v, want not found", err)
	}
	if _, err := c.Get(ctx, "b"); !IsNotFound(err) {
		t.Fatalf("Get(missing) error = %v, want not found", err)
	}
	if err := c.Delete(ctx, "a"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := c.Catalog(ctx, []string{"a", "b"}); !IsInsufficientCapacity(err) {
		t.Fatalf("Catalog error = %v, want insufficient capacity", err)
	}
}

func TestProviderIDRoundTrip(t *testing.T) {
	cloud, region, id, err := ParseProviderID(FormatProviderID("gcp", "us-central1", "vm-1"))
	if err != nil || cloud != "gcp" || region != "us-central1" || id != "vm-1" {
		t.Fatalf("ParseProviderID = %q %q %q %v", cloud, region, id, err)
	}
	for _, bad := range []string{"", "aws:///i-1", "cloudvm://aws/i-1", "cloudvm://aws/r/i-1/x", "kwok://a/b/c"} {
		if _, _, _, err := ParseProviderID(bad); err == nil {
			t.Errorf("ParseProviderID(%q) succeeded", bad)
		}
	}
}

func TestCreateRequestValidate(t *testing.T) {
	ok := CreateRequest{Name: "a", InstanceType: "t", Region: "r", CapacityType: CapacityTypeOnDemand}
	if err := ok.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	bad := ok
	bad.CapacityType = "reserved"
	if err := bad.Validate(); err == nil {
		t.Fatal("Validate accepted capacity type reserved")
	}
}

func TestTokenFileTransportReadsTheCurrentToken(t *testing.T) {
	path := t.TempDir() + "/token"
	var got []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got = append(got, r.Header.Get("Authorization"))
		_ = json.NewEncoder(w).Encode(List{})
	}))
	defer srv.Close()
	c := NewClient(srv.URL, &http.Client{Transport: &TokenFileTransport{Path: path}})
	if _, err := c.List(context.Background()); err == nil {
		t.Fatal("List without a token file succeeded")
	}
	for _, tok := range []string{"one", "two"} {
		if err := os.WriteFile(path, []byte(tok+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := c.List(context.Background()); err != nil {
			t.Fatalf("List: %v", err)
		}
	}
	if len(got) != 2 || got[0] != "Bearer one" || got[1] != "Bearer two" {
		t.Fatalf("Authorization headers = %q", got)
	}
}
