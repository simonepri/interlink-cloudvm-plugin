// Tests that pods launch in the region of the virtual node they are bound to unless an annotation overrides it.
package main

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestCreateLaunchesInBoundNodeRegion(t *testing.T) {
	t.Setenv("NODE_REGIONS", `{"virtual-node-region-a":"eu-west-1","virtual-node-region-b":"us-east-1"}`)

	cases := []struct {
		name        string
		nodeName    string
		annotations string
		want        string
	}{
		{name: "mapped node", nodeName: "virtual-node-region-a", want: "eu-west-1"},
		{name: "annotation overrides node", nodeName: "virtual-node-region-a", annotations: `"cloudvm.vk.io/region": "ap-south-1"`, want: "ap-south-1"},
		{name: "unmapped node uses default", nodeName: "virtual-node-other", want: "us-west-2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			mock := NewMockProvider()
			server := NewServer(mock, "test-cluster")
			server.SetDefaultRegion("us-west-2")
			ts := httptest.NewServer(server.Routes())
			defer ts.Close()

			payload := `{"pod": {"metadata": {"name": "worker", "namespace": "default", "uid": "uid-` + strings.ReplaceAll(tc.name, " ", "-") + `", "annotations": {` + tc.annotations + `}},
				"spec": {"nodeName": "` + tc.nodeName + `", "containers": [{"name": "main", "image": "ubuntu:24.04"}]}}}`
			resp, err := http.Post(ts.URL+"/create", "application/json", strings.NewReader(payload))
			if err != nil {
				t.Fatalf("POST /create failed: %v", err)
			}
			defer func() { _ = resp.Body.Close() }()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("expected 200 from /create, got %d", resp.StatusCode)
			}
			spec := mock.LastSpec()
			if spec == nil {
				t.Fatal("expected a launch")
			}
			if spec.Region != tc.want {
				t.Errorf("region = %q, want %q", spec.Region, tc.want)
			}
		})
	}
}
