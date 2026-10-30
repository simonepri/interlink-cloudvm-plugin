// Tests the helpers of the wire contract that the plugin and the node agent share.

package nodeclaim

import "testing"

func TestIsDaemonSetOwner(t *testing.T) {
	for _, tc := range []struct {
		apiVersion, kind string
		want             bool
	}{
		{"apps/v1", "DaemonSet", true},
		{"apps/v1beta2", "DaemonSet", true},
		{"apps/v1", "ReplicaSet", false},
		{"example.com/v1", "DaemonSet", false},
		{"v1", "Node", false},
	} {
		if got := IsDaemonSetOwner(tc.apiVersion, tc.kind); got != tc.want {
			t.Errorf("IsDaemonSetOwner(%q, %q) = %v, want %v", tc.apiVersion, tc.kind, got, tc.want)
		}
	}
}
