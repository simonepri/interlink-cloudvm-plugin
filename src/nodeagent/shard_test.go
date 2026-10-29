// Tests that rendezvous sharding spreads nodes evenly and moves few nodes when the shard count changes.

package main

import (
	"fmt"
	"testing"
)

func TestOwnerShardBalanceAndMovement(t *testing.T) {
	const nodes = 20000
	counts := make([]int, 4)
	moved := 0
	for i := range nodes {
		name := fmt.Sprintf("cloudvm-node-%d", i)
		before := OwnerShard(name, 4)
		counts[before]++
		if after := OwnerShard(name, 5); after != before && after != 4 {
			moved++
		}
	}
	for shard, c := range counts {
		if c < nodes/4*9/10 || c > nodes/4*11/10 {
			t.Errorf("shard %d owns %d of %d nodes, want within 10%% of %d", shard, c, nodes, nodes/4)
		}
	}
	if moved != 0 {
		t.Errorf("%d nodes moved between existing shards when a fifth shard was added, want 0", moved)
	}
}

func TestParseShard(t *testing.T) {
	cases := []struct {
		index, count, pod string
		want              Shard
		wantErr           bool
	}{
		{"", "3", "cloudvm-node-agent-2", Shard{Index: 2, Count: 3}, false},
		{"1", "3", "ignored", Shard{Index: 1, Count: 3}, false},
		{"", "1", "agent", Shard{}, true},
		{"3", "3", "", Shard{}, true},
		{"0", "0", "", Shard{}, true},
	}
	for _, c := range cases {
		got, err := ParseShard(c.index, c.count, c.pod)
		if (err != nil) != c.wantErr || (!c.wantErr && got != c.want) {
			t.Errorf("ParseShard(%q, %q, %q) = %+v, %v", c.index, c.count, c.pod, got, err)
		}
	}
}
