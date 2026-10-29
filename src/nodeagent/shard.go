// Shard ownership: each node name belongs to exactly one shard by rendezvous hashing, so changing the shard count
// moves only the nodes that the added or removed shards win or lose.

package main

import (
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
)

// Shard identifies this process among Count agent replicas.
type Shard struct {
	Index int
	Count int
}

// Owns reports whether this shard serves the node with the name.
func (s Shard) Owns(nodeName string) bool {
	return OwnerShard(nodeName, s.Count) == s.Index
}

// OwnerShard returns the shard in [0, count) with the highest hash weight for the node name.
func OwnerShard(nodeName string, count int) int {
	if count <= 1 {
		return 0
	}
	best, bestWeight := 0, uint64(0)
	for i := range count {
		h := fnv.New64a()
		_, _ = h.Write([]byte(nodeName))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(strconv.Itoa(i)))
		if w := mix64(h.Sum64()); i == 0 || w > bestWeight {
			best, bestWeight = i, w
		}
	}
	return best
}

// mix64 is the splitmix64 finalizer, which spreads FNV's weak low bits so that weights compare uniformly.
func mix64(x uint64) uint64 {
	x ^= x >> 30
	x *= 0xbf58476d1ce4e5b9
	x ^= x >> 27
	x *= 0x94d049bb133111eb
	x ^= x >> 31
	return x
}

// ParseShard reads the shard index from index, or from the StatefulSet ordinal that ends podName when index is
// empty, and validates it against count.
func ParseShard(index, count, podName string) (Shard, error) {
	n, err := strconv.Atoi(strings.TrimSpace(count))
	if err != nil || n < 1 {
		return Shard{}, fmt.Errorf("shard count %q must be a positive integer", count)
	}
	if strings.TrimSpace(index) == "" {
		dash := strings.LastIndex(podName, "-")
		if dash < 0 {
			return Shard{}, fmt.Errorf("shard index is unset and pod name %q has no StatefulSet ordinal", podName)
		}
		index = podName[dash+1:]
	}
	i, err := strconv.Atoi(strings.TrimSpace(index))
	if err != nil || i < 0 || i >= n {
		return Shard{}, fmt.Errorf("shard index %q must be an integer in [0, %d)", index, n)
	}
	return Shard{Index: i, Count: n}, nil
}
