package cluster

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEmptyRing verifies that Owner and Replica return false on an empty ring.
func TestEmptyRing(t *testing.T) {
	r := New(10)
	_, _, ok := r.Owner("any-key")
	assert.False(t, ok, "empty ring should return ok=false")
	_, _, ok = r.Replica("any-key")
	assert.False(t, ok)
}

// TestSingleNode verifies that all keys route to the only node.
func TestSingleNode(t *testing.T) {
	r := New(10)
	r.Add("node1", "localhost:9001")

	for _, key := range []string{"orders", "payments", "events"} {
		id, addr, ok := r.Owner(key)
		require.True(t, ok)
		assert.Equal(t, "node1", id)
		assert.Equal(t, "localhost:9001", addr)
	}
}

// TestReplicaSameAsPrimaryOnSingleNode verifies the degenerate case where
// replica == primary (only one distinct node exists).
func TestReplicaSameAsPrimaryOnSingleNode(t *testing.T) {
	r := New(10)
	r.Add("node1", "localhost:9001")

	oid, _, _ := r.Owner("topic")
	rid, _, _ := r.Replica("topic")
	assert.Equal(t, oid, rid)
}

// TestReplicaDiffersFromOwner verifies that with multiple nodes the replica
// is always a different node from the primary owner.
func TestReplicaDiffersFromOwner(t *testing.T) {
	r := New(150)
	r.Add("node1", "localhost:9001")
	r.Add("node2", "localhost:9002")
	r.Add("node3", "localhost:9003")

	topics := []string{"orders", "payments", "events", "metrics", "logs", "users"}
	for _, topic := range topics {
		oid, _, _ := r.Owner(topic)
		rid, _, _ := r.Replica(topic)
		assert.NotEqual(t, oid, rid,
			"topic %q: replica %s should differ from owner %s", topic, rid, oid)
	}
}

// TestDistribution verifies that no node owns more than 2× or less than 0.5×
// its fair share of keys over 10 000 samples.
// Trade-off: 150 vnodes gives good uniformity; fewer vnodes would need a larger
// tolerance here.
func TestDistribution(t *testing.T) {
	r := New(150)
	nodeIDs := []string{"node1", "node2", "node3"}
	for i, id := range nodeIDs {
		r.Add(id, fmt.Sprintf("localhost:%d", 9001+i))
	}

	counts := make(map[string]int, len(nodeIDs))
	const total = 10_000
	for i := 0; i < total; i++ {
		id, _, _ := r.Owner(fmt.Sprintf("topic-%d", i))
		counts[id]++
	}

	fairShare := float64(total) / float64(len(nodeIDs))
	for _, id := range nodeIDs {
		ratio := float64(counts[id]) / fairShare
		assert.Less(t, ratio, 2.0,
			"node %s owns too many keys (ratio=%.2f)", id, ratio)
		assert.Greater(t, ratio, 0.5,
			"node %s owns too few keys (ratio=%.2f)", id, ratio)
	}
	t.Logf("Distribution: %v", counts)
}

// TestStabilityAfterRemoval verifies that only keys that were owned by the
// removed node change assignment — all others remain stable.
// This is the core correctness property of consistent hashing.
func TestStabilityAfterRemoval(t *testing.T) {
	r := New(150)
	r.Add("node1", "localhost:9001")
	r.Add("node2", "localhost:9002")
	r.Add("node3", "localhost:9003")

	const n = 1000
	before := make(map[string]string, n)
	topics := make([]string, n)
	for i := range topics {
		topics[i] = fmt.Sprintf("topic-%d", i)
		id, _, _ := r.Owner(topics[i])
		before[topics[i]] = id
	}

	r.Remove("node3")

	moved := 0
	for _, topic := range topics {
		after, _, _ := r.Owner(topic)
		if after != before[topic] {
			moved++
			assert.Equal(t, "node3", before[topic],
				"key %q moved from %s→%s but only node3 was removed",
				topic, before[topic], after)
		}
	}
	assert.Greater(t, moved, 0, "some keys should have moved after removing node3")
	t.Logf("Moved %d/%d keys after removing node3", moved, n)
}

// TestRemoveAllKeysGoToSurvivor verifies that after removing all but one node,
// every key routes to the surviving node.
func TestRemoveAllKeysGoToSurvivor(t *testing.T) {
	r := New(150)
	r.Add("node1", "localhost:9001")
	r.Add("node2", "localhost:9002")
	r.Remove("node1")

	for i := 0; i < 100; i++ {
		id, _, ok := r.Owner(fmt.Sprintf("key-%d", i))
		require.True(t, ok)
		assert.Equal(t, "node2", id)
	}
}

// TestNodes verifies that Nodes() returns all distinct IDs.
func TestNodes(t *testing.T) {
	r := New(10)
	r.Add("alpha", "localhost:9001")
	r.Add("beta", "localhost:9002")
	r.Add("gamma", "localhost:9003")

	ids := r.Nodes()
	assert.Len(t, ids, 3)
	assert.ElementsMatch(t, []string{"alpha", "beta", "gamma"}, ids)

	r.Remove("beta")
	ids = r.Nodes()
	assert.Len(t, ids, 2)
	assert.NotContains(t, ids, "beta")
}

// TestConcurrentAccess verifies no data races under concurrent reads and writes.
// -race detector will catch any violations.
func TestConcurrentAccess(t *testing.T) {
	r := New(150)
	r.Add("node1", "localhost:9001")

	var wg sync.WaitGroup

	// Writer goroutine: rapidly adds and removes nodes.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			id := fmt.Sprintf("node%d", i+2)
			addr := fmt.Sprintf("localhost:%d", 9002+i)
			r.Add(id, addr)
			r.Remove(id)
		}
	}()

	// Reader goroutines: continuously look up owners.
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				r.Owner(fmt.Sprintf("topic-%d-%d", g, i))
				r.Replica(fmt.Sprintf("topic-%d-%d", g, i))
			}
		}(g)
	}

	wg.Wait()
}

// TestAddIdempotent verifies that adding the same node twice doesn't corrupt
// the ring (the second Add simply overwrites the virtual nodes).
func TestAddIdempotent(t *testing.T) {
	r := New(10)
	r.Add("node1", "localhost:9001")
	r.Add("node1", "localhost:9001") // duplicate

	ids := r.Nodes()
	assert.Len(t, ids, 1)

	_, _, ok := r.Owner("any")
	assert.True(t, ok)
}
