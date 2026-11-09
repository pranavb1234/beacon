package cluster

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestEmptyRing verifies that GetOwner and GetReplica return empty strings on an empty ring.
func TestEmptyRing(t *testing.T) {
	r := New(10)
	assert.Equal(t, "", r.GetOwner("any-key"))
	assert.Equal(t, "", r.GetReplica("any-key"))
	_, _, ok := r.Owner("any-key")
	assert.False(t, ok)
	_, _, ok = r.Replica("any-key")
	assert.False(t, ok)
}

// TestSingleNode verifies that all keys route to the only node, and replica == owner.
func TestSingleNode(t *testing.T) {
	r := New(10)
	r.AddNode("node1")

	for _, key := range []string{"orders", "payments", "events"} {
		assert.Equal(t, "node1", r.GetOwner(key))
		assert.Equal(t, "node1", r.GetReplica(key))
	}
}

// TestDistribution verifies that across 10,000 random topic names distributed across 3 nodes,
// each node receives within 20% of the mean (fair share).
func TestDistribution(t *testing.T) {
	r := New(150)
	nodes := []string{"node-1", "node-2", "node-3"}
	for _, n := range nodes {
		r.AddNode(n)
	}

	const total = 10_000
	counts := make(map[string]int, len(nodes))

	rng := rand.New(rand.NewSource(1337))
	for i := 0; i < total; i++ {
		topic := fmt.Sprintf("topic-%d-%x", i, rng.Uint64())
		owner := r.GetOwner(topic)
		counts[owner]++
	}

	mean := float64(total) / float64(len(nodes))
	t.Logf("Key counts across 3 nodes (mean = %.2f): %v", mean, counts)

	for _, n := range nodes {
		c := counts[n]
		diffPct := math.Abs(float64(c)-mean) / mean * 100.0
		t.Logf("Node %s: %d keys (diff: %.2f%%)", n, c, diffPct)
		assert.LessOrEqual(t, diffPct, 20.0,
			"node %s load %d deviated by %.2f%% which exceeds 20%% of the mean", n, c, diffPct)
	}
}

// TestMinimalMovement verifies that when one node is removed, ONLY keys previously
// owned by the removed node are reassigned; all other keys remain with their original owners.
func TestMinimalMovement(t *testing.T) {
	r := New(150)
	r.AddNode("node1")
	r.AddNode("node2")
	r.AddNode("node3")

	const n = 10_000
	before := make(map[string]string, n)
	topics := make([]string, n)

	rng := rand.New(rand.NewSource(42))
	for i := 0; i < n; i++ {
		topics[i] = fmt.Sprintf("topic-%d-%x", i, rng.Uint64())
		before[topics[i]] = r.GetOwner(topics[i])
	}

	// Remove node3
	r.RemoveNode("node3")

	moved := 0
	for _, topic := range topics {
		after := r.GetOwner(topic)
		if after != before[topic] {
			moved++
			assert.Equal(t, "node3", before[topic],
				"key %q moved from %s to %s, but only node3 was removed", topic, before[topic], after)
		}
	}

	movedFraction := float64(moved) / float64(n)
	t.Logf("Removed node3: moved %d/%d keys (fraction = %.4f, theoretical ~0.3333)", moved, n, movedFraction)

	assert.Greater(t, moved, 0, "some keys must move after node removal")
	// For 3 balanced nodes, removing 1 should move approximately 1/3 of the keys
	assert.InDelta(t, 0.3333, movedFraction, 0.10, "moved fraction should be close to 1/3")
}

// TestReplicaDiffersFromOwner verifies that when 2+ nodes exist, the replica is NEVER
// the same node as the owner.
func TestReplicaDiffersFromOwner(t *testing.T) {
	r := New(150)
	r.AddNode("node1")
	r.AddNode("node2")
	r.AddNode("node3")

	topics := []string{"orders", "payments", "events", "metrics", "logs", "users", "inventory", "auth"}
	for _, topic := range topics {
		owner := r.GetOwner(topic)
		replica := r.GetReplica(topic)
		assert.NotEmpty(t, owner)
		assert.NotEmpty(t, replica)
		assert.NotEqual(t, owner, replica,
			"for topic %q: replica %s must never be the same as owner %s when 2+ nodes exist", topic, replica, owner)
	}

	// Test with exactly 2 nodes as well
	r2 := New(150)
	r2.AddNode("nodeA")
	r2.AddNode("nodeB")
	for _, topic := range topics {
		owner := r2.GetOwner(topic)
		replica := r2.GetReplica(topic)
		assert.NotEqual(t, owner, replica,
			"for topic %q on 2-node ring: replica %s must not equal owner %s", topic, replica, owner)
	}
}

// TestDeterminism verifies that identical ring state produces identical owner mappings
// across different Ring instances, regardless of insertion order.
func TestDeterminism(t *testing.T) {
	r1 := New(150)
	r1.AddNode("node1")
	r1.AddNode("node2")
	r1.AddNode("node3")

	// r2 adds nodes in reverse order
	r2 := New(150)
	r2.AddNode("node3")
	r2.AddNode("node1")
	r2.AddNode("node2")

	for i := 0; i < 500; i++ {
		topic := fmt.Sprintf("account-event-%d", i)
		owner1 := r1.GetOwner(topic)
		owner2 := r2.GetOwner(topic)
		replica1 := r1.GetReplica(topic)
		replica2 := r2.GetReplica(topic)

		assert.Equal(t, owner1, owner2, "owner mismatch for topic %s across instances", topic)
		assert.Equal(t, replica1, replica2, "replica mismatch for topic %s across instances", topic)
	}
}

// TestNodes verifies Nodes() returns all distinct IDs in sorted order.
func TestNodes(t *testing.T) {
	r := New(10)
	r.AddNode("gamma")
	r.AddNode("alpha")
	r.AddNode("beta")

	assert.Equal(t, []string{"alpha", "beta", "gamma"}, r.Nodes())

	r.RemoveNode("beta")
	assert.Equal(t, []string{"alpha", "gamma"}, r.Nodes())
}

// TestConcurrentAccess verifies thread-safety under concurrent node additions, removals, and lookups.
func TestConcurrentAccess(t *testing.T) {
	r := New(150)
	r.AddNode("node1")
	r.AddNode("node2")

	var wg sync.WaitGroup

	// Writers
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			id := fmt.Sprintf("dynamic-node-%d", i)
			r.AddNode(id)
			r.RemoveNode(id)
		}
	}()

	// Readers
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 300; i++ {
				topic := fmt.Sprintf("concurrent-topic-%d-%d", gid, i)
				_ = r.GetOwner(topic)
				_ = r.GetReplica(topic)
				_ = r.Nodes()
			}
		}(g)
	}

	wg.Wait()
}

// TestRebuildFreeRemovalCorrectness ensures removing nodes preserves ring lookup correctness.
func TestRebuildFreeRemovalCorrectness(t *testing.T) {
	r := New(50)
	r.AddNode("a")
	r.AddNode("b")
	r.AddNode("c")

	r.RemoveNode("b")
	require.Len(t, r.Nodes(), 2)

	for i := 0; i < 100; i++ {
		owner := r.GetOwner(fmt.Sprintf("key-%d", i))
		assert.Contains(t, []string{"a", "c"}, owner)
	}
}
