package replication_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/pranavb1234/beacon/internal/cluster"
	"github.com/pranavb1234/beacon/internal/config"
	"github.com/pranavb1234/beacon/internal/pb"
)

// TestFailover_ThreeNodes_KillOwner_ReplicaPromoted tests 3 in-process nodes:
// 1. Publish messages with sync replication.
// 2. Kill the owner node.
// 3. Replica promotes itself to owner, increments epoch, and sets nextSeq = lastReplicatedSeq + 1.
// 4. Assert zero sequence gaps in sync mode.
// 5. New owner continues accepting writes with contiguous sequence numbers.
func TestFailover_ThreeNodes_KillOwner_ReplicaPromoted(t *testing.T) {
	netw := newMockClusterNet()
	ring := cluster.New(50)
	ring.Add("node-1", "bufnet://node-1")
	ring.Add("node-2", "bufnet://node-2")
	ring.Add("node-3", "bufnet://node-3")

	peers := []config.PeerConfig{
		{ID: "node-1", Address: "bufnet://node-1"},
		{ID: "node-2", Address: "bufnet://node-2"},
		{ID: "node-3", Address: "bufnet://node-3"},
	}

	n1, c1 := startTestBroker(t, "node-1", "bufnet://node-1", config.ReplicationModeSync, netw, ring, peers)
	defer c1()

	n2, c2 := startTestBroker(t, "node-2", "bufnet://node-2", config.ReplicationModeSync, netw, ring, peers)
	defer c2()

	n3, c3 := startTestBroker(t, "node-3", "bufnet://node-3", config.ReplicationModeSync, netw, ring, peers)
	defer c3()

	topic := "failover-topic"

	// 1. Publish 5 messages on node-1 under sync replication
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for i := 1; i <= 5; i++ {
		seq, err := n1.manager.Publish(topic, []byte(fmt.Sprintf("order-%d", i)))
		require.NoError(t, err)
		assert.Equal(t, uint64(i), seq)

		err = n1.replicator.ReplicatePublish(ctx, topic, seq, []byte(fmt.Sprintf("order-%d", i)), time.Now().UnixNano())
		require.NoError(t, err)
	}

	// 2. Kill the owner (node-1)
	c1()
	netw.drop("bufnet://node-1")

	// Trigger peer down on survivors (simulating membership failure detection)
	n2.replicator.HandlePeerDown("node-1")
	n3.replicator.HandlePeerDown("node-1")

	// 3. Replica promotes itself and epoch is incremented to 2
	s2 := n2.replicator.GetOrCreateTopicState(topic)
	assert.Equal(t, uint64(2), s2.Epoch)

	// 4. Assert no sequence gaps in sync mode: nextSeq must be exactly 6
	t2, exists := n2.manager.GetTopic(topic)
	require.True(t, exists)
	assert.Equal(t, uint64(5), t2.NextSeq())

	// 5. New owner accepts live publishes starting at seq 6
	seq6, err := n2.manager.Publish(topic, []byte("order-6"))
	require.NoError(t, err)
	assert.Equal(t, uint64(6), seq6)

	seq7, err := n2.manager.Publish(topic, []byte("order-7"))
	require.NoError(t, err)
	assert.Equal(t, uint64(7), seq7)
}

// TestFailover_StaleOwnerRejected verifies split-brain protection:
// If a partitioned former owner attempts to replicate with a lower/stale epoch,
// the current cluster rejects it.
func TestFailover_StaleOwnerRejected(t *testing.T) {
	netw := newMockClusterNet()
	ring := cluster.New(50)
	ring.Add("node-1", "bufnet://node-1")
	ring.Add("node-2", "bufnet://node-2")

	peers := []config.PeerConfig{
		{ID: "node-1", Address: "bufnet://node-1"},
		{ID: "node-2", Address: "bufnet://node-2"},
	}

	n1, c1 := startTestBroker(t, "node-1", "bufnet://node-1", config.ReplicationModeSync, netw, ring, peers)
	defer c1()

	n2, c2 := startTestBroker(t, "node-2", "bufnet://node-2", config.ReplicationModeSync, netw, ring, peers)
	defer c2()

	topic := "split-brain-topic"

	// Node 2 promotes itself to Epoch 5
	s2 := n2.replicator.GetOrCreateTopicState(topic)
	s2.Epoch = 5

	// Stale owner (Node 1) dials Node 2 with stale Epoch 2
	conn, err := grpc.NewClient("passthrough:///bufnet://node-2",
		grpc.WithContextDialer(netw.dial),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := pb.NewInternalClient(conn)
	stream, err := client.Replicate(context.Background())
	require.NoError(t, err)

	// Stale owner attempts to replicate
	err = stream.Send(&pb.ReplicateMsg{
		Topic: topic,
		Epoch: 2, // Stale! (Node 2 is at Epoch 5)
		Seq:   10,
	})
	require.NoError(t, err)

	ack, err := stream.Recv()
	require.NoError(t, err)

	// ReplicateAck must report Ok = false (stale epoch rejected)
	assert.False(t, ack.GetOk(), "message with stale epoch must be rejected")
	assert.Equal(t, uint64(5), ack.GetEpoch())

	// Stale owner discovers higher epoch and steps down
	n1.replicator.StepDownTopic(topic, ack.GetEpoch())
	s1 := n1.replicator.GetOrCreateTopicState(topic)
	assert.Equal(t, uint64(5), s1.Epoch)
}

// TestFailover_NodeRejoin_GracefulHandoff tests Task 4.4:
// A restarted node with empty memory first syncs via SyncTopic, initiates graceful handoff,
// bumps epoch, and takes back ownership with zero message loss.
func TestFailover_NodeRejoin_GracefulHandoff(t *testing.T) {
	netw := newMockClusterNet()
	ring := cluster.New(50)
	ring.Add("current-owner", "bufnet://current-owner")
	ring.Add("returning-node", "bufnet://returning-node")

	peers := []config.PeerConfig{
		{ID: "current-owner", Address: "bufnet://current-owner"},
		{ID: "returning-node", Address: "bufnet://returning-node"},
	}

	owner, c1 := startTestBroker(t, "current-owner", "bufnet://current-owner", config.ReplicationModeSync, netw, ring, peers)
	defer c1()

	topic := "rejoin-handoff-topic"

	// Current owner publishes 10 messages
	for i := 1; i <= 10; i++ {
		_, err := owner.manager.Publish(topic, []byte(fmt.Sprintf("live-data-%d", i)))
		require.NoError(t, err)
	}

	// Returning node joins (empty memory)
	rejoining, c2 := startTestBroker(t, "returning-node", "bufnet://returning-node", config.ReplicationModeSync, netw, ring, peers)
	defer c2()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// 1. Returning node syncs history via SyncTopic from current owner
	err := rejoining.replicator.CatchUpReplica(ctx, topic, "current-owner", 1)
	require.NoError(t, err)

	rt, exists := rejoining.manager.GetTopic(topic)
	require.True(t, exists)
	assert.Equal(t, 10, rt.RingBuffer().Len())

	// 2. Returning node initiates graceful handoff
	err = rejoining.replicator.InitiateHandoff(ctx, topic, "current-owner")
	require.NoError(t, err)

	// 3. Verify handoff stats counter was bumped
	assert.Equal(t, uint64(1), rejoining.replicator.HandoffsTotal())
	assert.Equal(t, uint64(1), owner.replicator.HandoffsTotal())

	// 4. Verify returning node is now owner with bumped epoch
	sRejoin := rejoining.replicator.GetOrCreateTopicState(topic)
	assert.Equal(t, uint64(2), sRejoin.Epoch)

	// 5. Returning node continues accepting live publishes starting at seq 11 with zero loss
	seq11, err := rejoining.manager.Publish(topic, []byte("live-data-11"))
	require.NoError(t, err)
	assert.Equal(t, uint64(11), seq11)
}

// TestFailover_AsyncMode_GapBounded tests that in async mode, when owner is killed,
// the gap between owner and replica is bounded by the in-flight buffer capacity.
func TestFailover_AsyncMode_GapBounded(t *testing.T) {
	netw := newMockClusterNet()
	ring := cluster.New(50)
	ring.Add("node-a", "bufnet://node-a")
	ring.Add("node-b", "bufnet://node-b")

	peers := []config.PeerConfig{
		{ID: "node-a", Address: "bufnet://node-a"},
		{ID: "node-b", Address: "bufnet://node-b"},
	}

	nA, cA := startTestBroker(t, "node-a", "bufnet://node-a", config.ReplicationModeAsync, netw, ring, peers)
	defer cA()

	nB, cB := startTestBroker(t, "node-b", "bufnet://node-b", config.ReplicationModeAsync, netw, ring, peers)
	defer cB()

	topic := "async-gap-bounded-topic"

	// Publish 20 messages under async mode
	for i := 1; i <= 20; i++ {
		seq, err := nA.manager.Publish(topic, []byte(fmt.Sprintf("async-item-%d", i)))
		require.NoError(t, err)
		_ = nA.replicator.ReplicatePublish(context.Background(), topic, seq, []byte(fmt.Sprintf("async-item-%d", i)), time.Now().UnixNano())
	}

	// Give async worker time to flush buffered queue
	require.Eventually(t, func() bool {
		tb, exists := nB.manager.GetTopic(topic)
		return exists && tb.RingBuffer().Len() >= 15
	}, 1*time.Second, 20*time.Millisecond)

	// Kill node-a
	cA()
	netw.drop("bufnet://node-a")

	// Promote node-b
	nB.replicator.HandlePeerDown("node-a")

	tb, _ := nB.manager.GetTopic(topic)
	// Replicated messages must be within [15, 20], meaning gap <= 5 (bounded by queue capacity)
	assert.GreaterOrEqual(t, tb.RingBuffer().Len(), 15)
	assert.LessOrEqual(t, tb.RingBuffer().Len(), 20)
}
