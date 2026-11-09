package replication_test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/pranavb1234/beacon/internal/broker"
	"github.com/pranavb1234/beacon/internal/cluster"
	"github.com/pranavb1234/beacon/internal/config"
	"github.com/pranavb1234/beacon/internal/pb"
	"github.com/pranavb1234/beacon/internal/replication"
)

type mockClusterNet struct {
	mu        sync.RWMutex
	listeners map[string]*bufconn.Listener
	dropped   map[string]bool
}

func newMockClusterNet() *mockClusterNet {
	return &mockClusterNet{
		listeners: make(map[string]*bufconn.Listener),
		dropped:   make(map[string]bool),
	}
}

func (m *mockClusterNet) register(addr string, l *bufconn.Listener) {
	m.mu.Lock()
	m.listeners[addr] = l
	m.mu.Unlock()
}

func (m *mockClusterNet) dial(ctx context.Context, addr string) (net.Conn, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.dropped[addr] {
		return nil, status.Error(codes.Unavailable, "node unreachable")
	}
	l, ok := m.listeners[addr]
	if !ok {
		return nil, fmt.Errorf("unknown address %q", addr)
	}
	return l.DialContext(ctx)
}

func (m *mockClusterNet) drop(addr string) {
	m.mu.Lock()
	m.dropped[addr] = true
	m.mu.Unlock()
}

type testBrokerNode struct {
	id         string
	addr       string
	cfg        *config.Config
	manager    *broker.Manager
	server     *broker.Server
	router     *broker.Router
	replicator *replication.Replicator
	ring       *cluster.Ring
	grpcServer *grpc.Server
	listener   *bufconn.Listener
}

func startTestBroker(
	t *testing.T,
	id, addr string,
	mode config.ReplicationMode,
	netw *mockClusterNet,
	ring *cluster.Ring,
	peers []config.PeerConfig,
) (*testBrokerNode, func()) {
	t.Helper()

	cfg := config.Default()
	cfg.NodeID = id
	cfg.ListenAddr = addr
	cfg.ReplicationMode = mode
	cfg.Peers = peers
	cfg.BlockTimeout = 200 * time.Millisecond
	cfg.AckTimeout = 200 * time.Millisecond

	mgr := broker.NewManager(cfg, nil)
	srv := broker.NewServer(mgr, cfg, slog.Default())

	replicator := replication.NewReplicator(
		id,
		cfg,
		ring,
		mgr,
		slog.Default(),
		replication.WithDialer(netw.dial),
	)

	router := broker.NewRouter(
		id,
		ring,
		nil,
		srv,
		cfg,
		slog.Default(),
		broker.WithRouterDialer(netw.dial),
	)

	router.AttachInternalHandler(replicator)
	srv.AttachRouter(router)
	srv.AttachReplicator(replicator)

	lis := bufconn.Listen(1024 * 1024)
	netw.register(addr, lis)

	go func() {
		_ = srv.Serve(lis)
	}()

	cleanup := func() {
		replicator.Close()
		router.Close()
		srv.Stop()
		_ = lis.Close()
	}

	return &testBrokerNode{
		id:         id,
		addr:       addr,
		cfg:        cfg,
		manager:    mgr,
		server:     srv,
		router:     router,
		replicator: replicator,
		ring:       ring,
		listener:   lis,
	}, cleanup
}

// TestReplication_AsyncMode tests that in async mode, publishes succeed immediately
// and the replica receives the messages in background.
func TestReplication_AsyncMode(t *testing.T) {
	netw := newMockClusterNet()
	ring := cluster.New(50)
	ring.Add("owner-node", "bufnet://owner")
	ring.Add("replica-node", "bufnet://replica")

	peers := []config.PeerConfig{
		{ID: "owner-node", Address: "bufnet://owner"},
		{ID: "replica-node", Address: "bufnet://replica"},
	}

	owner, c1 := startTestBroker(t, "owner-node", "bufnet://owner", config.ReplicationModeAsync, netw, ring, peers)
	defer c1()

	replica, c2 := startTestBroker(t, "replica-node", "bufnet://replica", config.ReplicationModeAsync, netw, ring, peers)
	defer c2()

	topic := "async-replicate-topic"

	// Publish 5 messages to the owner
	for i := 1; i <= 5; i++ {
		seq, err := owner.manager.Publish(topic, []byte(fmt.Sprintf("async-msg-%d", i)))
		require.NoError(t, err)
		assert.Equal(t, uint64(i), seq)

		err = owner.replicator.ReplicatePublish(context.Background(), topic, seq, []byte(fmt.Sprintf("async-msg-%d", i)), time.Now().UnixNano())
		require.NoError(t, err)
	}

	// Verify that replica receives all 5 messages in its ring buffer
	require.Eventually(t, func() bool {
		rt, exists := replica.manager.GetTopic(topic)
		if !exists {
			return false
		}
		return rt.RingBuffer().Len() == 5 && rt.NextSeq() == 5
	}, 2*time.Second, 20*time.Millisecond, "replica must receive all 5 replicated messages")

	rt, _ := replica.manager.GetTopic(topic)
	msgs, ok := rt.RingBuffer().GetFrom(1)
	require.True(t, ok)
	require.Len(t, msgs, 5)
	for i, m := range msgs {
		assert.Equal(t, uint64(i+1), m.Seq)
		assert.Equal(t, fmt.Sprintf("async-msg-%d", i+1), string(m.Payload))
	}
}

// TestReplication_SyncMode tests that in sync mode, the owner waits for replica's ack
// before returning success.
func TestReplication_SyncMode(t *testing.T) {
	netw := newMockClusterNet()
	ring := cluster.New(50)
	ring.Add("owner-node", "bufnet://owner")
	ring.Add("replica-node", "bufnet://replica")

	peers := []config.PeerConfig{
		{ID: "owner-node", Address: "bufnet://owner"},
		{ID: "replica-node", Address: "bufnet://replica"},
	}

	owner, c1 := startTestBroker(t, "owner-node", "bufnet://owner", config.ReplicationModeSync, netw, ring, peers)
	defer c1()

	replica, c2 := startTestBroker(t, "replica-node", "bufnet://replica", config.ReplicationModeSync, netw, ring, peers)
	defer c2()

	topic := "sync-replicate-topic"

	// Publish 5 messages to the owner under sync replication
	for i := 1; i <= 5; i++ {
		seq, err := owner.manager.Publish(topic, []byte(fmt.Sprintf("sync-msg-%d", i)))
		require.NoError(t, err)

		err = owner.replicator.ReplicatePublish(context.Background(), topic, seq, []byte(fmt.Sprintf("sync-msg-%d", i)), time.Now().UnixNano())
		require.NoError(t, err, "sync replication must succeed when replica is healthy")
	}

	assert.False(t, owner.replicator.IsDegraded(topic))

	// Replica must have all 5 messages persisted
	rt, exists := replica.manager.GetTopic(topic)
	require.True(t, exists)
	assert.Equal(t, 5, rt.RingBuffer().Len())
	assert.Equal(t, uint64(5), rt.NextSeq())
}

// TestReplication_SyncTimeout_Degraded tests that when the replica does not ack,
// sync publish times out, returns an error to the publisher, and marks the replica degraded.
func TestReplication_SyncTimeout_Degraded(t *testing.T) {
	netw := newMockClusterNet()
	ring := cluster.New(50)
	ring.Add("owner-node", "bufnet://owner")
	ring.Add("replica-node", "bufnet://replica")

	peers := []config.PeerConfig{
		{ID: "owner-node", Address: "bufnet://owner"},
		{ID: "replica-node", Address: "bufnet://replica"},
	}

	owner, c1 := startTestBroker(t, "owner-node", "bufnet://owner", config.ReplicationModeSync, netw, ring, peers)
	defer c1()

	// Do NOT start replica -> simulating unreachable or dead replica
	topic := "degraded-topic"

	seq, err := owner.manager.Publish(topic, []byte("will-timeout"))
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err = owner.replicator.ReplicatePublish(ctx, topic, seq, []byte("will-timeout"), time.Now().UnixNano())
	require.Error(t, err, "sync replication must fail when replica is unreachable")

	// Replica must be marked degraded
	assert.True(t, owner.replicator.IsDegraded(topic), "replica must be marked degraded on timeout")
}

// TestReplication_CatchUpNewReplica verifies that when a new replica is assigned,
// SyncTopic streams the historical messages from the ring buffer to bring it up to date.
func TestReplication_CatchUpNewReplica(t *testing.T) {
	netw := newMockClusterNet()
	ring := cluster.New(50)
	ring.Add("owner-node", "bufnet://owner")
	ring.Add("new-replica", "bufnet://new-replica")

	peers := []config.PeerConfig{
		{ID: "owner-node", Address: "bufnet://owner"},
		{ID: "new-replica", Address: "bufnet://new-replica"},
	}

	owner, c1 := startTestBroker(t, "owner-node", "bufnet://owner", config.ReplicationModeSync, netw, ring, peers)
	defer c1()

	topic := "catchup-topic"

	// Publish 10 messages on the owner before the replica joins
	for i := 1; i <= 10; i++ {
		_, err := owner.manager.Publish(topic, []byte(fmt.Sprintf("history-%d", i)))
		require.NoError(t, err)
	}

	// New replica joins
	replica, c2 := startTestBroker(t, "new-replica", "bufnet://new-replica", config.ReplicationModeSync, netw, ring, peers)
	defer c2()

	// Catch up new replica starting from seq 1
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := replica.replicator.CatchUpReplica(ctx, topic, "owner-node", 1)
	require.NoError(t, err)

	// Verify all 10 historical messages exist on the replica in exact sequence
	rt, exists := replica.manager.GetTopic(topic)
	require.True(t, exists)
	assert.Equal(t, 10, rt.RingBuffer().Len())
	assert.Equal(t, uint64(10), rt.NextSeq())

	msgs, ok := rt.RingBuffer().GetFrom(1)
	require.True(t, ok)
	require.Len(t, msgs, 10)
	for i, m := range msgs {
		assert.Equal(t, uint64(i+1), m.Seq)
		assert.Equal(t, fmt.Sprintf("history-%d", i+1), string(m.Payload))
	}
}

// TestReplication_GapDetection verifies that when messages arrive out of sequence,
// the replica detects and records sequence gaps.
func TestReplication_GapDetection(t *testing.T) {
	netw := newMockClusterNet()
	ring := cluster.New(50)
	ring.Add("owner-node", "bufnet://owner")
	ring.Add("replica-node", "bufnet://replica")

	peers := []config.PeerConfig{
		{ID: "owner-node", Address: "bufnet://owner"},
		{ID: "replica-node", Address: "bufnet://replica"},
	}

	_, c1 := startTestBroker(t, "owner-node", "bufnet://owner", config.ReplicationModeSync, netw, ring, peers)
	defer c1()

	replica, c2 := startTestBroker(t, "replica-node", "bufnet://replica", config.ReplicationModeSync, netw, ring, peers)
	defer c2()

	topic := "gap-test-topic"

	// Dial replica's Replicate stream directly and intentionally send seq 1, then seq 5 (skipping 2,3,4)
	conn, err := grpc.NewClient("passthrough:///bufnet://replica",
		grpc.WithContextDialer(netw.dial),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	client := pb.NewInternalClient(conn)
	stream, err := client.Replicate(context.Background())
	require.NoError(t, err)

	// Send message 1
	err = stream.Send(&pb.ReplicateMsg{Topic: topic, Epoch: 1, Seq: 1, Payload: []byte("first")})
	require.NoError(t, err)
	ack, err := stream.Recv()
	require.NoError(t, err)
	assert.True(t, ack.GetOk())

	// Send message 5 (gap of 3 messages)
	err = stream.Send(&pb.ReplicateMsg{Topic: topic, Epoch: 1, Seq: 5, Payload: []byte("jumped")})
	require.NoError(t, err)
	ack, err = stream.Recv()
	require.NoError(t, err)
	assert.True(t, ack.GetOk())

	// Assert gap detected
	assert.Equal(t, uint64(3), replica.replicator.DetectedGaps(topic))
}
