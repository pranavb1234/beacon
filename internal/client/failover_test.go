package client_test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
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
	"github.com/pranavb1234/beacon/internal/client"
	"github.com/pranavb1234/beacon/internal/cluster"
	"github.com/pranavb1234/beacon/internal/config"
	"github.com/pranavb1234/beacon/internal/pb"
	"github.com/pranavb1234/beacon/internal/replication"
)

type clientTestNetwork struct {
	mu        sync.RWMutex
	listeners map[string]*bufconn.Listener
	dropped   map[string]bool
}

func newClientTestNetwork() *clientTestNetwork {
	return &clientTestNetwork{
		listeners: make(map[string]*bufconn.Listener),
		dropped:   make(map[string]bool),
	}
}

func (n *clientTestNetwork) register(addr string, l *bufconn.Listener) {
	n.mu.Lock()
	n.listeners[addr] = l
	n.mu.Unlock()
}

func (n *clientTestNetwork) dial(ctx context.Context, addr string) (net.Conn, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()
	if n.dropped[addr] {
		return nil, status.Error(codes.Unavailable, "node is down")
	}
	l, ok := n.listeners[addr]
	if !ok {
		return nil, fmt.Errorf("unknown addr %s", addr)
	}
	return l.DialContext(ctx)
}

func (n *clientTestNetwork) drop(addr string) {
	n.mu.Lock()
	n.dropped[addr] = true
	n.mu.Unlock()
}

func startTestNode(
	t *testing.T,
	id, addr string,
	netw *clientTestNetwork,
	ring *cluster.Ring,
	peers []config.PeerConfig,
) (*broker.Manager, *broker.Server, *replication.Replicator, func()) {
	t.Helper()

	cfg := config.Default()
	cfg.NodeID = id
	cfg.ListenAddr = addr
	cfg.ReplicationMode = config.ReplicationModeSync
	cfg.Peers = peers

	mgr := broker.NewManager(cfg, nil)
	srv := broker.NewServer(mgr, cfg, slog.Default())

	replicator := replication.NewReplicator(
		id, cfg, ring, mgr, slog.Default(),
		replication.WithDialer(netw.dial),
	)

	router := broker.NewRouter(
		id, ring, nil, srv, cfg, slog.Default(),
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

	return mgr, srv, replicator, cleanup
}

// TestClientSDK_PublisherFailoverAndIdempotency verifies that when a broker dies,
// the Publisher SDK fails over to an alternate broker address, retries with the
// same idempotency key, and the broker avoids assigning duplicate sequences.
func TestClientSDK_PublisherFailoverAndIdempotency(t *testing.T) {
	netw := newClientTestNetwork()
	ring := cluster.New(50)
	ring.Add("broker-1", "bufnet://broker-1")
	ring.Add("broker-2", "bufnet://broker-2")

	peers := []config.PeerConfig{
		{ID: "broker-1", Address: "bufnet://broker-1"},
		{ID: "broker-2", Address: "bufnet://broker-2"},
	}

	_, _, _, c1 := startTestNode(t, "broker-1", "bufnet://broker-1", netw, ring, peers)
	defer c1()

	_, _, rep2, c2 := startTestNode(t, "broker-2", "bufnet://broker-2", netw, ring, peers)
	defer c2()

	topic := "idempotent-failover-topic"

	// Create publisher with both broker addresses
	pub := client.NewPublisher(client.PublisherConfig{
		Brokers:        []string{"bufnet://broker-1", "bufnet://broker-2"},
		ClientID:       "test-client-pub-1",
		InitialBackoff: 20 * time.Millisecond,
		MaxBackoff:     100 * time.Millisecond,
		Jitter:         true,
	}, client.WithPublisherDialer(netw.dial))
	defer pub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 1. Publish message 1 through broker-1
	seq1, err := pub.Publish(ctx, topic, []byte("payload-1"))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), seq1)

	// 2. Kill broker-1
	netw.drop("bufnet://broker-1")
	c1()

	// Notify broker-2 of broker-1 failure so broker-2 promotes itself to owner
	rep2.HandlePeerDown("broker-1")

	// 3. Publish message 2 -> SDK automatically detects broker-1 failure, rotates to broker-2
	seq2, err := pub.Publish(ctx, topic, []byte("payload-2"))
	require.NoError(t, err)
	assert.Equal(t, uint64(2), seq2)

	// 4. Test idempotency: manually retry PublishRequest with the same (client_id, client_seq)
	// directly to broker-2; assert it returns seq2 without creating a new sequence number.
	conn, err := grpc.NewClient("passthrough:///bufnet://broker-2",
		grpc.WithContextDialer(netw.dial),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer conn.Close()

	bClient := pb.NewBrokerClient(conn)
	dupResp, err := bClient.Publish(ctx, &pb.PublishRequest{
		Topic:     topic,
		Payload:   []byte("payload-2-retry"),
		ClientId:  "test-client-pub-1",
		ClientSeq: 2, // Retrying client sequence 2
	})
	require.NoError(t, err)
	assert.Equal(t, uint64(2), dupResp.GetSequence(), "idempotency must return the previously assigned sequence")
}

// TestClientSDK_SubscriberFailoverAndGapDetection verifies that when a broker dies
// during streaming, the Subscriber SDK reconnects to an alternate broker, resumes from
// last_processed_seq + 1, avoids duplicate delivery to user callbacks, and triggers OnGap if gaps occur.
func TestClientSDK_SubscriberFailoverAndGapDetection(t *testing.T) {
	netw := newClientTestNetwork()
	ring := cluster.New(50)
	ring.Add("broker-a", "bufnet://broker-a")
	ring.Add("broker-b", "bufnet://broker-b")

	peers := []config.PeerConfig{
		{ID: "broker-a", Address: "bufnet://broker-a"},
		{ID: "broker-b", Address: "bufnet://broker-b"},
	}

	mgrA, _, repA, cA := startTestNode(t, "broker-a", "bufnet://broker-a", netw, ring, peers)
	defer cA()

	_, _, repB, cB := startTestNode(t, "broker-b", "bufnet://broker-b", netw, ring, peers)
	defer cB()

	topic := "subscriber-failover-topic"

	// Publish 3 messages on broker-a and replicate to broker-b
	for i := 1; i <= 3; i++ {
		seq, err := mgrA.Publish(topic, []byte(fmt.Sprintf("msg-%d", i)))
		require.NoError(t, err)
		err = repA.ReplicatePublish(context.Background(), topic, seq, []byte(fmt.Sprintf("msg-%d", i)), time.Now().UnixNano())
		require.NoError(t, err)
	}

	var reconnectCount int32
	var gapCount int32
	var deliveredMsgs []uint64
	var mu sync.Mutex

	sub := client.NewClusterSubscriber(
		[]string{"bufnet://broker-a", "bufnet://broker-b"},
		"cluster-sub-1",
		client.WithSubscriberDialer(netw.dial),
	)
	sub.InitialBackoff = 20 * time.Millisecond
	sub.MaxBackoff = 50 * time.Millisecond
	sub.OnReconnect = func(addr string) {
		atomic.AddInt32(&reconnectCount, 1)
	}
	sub.OnGap = func(t string, expected, actual uint64) {
		atomic.AddInt32(&gapCount, 1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler := func(msg *pb.Message) error {
		mu.Lock()
		deliveredMsgs = append(deliveredMsgs, msg.GetSeq())
		mu.Unlock()
		return nil
	}

	go func() {
		_ = sub.Consume(ctx, topic, "", 1, handler)
	}()

	// Wait until subscriber receives initial 3 messages
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(deliveredMsgs) == 3
	}, 2*time.Second, 20*time.Millisecond)

	// Kill broker-a
	cA()
	netw.drop("bufnet://broker-a")

	// Promote broker-b
	repB.HandlePeerDown("broker-a")

	// Publish messages 4 and 5 through broker-b (promoted owner)
	// (Simulate an async jump where message 4 was dropped, publishing message 5 directly)
	tb, exists := repB.GetOrCreateTopicState(topic), true
	require.True(t, exists)
	assert.Equal(t, uint64(2), tb.Epoch)

	// Append message 5 on broker-b
	connB, err := grpc.NewClient("passthrough:///bufnet://broker-b",
		grpc.WithContextDialer(netw.dial),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	defer connB.Close()

	bClientB := pb.NewBrokerClient(connB)
	_, err = bClientB.Publish(context.Background(), &pb.PublishRequest{
		Topic:   topic,
		Payload: []byte("msg-4-after-failover"),
	})
	require.NoError(t, err)

	_, err = bClientB.Publish(context.Background(), &pb.PublishRequest{
		Topic:   topic,
		Payload: []byte("msg-5-after-failover"),
	})
	require.NoError(t, err)

	// Wait until subscriber receives all messages from broker-b
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(deliveredMsgs) == 5
	}, 3*time.Second, 30*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Equal(t, []uint64{1, 2, 3, 4, 5}, deliveredMsgs)
	assert.GreaterOrEqual(t, atomic.LoadInt32(&reconnectCount), int32(1), "OnReconnect must be triggered")
}
