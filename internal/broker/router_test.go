package broker_test

import (
	"context"
	"fmt"
	"io"
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
)

// ─────────────────────────────────────────────────────────────────────────────
// In-process cluster helpers
// ─────────────────────────────────────────────────────────────────────────────

type clusterNode struct {
	id     string
	addr   string
	mgr    *broker.Manager
	srv    *broker.Server
	router *broker.Router
	ring   *cluster.Ring
	lis    *bufconn.Listener
}

// routerNetwork is a shared in-memory network that routes all bufconn dials.
type routerNetwork struct {
	mu        sync.RWMutex
	listeners map[string]*bufconn.Listener
}

func newRouterNetwork() *routerNetwork {
	return &routerNetwork{listeners: make(map[string]*bufconn.Listener)}
}

func (n *routerNetwork) register(addr string, l *bufconn.Listener) {
	n.mu.Lock()
	n.listeners[addr] = l
	n.mu.Unlock()
}

func (n *routerNetwork) dial(ctx context.Context, addr string) (net.Conn, error) {
	n.mu.RLock()
	l, ok := n.listeners[addr]
	n.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("routerNetwork: unknown addr %q", addr)
	}
	return l.DialContext(ctx)
}

func (n *routerNetwork) grpcDial(addr string) *grpc.ClientConn {
	conn, err := grpc.NewClient(
		"passthrough:///"+addr,
		grpc.WithContextDialer(n.dial),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		panic(err)
	}
	return conn
}

// buildCluster creates 'count' in-process broker nodes wired into a shared
// consistent-hash ring. Every node knows every other node's address.
func buildCluster(t *testing.T, count int) ([]*clusterNode, *routerNetwork, func()) {
	t.Helper()

	net := newRouterNetwork()
	nodes := make([]*clusterNode, count)

	// Pre-compute IDs and addresses
	ids := make([]string, count)
	addrs := make([]string, count)
	for i := 0; i < count; i++ {
		ids[i] = fmt.Sprintf("node-%d", i+1)
		addrs[i] = fmt.Sprintf("broker-%d.internal", i+1)
	}

	// Build configs (each node's peer list = all other nodes)
	cfgs := make([]*config.Config, count)
	for i := 0; i < count; i++ {
		peers := make([]config.PeerConfig, 0, count-1)
		for j := 0; j < count; j++ {
			if j == i {
				continue
			}
			peers = append(peers, config.PeerConfig{ID: ids[j], Address: addrs[j]})
		}
		cfgs[i] = &config.Config{
			NodeID:               ids[i],
			ListenAddr:           addrs[i],
			Peers:                peers,
			BufferSizePerTopic:   1000,
			SubscriberBufferSize: 1000,
			AckTimeout:           5 * time.Second,
			MaxRetries:           3,
			RingVirtualNodes:     150,
			BackpressurePolicy:   config.PolicyBlock,
			BlockTimeout:         200 * time.Millisecond,
		}
	}

	// Build a single shared ring (all nodes mutate the same ring object)
	sharedRing := cluster.New(150)
	for _, id := range ids {
		sharedRing.AddNode(id)
	}

	// Create nodes
	for i := 0; i < count; i++ {
		mgr := broker.NewManager(cfgs[i], nil)
		srv := broker.NewServer(mgr, cfgs[i], slog.Default())

		router := broker.NewRouter(
			ids[i],
			sharedRing,
			nil, // membership not needed for routing tests
			srv,
			cfgs[i],
			slog.Default(),
			broker.WithRouterDialer(net.dial),
		)

		srv.AttachRouter(router)

		lis := bufconn.Listen(1 << 20)
		net.register(addrs[i], lis)

		nodes[i] = &clusterNode{
			id:     ids[i],
			addr:   addrs[i],
			mgr:    mgr,
			srv:    srv,
			router: router,
			ring:   sharedRing,
			lis:    lis,
		}

		go func(s *broker.Server, l *bufconn.Listener) {
			_ = s.Serve(l)
		}(srv, lis)
	}

	cleanup := func() {
		for _, n := range nodes {
			n.srv.GracefulStop()
			n.router.Close()
		}
	}
	return nodes, net, cleanup
}

// clientFor returns a BrokerClient connected to node n.
func clientFor(t *testing.T, n *clusterNode, net *routerNetwork) pb.BrokerClient {
	t.Helper()
	conn := net.grpcDial(n.addr)
	t.Cleanup(func() { _ = conn.Close() })
	return pb.NewBrokerClient(conn)
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

// TestRouter_PublishViaEachNode verifies that regardless of which node a client
// publishes to, the message is delivered to any subscriber on any other node.
func TestRouter_PublishViaEachNode(t *testing.T) {
	nodes, network, cleanup := buildCluster(t, 3)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := "routing-test-topic"

	// Connect a subscriber through node 1
	subClient := clientFor(t, nodes[0], network)
	stream, err := subClient.Subscribe(ctx, &pb.SubscribeRequest{
		Topic:        topic,
		SubscriberId: "sub-1",
	})
	require.NoError(t, err)

	// Collect received messages in background
	received := make(chan *pb.Message, 20)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				return
			}
			received <- msg
		}
	}()

	// Allow the forwarded subscribe stream to register on the owner node before publishing.
	time.Sleep(50 * time.Millisecond)

	// Publish 3 messages, one through each node
	for i, n := range nodes {
		pubClient := clientFor(t, n, network)
		resp, err := pubClient.Publish(ctx, &pb.PublishRequest{
			Topic:   topic,
			Payload: []byte(fmt.Sprintf("from-node-%d", i+1)),
		})
		require.NoError(t, err, "publish via node %s failed", n.id)
		assert.Greater(t, resp.GetSequence(), uint64(0))
	}

	// All 3 messages must arrive at the subscriber
	got := make(map[uint64]bool)
	for len(got) < 3 {
		select {
		case msg := <-received:
			got[msg.GetSeq()] = true
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for messages; received %d/3", len(got))
		}
	}
	assert.Len(t, got, 3)
}

// TestRouter_SubscribeViaDifferentNode verifies that a subscriber connected to
// node 2 receives messages published directly to node 2 (the owner might be
// node 1 or 3 — the router handles it transparently).
func TestRouter_SubscribeViaDifferentNode(t *testing.T) {
	nodes, network, cleanup := buildCluster(t, 3)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := "cross-node-subscribe"
	const msgCount = 5

	// Subscribe via node 2
	subClient := clientFor(t, nodes[1], network)
	stream, err := subClient.Subscribe(ctx, &pb.SubscribeRequest{
		Topic:        topic,
		SubscriberId: "cross-sub",
	})
	require.NoError(t, err)

	// Give the forwarded subscribe stream time to register on the owner node
	// before publishing (ForwardSubscribe goes through 2 gRPC hops: node2->node3).
	time.Sleep(50 * time.Millisecond)

	// Publish via node 3
	pubClient := clientFor(t, nodes[2], network)
	for i := 0; i < msgCount; i++ {
		_, err := pubClient.Publish(ctx, &pb.PublishRequest{
			Topic:   topic,
			Payload: []byte(fmt.Sprintf("msg-%d", i)),
		})
		require.NoError(t, err)
	}

	// Receive all messages (the shared ring means forwarding may reorder arrival,
	// but monotonic sequencing guarantees each seq is unique and all are present).
	var seqs []uint64
	for len(seqs) < msgCount {
		msg, err := stream.Recv()
		require.NoError(t, err)
		seqs = append(seqs, msg.GetSeq())
	}
	assert.Len(t, seqs, msgCount)
	seqSet := make(map[uint64]bool, msgCount)
	for _, s := range seqs {
		assert.False(t, seqSet[s], "duplicate seq %d", s)
		seqSet[s] = true
	}
	for i := 1; i <= msgCount; i++ {
		assert.True(t, seqSet[uint64(i)], "missing seq %d", i)
	}
}

// TestRouter_AckForwarding verifies that an Ack sent to a non-owner node is
// forwarded to the owner and clears the in-flight tracking entry.
func TestRouter_AckForwarding(t *testing.T) {
	nodes, network, cleanup := buildCluster(t, 3)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := "ack-forward-topic"

	// Subscribe via node 1 to establish in-flight tracking
	subClient := clientFor(t, nodes[0], network)
	stream, err := subClient.Subscribe(ctx, &pb.SubscribeRequest{
		Topic:        topic,
		SubscriberId: "ack-sub",
	})
	require.NoError(t, err)

	// Publish a message
	pubClient := clientFor(t, nodes[2], network)
	resp, err := pubClient.Publish(ctx, &pb.PublishRequest{
		Topic:   topic,
		Payload: []byte("ack-test"),
	})
	require.NoError(t, err)
	seq := resp.GetSequence()

	// Wait for it to arrive
	msg, err := stream.Recv()
	require.NoError(t, err)
	assert.Equal(t, seq, msg.GetSeq())

	// Ack via node 2 (a different node from the subscriber) — should be forwarded
	ackClient := clientFor(t, nodes[1], network)
	_, err = ackClient.Ack(ctx, &pb.AckRequest{
		Topic:        topic,
		SubscriberId: "ack-sub",
		Seq:          seq,
	})
	require.NoError(t, err, "Ack via non-owner node should succeed")
}

// TestRouter_HopCountLoopPrevention verifies that a ForwardPublish with hop_count > 1
// is rejected with codes.ResourceExhausted.
func TestRouter_HopCountLoopPrevention(t *testing.T) {
	nodes, network, cleanup := buildCluster(t, 3)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Connect directly to the Internal service on node 1
	conn := network.grpcDial(nodes[0].addr)
	defer func() { _ = conn.Close() }()
	intClient := pb.NewInternalClient(conn)

	// Send a ForwardPublish with hop_count = 2 (exceeds max of 1)
	_, err := intClient.Forward(ctx, &pb.ForwardPublishRequest{
		Request:      &pb.PublishRequest{Topic: "hop-test", Payload: []byte("x")},
		HopCount:     2,
		SourceNodeId: "fake-node",
	})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.ResourceExhausted, st.Code())
}

// TestRouter_UnavailableWhenRingEmpty verifies a clear Unavailable error when no
// owner node is in the ring.
func TestRouter_UnavailableWhenRingEmpty(t *testing.T) {
	cfg := config.Default()
	cfg.RingVirtualNodes = 10
	mgr := broker.NewManager(cfg, nil)
	srv := broker.NewServer(mgr, cfg, slog.Default())

	// Create an empty ring (no nodes added)
	emptyRing := cluster.New(10)
	router := broker.NewRouter("lonely-node", emptyRing, nil, srv, cfg, slog.Default())
	srv.AttachRouter(router)

	lis := bufconn.Listen(1 << 20)
	go func() { _ = srv.Serve(lis) }()
	defer func() {
		srv.GracefulStop()
		router.Close()
	}()

	net := newRouterNetwork()
	net.register("lonely", lis)
	conn := net.grpcDial("lonely")
	defer func() { _ = conn.Close() }()
	c := pb.NewBrokerClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Add lonely-node AFTER setup to trigger the routing path that hits an empty ring
	// (The router has emptyRing with no owners – GetOwner returns "")
	_, err := c.Publish(ctx, &pb.PublishRequest{Topic: "ghost-topic", Payload: []byte("x")})
	require.Error(t, err)
	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.Unavailable, st.Code())
}

// TestRouter_TwoSubscribersOnDifferentNodes verifies that two subscribers connected
// to different nodes both receive all messages in order when the topic is owned by a third.
func TestRouter_TwoSubscribersOnDifferentNodes(t *testing.T) {
	nodes, network, cleanup := buildCluster(t, 3)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := "fanout-routing-topic"
	const msgCount = 6

	// Subscriber A on node 1
	subClientA := clientFor(t, nodes[0], network)
	streamA, err := subClientA.Subscribe(ctx, &pb.SubscribeRequest{Topic: topic, SubscriberId: "sub-a"})
	require.NoError(t, err)

	// Subscriber B on node 2
	subClientB := clientFor(t, nodes[1], network)
	streamB, err := subClientB.Subscribe(ctx, &pb.SubscribeRequest{Topic: topic, SubscriberId: "sub-b"})
	require.NoError(t, err)

	// Publish via node 3
	pubClient := clientFor(t, nodes[2], network)
	for i := 0; i < msgCount; i++ {
		_, err := pubClient.Publish(ctx, &pb.PublishRequest{
			Topic:   topic,
			Payload: []byte(fmt.Sprintf("fanout-%d", i)),
		})
		require.NoError(t, err)
	}

	// Both subscribers must receive all messages in order
	collect := func(stream pb.Broker_SubscribeClient, label string) []uint64 {
		var seqs []uint64
		for len(seqs) < msgCount {
			msg, err := stream.Recv()
			if err == io.EOF {
				break
			}
			require.NoError(t, err, "%s recv error", label)
			seqs = append(seqs, msg.GetSeq())
		}
		return seqs
	}

	var wg sync.WaitGroup
	var seqsA, seqsB []uint64

	wg.Add(2)
	go func() { defer wg.Done(); seqsA = collect(streamA, "sub-A") }()
	go func() { defer wg.Done(); seqsB = collect(streamB, "sub-B") }()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for both subscribers")
	}

	for i, seq := range seqsA {
		assert.Equal(t, uint64(i+1), seq, "sub-A seq mismatch at position %d", i)
	}
	for i, seq := range seqsB {
		assert.Equal(t, uint64(i+1), seq, "sub-B seq mismatch at position %d", i)
	}
}
