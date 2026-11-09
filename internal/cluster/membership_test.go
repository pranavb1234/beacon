package cluster_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"

	"github.com/pranavb1234/beacon/internal/cluster"
	"github.com/pranavb1234/beacon/internal/config"
	"github.com/pranavb1234/beacon/internal/pb"
)

// memoryNetwork routes gRPC connections between in-process nodes using bufconn.
type memoryNetwork struct {
	mu        sync.RWMutex
	listeners map[string]*bufconn.Listener
	stopped   map[string]bool
}

func newMemoryNetwork() *memoryNetwork {
	return &memoryNetwork{
		listeners: make(map[string]*bufconn.Listener),
		stopped:   make(map[string]bool),
	}
}

func (n *memoryNetwork) Register(addr string, lis *bufconn.Listener) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.listeners[addr] = lis
	delete(n.stopped, addr)
}

func (n *memoryNetwork) StopNode(addr string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.stopped[addr] = true
	if lis, ok := n.listeners[addr]; ok {
		_ = lis.Close()
	}
}

func (n *memoryNetwork) Dial(ctx context.Context, addr string) (net.Conn, error) {
	n.mu.RLock()
	defer n.mu.RUnlock()

	if n.stopped[addr] {
		return nil, errors.New("connection refused: node stopped")
	}
	lis, ok := n.listeners[addr]
	if !ok {
		return nil, fmt.Errorf("unknown address: %s", addr)
	}
	return lis.DialContext(ctx)
}

type testNode struct {
	id         string
	addr       string
	cfg        *config.Config
	ring       *cluster.Ring
	membership *cluster.Membership
	grpcServer *grpc.Server
	listener   *bufconn.Listener
	ctx        context.Context
	cancel     context.CancelFunc
}

func startTestNode(
	ctx context.Context,
	id, addr string,
	peers []config.PeerConfig,
	netw *memoryNetwork,
	hbInterval, hbTimeout time.Duration,
) (*testNode, error) {
	cfg := &config.Config{
		NodeID:            id,
		ListenAddr:        addr,
		Peers:             peers,
		HeartbeatInterval: hbInterval,
		HeartbeatTimeout:  hbTimeout,
		RingVirtualNodes:  50,
	}

	ring := cluster.New(50)
	logger := slog.Default()

	membership, err := cluster.NewMembership(
		cfg,
		ring,
		logger,
		cluster.WithDialer(netw.Dial),
		cluster.WithHeartbeatInterval(hbInterval),
		cluster.WithHeartbeatTimeout(hbTimeout),
	)
	if err != nil {
		return nil, err
	}

	lis := bufconn.Listen(1024 * 1024)
	netw.Register(addr, lis)

	grpcServer := grpc.NewServer()
	pb.RegisterInternalServer(grpcServer, membership)

	go func() {
		_ = grpcServer.Serve(lis)
	}()

	nodeCtx, cancel := context.WithCancel(ctx)
	if err := membership.Start(nodeCtx); err != nil {
		cancel()
		grpcServer.Stop()
		return nil, err
	}

	return &testNode{
		id:         id,
		addr:       addr,
		cfg:        cfg,
		ring:       ring,
		membership: membership,
		grpcServer: grpcServer,
		listener:   lis,
		ctx:        nodeCtx,
		cancel:     cancel,
	}, nil
}

func (n *testNode) Stop() {
	n.cancel()
	n.membership.Stop()
	n.grpcServer.Stop()
	_ = n.listener.Close()
}

// TestMembership_ThreeNodes_FailureAndRecovery tests a 3-node cluster:
// 1. All 3 nodes discover each other and form a 3-node hash ring.
// 2. Stop Node 3: Node 1 and Node 2 detect Node 3 is dead within the timeout and remove it from the ring.
// 3. Restart Node 3: Node 1 and Node 2 detect recovery and restore Node 3 to the ring.
func TestMembership_ThreeNodes_FailureAndRecovery(t *testing.T) {
	netw := newMemoryNetwork()

	hbInterval := 30 * time.Millisecond
	hbTimeout := 120 * time.Millisecond

	peerList := []config.PeerConfig{
		{ID: "node-1", Address: "bufnet://node-1"},
		{ID: "node-2", Address: "bufnet://node-2"},
		{ID: "node-3", Address: "bufnet://node-3"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Start 3 in-process nodes
	n1, err := startTestNode(ctx, "node-1", "bufnet://node-1", peerList, netw, hbInterval, hbTimeout)
	require.NoError(t, err)
	defer n1.Stop()

	n2, err := startTestNode(ctx, "node-2", "bufnet://node-2", peerList, netw, hbInterval, hbTimeout)
	require.NoError(t, err)
	defer n2.Stop()

	n3, err := startTestNode(ctx, "node-3", "bufnet://node-3", peerList, netw, hbInterval, hbTimeout)
	require.NoError(t, err)

	// Set up event tracking for n1
	var n1PeerDownCount int32
	var n1PeerUpCount int32
	n1.membership.OnPeerDown(func(id string) {
		atomic.AddInt32(&n1PeerDownCount, 1)
	})
	n1.membership.OnPeerUp(func(id string) {
		atomic.AddInt32(&n1PeerUpCount, 1)
	})

	// 2. Assert all 3 nodes detect each other as alive and hash rings contain 3 nodes
	require.Eventually(t, func() bool {
		nodes1 := n1.ring.Nodes()
		nodes2 := n2.ring.Nodes()
		nodes3 := n3.ring.Nodes()
		return len(nodes1) == 3 && len(nodes2) == 3 && len(nodes3) == 3
	}, 1*time.Second, 20*time.Millisecond, "all nodes must discover 3 peers on the ring")

	assert.ElementsMatch(t, []string{"node-1", "node-2", "node-3"}, n1.ring.Nodes())
	assert.ElementsMatch(t, []string{"node-1", "node-2", "node-3"}, n2.ring.Nodes())
	assert.ElementsMatch(t, []string{"node-1", "node-2", "node-3"}, n3.ring.Nodes())

	pState, exists := n1.membership.GetPeerState("node-3")
	require.True(t, exists)
	assert.True(t, pState.Alive)

	// 3. Stop Node 3
	t.Log("Stopping node-3...")
	netw.StopNode("bufnet://node-3")
	n3.Stop()

	// 4. Assert Node 1 and Node 2 detect Node 3 down within timeout and remove it from their rings
	require.Eventually(t, func() bool {
		s1, _ := n1.membership.GetPeerState("node-3")
		s2, _ := n2.membership.GetPeerState("node-3")
		return !s1.Alive && !s2.Alive
	}, 500*time.Millisecond, 20*time.Millisecond, "node-1 and node-2 must mark node-3 dead")

	assert.ElementsMatch(t, []string{"node-1", "node-2"}, n1.ring.Nodes())
	assert.ElementsMatch(t, []string{"node-1", "node-2"}, n2.ring.Nodes())
	assert.GreaterOrEqual(t, atomic.LoadInt32(&n1PeerDownCount), int32(1), "OnPeerDown callback must fire")

	// 5. Restart Node 3
	t.Log("Restarting node-3...")
	n3Restarted, err := startTestNode(ctx, "node-3", "bufnet://node-3", peerList, netw, hbInterval, hbTimeout)
	require.NoError(t, err)
	defer n3Restarted.Stop()

	// 6. Assert Node 1 and Node 2 detect recovery and restore Node 3 to their rings
	require.Eventually(t, func() bool {
		s1, _ := n1.membership.GetPeerState("node-3")
		s2, _ := n2.membership.GetPeerState("node-3")
		return s1.Alive && s2.Alive && len(n1.ring.Nodes()) == 3 && len(n2.ring.Nodes()) == 3
	}, 1*time.Second, 20*time.Millisecond, "node-1 and node-2 must restore node-3 on recovery")

	assert.ElementsMatch(t, []string{"node-1", "node-2", "node-3"}, n1.ring.Nodes())
	assert.ElementsMatch(t, []string{"node-1", "node-2", "node-3"}, n2.ring.Nodes())
	assert.GreaterOrEqual(t, atomic.LoadInt32(&n1PeerUpCount), int32(2), "OnPeerUp callback must fire on recovery")
}

// TestMembership_NoGoroutineLeaks verifies that stopping the membership manager
// via context cancellation cleanly terminates all goroutines without leaking.
func TestMembership_NoGoroutineLeaks(t *testing.T) {
	// Baseline goroutine count
	runtime.GC()
	initialGoroutines := runtime.NumGoroutine()

	netw := newMemoryNetwork()
	peerList := []config.PeerConfig{
		{ID: "peer-a", Address: "bufnet://peer-a"},
		{ID: "peer-b", Address: "bufnet://peer-b"},
	}

	ctx, cancel := context.WithCancel(context.Background())

	// Create and start membership with 2 remote peers
	node, err := startTestNode(ctx, "local-node", "bufnet://local-node", peerList, netw, 20*time.Millisecond, 80*time.Millisecond)
	require.NoError(t, err)

	// Run for a few ticks
	time.Sleep(100 * time.Millisecond)

	// Stop everything
	cancel()
	node.Stop()

	// Assert goroutine count returns to baseline
	require.Eventually(t, func() bool {
		runtime.GC()
		curr := runtime.NumGoroutine()
		return curr <= initialGoroutines+1 // allow at most 1 transient runtime goroutine
	}, 2*time.Second, 30*time.Millisecond, "all background goroutines must terminate cleanly")
}

// TestMembership_EventChannel verifies that events are delivered to the Events channel.
func TestMembership_EventChannel(t *testing.T) {
	netw := newMemoryNetwork()
	peerList := []config.PeerConfig{
		{ID: "node-2", Address: "bufnet://node-2"},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	n1, err := startTestNode(ctx, "node-1", "bufnet://node-1", peerList, netw, 30*time.Millisecond, 100*time.Millisecond)
	require.NoError(t, err)
	defer n1.Stop()

	n2, err := startTestNode(ctx, "node-2", "bufnet://node-2", []config.PeerConfig{{ID: "node-1", Address: "bufnet://node-1"}}, netw, 30*time.Millisecond, 100*time.Millisecond)
	require.NoError(t, err)
	defer n2.Stop()

	select {
	case ev := <-n1.membership.Events():
		assert.Equal(t, cluster.PeerUp, ev.Type)
		assert.Equal(t, "node-2", ev.NodeID)
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for PeerUp event on event channel")
	}
}
