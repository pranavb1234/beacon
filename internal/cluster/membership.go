// Package cluster provides consistent hash ring routing and cluster membership management.
package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/pranavb1234/beacon/internal/config"
	"github.com/pranavb1234/beacon/internal/pb"
)

// PeerState tracks the liveness and metadata of a cluster peer.
type PeerState struct {
	ID       string    `json:"id"`
	Address  string    `json:"address"`
	LastSeen time.Time `json:"last_seen"`
	Alive    bool      `json:"alive"`
	Epoch    uint64    `json:"epoch"`
}

// DialerFunc allows custom network dialing (e.g. bufconn for testing).
type DialerFunc func(ctx context.Context, addr string) (net.Conn, error)

type peerTracker struct {
	state  PeerState
	conn   *grpc.ClientConn
	client pb.InternalClient
}

// MembershipOption configures Membership instances.
type MembershipOption func(*Membership)

// WithDialer sets a custom dialer function for inter-peer gRPC connections.
func WithDialer(dialer DialerFunc) MembershipOption {
	return func(m *Membership) {
		m.dialer = dialer
	}
}

// WithEpoch sets the initial epoch for this broker.
func WithEpoch(epoch uint64) MembershipOption {
	return func(m *Membership) {
		m.epoch = epoch
	}
}

// WithHeartbeatInterval overrides the interval at which heartbeats are dispatched.
func WithHeartbeatInterval(interval time.Duration) MembershipOption {
	return func(m *Membership) {
		m.heartbeatInterval = interval
	}
}

// WithHeartbeatTimeout overrides the duration of silence after which a peer is declared dead.
func WithHeartbeatTimeout(timeout time.Duration) MembershipOption {
	return func(m *Membership) {
		m.heartbeatTimeout = timeout
	}
}

// Membership manages broker cluster membership, peer health tracking via periodic heartbeats,
// and automatic hash ring synchronization on peer topology changes.
type Membership struct {
	pb.UnimplementedInternalServer

	mu     sync.RWMutex
	nodeID string
	epoch  uint64
	cfg    *config.Config
	ring   *Ring
	logger *slog.Logger

	heartbeatInterval time.Duration
	heartbeatTimeout  time.Duration

	peers  map[string]*peerTracker
	dialer DialerFunc

	cbMu              sync.RWMutex
	peerUpCallbacks   []func(id string)
	peerDownCallbacks []func(id string)

	eventCh chan PeerEvent

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

// PeerEventType indicates whether a peer joined or failed.
type PeerEventType int

const (
	// PeerUp indicates a peer has become healthy/alive.
	PeerUp PeerEventType = iota
	// PeerDown indicates a peer has timed out or failed.
	PeerDown
)

// PeerEvent represents a topology change event.
type PeerEvent struct {
	Type   PeerEventType
	NodeID string
}

// NewMembership initializes cluster membership tracking using static peer definitions from config.
func NewMembership(cfg *config.Config, ring *Ring, logger *slog.Logger, opts ...MembershipOption) (*Membership, error) {
	if cfg == nil {
		cfg = config.Default()
	}
	if logger == nil {
		logger = slog.Default()
	}
	if ring == nil {
		ring = New(cfg.RingVirtualNodes)
	}

	interval := cfg.HeartbeatInterval
	if interval <= 0 {
		interval = 1 * time.Second
	}
	timeout := cfg.HeartbeatTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	m := &Membership{
		nodeID:            cfg.NodeID,
		epoch:             1,
		cfg:               cfg,
		ring:              ring,
		logger:            logger.With("module", "membership", "local_node", cfg.NodeID),
		heartbeatInterval: interval,
		heartbeatTimeout:  timeout,
		peers:             make(map[string]*peerTracker),
		eventCh:           make(chan PeerEvent, 64),
	}

	for _, opt := range opts {
		opt(m)
	}

	// Always add the local broker node to the hash ring
	m.ring.Add(cfg.NodeID, cfg.ListenAddr)

	// Initialize peer tracking states for each configured remote peer
	for _, p := range cfg.Peers {
		if p.ID == cfg.NodeID {
			continue // Do not self-monitor
		}
		m.peers[p.ID] = &peerTracker{
			state: PeerState{
				ID:      p.ID,
				Address: p.Address,
				Alive:   false,
			},
		}
	}

	return m, nil
}

// NodeID returns this broker node's identifier.
func (m *Membership) NodeID() string {
	return m.nodeID
}

// Epoch returns the current cluster epoch.
func (m *Membership) Epoch() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.epoch
}

// Ring returns the synchronized consistent hash ring.
func (m *Membership) Ring() *Ring {
	return m.ring
}

// OnPeerUp registers a callback invoked whenever a peer node transitions to alive.
func (m *Membership) OnPeerUp(cb func(id string)) {
	m.cbMu.Lock()
	defer m.cbMu.Unlock()
	m.peerUpCallbacks = append(m.peerUpCallbacks, cb)
}

// OnPeerDown registers a callback invoked whenever a peer node transitions to dead.
func (m *Membership) OnPeerDown(cb func(id string)) {
	m.cbMu.Lock()
	defer m.cbMu.Unlock()
	m.peerDownCallbacks = append(m.peerDownCallbacks, cb)
}

// Events returns a receive-only channel of peer up/down topology events.
func (m *Membership) Events() <-chan PeerEvent {
	return m.eventCh
}

// RegisterGRPC registers the internal cluster service onto a gRPC server.
func (m *Membership) RegisterGRPC(s *grpc.Server) {
	pb.RegisterInternalServer(s, m)
}

// Start begins periodic heartbeat monitoring routines for each peer.
func (m *Membership) Start(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.cancel != nil {
		return fmt.Errorf("membership monitor already running")
	}

	m.ctx, m.cancel = context.WithCancel(ctx)

	for _, tracker := range m.peers {
		var dialOpts []grpc.DialOption
		dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if m.dialer != nil {
			dialOpts = append(dialOpts, grpc.WithContextDialer(m.dialer))
		}

		target := tracker.state.Address
		if m.dialer != nil {
			target = "passthrough:///" + tracker.state.Address
		}

		conn, err := grpc.NewClient(target, dialOpts...)
		if err != nil {
			m.logger.Error("failed to create gRPC client for peer",
				"peer_id", tracker.state.ID,
				"addr", tracker.state.Address,
				"err", err,
			)
			continue
		}

		tracker.conn = conn
		tracker.client = pb.NewInternalClient(conn)

		m.wg.Add(1)
		go m.peerHeartbeatLoop(m.ctx, tracker)
	}

	return nil
}

// Stop gracefully stops all peer heartbeat routines and closes reused connections.
func (m *Membership) Stop() {
	m.mu.Lock()
	cancel := m.cancel
	m.cancel = nil
	m.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	m.wg.Wait()
}

func (m *Membership) peerHeartbeatLoop(ctx context.Context, tracker *peerTracker) {
	defer m.wg.Done()
	defer func() {
		if tracker.conn != nil {
			_ = tracker.conn.Close()
		}
	}()

	ticker := time.NewTicker(m.heartbeatInterval)
	defer ticker.Stop()

	// Immediately perform initial heartbeat check
	m.sendHeartbeat(ctx, tracker)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			m.sendHeartbeat(ctx, tracker)
		}
	}
}

func (m *Membership) sendHeartbeat(ctx context.Context, tracker *peerTracker) {
	callCtx, cancel := context.WithTimeout(ctx, m.heartbeatInterval)
	defer cancel()

	m.mu.RLock()
	epoch := m.epoch
	nodeID := m.nodeID
	m.mu.RUnlock()

	resp, err := tracker.client.Heartbeat(callCtx, &pb.HeartbeatRequest{
		NodeId:    nodeID,
		Epoch:     epoch,
		Timestamp: time.Now().UnixNano(),
	})

	now := time.Now()
	var toNotifyUp, toNotifyDown string

	if err != nil {
		m.logger.Debug("heartbeat RPC failed", "peer", tracker.state.ID, "addr", tracker.state.Address, "err", err)
	}

	m.mu.Lock()
	if err == nil && resp != nil && resp.GetOk() {
		tracker.state.LastSeen = now
		if !tracker.state.Alive {
			tracker.state.Alive = true
			m.ring.Add(tracker.state.ID, tracker.state.Address)
			toNotifyUp = tracker.state.ID
			m.logger.Info("peer marked ALIVE", "peer_id", tracker.state.ID, "addr", tracker.state.Address)
		}
	} else {
		if tracker.state.Alive && now.Sub(tracker.state.LastSeen) >= m.heartbeatTimeout {
			tracker.state.Alive = false
			m.ring.Remove(tracker.state.ID)
			toNotifyDown = tracker.state.ID
			m.logger.Warn("peer marked DEAD (timeout)",
				"peer_id", tracker.state.ID,
				"elapsed", now.Sub(tracker.state.LastSeen),
			)
		}
	}
	m.mu.Unlock()

	if toNotifyUp != "" {
		m.triggerPeerUp(toNotifyUp)
	}
	if toNotifyDown != "" {
		m.triggerPeerDown(toNotifyDown)
	}
}

// Heartbeat handles incoming Heartbeat RPCs from cluster peers.
func (m *Membership) Heartbeat(ctx context.Context, req *pb.HeartbeatRequest) (*pb.HeartbeatResponse, error) {
	fromID := req.GetNodeId()
	if fromID != "" {
		now := time.Now()
		var toNotifyUp string

		m.mu.Lock()
		if tracker, exists := m.peers[fromID]; exists {
			tracker.state.LastSeen = now
			tracker.state.Epoch = req.GetEpoch()
			if !tracker.state.Alive {
				tracker.state.Alive = true
				m.ring.Add(fromID, tracker.state.Address)
				toNotifyUp = fromID
				m.logger.Info("peer marked ALIVE (incoming heartbeat)", "peer_id", fromID)
			}
		}
		m.mu.Unlock()

		if toNotifyUp != "" {
			m.triggerPeerUp(toNotifyUp)
		}
	}

	m.mu.RLock()
	localNodeID := m.nodeID
	m.mu.RUnlock()

	return &pb.HeartbeatResponse{
		NodeId: localNodeID,
		Ok:     true,
	}, nil
}

// PeerStates returns a snapshot map of all tracked peers and their health status.
func (m *Membership) PeerStates() map[string]PeerState {
	m.mu.RLock()
	defer m.mu.RUnlock()

	out := make(map[string]PeerState, len(m.peers))
	for id, t := range m.peers {
		out[id] = t.state
	}
	return out
}

// GetPeerState returns the state of a specific peer.
func (m *Membership) GetPeerState(id string) (PeerState, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	t, exists := m.peers[id]
	if !exists {
		return PeerState{}, false
	}
	return t.state, true
}

func (m *Membership) triggerPeerUp(id string) {
	m.cbMu.RLock()
	cbs := make([]func(string), len(m.peerUpCallbacks))
	copy(cbs, m.peerUpCallbacks)
	m.cbMu.RUnlock()

	for _, cb := range cbs {
		cb(id)
	}

	select {
	case m.eventCh <- PeerEvent{Type: PeerUp, NodeID: id}:
	default:
	}
}

func (m *Membership) triggerPeerDown(id string) {
	m.cbMu.RLock()
	cbs := make([]func(string), len(m.peerDownCallbacks))
	copy(cbs, m.peerDownCallbacks)
	m.cbMu.RUnlock()

	for _, cb := range cbs {
		cb(id)
	}

	select {
	case m.eventCh <- PeerEvent{Type: PeerDown, NodeID: id}:
	default:
	}
}
