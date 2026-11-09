// Package replication implements owner-to-replica topic replication,
// synchronous and asynchronous delivery modes, replica catch-up via ring buffer replay,
// and state transitions for cluster failover.
package replication

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/pranavb1234/beacon/internal/broker"
	"github.com/pranavb1234/beacon/internal/cluster"
	"github.com/pranavb1234/beacon/internal/config"
	"github.com/pranavb1234/beacon/internal/pb"
)

// Role describes the broker's current role for a topic.
type Role int

const (
	// RoleOwner indicates this node is the active primary owner of the topic.
	RoleOwner Role = iota
	// RoleReplica indicates this node is the backup replica for the topic.
	RoleReplica
	// RoleSteppedDown indicates a partitioned or stale owner has stepped down.
	RoleSteppedDown
)

func (r Role) String() string {
	switch r {
	case RoleOwner:
		return "owner"
	case RoleReplica:
		return "replica"
	case RoleSteppedDown:
		return "stepped down"
	default:
		return "unknown"
	}
}

// TopicState holds the replication and epoch state for a single topic.
type TopicState struct {
	mu                sync.RWMutex
	Topic             string
	Epoch             uint64
	Role              Role
	ReplicaID         string
	IsDegraded        bool
	LastReplicatedSeq uint64
	DetectedGaps      uint64
}

// Replicator manages data and consumer-group offset replication from topic owners to replicas.
type Replicator struct {
	mu      sync.RWMutex
	nodeID  string
	cfg     *config.Config
	ring    *cluster.Ring
	manager *broker.Manager
	logger  *slog.Logger
	dialer  cluster.DialerFunc

	states map[string]*TopicState

	// syncWaiters tracks pending acks in sync mode: topic -> seq -> chan error
	waitersMu   sync.Mutex
	syncWaiters map[string]map[uint64]chan error

	// asyncQueues holds buffered channels for async replication mode
	queuesMu    sync.RWMutex
	asyncQueues map[string]chan *pb.ReplicateMsg

	// Reusable peer client connections
	peersMu sync.RWMutex
	peers   map[string]*grpc.ClientConn

	// Stream connections for live replication
	streamsMu sync.Mutex
	streams   map[string]pb.Internal_ReplicateClient

	handoffCount uint64
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

// Option configures Replicator.
type Option func(*Replicator)

// WithDialer sets a custom dialer for gRPC connections (useful for bufconn in tests).
func WithDialer(d cluster.DialerFunc) Option {
	return func(r *Replicator) {
		r.dialer = d
	}
}

// NewReplicator creates a new Replicator instance.
func NewReplicator(
	nodeID string,
	cfg *config.Config,
	ring *cluster.Ring,
	manager *broker.Manager,
	logger *slog.Logger,
	opts ...Option,
) *Replicator {
	if cfg == nil {
		cfg = config.Default()
	}
	if logger == nil {
		logger = slog.Default()
	}

	ctx, cancel := context.WithCancel(context.Background())

	r := &Replicator{
		nodeID:      nodeID,
		cfg:         cfg,
		ring:        ring,
		manager:     manager,
		logger:      logger.With("module", "replication", "local_node", nodeID),
		states:      make(map[string]*TopicState),
		syncWaiters: make(map[string]map[uint64]chan error),
		asyncQueues: make(map[string]chan *pb.ReplicateMsg),
		peers:       make(map[string]*grpc.ClientConn),
		streams:     make(map[string]pb.Internal_ReplicateClient),
		ctx:         ctx,
		cancel:      cancel,
	}

	for _, opt := range opts {
		opt(r)
	}

	return r
}

// Close cleans up all active streams, connections, and worker routines.
func (r *Replicator) Close() {
	r.cancel()
	r.wg.Wait()

	r.peersMu.Lock()
	for id, conn := range r.peers {
		_ = conn.Close()
		delete(r.peers, id)
	}
	r.peersMu.Unlock()
}

// GetOrCreateTopicState returns or initializes the topic replication state.
func (r *Replicator) GetOrCreateTopicState(topic string) *TopicState {
	r.mu.Lock()
	defer r.mu.Unlock()

	if s, exists := r.states[topic]; exists {
		return s
	}

	epoch := uint64(1)
	if t, exists := r.manager.GetTopic(topic); exists {
		epoch = t.Epoch()
	}

	s := &TopicState{
		Topic: topic,
		Epoch: epoch,
		Role:  RoleOwner,
	}
	r.states[topic] = s
	return s
}

// IsDegraded returns whether the replica for the specified topic is degraded.
func (r *Replicator) IsDegraded(topic string) bool {
	s := r.GetOrCreateTopicState(topic)
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.IsDegraded
}

// DetectedGaps returns the number of sequence gaps detected by this replica on topic.
func (r *Replicator) DetectedGaps(topic string) uint64 {
	s := r.GetOrCreateTopicState(topic)
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.DetectedGaps
}

// HandoffsTotal returns the total count of graceful topic hand-offs completed.
func (r *Replicator) HandoffsTotal() uint64 {
	return atomic.LoadUint64(&r.handoffCount)
}

func (r *Replicator) targetReplica(topic string) string {
	replicaID := r.ring.GetReplica(topic)
	if replicaID != "" && replicaID != r.nodeID {
		return replicaID
	}
	ownerID := r.ring.GetOwner(topic)
	if ownerID != "" && ownerID != r.nodeID {
		return ownerID
	}
	for _, n := range r.ring.Nodes() {
		if n != r.nodeID {
			return n
		}
	}
	return ""
}

// ReplicatePublish replicates a published message from the owner to the current replica.
func (r *Replicator) ReplicatePublish(ctx context.Context, topic string, seq uint64, payload []byte, timestampNS int64) error {
	state := r.GetOrCreateTopicState(topic)

	state.mu.RLock()
	if state.Role == RoleSteppedDown {
		state.mu.RUnlock()
		return status.Errorf(codes.FailedPrecondition, "node %s has stepped down from topic %s", r.nodeID, topic)
	}
	epoch := state.Epoch
	state.mu.RUnlock()

	// Determine replica node ID
	replicaID := r.targetReplica(topic)
	if replicaID == "" || replicaID == r.nodeID {
		// Single-node cluster or no distinct replica available
		return nil
	}

	msg := &pb.ReplicateMsg{
		Topic:       topic,
		Epoch:       epoch,
		Seq:         seq,
		Payload:     payload,
		TimestampNs: timestampNS,
	}

	if r.cfg.ReplicationMode == config.ReplicationModeSync {
		return r.replicateSync(ctx, replicaID, msg)
	}

	// Async mode
	r.replicateAsync(replicaID, msg)
	return nil
}

// ReplicateOffsetCommit replicates a consumer group committed offset update to the replica.
func (r *Replicator) ReplicateOffsetCommit(topic, group string, committedOffset uint64) {
	state := r.GetOrCreateTopicState(topic)

	state.mu.RLock()
	epoch := state.Epoch
	state.mu.RUnlock()

	replicaID := r.targetReplica(topic)
	if replicaID == "" || replicaID == r.nodeID {
		return
	}

	msg := &pb.ReplicateMsg{
		Topic:           topic,
		Epoch:           epoch,
		Group:           group,
		CommittedOffset: committedOffset,
		IsOffsetCommit:  true,
	}

	if r.cfg.ReplicationMode == config.ReplicationModeSync {
		_ = r.replicateSync(context.Background(), replicaID, msg)
		return
	}
	r.replicateAsync(replicaID, msg)
}

func (r *Replicator) replicateSync(ctx context.Context, replicaID string, msg *pb.ReplicateMsg) error {
	state := r.GetOrCreateTopicState(msg.Topic)

	// Register ack waiter
	ackCh := make(chan error, 1)
	r.waitersMu.Lock()
	if _, ok := r.syncWaiters[msg.Topic]; !ok {
		r.syncWaiters[msg.Topic] = make(map[uint64]chan error)
	}
	r.syncWaiters[msg.Topic][msg.Seq] = ackCh
	r.waitersMu.Unlock()

	defer func() {
		r.waitersMu.Lock()
		if m, ok := r.syncWaiters[msg.Topic]; ok {
			delete(m, msg.Seq)
		}
		r.waitersMu.Unlock()
	}()

	// Send message over replicate stream
	stream, err := r.getOrCreateStream(replicaID)
	if err != nil {
		state.mu.Lock()
		state.IsDegraded = true
		state.mu.Unlock()
		return status.Errorf(codes.Unavailable, "cannot reach replica %s: %v", replicaID, err)
	}

	if err := stream.Send(msg); err != nil {
		state.mu.Lock()
		state.IsDegraded = true
		state.mu.Unlock()
		r.invalidateStream(replicaID)
		return status.Errorf(codes.Unavailable, "failed to send replicate msg to %s: %v", replicaID, err)
	}

	timeout := r.cfg.BlockTimeout
	if timeout <= 0 {
		timeout = 500 * time.Millisecond
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-ackCh:
		if err != nil {
			state.mu.Lock()
			state.IsDegraded = true
			state.mu.Unlock()
			return err
		}
		state.mu.Lock()
		state.IsDegraded = false
		state.LastReplicatedSeq = msg.Seq
		state.mu.Unlock()
		return nil
	case <-timer.C:
		state.mu.Lock()
		state.IsDegraded = true
		state.mu.Unlock()
		r.logger.Warn("sync replication timed out; replica marked degraded",
			"topic", msg.Topic,
			"seq", msg.Seq,
			"replica", replicaID,
		)
		return status.Errorf(codes.DeadlineExceeded,
			"sync replication timeout to replica %s on topic %s (seq %d)", replicaID, msg.Topic, msg.Seq)
	}
}

func (r *Replicator) replicateAsync(replicaID string, msg *pb.ReplicateMsg) {
	r.queuesMu.Lock()
	q, ok := r.asyncQueues[replicaID]
	if !ok {
		q = make(chan *pb.ReplicateMsg, 1000)
		r.asyncQueues[replicaID] = q
		r.wg.Add(1)
		go r.asyncWorker(replicaID, q)
	}
	r.queuesMu.Unlock()

	select {
	case q <- msg:
	default:
		r.logger.Warn("async replication buffer full; dropping message from queue",
			"topic", msg.Topic, "seq", msg.Seq, "replica", replicaID)
	}
}

func (r *Replicator) asyncWorker(replicaID string, q <-chan *pb.ReplicateMsg) {
	defer r.wg.Done()
	for {
		select {
		case <-r.ctx.Done():
			return
		case msg := <-q:
			stream, err := r.getOrCreateStream(replicaID)
			if err != nil {
				time.Sleep(50 * time.Millisecond)
				continue
			}
			if err := stream.Send(msg); err != nil {
				r.invalidateStream(replicaID)
				time.Sleep(50 * time.Millisecond)
			}
		}
	}
}

// HandleReplicate processes incoming Replicate streams on the replica node.
func (r *Replicator) HandleReplicate(stream pb.Internal_ReplicateServer) error {
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		state := r.GetOrCreateTopicState(msg.Topic)

		state.mu.Lock()
		state.Role = RoleReplica

		// Epoch check: reject if incoming epoch is lower than replica's epoch
		if msg.Epoch < state.Epoch {
			state.mu.Unlock()
			r.logger.Warn("rejecting replication with stale epoch",
				"topic", msg.Topic,
				"msg_epoch", msg.Epoch,
				"replica_epoch", state.Epoch,
			)
			_ = stream.Send(&pb.ReplicateAck{
				Topic: msg.Topic,
				Seq:   msg.Seq,
				Epoch: state.Epoch,
				Ok:    false,
			})
			continue
		}

		if msg.Epoch > state.Epoch {
			state.Epoch = msg.Epoch
			if t, exists := r.manager.GetTopic(msg.Topic); exists {
				t.SetEpoch(msg.Epoch)
			}
		}

		if !msg.IsOffsetCommit {
			// Gap detection
			if state.LastReplicatedSeq > 0 && msg.Seq > state.LastReplicatedSeq+1 {
				gap := msg.Seq - (state.LastReplicatedSeq + 1)
				state.DetectedGaps += gap
				r.logger.Warn("replication sequence gap detected",
					"topic", msg.Topic,
					"expected", state.LastReplicatedSeq+1,
					"received", msg.Seq,
					"gap_size", gap,
				)
			}
			state.LastReplicatedSeq = msg.Seq
		}
		state.mu.Unlock()

		if msg.IsOffsetCommit {
			r.manager.SetGroupCommittedOffset(msg.Topic, msg.Group, msg.CommittedOffset)
		} else {
			r.manager.AppendReplicatedMessage(&broker.Message{
				Topic:       msg.Topic,
				Seq:         msg.Seq,
				Payload:     msg.Payload,
				TimestampNS: msg.TimestampNs,
			})
		}

		// Acknowledge receipt
		if err := stream.Send(&pb.ReplicateAck{
			Topic: msg.Topic,
			Seq:   msg.Seq,
			Epoch: msg.Epoch,
			Ok:    true,
		}); err != nil {
			return err
		}
	}
}

// SyncTopic streams messages from the owner's ring buffer starting from req.FromSeq.
func (r *Replicator) SyncTopic(req *pb.SyncRequest, stream pb.Internal_SyncTopicServer) error {
	topicName := req.GetTopic()
	fromSeq := req.GetFromSeq()

	t, exists := r.manager.GetTopic(topicName)
	if !exists {
		return status.Errorf(codes.NotFound, "topic %q not found", topicName)
	}

	state := r.GetOrCreateTopicState(topicName)
	state.mu.RLock()
	currentEpoch := state.Epoch
	state.mu.RUnlock()

	rawMsgs, ok := t.RingBuffer().GetFrom(fromSeq)
	if !ok {
		// If fromSeq was evicted, start from oldest available
		oldest := t.RingBuffer().OldestSeq()
		rawMsgs, _ = t.RingBuffer().GetFrom(oldest)
	}

	for _, m := range rawMsgs {
		if err := stream.Send(&pb.ReplicateMsg{
			Topic:       m.Topic,
			Epoch:       currentEpoch,
			Seq:         m.Seq,
			Payload:     m.Payload,
			TimestampNs: m.TimestampNS,
		}); err != nil {
			return err
		}
	}

	return nil
}

// CatchUpReplica connects to the owner of topic and streams historical messages via SyncTopic.
func (r *Replicator) CatchUpReplica(ctx context.Context, topic, ownerID string, fromSeq uint64) error {
	conn, err := r.getPeerConn(ownerID)
	if err != nil {
		return fmt.Errorf("dial owner %s: %w", ownerID, err)
	}

	client := pb.NewInternalClient(conn)

	state := r.GetOrCreateTopicState(topic)
	state.mu.RLock()
	epoch := state.Epoch
	state.mu.RUnlock()

	syncStream, err := client.SyncTopic(ctx, &pb.SyncRequest{
		Topic:   topic,
		FromSeq: fromSeq,
		Epoch:   epoch,
	})
	if err != nil {
		return fmt.Errorf("SyncTopic error: %w", err)
	}

	for {
		msg, err := syncStream.Recv()
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}

		state.mu.Lock()
		if msg.Epoch > state.Epoch {
			state.Epoch = msg.Epoch
		}
		state.LastReplicatedSeq = msg.Seq
		state.mu.Unlock()

		r.manager.AppendReplicatedMessage(&broker.Message{
			Topic:       msg.Topic,
			Seq:         msg.Seq,
			Payload:     msg.Payload,
			TimestampNS: msg.TimestampNs,
		})
	}

	return nil
}

func (r *Replicator) getOrCreateStream(replicaID string) (pb.Internal_ReplicateClient, error) {
	r.streamsMu.Lock()
	defer r.streamsMu.Unlock()

	if s, ok := r.streams[replicaID]; ok {
		return s, nil
	}

	conn, err := r.getPeerConn(replicaID)
	if err != nil {
		return nil, err
	}

	client := pb.NewInternalClient(conn)
	stream, err := client.Replicate(r.ctx)
	if err != nil {
		return nil, fmt.Errorf("open Replicate stream to %s: %w", replicaID, err)
	}

	r.streams[replicaID] = stream

	// Start background reader for acks
	r.wg.Add(1)
	go r.ackReader(replicaID, stream)

	return stream, nil
}

func (r *Replicator) invalidateStream(replicaID string) {
	r.streamsMu.Lock()
	delete(r.streams, replicaID)
	r.streamsMu.Unlock()
}

func (r *Replicator) ackReader(replicaID string, stream pb.Internal_ReplicateClient) {
	defer r.wg.Done()
	for {
		ack, err := stream.Recv()
		if err != nil {
			r.invalidateStream(replicaID)
			return
		}

		r.waitersMu.Lock()
		if m, ok := r.syncWaiters[ack.Topic]; ok {
			if ch, found := m[ack.Seq]; found {
				if ack.Ok {
					ch <- nil
				} else {
					ch <- status.Errorf(codes.FailedPrecondition, "replica rejected replication (stale epoch)")
				}
			}
		}
		r.waitersMu.Unlock()
	}
}

func (r *Replicator) getPeerConn(peerID string) (*grpc.ClientConn, error) {
	r.peersMu.RLock()
	if conn, ok := r.peers[peerID]; ok {
		r.peersMu.RUnlock()
		return conn, nil
	}
	r.peersMu.RUnlock()

	r.peersMu.Lock()
	defer r.peersMu.Unlock()

	if conn, ok := r.peers[peerID]; ok {
		return conn, nil
	}

	addr := ""
	for _, p := range r.cfg.Peers {
		if p.ID == peerID {
			addr = p.Address
			break
		}
	}
	if addr == "" {
		addr = peerID
	}

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	if r.dialer != nil {
		dialOpts = append(dialOpts, grpc.WithContextDialer(r.dialer))
	}

	target := addr
	if r.dialer != nil {
		target = "passthrough:///" + addr
	}

	conn, err := grpc.NewClient(target, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("dial peer %s (%s): %w", peerID, addr, err)
	}

	r.peers[peerID] = conn
	return conn, nil
}
