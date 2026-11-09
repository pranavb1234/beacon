package replication

import (
	"context"
	"fmt"
	"sync/atomic"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/pranavb1234/beacon/internal/pb"
)

// PromoteTopic promotes this node from follower/replica to primary owner for topic.
// It increments the topic's epoch, sets nextSeq to lastReplicatedSeq + 1, logs the
// transition, and initiates catch-up for the newly assigned replica.
func (r *Replicator) PromoteTopic(ctx context.Context, topic string) (uint64, error) {
	state := r.GetOrCreateTopicState(topic)

	state.mu.Lock()
	newEpoch := state.Epoch + 1
	state.Epoch = newEpoch
	state.Role = RoleOwner
	lastSeq := state.LastReplicatedSeq
	nextSeq := lastSeq + 1
	state.mu.Unlock()

	// Update topic state in broker manager
	t := r.manager.GetOrCreateTopic(topic)
	t.SetEpoch(newEpoch)
	t.SetNextSeq(lastSeq)

	r.logger.Info("topic state transition: follower -> owner",
		"topic", topic,
		"epoch", newEpoch,
		"last_replicated_seq", lastSeq,
		"next_seq", nextSeq,
		"node_id", r.nodeID,
	)

	// Determine new replica clockwise on ring
	newReplicaID := r.ring.GetReplica(topic)
	if newReplicaID != "" && newReplicaID != r.nodeID {
		state.mu.Lock()
		state.ReplicaID = newReplicaID
		state.mu.Unlock()

		// Stream history to new replica in background
		go func() {
			if err := r.CatchUpReplica(ctx, topic, newReplicaID, 1); err != nil {
				r.logger.Warn("failed initial catch-up with new replica",
					"topic", topic,
					"replica", newReplicaID,
					"err", err,
				)
			}
		}()
	}

	return newEpoch, nil
}

// StepDownTopic transitions an owner to stepped-down / follower upon detecting a higher epoch.
func (r *Replicator) StepDownTopic(topic string, higherEpoch uint64) {
	state := r.GetOrCreateTopicState(topic)

	state.mu.Lock()
	if higherEpoch > state.Epoch {
		state.Epoch = higherEpoch
	}
	prevRole := state.Role
	state.Role = RoleSteppedDown
	state.mu.Unlock()

	if t, exists := r.manager.GetTopic(topic); exists {
		t.SetEpoch(higherEpoch)
	}

	if prevRole == RoleOwner {
		r.logger.Info("topic state transition: owner -> stepped down",
			"topic", topic,
			"epoch", higherEpoch,
			"node_id", r.nodeID,
		)
	}
}

// HandlePeerDown handles failure of a peer node. If this node is the assigned replica
// for topics previously owned by the dead node, it promotes itself to primary owner.
func (r *Replicator) HandlePeerDown(deadNodeID string) {
	r.logger.Info("handling peer down event", "dead_peer", deadNodeID)
	r.ring.RemoveNode(deadNodeID)

	// Check topics registered with this broker
	topics := r.manager.Topics()
	for _, t := range topics {
		topic := t.Name
		state := r.GetOrCreateTopicState(topic)
		state.mu.RLock()
		currentRole := state.Role
		state.mu.RUnlock()

		if currentRole == RoleReplica || r.ring.GetOwner(topic) == r.nodeID {
			_, _ = r.PromoteTopic(r.ctx, topic)
		}
	}
}

// Handoff processes a graceful ownership transfer request from a returning node.
func (r *Replicator) Handoff(ctx context.Context, req *pb.HandoffRequest) (*pb.HandoffResponse, error) {
	topic := req.GetTopic()
	newOwnerID := req.GetNewOwnerId()
	requestedEpoch := req.GetEpoch()

	state := r.GetOrCreateTopicState(topic)

	state.mu.Lock()
	if requestedEpoch <= state.Epoch {
		state.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition,
			"handoff epoch %d must be greater than current epoch %d", requestedEpoch, state.Epoch)
	}

	// Step down current owner
	state.Epoch = requestedEpoch
	state.Role = RoleReplica
	state.ReplicaID = ""
	state.mu.Unlock()

	if t, exists := r.manager.GetTopic(topic); exists {
		t.SetEpoch(requestedEpoch)
	}

	r.logger.Info("topic state transition: owner -> stepped down",
		"topic", topic,
		"epoch", requestedEpoch,
		"transferred_to", newOwnerID,
	)

	atomic.AddUint64(&r.handoffCount, 1)

	return &pb.HandoffResponse{
		Ok:    true,
		Epoch: requestedEpoch,
	}, nil
}

// InitiateHandoff is called by a returning node after it has synchronized all data
// via SyncTopic to gracefully take back topic ownership from the temporary owner.
func (r *Replicator) InitiateHandoff(ctx context.Context, topic, currentOwnerID string) error {
	state := r.GetOrCreateTopicState(topic)

	state.mu.Lock()
	newEpoch := state.Epoch + 1
	state.mu.Unlock()

	conn, err := r.getPeerConn(currentOwnerID)
	if err != nil {
		return fmt.Errorf("dial current owner %s: %w", currentOwnerID, err)
	}

	client := pb.NewInternalClient(conn)
	resp, err := client.Handoff(ctx, &pb.HandoffRequest{
		Topic:      topic,
		NewOwnerId: r.nodeID,
		Epoch:      newEpoch,
	})
	if err != nil {
		return fmt.Errorf("handoff RPC failed: %w", err)
	}

	if !resp.GetOk() {
		return fmt.Errorf("handoff rejected by current owner %s", currentOwnerID)
	}

	// Transition returning node to owner
	state.mu.Lock()
	state.Epoch = newEpoch
	state.Role = RoleOwner
	state.mu.Unlock()

	t := r.manager.GetOrCreateTopic(topic)
	t.SetEpoch(newEpoch)

	r.logger.Info("topic state transition: follower -> owner",
		"topic", topic,
		"epoch", newEpoch,
		"node_id", r.nodeID,
	)

	atomic.AddUint64(&r.handoffCount, 1)
	return nil
}
