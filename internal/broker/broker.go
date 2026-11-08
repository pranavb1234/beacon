// Package broker is the core engine for a single beacon node.
//
// In Phase 0 this is a skeleton that compiles and passes tests.
// Real fan-out, ring-buffer storage, ACK tracking, and backpressure are
// implemented incrementally in Phases 1–7.
//
// Lock ordering (must be respected to prevent deadlock):
//   Broker.mu → topic.mu
//
// Never acquire topic.mu while holding Broker.mu for writing, and never
// acquire Broker.mu while already holding topic.mu.
package broker

import (
	"context"
	"sync"

	"github.com/pranavb1234/beacon/internal/config"
	"github.com/pranavb1234/beacon/internal/metrics"
)

// Broker is the top-level coordinator on one node.
// One Broker instance runs per process.
//
// mu protects topics.
type Broker struct {
	mu     sync.RWMutex
	topics map[string]*topic

	cfg *config.Config
	met *metrics.Metrics
}

// topic holds state for one named topic local to this node.
//
// mu protects subscribers and nextSeq.
// Ring buffer and unacknowledged-message map are added in Phase 5.
type topic struct {
	name string

	mu          sync.RWMutex
	subscribers []*subscriber
	nextSeq     uint64 // monotonic counter; protected by mu
}

// subscriber represents one active streaming connection.
//
// send is a bounded channel; its capacity is set by config.BufferSize and
// overflow behaviour is dictated by config.BackpressurePolicy.
// cancelFn tears down the stream when called (called on disconnect or policy
// enforcement).
type subscriber struct {
	id       string
	groupID  string         // empty → fan-out to all; non-empty → consumer group
	send     chan []byte
	cancelFn context.CancelFunc
}

// New creates a Broker wired to cfg and met.
func New(cfg *config.Config, met *metrics.Metrics) *Broker {
	return &Broker{
		topics: make(map[string]*topic),
		cfg:    cfg,
		met:    met,
	}
}

// getOrCreateTopic returns the *topic for name, creating it if absent.
//
// Lock ordering: acquires Broker.mu (write) only; callers must not already
// hold topic.mu when calling this.
func (b *Broker) getOrCreateTopic(name string) *topic {
	// Fast path — already exists.
	b.mu.RLock()
	if t, ok := b.topics[name]; ok {
		b.mu.RUnlock()
		return t
	}
	b.mu.RUnlock()

	// Slow path — create.
	b.mu.Lock()
	defer b.mu.Unlock()
	// Double-check after upgrading the lock.
	if t, ok := b.topics[name]; ok {
		return t
	}
	t := &topic{name: name}
	b.topics[name] = t
	return t
}

// Publish is a placeholder; full implementation arrives in Phase 1.
// It stores the message in the topic's ring buffer, fans out to all matching
// subscribers, and returns a monotonic sequence number.
func (b *Broker) Publish(_ context.Context, topicName string, payload []byte) (uint64, error) {
	// TODO(phase1): ring buffer write, fan-out, seq increment, ACK timer start.
	_ = b.getOrCreateTopic(topicName)
	_ = payload
	return 0, nil
}

// Subscribe is a placeholder; full implementation arrives in Phase 1.
// It registers a subscriber, optionally replaying buffered messages from
// startSeq, and returns a channel over which events are delivered.
func (b *Broker) Subscribe(
	_ context.Context,
	topicName, groupID, subscriberID string,
) (<-chan []byte, context.CancelFunc, error) {
	// TODO(phase1): add to topic's subscriber list, honour backpressure policy,
	//               replay from ring buffer if startSeq > 0.
	ch := make(chan []byte, 64)
	return ch, func() { close(ch) }, nil
}
