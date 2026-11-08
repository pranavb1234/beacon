// Package broker implements the core topic management, message sequencing,
// fan-out, and configurable backpressure engine for a single broker node.
//
// Thread Safety & Lock Hierarchy:
// To prevent deadlocks, locks must strictly be acquired in the following order:
//
//	Manager.mu (RWMutex) -> Topic.mu (RWMutex) -> Subscriber.mu (Mutex)
//
// Lock invariants:
//  1. Manager.mu protects the `topics` registry map.
//  2. Topic.mu protects the `subscribers` map and subscriber membership.
//  3. Subscriber.mu protects the subscriber's `closed` state to guarantee that
//     message delivery never sends on a closed channel, and that channel closure
//     happens exactly once.
//  4. Sequence numbers are assigned atomically via atomic.AddUint64, ensuring
//     lock-free monotonic sequence ordering even under heavy concurrent publishing.
//  5. Never acquire Manager.mu while holding Topic.mu.
//  6. Never acquire Topic.mu while holding Subscriber.mu.
package broker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pranavb1234/beacon/internal/config"
	"github.com/pranavb1234/beacon/internal/metrics"
)

var (
	// ErrTopicNotFound is returned when attempting to access a non-existent topic.
	ErrTopicNotFound = errors.New("broker: topic not found")
	// ErrSubscriberNotFound is returned when attempting to remove an unregistered subscriber.
	ErrSubscriberNotFound = errors.New("broker: subscriber not found")
	// ErrNilSubscriber is returned when adding a nil subscriber.
	ErrNilSubscriber = errors.New("broker: subscriber cannot be nil")
	// ErrBufferFull is set when a subscriber is disconnected due to a full buffer under PolicyDisconnect.
	ErrBufferFull = errors.New("broker: subscriber buffer full; disconnected")
)

// Message represents an event published to a topic.
type Message struct {
	Topic       string `json:"topic"`
	Seq         uint64 `json:"seq"`
	Payload     []byte `json:"payload"`
	TimestampNS int64  `json:"timestamp_ns"`
}

// DeliveryResult describes the outcome of an attempted message delivery to a subscriber.
type DeliveryResult int

const (
	// DeliverySuccess indicates the message was enqueued into the subscriber buffer.
	DeliverySuccess DeliveryResult = iota
	// DeliveryDropped indicates a message was dropped (either evicted or rejected due to timeout).
	DeliveryDropped
	// DeliveryDisconnected indicates the subscriber was disconnected due to buffer exhaustion.
	DeliveryDisconnected
)

// BackpressurePolicyHandler defines the interface for handling full subscriber channels.
type BackpressurePolicyHandler interface {
	Deliver(sub *Subscriber, msg *Message, timeout time.Duration) DeliveryResult
}

// BackpressureFunc is a functional adapter that implements BackpressurePolicyHandler.
type BackpressureFunc func(sub *Subscriber, msg *Message, timeout time.Duration) DeliveryResult

// Deliver implements BackpressurePolicyHandler.
func (f BackpressureFunc) Deliver(sub *Subscriber, msg *Message, timeout time.Duration) DeliveryResult {
	return f(sub, msg, timeout)
}

// BlockDelivery waits with a timeout until the subscriber channel has room.
func BlockDelivery(sub *Subscriber, msg *Message, timeout time.Duration) DeliveryResult {
	// Fast path: try immediate send
	select {
	case sub.Send <- msg:
		return DeliverySuccess
	default:
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case <-sub.Ctx.Done():
		return DeliveryDisconnected
	case sub.Send <- msg:
		return DeliverySuccess
	case <-timer.C:
		// Publisher timed out waiting for subscriber to drain buffer
		return DeliveryDropped
	}
}

// DropOldestDelivery evicts the oldest queued message in the subscriber buffer and enqueues the new one.
func DropOldestDelivery(sub *Subscriber, msg *Message, _ time.Duration) DeliveryResult {
	// Fast path: try immediate send
	select {
	case sub.Send <- msg:
		return DeliverySuccess
	default:
	}

	// Buffer is full: drain oldest message if possible
	select {
	case <-sub.Send:
	default:
	}

	// Enqueue the new message
	select {
	case sub.Send <- msg:
		return DeliveryDropped // older message dropped to enqueue new one
	default:
		// Fallback if another push raced
		return DeliveryDropped
	}
}

// DisconnectDelivery forcibly closes the subscriber if its channel is full.
func DisconnectDelivery(sub *Subscriber, msg *Message, _ time.Duration) DeliveryResult {
	select {
	case sub.Send <- msg:
		return DeliverySuccess
	default:
		sub.setErr(ErrBufferFull)
		return DeliveryDisconnected
	}
}

// PolicyHandler maps a config policy to its corresponding delivery handler.
func PolicyHandler(policy config.BackpressurePolicy) BackpressurePolicyHandler {
	switch config.NormalizePolicy(string(policy)) {
	case config.PolicyDropOldest:
		return BackpressureFunc(DropOldestDelivery)
	case config.PolicyDisconnect:
		return BackpressureFunc(DisconnectDelivery)
	case config.PolicyBlock:
		fallthrough
	default:
		return BackpressureFunc(BlockDelivery)
	}
}

// Stats holds operational telemetry counters for the broker.
type Stats struct {
	Published    uint64 `json:"published"`
	Delivered    uint64 `json:"delivered"`
	Dropped      uint64 `json:"dropped"`
	Disconnected uint64 `json:"disconnected"`
}

// Subscriber represents an active subscriber to a topic.
type Subscriber struct {
	ID     string
	Send   chan *Message
	Done   chan struct{}
	Ctx    context.Context
	Cancel context.CancelFunc

	err   error
	errMu sync.RWMutex

	mu     sync.Mutex
	closed bool
	once   sync.Once
}

// NewSubscriber creates a new Subscriber with a bounded message channel.
func NewSubscriber(id string, bufferSize int) *Subscriber {
	if bufferSize <= 0 {
		bufferSize = 1000
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Subscriber{
		ID:     id,
		Send:   make(chan *Message, bufferSize),
		Done:   make(chan struct{}),
		Ctx:    ctx,
		Cancel: cancel,
	}
}

// Err returns the error that caused subscriber termination, if any.
func (s *Subscriber) Err() error {
	s.errMu.RLock()
	defer s.errMu.RUnlock()
	return s.err
}

func (s *Subscriber) setErr(err error) {
	s.errMu.Lock()
	if s.err == nil {
		s.err = err
	}
	s.errMu.Unlock()
}

// Deliver attempts to deliver a message using the given backpressure handler.
// It acquires Subscriber.mu to guarantee that a send is never attempted on a closed channel.
func (s *Subscriber) Deliver(msg *Message, handler BackpressurePolicyHandler, timeout time.Duration) DeliveryResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return DeliveryDisconnected
	}

	result := handler.Deliver(s, msg, timeout)
	if result == DeliveryDisconnected {
		// Cleanly close subscriber channel immediately on disconnect
		if s.Cancel != nil {
			s.Cancel()
		}
		s.closed = true
		close(s.Done)
		close(s.Send)
	}
	return result
}

// Close closes the subscriber's channels exactly once and cancels its context.
// Safe to call multiple times and from multiple concurrent goroutines.
func (s *Subscriber) Close() {
	s.once.Do(func() {
		if s.Cancel != nil {
			s.Cancel()
		}
		s.mu.Lock()
		if !s.closed {
			s.closed = true
			close(s.Done)
			close(s.Send)
		}
		s.mu.Unlock()
	})
}

// IsClosed returns whether the subscriber has been closed.
func (s *Subscriber) IsClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

// Topic holds state for a single topic, including its subscribers and next sequence counter.
type Topic struct {
	Name        string
	nextSeq     uint64
	subscribers map[string]*Subscriber
	mu          sync.RWMutex
}

// NextSeq returns the current sequence counter value.
func (t *Topic) NextSeq() uint64 {
	return atomic.LoadUint64(&t.nextSeq)
}

// SubscribersCount returns the number of active subscribers on this topic.
func (t *Topic) SubscribersCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.subscribers)
}

// Manager coordinates topics, publishers, subscribers, and backpressure on a broker node.
type Manager struct {
	mu     sync.RWMutex
	topics map[string]*Topic
	cfg    *config.Config
	met    *metrics.Metrics

	// Telemetry counters
	publishedCount    uint64
	deliveredCount    uint64
	droppedCount      uint64
	disconnectedCount uint64
}

// Broker is an alias for Manager for compatibility.
type Broker = Manager

// NewManager creates a Manager with the specified configuration.
func NewManager(cfg *config.Config, met *metrics.Metrics) *Manager {
	if cfg == nil {
		cfg = config.Default()
	}
	return &Manager{
		topics: make(map[string]*Topic),
		cfg:    cfg,
		met:    met,
	}
}

// New creates a broker Coordinator/Manager (compatible with previous signature).
func New(cfg *config.Config, met *metrics.Metrics) *Broker {
	return NewManager(cfg, met)
}

// Stats returns a snapshot of current broker counters.
func (m *Manager) Stats() Stats {
	return Stats{
		Published:    atomic.LoadUint64(&m.publishedCount),
		Delivered:    atomic.LoadUint64(&m.deliveredCount),
		Dropped:      atomic.LoadUint64(&m.droppedCount),
		Disconnected: atomic.LoadUint64(&m.disconnectedCount),
	}
}

// GetOrCreateTopic returns the *Topic for the given name, creating it if it does not yet exist.
func (m *Manager) GetOrCreateTopic(name string) *Topic {
	m.mu.RLock()
	if t, exists := m.topics[name]; exists {
		m.mu.RUnlock()
		return t
	}
	m.mu.RUnlock()

	m.mu.Lock()
	defer m.mu.Unlock()

	if t, exists := m.topics[name]; exists {
		return t
	}

	t := &Topic{
		Name:        name,
		subscribers: make(map[string]*Subscriber),
	}
	m.topics[name] = t
	return t
}

// getOrCreateTopic provides private compatibility.
func (m *Manager) getOrCreateTopic(name string) *Topic {
	return m.GetOrCreateTopic(name)
}

// AddSubscriber registers a subscriber to a topic.
func (m *Manager) AddSubscriber(topicName string, sub *Subscriber) error {
	if sub == nil {
		return ErrNilSubscriber
	}

	t := m.GetOrCreateTopic(topicName)

	t.mu.Lock()
	defer t.mu.Unlock()

	t.subscribers[sub.ID] = sub
	return nil
}

// RemoveSubscriber unregisters a subscriber from a topic and safely closes its channels.
// Removal closes the subscriber channel exactly once and prevents any further sends.
func (m *Manager) RemoveSubscriber(topicName string, subID string) error {
	m.mu.RLock()
	t, exists := m.topics[topicName]
	m.mu.RUnlock()

	if !exists {
		return ErrTopicNotFound
	}

	t.mu.Lock()
	sub, exists := t.subscribers[subID]
	if exists {
		delete(t.subscribers, subID)
	}
	t.mu.Unlock()

	if !exists {
		return ErrSubscriberNotFound
	}

	sub.Close()
	return nil
}

// Publish assigns the next monotonic sequence number and fans out the message to all active subscribers.
func (m *Manager) Publish(topicName string, payload []byte) (uint64, error) {
	t := m.GetOrCreateTopic(topicName)

	seq := atomic.AddUint64(&t.nextSeq, 1)
	atomic.AddUint64(&m.publishedCount, 1)

	msg := &Message{
		Topic:       topicName,
		Seq:         seq,
		Payload:     payload,
		TimestampNS: time.Now().UnixNano(),
	}

	// Snapshot subscribers under read lock
	t.mu.RLock()
	subs := make([]*Subscriber, 0, len(t.subscribers))
	for _, s := range t.subscribers {
		subs = append(subs, s)
	}
	t.mu.RUnlock()

	handler := PolicyHandler(m.cfg.BackpressurePolicy)
	timeout := m.cfg.BlockTimeout
	if timeout <= 0 {
		timeout = 500 * time.Millisecond
	}

	var disconnectedIDs []string
	for _, s := range subs {
		result := s.Deliver(msg, handler, timeout)
		switch result {
		case DeliverySuccess:
			atomic.AddUint64(&m.deliveredCount, 1)
		case DeliveryDropped:
			atomic.AddUint64(&m.droppedCount, 1)
		case DeliveryDisconnected:
			atomic.AddUint64(&m.disconnectedCount, 1)
			disconnectedIDs = append(disconnectedIDs, s.ID)
		}
	}

	// Remove any disconnected subscribers from topic
	if len(disconnectedIDs) > 0 {
		t.mu.Lock()
		for _, id := range disconnectedIDs {
			delete(t.subscribers, id)
		}
		t.mu.Unlock()
	}

	return seq, nil
}

// Subscribe is a convenience method for registering a subscriber with a given ID.
func (m *Manager) Subscribe(ctx context.Context, topicName, groupID, subscriberID string) (<-chan *Message, context.CancelFunc, error) {
	bufSize := m.cfg.SubscriberBufferSize
	if bufSize <= 0 {
		bufSize = 1000
	}

	sub := NewSubscriber(subscriberID, bufSize)
	if ctx != nil {
		go func() {
			select {
			case <-ctx.Done():
				_ = m.RemoveSubscriber(topicName, subscriberID)
			case <-sub.Done:
			}
		}()
	}

	if err := m.AddSubscriber(topicName, sub); err != nil {
		sub.Close()
		return nil, nil, err
	}

	cancel := func() {
		_ = m.RemoveSubscriber(topicName, subscriberID)
	}

	return sub.Send, cancel, nil
}
