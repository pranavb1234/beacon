// Package broker implements the core topic management, message sequencing,
// and fan-out engine for a single broker node.
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
)

// Message represents an event published to a topic.
type Message struct {
	Topic       string `json:"topic"`
	Seq         uint64 `json:"seq"`
	Payload     []byte `json:"payload"`
	TimestampNS int64  `json:"timestamp_ns"`
}

// Subscriber represents an active subscriber to a topic.
type Subscriber struct {
	ID     string
	Send   chan *Message
	Done   chan struct{}
	Ctx    context.Context
	Cancel context.CancelFunc

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

// Deliver attempts to send a message to the subscriber using the configured backpressure policy.
// It acquires Subscriber.mu to guarantee that a send is never attempted on a closed channel.
func (s *Subscriber) Deliver(msg *Message, policy config.BackpressurePolicy) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return false
	}

	switch policy {
	case config.PolicyBlock:
		select {
		case <-s.Ctx.Done():
			return false
		case s.Send <- msg:
			return true
		}

	case config.PolicyDropOldest:
		select {
		case s.Send <- msg:
			return true
		default:
			// Buffer is full: evict oldest message to make room
			select {
			case <-s.Send:
			default:
			}
			// Attempt to insert new message
			select {
			case s.Send <- msg:
				return true
			default:
				return false
			}
		}

	case config.PolicyDisconnect:
		select {
		case s.Send <- msg:
			return true
		default:
			// Slow consumer cannot keep up; signal disconnect
			s.Cancel()
			return false
		}

	default:
		select {
		case s.Send <- msg:
			return true
		default:
			return false
		}
	}
}

// Close closes the subscriber's channels exactly once and cancels its context.
// Safe to call multiple times and from multiple concurrent goroutines.
func (s *Subscriber) Close() {
	s.once.Do(func() {
		// Cancel context first to abort any blocked operations
		if s.Cancel != nil {
			s.Cancel()
		}
		s.mu.Lock()
		s.closed = true
		close(s.Done)
		close(s.Send)
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

// Manager coordinates topics, publishers, and subscribers on a broker node.
type Manager struct {
	mu     sync.RWMutex
	topics map[string]*Topic
	cfg    *config.Config
	met    *metrics.Metrics
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

// GetOrCreateTopic returns the *Topic for the given name, creating it if it does not yet exist.
func (m *Manager) GetOrCreateTopic(name string) *Topic {
	// Fast path: acquire read lock
	m.mu.RLock()
	if t, exists := m.topics[name]; exists {
		m.mu.RUnlock()
		return t
	}
	m.mu.RUnlock()

	// Slow path: acquire write lock to insert topic
	m.mu.Lock()
	defer m.mu.Unlock()

	// Re-check under write lock
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

	// Close outside of topic lock to minimize lock contention
	sub.Close()
	return nil
}

// Publish assigns the next monotonic sequence number and fans out the message to all active subscribers.
func (m *Manager) Publish(topicName string, payload []byte) (uint64, error) {
	t := m.GetOrCreateTopic(topicName)

	// Atomically increment and assign the sequence number
	seq := atomic.AddUint64(&t.nextSeq, 1)

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

	// Deliver to all subscribers according to backpressure policy
	policy := m.cfg.BackpressurePolicy
	for _, s := range subs {
		s.Deliver(msg, policy)
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
