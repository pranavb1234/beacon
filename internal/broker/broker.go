// Package broker implements the core topic management, message sequencing,
// ring-buffer storage, fan-out, configurable backpressure, and in-flight ACK tracking
// for a single broker node.
//
// Thread Safety & Lock Hierarchy:
// To prevent deadlocks, locks must strictly be acquired in the following order:
//
//	Manager.mu (RWMutex) -> Topic.mu (RWMutex) -> Subscriber.mu (Mutex) -> Subscriber.inFlightMu (Mutex)
//
// Lock invariants:
//  1. Manager.mu protects the `topics` registry map.
//  2. Topic.mu protects the `subscribers` map and subscriber membership.
//  3. Subscriber.mu protects the subscriber's `closed` state to guarantee that
//     message delivery never sends on a closed channel, and that channel closure
//     happens exactly once.
//  4. Subscriber.inFlightMu protects the in-flight tracking map and retry state.
//  5. Sequence numbers are assigned atomically via atomic.AddUint64.
//  6. Never acquire Manager.mu while holding Topic.mu.
//  7. Never acquire Topic.mu while holding Subscriber.mu.
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
		return DeliveryDropped
	default:
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

// InFlightMessage tracks an unacknowledged message awaiting an ACK.
type InFlightMessage struct {
	Message  *Message
	Deadline time.Time
	Retries  int
}

// Stats holds operational telemetry counters for the broker.
type Stats struct {
	Published    uint64 `json:"published"`
	Delivered    uint64 `json:"delivered"`
	Dropped      uint64 `json:"dropped"`
	Disconnected uint64 `json:"disconnected"`
	DeadLetter   uint64 `json:"dead_letter"`
	Retried      uint64 `json:"retried"`
	Acked        uint64 `json:"acked"`
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

	inFlight   map[uint64]*InFlightMessage
	inFlightMu sync.Mutex

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
		ID:       id,
		Send:     make(chan *Message, bufferSize),
		Done:     make(chan struct{}),
		Ctx:      ctx,
		Cancel:   cancel,
		inFlight: make(map[uint64]*InFlightMessage),
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

// RecordInFlight registers a message in the in-flight tracking map.
func (s *Subscriber) RecordInFlight(msg *Message, timeout time.Duration) {
	s.inFlightMu.Lock()
	defer s.inFlightMu.Unlock()
	s.inFlight[msg.Seq] = &InFlightMessage{
		Message:  msg,
		Deadline: time.Now().Add(timeout),
		Retries:  0,
	}
}

// Acknowledge removes a message sequence from in-flight tracking.
func (s *Subscriber) Acknowledge(seq uint64) bool {
	s.inFlightMu.Lock()
	defer s.inFlightMu.Unlock()
	if _, exists := s.inFlight[seq]; exists {
		delete(s.inFlight, seq)
		return true
	}
	return false
}

// InFlightCount returns the number of currently unacknowledged messages.
func (s *Subscriber) InFlightCount() int {
	s.inFlightMu.Lock()
	defer s.inFlightMu.Unlock()
	return len(s.inFlight)
}

// Deliver attempts to deliver a message using the given backpressure handler and records in-flight state.
func (s *Subscriber) Deliver(
	msg *Message,
	handler BackpressurePolicyHandler,
	timeout time.Duration,
	ackTimeout time.Duration,
) DeliveryResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return DeliveryDisconnected
	}

	result := handler.Deliver(s, msg, timeout)
	if result == DeliveryDisconnected {
		if s.Cancel != nil {
			s.Cancel()
		}
		s.closed = true
		close(s.Done)
		close(s.Send)
	} else if result == DeliverySuccess && ackTimeout > 0 {
		s.RecordInFlight(msg, ackTimeout)
	}
	return result
}

// DeliverRedelivery attempts redelivery of an in-flight message without creating duplicate tracking entries.
func (s *Subscriber) DeliverRedelivery(
	msg *Message,
	handler BackpressurePolicyHandler,
	timeout time.Duration,
) DeliveryResult {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return DeliveryDisconnected
	}

	result := handler.Deliver(s, msg, timeout)
	if result == DeliveryDisconnected {
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

// Topic holds state for a single topic, including its ring buffer, subscribers, and next sequence counter.
type Topic struct {
	Name        string
	nextSeq     uint64
	subscribers map[string]*Subscriber
	ringBuffer  *RingBuffer
	mu          sync.RWMutex
}

// RingBuffer returns the ring buffer storing recent messages for this topic.
func (t *Topic) RingBuffer() *RingBuffer {
	return t.ringBuffer
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

// Manager coordinates topics, publishers, subscribers, backpressure, and ACK tracking.
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
	deadLetterCount   uint64
	retriedCount      uint64
	ackedCount        uint64
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
		DeadLetter:   atomic.LoadUint64(&m.deadLetterCount),
		Retried:      atomic.LoadUint64(&m.retriedCount),
		Acked:        atomic.LoadUint64(&m.ackedCount),
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

	bufCap := m.cfg.BufferSizePerTopic
	if bufCap <= 0 {
		bufCap = 10000
	}

	t := &Topic{
		Name:        name,
		subscribers: make(map[string]*Subscriber),
		ringBuffer:  NewRingBuffer(bufCap),
	}
	m.topics[name] = t
	return t
}

// getOrCreateTopic provides private compatibility.
func (m *Manager) getOrCreateTopic(name string) *Topic {
	return m.GetOrCreateTopic(name)
}

// AddSubscriber registers a subscriber to a topic and starts its background retry scanner.
func (m *Manager) AddSubscriber(topicName string, sub *Subscriber) error {
	if sub == nil {
		return ErrNilSubscriber
	}

	t := m.GetOrCreateTopic(topicName)

	t.mu.Lock()
	t.subscribers[sub.ID] = sub
	t.mu.Unlock()

	m.startRetryScanner(sub)
	return nil
}

// RemoveSubscriber unregisters a subscriber from a topic and safely closes its channels.
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

// Acknowledge records an ACK from a subscriber and removes the message from in-flight tracking.
func (m *Manager) Acknowledge(topicName, subID string, seq uint64) bool {
	m.mu.RLock()
	t, exists := m.topics[topicName]
	m.mu.RUnlock()
	if !exists {
		return false
	}

	t.mu.RLock()
	sub, exists := t.subscribers[subID]
	t.mu.RUnlock()
	if !exists {
		return false
	}

	ok := sub.Acknowledge(seq)
	if ok {
		atomic.AddUint64(&m.ackedCount, 1)
	}
	return ok
}

// Publish assigns the next sequence number, stores it in the ring buffer, and fans out to subscribers.
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

	// Append to topic ring buffer
	t.ringBuffer.Append(msg)

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
	ackTimeout := m.cfg.AckTimeout
	if ackTimeout <= 0 {
		ackTimeout = 1 * time.Second
	}

	var disconnectedIDs []string
	for _, s := range subs {
		result := s.Deliver(msg, handler, timeout, ackTimeout)
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

	// Prune disconnected subscribers from topic
	if len(disconnectedIDs) > 0 {
		t.mu.Lock()
		for _, id := range disconnectedIDs {
			delete(t.subscribers, id)
		}
		t.mu.Unlock()
	}

	return seq, nil
}

// startRetryScanner launches a background scanner checking for expired in-flight ACK deadlines.
func (m *Manager) startRetryScanner(sub *Subscriber) {
	go func() {
		baseTimeout := m.cfg.AckTimeout
		if baseTimeout <= 0 {
			baseTimeout = 1 * time.Second
		}
		maxRetries := m.cfg.MaxRetries
		if maxRetries < 0 {
			maxRetries = 3
		}
		blockTimeout := m.cfg.BlockTimeout
		if blockTimeout <= 0 {
			blockTimeout = 500 * time.Millisecond
		}
		handler := PolicyHandler(m.cfg.BackpressurePolicy)

		tickInterval := baseTimeout / 4
		if tickInterval < 10*time.Millisecond {
			tickInterval = 10 * time.Millisecond
		} else if tickInterval > 200*time.Millisecond {
			tickInterval = 200 * time.Millisecond
		}

		ticker := time.NewTicker(tickInterval)
		defer ticker.Stop()

		for {
			select {
			case <-sub.Ctx.Done():
				return
			case <-sub.Done:
				return
			case <-ticker.C:
				m.scanInFlightRetries(sub, baseTimeout, maxRetries, blockTimeout, handler)
			}
		}
	}()
}

// scanInFlightRetries scans subscriber's in-flight messages and redelivers with exponential backoff.
func (m *Manager) scanInFlightRetries(
	sub *Subscriber,
	baseTimeout time.Duration,
	maxRetries int,
	blockTimeout time.Duration,
	handler BackpressurePolicyHandler,
) {
	now := time.Now()

	sub.inFlightMu.Lock()
	var toRedeliver []*InFlightMessage
	var toDeadLetter []*Message

	for seq, item := range sub.inFlight {
		if now.After(item.Deadline) {
			if item.Retries >= maxRetries {
				delete(sub.inFlight, seq)
				toDeadLetter = append(toDeadLetter, item.Message)
			} else {
				item.Retries++
				backoff := baseTimeout * (1 << item.Retries)
				item.Deadline = now.Add(backoff)
				toRedeliver = append(toRedeliver, item)
			}
		}
	}
	sub.inFlightMu.Unlock()

	for range toDeadLetter {
		atomic.AddUint64(&m.deadLetterCount, 1)
	}

	for _, item := range toRedeliver {
		atomic.AddUint64(&m.retriedCount, 1)
		sub.DeliverRedelivery(item.Message, handler, blockTimeout)
	}
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
