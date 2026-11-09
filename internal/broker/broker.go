// Package broker implements the core topic management, message sequencing,
// ring-buffer storage, consumer groups, fan-out, configurable backpressure,
// and in-flight ACK tracking for a single broker node.
//
// Thread Safety & Lock Hierarchy:
// To prevent deadlocks, locks must strictly be acquired in the following order:
//
//	Manager.mu (RWMutex) -> Topic.mu (RWMutex) -> ConsumerGroup.mu (Mutex) -> Subscriber.mu (Mutex) -> Subscriber.inFlightMu (Mutex)
//
// Lock invariants:
//  1. Manager.mu protects the `topics` registry map.
//  2. Topic.mu protects `subscribers`, `broadcastSubs`, and `groups`.
//  3. ConsumerGroup.mu protects group members and committed offset calculation.
//  4. Subscriber.mu protects `closed` state to guarantee that sends are never attempted on a closed channel.
//  5. Subscriber.inFlightMu protects in-flight tracking and retry deadlines.
//  6. Sequence numbers are assigned atomically via atomic.AddUint64.
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
	DeliverySuccess DeliveryResult = iota
	DeliveryDropped
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
	select {
	case sub.Send <- msg:
		return DeliverySuccess
	default:
	}

	select {
	case <-sub.Send:
	default:
	}

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
	ID      string
	GroupID string
	Send    chan *Message
	Done    chan struct{}
	Ctx     context.Context
	Cancel  context.CancelFunc

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

// DrainInFlight extracts and clears all unacknowledged in-flight messages.
func (s *Subscriber) DrainInFlight() []*InFlightMessage {
	s.inFlightMu.Lock()
	defer s.inFlightMu.Unlock()
	items := make([]*InFlightMessage, 0, len(s.inFlight))
	for _, it := range s.inFlight {
		items = append(items, it)
	}
	s.inFlight = make(map[uint64]*InFlightMessage)
	return items
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

// DeliverRedelivery attempts redelivery without creating duplicate in-flight entries.
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

// ConsumerGroup manages a pool of subscribers sharing messages round-robin on a topic.
type ConsumerGroup struct {
	Name            string
	TopicName       string
	members         []*Subscriber
	memberIndex     int
	committedOffset uint64
	ackedSeqs       map[uint64]bool
	mu              sync.Mutex
}

// NewConsumerGroup creates a new ConsumerGroup for a topic.
func NewConsumerGroup(name, topic string) *ConsumerGroup {
	return &ConsumerGroup{
		Name:      name,
		TopicName: topic,
		ackedSeqs: make(map[uint64]bool),
	}
}

// AddMember adds a subscriber to the group.
func (cg *ConsumerGroup) AddMember(sub *Subscriber) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	cg.members = append(cg.members, sub)
}

// RemoveMember removes a subscriber from the group.
func (cg *ConsumerGroup) RemoveMember(subID string) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	for i, m := range cg.members {
		if m.ID == subID {
			cg.members = append(cg.members[:i], cg.members[i+1:]...)
			break
		}
	}
}

// NextMember returns the next alive member in round-robin order.
func (cg *ConsumerGroup) NextMember() *Subscriber {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	if len(cg.members) == 0 {
		return nil
	}
	sub := cg.members[cg.memberIndex%len(cg.members)]
	cg.memberIndex++
	return sub
}

// MemberCount returns the number of active members in the group.
func (cg *ConsumerGroup) MemberCount() int {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	return len(cg.members)
}

// CommittedOffset returns the highest contiguous acknowledged sequence number for this group.
func (cg *ConsumerGroup) CommittedOffset() uint64 {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	return cg.committedOffset
}

// Acknowledge updates the group's highest contiguous acknowledged sequence number.
func (cg *ConsumerGroup) Acknowledge(seq uint64) uint64 {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	if seq <= cg.committedOffset {
		return cg.committedOffset
	}

	cg.ackedSeqs[seq] = true
	for {
		next := cg.committedOffset + 1
		if cg.ackedSeqs[next] {
			delete(cg.ackedSeqs, next)
			cg.committedOffset = next
		} else {
			break
		}
	}
	return cg.committedOffset
}

// SetCommittedOffset overrides the committed offset if higher than current.
func (cg *ConsumerGroup) SetCommittedOffset(offset uint64) {
	cg.mu.Lock()
	defer cg.mu.Unlock()
	if offset > cg.committedOffset {
		cg.committedOffset = offset
	}
}

// IdempotencyKey identifies a client publication attempt.
type idempotencyKey struct {
	clientID  string
	clientSeq uint64
}

// IdempotencyLRU stores recent publication deduplication entries in a bounded LRU.
type IdempotencyLRU struct {
	mu       sync.Mutex
	capacity int
	entries  map[idempotencyKey]uint64
	order    []idempotencyKey
}

// NewIdempotencyLRU creates a bounded LRU cache for deduplicating retried publishes.
func NewIdempotencyLRU(capacity int) *IdempotencyLRU {
	if capacity <= 0 {
		capacity = 1000
	}
	return &IdempotencyLRU{
		capacity: capacity,
		entries:  make(map[idempotencyKey]uint64),
		order:    make([]idempotencyKey, 0, capacity),
	}
}

// Get checks if a client's sequence was already assigned a sequence number.
func (l *IdempotencyLRU) Get(clientID string, clientSeq uint64) (uint64, bool) {
	if clientID == "" || clientSeq == 0 {
		return 0, false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	k := idempotencyKey{clientID: clientID, clientSeq: clientSeq}
	seq, ok := l.entries[k]
	return seq, ok
}

// Put records the assigned sequence for a client's publish sequence.
func (l *IdempotencyLRU) Put(clientID string, clientSeq, assignedSeq uint64) {
	if clientID == "" || clientSeq == 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	k := idempotencyKey{clientID: clientID, clientSeq: clientSeq}
	if _, ok := l.entries[k]; ok {
		return
	}
	if len(l.order) >= l.capacity {
		oldest := l.order[0]
		l.order = l.order[1:]
		delete(l.entries, oldest)
	}
	l.entries[k] = assignedSeq
	l.order = append(l.order, k)
}

// Topic holds state for a single topic, including ring buffer, subscribers, and consumer groups.
type Topic struct {
	Name          string
	nextSeq       uint64
	epoch         uint64
	idempotency   *IdempotencyLRU
	subscribers   map[string]*Subscriber
	broadcastSubs map[string]*Subscriber
	groups        map[string]*ConsumerGroup
	ringBuffer    *RingBuffer
	mu            sync.RWMutex
}

// Epoch returns the current term/epoch of the topic.
func (t *Topic) Epoch() uint64 {
	e := atomic.LoadUint64(&t.epoch)
	if e == 0 {
		return 1
	}
	return e
}

// SetEpoch sets the topic epoch.
func (t *Topic) SetEpoch(epoch uint64) {
	atomic.StoreUint64(&t.epoch, epoch)
}

// IncrementEpoch increments the topic epoch and returns the new value.
func (t *Topic) IncrementEpoch() uint64 {
	return atomic.AddUint64(&t.epoch, 1)
}

// SetNextSeq updates the topic's next sequence counter.
func (t *Topic) SetNextSeq(seq uint64) {
	atomic.StoreUint64(&t.nextSeq, seq)
}

// RingBuffer returns the ring buffer storing recent messages for this topic.
func (t *Topic) RingBuffer() *RingBuffer {
	return t.ringBuffer
}

// NextSeq returns the current sequence counter value.
func (t *Topic) NextSeq() uint64 {
	return atomic.LoadUint64(&t.nextSeq)
}

// SubscribersCount returns the total number of active subscribers on this topic.
func (t *Topic) SubscribersCount() int {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return len(t.subscribers)
}

// AddSubscriberLocked registers a subscriber assuming Topic.mu is already held.
func (t *Topic) AddSubscriberLocked(sub *Subscriber) {
	t.subscribers[sub.ID] = sub
	if sub.GroupID == "" {
		t.broadcastSubs[sub.ID] = sub
	} else {
		cg, exists := t.groups[sub.GroupID]
		if !exists {
			cg = NewConsumerGroup(sub.GroupID, t.Name)
			t.groups[sub.GroupID] = cg
		}
		cg.AddMember(sub)
	}
}

// Manager coordinates topics, publishers, subscribers, consumer groups, and retries.
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
		Name:          name,
		epoch:         1,
		idempotency:   NewIdempotencyLRU(1000),
		subscribers:   make(map[string]*Subscriber),
		broadcastSubs: make(map[string]*Subscriber),
		groups:        make(map[string]*ConsumerGroup),
		ringBuffer:    NewRingBuffer(bufCap),
	}
	m.topics[name] = t
	return t
}

// GetTopic returns the *Topic if it exists, without creating it.
func (m *Manager) GetTopic(name string) (*Topic, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.topics[name]
	return t, ok
}

// Topics returns all topics currently registered with the manager.
func (m *Manager) Topics() []*Topic {
	m.mu.RLock()
	defer m.mu.RUnlock()
	res := make([]*Topic, 0, len(m.topics))
	for _, t := range m.topics {
		res = append(res, t)
	}
	return res
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
	t.AddSubscriberLocked(sub)

	// If group subscriber and group has committedOffset, catch up unconsumed messages from ring buffer
	var catchUpMsgs []*Message
	if sub.GroupID != "" {
		cg := t.groups[sub.GroupID]
		offset := cg.CommittedOffset()
		if t.ringBuffer.Len() > 0 && offset < t.ringBuffer.NewestSeq() {
			if msgs, ok := t.ringBuffer.GetFrom(offset + 1); ok {
				catchUpMsgs = msgs
			}
		}
	}
	t.mu.Unlock()

	// Deliver catch-up messages to the joining member
	if len(catchUpMsgs) > 0 {
		handler := PolicyHandler(m.cfg.BackpressurePolicy)
		timeout := m.cfg.BlockTimeout
		if timeout <= 0 {
			timeout = 500 * time.Millisecond
		}
		ackTimeout := m.cfg.AckTimeout
		if ackTimeout <= 0 {
			ackTimeout = 1 * time.Second
		}
		for _, msg := range catchUpMsgs {
			sub.Deliver(msg, handler, timeout, ackTimeout)
		}
	}

	m.startRetryScanner(sub)
	return nil
}

// RemoveSubscriber unregisters a subscriber and redistributes any unacked messages to remaining group members.
func (m *Manager) RemoveSubscriber(topicName string, subID string) error {
	m.mu.RLock()
	t, exists := m.topics[topicName]
	m.mu.RUnlock()

	if !exists {
		return ErrTopicNotFound
	}

	t.mu.Lock()
	sub, exists := t.subscribers[subID]
	if !exists {
		t.mu.Unlock()
		return ErrSubscriberNotFound
	}

	delete(t.subscribers, subID)
	if sub.GroupID == "" {
		delete(t.broadcastSubs, subID)
	} else if cg, ok := t.groups[sub.GroupID]; ok {
		cg.RemoveMember(subID)
	}
	t.mu.Unlock()

	// Drain unacknowledged in-flight messages from disconnected subscriber
	unacked := sub.DrainInFlight()
	sub.Close()

	// If subscriber was in a group, redistribute its unacked messages to remaining group members
	if sub.GroupID != "" && len(unacked) > 0 {
		t.mu.RLock()
		cg, ok := t.groups[sub.GroupID]
		t.mu.RUnlock()

		if ok && cg.MemberCount() > 0 {
			handler := PolicyHandler(m.cfg.BackpressurePolicy)
			timeout := m.cfg.BlockTimeout
			if timeout <= 0 {
				timeout = 500 * time.Millisecond
			}
			ackTimeout := m.cfg.AckTimeout
			if ackTimeout <= 0 {
				ackTimeout = 1 * time.Second
			}

			for _, item := range unacked {
				target := cg.NextMember()
				if target != nil {
					target.Deliver(item.Message, handler, timeout, ackTimeout)
				}
			}
		}
	}

	return nil
}

// Acknowledge records an ACK and advances group committed offset if part of a consumer group.
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
		if sub.GroupID != "" {
			t.mu.RLock()
			cg, okGroup := t.groups[sub.GroupID]
			t.mu.RUnlock()
			if okGroup {
				cg.Acknowledge(seq)
			}
		}
	}
	return ok
}

// GetGroupCommittedOffset returns the committed offset for a (topic, group).
func (m *Manager) GetGroupCommittedOffset(topicName, group string) uint64 {
	m.mu.RLock()
	t, exists := m.topics[topicName]
	m.mu.RUnlock()
	if !exists {
		return 0
	}

	t.mu.RLock()
	cg, exists := t.groups[group]
	t.mu.RUnlock()
	if !exists {
		return 0
	}

	return cg.CommittedOffset()
}

// SetGroupCommittedOffset sets the committed offset for a consumer group on this broker (used by replica sync).
func (m *Manager) SetGroupCommittedOffset(topicName, group string, offset uint64) {
	t := m.GetOrCreateTopic(topicName)
	t.mu.Lock()
	cg, exists := t.groups[group]
	if !exists {
		cg = NewConsumerGroup(group, topicName)
		t.groups[group] = cg
	}
	t.mu.Unlock()
	cg.SetCommittedOffset(offset)
}

// AppendReplicatedMessage stores a replicated message into the local ring buffer,
// updates the sequence counter, and delivers to local subscribers.
func (m *Manager) AppendReplicatedMessage(msg *Message) {
	t := m.GetOrCreateTopic(msg.Topic)

	// Ensure sequence number is at least msg.Seq
	for {
		curr := atomic.LoadUint64(&t.nextSeq)
		if msg.Seq <= curr || atomic.CompareAndSwapUint64(&t.nextSeq, curr, msg.Seq) {
			break
		}
	}

	t.ringBuffer.Append(msg)

	// Fan out to any local subscribers attached to this replica
	t.mu.RLock()
	broadcastList := make([]*Subscriber, 0, len(t.broadcastSubs))
	for _, s := range t.broadcastSubs {
		broadcastList = append(broadcastList, s)
	}
	groupRecipients := make([]*Subscriber, 0, len(t.groups))
	for _, cg := range t.groups {
		if member := cg.NextMember(); member != nil {
			groupRecipients = append(groupRecipients, member)
		}
	}
	t.mu.RUnlock()

	allRecipients := append(broadcastList, groupRecipients...)
	handler := PolicyHandler(m.cfg.BackpressurePolicy)
	timeout := m.cfg.BlockTimeout
	if timeout <= 0 {
		timeout = 500 * time.Millisecond
	}
	ackTimeout := m.cfg.AckTimeout
	if ackTimeout <= 0 {
		ackTimeout = 1 * time.Second
	}

	for _, s := range allRecipients {
		_ = s.Deliver(msg, handler, timeout, ackTimeout)
	}
}

// Publish assigns the next sequence number, stores it in the ring buffer,
// broadcasts to independent subscribers, and round-robins to one member per consumer group.
func (m *Manager) Publish(topicName string, payload []byte) (uint64, error) {
	return m.PublishWithIdempotency(topicName, payload, "", 0)
}

// PublishWithIdempotency assigns the next sequence number (or returns existing seq if retried),
// stores it in the ring buffer, and fans out to subscribers.
func (m *Manager) PublishWithIdempotency(topicName string, payload []byte, clientID string, clientSeq uint64) (uint64, error) {
	t := m.GetOrCreateTopic(topicName)

	// Idempotency check: if already processed, return existing assigned sequence
	if clientID != "" && clientSeq > 0 {
		if existingSeq, found := t.idempotency.Get(clientID, clientSeq); found {
			return existingSeq, nil
		}
	}

	seq := atomic.AddUint64(&t.nextSeq, 1)
	atomic.AddUint64(&m.publishedCount, 1)

	if clientID != "" && clientSeq > 0 {
		t.idempotency.Put(clientID, clientSeq, seq)
	}

	msg := &Message{
		Topic:       topicName,
		Seq:         seq,
		Payload:     payload,
		TimestampNS: time.Now().UnixNano(),
	}

	// Append to topic ring buffer
	t.ringBuffer.Append(msg)

	// Snapshot broadcast subscribers and select one member per consumer group under read lock
	t.mu.RLock()
	broadcastList := make([]*Subscriber, 0, len(t.broadcastSubs))
	for _, s := range t.broadcastSubs {
		broadcastList = append(broadcastList, s)
	}

	groupRecipients := make([]*Subscriber, 0, len(t.groups))
	for _, cg := range t.groups {
		if member := cg.NextMember(); member != nil {
			groupRecipients = append(groupRecipients, member)
		}
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

	allRecipients := append(broadcastList, groupRecipients...)
	var disconnectedIDs []string

	for _, s := range allRecipients {
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
		for _, id := range disconnectedIDs {
			_ = m.RemoveSubscriber(topicName, id)
		}
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

// Subscribe is a convenience method for registering a subscriber with a given ID and group.
func (m *Manager) Subscribe(ctx context.Context, topicName, groupID, subscriberID string) (<-chan *Message, context.CancelFunc, error) {
	bufSize := m.cfg.SubscriberBufferSize
	if bufSize <= 0 {
		bufSize = 1000
	}

	sub := NewSubscriber(subscriberID, bufSize)
	sub.GroupID = groupID
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
