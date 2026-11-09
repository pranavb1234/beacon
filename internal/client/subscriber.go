// Package client provides client-side SDK utilities for communicating with Beacon pub/sub brokers.
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/pranavb1234/beacon/internal/pb"
)

// MessageHandler is the user-provided handler callback invoked for each new message.
type MessageHandler func(msg *pb.Message) error

// Deduplicator tracks the highest sequence number processed per topic
// to detect duplicate redeliveries.
type Deduplicator struct {
	mu      sync.Mutex
	highest map[string]uint64
}

// NewDeduplicator creates a new Deduplicator.
func NewDeduplicator() *Deduplicator {
	return &Deduplicator{
		highest: make(map[string]uint64),
	}
}

// IsDuplicate returns true if seq has already been processed for the topic.
// If the message is new (seq > highest), it updates the record and returns false.
func (d *Deduplicator) IsDuplicate(topic string, seq uint64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()

	last, exists := d.highest[topic]
	if exists && seq <= last {
		return true // Duplicate message
	}
	d.highest[topic] = seq
	return false // New message
}

// HighestSeq returns the highest sequence number processed for the given topic.
func (d *Deduplicator) HighestSeq(topic string) uint64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.highest[topic]
}

// Subscriber is an SDK subscriber that handles streaming, deduplication, automatic ACKs,
// and seamless auto-reconnect from last processed sequence.
type Subscriber struct {
	client       pb.BrokerClient
	subscriberID string
	dedup        *Deduplicator

	Brokers        []string
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	Jitter         bool

	// Callbacks
	OnReconnect func(brokerAddr string)
	OnGap       func(topic string, expectedSeq, actualSeq uint64)

	mu          sync.Mutex
	connIdx     int
	conns       map[string]*grpc.ClientConn
	clients     map[string]pb.BrokerClient
	lastSeenSeq map[string]uint64
	dialer      func(ctx context.Context, addr string) (net.Conn, error)
	rng         *rand.Rand
}

// SubscriberOption configures Subscriber.
type SubscriberOption func(*Subscriber)

// WithSubscriberDialer sets a custom dialer (e.g. bufconn).
func WithSubscriberDialer(d func(ctx context.Context, addr string) (net.Conn, error)) SubscriberOption {
	return func(s *Subscriber) {
		s.dialer = d
	}
}

// NewSubscriber creates a new SDK Subscriber.
func NewSubscriber(client pb.BrokerClient, subscriberID string) *Subscriber {
	return &Subscriber{
		client:         client,
		subscriberID:   subscriberID,
		dedup:          NewDeduplicator(),
		InitialBackoff: 30 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		conns:          make(map[string]*grpc.ClientConn),
		clients:        make(map[string]pb.BrokerClient),
		lastSeenSeq:    make(map[string]uint64),
		rng:            rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// NewClusterSubscriber creates a subscriber configured with multiple broker addresses.
func NewClusterSubscriber(brokers []string, subscriberID string, opts ...SubscriberOption) *Subscriber {
	s := &Subscriber{
		subscriberID:   subscriberID,
		dedup:          NewDeduplicator(),
		Brokers:        brokers,
		InitialBackoff: 30 * time.Millisecond,
		MaxBackoff:     1 * time.Second,
		Jitter:         true,
		conns:          make(map[string]*grpc.ClientConn),
		clients:        make(map[string]pb.BrokerClient),
		lastSeenSeq:    make(map[string]uint64),
		rng:            rand.New(rand.NewSource(time.Now().UnixNano())),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Deduplicator returns the subscriber's deduplicator instance.
func (s *Subscriber) Deduplicator() *Deduplicator {
	return s.dedup
}

// Consume starts consuming messages from topic. If the stream disconnects unexpectedly,
// it automatically reconnects to any available broker with from_seq = last_processed_seq + 1.
func (s *Subscriber) Consume(ctx context.Context, topic, group string, fromSeq uint64, handler MessageHandler) error {
	backoff := s.InitialBackoff
	if backoff <= 0 {
		backoff = 30 * time.Millisecond
	}
	maxBackoff := s.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 1 * time.Second
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Calculate resume sequence: resume at highest contiguous sequence + 1
		reqFromSeq := fromSeq
		highest := s.dedup.HighestSeq(topic)
		if highest > 0 {
			reqFromSeq = highest + 1
		}

		client, addr, err := s.getClient()
		if err != nil {
			s.rotateBroker()
			continue
		}

		if s.OnReconnect != nil {
			s.OnReconnect(addr)
		}

		err = s.streamOnce(ctx, client, addr, topic, group, reqFromSeq, handler)
		if err == nil || errors.Is(err, io.EOF) || errors.Is(ctx.Err(), context.Canceled) {
			return nil
		}

		s.rotateBroker()

		// Backoff with optional jitter
		sleepDuration := backoff
		if s.Jitter {
			s.mu.Lock()
			factor := 0.5 + s.rng.Float64()*0.5
			s.mu.Unlock()
			sleepDuration = time.Duration(float64(sleepDuration) * factor)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepDuration):
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func (s *Subscriber) streamOnce(ctx context.Context, client pb.BrokerClient, addr, topic, group string, fromSeq uint64, handler MessageHandler) error {
	stream, err := client.Subscribe(ctx, &pb.SubscribeRequest{
		Topic:        topic,
		Group:        group,
		FromSeq:      fromSeq,
		SubscriberId: s.subscriberID,
	})
	if err != nil {
		return fmt.Errorf("subscribe to topic %q at %s: %w", topic, addr, err)
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}

		// Gap detection: check if received sequence skipped ahead
		s.mu.Lock()
		last := s.lastSeenSeq[msg.GetTopic()]
		expected := last + 1
		if last > 0 && msg.GetSeq() > expected {
			if s.OnGap != nil {
				s.OnGap(msg.GetTopic(), expected, msg.GetSeq())
			}
		}
		if msg.GetSeq() > last {
			s.lastSeenSeq[msg.GetTopic()] = msg.GetSeq()
		}
		s.mu.Unlock()

		// Deduplication check
		if s.dedup.IsDuplicate(msg.GetTopic(), msg.GetSeq()) {
			s.ack(ctx, client, msg.GetTopic(), group, msg.GetSeq())
			continue
		}

		// Dispatch to user handler
		if err := handler(msg); err != nil {
			continue
		}

		// ACK processed message
		s.ack(ctx, client, msg.GetTopic(), group, msg.GetSeq())
	}
}

func (s *Subscriber) ack(ctx context.Context, client pb.BrokerClient, topic, group string, seq uint64) {
	ackCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, _ = client.Ack(ackCtx, &pb.AckRequest{
		Topic:        topic,
		Group:        group,
		SubscriberId: s.subscriberID,
		Seq:          seq,
	})
}

func (s *Subscriber) getClient() (pb.BrokerClient, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.client != nil {
		return s.client, "primary", nil
	}

	if len(s.Brokers) == 0 {
		return nil, "", fmt.Errorf("no broker addresses configured")
	}

	addr := s.Brokers[s.connIdx%len(s.Brokers)]
	if c, ok := s.clients[addr]; ok {
		return c, addr, nil
	}

	var dialOpts []grpc.DialOption
	dialOpts = append(dialOpts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if s.dialer != nil {
		dialOpts = append(dialOpts, grpc.WithContextDialer(s.dialer))
	}

	target := addr
	if s.dialer != nil {
		target = "passthrough:///" + addr
	}

	conn, err := grpc.NewClient(target, dialOpts...)
	if err != nil {
		return nil, addr, fmt.Errorf("dial %s: %w", addr, err)
	}

	c := pb.NewBrokerClient(conn)
	s.conns[addr] = conn
	s.clients[addr] = c

	return c, addr, nil
}

func (s *Subscriber) rotateBroker() {
	s.mu.Lock()
	s.connIdx++
	s.mu.Unlock()
}
