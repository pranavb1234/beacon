// Package client provides client-side SDK utilities for communicating with Beacon pub/sub brokers.
package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

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

	InitialBackoff time.Duration
	MaxBackoff     time.Duration
}

// NewSubscriber creates a new SDK Subscriber.
func NewSubscriber(client pb.BrokerClient, subscriberID string) *Subscriber {
	return &Subscriber{
		client:         client,
		subscriberID:   subscriberID,
		dedup:          NewDeduplicator(),
		InitialBackoff: 50 * time.Millisecond,
		MaxBackoff:     2 * time.Second,
	}
}

// Deduplicator returns the subscriber's deduplicator instance.
func (s *Subscriber) Deduplicator() *Deduplicator {
	return s.dedup
}

// Consume starts consuming messages from topic. If the stream disconnects unexpectedly,
// it automatically reconnects with from_seq = last_processed_seq + 1 using exponential backoff.
func (s *Subscriber) Consume(ctx context.Context, topic, group string, fromSeq uint64, handler MessageHandler) error {
	backoff := s.InitialBackoff
	if backoff <= 0 {
		backoff = 50 * time.Millisecond
	}
	maxBackoff := s.MaxBackoff
	if maxBackoff <= 0 {
		maxBackoff = 2 * time.Second
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Calculate resume sequence: if we've processed messages, resume at highest + 1
		reqFromSeq := fromSeq
		highest := s.dedup.HighestSeq(topic)
		if highest > 0 {
			reqFromSeq = highest + 1
		}

		err := s.streamOnce(ctx, topic, group, reqFromSeq, handler)
		if err == nil || errors.Is(err, io.EOF) || errors.Is(ctx.Err(), context.Canceled) {
			return nil
		}

		// Exponential backoff before reconnecting
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
			backoff *= 2
			if backoff > maxBackoff {
				backoff = maxBackoff
			}
		}
	}
}

func (s *Subscriber) streamOnce(ctx context.Context, topic, group string, fromSeq uint64, handler MessageHandler) error {
	stream, err := s.client.Subscribe(ctx, &pb.SubscribeRequest{
		Topic:        topic,
		Group:        group,
		FromSeq:      fromSeq,
		SubscriberId: s.subscriberID,
	})
	if err != nil {
		return fmt.Errorf("subscribe to topic %q: %w", topic, err)
	}

	for {
		msg, err := stream.Recv()
		if err != nil {
			return err
		}

		// Check deduplication
		if s.dedup.IsDuplicate(msg.GetTopic(), msg.GetSeq()) {
			s.ack(ctx, msg.GetTopic(), group, msg.GetSeq())
			continue
		}

		// Dispatch to user handler
		if err := handler(msg); err != nil {
			continue
		}

		// ACK processed message
		s.ack(ctx, msg.GetTopic(), group, msg.GetSeq())
	}
}

func (s *Subscriber) ack(ctx context.Context, topic, group string, seq uint64) {
	ackCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, _ = s.client.Ack(ackCtx, &pb.AckRequest{
		Topic:        topic,
		Group:        group,
		SubscriberId: s.subscriberID,
		Seq:          seq,
	})
}
