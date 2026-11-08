// Package client provides client-side SDK utilities for communicating with Beacon pub/sub brokers.
package client

import (
	"context"
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

// Subscriber is an SDK subscriber that handles streaming, deduplication, and automatic ACKs.
type Subscriber struct {
	client       pb.BrokerClient
	subscriberID string
	dedup        *Deduplicator
}

// NewSubscriber creates a new SDK Subscriber.
func NewSubscriber(client pb.BrokerClient, subscriberID string) *Subscriber {
	return &Subscriber{
		client:       client,
		subscriberID: subscriberID,
		dedup:        NewDeduplicator(),
	}
}

// Deduplicator returns the subscriber's deduplicator instance.
func (s *Subscriber) Deduplicator() *Deduplicator {
	return s.dedup
}

// Consume subscribes to a topic and processes incoming messages.
// It de-duplicates by remembering the highest contiguous sequence processed per topic,
// ignores redeliveries for processing, and always sends an ACK for duplicates so the broker stops retrying.
func (s *Subscriber) Consume(ctx context.Context, topic, group string, fromSeq uint64, handler MessageHandler) error {
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
			if err == io.EOF || ctx.Err() != nil {
				return nil
			}
			return err
		}

		// Check if message is a redelivered duplicate
		if s.dedup.IsDuplicate(msg.GetTopic(), msg.GetSeq()) {
			// Always ACK duplicate so the broker clears in-flight state and stops retrying
			s.ack(ctx, msg.GetTopic(), group, msg.GetSeq())
			continue
		}

		// Dispatch new message to handler
		if err := handler(msg); err != nil {
			// If handler failed, do not ACK so message will be redelivered
			continue
		}

		// Processed successfully: send ACK
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
