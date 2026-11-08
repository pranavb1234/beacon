package broker

import (
	"sync"
)

// RingBuffer is a thread-safe, fixed-capacity circular buffer storing the last N messages of a topic.
type RingBuffer struct {
	mu       sync.RWMutex
	capacity int
	buffer   []*Message
	head     int // next write slot
	count    int // number of messages currently stored
}

// NewRingBuffer creates a new RingBuffer with the given fixed capacity.
func NewRingBuffer(capacity int) *RingBuffer {
	if capacity <= 0 {
		capacity = 10000
	}
	return &RingBuffer{
		capacity: capacity,
		buffer:   make([]*Message, capacity),
	}
}

// Append inserts a message into the ring buffer, overwriting the oldest message if full.
func (rb *RingBuffer) Append(msg *Message) {
	if msg == nil {
		return
	}

	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.buffer[rb.head] = msg
	rb.head = (rb.head + 1) % rb.capacity

	if rb.count < rb.capacity {
		rb.count++
	}
}

// OldestSeq returns the sequence number of the oldest message stored, or 0 if empty.
func (rb *RingBuffer) OldestSeq() uint64 {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.count == 0 {
		return 0
	}
	oldestIdx := (rb.head - rb.count + rb.capacity) % rb.capacity
	return rb.buffer[oldestIdx].Seq
}

// NewestSeq returns the sequence number of the newest message stored, or 0 if empty.
func (rb *RingBuffer) NewestSeq() uint64 {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.count == 0 {
		return 0
	}
	newestIdx := (rb.head - 1 + rb.capacity) % rb.capacity
	return rb.buffer[newestIdx].Seq
}

// Len returns the number of messages currently stored in the buffer.
func (rb *RingBuffer) Len() int {
	rb.mu.RLock()
	defer rb.mu.RUnlock()
	return rb.count
}

// Cap returns the total capacity of the ring buffer.
func (rb *RingBuffer) Cap() int {
	return rb.capacity
}

// GetFrom returns all stored messages with seq >= given value in chronological order.
// If the requested seq is strictly older than OldestSeq, it returns (nil, false)
// to indicate that messages have already been evicted and data was lost.
func (rb *RingBuffer) GetFrom(seq uint64) ([]*Message, bool) {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if rb.count == 0 {
		return nil, true
	}

	startIdx := (rb.head - rb.count + rb.capacity) % rb.capacity
	oldestSeq := rb.buffer[startIdx].Seq

	// If requested seq is older than the oldest available message, data was evicted/lost
	if seq < oldestSeq {
		return nil, false
	}

	var results []*Message
	for i := 0; i < rb.count; i++ {
		idx := (startIdx + i) % rb.capacity
		msg := rb.buffer[idx]
		if msg.Seq >= seq {
			results = append(results, msg)
		}
	}

	return results, true
}

