package broker

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pranavb1234/beacon/internal/config"
)

// Tests that when an ACK is lost or omitted, the broker redelivers the message.
func TestAckRetry_LostAckCausesRedelivery(t *testing.T) {
	cfg := config.Default()
	cfg.AckTimeout = 40 * time.Millisecond
	cfg.MaxRetries = 3
	cfg.SubscriberBufferSize = 100

	m := NewManager(cfg, nil)
	topic := "lost-ack-topic"
	subID := "sub-unreliable"

	sub := NewSubscriber(subID, 100)
	require.NoError(t, m.AddSubscriber(topic, sub))
	defer sub.Close()

	// Publish 1 message
	seq, err := m.Publish(topic, []byte("payload-1"))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), seq)

	// First delivery
	select {
	case msg := <-sub.Send:
		assert.Equal(t, uint64(1), msg.Seq)
		assert.Equal(t, "payload-1", string(msg.Payload))
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for first delivery")
	}

	// Do NOT send ACK. The background scanner should detect expired deadline and redeliver.
	select {
	case redelivered := <-sub.Send:
		assert.Equal(t, uint64(1), redelivered.Seq)
		assert.Equal(t, "payload-1", string(redelivered.Payload))
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for redelivery after lost ACK")
	}

	stats := m.Stats()
	assert.GreaterOrEqual(t, stats.Retried, uint64(1))
}

// Tests that when an ACK is received, in-flight tracking is cleared and retries stop.
func TestAckRetry_AckStopsRetries(t *testing.T) {
	cfg := config.Default()
	cfg.AckTimeout = 40 * time.Millisecond
	cfg.MaxRetries = 3
	cfg.SubscriberBufferSize = 100

	m := NewManager(cfg, nil)
	topic := "ack-stops-topic"
	subID := "sub-reliable"

	sub := NewSubscriber(subID, 100)
	require.NoError(t, m.AddSubscriber(topic, sub))
	defer sub.Close()

	// Publish 1 message
	seq, err := m.Publish(topic, []byte("payload-ack"))
	require.NoError(t, err)

	// Receive first delivery
	select {
	case msg := <-sub.Send:
		assert.Equal(t, seq, msg.Seq)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for message")
	}

	// Immediately ACK the message
	acked := m.Acknowledge(topic, subID, seq)
	assert.True(t, acked, "expected acknowledge to succeed")
	assert.Equal(t, 0, sub.InFlightCount())

	// Wait past the ACK timeout window and verify NO redeliveries occur
	select {
	case redelivery := <-sub.Send:
		t.Fatalf("unexpected redelivery for acknowledged message seq=%d", redelivery.Seq)
	case <-time.After(120 * time.Millisecond):
		// Success: no redeliveries
	}

	stats := m.Stats()
	assert.Equal(t, uint64(0), stats.Retried)
	assert.Equal(t, uint64(1), stats.Acked)
	assert.Equal(t, uint64(0), stats.DeadLetter)
}

// Tests that when max retries is reached without ACK, the message is moved to the dead-letter counter.
func TestAckRetry_MaxRetriesReached(t *testing.T) {
	cfg := config.Default()
	cfg.AckTimeout = 25 * time.Millisecond
	cfg.MaxRetries = 2
	cfg.SubscriberBufferSize = 100

	m := NewManager(cfg, nil)
	topic := "dead-letter-topic"
	subID := "sub-dead-letter"

	sub := NewSubscriber(subID, 100)
	require.NoError(t, m.AddSubscriber(topic, sub))
	defer sub.Close()

	_, err := m.Publish(topic, []byte("never-acked"))
	require.NoError(t, err)

	// Consume initial delivery + 2 retries without acking
	deliveriesSeen := 0
	timeout := time.After(350 * time.Millisecond)

collect:
	for {
		select {
		case <-sub.Send:
			deliveriesSeen++
		case <-timeout:
			break collect
		}
	}

	// Initial delivery (1) + retries (up to 2)
	assert.GreaterOrEqual(t, deliveriesSeen, 2)

	// Verify message moved to dead-letter counter and cleared from in-flight
	stats := m.Stats()
	assert.Equal(t, uint64(1), stats.DeadLetter, "expected message to move to dead-letter counter")
	assert.Equal(t, 0, sub.InFlightCount(), "in-flight map should be cleared after dead-lettering")
}

// Tests Topic ring buffer integration: verifies that every published message is appended to the ring buffer.
func TestTopic_RingBufferIntegration(t *testing.T) {
	cfg := config.Default()
	cfg.BufferSizePerTopic = 5
	m := NewManager(cfg, nil)
	topic := "ringbuf-topic"

	top := m.GetOrCreateTopic(topic)
	require.NotNil(t, top.RingBuffer())
	assert.Equal(t, 5, top.RingBuffer().Cap())

	// Publish 7 messages
	for i := 1; i <= 7; i++ {
		_, err := m.Publish(topic, []byte(fmt.Sprintf("item-%d", i)))
		require.NoError(t, err)
	}

	// Ring buffer of capacity 5 should now store [3, 4, 5, 6, 7]
	rb := top.RingBuffer()
	assert.Equal(t, 5, rb.Len())
	assert.Equal(t, uint64(3), rb.OldestSeq())
	assert.Equal(t, uint64(7), rb.NewestSeq())

	msgs, ok := rb.GetFrom(3)
	assert.True(t, ok)
	assert.Len(t, msgs, 5)

	// Sequence 1 was evicted
	_, ok = rb.GetFrom(1)
	assert.False(t, ok)
}

// Tests concurrent Acknowledges from multiple goroutines under the race detector.
func TestAckRetry_ConcurrentAcks(t *testing.T) {
	cfg := config.Default()
	cfg.AckTimeout = 200 * time.Millisecond
	m := NewManager(cfg, nil)
	topic := "concurrent-acks"
	subID := "sub-concurrent"

	sub := NewSubscriber(subID, 500)
	require.NoError(t, m.AddSubscriber(topic, sub))
	defer sub.Close()

	msgCount := 100
	for i := 1; i <= msgCount; i++ {
		_, err := m.Publish(topic, []byte(fmt.Sprintf("m-%d", i)))
		require.NoError(t, err)
	}

	// Drain messages
	for i := 1; i <= msgCount; i++ {
		<-sub.Send
	}

	assert.Equal(t, msgCount, sub.InFlightCount())

	// Concurrently ACK all messages
	var acksRecorded uint64
	done := make(chan struct{})
	for i := 1; i <= msgCount; i++ {
		go func(seq uint64) {
			if m.Acknowledge(topic, subID, seq) {
				atomic.AddUint64(&acksRecorded, 1)
			}
			done <- struct{}{}
		}(uint64(i))
	}

	for i := 1; i <= msgCount; i++ {
		<-done
	}

	assert.Equal(t, uint64(msgCount), atomic.LoadUint64(&acksRecorded))
	assert.Equal(t, 0, sub.InFlightCount())
}

