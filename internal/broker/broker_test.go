package broker

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pranavb1234/beacon/internal/config"
)

func newTestManager(t *testing.T) *Manager {
	t.Helper()
	cfg := config.Default()
	cfg.SubscriberBufferSize = 1000
	return NewManager(cfg, nil)
}

func TestGetOrCreateTopic(t *testing.T) {
	m := newTestManager(t)

	tests := []struct {
		name      string
		topicName string
	}{
		{name: "create first topic", topicName: "orders"},
		{name: "create second topic", topicName: "payments"},
		{name: "access existing topic", topicName: "orders"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t1 := m.GetOrCreateTopic(tt.topicName)
			require.NotNil(t, t1)
			assert.Equal(t, tt.topicName, t1.Name)

			// Idempotent retrieval
			t2 := m.GetOrCreateTopic(tt.topicName)
			assert.Same(t, t1, t2, "expected identical pointer for topic %s", tt.topicName)
		})
	}
}

// Table-driven test for fan-out scenarios: single subscriber and many subscribers.
func TestPublish_FanOut(t *testing.T) {
	tests := []struct {
		name         string
		topic        string
		numSubs      int
		numMessages  int
	}{
		{
			name:        "single subscriber",
			topic:       "telemetry",
			numSubs:     1,
			numMessages: 50,
		},
		{
			name:        "many subscribers (fan-out to 5)",
			topic:       "news",
			numSubs:     5,
			numMessages: 50,
		},
		{
			name:        "many subscribers (fan-out to 25)",
			topic:       "market-data",
			numSubs:     25,
			numMessages: 30,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newTestManager(t)

			subs := make([]*Subscriber, tt.numSubs)
			for i := 0; i < tt.numSubs; i++ {
				sub := NewSubscriber(fmt.Sprintf("sub-%d", i), tt.numMessages+10)
				err := m.AddSubscriber(tt.topic, sub)
				require.NoError(t, err)
				subs[i] = sub
			}

			// Publish messages
			for i := 1; i <= tt.numMessages; i++ {
				payload := []byte(fmt.Sprintf("msg-%d", i))
				seq, err := m.Publish(tt.topic, payload)
				require.NoError(t, err)
				assert.Equal(t, uint64(i), seq)
			}

			// Verify every subscriber received all messages in sequence
			for _, sub := range subs {
				for i := 1; i <= tt.numMessages; i++ {
					select {
					case msg := <-sub.Send:
						assert.Equal(t, uint64(i), msg.Seq)
						assert.Equal(t, tt.topic, msg.Topic)
						assert.Equal(t, fmt.Sprintf("msg-%d", i), string(msg.Payload))
					case <-time.After(500 * time.Millisecond):
						t.Fatalf("timeout waiting for message %d on subscriber %s", i, sub.ID)
					}
				}
			}
		})
	}
}

// Tests subscribe and unsubscribe concurrently while actively publishing.
// Verifies no panics, no sends on closed channels, and exact-once closure.
func TestSubscribeUnsubscribe_WhilePublishing(t *testing.T) {
	m := newTestManager(t)
	topic := "dynamic-traffic"

	var stopPublishing int32
	var totalPublished uint64
	var publishErrors uint64

	// Concurrently publish in background
	var pubWg sync.WaitGroup
	for p := 0; p < 4; p++ {
		pubWg.Add(1)
		go func(publisherID int) {
			defer pubWg.Done()
			idx := 0
			for atomic.LoadInt32(&stopPublishing) == 0 {
				idx++
				payload := []byte(fmt.Sprintf("payload-%d-%d", publisherID, idx))
				if _, err := m.Publish(topic, payload); err != nil {
					atomic.AddUint64(&publishErrors, 1)
				}
				atomic.AddUint64(&totalPublished, 1)
				time.Sleep(100 * time.Microsecond)
			}
		}(p)
	}

	// Concurrently add and remove subscribers
	var subWg sync.WaitGroup
	for i := 0; i < 20; i++ {
		subWg.Add(1)
		go func(id int) {
			defer subWg.Done()
			subID := fmt.Sprintf("churn-sub-%d", id)
			sub := NewSubscriber(subID, 50)

			err := m.AddSubscriber(topic, sub)
			assert.NoError(t, err)

			// Read a few messages or yield
			timeout := time.After(20 * time.Millisecond)
		consume:
			for {
				select {
				case <-sub.Send:
				case <-timeout:
					break consume
				}
			}

			// Remove subscriber (must not panic or cause closed channel send)
			err = m.RemoveSubscriber(topic, subID)
			assert.NoError(t, err)

			// Duplicate removal should return ErrSubscriberNotFound
			err2 := m.RemoveSubscriber(topic, subID)
			assert.Equal(t, ErrSubscriberNotFound, err2)

			// Sub must be closed
			assert.True(t, sub.IsClosed())
		}(i)
	}

	subWg.Wait()
	atomic.StoreInt32(&stopPublishing, 1)
	pubWg.Wait()

	assert.Zero(t, atomic.LoadUint64(&publishErrors))
	assert.Greater(t, atomic.LoadUint64(&totalPublished), uint64(0))
}

// Table-driven test for monotonic sequence ordering under concurrent publishers.
func TestPublish_SequenceOrdering_ConcurrentPublishers(t *testing.T) {
	tests := []struct {
		name            string
		numPublishers   int
		msgsPerPub      int
	}{
		{name: "2 publishers, 100 msgs each", numPublishers: 2, msgsPerPub: 100},
		{name: "10 publishers, 100 msgs each", numPublishers: 10, msgsPerPub: 100},
		{name: "20 publishers, 50 msgs each", numPublishers: 20, msgsPerPub: 50},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := newTestManager(t)
			topic := "order-log"

			totalExpected := tt.numPublishers * tt.msgsPerPub
			seqChan := make(chan uint64, totalExpected)

			var wg sync.WaitGroup
			for p := 0; p < tt.numPublishers; p++ {
				wg.Add(1)
				go func(pubID int) {
					defer wg.Done()
					for i := 0; i < tt.msgsPerPub; i++ {
						seq, err := m.Publish(topic, []byte(fmt.Sprintf("p%d-m%d", pubID, i)))
						if assert.NoError(t, err) {
							seqChan <- seq
						}
					}
				}(p)
			}

			wg.Wait()
			close(seqChan)

			seqs := make([]uint64, 0, totalExpected)
			for seq := range seqChan {
				seqs = append(seqs, seq)
			}

			require.Equal(t, totalExpected, len(seqs), "did not collect expected count of sequence numbers")

			// Sort sequence numbers and verify strictly consecutive 1..totalExpected without gaps or duplicates
			sort.Slice(seqs, func(i, j int) bool { return seqs[i] < seqs[j] })
			for idx, seq := range seqs {
				expected := uint64(idx + 1)
				assert.Equal(t, expected, seq, "gap or duplicate sequence number at index %d", idx)
			}
		})
	}
}

func TestRemoveSubscriber_NotFound(t *testing.T) {
	m := newTestManager(t)

	err := m.RemoveSubscriber("non-existent-topic", "sub-1")
	assert.Equal(t, ErrTopicNotFound, err)

	_ = m.GetOrCreateTopic("existing-topic")
	err = m.RemoveSubscriber("existing-topic", "sub-unknown")
	assert.Equal(t, ErrSubscriberNotFound, err)
}

func TestAddSubscriber_Nil(t *testing.T) {
	m := newTestManager(t)
	err := m.AddSubscriber("orders", nil)
	assert.Equal(t, ErrNilSubscriber, err)
}

func TestSubscribe_HelperMethod(t *testing.T) {
	m := newTestManager(t)
	topic := "events"

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	msgChan, cancelSub, err := m.Subscribe(ctx, topic, "", "sub-1")
	require.NoError(t, err)
	require.NotNil(t, msgChan)

	seq, err := m.Publish(topic, []byte("event-1"))
	require.NoError(t, err)
	assert.Equal(t, uint64(1), seq)

	select {
	case msg := <-msgChan:
		assert.Equal(t, "event-1", string(msg.Payload))
	case <-time.After(100 * time.Millisecond):
		t.Fatal("timed out waiting for message")
	}

	cancelSub()

	// Verify subscriber was removed
	top := m.GetOrCreateTopic(topic)
	assert.Equal(t, 0, top.SubscribersCount())
}
