package broker

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Tests that the buffer correctly fills to exact capacity without dropping anything.
func TestRingBuffer_ExactCapacity(t *testing.T) {
	capacity := 5
	rb := NewRingBuffer(capacity)

	assert.Equal(t, 0, rb.Len())
	assert.Equal(t, uint64(0), rb.OldestSeq())
	assert.Equal(t, uint64(0), rb.NewestSeq())

	msgs, ok := rb.GetFrom(1)
	assert.True(t, ok)
	assert.Empty(t, msgs)

	for i := 1; i <= capacity; i++ {
		rb.Append(&Message{
			Topic:   "test",
			Seq:     uint64(i),
			Payload: []byte(fmt.Sprintf("data-%d", i)),
		})
		assert.Equal(t, i, rb.Len())
		assert.Equal(t, uint64(1), rb.OldestSeq())
		assert.Equal(t, uint64(i), rb.NewestSeq())
	}

	assert.Equal(t, capacity, rb.Len())

	// Fetch from seq 1 -> all 5 returned in order
	msgs, ok = rb.GetFrom(1)
	require.True(t, ok)
	require.Len(t, msgs, capacity)
	for i, m := range msgs {
		assert.Equal(t, uint64(i+1), m.Seq)
		assert.Equal(t, fmt.Sprintf("data-%d", i+1), string(m.Payload))
	}
}

// Tests buffer wraparound when appending beyond capacity.
func TestRingBuffer_Wraparound(t *testing.T) {
	capacity := 3
	rb := NewRingBuffer(capacity)

	// Append 6 messages into buffer of size 3
	for i := 1; i <= 6; i++ {
		rb.Append(&Message{
			Topic:   "test",
			Seq:     uint64(i),
			Payload: []byte(fmt.Sprintf("msg-%d", i)),
		})
	}

	assert.Equal(t, capacity, rb.Len())
	assert.Equal(t, uint64(4), rb.OldestSeq())
	assert.Equal(t, uint64(6), rb.NewestSeq())

	// Stored messages should be 4, 5, 6
	msgs, ok := rb.GetFrom(4)
	require.True(t, ok)
	require.Len(t, msgs, 3)
	assert.Equal(t, uint64(4), msgs[0].Seq)
	assert.Equal(t, uint64(5), msgs[1].Seq)
	assert.Equal(t, uint64(6), msgs[2].Seq)

	// Fetch from seq 5 -> messages 5, 6 returned
	msgs, ok = rb.GetFrom(5)
	require.True(t, ok)
	require.Len(t, msgs, 2)
	assert.Equal(t, uint64(5), msgs[0].Seq)
	assert.Equal(t, uint64(6), msgs[1].Seq)
}

// Tests requesting evicted sequence: when requested seq < OldestSeq, return ok=false.
func TestRingBuffer_RequestingEvictedSequence(t *testing.T) {
	capacity := 3
	rb := NewRingBuffer(capacity)

	// Append messages 1 through 5.
	// Buffer will hold [3, 4, 5]. Messages 1 and 2 are evicted.
	for i := 1; i <= 5; i++ {
		rb.Append(&Message{
			Topic: "test",
			Seq:   uint64(i),
		})
	}

	assert.Equal(t, uint64(3), rb.OldestSeq())
	assert.Equal(t, uint64(5), rb.NewestSeq())

	// Requesting seq 1 (evicted) -> ok=false
	msgs, ok := rb.GetFrom(1)
	assert.False(t, ok, "expected ok=false for evicted sequence 1")
	assert.Nil(t, msgs)

	// Requesting seq 2 (evicted) -> ok=false
	msgs, ok = rb.GetFrom(2)
	assert.False(t, ok, "expected ok=false for evicted sequence 2")
	assert.Nil(t, msgs)

	// Requesting seq 3 (oldest available) -> ok=true
	msgs, ok = rb.GetFrom(3)
	assert.True(t, ok)
	assert.Len(t, msgs, 3)

	// Requesting future sequence 10 -> ok=true, empty slice
	msgs, ok = rb.GetFrom(10)
	assert.True(t, ok)
	assert.Empty(t, msgs)
}

// Tests thread-safe concurrent Appends and Reads under the race detector.
func TestRingBuffer_ConcurrentAppendRead(t *testing.T) {
	capacity := 50
	rb := NewRingBuffer(capacity)

	var wg sync.WaitGroup
	appendCount := 1000

	// 5 concurrent writer goroutines
	for w := 0; w < 5; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < appendCount; i++ {
				rb.Append(&Message{
					Topic: "concurrent",
					Seq:   uint64(workerID*appendCount + i + 1),
				})
			}
		}(w)
	}

	// 5 concurrent reader goroutines
	for r := 0; r < 5; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 200; i++ {
				_ = rb.Len()
				_ = rb.OldestSeq()
				_ = rb.NewestSeq()
				_, _ = rb.GetFrom(uint64(i * 10))
			}
		}()
	}

	wg.Wait()

	assert.Equal(t, capacity, rb.Len())
	assert.Greater(t, rb.NewestSeq(), uint64(0))
	assert.Greater(t, rb.OldestSeq(), uint64(0))
}
