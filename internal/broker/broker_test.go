package broker

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pranavb1234/beacon/internal/config"
	"github.com/pranavb1234/beacon/internal/metrics"
)

// newTestBroker is a test helper that builds a Broker with defaults and an
// isolated Prometheus registry so tests never share metric state.
func newTestBroker(t *testing.T) *Broker {
	t.Helper()
	cfg, err := config.Load("")
	require.NoError(t, err)
	met := metrics.New(prometheus.NewRegistry())
	return New(cfg, met)
}

func TestNew(t *testing.T) {
	b := newTestBroker(t)
	assert.NotNil(t, b)
	assert.Empty(t, b.topics)
}

func TestGetOrCreateTopic_ReturnsSamePointer(t *testing.T) {
	b := newTestBroker(t)
	t1 := b.getOrCreateTopic("orders")
	t2 := b.getOrCreateTopic("orders")
	assert.Same(t, t1, t2, "must return the identical *topic on repeated calls")
}

func TestGetOrCreateTopic_DifferentTopics(t *testing.T) {
	b := newTestBroker(t)
	ta := b.getOrCreateTopic("orders")
	tb := b.getOrCreateTopic("payments")
	assert.NotSame(t, ta, tb)
}

// TestPublishStub ensures the Phase 0 stub returns no error and seq=0.
func TestPublishStub(t *testing.T) {
	b := newTestBroker(t)
	seq, err := b.Publish(context.Background(), "orders", []byte("hello"))
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), seq)
}

// TestSubscribeStub ensures the Phase 0 stub returns a usable channel.
func TestSubscribeStub(t *testing.T) {
	b := newTestBroker(t)
	ch, cancel, err := b.Subscribe(context.Background(), "orders", "", "sub-1")
	require.NoError(t, err)
	assert.NotNil(t, ch)
	cancel() // must not panic; channel is closed
}

// TestGetOrCreateTopicConcurrent verifies no data race under concurrent callers.
func TestGetOrCreateTopicConcurrent(t *testing.T) {
	b := newTestBroker(t)
	const goroutines = 20

	start := make(chan struct{})
	done := make(chan struct{}, goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			<-start
			b.getOrCreateTopic("shared-topic")
			done <- struct{}{}
		}()
	}

	close(start)
	for i := 0; i < goroutines; i++ {
		<-done
	}

	b.mu.RLock()
	assert.Len(t, b.topics, 1)
	b.mu.RUnlock()
}
