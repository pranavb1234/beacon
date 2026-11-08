package client

import (
	"context"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/pranavb1234/beacon/internal/pb"
)

func TestDeduplicator_HighestContiguousSequence(t *testing.T) {
	d := NewDeduplicator()
	topic := "events"

	// Message 1 is new
	assert.False(t, d.IsDuplicate(topic, 1))
	assert.Equal(t, uint64(1), d.HighestSeq(topic))

	// Message 1 repeated is duplicate
	assert.True(t, d.IsDuplicate(topic, 1))

	// Message 2 is new
	assert.False(t, d.IsDuplicate(topic, 2))
	assert.Equal(t, uint64(2), d.HighestSeq(topic))

	// Message 1 or 2 repeated are duplicates
	assert.True(t, d.IsDuplicate(topic, 1))
	assert.True(t, d.IsDuplicate(topic, 2))

	// Message 3 is new
	assert.False(t, d.IsDuplicate(topic, 3))
	assert.Equal(t, uint64(3), d.HighestSeq(topic))

	// Different topic maintains separate tracking
	assert.False(t, d.IsDuplicate("other-topic", 1))
	assert.True(t, d.IsDuplicate("other-topic", 1))
}

// Mock gRPC BrokerClient and Stream for testing SDK subscriber
type mockBrokerClient struct {
	pb.BrokerClient
	stream *mockStream
	ackMu  sync.Mutex
	acks   []*pb.AckRequest
}

func (m *mockBrokerClient) Subscribe(ctx context.Context, in *pb.SubscribeRequest, opts ...grpc.CallOption) (pb.Broker_SubscribeClient, error) {
	return m.stream, nil
}

func (m *mockBrokerClient) Ack(ctx context.Context, in *pb.AckRequest, opts ...grpc.CallOption) (*pb.AckResponse, error) {
	m.ackMu.Lock()
	defer m.ackMu.Unlock()
	m.acks = append(m.acks, in)
	return &pb.AckResponse{}, nil
}

func (m *mockBrokerClient) AckCount() int {
	m.ackMu.Lock()
	defer m.ackMu.Unlock()
	return len(m.acks)
}

type mockStream struct {
	grpc.ClientStream
	mu       sync.Mutex
	messages []*pb.Message
	idx      int
}

func (s *mockStream) Recv() (*pb.Message, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.idx >= len(s.messages) {
		return nil, io.EOF
	}
	msg := s.messages[s.idx]
	s.idx++
	return msg, nil
}

// Tests that duplicate messages are ignored by SDK business handler, but still ACKed to the broker.
func TestSubscriber_DuplicateIgnoredBySDKButAcked(t *testing.T) {
	topic := "orders"
	subID := "sdk-sub-1"

	// Mock stream providing: msg1, msg1 (duplicate redelivery), msg2
	mock := &mockBrokerClient{
		stream: &mockStream{
			messages: []*pb.Message{
				{Topic: topic, Seq: 1, Payload: []byte("order-1")},
				{Topic: topic, Seq: 1, Payload: []byte("order-1-redelivery")},
				{Topic: topic, Seq: 2, Payload: []byte("order-2")},
			},
		},
	}

	sdkSub := NewSubscriber(mock, subID)

	var handlerCalls int32
	receivedPayloads := make([]string, 0)
	var mu sync.Mutex

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := sdkSub.Consume(ctx, topic, "", 0, func(msg *pb.Message) error {
		atomic.AddInt32(&handlerCalls, 1)
		mu.Lock()
		receivedPayloads = append(receivedPayloads, string(msg.Payload))
		mu.Unlock()
		return nil
	})
	require.NoError(t, err)

	// 1. Business handler should be called exactly twice (for msg 1 and msg 2; duplicate ignored)
	assert.Equal(t, int32(2), atomic.LoadInt32(&handlerCalls))
	assert.Equal(t, []string{"order-1", "order-2"}, receivedPayloads)

	// 2. BrokerClient.Ack must have been called 3 times (including for the duplicate redelivery)
	assert.Equal(t, 3, mock.AckCount())

	mock.ackMu.Lock()
	defer mock.ackMu.Unlock()
	assert.Equal(t, uint64(1), mock.acks[0].Seq)
	assert.Equal(t, uint64(1), mock.acks[1].Seq, "duplicate message must be ACKed")
	assert.Equal(t, uint64(2), mock.acks[2].Seq)
}
