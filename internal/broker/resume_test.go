package broker_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/pranavb1234/beacon/internal/broker"
	"github.com/pranavb1234/beacon/internal/client"
	"github.com/pranavb1234/beacon/internal/config"
	"github.com/pranavb1234/beacon/internal/pb"
)

// setupInProcessBroker initializes a test gRPC server with bufconn or TCP.
func setupInProcessBroker(t *testing.T, cfg *config.Config) (pb.BrokerClient, func()) {
	t.Helper()
	mgr := broker.NewManager(cfg, nil)
	srv := broker.NewServer(mgr, cfg, slog.Default())

	bufLis := bufconn.Listen(1024 * 1024)
	go func() {
		_ = srv.Serve(bufLis)
	}()

	dialer := func(context.Context, string) (net.Conn, error) {
		return bufLis.Dial()
	}

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(dialer),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)

	client := pb.NewBrokerClient(conn)
	cleanup := func() {
		_ = conn.Close()
		srv.GracefulStop()
		_ = bufLis.Close()
	}

	return client, cleanup
}

// TestResume_OutOfRangeError verifies that requesting a from_seq older than the
// ring buffer's oldest message returns gRPC error code OUT_OF_RANGE with oldest seq.
func TestResume_OutOfRangeError(t *testing.T) {
	cfg := config.Default()
	cfg.BufferSizePerTopic = 5 // Small ring buffer: stores last 5 messages
	bClient, cleanup := setupInProcessBroker(t, cfg)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	topic := "eviction-test-topic"

	// Publish 10 messages -> messages 1..5 will be evicted; ring buffer holds 6..10
	for i := 1; i <= 10; i++ {
		resp, err := bClient.Publish(ctx, &pb.PublishRequest{
			Topic:   topic,
			Payload: []byte(fmt.Sprintf("msg-%d", i)),
		})
		require.NoError(t, err)
		assert.Equal(t, uint64(i), resp.GetSequence())
	}

	// Request sequence 3, which has already been evicted (oldest available is 6)
	stream, err := bClient.Subscribe(ctx, &pb.SubscribeRequest{
		Topic:        topic,
		FromSeq:      3,
		SubscriberId: "test-evicted-sub",
	})
	require.NoError(t, err)

	_, err = stream.Recv()
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok, "expected gRPC status error")
	assert.Equal(t, codes.OutOfRange, st.Code())
	assert.Contains(t, st.Message(), "older than oldest available sequence 6")
}

// TestResume_ReplayAndLiveBoundaryUnderConcurrentPublishing tests that when replaying
// from_seq = 6 while publishers concurrently send new messages, the subscriber receives
// strictly contiguous, ordered messages without duplicates or gaps.
func TestResume_ReplayAndLiveBoundaryUnderConcurrentPublishing(t *testing.T) {
	cfg := config.Default()
	cfg.BufferSizePerTopic = 500
	cfg.SubscriberBufferSize = 500
	bClient, cleanup := setupInProcessBroker(t, cfg)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	topic := "boundary-race-topic"

	// 1. Publish initial 10 messages (1..10)
	for i := 1; i <= 10; i++ {
		_, err := bClient.Publish(ctx, &pb.PublishRequest{
			Topic:   topic,
			Payload: []byte(fmt.Sprintf("initial-%d", i)),
		})
		require.NoError(t, err)
	}

	// 2. Start subscriber requesting from_seq = 6
	stream, err := bClient.Subscribe(ctx, &pb.SubscribeRequest{
		Topic:        topic,
		FromSeq:      6,
		SubscriberId: "boundary-sub",
	})
	require.NoError(t, err)

	// 3. Concurrently publish 40 more messages (11..50)
	var pubWg sync.WaitGroup
	pubWg.Add(1)
	go func() {
		defer pubWg.Done()
		for i := 11; i <= 50; i++ {
			_, pubErr := bClient.Publish(ctx, &pb.PublishRequest{
				Topic:   topic,
				Payload: []byte(fmt.Sprintf("concurrent-%d", i)),
			})
			if pubErr != nil {
				return
			}
		}
	}()

	// 4. Subscriber reads messages until 50
	var receivedSeqs []uint64
	for len(receivedSeqs) < 45 { // Expecting sequences 6 through 50 (45 messages)
		msg, recvErr := stream.Recv()
		require.NoError(t, recvErr)
		receivedSeqs = append(receivedSeqs, msg.GetSeq())
	}

	pubWg.Wait()

	// Verify contiguous ordering and zero duplicates: exactly 6, 7, 8, ..., 50
	expectedSeq := uint64(6)
	for _, seq := range receivedSeqs {
		assert.Equal(t, expectedSeq, seq, "gap or out-of-order sequence detected at %d", seq)
		expectedSeq++
	}
	assert.Equal(t, uint64(51), expectedSeq)
}

// TestResume_ReconnectNoLossOrDup tests the SDK subscriber auto-reconnect behavior.
// When an active stream disconnects, the SDK resumes consumption with from_seq = last_processed + 1,
// verifying zero message loss and zero duplicates.
func TestResume_ReconnectNoLossOrDup(t *testing.T) {
	cfg := config.Default()
	cfg.BufferSizePerTopic = 100
	bClient, cleanup := setupInProcessBroker(t, cfg)
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	topic := "reconnect-topic"

	// Publish initial 5 messages
	for i := 1; i <= 5; i++ {
		_, err := bClient.Publish(ctx, &pb.PublishRequest{
			Topic:   topic,
			Payload: []byte(fmt.Sprintf("msg-%d", i)),
		})
		require.NoError(t, err)
	}

	sdkSub := client.NewSubscriber(bClient, "sdk-reconnect-sub")
	sdkSub.InitialBackoff = 10 * time.Millisecond
	sdkSub.MaxBackoff = 50 * time.Millisecond

	var receivedCount int32
	var mu sync.Mutex
	var receivedSeqs []uint64

	consumeCtx, consumeCancel := context.WithCancel(ctx)

	handler := func(msg *pb.Message) error {
		mu.Lock()
		receivedSeqs = append(receivedSeqs, msg.GetSeq())
		mu.Unlock()

		count := atomic.AddInt32(&receivedCount, 1)
		// Simulate network disconnect after receiving 5 messages
		if count == 5 {
			consumeCancel() // Terminate current stream
		}
		return nil
	}

	// 1. First consume session receives 1..5
	err := sdkSub.Consume(consumeCtx, topic, "", 1, handler)
	assert.True(t, err == nil || err == context.Canceled || err == io.EOF)
	assert.Equal(t, int32(5), atomic.LoadInt32(&receivedCount))
	assert.Equal(t, uint64(5), sdkSub.Deduplicator().HighestSeq(topic))

	// 2. Publish 5 more messages while client is disconnected
	for i := 6; i <= 10; i++ {
		_, err := bClient.Publish(ctx, &pb.PublishRequest{
			Topic:   topic,
			Payload: []byte(fmt.Sprintf("msg-%d", i)),
		})
		require.NoError(t, err)
	}

	// 3. Reconnect with a new context - SDK automatically resumes at HighestSeq + 1 (seq 6)
	consumeCtx2, consumeCancel2 := context.WithCancel(ctx)
	defer consumeCancel2()

	go func() {
		_ = sdkSub.Consume(consumeCtx2, topic, "", 1, func(msg *pb.Message) error {
			mu.Lock()
			receivedSeqs = append(receivedSeqs, msg.GetSeq())
			mu.Unlock()

			if atomic.AddInt32(&receivedCount, 1) == 10 {
				consumeCancel2()
			}
			return nil
		})
	}()

	// Wait until all 10 messages are processed
	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&receivedCount) == 10
	}, 3*time.Second, 20*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, receivedSeqs, 10)
	for i := 0; i < 10; i++ {
		assert.Equal(t, uint64(i+1), receivedSeqs[i])
	}
}

