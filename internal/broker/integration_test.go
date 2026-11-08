package broker_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/pranavb1234/beacon/internal/broker"
	"github.com/pranavb1234/beacon/internal/config"
	"github.com/pranavb1234/beacon/internal/pb"
)

// TestBroker_Integration_OnePublisherTwoSubscribers tests the gRPC broker server in-process
// with 1 publisher and 2 concurrent subscribers, verifying full fan-out and ordered delivery.
func TestBroker_Integration_OnePublisherTwoSubscribers(t *testing.T) {
	cfg := config.Default()
	cfg.SubscriberBufferSize = 1000

	mgr := broker.NewManager(cfg, nil)
	srv := broker.NewServer(mgr, cfg, slog.Default())

	// Start server on a random port
	tcpLis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// Check if TCP connection is allowed in this environment (sandboxed environments block local TCP connect)
	testConn, dialErr := net.DialTimeout("tcp", tcpLis.Addr().String(), 50*time.Millisecond)

	var lis net.Listener
	var dialer func(context.Context, string) (net.Conn, error)

	if dialErr != nil {
		// Fallback to in-process bufconn for restricted sandbox environments
		_ = tcpLis.Close()
		bufLis := bufconn.Listen(1024 * 1024)
		lis = bufLis
		dialer = func(context.Context, string) (net.Conn, error) {
			return bufLis.Dial()
		}
	} else {
		_ = testConn.Close()
		lis = tcpLis
		dialer = nil
	}

	go func() {
		_ = srv.Serve(lis)
	}()
	defer srv.GracefulStop()

	// Connect gRPC client
	var conn *grpc.ClientConn
	if dialer != nil {
		conn, err = grpc.NewClient("passthrough://bufnet",
			grpc.WithContextDialer(dialer),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
	} else {
		conn, err = grpc.NewClient(lis.Addr().String(),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
	}
	require.NoError(t, err)
	defer conn.Close()

	client := pb.NewBrokerClient(conn)

	topic := "integration-stream"
	messageCount := 25

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Spawn Subscriber 1
	sub1Stream, err := client.Subscribe(ctx, &pb.SubscribeRequest{
		Topic:        topic,
		SubscriberId: "subscriber-alpha",
	})
	require.NoError(t, err)

	// Spawn Subscriber 2
	sub2Stream, err := client.Subscribe(ctx, &pb.SubscribeRequest{
		Topic:        topic,
		SubscriberId: "subscriber-beta",
	})
	require.NoError(t, err)

	// Pause briefly to ensure subscriptions are registered before publishing
	time.Sleep(50 * time.Millisecond)

	var wg sync.WaitGroup
	sub1Received := make([]*pb.Message, 0, messageCount)
	sub2Received := make([]*pb.Message, 0, messageCount)
	var sub1Mu, sub2Mu sync.Mutex

	// Consumer goroutine for Subscriber 1
	wg.Add(1)
	go func() {
		defer wg.Done()
		for len(sub1Received) < messageCount {
			msg, err := sub1Stream.Recv()
			if err != nil {
				if err == io.EOF || ctx.Err() != nil {
					return
				}
				t.Errorf("subscriber 1 recv error: %v", err)
				return
			}
			sub1Mu.Lock()
			sub1Received = append(sub1Received, msg)
			sub1Mu.Unlock()

			// Send Ack
			_, _ = client.Ack(ctx, &pb.AckRequest{
				Topic:        msg.Topic,
				SubscriberId: "subscriber-alpha",
				Seq:          msg.Seq,
			})
		}
	}()

	// Consumer goroutine for Subscriber 2
	wg.Add(1)
	go func() {
		defer wg.Done()
		for len(sub2Received) < messageCount {
			msg, err := sub2Stream.Recv()
			if err != nil {
				if err == io.EOF || ctx.Err() != nil {
					return
				}
				t.Errorf("subscriber 2 recv error: %v", err)
				return
			}
			sub2Mu.Lock()
			sub2Received = append(sub2Received, msg)
			sub2Mu.Unlock()

			// Send Ack
			_, _ = client.Ack(ctx, &pb.AckRequest{
				Topic:        msg.Topic,
				SubscriberId: "subscriber-beta",
				Seq:          msg.Seq,
			})
		}
	}()

	// Publish messages sequentially
	for i := 1; i <= messageCount; i++ {
		payload := []byte(fmt.Sprintf("order-payload-%d", i))
		pubCtx, pubCancel := context.WithTimeout(context.Background(), 2*time.Second)
		resp, err := client.Publish(pubCtx, &pb.PublishRequest{
			Topic:   topic,
			Payload: payload,
		})
		pubCancel()

		require.NoError(t, err, "publish failed for message %d", i)
		assert.Equal(t, uint64(i), resp.Sequence, "unexpected sequence number assigned")
	}

	// Wait for consumers to receive all messages
	doneChan := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneChan)
	}()

	select {
	case <-doneChan:
		// Succeeded
	case <-time.After(3 * time.Second):
		t.Fatalf("timed out waiting for subscribers to receive all %d messages (sub1: %d, sub2: %d)",
			messageCount, len(sub1Received), len(sub2Received))
	}

	// Verify Subscriber 1 received all messages in strictly increasing sequence order
	require.Len(t, sub1Received, messageCount)
	for idx, msg := range sub1Received {
		expectedSeq := uint64(idx + 1)
		expectedPayload := fmt.Sprintf("order-payload-%d", idx+1)
		assert.Equal(t, topic, msg.Topic)
		assert.Equal(t, expectedSeq, msg.Seq, "sub1 seq mismatch at index %d", idx)
		assert.Equal(t, expectedPayload, string(msg.Payload))
		assert.Greater(t, msg.TimestampNs, int64(0))
	}

	// Verify Subscriber 2 received all messages in strictly increasing sequence order
	require.Len(t, sub2Received, messageCount)
	for idx, msg := range sub2Received {
		expectedSeq := uint64(idx + 1)
		expectedPayload := fmt.Sprintf("order-payload-%d", idx+1)
		assert.Equal(t, topic, msg.Topic)
		assert.Equal(t, expectedSeq, msg.Seq, "sub2 seq mismatch at index %d", idx)
		assert.Equal(t, expectedPayload, string(msg.Payload))
		assert.Greater(t, msg.TimestampNs, int64(0))
	}
}
