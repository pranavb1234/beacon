package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/pranavb1234/beacon/internal/pb"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9001", "broker gRPC address")
	topic := flag.String("topic", "orders", "topic to subscribe to")
	group := flag.String("group", "", "consumer group ID (optional)")
	id := flag.String("id", "", "subscriber identifier (optional; auto-generated if empty)")
	flag.Parse()

	subID := *id
	if subID == "" {
		subID = fmt.Sprintf("sub-%d", os.Getpid())
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to broker at %s: %v\n", *addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewBrokerClient(conn)

	fmt.Printf("[subscriber %s] connecting to %s for topic=%q (group=%q)\n",
		subID, *addr, *topic, *group)

	stream, err := client.Subscribe(ctx, &pb.SubscribeRequest{
		Topic:        *topic,
		Group:        *group,
		SubscriberId: subID,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "[subscriber %s] subscribe error: %v\n", subID, err)
		os.Exit(1)
	}

	fmt.Printf("[subscriber %s] subscribed successfully; listening for messages (Ctrl+C to stop)...\n", subID)

	for {
		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(ctx.Err(), context.Canceled) {
				fmt.Printf("[subscriber %s] stream closed gracefully\n", subID)
				break
			}
			fmt.Fprintf(os.Stderr, "[subscriber %s] receive error: %v\n", subID, err)
			break
		}

		fmt.Printf("[subscriber %s] received topic=%q seq=%d payload=%q timestamp_ns=%d\n",
			subID, msg.GetTopic(), msg.GetSeq(), string(msg.GetPayload()), msg.GetTimestampNs())

		// Acknowledge receipt
		ackCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, ackErr := client.Ack(ackCtx, &pb.AckRequest{
			Topic:        msg.GetTopic(),
			Group:        *group,
			SubscriberId: subID,
			Seq:          msg.GetSeq(),
		})
		cancel()
		if ackErr != nil {
			fmt.Fprintf(os.Stderr, "[subscriber %s] ack warning for seq=%d: %v\n", subID, msg.GetSeq(), ackErr)
		}
	}
}
