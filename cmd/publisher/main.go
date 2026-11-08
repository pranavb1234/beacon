package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/pranavb1234/beacon/internal/pb"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:9001", "broker gRPC address")
	topic := flag.String("topic", "orders", "topic to publish messages to")
	count := flag.Int("count", 10, "number of messages to publish")
	rate := flag.Float64("rate", 1.0, "publication rate in messages per second (0 for unlimited)")
	payload := flag.String("payload", "hello beacon", "message payload string")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to broker at %s: %v\n", *addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	client := pb.NewBrokerClient(conn)

	var delay time.Duration
	if *rate > 0 {
		delay = time.Duration(float64(time.Second) / *rate)
	}

	fmt.Printf("[publisher] starting publication to topic=%q at %s (count=%d, rate=%.1f msg/s)\n",
		*topic, *addr, *count, *rate)

	for i := 1; i <= *count; i++ {
		msgContent := fmt.Sprintf("%s #%d", *payload, i)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

		resp, err := client.Publish(ctx, &pb.PublishRequest{
			Topic:   *topic,
			Payload: []byte(msgContent),
		})
		cancel()

		if err != nil {
			fmt.Fprintf(os.Stderr, "[publisher] error publishing message #%d: %v\n", i, err)
		} else {
			fmt.Printf("[publisher] sent topic=%q payload=%q -> assigned seq=%d\n",
				*topic, msgContent, resp.GetSequence())
		}

		if delay > 0 && i < *count {
			time.Sleep(delay)
		}
	}

	fmt.Printf("[publisher] finished publishing %d messages to topic %q\n", *count, *topic)
}
