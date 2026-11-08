# Beacon Pub/Sub Broker

Beacon is a distributed, in-memory publish-subscribe message broker written in Go. It provides high-throughput message streaming with consistent hashing for cluster partitioning, bounded per-subscriber ring buffers, configurable backpressure handling (block, drop-oldest, disconnect), and at-least-once delivery semantics over gRPC.
