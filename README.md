# Beacon — Distributed In-Memory Pub/Sub Broker

> A Kafka-inspired distributed pub/sub broker written in idiomatic Go.
> Built as a portfolio/learning project demonstrating distributed systems fundamentals.

## Architecture at a Glance

```
 Publisher          Consumer A
     │                  ▲
     ▼                  │
 ┌────────┐   gRPC   ┌──────────────────────────────────────┐
 │Broker 1│◄────────►│           Cluster (3 nodes)           │
 └────────┘ cluster  │                                      │
                     │  Broker 1   Broker 2   Broker 3      │
                     │  ┌──────┐  ┌──────┐  ┌──────┐       │
                     │  │topic │  │topic │  │topic │       │
                     │  │ring  │  │ring  │  │ring  │       │
                     │  │buf   │  │buf   │  │buf   │       │
                     │  └──────┘  └──────┘  └──────┘       │
                     └──────────────────────────────────────┘
```

Each topic is owned by one node (determined by **consistent hashing with virtual nodes**).
The next distinct node on the ring holds the **in-memory replica**.

## Key Design Decisions

| Decision | Why |
|----------|-----|
| **In-memory only** | Eliminates I/O bottleneck. Explicit trade-off: messages are lost if all replicas crash before delivery. |
| **Consistent hashing (FNV-32a, 150 vnodes)** | Deterministic ownership, O(log n) lookup, minimal reshuffling on node join/leave. |
| **At-least-once delivery** | Seq numbers + ACKs + retries. Exactly-once requires distributed transactions — out of scope. |
| **gRPC server-streaming for Subscribe** | Natural back-pressure from TCP flow control; no polling; bidirectional control plane for ACKs. |
| **Per-subscriber bounded channels** | Isolates slow consumers. Overflow policy is configurable: block / drop-oldest / disconnect. |
| **Epoch numbers on heartbeats** | Prevents split-brain: a stale leader with a lower epoch is ignored. |
| **Ring buffer per topic** | O(1) replay by sequence number, bounded memory, no disk I/O. |

## Project Layout

```
beacon/
├── cmd/
│   ├── broker/      # Broker node binary
│   ├── publisher/   # CLI publisher
│   ├── subscriber/  # CLI subscriber
│   └── bench/       # Load generator / benchmark
├── internal/
│   ├── broker/      # Core pub/sub engine
│   ├── cluster/     # Consistent hash ring + membership
│   ├── replication/ # Primary → replica sync  (Phase 3)
│   ├── config/      # 12-factor configuration
│   └── metrics/     # Prometheus instruments
├── proto/           # protobuf definitions + buf config
├── deploy/          # Dockerfile, docker-compose, prometheus
├── docs/            # Architecture diagrams
└── scripts/         # Helper scripts
```

## Quick Start

### Prerequisites

- Go 1.22+
- [buf](https://buf.build/docs/installation) (proto generation)
- Docker + Docker Compose (optional, for the 3-node cluster)

### 1. Generate proto code

```bash
make proto
```

### 2. Build & test

```bash
make build
make test
```

### 3. Run a 3-node cluster locally

```bash
make docker-up
# Prometheus: http://localhost:9090
# Grafana:    http://localhost:3000  (admin / beacon)
# Broker 1 metrics: http://localhost:9091/metrics
```

### 4. Single-node dev run (Phase 1+)

```bash
go run ./cmd/broker -config deploy/broker1.yaml
```

## Phase Roadmap

| # | Status | Description |
|---|--------|-------------|
| 0 | ✅ Done | Scaffold: module, proto, config, metrics, ring, CI |
| 1 | 🔜 | Single-node pub/sub over gRPC |
| 2 | 🔜 | Cluster routing (consistent hashing, forward on miss) |
| 3 | 🔜 | Replication (primary → replica) |
| 4 | 🔜 | Consumer groups (partition delivery among group members) |
| 5 | 🔜 | Ring buffer + at-least-once (seq nums, ACKs, retries) |
| 6 | 🔜 | Failure detection (heartbeats, epoch, replica promotion) |
| 7 | 🔜 | Backpressure policies (block / drop-oldest / disconnect) |
| 8 | 🔜 | Benchmarks + Grafana dashboards |

## Running Tests

```bash
# All tests with race detector (required before every commit)
go test -race ./...

# Verbose output for a single package
go test -race -v ./internal/cluster/...
```

## Known Limitations (Phase 0)

1. **No actual message delivery** — `Publish` and `Subscribe` are stubs.
2. **No gRPC server** — the broker binary only serves `/metrics` and `/healthz`.
3. **Proto code not committed** — run `make proto` to generate `proto/pubsubv1/`.
4. **No persistence** — this is by design; a node crash loses its in-memory state.
