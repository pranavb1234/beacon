// Package config loads broker configuration from an optional YAML file and
// BEACON_* environment variables. Environment variables always win so that
// Docker / Kubernetes deployments can inject addresses and secrets without
// touching committed config files (12-factor principle).
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// BackpressurePolicy controls what happens when a subscriber's send buffer is full.
type BackpressurePolicy string

const (
	// PolicyBlock makes the broker wait until the subscriber drains its buffer.
	// Safe for slow consumers but can stall the entire fan-out for a topic.
	PolicyBlock BackpressurePolicy = "block"

	// PolicyDropOldest evicts the oldest undelivered message to make room.
	// Preserves freshness at the cost of at-least-once guarantee for slow subs.
	PolicyDropOldest BackpressurePolicy = "drop-oldest"

	// PolicyDisconnect forcibly closes the subscriber connection.
	// Harsh but prevents one slow subscriber from hurting the whole cluster.
	PolicyDisconnect BackpressurePolicy = "disconnect"
)

// Config holds all tuneable knobs for one broker node.
// Fields are tagged for both YAML unmarshalling and documentation.
type Config struct {
	// NodeID uniquely identifies this broker in the cluster.
	// Used as the key in the consistent hash ring and in log output.
	NodeID string `yaml:"node_id"`

	// AdvertiseAddr is the address other brokers use to reach this node over gRPC.
	// May differ from GRPCAddr when running behind a load-balancer or in Docker.
	AdvertiseAddr string `yaml:"advertise_addr"`

	// GRPCAddr is the local listen address for the gRPC server.
	GRPCAddr string `yaml:"grpc_addr"`

	// Peers is the seed list of broker addresses known at startup.
	// After a node joins, the cluster layer maintains live membership itself.
	Peers []string `yaml:"peers"`

	// RingVirtualNodes is the number of virtual nodes per real node in the
	// consistent hash ring. More vnodes → more even distribution but more memory.
	// 150 is a reasonable default for clusters of ≤ 10 nodes.
	RingVirtualNodes int `yaml:"ring_virtual_nodes"`

	// BufferSize is the capacity of the per-topic in-memory ring buffer.
	// Messages older than BufferSize are silently evicted — there is no disk fallback.
	BufferSize int `yaml:"buffer_size"`

	// BackpressurePolicy determines behaviour when a subscriber cannot keep up.
	BackpressurePolicy BackpressurePolicy `yaml:"backpressure_policy"`

	// HeartbeatInterval is how often this node sends heartbeats to its peers.
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`

	// HeartbeatTimeout is the silence window after which a peer is declared dead.
	HeartbeatTimeout time.Duration `yaml:"heartbeat_timeout"`

	// MetricsAddr is the HTTP listen address for the Prometheus /metrics endpoint.
	MetricsAddr string `yaml:"metrics_addr"`
}

// defaults returns a Config with safe, production-ready values so the broker
// can start with zero configuration for local development.
func defaults() *Config {
	return &Config{
		NodeID:             "node1",
		AdvertiseAddr:      "localhost:9001",
		GRPCAddr:           ":9001",
		Peers:              []string{},
		RingVirtualNodes:   150,
		BufferSize:         10_000,
		BackpressurePolicy: PolicyBlock,
		HeartbeatInterval:  1 * time.Second,
		HeartbeatTimeout:   5 * time.Second,
		MetricsAddr:        ":9091",
	}
}

// Load builds a Config by:
//  1. Starting from safe defaults.
//  2. Unmarshalling path (skipped when path is "").
//  3. Overlaying BEACON_* environment variables.
//  4. Validating the final result.
func Load(path string) (*Config, error) {
	cfg := defaults()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("config: read %q: %w", path, err)
		}
		if err := yaml.Unmarshal(data, cfg); err != nil {
			return nil, fmt.Errorf("config: parse yaml: %w", err)
		}
	}

	applyEnv(cfg)

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// applyEnv overlays BEACON_* environment variables onto cfg.
// Parsing errors are silently ignored so a bad env var doesn't crash the
// process — the original value from the file (or default) is kept.
func applyEnv(cfg *Config) {
	if v := os.Getenv("BEACON_NODE_ID"); v != "" {
		cfg.NodeID = v
	}
	if v := os.Getenv("BEACON_ADVERTISE_ADDR"); v != "" {
		cfg.AdvertiseAddr = v
	}
	if v := os.Getenv("BEACON_GRPC_ADDR"); v != "" {
		cfg.GRPCAddr = v
	}
	if v := os.Getenv("BEACON_PEERS"); v != "" {
		cfg.Peers = strings.Split(v, ",")
	}
	if v := os.Getenv("BEACON_RING_VNODES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.RingVirtualNodes = n
		}
	}
	if v := os.Getenv("BEACON_BUFFER_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.BufferSize = n
		}
	}
	if v := os.Getenv("BEACON_BACKPRESSURE_POLICY"); v != "" {
		cfg.BackpressurePolicy = BackpressurePolicy(v)
	}
	if v := os.Getenv("BEACON_HEARTBEAT_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.HeartbeatInterval = d
		}
	}
	if v := os.Getenv("BEACON_HEARTBEAT_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.HeartbeatTimeout = d
		}
	}
	if v := os.Getenv("BEACON_METRICS_ADDR"); v != "" {
		cfg.MetricsAddr = v
	}
}

// validate returns an error if cfg contains obviously invalid values.
// It is intentionally strict so misconfigurations surface at startup, not
// hours later when a code path finally exercises the broken value.
func (c *Config) validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("config: node_id must not be empty")
	}
	if c.RingVirtualNodes <= 0 {
		return fmt.Errorf("config: ring_virtual_nodes must be positive, got %d", c.RingVirtualNodes)
	}
	if c.BufferSize <= 0 {
		return fmt.Errorf("config: buffer_size must be positive, got %d", c.BufferSize)
	}
	switch c.BackpressurePolicy {
	case PolicyBlock, PolicyDropOldest, PolicyDisconnect:
		// valid
	default:
		return fmt.Errorf("config: unknown backpressure_policy %q (want block|drop-oldest|disconnect)",
			c.BackpressurePolicy)
	}
	return nil
}
