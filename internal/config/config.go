// Package config loads broker configuration from a YAML file or environment
// variables. Environment variables override file settings to follow 12-factor
// configuration principles.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// BackpressurePolicy defines what happens when a subscriber's buffer is full.
type BackpressurePolicy string

const (
	PolicyBlock      BackpressurePolicy = "block"
	PolicyDropOldest BackpressurePolicy = "drop-oldest"
	PolicyDisconnect BackpressurePolicy = "disconnect"
)

// ReplicationMode specifies the cluster replication strategy.
type ReplicationMode string

const (
	ReplicationModeSync  ReplicationMode = "sync"
	ReplicationModeAsync ReplicationMode = "async"
)

// PeerConfig represents a remote peer node in the cluster with its ID and address.
type PeerConfig struct {
	ID      string `yaml:"id"`
	Address string `yaml:"address"`
}

// Config holds all tuneable settings for a pub/sub broker node.
type Config struct {
	// NodeID uniquely identifies this broker node in the cluster.
	NodeID string `yaml:"node_id"`

	// ListenAddr is the network address the broker listens on (e.g., ":9001" or "0.0.0.0:9001").
	ListenAddr string `yaml:"listen_address"`

	// Peers is the list of cluster peer nodes (id and address).
	Peers []PeerConfig `yaml:"peers"`

	// HeartbeatInterval is the interval at which heartbeats are sent to peers.
	HeartbeatInterval time.Duration `yaml:"heartbeat_interval"`

	// HeartbeatTimeout is the duration of silence before a peer is considered dead.
	HeartbeatTimeout time.Duration `yaml:"heartbeat_timeout"`

	// BufferSizePerTopic is the ring buffer capacity for each topic.
	BufferSizePerTopic int `yaml:"buffer_size_per_topic"`

	// SubscriberBufferSize is the bounded queue size for each subscriber connection.
	SubscriberBufferSize int `yaml:"subscriber_buffer_size"`

	// AckTimeout is the time to wait for a subscriber ACK before retrying message delivery.
	AckTimeout time.Duration `yaml:"ack_timeout"`

	// MaxRetries is the maximum number of redelivery attempts before giving up.
	MaxRetries int `yaml:"max_retries"`

	// BackpressurePolicy specifies the policy when subscriber buffer overflows (block, drop-oldest, disconnect).
	BackpressurePolicy BackpressurePolicy `yaml:"backpressure_policy"`

	// BlockTimeout is the maximum time a publisher waits in PolicyBlock before abandoning delivery to a full buffer.
	BlockTimeout time.Duration `yaml:"block_timeout,omitempty"`

	// ReplicationMode specifies whether replication across brokers is sync or async.
	ReplicationMode ReplicationMode `yaml:"replication_mode"`

	// RingVirtualNodes is the number of virtual nodes on the consistent hash ring per physical node.
	RingVirtualNodes int `yaml:"ring_virtual_nodes,omitempty"`

	// MetricsAddr is the address for serving Prometheus metrics.
	MetricsAddr string `yaml:"metrics_addr,omitempty"`
}

// rawYAMLConfig supports alternative key aliases in YAML files.
type rawYAMLConfig struct {
	NodeID               string             `yaml:"node_id"`
	ListenAddress        string             `yaml:"listen_address"`
	ListenAddr           string             `yaml:"listen_addr"`
	GRPCAddr             string             `yaml:"grpc_addr"`
	Peers                []PeerConfig       `yaml:"peers"`
	HeartbeatInterval    time.Duration      `yaml:"heartbeat_interval"`
	HeartbeatTimeout     time.Duration      `yaml:"heartbeat_timeout"`
	BufferSizePerTopic   int                `yaml:"buffer_size_per_topic"`
	BufferSize           int                `yaml:"buffer_size"`
	SubscriberBufferSize int                `yaml:"subscriber_buffer_size"`
	AckTimeout           time.Duration      `yaml:"ack_timeout"`
	MaxRetries           int                `yaml:"max_retries"`
	BackpressurePolicy   BackpressurePolicy `yaml:"backpressure_policy"`
	BlockTimeout         time.Duration      `yaml:"block_timeout"`
	ReplicationMode      ReplicationMode    `yaml:"replication_mode"`
	RingVirtualNodes     int                `yaml:"ring_virtual_nodes"`
	MetricsAddr          string             `yaml:"metrics_addr"`
}

// Default returns a Config with safe default settings.
func Default() *Config {
	return &Config{
		NodeID:               "node-1",
		ListenAddr:           ":9001",
		Peers:                []PeerConfig{},
		HeartbeatInterval:    1 * time.Second,
		HeartbeatTimeout:     5 * time.Second,
		BufferSizePerTopic:   10_000,
		SubscriberBufferSize: 1_000,
		AckTimeout:           3 * time.Second,
		MaxRetries:           3,
		BackpressurePolicy:   PolicyBlock,
		BlockTimeout:         500 * time.Millisecond,
		ReplicationMode:      ReplicationModeAsync,
		RingVirtualNodes:     150,
		MetricsAddr:          ":9091",
	}
}

// Load loads configuration from a YAML file (if provided) and overlays environment variables.
func Load(path string) (*Config, error) {
	cfg := Default()

	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("config: read %q: %w", path, err)
		}

		var raw rawYAMLConfig
		if err := yaml.Unmarshal(data, &raw); err != nil {
			return nil, fmt.Errorf("config: parse yaml: %w", err)
		}

		applyYAML(cfg, &raw)
	}

	applyEnv(cfg)

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func applyYAML(cfg *Config, raw *rawYAMLConfig) {
	if raw.NodeID != "" {
		cfg.NodeID = raw.NodeID
	}
	if raw.ListenAddress != "" {
		cfg.ListenAddr = raw.ListenAddress
	} else if raw.ListenAddr != "" {
		cfg.ListenAddr = raw.ListenAddr
	} else if raw.GRPCAddr != "" {
		cfg.ListenAddr = raw.GRPCAddr
	}
	if raw.Peers != nil {
		cfg.Peers = raw.Peers
	}
	if raw.HeartbeatInterval > 0 {
		cfg.HeartbeatInterval = raw.HeartbeatInterval
	}
	if raw.HeartbeatTimeout > 0 {
		cfg.HeartbeatTimeout = raw.HeartbeatTimeout
	}
	if raw.BufferSizePerTopic > 0 {
		cfg.BufferSizePerTopic = raw.BufferSizePerTopic
	} else if raw.BufferSize > 0 {
		cfg.BufferSizePerTopic = raw.BufferSize
	}
	if raw.SubscriberBufferSize > 0 {
		cfg.SubscriberBufferSize = raw.SubscriberBufferSize
	}
	if raw.AckTimeout > 0 {
		cfg.AckTimeout = raw.AckTimeout
	}
	if raw.MaxRetries != 0 {
		cfg.MaxRetries = raw.MaxRetries
	}
	if raw.BackpressurePolicy != "" {
		cfg.BackpressurePolicy = NormalizePolicy(string(raw.BackpressurePolicy))
	}
	if raw.BlockTimeout > 0 {
		cfg.BlockTimeout = raw.BlockTimeout
	}
	if raw.ReplicationMode != "" {
		cfg.ReplicationMode = raw.ReplicationMode
	}
	if raw.RingVirtualNodes > 0 {
		cfg.RingVirtualNodes = raw.RingVirtualNodes
	}
	if raw.MetricsAddr != "" {
		cfg.MetricsAddr = raw.MetricsAddr
	}
}

// NormalizePolicy normalizes backpressure policy strings (handling hyphen and underscore variants).
func NormalizePolicy(p string) BackpressurePolicy {
	switch strings.ToLower(strings.TrimSpace(p)) {
	case "block":
		return PolicyBlock
	case "drop-oldest", "drop_oldest":
		return PolicyDropOldest
	case "disconnect":
		return PolicyDisconnect
	default:
		return BackpressurePolicy(p)
	}
}

// applyEnv overlays environment variables onto the configuration.
func applyEnv(cfg *Config) {
	if v := getEnv("PUBSUB_NODE_ID", "BEACON_NODE_ID"); v != "" {
		cfg.NodeID = v
	}
	if v := getEnv("PUBSUB_LISTEN_ADDRESS", "PUBSUB_LISTEN_ADDR", "BEACON_LISTEN_ADDR", "BEACON_GRPC_ADDR"); v != "" {
		cfg.ListenAddr = v
	}
	if v := getEnv("PUBSUB_PEERS", "BEACON_PEERS"); v != "" {
		cfg.Peers = parsePeersEnv(v)
	}
	if v := getEnv("PUBSUB_HEARTBEAT_INTERVAL", "BEACON_HEARTBEAT_INTERVAL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.HeartbeatInterval = d
		}
	}
	if v := getEnv("PUBSUB_HEARTBEAT_TIMEOUT", "BEACON_HEARTBEAT_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.HeartbeatTimeout = d
		}
	}
	if v := getEnv("PUBSUB_BUFFER_SIZE_PER_TOPIC", "PUBSUB_BUFFER_SIZE", "BEACON_BUFFER_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.BufferSizePerTopic = n
		}
	}
	if v := getEnv("PUBSUB_SUBSCRIBER_BUFFER_SIZE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.SubscriberBufferSize = n
		}
	}
	if v := getEnv("PUBSUB_ACK_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.AckTimeout = d
		}
	}
	if v := getEnv("PUBSUB_MAX_RETRIES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.MaxRetries = n
		}
	}
	if v := getEnv("PUBSUB_BACKPRESSURE_POLICY", "BEACON_BACKPRESSURE_POLICY"); v != "" {
		cfg.BackpressurePolicy = NormalizePolicy(v)
	}
	if v := getEnv("PUBSUB_BLOCK_TIMEOUT", "BEACON_BLOCK_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			cfg.BlockTimeout = d
		}
	}
	if v := getEnv("PUBSUB_REPLICATION_MODE"); v != "" {
		cfg.ReplicationMode = ReplicationMode(v)
	}
	if v := getEnv("PUBSUB_RING_VNODES", "BEACON_RING_VNODES"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			cfg.RingVirtualNodes = n
		}
	}
	if v := getEnv("PUBSUB_METRICS_ADDR", "BEACON_METRICS_ADDR"); v != "" {
		cfg.MetricsAddr = v
	}
}

func getEnv(keys ...string) string {
	for _, key := range keys {
		if v := os.Getenv(key); v != "" {
			return v
		}
	}
	return ""
}

// parsePeersEnv parses comma-separated peers in "id=address" or "address" format.
func parsePeersEnv(raw string) []PeerConfig {
	parts := strings.Split(raw, ",")
	peers := make([]PeerConfig, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if id, addr, found := strings.Cut(p, "="); found {
			peers = append(peers, PeerConfig{ID: strings.TrimSpace(id), Address: strings.TrimSpace(addr)})
		} else {
			peers = append(peers, PeerConfig{ID: p, Address: p})
		}
	}
	return peers
}

// Validate verifies that the configuration values are sound and consistent.
func (c *Config) Validate() error {
	if strings.TrimSpace(c.NodeID) == "" {
		return fmt.Errorf("config: node_id must not be empty")
	}
	if strings.TrimSpace(c.ListenAddr) == "" {
		return fmt.Errorf("config: listen_address must not be empty")
	}
	if c.HeartbeatInterval <= 0 {
		return fmt.Errorf("config: heartbeat_interval must be positive, got %v", c.HeartbeatInterval)
	}
	if c.HeartbeatTimeout <= 0 {
		return fmt.Errorf("config: heartbeat_timeout must be positive, got %v", c.HeartbeatTimeout)
	}
	if c.HeartbeatTimeout <= c.HeartbeatInterval {
		return fmt.Errorf("config: heartbeat_timeout (%v) must be greater than heartbeat_interval (%v)",
			c.HeartbeatTimeout, c.HeartbeatInterval)
	}
	if c.BufferSizePerTopic <= 0 {
		return fmt.Errorf("config: buffer_size_per_topic must be positive, got %d", c.BufferSizePerTopic)
	}
	if c.SubscriberBufferSize <= 0 {
		return fmt.Errorf("config: subscriber_buffer_size must be positive, got %d", c.SubscriberBufferSize)
	}
	if c.AckTimeout <= 0 {
		return fmt.Errorf("config: ack_timeout must be positive, got %v", c.AckTimeout)
	}
	if c.MaxRetries < 0 {
		return fmt.Errorf("config: max_retries must be non-negative, got %d", c.MaxRetries)
	}
	c.BackpressurePolicy = NormalizePolicy(string(c.BackpressurePolicy))
	switch c.BackpressurePolicy {
	case PolicyBlock, PolicyDropOldest, PolicyDisconnect:
	default:
		return fmt.Errorf("config: unknown backpressure_policy %q (must be block, drop-oldest, or disconnect)",
			c.BackpressurePolicy)
	}
	if c.BlockTimeout <= 0 {
		c.BlockTimeout = 500 * time.Millisecond
	}
	switch c.ReplicationMode {
	case ReplicationModeSync, ReplicationModeAsync:
	default:
		return fmt.Errorf("config: unknown replication_mode %q (must be sync or async)",
			c.ReplicationMode)
	}
	return nil
}
