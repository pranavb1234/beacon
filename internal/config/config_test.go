package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaults(t *testing.T) {
	cfg, err := Load("")
	require.NoError(t, err)

	assert.Equal(t, "node-1", cfg.NodeID)
	assert.Equal(t, ":9001", cfg.ListenAddr)
	assert.Empty(t, cfg.Peers)
	assert.Equal(t, 1*time.Second, cfg.HeartbeatInterval)
	assert.Equal(t, 5*time.Second, cfg.HeartbeatTimeout)
	assert.Equal(t, 10_000, cfg.BufferSizePerTopic)
	assert.Equal(t, 1_000, cfg.SubscriberBufferSize)
	assert.Equal(t, 3*time.Second, cfg.AckTimeout)
	assert.Equal(t, 3, cfg.MaxRetries)
	assert.Equal(t, PolicyBlock, cfg.BackpressurePolicy)
	assert.Equal(t, ReplicationModeAsync, cfg.ReplicationMode)
}

func TestYAMLLoad(t *testing.T) {
	yamlContent := `
node_id: "broker-node-42"
listen_address: "127.0.0.1:9099"
peers:
  - id: "peer-2"
    address: "127.0.0.1:9092"
  - id: "peer-3"
    address: "127.0.0.1:9093"
heartbeat_interval: 500ms
heartbeat_timeout: 2s
buffer_size_per_topic: 20000
subscriber_buffer_size: 500
ack_timeout: 4s
max_retries: 5
backpressure_policy: "drop-oldest"
replication_mode: "sync"
`
	tmpFile := writeTempYAML(t, yamlContent)

	cfg, err := Load(tmpFile)
	require.NoError(t, err)

	assert.Equal(t, "broker-node-42", cfg.NodeID)
	assert.Equal(t, "127.0.0.1:9099", cfg.ListenAddr)
	assert.Len(t, cfg.Peers, 2)
	assert.Equal(t, "peer-2", cfg.Peers[0].ID)
	assert.Equal(t, "127.0.0.1:9092", cfg.Peers[0].Address)
	assert.Equal(t, "peer-3", cfg.Peers[1].ID)
	assert.Equal(t, "127.0.0.1:9093", cfg.Peers[1].Address)
	assert.Equal(t, 500*time.Millisecond, cfg.HeartbeatInterval)
	assert.Equal(t, 2*time.Second, cfg.HeartbeatTimeout)
	assert.Equal(t, 20_000, cfg.BufferSizePerTopic)
	assert.Equal(t, 500, cfg.SubscriberBufferSize)
	assert.Equal(t, 4*time.Second, cfg.AckTimeout)
	assert.Equal(t, 5, cfg.MaxRetries)
	assert.Equal(t, PolicyDropOldest, cfg.BackpressurePolicy)
	assert.Equal(t, ReplicationModeSync, cfg.ReplicationMode)
}

func TestEnvOverride(t *testing.T) {
	t.Setenv("PUBSUB_NODE_ID", "env-node-1")
	t.Setenv("PUBSUB_LISTEN_ADDRESS", "0.0.0.0:8000")
	t.Setenv("PUBSUB_PEERS", "p1=10.0.0.1:8000,p2=10.0.0.2:8000")
	t.Setenv("PUBSUB_HEARTBEAT_INTERVAL", "200ms")
	t.Setenv("PUBSUB_HEARTBEAT_TIMEOUT", "1s")
	t.Setenv("PUBSUB_BUFFER_SIZE_PER_TOPIC", "5000")
	t.Setenv("PUBSUB_SUBSCRIBER_BUFFER_SIZE", "250")
	t.Setenv("PUBSUB_ACK_TIMEOUT", "1s")
	t.Setenv("PUBSUB_MAX_RETRIES", "7")
	t.Setenv("PUBSUB_BACKPRESSURE_POLICY", "disconnect")
	t.Setenv("PUBSUB_REPLICATION_MODE", "sync")

	cfg, err := Load("")
	require.NoError(t, err)

	assert.Equal(t, "env-node-1", cfg.NodeID)
	assert.Equal(t, "0.0.0.0:8000", cfg.ListenAddr)
	assert.Equal(t, []PeerConfig{
		{ID: "p1", Address: "10.0.0.1:8000"},
		{ID: "p2", Address: "10.0.0.2:8000"},
	}, cfg.Peers)
	assert.Equal(t, 200*time.Millisecond, cfg.HeartbeatInterval)
	assert.Equal(t, 1*time.Second, cfg.HeartbeatTimeout)
	assert.Equal(t, 5_000, cfg.BufferSizePerTopic)
	assert.Equal(t, 250, cfg.SubscriberBufferSize)
	assert.Equal(t, 1*time.Second, cfg.AckTimeout)
	assert.Equal(t, 7, cfg.MaxRetries)
	assert.Equal(t, PolicyDisconnect, cfg.BackpressurePolicy)
	assert.Equal(t, ReplicationModeSync, cfg.ReplicationMode)
}

func TestEnvOverridesYAML(t *testing.T) {
	yamlContent := `
node_id: "from-yaml"
listen_address: "127.0.0.1:9001"
`
	tmpFile := writeTempYAML(t, yamlContent)
	t.Setenv("PUBSUB_NODE_ID", "from-env")

	cfg, err := Load(tmpFile)
	require.NoError(t, err)
	assert.Equal(t, "from-env", cfg.NodeID)
	assert.Equal(t, "127.0.0.1:9001", cfg.ListenAddr)
}

func TestValidationErrors(t *testing.T) {
	testCases := []struct {
		name    string
		mutate  func(c *Config)
		wantErr string
	}{
		{
			name: "empty node id",
			mutate: func(c *Config) {
				c.NodeID = "   "
			},
			wantErr: "node_id",
		},
		{
			name: "empty listen address",
			mutate: func(c *Config) {
				c.ListenAddr = ""
			},
			wantErr: "listen_address",
		},
		{
			name: "negative heartbeat interval",
			mutate: func(c *Config) {
				c.HeartbeatInterval = -1 * time.Second
			},
			wantErr: "heartbeat_interval",
		},
		{
			name: "heartbeat timeout <= interval",
			mutate: func(c *Config) {
				c.HeartbeatInterval = 5 * time.Second
				c.HeartbeatTimeout = 2 * time.Second
			},
			wantErr: "heartbeat_timeout",
		},
		{
			name: "non-positive buffer size per topic",
			mutate: func(c *Config) {
				c.BufferSizePerTopic = 0
			},
			wantErr: "buffer_size_per_topic",
		},
		{
			name: "non-positive subscriber buffer size",
			mutate: func(c *Config) {
				c.SubscriberBufferSize = 0
			},
			wantErr: "subscriber_buffer_size",
		},
		{
			name: "non-positive ack timeout",
			mutate: func(c *Config) {
				c.AckTimeout = 0
			},
			wantErr: "ack_timeout",
		},
		{
			name: "negative max retries",
			mutate: func(c *Config) {
				c.MaxRetries = -1
			},
			wantErr: "max_retries",
		},
		{
			name: "invalid backpressure policy",
			mutate: func(c *Config) {
				c.BackpressurePolicy = "unsupported"
			},
			wantErr: "backpressure_policy",
		},
		{
			name: "invalid replication mode",
			mutate: func(c *Config) {
				c.ReplicationMode = "semi-sync"
			},
			wantErr: "replication_mode",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := Default()
			tc.mutate(cfg)
			err := cfg.Validate()
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

func writeTempYAML(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "pubsub-*.yaml")
	require.NoError(t, err)
	_, err = f.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}
