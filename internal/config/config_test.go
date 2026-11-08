package config

import (
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestDefaults verifies that Load("") returns a valid config without
// any files or env vars — the broker must be runnable out of the box.
func TestDefaults(t *testing.T) {
	cfg, err := Load("")
	require.NoError(t, err)

	assert.Equal(t, "node1", cfg.NodeID)
	assert.Equal(t, ":9001", cfg.GRPCAddr)
	assert.Equal(t, 150, cfg.RingVirtualNodes)
	assert.Equal(t, 10_000, cfg.BufferSize)
	assert.Equal(t, PolicyBlock, cfg.BackpressurePolicy)
	assert.Equal(t, 1*time.Second, cfg.HeartbeatInterval)
	assert.Equal(t, 5*time.Second, cfg.HeartbeatTimeout)
	assert.Equal(t, ":9091", cfg.MetricsAddr)
}

// TestYAMLLoad verifies that values in a YAML file override defaults.
func TestYAMLLoad(t *testing.T) {
	content := `
node_id: yaml-node
grpc_addr: ":9999"
ring_virtual_nodes: 50
buffer_size: 500
backpressure_policy: disconnect
heartbeat_interval: 2s
heartbeat_timeout: 10s
peers:
  - "peer1:9001"
  - "peer2:9001"
`
	f := writeTempYAML(t, content)

	cfg, err := Load(f)
	require.NoError(t, err)

	assert.Equal(t, "yaml-node", cfg.NodeID)
	assert.Equal(t, ":9999", cfg.GRPCAddr)
	assert.Equal(t, 50, cfg.RingVirtualNodes)
	assert.Equal(t, 500, cfg.BufferSize)
	assert.Equal(t, PolicyDisconnect, cfg.BackpressurePolicy)
	assert.Equal(t, 2*time.Second, cfg.HeartbeatInterval)
	assert.Equal(t, 10*time.Second, cfg.HeartbeatTimeout)
	assert.Equal(t, []string{"peer1:9001", "peer2:9001"}, cfg.Peers)
}

// TestEnvOverride verifies that BEACON_* env vars override defaults.
func TestEnvOverride(t *testing.T) {
	t.Setenv("BEACON_NODE_ID", "env-node")
	t.Setenv("BEACON_RING_VNODES", "200")
	t.Setenv("BEACON_BUFFER_SIZE", "5000")
	t.Setenv("BEACON_BACKPRESSURE_POLICY", "drop-oldest")
	t.Setenv("BEACON_HEARTBEAT_INTERVAL", "500ms")
	t.Setenv("BEACON_PEERS", "a:9001,b:9001")

	cfg, err := Load("")
	require.NoError(t, err)

	assert.Equal(t, "env-node", cfg.NodeID)
	assert.Equal(t, 200, cfg.RingVirtualNodes)
	assert.Equal(t, 5000, cfg.BufferSize)
	assert.Equal(t, PolicyDropOldest, cfg.BackpressurePolicy)
	assert.Equal(t, 500*time.Millisecond, cfg.HeartbeatInterval)
	assert.Equal(t, []string{"a:9001", "b:9001"}, cfg.Peers)
}

// TestEnvWinsOverYAML verifies 12-factor layering: env beats file.
func TestEnvWinsOverYAML(t *testing.T) {
	f := writeTempYAML(t, "node_id: yaml-node\n")
	t.Setenv("BEACON_NODE_ID", "env-wins")

	cfg, err := Load(f)
	require.NoError(t, err)
	assert.Equal(t, "env-wins", cfg.NodeID)
}

// TestMissingFile verifies that a non-existent path returns an error.
func TestMissingFile(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	assert.Error(t, err)
}

// TestBadYAML verifies that malformed YAML returns an error.
func TestBadYAML(t *testing.T) {
	f := writeTempYAML(t, "node_id: [bad yaml\n")
	_, err := Load(f)
	assert.Error(t, err)
}

// TestValidation exercises validate() directly.
func TestValidation(t *testing.T) {
	t.Run("empty node id", func(t *testing.T) {
		cfg := defaults()
		cfg.NodeID = ""
		assert.Error(t, cfg.validate())
	})

	t.Run("zero ring vnodes", func(t *testing.T) {
		cfg := defaults()
		cfg.RingVirtualNodes = 0
		assert.Error(t, cfg.validate())
	})

	t.Run("negative buffer size", func(t *testing.T) {
		cfg := defaults()
		cfg.BufferSize = -1
		assert.Error(t, cfg.validate())
	})

	t.Run("unknown policy", func(t *testing.T) {
		cfg := defaults()
		cfg.BackpressurePolicy = "banana"
		assert.Error(t, cfg.validate())
	})

	t.Run("all three policies accepted", func(t *testing.T) {
		for _, p := range []BackpressurePolicy{PolicyBlock, PolicyDropOldest, PolicyDisconnect} {
			cfg := defaults()
			cfg.BackpressurePolicy = p
			assert.NoError(t, cfg.validate(), "policy %s should be valid", p)
		}
	})
}

// writeTempYAML creates a temp file with content and returns its path.
func writeTempYAML(t *testing.T, content string) string {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "beacon-*.yaml")
	require.NoError(t, err)
	_, err = f.WriteString(content)
	require.NoError(t, err)
	require.NoError(t, f.Close())
	return f.Name()
}
