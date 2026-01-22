//go:build integration

package clickhouse

import (
	"testing"

	"github.com/ethpandaops/execution-processor/internal/testutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Integration tests using testcontainers - run with: go test -tags=integration ./...

func TestClient_Integration_Container_New(t *testing.T) {
	conn := testutil.NewClickHouseContainer(t)

	cfg := &Config{
		Addr:        conn.Addr(),
		Database:    conn.Database,
		Username:    conn.Username,
		Password:    conn.Password,
		Compression: "lz4",
	}

	client, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, client)

	err = client.Stop()
	require.NoError(t, err)
}

func TestClient_Integration_Container_StartStop(t *testing.T) {
	conn := testutil.NewClickHouseContainer(t)

	cfg := &Config{
		Addr:        conn.Addr(),
		Database:    conn.Database,
		Username:    conn.Username,
		Password:    conn.Password,
		Compression: "lz4",
		Network:     "test",
	}

	client, err := New(cfg)
	require.NoError(t, err)

	// Start should ping successfully
	err = client.Start()
	require.NoError(t, err)

	// Stop should close pool
	err = client.Stop()
	require.NoError(t, err)
}

func TestClient_Integration_Container_Execute(t *testing.T) {
	conn := testutil.NewClickHouseContainer(t)

	cfg := &Config{
		Addr:        conn.Addr(),
		Database:    conn.Database,
		Username:    conn.Username,
		Password:    conn.Password,
		Compression: "lz4",
		Network:     "test",
	}

	client, err := New(cfg)
	require.NoError(t, err)

	defer func() { _ = client.Stop() }()

	err = client.Start()
	require.NoError(t, err)

	// Execute a simple query
	err = client.Execute(t.Context(), "SELECT 1")
	require.NoError(t, err)
}

func TestClient_Integration_Container_QueryOne(t *testing.T) {
	conn := testutil.NewClickHouseContainer(t)

	cfg := &Config{
		Addr:        conn.Addr(),
		Database:    conn.Database,
		Username:    conn.Username,
		Password:    conn.Password,
		Compression: "lz4",
		Network:     "test",
	}

	client, err := New(cfg)
	require.NoError(t, err)

	defer func() { _ = client.Stop() }()

	err = client.Start()
	require.NoError(t, err)

	var result struct {
		Value int64 `json:"value"`
	}

	err = client.QueryOne(t.Context(), "SELECT 42 as value", &result)
	require.NoError(t, err)
	assert.Equal(t, int64(42), result.Value)
}

func TestClient_Integration_Container_IsStorageEmpty(t *testing.T) {
	conn := testutil.NewClickHouseContainer(t)

	cfg := &Config{
		Addr:        conn.Addr(),
		Database:    conn.Database,
		Username:    conn.Username,
		Password:    conn.Password,
		Compression: "lz4",
		Network:     "test",
	}

	client, err := New(cfg)
	require.NoError(t, err)

	defer func() { _ = client.Stop() }()

	err = client.Start()
	require.NoError(t, err)

	// Create a temporary table
	err = client.Execute(t.Context(), `
		CREATE TABLE IF NOT EXISTS test_empty_check (
			id UInt64,
			name String
		) ENGINE = Memory
	`)
	require.NoError(t, err)

	// Should be empty
	isEmpty, err := client.IsStorageEmpty(t.Context(), "test_empty_check", nil)
	require.NoError(t, err)
	assert.True(t, isEmpty)

	// Drop the table
	err = client.Execute(t.Context(), "DROP TABLE IF EXISTS test_empty_check")
	require.NoError(t, err)
}
