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

	// Execute DDL - Create and drop a table
	err = client.Execute(t.Context(), `
		CREATE TABLE IF NOT EXISTS test_execute (
			id UInt64
		) ENGINE = Memory
	`)
	require.NoError(t, err)

	err = client.Execute(t.Context(), "DROP TABLE IF EXISTS test_execute")
	require.NoError(t, err)
}

func TestClient_Integration_Container_QueryUInt64(t *testing.T) {
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

	// QueryUInt64 returns a single UInt64 value
	result, err := client.QueryUInt64(t.Context(), "SELECT toUInt64(42) as value", "value")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, uint64(42), *result)
}

func TestClient_Integration_Container_QueryMinMaxUInt64(t *testing.T) {
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

	// QueryMinMaxUInt64 returns min and max values
	minVal, maxVal, err := client.QueryMinMaxUInt64(t.Context(), "SELECT toUInt64(10) as min, toUInt64(100) as max")
	require.NoError(t, err)
	require.NotNil(t, minVal)
	require.NotNil(t, maxVal)
	assert.Equal(t, uint64(10), *minVal)
	assert.Equal(t, uint64(100), *maxVal)
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

	// Create a table with ReplacingMergeTree engine (supports FINAL)
	err = client.Execute(t.Context(), `
		CREATE TABLE IF NOT EXISTS test_empty_check (
			id UInt64,
			name String
		) ENGINE = ReplacingMergeTree()
		ORDER BY id
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
