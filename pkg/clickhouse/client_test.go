package clickhouse

import (
	"context"
	"errors"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/compress"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_Validate(t *testing.T) {
	tests := []struct {
		name        string
		config      Config
		expectError bool
	}{
		{
			name: "valid config with addr",
			config: Config{
				Addr: "localhost:9000",
			},
			expectError: false,
		},
		{
			name:        "missing addr",
			config:      Config{},
			expectError: true,
		},
		{
			name: "valid config with all fields",
			config: Config{
				Addr:              "localhost:9000",
				Database:          "test_db",
				Username:          "default",
				Password:          "secret",
				MaxConns:          20,
				MinConns:          5,
				ConnMaxLifetime:   2 * time.Hour,
				ConnMaxIdleTime:   1 * time.Hour,
				HealthCheckPeriod: 30 * time.Second,
				Compression:       "zstd",
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.Validate()
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestConfig_SetDefaults(t *testing.T) {
	config := Config{
		Addr: "localhost:9000",
	}

	config.SetDefaults()

	assert.Equal(t, "default", config.Database)
	assert.Equal(t, int32(10), config.MaxConns)
	assert.Equal(t, int32(2), config.MinConns)
	assert.Equal(t, time.Hour, config.ConnMaxLifetime)
	assert.Equal(t, 30*time.Minute, config.ConnMaxIdleTime)
	assert.Equal(t, time.Minute, config.HealthCheckPeriod)
	assert.Equal(t, 10*time.Second, config.DialTimeout)
	assert.Equal(t, "lz4", config.Compression)
	assert.Equal(t, 60*time.Second, config.QueryTimeout)
	assert.Equal(t, 10*time.Second, config.RetryMaxDelay)
}

func TestConfig_SetDefaults_PreservesValues(t *testing.T) {
	config := Config{
		Addr:              "localhost:9000",
		Database:          "custom_db",
		MaxConns:          50,
		MinConns:          10,
		ConnMaxLifetime:   3 * time.Hour,
		ConnMaxIdleTime:   2 * time.Hour,
		HealthCheckPeriod: 5 * time.Minute,
		DialTimeout:       30 * time.Second,
		Compression:       "zstd",
		QueryTimeout:      120 * time.Second,
		RetryMaxDelay:     30 * time.Second,
	}

	config.SetDefaults()

	// Should preserve custom values
	assert.Equal(t, "custom_db", config.Database)
	assert.Equal(t, int32(50), config.MaxConns)
	assert.Equal(t, int32(10), config.MinConns)
	assert.Equal(t, 3*time.Hour, config.ConnMaxLifetime)
	assert.Equal(t, 2*time.Hour, config.ConnMaxIdleTime)
	assert.Equal(t, 5*time.Minute, config.HealthCheckPeriod)
	assert.Equal(t, 30*time.Second, config.DialTimeout)
	assert.Equal(t, "zstd", config.Compression)
	assert.Equal(t, 120*time.Second, config.QueryTimeout)
	assert.Equal(t, 30*time.Second, config.RetryMaxDelay)
}

func TestClient_withQueryTimeout(t *testing.T) {
	tests := []struct {
		name             string
		queryTimeout     time.Duration
		ctxHasDeadline   bool
		expectNewContext bool
	}{
		{
			name:             "applies timeout when no deadline exists",
			queryTimeout:     5 * time.Second,
			ctxHasDeadline:   false,
			expectNewContext: true,
		},
		{
			name:             "preserves existing deadline",
			queryTimeout:     5 * time.Second,
			ctxHasDeadline:   true,
			expectNewContext: false,
		},
		{
			name:             "no-op when timeout is zero",
			queryTimeout:     0,
			ctxHasDeadline:   false,
			expectNewContext: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{
				config: &Config{
					QueryTimeout: tt.queryTimeout,
				},
			}

			var ctx context.Context

			var originalCancel context.CancelFunc

			if tt.ctxHasDeadline {
				ctx, originalCancel = context.WithTimeout(context.Background(), 10*time.Second)
				defer originalCancel()
			} else {
				ctx = context.Background()
			}

			newCtx, cancel := client.withQueryTimeout(ctx)
			defer cancel()

			_, hasDeadline := newCtx.Deadline()

			if tt.expectNewContext {
				require.True(t, hasDeadline, "expected context to have deadline")
			} else if tt.ctxHasDeadline {
				require.True(t, hasDeadline, "expected original deadline to be preserved")
			} else {
				require.False(t, hasDeadline, "expected no deadline")
			}
		})
	}
}

// Integration tests - require CLICKHOUSE_ADDR environment variable

func TestClient_Integration_New(t *testing.T) {
	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("CLICKHOUSE_ADDR not set, skipping integration test")
	}

	cfg := &Config{
		Addr:        addr,
		Database:    "default",
		Compression: "lz4",
	}

	client, err := New(cfg)
	require.NoError(t, err)
	require.NotNil(t, client)

	err = client.Stop()
	require.NoError(t, err)
}

func TestClient_Integration_StartStop(t *testing.T) {
	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("CLICKHOUSE_ADDR not set, skipping integration test")
	}

	cfg := &Config{
		Addr:        addr,
		Database:    "default",
		Compression: "lz4",
		Network:     "test",
	}

	client, err := New(cfg)
	require.NoError(t, err)

	// Start should dial and connect successfully
	err = client.Start()
	require.NoError(t, err)

	// Stop should close pool
	err = client.Stop()
	require.NoError(t, err)
}

func TestClient_Integration_Execute(t *testing.T) {
	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("CLICKHOUSE_ADDR not set, skipping integration test")
	}

	cfg := &Config{
		Addr:        addr,
		Database:    "default",
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

func TestClient_Integration_QueryUInt64(t *testing.T) {
	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("CLICKHOUSE_ADDR not set, skipping integration test")
	}

	cfg := &Config{
		Addr:        addr,
		Database:    "default",
		Compression: "lz4",
		Network:     "test",
	}

	client, err := New(cfg)
	require.NoError(t, err)

	defer func() { _ = client.Stop() }()

	err = client.Start()
	require.NoError(t, err)

	result, err := client.QueryUInt64(t.Context(), "SELECT toUInt64(42) as value", "value")
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, uint64(42), *result)
}

func TestClient_Integration_IsStorageEmpty(t *testing.T) {
	addr := os.Getenv("CLICKHOUSE_ADDR")
	if addr == "" {
		t.Skip("CLICKHOUSE_ADDR not set, skipping integration test")
	}

	cfg := &Config{
		Addr:        addr,
		Database:    "default",
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

// mockNetError implements net.Error for testing.
type mockNetError struct {
	timeout   bool
	temporary bool
}

func (e *mockNetError) Error() string   { return "mock network error" }
func (e *mockNetError) Timeout() bool   { return e.timeout }
func (e *mockNetError) Temporary() bool { return e.temporary }

func TestIsRetryableError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		// Nil error
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		// Context errors - non-retryable
		{
			name:     "context canceled",
			err:      context.Canceled,
			expected: false,
		},
		{
			name:     "context deadline exceeded",
			err:      context.DeadlineExceeded,
			expected: false,
		},
		// ch-go sentinel errors - non-retryable
		{
			name:     "ch.ErrClosed",
			err:      ch.ErrClosed,
			expected: false,
		},
		{
			name:     "wrapped ch.ErrClosed",
			err:      errors.Join(errors.New("operation failed"), ch.ErrClosed),
			expected: false,
		},
		// ch-go server exceptions - retryable codes (using ch.Exception which implements error)
		{
			name:     "proto.ErrTimeoutExceeded",
			err:      &ch.Exception{Code: proto.ErrTimeoutExceeded, Message: "timeout"},
			expected: true,
		},
		{
			name:     "proto.ErrNoFreeConnection",
			err:      &ch.Exception{Code: proto.ErrNoFreeConnection, Message: "no free connection"},
			expected: true,
		},
		{
			name:     "proto.ErrTooManySimultaneousQueries",
			err:      &ch.Exception{Code: proto.ErrTooManySimultaneousQueries, Message: "rate limited"},
			expected: true,
		},
		{
			name:     "proto.ErrSocketTimeout",
			err:      &ch.Exception{Code: proto.ErrSocketTimeout, Message: "socket timeout"},
			expected: true,
		},
		{
			name:     "proto.ErrNetworkError",
			err:      &ch.Exception{Code: proto.ErrNetworkError, Message: "network error"},
			expected: true,
		},
		// ch-go server exceptions - non-retryable codes
		{
			name:     "proto.ErrBadArguments",
			err:      &ch.Exception{Code: proto.ErrBadArguments, Message: "bad arguments"},
			expected: false,
		},
		{
			name:     "proto.ErrUnknownTable",
			err:      &ch.Exception{Code: proto.ErrUnknownTable, Message: "unknown table"},
			expected: false,
		},
		// Data corruption - non-retryable
		{
			name:     "compress.CorruptedDataErr",
			err:      &compress.CorruptedDataErr{},
			expected: false,
		},
		// Network errors
		{
			name:     "network timeout error",
			err:      &mockNetError{timeout: true},
			expected: true,
		},
		{
			name:     "network non-timeout error",
			err:      &mockNetError{timeout: false},
			expected: false,
		},
		// Syscall errors - retryable
		{
			name:     "syscall.ECONNRESET",
			err:      syscall.ECONNRESET,
			expected: true,
		},
		{
			name:     "syscall.ECONNREFUSED",
			err:      syscall.ECONNREFUSED,
			expected: true,
		},
		{
			name:     "syscall.EPIPE",
			err:      syscall.EPIPE,
			expected: true,
		},
		{
			name:     "io.EOF",
			err:      io.EOF,
			expected: true,
		},
		{
			name:     "io.ErrUnexpectedEOF",
			err:      io.ErrUnexpectedEOF,
			expected: true,
		},
		// String pattern fallback - retryable
		{
			name:     "connection reset string",
			err:      errors.New("connection reset by peer"),
			expected: true,
		},
		{
			name:     "server overloaded string",
			err:      errors.New("server is overloaded"),
			expected: true,
		},
		{
			name:     "too many connections string",
			err:      errors.New("too many connections"),
			expected: true,
		},
		// Unknown errors - non-retryable
		{
			name:     "unknown error",
			err:      errors.New("some unknown error"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isRetryableError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// Ensure mockNetError implements net.Error.
var _ net.Error = (*mockNetError)(nil)
