package structlog_test

import (
	"testing"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
	transaction_structlog "github.com/ethpandaops/execution-processor/pkg/processor/transaction/structlog"
	"github.com/stretchr/testify/assert"
)

func TestProcessor_Creation(t *testing.T) {
	cfg := transaction_structlog.Config{
		Enabled:   true,
		Table:     "test_structlog",
		BatchSize: 10,
		Config: clickhouse.Config{
			DSN:          "clickhouse://localhost:9000/test",
			MaxOpenConns: 10,
			MaxIdleConns: 5,
		},
	}

	// Test config validation
	err := cfg.Validate()
	assert.NoError(t, err)
}

func TestProcessor_ConfigValidation(t *testing.T) {
	testCases := []struct {
		name        string
		config      transaction_structlog.Config
		expectError bool
	}{
		{
			name: "valid config",
			config: transaction_structlog.Config{
				Enabled:   true,
				Table:     "test_table",
				BatchSize: 100,
				Config: clickhouse.Config{
					DSN:          "clickhouse://localhost:9000/test",
					MaxOpenConns: 10,
					MaxIdleConns: 5,
				},
			},
			expectError: false,
		},
		{
			name: "disabled config",
			config: transaction_structlog.Config{
				Enabled: false,
			},
			expectError: false,
		},
		{
			name: "missing DSN",
			config: transaction_structlog.Config{
				Enabled:   true,
				Table:     "test_table",
				BatchSize: 100,
			},
			expectError: true,
		},
		{
			name: "missing table",
			config: transaction_structlog.Config{
				Enabled:   true,
				BatchSize: 100,
				Config: clickhouse.Config{
					DSN: "clickhouse://localhost:9000/test",
				},
			},
			expectError: true,
		},
		{
			name: "invalid batch size",
			config: transaction_structlog.Config{
				Enabled:   true,
				Table:     "test_table",
				BatchSize: 0,
				Config: clickhouse.Config{
					DSN: "clickhouse://localhost:9000/test",
				},
			},
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.config.Validate()
			if tc.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestProcessor_ConcurrentConfigValidation(t *testing.T) {
	cfg := transaction_structlog.Config{
		Enabled:   true,
		Table:     "test_concurrent",
		BatchSize: 50,
		Config: clickhouse.Config{
			DSN:          "clickhouse://localhost:9000/test",
			MaxOpenConns: 10,
			MaxIdleConns: 5,
		},
	}

	// Test concurrent validation calls
	const numGoroutines = 10
	results := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			results <- cfg.Validate()
		}()
	}

	// All validations should succeed
	for i := 0; i < numGoroutines; i++ {
		err := <-results
		assert.NoError(t, err)
	}
}
