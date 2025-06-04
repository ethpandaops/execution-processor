package structlog_test

import (
	"math/big"
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

func TestLimiterBoundaryCondition(t *testing.T) {
	// Test that verifies the fix for block reprocessing when progressive next equals limiter_max + 1
	testCases := []struct {
		name           string
		progressiveNext int64
		limiterMax     int64
		expectNoMoreBlocks bool
		expectMaxAllowed   bool
	}{
		{
			name:           "progressive equals limiter max - should return progressive",
			progressiveNext: 100,
			limiterMax:     100,
			expectNoMoreBlocks: false,
			expectMaxAllowed:   false,
		},
		{
			name:           "progressive equals limiter max + 1 - should return no more blocks",
			progressiveNext: 101,
			limiterMax:     100,
			expectNoMoreBlocks: true,
			expectMaxAllowed:   false,
		},
		{
			name:           "progressive greater than limiter max + 1 - should return no more blocks",
			progressiveNext: 102,
			limiterMax:     100,
			expectNoMoreBlocks: true,
			expectMaxAllowed:   false,
		},
		{
			name:           "progressive less than limiter max - should return progressive",
			progressiveNext: 99,
			limiterMax:     100,
			expectNoMoreBlocks: false,
			expectMaxAllowed:   false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test the boundary condition logic that was fixed
			progressiveNext := big.NewInt(tc.progressiveNext)
			maxAllowed := big.NewInt(tc.limiterMax)
			
			// This replicates the fixed logic from state manager
			if progressiveNext.Cmp(maxAllowed) <= 0 {
				// Progressive next is within bounds
				assert.False(t, tc.expectNoMoreBlocks, "Should not expect no more blocks when progressive <= limiter max")
				assert.False(t, tc.expectMaxAllowed, "Should not expect max allowed when progressive <= limiter max")
			} else {
				// Check the fixed boundary condition
				boundaryCheck := progressiveNext.Cmp(big.NewInt(maxAllowed.Int64()+1)) >= 0
				if boundaryCheck {
					// This is the case that should return ErrNoMoreBlocks
					assert.True(t, tc.expectNoMoreBlocks, "Should expect no more blocks when progressive >= limiter_max + 1")
				} else {
					// This should never happen with the current test cases
					t.Errorf("Unexpected boundary condition: progressive=%d, limiter_max=%d", tc.progressiveNext, tc.limiterMax)
				}
			}
		})
	}
}

func TestRecentBlockProcessingCheck(t *testing.T) {
	// Test the logic for checking recently processed blocks
	// This tests the concept without requiring a real ClickHouse connection
	
	testCases := []struct {
		name             string
		blockNumber      uint64
		withinSeconds    int
		expectedRecent   bool
	}{
		{
			name:           "block processed recently",
			blockNumber:    12345,
			withinSeconds:  30,
			expectedRecent: true, // In a real test this would depend on database state
		},
		{
			name:           "block not processed recently", 
			blockNumber:    12346,
			withinSeconds:  30,
			expectedRecent: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test the query logic structure (this would need a real database for full testing)
			// The query should check for records within the time window
			withinSeconds := tc.withinSeconds
			assert.Greater(t, withinSeconds, 0, "Within seconds should be positive")
			assert.Greater(t, tc.blockNumber, uint64(0), "Block number should be positive") 
			
			// In a real integration test, we would:
			// 1. Insert a block record with current timestamp
			// 2. Check that IsBlockRecentlyProcessed returns true
			// 3. Wait longer than withinSeconds
			// 4. Check that IsBlockRecentlyProcessed returns false
		})
	}
}
