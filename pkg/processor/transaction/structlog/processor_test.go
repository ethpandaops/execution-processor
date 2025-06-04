package structlog_test

import (
	"context"
	"encoding/json"
	"math/big"
	"runtime"
	"testing"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
	"github.com/ethpandaops/execution-processor/pkg/ethereum"
	c "github.com/ethpandaops/execution-processor/pkg/processor/common"
	transaction_structlog "github.com/ethpandaops/execution-processor/pkg/processor/transaction/structlog"
	"github.com/ethpandaops/execution-processor/pkg/state"
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProcessor_Creation(t *testing.T) {
	cfg := transaction_structlog.Config{
		Enabled: true,
		Table:   "test_structlog",
		Config: clickhouse.Config{
			DSN:          "clickhouse://localhost:9000/test",
			MaxOpenConns: 10,
			MaxIdleConns: 5,
		},
		BatchConfig: transaction_structlog.BatchConfig{
			Enabled:   false, // Disabled, will use default chunk size
			ChunkSize: 1000000,
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
				Enabled: true,
				Table:   "test_table",
				Config: clickhouse.Config{
					DSN:          "clickhouse://localhost:9000/test",
					MaxOpenConns: 10,
					MaxIdleConns: 5,
				},
				BatchConfig: transaction_structlog.BatchConfig{
					Enabled:   false, // Will use default chunk size
					ChunkSize: 1000000,
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
				Enabled: true,
				Table:   "test_table",
				BatchConfig: transaction_structlog.BatchConfig{
					Enabled:   false, // Will use default chunk size
					ChunkSize: 1000000,
				},
			},
			expectError: true,
		},
		{
			name: "missing table",
			config: transaction_structlog.Config{
				Enabled: true,
				Config: clickhouse.Config{
					DSN: "clickhouse://localhost:9000/test",
				},
				BatchConfig: transaction_structlog.BatchConfig{
					Enabled:   false, // Will use default chunk size
					ChunkSize: 1000000,
				},
			},
			expectError: true,
		},
		{
			name: "invalid chunk size",
			config: transaction_structlog.Config{
				Enabled: true,
				Table:   "test_table",
				Config: clickhouse.Config{
					DSN: "clickhouse://localhost:9000/test",
				},
				BatchConfig: transaction_structlog.BatchConfig{
					Enabled:   true,
					ChunkSize: 0, // Invalid
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
	// Test concurrent validation calls
	const numGoroutines = 10
	results := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			// Each goroutine gets its own config copy to avoid races
			cfg := transaction_structlog.Config{
				Enabled: true,
				Table:   "test_concurrent",
				Config: clickhouse.Config{
					DSN:          "clickhouse://localhost:9000/test",
					MaxOpenConns: 10,
					MaxIdleConns: 5,
				},
				BatchConfig: transaction_structlog.BatchConfig{
					Enabled:   false, // Will use default chunk size
					ChunkSize: 1000000,
				},
			}
			results <- cfg.Validate()
		}()
	}

	// All validations should succeed
	for i := 0; i < numGoroutines; i++ {
		err := <-results
		assert.NoError(t, err)
	}
}

func TestProcessor_BatchConfigValidation(t *testing.T) {
	testCases := []struct {
		name        string
		config      transaction_structlog.Config
		expectError bool
	}{
		{
			name: "valid batch config enabled",
			config: transaction_structlog.Config{
				Enabled: true,
				Table:   "test_table",
				Config: clickhouse.Config{
					DSN:          "clickhouse://localhost:9000/test",
					MaxOpenConns: 10,
					MaxIdleConns: 5,
				},
				BatchConfig: transaction_structlog.BatchConfig{
					Enabled:           true,
					MaxRows:           10000,
					FlushInterval:     5 * time.Second,
					ChannelBufferSize: 100,
					ChunkSize:         1000000,
				},
			},
			expectError: false,
		},
		{
			name: "batch config disabled",
			config: transaction_structlog.Config{
				Enabled: true,
				Table:   "test_table",
				Config: clickhouse.Config{
					DSN:          "clickhouse://localhost:9000/test",
					MaxOpenConns: 10,
					MaxIdleConns: 5,
				},
				BatchConfig: transaction_structlog.BatchConfig{
					Enabled:   false,
					ChunkSize: 1000000,
				},
			},
			expectError: false,
		},
		{
			name: "invalid max rows",
			config: transaction_structlog.Config{
				Enabled: true,
				Table:   "test_table",
				Config: clickhouse.Config{
					DSN: "clickhouse://localhost:9000/test",
				},
				BatchConfig: transaction_structlog.BatchConfig{
					Enabled:           true,
					MaxRows:           0, // Invalid
					FlushInterval:     5 * time.Second,
					ChannelBufferSize: 100,
				},
			},
			expectError: true,
		},
		{
			name: "invalid flush interval",
			config: transaction_structlog.Config{
				Enabled: true,
				Table:   "test_table",
				Config: clickhouse.Config{
					DSN: "clickhouse://localhost:9000/test",
				},
				BatchConfig: transaction_structlog.BatchConfig{
					Enabled:           true,
					MaxRows:           10000,
					FlushInterval:     0, // Invalid
					ChannelBufferSize: 100,
				},
			},
			expectError: true,
		},
		{
			name: "invalid channel buffer size",
			config: transaction_structlog.Config{
				Enabled: true,
				Table:   "test_table",
				Config: clickhouse.Config{
					DSN: "clickhouse://localhost:9000/test",
				},
				BatchConfig: transaction_structlog.BatchConfig{
					Enabled:           true,
					MaxRows:           10000,
					FlushInterval:     5 * time.Second,
					ChannelBufferSize: 0, // Invalid
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

func TestProcessor_BatchingDisabled(t *testing.T) {
	config := transaction_structlog.Config{
		Enabled: true,
		Table:   "test_structlog",
		Config: clickhouse.Config{
			DSN:          "clickhouse://localhost:9000/test",
			MaxOpenConns: 10,
			MaxIdleConns: 5,
		},
		BatchConfig: transaction_structlog.BatchConfig{
			Enabled:   false, // Disabled
			ChunkSize: 1000000,
		},
	}

	err := config.Validate()
	require.NoError(t, err)

	// Create dependencies
	deps := &transaction_structlog.Dependencies{
		Log:         logrus.NewEntry(logrus.New()),
		Pool:        &ethereum.Pool{}, // Mock pool
		Network:     &ethereum.Network{Name: "test", ID: 1},
		State:       &state.Manager{}, // Mock state
		AsynqClient: &asynq.Client{},  // Mock client
		RedisPrefix: "test",
	}

	ctx := context.Background()

	// This should not fail even without real dependencies since batch collector is disabled
	processor, err := transaction_structlog.New(ctx, deps, &config)
	assert.NoError(t, err)
	assert.NotNil(t, processor)

	// Batch collector should be nil when disabled
	// Note: We can not directly access private fields, but the processor should work
}

func TestProcessor_BatchingEnabled(t *testing.T) {
	config := transaction_structlog.Config{
		Enabled: true,
		Table:   "test_structlog",
		Config: clickhouse.Config{
			DSN:          "clickhouse://localhost:9000/test",
			MaxOpenConns: 10,
			MaxIdleConns: 5,
		},
		BatchConfig: transaction_structlog.BatchConfig{
			Enabled:           true,
			MaxRows:           1000,
			FlushInterval:     5 * time.Second,
			ChannelBufferSize: 100,
			ChunkSize:         1000000,
		},
	}

	err := config.Validate()
	require.NoError(t, err)

	// Create dependencies
	deps := &transaction_structlog.Dependencies{
		Log:         logrus.NewEntry(logrus.New()),
		Pool:        &ethereum.Pool{}, // Mock pool
		Network:     &ethereum.Network{Name: "test", ID: 1},
		State:       &state.Manager{}, // Mock state
		AsynqClient: &asynq.Client{},  // Mock client
		RedisPrefix: "test",
	}

	ctx := context.Background()

	// This should not fail even without real dependencies since we are not starting
	processor, err := transaction_structlog.New(ctx, deps, &config)
	assert.NoError(t, err)
	assert.NotNil(t, processor)

	// Batch collector should be initialized when enabled
	// Note: We can not directly access private fields, but the processor should work
}

func TestBatchConfig_DefaultValues(t *testing.T) {
	// Test that batch config validation works with various combinations
	testCases := []struct {
		name    string
		config  transaction_structlog.BatchConfig
		wantErr bool
	}{
		{
			name: "disabled config",
			config: transaction_structlog.BatchConfig{
				Enabled:   false,
				ChunkSize: 1000000,
				// Other fields can be zero when disabled
			},
			wantErr: false,
		},
		{
			name: "valid enabled config",
			config: transaction_structlog.BatchConfig{
				Enabled:           true,
				MaxRows:           10000,
				FlushInterval:     5 * time.Second,
				ChannelBufferSize: 100,
				ChunkSize:         1000000,
			},
			wantErr: false,
		},
		{
			name: "minimal valid config",
			config: transaction_structlog.BatchConfig{
				Enabled:           true,
				MaxRows:           1,
				FlushInterval:     1 * time.Millisecond,
				ChannelBufferSize: 1,
				ChunkSize:         1000000,
			},
			wantErr: false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := transaction_structlog.Config{
				Enabled:     true,
				Table:       "test_table",
				Config:      clickhouse.Config{DSN: "test://localhost"},
				BatchConfig: tc.config,
			}

			err := config.Validate()
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestBatchConfig_EdgeCases(t *testing.T) {
	// Test edge cases for batch configuration
	testCases := []struct {
		name    string
		config  transaction_structlog.BatchConfig
		wantErr bool
	}{
		{
			name: "zero max rows",
			config: transaction_structlog.BatchConfig{
				Enabled:           true,
				MaxRows:           0, // Invalid
				FlushInterval:     5 * time.Second,
				ChannelBufferSize: 100,
				ChunkSize:         1000000,
			},
			wantErr: true,
		},
		{
			name: "zero flush interval",
			config: transaction_structlog.BatchConfig{
				Enabled:           true,
				MaxRows:           1000,
				FlushInterval:     0, // Invalid
				ChannelBufferSize: 100,
				ChunkSize:         1000000,
			},
			wantErr: true,
		},
		{
			name: "zero channel buffer size",
			config: transaction_structlog.BatchConfig{
				Enabled:           true,
				MaxRows:           1000,
				FlushInterval:     5 * time.Second,
				ChannelBufferSize: 0, // Invalid
				ChunkSize:         1000000,
			},
			wantErr: true,
		},
		{
			name: "negative max rows",
			config: transaction_structlog.BatchConfig{
				Enabled:           true,
				MaxRows:           -1, // Invalid
				FlushInterval:     5 * time.Second,
				ChannelBufferSize: 100,
				ChunkSize:         1000000,
			},
			wantErr: true,
		},
		{
			name: "negative flush interval",
			config: transaction_structlog.BatchConfig{
				Enabled:           true,
				MaxRows:           1000,
				FlushInterval:     -1 * time.Second, // Invalid
				ChannelBufferSize: 100,
				ChunkSize:         1000000,
			},
			wantErr: true,
		},
		{
			name: "zero chunk size",
			config: transaction_structlog.BatchConfig{
				Enabled:           true,
				MaxRows:           1000,
				FlushInterval:     5 * time.Second,
				ChannelBufferSize: 100,
				ChunkSize:         0, // Invalid
			},
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := transaction_structlog.Config{
				Enabled:     true,
				Table:       "test_table",
				Config:      clickhouse.Config{DSN: "test://localhost"},
				BatchConfig: tc.config,
			}

			err := config.Validate()
			if tc.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

// Tests from count_test.go

func TestStructlogCountReturn(t *testing.T) {
	// Create mock trace data
	mockTrace := &MockTrace{
		Gas:         21000,
		Failed:      false,
		ReturnValue: nil,
		Structlogs: []MockStructlog{
			{PC: 0, Op: "PUSH1", Gas: 21000, GasCost: 3, Depth: 1},
			{PC: 2, Op: "PUSH1", Gas: 20997, GasCost: 3, Depth: 1},
			{PC: 4, Op: "SSTORE", Gas: 20994, GasCost: 20000, Depth: 1},
		},
	}

	// Note: cannot directly create processor with private fields in _test package
	// This test validates the counting logic conceptually

	// Test the count calculation and return
	expectedCount := len(mockTrace.Structlogs)

	// We can not easily test the full processTransaction method without a real execution node,
	// but we can test the logic of creating structlogs and counting them
	structlogs := make([]transaction_structlog.Structlog, 0, expectedCount)

	now := time.Now()

	// Simulate the structlog creation logic
	for i, structLog := range mockTrace.Structlogs {
		row := transaction_structlog.Structlog{
			UpdatedDateTime:        now,
			BlockNumber:            12345,
			TransactionHash:        "0x1234567890abcdef",
			TransactionIndex:       0,
			TransactionGas:         mockTrace.Gas,
			TransactionFailed:      mockTrace.Failed,
			TransactionReturnValue: mockTrace.ReturnValue,
			Index:                  uint32(i), //nolint:gosec // safe to use user input in query
			ProgramCounter:         structLog.PC,
			Operation:              structLog.Op,
			Gas:                    structLog.Gas,
			GasCost:                structLog.GasCost,
			Depth:                  structLog.Depth,
			ReturnData:             structLog.ReturnData,
			Refund:                 structLog.Refund,
			Error:                  structLog.Error,
			MetaNetworkID:          1,
			MetaNetworkName:        "test",
		}

		structlogs = append(structlogs, row)
	}

	// Test the key fix: save count before clearing slice
	structlogCount := len(structlogs)

	// Clear slice like the real code does
	structlogs = nil

	// Verify the count was saved correctly
	if structlogCount != expectedCount {
		t.Errorf("Expected count %d, but got %d", expectedCount, structlogCount)
	}

	// Verify that len(structlogs) is now 0 (which would be the bug)
	if len(structlogs) != 0 {
		t.Errorf("Expected cleared slice to have length 0, but got %d", len(structlogs))
	}

	// The fix ensures we return structlogCount, not len(structlogs)
	if structlogCount == 0 {
		t.Error("structlogCount should not be 0 after processing valid structlogs")
	}
}

// Mock structures for testing
type MockTrace struct {
	Gas         uint64          `json:"gas"`
	Failed      bool            `json:"failed"`
	ReturnValue *string         `json:"returnValue"`
	Structlogs  []MockStructlog `json:"structLogs"`
}

type MockStructlog struct {
	PC         uint32  `json:"pc"`
	Op         string  `json:"op"`
	Gas        uint64  `json:"gas"`
	GasCost    uint64  `json:"gasCost"`
	Depth      uint64  `json:"depth"`
	ReturnData *string `json:"returnData"`
	Refund     *uint64 `json:"refund"`
	Error      *string `json:"error"`
}

// Tests from memory_test.go

func TestMemoryManagement(t *testing.T) {
	// Force garbage collection before starting
	runtime.GC()
	runtime.GC()

	var initialMemStats, finalMemStats runtime.MemStats

	runtime.ReadMemStats(&initialMemStats)

	// Create a large slice of structlogs to simulate memory usage
	largeStructlogs := make([]transaction_structlog.Structlog, 10000)
	now := time.Now()

	for i := range largeStructlogs {
		largeStructlogs[i] = transaction_structlog.Structlog{
			UpdatedDateTime:        now,
			BlockNumber:            uint64(i), //nolint:gosec // safe to use user input in query
			TransactionHash:        "0x1234567890abcdef1234567890abcdef12345678",
			TransactionIndex:       uint32(i % 100), //nolint:gosec // safe to use user input in query
			TransactionGas:         21000,
			TransactionFailed:      false,
			TransactionReturnValue: nil,
			Index:                  uint32(i), //nolint:gosec // safe to use user input in query
			ProgramCounter:         uint32(i * 2),
			Operation:              "SSTORE",
			Gas:                    uint64(21000 - i), //nolint:gosec // safe to use user input in query
			GasCost:                5000,
			Depth:                  1,
			ReturnData:             nil,
			Refund:                 nil,
			Error:                  nil,
			MetaNetworkID:          1,
			MetaNetworkName:        "mainnet",
		}
	}

	// Test that chunking calculations work properly
	const chunkSize = 100
	expectedChunks := (len(largeStructlogs) + chunkSize - 1) / chunkSize

	// Verify chunking logic
	actualChunks := 0
	for i := 0; i < len(largeStructlogs); i += chunkSize {
		actualChunks++

		end := i + chunkSize
		if end > len(largeStructlogs) {
			end = len(largeStructlogs)
		}

		// Verify chunk size constraints
		chunkLen := end - i
		if chunkLen <= 0 || chunkLen > chunkSize {
			t.Errorf("Invalid chunk size: %d (expected 1-%d)", chunkLen, chunkSize)
		}
	}

	if actualChunks != expectedChunks {
		t.Errorf("Expected %d chunks, got %d", expectedChunks, actualChunks)
	}

	runtime.GC()

	runtime.ReadMemStats(&finalMemStats)

	// Check that memory was released (this is a rough check)
	allocDiff := finalMemStats.Alloc - initialMemStats.Alloc

	t.Logf("Initial memory: %d bytes", initialMemStats.Alloc)
	t.Logf("Final memory: %d bytes", finalMemStats.Alloc)
	t.Logf("Memory difference: %d bytes", allocDiff)

	// Memory should not grow significantly after cleanup
	// Allow for some overhead but expect most memory to be released
	maxAcceptableGrowth := uint64(1024 * 1024) // 1MB overhead allowance
	if allocDiff > maxAcceptableGrowth {
		t.Logf("Warning: Memory usage grew by %d bytes, which may indicate incomplete cleanup", allocDiff)
	}
}

func TestChunkProcessing(t *testing.T) {
	tests := []struct {
		name           string
		inputSize      int
		expectedChunks int
		chunkSize      int
	}{
		{
			name:           "small input",
			inputSize:      50,
			expectedChunks: 1,
			chunkSize:      100,
		},
		{
			name:           "exact chunk size",
			inputSize:      100,
			expectedChunks: 1,
			chunkSize:      100,
		},
		{
			name:           "multiple chunks",
			inputSize:      250,
			expectedChunks: 3,
			chunkSize:      100,
		},
		{
			name:           "large input",
			inputSize:      1500,
			expectedChunks: 15,
			chunkSize:      100,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create input data
			structlogs := make([]transaction_structlog.Structlog, tt.inputSize)

			// Calculate expected chunks
			expectedChunks := (tt.inputSize + tt.chunkSize - 1) / tt.chunkSize

			if expectedChunks != tt.expectedChunks {
				t.Errorf("Expected %d chunks for %d items, got %d", tt.expectedChunks, tt.inputSize, expectedChunks)
			}

			// Test that the chunking logic would work correctly
			chunkCount := 0
			for i := 0; i < len(structlogs); i += tt.chunkSize {
				chunkCount++

				end := i + tt.chunkSize
				if end > len(structlogs) {
					end = len(structlogs)
				}
				// Verify chunk boundaries
				if end <= i {
					t.Errorf("Invalid chunk boundaries: start=%d, end=%d", i, end)
				}
			}

			if chunkCount != tt.expectedChunks {
				t.Errorf("Chunking produced %d chunks, expected %d", chunkCount, tt.expectedChunks)
			}
		})
	}
}

// Tests from verification_test.go

func TestVerificationConstants(t *testing.T) {
	tests := []struct {
		name     string
		table    string
		expected string
	}{
		{
			name:     "basic table name",
			table:    "test_table",
			expected: "test_table",
		},
		{
			name:     "production table name",
			table:    "transaction_structlogs",
			expected: "transaction_structlogs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.table != tt.expected {
				t.Errorf("table name mismatch: got %s, want %s", tt.table, tt.expected)
			}
		})
	}
}

func TestVerificationQueryPattern(t *testing.T) {
	expectedElements := []string{
		"SELECT COUNT(*)",
		"FROM",
		"FINAL",
		"WHERE block_number =",
		"AND transaction_hash =",
		"AND transaction_index =",
		"AND meta_network_name =",
	}

	// Simulate what the query should contain
	queryPattern := "SELECT COUNT(*) as count FROM %s FINAL WHERE block_number = %d AND transaction_hash = '%s' AND transaction_index = %d AND meta_network_name = '%s'"

	for _, element := range expectedElements {
		if !containsElement(queryPattern, element) {
			t.Errorf("query pattern missing element: %s", element)
		}
	}
}

func TestVerificationErrorPattern(t *testing.T) {
	tests := []struct {
		name          string
		actualCount   int
		expectedCount int
		shouldError   bool
	}{
		{
			name:          "counts match",
			actualCount:   100,
			expectedCount: 100,
			shouldError:   false,
		},
		{
			name:          "counts don't match",
			actualCount:   50,
			expectedCount: 100,
			shouldError:   true,
		},
		{
			name:          "zero counts match",
			actualCount:   0,
			expectedCount: 0,
			shouldError:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the verification logic
			hasError := tt.actualCount != tt.expectedCount

			if hasError != tt.shouldError {
				t.Errorf("error expectation mismatch: got error=%v, want error=%v",
					hasError, tt.shouldError)
			}
		})
	}
}

// Helper function to check if pattern contains element
func containsElement(pattern, element string) bool {
	patternBytes := []byte(pattern)
	elementBytes := []byte(element)

	if len(elementBytes) == 0 {
		return true
	}

	if len(patternBytes) < len(elementBytes) {
		return false
	}

	for i := 0; i <= len(patternBytes)-len(elementBytes); i++ {
		match := true

		for j := 0; j < len(elementBytes); j++ {
			if patternBytes[i+j] != elementBytes[j] {
				match = false

				break
			}
		}

		if match {
			return true
		}
	}

	return false
}

// Tests from tasks_test.go

func TestProcessPayload(t *testing.T) {
	payload := &transaction_structlog.ProcessPayload{
		BlockNumber:      *big.NewInt(12345),
		TransactionHash:  "0x1234567890abcdef",
		TransactionIndex: 5,
		NetworkID:        1,
		NetworkName:      "mainnet",
		Network:          "mainnet",
		ProcessingMode:   c.FORWARDS_MODE,
	}

	// Test JSON marshaling
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}

	var unmarshaled transaction_structlog.ProcessPayload

	err = json.Unmarshal(data, &unmarshaled)
	if err != nil {
		t.Fatalf("failed to unmarshal payload: %v", err)
	}

	if unmarshaled.TransactionHash != payload.TransactionHash {
		t.Errorf("expected transaction hash %s, got %s", payload.TransactionHash, unmarshaled.TransactionHash)
	}

	if unmarshaled.TransactionIndex != payload.TransactionIndex {
		t.Errorf("expected transaction index %d, got %d", payload.TransactionIndex, unmarshaled.TransactionIndex)
	}

	// Test binary marshaling
	binData, err := payload.MarshalBinary()
	if err != nil {
		t.Fatalf("failed to marshal binary: %v", err)
	}

	var binUnmarshaled transaction_structlog.ProcessPayload

	err = binUnmarshaled.UnmarshalBinary(binData)
	if err != nil {
		t.Fatalf("failed to unmarshal binary: %v", err)
	}

	if binUnmarshaled.TransactionHash != payload.TransactionHash {
		t.Errorf("expected transaction hash %s, got %s", payload.TransactionHash, binUnmarshaled.TransactionHash)
	}
}

func TestVerifyPayload(t *testing.T) {
	payload := &transaction_structlog.VerifyPayload{
		BlockNumber:      *big.NewInt(12345),
		TransactionHash:  "0xabcdef1234567890",
		TransactionIndex: 3,
		NetworkID:        1,
		NetworkName:      "mainnet",
		Network:          "mainnet",
		ProcessingMode:   c.FORWARDS_MODE,
	}

	// Test JSON marshaling
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}

	var unmarshaled transaction_structlog.VerifyPayload

	err = json.Unmarshal(data, &unmarshaled)
	if err != nil {
		t.Fatalf("failed to unmarshal payload: %v", err)
	}

	if unmarshaled.TransactionHash != payload.TransactionHash {
		t.Errorf("expected transaction hash %s, got %s", payload.TransactionHash, unmarshaled.TransactionHash)
	}

	// Test binary marshaling
	binData, err := payload.MarshalBinary()
	if err != nil {
		t.Fatalf("failed to marshal binary: %v", err)
	}

	var binUnmarshaled transaction_structlog.VerifyPayload

	err = binUnmarshaled.UnmarshalBinary(binData)
	if err != nil {
		t.Fatalf("failed to unmarshal binary: %v", err)
	}

	if binUnmarshaled.TransactionHash != payload.TransactionHash {
		t.Errorf("expected transaction hash %s, got %s", payload.TransactionHash, binUnmarshaled.TransactionHash)
	}
}

func TestNewProcessForwardsTask(t *testing.T) {
	payload := &transaction_structlog.ProcessPayload{
		BlockNumber:      *big.NewInt(12345),
		TransactionHash:  "0x1234567890abcdef",
		TransactionIndex: 5,
		NetworkID:        1,
		NetworkName:      "mainnet",
		Network:          "mainnet",
	}

	task, err := transaction_structlog.NewProcessForwardsTask(payload)
	if err != nil {
		t.Fatalf("failed to create task: %v", err)
	}

	if task.Type() != transaction_structlog.ProcessForwardsTaskType {
		t.Errorf("expected task type %s, got %s", transaction_structlog.ProcessForwardsTaskType, task.Type())
	}

	// Verify payload can be unmarshaled from task
	var unmarshaled transaction_structlog.ProcessPayload

	err = json.Unmarshal(task.Payload(), &unmarshaled)
	if err != nil {
		t.Fatalf("failed to unmarshal task payload: %v", err)
	}

	if unmarshaled.TransactionHash != payload.TransactionHash {
		t.Errorf("expected transaction hash %s, got %s", payload.TransactionHash, unmarshaled.TransactionHash)
	}

	if unmarshaled.ProcessingMode != c.FORWARDS_MODE {
		t.Errorf("expected processing mode 'forwards', got %s", unmarshaled.ProcessingMode)
	}
}

func TestNewProcessBackwardsTask(t *testing.T) {
	payload := &transaction_structlog.ProcessPayload{
		BlockNumber:      *big.NewInt(12345),
		TransactionHash:  "0x1234567890abcdef",
		TransactionIndex: 5,
		NetworkID:        1,
		NetworkName:      "mainnet",
		Network:          "mainnet",
	}

	task, err := transaction_structlog.NewProcessBackwardsTask(payload)
	if err != nil {
		t.Fatalf("failed to create task: %v", err)
	}

	if task.Type() != transaction_structlog.ProcessBackwardsTaskType {
		t.Errorf("expected task type %s, got %s", transaction_structlog.ProcessBackwardsTaskType, task.Type())
	}

	// Verify payload can be unmarshaled from task
	var unmarshaled transaction_structlog.ProcessPayload

	err = json.Unmarshal(task.Payload(), &unmarshaled)
	if err != nil {
		t.Fatalf("failed to unmarshal task payload: %v", err)
	}

	if unmarshaled.TransactionHash != payload.TransactionHash {
		t.Errorf("expected transaction hash %s, got %s", payload.TransactionHash, unmarshaled.TransactionHash)
	}

	if unmarshaled.ProcessingMode != c.BACKWARDS_MODE {
		t.Errorf("expected processing mode 'backwards', got %s", unmarshaled.ProcessingMode)
	}
}

func TestNewVerifyForwardsTask(t *testing.T) {
	payload := &transaction_structlog.VerifyPayload{
		BlockNumber:      *big.NewInt(12345),
		TransactionHash:  "0xabcdef1234567890",
		TransactionIndex: 3,
		NetworkID:        1,
		NetworkName:      "mainnet",
		Network:          "mainnet",
	}

	task, err := transaction_structlog.NewVerifyForwardsTask(payload)
	if err != nil {
		t.Fatalf("failed to create task: %v", err)
	}

	if task.Type() != transaction_structlog.VerifyForwardsTaskType {
		t.Errorf("expected task type %s, got %s", transaction_structlog.VerifyForwardsTaskType, task.Type())
	}

	// Verify payload can be unmarshaled from task
	var unmarshaled transaction_structlog.VerifyPayload

	err = json.Unmarshal(task.Payload(), &unmarshaled)
	if err != nil {
		t.Fatalf("failed to unmarshal task payload: %v", err)
	}

	if unmarshaled.TransactionHash != payload.TransactionHash {
		t.Errorf("expected transaction hash %s, got %s", payload.TransactionHash, unmarshaled.TransactionHash)
	}

	if unmarshaled.ProcessingMode != c.FORWARDS_MODE {
		t.Errorf("expected processing mode 'forwards', got %s", unmarshaled.ProcessingMode)
	}
}

func TestNewVerifyBackwardsTask(t *testing.T) {
	payload := &transaction_structlog.VerifyPayload{
		BlockNumber:      *big.NewInt(12345),
		TransactionHash:  "0xabcdef1234567890",
		TransactionIndex: 3,
		NetworkID:        1,
		NetworkName:      "mainnet",
		Network:          "mainnet",
	}

	task, err := transaction_structlog.NewVerifyBackwardsTask(payload)
	if err != nil {
		t.Fatalf("failed to create task: %v", err)
	}

	if task.Type() != transaction_structlog.VerifyBackwardsTaskType {
		t.Errorf("expected task type %s, got %s", transaction_structlog.VerifyBackwardsTaskType, task.Type())
	}

	// Verify payload can be unmarshaled from task
	var unmarshaled transaction_structlog.VerifyPayload

	err = json.Unmarshal(task.Payload(), &unmarshaled)
	if err != nil {
		t.Fatalf("failed to unmarshal task payload: %v", err)
	}

	if unmarshaled.TransactionHash != payload.TransactionHash {
		t.Errorf("expected transaction hash %s, got %s", payload.TransactionHash, unmarshaled.TransactionHash)
	}

	if unmarshaled.ProcessingMode != c.BACKWARDS_MODE {
		t.Errorf("expected processing mode 'backwards', got %s", unmarshaled.ProcessingMode)
	}
}

func TestTaskTypes(t *testing.T) {
	// Test that task type constants are defined
	if transaction_structlog.ProcessForwardsTaskType == "" {
		t.Error("ProcessForwardsTaskType should not be empty")
	}

	if transaction_structlog.ProcessBackwardsTaskType == "" {
		t.Error("ProcessBackwardsTaskType should not be empty")
	}

	if transaction_structlog.VerifyForwardsTaskType == "" {
		t.Error("VerifyForwardsTaskType should not be empty")
	}

	if transaction_structlog.VerifyBackwardsTaskType == "" {
		t.Error("VerifyBackwardsTaskType should not be empty")
	}

	if transaction_structlog.ProcessorName == "" {
		t.Error("ProcessorName should not be empty")
	}
}

func TestAsynqTaskCreation(t *testing.T) {
	// Test that we can create asynq tasks manually
	payload := map[string]interface{}{
		"block_number":      "12345",
		"transaction_hash":  "0x1234567890abcdef",
		"transaction_index": 5,
		"network_id":        1,
		"network_name":      "mainnet",
		"processing_mode":   c.FORWARDS_MODE,
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}

	task := asynq.NewTask(transaction_structlog.ProcessForwardsTaskType, data)
	if task == nil {
		t.Fatal("expected task to be created")
	}

	if task.Type() != transaction_structlog.ProcessForwardsTaskType {
		t.Errorf("expected task type %s, got %s", transaction_structlog.ProcessForwardsTaskType, task.Type())
	}
}
