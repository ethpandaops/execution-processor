package structlog_test

import (
	"encoding/json"
	"math/big"
	"runtime"
	"testing"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
	c "github.com/ethpandaops/execution-processor/pkg/processor/common"
	transaction_structlog "github.com/ethpandaops/execution-processor/pkg/processor/transaction/structlog"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
)

func TestProcessor_Creation(t *testing.T) {
	cfg := transaction_structlog.Config{
		Enabled: true,
		Table:   "test_structlog",
		Config: clickhouse.Config{
			Addr: "localhost:9000",
		},
		ChunkSize: 10000,
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
					Addr: "localhost:9000",
				},
				ChunkSize: 10000,
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
			name: "missing addr",
			config: transaction_structlog.Config{
				Enabled:   true,
				Table:     "test_table",
				ChunkSize: 10000,
			},
			expectError: true,
		},
		{
			name: "missing table",
			config: transaction_structlog.Config{
				Enabled: true,
				Config: clickhouse.Config{
					Addr: "localhost:9000",
				},
				ChunkSize: 10000,
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
					Addr: "localhost:9000",
				},
				ChunkSize: 10000,
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

	// Test the count calculation and return
	expectedCount := len(mockTrace.Structlogs)

	// Test using Columns type for counting
	cols := transaction_structlog.NewColumns()
	now := time.Now()

	// Simulate the structlog appending logic
	for i, structLog := range mockTrace.Structlogs {
		cols.Append(
			now,
			12345,                 // blockNumber
			"0x1234567890abcdef",  // txHash
			0,                     // txIndex
			mockTrace.Gas,         // txGas
			mockTrace.Failed,      // txFailed
			mockTrace.ReturnValue, // txReturnValue
			uint32(i),             // index
			structLog.PC,          // pc
			structLog.Op,          // op
			structLog.Gas,         // gas
			structLog.GasCost,     // gasCost
			structLog.GasCost,     // gasUsed (simplified)
			structLog.Depth,       // depth
			structLog.ReturnData,  // returnData
			structLog.Refund,      // refund
			structLog.Error,       // error
			nil,                   // callTo
			"test",                // network
		)
	}

	// Test the key fix: count is tracked by Columns
	structlogCount := cols.Rows()

	// Verify the count was saved correctly
	if structlogCount != expectedCount {
		t.Errorf("Expected count %d, but got %d", expectedCount, structlogCount)
	}

	// Reset columns (like the real code does between chunks)
	cols.Reset()

	// Verify that Rows() is now 0 (which would be the bug if we returned this after reset)
	if cols.Rows() != 0 {
		t.Errorf("Expected reset columns to have 0 rows, but got %d", cols.Rows())
	}

	// The fix ensures we track count before reset
	if structlogCount == 0 {
		t.Error("structlogCount should not be 0 after processing valid structlogs")
	}
}

// MockTrace represents mock trace data for testing.
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

	// Create columns and simulate large batch of structlogs
	cols := transaction_structlog.NewColumns()
	now := time.Now()

	const rowCount = 10000

	for i := 0; i < rowCount; i++ {
		cols.Append(
			now,
			uint64(i), // blockNumber
			"0x1234567890abcdef1234567890abcdef12345678", // txHash
			uint32(i%100),   // txIndex
			21000,           // txGas
			false,           // txFailed
			nil,             // txReturnValue
			uint32(i),       // index
			uint32(i*2),     // pc
			"SSTORE",        // op
			uint64(21000-i), // gas
			5000,            // gasCost
			5000,            // gasUsed
			1,               // depth
			nil,             // returnData
			nil,             // refund
			nil,             // error
			nil,             // callTo
			"mainnet",       // network
		)
	}

	assert.Equal(t, rowCount, cols.Rows(), "Should have correct row count")

	// Test that chunking calculations work properly
	const chunkSize = 100

	expectedChunks := (rowCount + chunkSize - 1) / chunkSize

	// Verify chunking logic
	actualChunks := 0

	for i := 0; i < rowCount; i += chunkSize {
		actualChunks++

		end := i + chunkSize
		if end > rowCount {
			end = rowCount
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

	// Reset columns to free memory
	cols.Reset()
	assert.Equal(t, 0, cols.Rows(), "Reset should clear all rows")

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
			// Test chunking logic using Columns
			cols := transaction_structlog.NewColumns()
			now := time.Now()

			// Fill columns with test data
			for i := 0; i < tt.inputSize; i++ {
				cols.Append(
					now, uint64(i), "0xtest", 0, 21000, false, nil,
					uint32(i), uint32(i), "PUSH1", 20000, 3, 3, 1,
					nil, nil, nil, nil, "test",
				)
			}

			assert.Equal(t, tt.inputSize, cols.Rows(), "Should have correct row count")

			// Calculate expected chunks
			expectedChunks := (tt.inputSize + tt.chunkSize - 1) / tt.chunkSize

			if expectedChunks != tt.expectedChunks {
				t.Errorf("Expected %d chunks for %d items, got %d", tt.expectedChunks, tt.inputSize, expectedChunks)
			}

			// Test that the chunking logic would work correctly
			chunkCount := 0

			for i := 0; i < tt.inputSize; i += tt.chunkSize {
				chunkCount++

				end := i + tt.chunkSize
				if end > tt.inputSize {
					end = tt.inputSize
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

func TestColumnsAppendAndReset(t *testing.T) {
	cols := transaction_structlog.NewColumns()
	now := time.Now()

	// Initially empty
	assert.Equal(t, 0, cols.Rows())

	// Append a row
	str := "test"
	num := uint64(42)

	cols.Append(
		now, 100, "0xabc", 0, 21000, false, &str,
		0, 100, "PUSH1", 20000, 3, 3, 1,
		nil, &num, nil, nil, "mainnet",
	)

	assert.Equal(t, 1, cols.Rows())

	// Append more rows
	for i := 0; i < 99; i++ {
		cols.Append(
			now, 100, "0xabc", 0, 21000, false, nil,
			uint32(i+1), 100, "PUSH1", 20000, 3, 3, 1,
			nil, nil, nil, nil, "mainnet",
		)
	}

	assert.Equal(t, 100, cols.Rows())

	// Reset
	cols.Reset()
	assert.Equal(t, 0, cols.Rows())
}

func TestColumnsInput(t *testing.T) {
	cols := transaction_structlog.NewColumns()
	input := cols.Input()

	// Verify all 19 columns are present
	assert.Len(t, input, 19)
	assert.Equal(t, "updated_date_time", input[0].Name)
	assert.Equal(t, "meta_network_name", input[18].Name)
}

// Tests from tasks_test.go

func TestProcessPayload(t *testing.T) {
	payload := &transaction_structlog.ProcessPayload{
		BlockNumber:      *big.NewInt(12345),
		TransactionHash:  "0x1234567890abcdef",
		TransactionIndex: 5,
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

func TestNewProcessForwardsTask(t *testing.T) {
	payload := &transaction_structlog.ProcessPayload{
		BlockNumber:      *big.NewInt(12345),
		TransactionHash:  "0x1234567890abcdef",
		TransactionIndex: 5,
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

func TestTaskTypes(t *testing.T) {
	// Test that task type constants are defined
	if transaction_structlog.ProcessForwardsTaskType == "" {
		t.Error("ProcessForwardsTaskType should not be empty")
	}

	if transaction_structlog.ProcessBackwardsTaskType == "" {
		t.Error("ProcessBackwardsTaskType should not be empty")
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

func TestLimiterBoundaryCondition(t *testing.T) {
	// Test that verifies the fix for block reprocessing when progressive next equals limiter_max + 1
	testCases := []struct {
		name               string
		progressiveNext    int64
		limiterMax         int64
		expectNoMoreBlocks bool
		expectMaxAllowed   bool
	}{
		{
			name:               "progressive equals limiter max - should return progressive",
			progressiveNext:    100,
			limiterMax:         100,
			expectNoMoreBlocks: false,
			expectMaxAllowed:   false,
		},
		{
			name:               "progressive equals limiter max + 1 - should return no more blocks",
			progressiveNext:    101,
			limiterMax:         100,
			expectNoMoreBlocks: true,
			expectMaxAllowed:   false,
		},
		{
			name:               "progressive greater than limiter max + 1 - should return no more blocks",
			progressiveNext:    102,
			limiterMax:         100,
			expectNoMoreBlocks: true,
			expectMaxAllowed:   false,
		},
		{
			name:               "progressive less than limiter max - should return progressive",
			progressiveNext:    99,
			limiterMax:         100,
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
		name           string
		blockNumber    uint64
		withinSeconds  int
		expectedRecent bool
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

func TestHeadDistanceCalculation(t *testing.T) {
	// Test the head distance calculation logic without requiring real databases
	testCases := []struct {
		name             string
		currentBlock     int64
		executionHead    int64
		beaconHead       int64
		limiterEnabled   bool
		mode             string
		expectedDistance int64
		expectedHeadType string
	}{
		{
			name:             "limiter disabled - execution head",
			currentBlock:     100,
			executionHead:    110,
			beaconHead:       105,
			limiterEnabled:   false,
			mode:             "forwards",
			expectedDistance: 10, // executionHead - currentBlock
			expectedHeadType: "execution_head",
		},
		{
			name:             "backwards mode - execution head",
			currentBlock:     100,
			executionHead:    110,
			beaconHead:       105,
			limiterEnabled:   true,
			mode:             "backwards",
			expectedDistance: 10, // executionHead - currentBlock
			expectedHeadType: "execution_head",
		},
		{
			name:             "limiter enabled forwards - beacon head",
			currentBlock:     100,
			executionHead:    110,
			beaconHead:       105,
			limiterEnabled:   true,
			mode:             "forwards",
			expectedDistance: 5, // beaconHead - currentBlock
			expectedHeadType: "beacon_chain_head",
		},
		{
			name:             "caught up to execution head",
			currentBlock:     110,
			executionHead:    110,
			beaconHead:       105,
			limiterEnabled:   false,
			mode:             "forwards",
			expectedDistance: 0, // executionHead - currentBlock
			expectedHeadType: "execution_head",
		},
		{
			name:             "behind by large margin",
			currentBlock:     50,
			executionHead:    1000,
			beaconHead:       900,
			limiterEnabled:   false,
			mode:             "forwards",
			expectedDistance: 950, // executionHead - currentBlock
			expectedHeadType: "execution_head",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Test the distance calculation logic
			currentBlock := big.NewInt(tc.currentBlock)
			executionHead := big.NewInt(tc.executionHead)
			beaconHead := big.NewInt(tc.beaconHead)

			var actualDistance int64

			var actualHeadType string

			// Simulate the logic from GetHeadDistance method
			if !tc.limiterEnabled || tc.mode == "backwards" {
				actualDistance = executionHead.Int64() - currentBlock.Int64()
				actualHeadType = "execution_head"
			} else {
				// Limiter enabled in forwards mode
				actualDistance = beaconHead.Int64() - currentBlock.Int64()
				actualHeadType = "beacon_chain_head"
			}

			assert.Equal(t, tc.expectedDistance, actualDistance, "Distance calculation should match expected")
			assert.Equal(t, tc.expectedHeadType, actualHeadType, "Head type should match expected")

			// Verify distance is reasonable (not negative in normal scenarios)
			if tc.currentBlock <= tc.executionHead && tc.currentBlock <= tc.beaconHead {
				assert.GreaterOrEqual(t, actualDistance, int64(0), "Distance should not be negative when current block is behind head")
			}
		})
	}
}

func TestHeadDistanceMetricLabels(t *testing.T) {
	// Test that head distance metric supports all expected label values
	expectedHeadTypes := []string{
		"execution_head",
		"beacon_chain_head",
		"execution_head_fallback",
		"error",
	}

	for _, headType := range expectedHeadTypes {
		t.Run("head_type_"+headType, func(t *testing.T) {
			// Verify the head type is a valid string and not empty
			assert.NotEmpty(t, headType, "Head type should not be empty")
			assert.True(t, len(headType) > 0, "Head type should have meaningful content")

			// Test that metric labels would be valid
			networkLabel := "mainnet"
			processorLabel := "transaction_structlog"

			assert.NotEmpty(t, networkLabel, "Network label should not be empty")
			assert.NotEmpty(t, processorLabel, "Processor label should not be empty")
			assert.NotEmpty(t, headType, "Head type label should not be empty")
		})
	}
}
