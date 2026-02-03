package simple_test

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
	"github.com/ethpandaops/execution-processor/pkg/processor/tracker"
	transaction_simple "github.com/ethpandaops/execution-processor/pkg/processor/transaction/simple"
)

func TestProcessor_ConfigValidation(t *testing.T) {
	testCases := []struct {
		name        string
		config      transaction_simple.Config
		expectError bool
	}{
		{
			name: "valid config",
			config: transaction_simple.Config{
				Enabled: true,
				Table:   "test_table",
				Config: clickhouse.Config{
					Addr: "localhost:9000",
				},
			},
			expectError: false,
		},
		{
			name: "disabled config",
			config: transaction_simple.Config{
				Enabled: false,
			},
			expectError: false,
		},
		{
			name: "missing addr",
			config: transaction_simple.Config{
				Enabled: true,
				Table:   "test_table",
			},
			expectError: true,
		},
		{
			name: "missing table",
			config: transaction_simple.Config{
				Enabled: true,
				Config: clickhouse.Config{
					Addr: "localhost:9000",
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
	const numGoroutines = 10

	results := make(chan error, numGoroutines)

	for i := 0; i < numGoroutines; i++ {
		go func() {
			cfg := transaction_simple.Config{
				Enabled: true,
				Table:   "test_concurrent",
				Config: clickhouse.Config{
					Addr: "localhost:9000",
				},
			}
			results <- cfg.Validate()
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		err := <-results
		assert.NoError(t, err)
	}
}

func TestProcessPayload(t *testing.T) {
	payload := &transaction_simple.ProcessPayload{
		BlockNumber:    *big.NewInt(12345),
		NetworkName:    "mainnet",
		ProcessingMode: tracker.FORWARDS_MODE,
	}

	// Test JSON marshaling
	data, err := json.Marshal(payload)
	assert.NoError(t, err)

	var unmarshaled transaction_simple.ProcessPayload

	err = json.Unmarshal(data, &unmarshaled)
	assert.NoError(t, err)

	assert.Equal(t, payload.NetworkName, unmarshaled.NetworkName)
	assert.Equal(t, payload.ProcessingMode, unmarshaled.ProcessingMode)
	assert.Equal(t, int64(12345), unmarshaled.BlockNumber.Int64())

	// Test binary marshaling
	binData, err := payload.MarshalBinary()
	assert.NoError(t, err)

	var binUnmarshaled transaction_simple.ProcessPayload

	err = binUnmarshaled.UnmarshalBinary(binData)
	assert.NoError(t, err)

	assert.Equal(t, payload.NetworkName, binUnmarshaled.NetworkName)
	assert.Equal(t, int64(12345), binUnmarshaled.BlockNumber.Int64())
}

func TestNewProcessForwardsTask(t *testing.T) {
	payload := &transaction_simple.ProcessPayload{
		BlockNumber: *big.NewInt(12345),
		NetworkName: "mainnet",
	}

	task, taskID, err := transaction_simple.NewProcessForwardsTask(payload)
	assert.NoError(t, err)
	assert.NotNil(t, task)
	assert.Equal(t, transaction_simple.ProcessForwardsTaskType, task.Type())

	// Verify taskID is generated correctly
	expectedTaskID := transaction_simple.GenerateTaskID(payload.NetworkName, payload.BlockNumber.Uint64())
	assert.Equal(t, expectedTaskID, taskID)

	// Verify payload can be unmarshaled from task
	var unmarshaled transaction_simple.ProcessPayload

	err = json.Unmarshal(task.Payload(), &unmarshaled)
	assert.NoError(t, err)

	assert.Equal(t, int64(12345), unmarshaled.BlockNumber.Int64())
	assert.Equal(t, tracker.FORWARDS_MODE, unmarshaled.ProcessingMode)
}

func TestNewProcessBackwardsTask(t *testing.T) {
	payload := &transaction_simple.ProcessPayload{
		BlockNumber: *big.NewInt(12345),
		NetworkName: "mainnet",
	}

	task, taskID, err := transaction_simple.NewProcessBackwardsTask(payload)
	assert.NoError(t, err)
	assert.NotNil(t, task)
	assert.Equal(t, transaction_simple.ProcessBackwardsTaskType, task.Type())

	// Verify taskID is generated correctly
	expectedTaskID := transaction_simple.GenerateTaskID(payload.NetworkName, payload.BlockNumber.Uint64())
	assert.Equal(t, expectedTaskID, taskID)

	// Verify payload can be unmarshaled from task
	var unmarshaled transaction_simple.ProcessPayload

	err = json.Unmarshal(task.Payload(), &unmarshaled)
	assert.NoError(t, err)

	assert.Equal(t, int64(12345), unmarshaled.BlockNumber.Int64())
	assert.Equal(t, tracker.BACKWARDS_MODE, unmarshaled.ProcessingMode)
}

func TestTaskTypes(t *testing.T) {
	assert.NotEmpty(t, transaction_simple.ProcessForwardsTaskType)
	assert.NotEmpty(t, transaction_simple.ProcessBackwardsTaskType)
	assert.NotEmpty(t, transaction_simple.ProcessorName)

	// Verify unique task types
	assert.NotEqual(t, transaction_simple.ProcessForwardsTaskType, transaction_simple.ProcessBackwardsTaskType)
}

func TestAsynqTaskCreation(t *testing.T) {
	payload := map[string]interface{}{
		"block_number":    "12345",
		"network_name":    "mainnet",
		"processing_mode": tracker.FORWARDS_MODE,
	}

	data, err := json.Marshal(payload)
	assert.NoError(t, err)

	task := asynq.NewTask(transaction_simple.ProcessForwardsTaskType, data)
	assert.NotNil(t, task)
	assert.Equal(t, transaction_simple.ProcessForwardsTaskType, task.Type())
}

func TestProcessPayloadEdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		payload transaction_simple.ProcessPayload
	}{
		{
			name: "zero block number",
			payload: transaction_simple.ProcessPayload{
				BlockNumber: *big.NewInt(0),
				NetworkName: "mainnet",
			},
		},
		{
			name: "large block number",
			payload: transaction_simple.ProcessPayload{
				BlockNumber: *big.NewInt(9223372036854775807), // max int64
				NetworkName: "mainnet",
			},
		},
		{
			name: "empty network name",
			payload: transaction_simple.ProcessPayload{
				BlockNumber: *big.NewInt(12345),
				NetworkName: "",
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Test marshaling roundtrip
			data, err := tc.payload.MarshalBinary()
			assert.NoError(t, err)

			var unmarshaled transaction_simple.ProcessPayload

			err = unmarshaled.UnmarshalBinary(data)
			assert.NoError(t, err)

			assert.Equal(t, tc.payload.BlockNumber.Int64(), unmarshaled.BlockNumber.Int64())
			assert.Equal(t, tc.payload.NetworkName, unmarshaled.NetworkName)
		})
	}
}

func TestConfigDefaults(t *testing.T) {
	cfg := transaction_simple.Config{
		Enabled: true,
		Table:   "test_table",
		Config: clickhouse.Config{
			Addr: "localhost:9000",
		},
	}

	err := cfg.Validate()
	assert.NoError(t, err)

	// MaxPendingBlockRange can be 0 as the processor will set the default
	assert.GreaterOrEqual(t, cfg.MaxPendingBlockRange, 0)
}
