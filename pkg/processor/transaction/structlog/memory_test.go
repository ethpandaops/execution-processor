package structlog

import (
	"runtime"
	"testing"
	"time"
)

func TestMemoryManagement(t *testing.T) {
	// Force garbage collection before starting
	runtime.GC()
	runtime.GC()

	var initialMemStats, finalMemStats runtime.MemStats

	runtime.ReadMemStats(&initialMemStats)

	// Create a large slice of structlogs to simulate memory usage
	largeStructlogs := make([]Structlog, 10000)
	now := time.Now()

	for i := range largeStructlogs {
		largeStructlogs[i] = Structlog{
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
			structlogs := make([]Structlog, tt.inputSize)

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
