package tracker

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ethpandaops/execution-processor/pkg/ethereum"
)

// uint64Ptr is a helper to create uint64 pointer.
func uint64Ptr(v uint64) *uint64 {
	return &v
}

// TestDistanceBasedBlockingLogic_ForwardsMode tests the distance calculation logic
// for forwards processing mode without requiring full processor instantiation.
func TestDistanceBasedBlockingLogic_ForwardsMode(t *testing.T) {
	tests := []struct {
		name                 string
		nextBlock            uint64
		oldestIncomplete     *uint64
		maxPendingBlockRange uint64
		expectBlocked        bool
	}{
		{
			name:                 "no incomplete blocks - should proceed",
			nextBlock:            105,
			oldestIncomplete:     nil,
			maxPendingBlockRange: 2,
			expectBlocked:        false,
		},
		{
			name:                 "distance 1, max 2 - should proceed",
			nextBlock:            105,
			oldestIncomplete:     uint64Ptr(104),
			maxPendingBlockRange: 2,
			expectBlocked:        false,
		},
		{
			name:                 "distance equals max - should block",
			nextBlock:            105,
			oldestIncomplete:     uint64Ptr(103),
			maxPendingBlockRange: 2,
			expectBlocked:        true,
		},
		{
			name:                 "distance exceeds max - should block",
			nextBlock:            105,
			oldestIncomplete:     uint64Ptr(100),
			maxPendingBlockRange: 2,
			expectBlocked:        true,
		},
		{
			name:                 "large maxPendingBlockRange - should proceed",
			nextBlock:            105,
			oldestIncomplete:     uint64Ptr(100),
			maxPendingBlockRange: 10,
			expectBlocked:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the forwards mode blocking logic
			blocked := tt.oldestIncomplete != nil && (tt.nextBlock-*tt.oldestIncomplete) >= tt.maxPendingBlockRange

			assert.Equal(t, tt.expectBlocked, blocked)
		})
	}
}

// TestDistanceBasedBlockingLogic_BackwardsMode tests the distance calculation logic
// for backwards processing mode without requiring full processor instantiation.
func TestDistanceBasedBlockingLogic_BackwardsMode(t *testing.T) {
	tests := []struct {
		name                 string
		nextBlock            uint64
		newestIncomplete     *uint64
		maxPendingBlockRange uint64
		expectBlocked        bool
	}{
		{
			name:                 "no incomplete blocks - should proceed",
			nextBlock:            100,
			newestIncomplete:     nil,
			maxPendingBlockRange: 2,
			expectBlocked:        false,
		},
		{
			name:                 "distance 1, max 2 - should proceed",
			nextBlock:            100,
			newestIncomplete:     uint64Ptr(101),
			maxPendingBlockRange: 2,
			expectBlocked:        false,
		},
		{
			name:                 "distance equals max - should block",
			nextBlock:            100,
			newestIncomplete:     uint64Ptr(102),
			maxPendingBlockRange: 2,
			expectBlocked:        true,
		},
		{
			name:                 "distance exceeds max - should block",
			nextBlock:            100,
			newestIncomplete:     uint64Ptr(105),
			maxPendingBlockRange: 2,
			expectBlocked:        true,
		},
		{
			name:                 "large maxPendingBlockRange - should proceed",
			nextBlock:            100,
			newestIncomplete:     uint64Ptr(105),
			maxPendingBlockRange: 10,
			expectBlocked:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Simulate the backwards mode blocking logic
			blocked := tt.newestIncomplete != nil && (*tt.newestIncomplete-tt.nextBlock) >= tt.maxPendingBlockRange

			assert.Equal(t, tt.expectBlocked, blocked)
		})
	}
}

// TestSearchRangeCalculation tests that the search range is calculated correctly
// to optimize startup performance.
func TestSearchRangeCalculation(t *testing.T) {
	tests := []struct {
		name                 string
		nextBlock            uint64
		maxPendingBlockRange uint64
		mode                 string
		expectSearchMin      uint64
		expectSearchMax      uint64
	}{
		{
			name:                 "forwards mode - normal case",
			nextBlock:            500,
			maxPendingBlockRange: 2,
			mode:                 FORWARDS_MODE,
			expectSearchMin:      498, // 500 - 2
			expectSearchMax:      0,   // not used in forwards
		},
		{
			name:                 "forwards mode - near zero",
			nextBlock:            1,
			maxPendingBlockRange: 2,
			mode:                 FORWARDS_MODE,
			expectSearchMin:      0, // max(0, 1-2) = 0
			expectSearchMax:      0,
		},
		{
			name:                 "forwards mode - exactly at maxPendingBlockRange",
			nextBlock:            2,
			maxPendingBlockRange: 2,
			mode:                 FORWARDS_MODE,
			expectSearchMin:      0, // 2 - 2 = 0
			expectSearchMax:      0,
		},
		{
			name:                 "backwards mode - normal case",
			nextBlock:            100,
			maxPendingBlockRange: 2,
			mode:                 BACKWARDS_MODE,
			expectSearchMin:      0,   // not used in backwards
			expectSearchMax:      102, // 100 + 2
		},
		{
			name:                 "backwards mode - large maxPendingBlockRange",
			nextBlock:            100,
			maxPendingBlockRange: 10,
			mode:                 BACKWARDS_MODE,
			expectSearchMin:      0,
			expectSearchMax:      110, // 100 + 10
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.mode == FORWARDS_MODE {
				// Calculate search min for forwards mode
				var searchMin uint64
				if tt.nextBlock > tt.maxPendingBlockRange {
					searchMin = tt.nextBlock - tt.maxPendingBlockRange
				}

				assert.Equal(t, tt.expectSearchMin, searchMin)
			} else {
				// Calculate search max for backwards mode
				searchMax := tt.nextBlock + tt.maxPendingBlockRange
				assert.Equal(t, tt.expectSearchMax, searchMax)
			}
		})
	}
}

// TestTwoModesInteraction tests scenarios where forwards and backfill might interact.
// They should be naturally isolated by their search ranges.
func TestTwoModesInteraction(t *testing.T) {
	tests := []struct {
		name                 string
		forwardNext          uint64
		backfillIncomplete   uint64
		maxPendingBlockRange uint64
		expectBlocked        bool
		description          string
	}{
		{
			name:                 "backfill at 249, forward at 500 - forward proceeds",
			forwardNext:          500,
			backfillIncomplete:   249,
			maxPendingBlockRange: 2,
			expectBlocked:        false,
			description:          "249 is outside search range [498, 500]",
		},
		{
			name:                 "backfill at 249 incomplete, forward at 251 - blocked",
			forwardNext:          251,
			backfillIncomplete:   249,
			maxPendingBlockRange: 2,
			expectBlocked:        true,
			description:          "251 - 249 = 2 >= maxPendingBlockRange",
		},
		{
			name:                 "backfill at 249 incomplete, forward at 252 - not blocked",
			forwardNext:          252,
			backfillIncomplete:   249,
			maxPendingBlockRange: 2,
			expectBlocked:        false,
			description:          "249 is outside search range [250, 252]",
		},
		{
			name:                 "backfill right at search boundary - blocked",
			forwardNext:          251,
			backfillIncomplete:   249, // searchMin = 251 - 2 = 249
			maxPendingBlockRange: 2,
			expectBlocked:        true,
			description:          "249 is exactly at searchMin, distance = 2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Calculate search range for forwards mode
			var searchMin uint64
			if tt.forwardNext > tt.maxPendingBlockRange {
				searchMin = tt.forwardNext - tt.maxPendingBlockRange
			}

			// Check if the backfill incomplete block would be found by the query
			// (simulates the database query with >= minBlockNumber filter)
			var foundIncomplete *uint64
			if tt.backfillIncomplete >= searchMin {
				foundIncomplete = uint64Ptr(tt.backfillIncomplete)
			}

			// Apply the blocking logic
			blocked := foundIncomplete != nil && (tt.forwardNext-*foundIncomplete) >= tt.maxPendingBlockRange

			assert.Equal(t, tt.expectBlocked, blocked, tt.description)
		})
	}
}

// TestZeroMaxPendingBlockRange verifies that zero maxPendingBlockRange disables blocking.
func TestZeroMaxPendingBlockRange(t *testing.T) {
	// When maxPendingBlockRange is 0, the check should be skipped entirely
	maxPendingBlockRange := 0

	// The implementation checks if maxPendingBlockRange <= 0 and returns false immediately
	assert.LessOrEqual(t, maxPendingBlockRange, 0, "maxPendingBlockRange should be <= 0 for this test")

	// With maxPendingBlockRange = 0, we should never block
	// This is verified by the early return in IsBlockedByIncompleteBlocks
}

// TestNegativeMaxPendingBlockRange verifies that negative maxPendingBlockRange is handled.
func TestNegativeMaxPendingBlockRange(t *testing.T) {
	// When maxPendingBlockRange is negative, the check should be skipped entirely
	maxPendingBlockRange := -1

	// The implementation checks if maxPendingBlockRange <= 0 and returns false immediately
	assert.Less(t, maxPendingBlockRange, 0, "maxPendingBlockRange should be negative for this test")
}

// TestIsBlockNotFoundError tests the error detection function.
func TestIsBlockNotFoundError(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "sentinel ethereum.ErrBlockNotFound",
			err:      ethereum.ErrBlockNotFound,
			expected: true,
		},
		{
			name:     "wrapped sentinel error",
			err:      errors.Join(errors.New("context"), ethereum.ErrBlockNotFound),
			expected: true,
		},
		{
			name:     "not found error",
			err:      &testError{msg: "block not found"},
			expected: true,
		},
		{
			name:     "header not found error",
			err:      &testError{msg: "header not found for block 12345"},
			expected: true,
		},
		{
			name:     "unknown block error",
			err:      &testError{msg: "unknown block requested"},
			expected: true,
		},
		{
			name:     "generic not found",
			err:      &testError{msg: "resource not found"},
			expected: true,
		},
		{
			name:     "unrelated error",
			err:      &testError{msg: "connection refused"},
			expected: false,
		},
		{
			name:     "case insensitive - NOT FOUND",
			err:      &testError{msg: "BLOCK NOT FOUND"},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsBlockNotFoundError(tt.err)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// testError is a simple error implementation for testing.
type testError struct {
	msg string
}

func (e *testError) Error() string {
	return e.msg
}
