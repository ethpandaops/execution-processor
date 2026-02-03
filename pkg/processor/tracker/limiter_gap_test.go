package tracker

import (
	"context"
	"math/big"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockGapStateProvider implements GapStateProvider for testing.
type mockGapStateProvider struct {
	mockStateProvider
	incompleteBlocksInRange []uint64
	minStoredBlock          *big.Int
	maxStoredBlock          *big.Int
	getIncompleteErr        error
	getMinMaxErr            error
}

func (m *mockGapStateProvider) GetIncompleteBlocksInRange(
	_ context.Context, _, _ string, _, _ uint64, _ int,
) ([]uint64, error) {
	if m.getIncompleteErr != nil {
		return nil, m.getIncompleteErr
	}

	return m.incompleteBlocksInRange, nil
}

func (m *mockGapStateProvider) GetMinMaxStoredBlocks(
	_ context.Context, _, _ string,
) (*big.Int, *big.Int, error) {
	if m.getMinMaxErr != nil {
		return nil, nil, m.getMinMaxErr
	}

	return m.minStoredBlock, m.maxStoredBlock, nil
}

func TestGetGaps_FindsMissingBlocks(t *testing.T) {
	// Setup: blocks 5,6,7,9,10...100 are complete, block 8 is incomplete
	// With maxPendingBlockRange=2 and currentBlock=100, gap scanner looks at blocks up to 97
	// (excluding the window 98-100 that the limiter handles)
	mockProvider := &mockGapStateProvider{
		minStoredBlock:          big.NewInt(5),
		maxStoredBlock:          big.NewInt(100),
		incompleteBlocksInRange: []uint64{8},
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "mainnet",
		Processor:     "simple",
	}, LimiterConfig{MaxPendingBlockRange: 2})

	ctx := context.Background()
	currentBlock := uint64(100)
	lookbackRange := uint64(0) // Unlimited

	// Gap scanner searches [5, 97] (excludes 98-100 handled by limiter)
	gaps, err := limiter.GetGaps(ctx, currentBlock, lookbackRange, 100)

	require.NoError(t, err)
	assert.Equal(t, []uint64{8}, gaps)
}

func TestGetGaps_RespectsLookbackRange(t *testing.T) {
	mockProvider := &mockGapStateProvider{
		// GetMinMaxStoredBlocks should NOT be called when lookbackRange is set
		incompleteBlocksInRange: []uint64{75, 80},
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "mainnet",
		Processor:     "simple",
	}, LimiterConfig{MaxPendingBlockRange: 2})

	ctx := context.Background()
	currentBlock := uint64(100)
	lookbackRange := uint64(50) // Only look back 50 blocks

	// Should query from block 50 (100 - 50) to 100
	gaps, err := limiter.GetGaps(ctx, currentBlock, lookbackRange, 100)

	require.NoError(t, err)
	assert.Equal(t, []uint64{75, 80}, gaps)
}

func TestGetGaps_NoGaps(t *testing.T) {
	mockProvider := &mockGapStateProvider{
		minStoredBlock:          big.NewInt(1),
		maxStoredBlock:          big.NewInt(100),
		incompleteBlocksInRange: []uint64{},
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "mainnet",
		Processor:     "simple",
	}, LimiterConfig{MaxPendingBlockRange: 2})

	ctx := context.Background()
	currentBlock := uint64(100)
	lookbackRange := uint64(0)

	gaps, err := limiter.GetGaps(ctx, currentBlock, lookbackRange, 100)

	require.NoError(t, err)
	assert.Empty(t, gaps)
}

func TestGetGaps_DoesNotLookBeforeOldestStoredBlock(t *testing.T) {
	// Ensure we don't query for blocks before the oldest stored block
	mockProvider := &mockGapStateProvider{
		// Oldest stored is 50, so we should only query from 50 onwards
		minStoredBlock:          big.NewInt(50),
		maxStoredBlock:          big.NewInt(100),
		incompleteBlocksInRange: []uint64{},
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "mainnet",
		Processor:     "simple",
	}, LimiterConfig{MaxPendingBlockRange: 2})

	ctx := context.Background()
	currentBlock := uint64(100)
	lookbackRange := uint64(0) // Unlimited

	gaps, err := limiter.GetGaps(ctx, currentBlock, lookbackRange, 100)

	require.NoError(t, err)
	assert.Empty(t, gaps)
}

func TestGetGaps_NoBlocksStored(t *testing.T) {
	mockProvider := &mockGapStateProvider{
		minStoredBlock: nil, // No blocks stored
		maxStoredBlock: nil,
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "mainnet",
		Processor:     "simple",
	}, LimiterConfig{MaxPendingBlockRange: 2})

	ctx := context.Background()
	currentBlock := uint64(100)
	lookbackRange := uint64(0)

	gaps, err := limiter.GetGaps(ctx, currentBlock, lookbackRange, 100)

	require.NoError(t, err)
	assert.Nil(t, gaps)
}

func TestGetGaps_StateProviderDoesNotSupportGapDetection(t *testing.T) {
	// Use the basic mockStateProvider which doesn't implement GapStateProvider
	mockProvider := &mockStateProvider{}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "mainnet",
		Processor:     "simple",
	}, LimiterConfig{MaxPendingBlockRange: 2})

	ctx := context.Background()
	currentBlock := uint64(100)
	lookbackRange := uint64(0)

	gaps, err := limiter.GetGaps(ctx, currentBlock, lookbackRange, 100)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "state provider does not support gap detection")
	assert.Nil(t, gaps)
}

func TestGetGaps_MultipleGaps(t *testing.T) {
	mockProvider := &mockGapStateProvider{
		minStoredBlock:          big.NewInt(1),
		maxStoredBlock:          big.NewInt(100),
		incompleteBlocksInRange: []uint64{5, 10, 15, 20, 25},
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "mainnet",
		Processor:     "simple",
	}, LimiterConfig{MaxPendingBlockRange: 2})

	ctx := context.Background()
	currentBlock := uint64(100)
	lookbackRange := uint64(0)

	gaps, err := limiter.GetGaps(ctx, currentBlock, lookbackRange, 100)

	require.NoError(t, err)
	assert.Equal(t, []uint64{5, 10, 15, 20, 25}, gaps)
}

func TestGetGaps_LookbackRangeGreaterThanCurrentBlock(t *testing.T) {
	// When lookbackRange is greater than currentBlock, minBlock should be 0
	// With maxPendingBlockRange=2 and currentBlock=10, gap scanner looks at blocks up to 7
	mockProvider := &mockGapStateProvider{
		incompleteBlocksInRange: []uint64{3},
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "mainnet",
		Processor:     "simple",
	}, LimiterConfig{MaxPendingBlockRange: 2})

	ctx := context.Background()
	currentBlock := uint64(10)
	lookbackRange := uint64(100) // Greater than currentBlock

	// Gap scanner searches [0, 7] (excludes 8-10 handled by limiter)
	gaps, err := limiter.GetGaps(ctx, currentBlock, lookbackRange, 100)

	require.NoError(t, err)
	assert.Equal(t, []uint64{3}, gaps)
}

func TestGetGaps_ExcludesMaxPendingBlockRangeWindow(t *testing.T) {
	// Verify that gaps within maxPendingBlockRange are NOT returned
	// because they're already handled by IsBlockedByIncompleteBlocks
	mockProvider := &mockGapStateProvider{
		minStoredBlock:          big.NewInt(1),
		maxStoredBlock:          big.NewInt(100),
		incompleteBlocksInRange: []uint64{50}, // Gap at block 50, outside the exclusion window
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "mainnet",
		Processor:     "simple",
	}, LimiterConfig{MaxPendingBlockRange: 5})

	ctx := context.Background()
	currentBlock := uint64(100)
	lookbackRange := uint64(0)

	// With maxPendingBlockRange=5, gap scanner searches [1, 94] (excludes 95-100)
	// Block 50 should be found since it's outside the exclusion window
	gaps, err := limiter.GetGaps(ctx, currentBlock, lookbackRange, 100)

	require.NoError(t, err)
	assert.Equal(t, []uint64{50}, gaps)
}

func TestGetGaps_CurrentBlockWithinExclusionWindow(t *testing.T) {
	// When currentBlock is smaller than or equal to maxPendingBlockRange,
	// there's nothing to scan outside the exclusion window
	mockProvider := &mockGapStateProvider{
		minStoredBlock:          big.NewInt(1),
		maxStoredBlock:          big.NewInt(5),
		incompleteBlocksInRange: []uint64{2},
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "mainnet",
		Processor:     "simple",
	}, LimiterConfig{MaxPendingBlockRange: 10}) // Larger than currentBlock

	ctx := context.Background()
	currentBlock := uint64(5)
	lookbackRange := uint64(0)

	// currentBlock (5) <= maxPendingBlockRange (10), so nothing to scan
	gaps, err := limiter.GetGaps(ctx, currentBlock, lookbackRange, 100)

	require.NoError(t, err)
	assert.Nil(t, gaps)
}
