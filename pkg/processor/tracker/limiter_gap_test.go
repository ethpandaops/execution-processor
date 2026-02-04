package tracker

import (
	"context"
	"errors"
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
	missingBlocksInRange    []uint64
	minStoredBlock          *big.Int
	maxStoredBlock          *big.Int
	getIncompleteErr        error
	getMissingErr           error
	getMinMaxErr            error

	// Track calls for parameter validation
	lastIncompleteCall struct {
		network, processor string
		minBlock, maxBlock uint64
		limit              int
	}
	lastMissingCall struct {
		network, processor string
		minBlock, maxBlock uint64
		limit              int
	}
}

func (m *mockGapStateProvider) GetIncompleteBlocksInRange(
	_ context.Context, network, processor string, minBlock, maxBlock uint64, limit int,
) ([]uint64, error) {
	m.lastIncompleteCall.network = network
	m.lastIncompleteCall.processor = processor
	m.lastIncompleteCall.minBlock = minBlock
	m.lastIncompleteCall.maxBlock = maxBlock
	m.lastIncompleteCall.limit = limit

	if m.getIncompleteErr != nil {
		return nil, m.getIncompleteErr
	}

	return m.incompleteBlocksInRange, nil
}

func (m *mockGapStateProvider) GetMissingBlocksInRange(
	_ context.Context, network, processor string, minBlock, maxBlock uint64, limit int,
) ([]uint64, error) {
	m.lastMissingCall.network = network
	m.lastMissingCall.processor = processor
	m.lastMissingCall.minBlock = minBlock
	m.lastMissingCall.maxBlock = maxBlock
	m.lastMissingCall.limit = limit

	if m.getMissingErr != nil {
		return nil, m.getMissingErr
	}

	return m.missingBlocksInRange, nil
}

func (m *mockGapStateProvider) GetMinMaxStoredBlocks(
	_ context.Context, _, _ string,
) (*big.Int, *big.Int, error) {
	if m.getMinMaxErr != nil {
		return nil, nil, m.getMinMaxErr
	}

	return m.minStoredBlock, m.maxStoredBlock, nil
}

func TestGetGaps_FindsBothTypes(t *testing.T) {
	// Test that GetGaps returns both incomplete and missing blocks
	mockProvider := &mockGapStateProvider{
		minStoredBlock:          big.NewInt(5),
		maxStoredBlock:          big.NewInt(100),
		incompleteBlocksInRange: []uint64{8, 15},
		missingBlocksInRange:    []uint64{12, 20},
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

	result, err := limiter.GetGaps(ctx, currentBlock, lookbackRange, 100)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, []uint64{8, 15}, result.Incomplete)
	assert.Equal(t, []uint64{12, 20}, result.Missing)
	assert.True(t, result.ScanDuration > 0)
}

func TestGetGaps_OnlyIncomplete(t *testing.T) {
	// Test when only incomplete blocks exist (no missing)
	mockProvider := &mockGapStateProvider{
		minStoredBlock:          big.NewInt(1),
		maxStoredBlock:          big.NewInt(100),
		incompleteBlocksInRange: []uint64{5, 10, 15},
		missingBlocksInRange:    []uint64{},
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "mainnet",
		Processor:     "simple",
	}, LimiterConfig{MaxPendingBlockRange: 2})

	ctx := context.Background()
	result, err := limiter.GetGaps(ctx, 100, 0, 100)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, []uint64{5, 10, 15}, result.Incomplete)
	assert.Empty(t, result.Missing)
}

func TestGetGaps_OnlyMissing(t *testing.T) {
	// Test when only missing blocks exist (no incomplete)
	mockProvider := &mockGapStateProvider{
		minStoredBlock:          big.NewInt(1),
		maxStoredBlock:          big.NewInt(100),
		incompleteBlocksInRange: []uint64{},
		missingBlocksInRange:    []uint64{7, 14, 21},
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "mainnet",
		Processor:     "simple",
	}, LimiterConfig{MaxPendingBlockRange: 2})

	ctx := context.Background()
	result, err := limiter.GetGaps(ctx, 100, 0, 100)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Incomplete)
	assert.Equal(t, []uint64{7, 14, 21}, result.Missing)
}

func TestGetGaps_NoGaps(t *testing.T) {
	mockProvider := &mockGapStateProvider{
		minStoredBlock:          big.NewInt(1),
		maxStoredBlock:          big.NewInt(100),
		incompleteBlocksInRange: []uint64{},
		missingBlocksInRange:    []uint64{},
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "mainnet",
		Processor:     "simple",
	}, LimiterConfig{MaxPendingBlockRange: 2})

	ctx := context.Background()
	result, err := limiter.GetGaps(ctx, 100, 0, 100)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Incomplete)
	assert.Empty(t, result.Missing)
}

func TestGetGaps_ErrorFromGetIncomplete(t *testing.T) {
	expectedErr := errors.New("incomplete query failed")
	mockProvider := &mockGapStateProvider{
		minStoredBlock:   big.NewInt(1),
		maxStoredBlock:   big.NewInt(100),
		getIncompleteErr: expectedErr,
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "mainnet",
		Processor:     "simple",
	}, LimiterConfig{MaxPendingBlockRange: 2})

	ctx := context.Background()
	result, err := limiter.GetGaps(ctx, 100, 0, 100)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to get incomplete blocks")
}

func TestGetGaps_ErrorFromGetMissing(t *testing.T) {
	expectedErr := errors.New("missing query failed")
	mockProvider := &mockGapStateProvider{
		minStoredBlock:          big.NewInt(1),
		maxStoredBlock:          big.NewInt(100),
		incompleteBlocksInRange: []uint64{},
		getMissingErr:           expectedErr,
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "mainnet",
		Processor:     "simple",
	}, LimiterConfig{MaxPendingBlockRange: 2})

	ctx := context.Background()
	result, err := limiter.GetGaps(ctx, 100, 0, 100)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to get missing blocks")
}

func TestGetGaps_ErrorFromGetMinMax(t *testing.T) {
	expectedErr := errors.New("min/max query failed")
	mockProvider := &mockGapStateProvider{
		getMinMaxErr: expectedErr,
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "mainnet",
		Processor:     "simple",
	}, LimiterConfig{MaxPendingBlockRange: 2})

	ctx := context.Background()
	// lookbackRange=0 triggers GetMinMaxStoredBlocks call
	result, err := limiter.GetGaps(ctx, 100, 0, 100)

	require.Error(t, err)
	assert.Nil(t, result)
	assert.Contains(t, err.Error(), "failed to get min/max stored blocks")
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
	result, err := limiter.GetGaps(ctx, 100, 0, 100)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Incomplete)
	assert.Empty(t, result.Missing)
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
	result, err := limiter.GetGaps(ctx, 100, 0, 100)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "state provider does not support gap detection")
	assert.Nil(t, result)
}

func TestGetGaps_MaxPendingBlockRangeZero(t *testing.T) {
	// When maxPendingBlockRange is 0, no exclusion window should be applied
	mockProvider := &mockGapStateProvider{
		minStoredBlock:          big.NewInt(1),
		maxStoredBlock:          big.NewInt(100),
		incompleteBlocksInRange: []uint64{95, 98},
		missingBlocksInRange:    []uint64{99},
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "mainnet",
		Processor:     "simple",
	}, LimiterConfig{MaxPendingBlockRange: 0})

	ctx := context.Background()
	result, err := limiter.GetGaps(ctx, 100, 0, 100)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, []uint64{95, 98}, result.Incomplete)
	assert.Equal(t, []uint64{99}, result.Missing)
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
	result, err := limiter.GetGaps(ctx, 5, 0, 100)

	require.NoError(t, err)
	require.NotNil(t, result)
	// No scanning should occur when currentBlock <= maxPendingBlockRange
	assert.Empty(t, result.Incomplete)
	assert.Empty(t, result.Missing)
}

func TestGetGaps_ParameterValidation(t *testing.T) {
	// Verify correct parameters are passed to state provider
	mockProvider := &mockGapStateProvider{
		minStoredBlock:          big.NewInt(10),
		maxStoredBlock:          big.NewInt(100),
		incompleteBlocksInRange: []uint64{},
		missingBlocksInRange:    []uint64{},
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "testnet",
		Processor:     "structlog",
	}, LimiterConfig{MaxPendingBlockRange: 5})

	ctx := context.Background()
	// currentBlock=100, maxPendingBlockRange=5 means maxBlock=94
	// lookbackRange=0 uses minStored=10
	_, err := limiter.GetGaps(ctx, 100, 0, 50)

	require.NoError(t, err)

	// Verify incomplete call parameters
	assert.Equal(t, "testnet", mockProvider.lastIncompleteCall.network)
	assert.Equal(t, "structlog", mockProvider.lastIncompleteCall.processor)
	assert.Equal(t, uint64(10), mockProvider.lastIncompleteCall.minBlock)
	assert.Equal(t, uint64(94), mockProvider.lastIncompleteCall.maxBlock) // 100 - 5 - 1
	assert.Equal(t, 50, mockProvider.lastIncompleteCall.limit)

	// Verify missing call parameters
	assert.Equal(t, "testnet", mockProvider.lastMissingCall.network)
	assert.Equal(t, "structlog", mockProvider.lastMissingCall.processor)
	assert.Equal(t, uint64(10), mockProvider.lastMissingCall.minBlock)
	assert.Equal(t, uint64(94), mockProvider.lastMissingCall.maxBlock)
	assert.Equal(t, 50, mockProvider.lastMissingCall.limit) // Full limit since no incomplete found
}

func TestGetGaps_LimitSplitBetweenTypes(t *testing.T) {
	// Test that remaining limit is correctly calculated for missing blocks
	mockProvider := &mockGapStateProvider{
		minStoredBlock:          big.NewInt(1),
		maxStoredBlock:          big.NewInt(100),
		incompleteBlocksInRange: []uint64{5, 10, 15}, // 3 incomplete
		missingBlocksInRange:    []uint64{20, 25},    // Should only get limit-3=7 slots
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "mainnet",
		Processor:     "simple",
	}, LimiterConfig{MaxPendingBlockRange: 2})

	ctx := context.Background()
	result, err := limiter.GetGaps(ctx, 100, 0, 10)

	require.NoError(t, err)
	require.NotNil(t, result)

	// With limit=10 and 3 incomplete, missing should be called with limit=7
	assert.Equal(t, 7, mockProvider.lastMissingCall.limit)
}

func TestGetGaps_ScanDurationTracked(t *testing.T) {
	mockProvider := &mockGapStateProvider{
		minStoredBlock:          big.NewInt(1),
		maxStoredBlock:          big.NewInt(100),
		incompleteBlocksInRange: []uint64{},
		missingBlocksInRange:    []uint64{},
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "mainnet",
		Processor:     "simple",
	}, LimiterConfig{MaxPendingBlockRange: 2})

	ctx := context.Background()
	result, err := limiter.GetGaps(ctx, 100, 0, 100)

	require.NoError(t, err)
	require.NotNil(t, result)
	// Duration should be tracked even with no gaps found
	assert.True(t, result.ScanDuration >= 0)
}

func TestGetGaps_RespectsLookbackRange(t *testing.T) {
	mockProvider := &mockGapStateProvider{
		// minStoredBlock is always needed now to constrain the search range
		minStoredBlock:          big.NewInt(50), // Matches the calculated min from lookback
		maxStoredBlock:          big.NewInt(100),
		incompleteBlocksInRange: []uint64{75, 80},
		missingBlocksInRange:    []uint64{77},
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

	// With lookbackRange=50 and currentBlock=100, minBlock=50
	// With maxPendingBlockRange=2, maxBlock=97
	result, err := limiter.GetGaps(ctx, currentBlock, lookbackRange, 100)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, []uint64{75, 80}, result.Incomplete)
	assert.Equal(t, []uint64{77}, result.Missing)
}

func TestGetGaps_LookbackRangeGreaterThanCurrentBlock(t *testing.T) {
	// When lookbackRange is greater than currentBlock, minBlock should be 0
	// but constrained to minStoredBlock
	mockProvider := &mockGapStateProvider{
		incompleteBlocksInRange: []uint64{3},
		missingBlocksInRange:    []uint64{5},
		minStoredBlock:          big.NewInt(0), // Set min stored to 0 so gaps at 3,5 are valid
		maxStoredBlock:          big.NewInt(10),
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

	result, err := limiter.GetGaps(ctx, currentBlock, lookbackRange, 100)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, []uint64{3}, result.Incomplete)
	assert.Equal(t, []uint64{5}, result.Missing)
}

func TestGetGaps_MinBlockGreaterThanMaxBlock(t *testing.T) {
	// When minBlock > maxBlock, should return empty result
	mockProvider := &mockGapStateProvider{
		minStoredBlock: big.NewInt(95), // Min stored is 95
		maxStoredBlock: big.NewInt(100),
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "mainnet",
		Processor:     "simple",
	}, LimiterConfig{MaxPendingBlockRange: 10}) // Excludes 90-100

	ctx := context.Background()
	// minBlock=95, maxBlock=100-10-1=89
	// 95 > 89, so no scanning should occur
	result, err := limiter.GetGaps(ctx, 100, 0, 100)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Empty(t, result.Incomplete)
	assert.Empty(t, result.Missing)
}

func TestGetGaps_ExcludesMaxPendingBlockRangeWindow(t *testing.T) {
	// Verify that gaps within maxPendingBlockRange are NOT scanned
	// because they're already handled by IsBlockedByIncompleteBlocks
	mockProvider := &mockGapStateProvider{
		minStoredBlock:          big.NewInt(1),
		maxStoredBlock:          big.NewInt(100),
		incompleteBlocksInRange: []uint64{50},
		missingBlocksInRange:    []uint64{60},
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "mainnet",
		Processor:     "simple",
	}, LimiterConfig{MaxPendingBlockRange: 5})

	ctx := context.Background()

	// With maxPendingBlockRange=5, gap scanner searches [1, 94] (excludes 95-100)
	result, err := limiter.GetGaps(ctx, 100, 0, 100)

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, []uint64{50}, result.Incomplete)
	assert.Equal(t, []uint64{60}, result.Missing)

	// Verify maxBlock was correctly calculated
	assert.Equal(t, uint64(94), mockProvider.lastIncompleteCall.maxBlock)
	assert.Equal(t, uint64(94), mockProvider.lastMissingCall.maxBlock)
}

func TestGetGaps_LimitExhaustedByIncomplete(t *testing.T) {
	// Test when incomplete fills entire limit, missing should not be called
	mockProvider := &mockGapStateProvider{
		minStoredBlock:          big.NewInt(1),
		maxStoredBlock:          big.NewInt(100),
		incompleteBlocksInRange: []uint64{5, 10, 15, 20, 25}, // 5 items, fills limit
		missingBlocksInRange:    []uint64{30, 35},            // Should not be called
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.NewEntry(logrus.New()),
		StateProvider: mockProvider,
		Network:       "mainnet",
		Processor:     "simple",
	}, LimiterConfig{MaxPendingBlockRange: 2})

	ctx := context.Background()
	result, err := limiter.GetGaps(ctx, 100, 0, 5) // limit=5

	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Len(t, result.Incomplete, 5)

	// Missing should be called with limit=0, resulting in empty result
	assert.Equal(t, 0, mockProvider.lastMissingCall.limit)
}
