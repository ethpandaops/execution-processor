package tracker

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockStateProviderForTracker implements StateProvider for testing.
type mockStateProviderForTracker struct {
	oldestIncomplete *uint64
	newestIncomplete *uint64
	markCompleteErr  error
}

func (m *mockStateProviderForTracker) GetOldestIncompleteBlock(
	_ context.Context, _, _ string, _ uint64,
) (*uint64, error) {
	return m.oldestIncomplete, nil
}

func (m *mockStateProviderForTracker) GetNewestIncompleteBlock(
	_ context.Context, _, _ string, _ uint64,
) (*uint64, error) {
	return m.newestIncomplete, nil
}

func (m *mockStateProviderForTracker) MarkBlockComplete(
	_ context.Context, _ uint64, _, _ string,
) error {
	return m.markCompleteErr
}

func TestBlockCompletionTracker_HasBlockTracking(t *testing.T) {
	tests := []struct {
		name        string
		setupRedis  func(mr *miniredis.Miniredis, blockNum uint64)
		blockNum    uint64
		wantTracked bool
	}{
		{
			name:     "returns true when block_meta key exists",
			blockNum: 100,
			setupRedis: func(mr *miniredis.Miniredis, blockNum uint64) {
				mr.HSet(
					fmt.Sprintf("block_meta:test_processor:test_network:forwards:%d", blockNum),
					"enqueued_at", fmt.Sprintf("%d", time.Now().Unix()),
				)
			},
			wantTracked: true,
		},
		{
			name:        "returns false when block_meta key does not exist",
			blockNum:    100,
			setupRedis:  func(_ *miniredis.Miniredis, _ uint64) {},
			wantTracked: false,
		},
		{
			name:     "returns false for different block number",
			blockNum: 100,
			setupRedis: func(mr *miniredis.Miniredis, _ uint64) {
				// Set up tracking for a DIFFERENT block
				mr.HSet(
					"block_meta:test_processor:test_network:forwards:999",
					"enqueued_at", fmt.Sprintf("%d", time.Now().Unix()),
				)
			},
			wantTracked: false,
		},
		{
			name:     "returns true for block with prefix",
			blockNum: 100,
			setupRedis: func(mr *miniredis.Miniredis, blockNum uint64) {
				// Note: prefix is empty in this test, so key format doesn't include prefix
				mr.HSet(
					fmt.Sprintf("block_meta:test_processor:test_network:forwards:%d", blockNum),
					"enqueued_at", fmt.Sprintf("%d", time.Now().Unix()),
				)
			},
			wantTracked: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mr, err := miniredis.Run()
			require.NoError(t, err)

			defer mr.Close()

			client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
			defer client.Close()

			tracker := NewBlockCompletionTracker(
				client, "", logrus.New(), &mockStateProviderForTracker{},
				BlockCompletionTrackerConfig{},
			)

			tt.setupRedis(mr, tt.blockNum)

			got, err := tracker.HasBlockTracking(
				context.Background(), tt.blockNum,
				"test_network", "test_processor", "forwards",
			)

			require.NoError(t, err)
			assert.Equal(t, tt.wantTracked, got)
		})
	}
}

func TestBlockCompletionTracker_HasBlockTracking_WithPrefix(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)

	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	prefix := "myapp"
	tracker := NewBlockCompletionTracker(
		client, prefix, logrus.New(), &mockStateProviderForTracker{},
		BlockCompletionTrackerConfig{},
	)

	// Set up tracking with prefix
	blockNum := uint64(100)
	mr.HSet(
		fmt.Sprintf("%s:block_meta:test_processor:test_network:forwards:%d", prefix, blockNum),
		"enqueued_at", fmt.Sprintf("%d", time.Now().Unix()),
	)

	got, err := tracker.HasBlockTracking(
		context.Background(), blockNum,
		"test_network", "test_processor", "forwards",
	)

	require.NoError(t, err)
	assert.True(t, got)
}

func TestBlockCompletionTracker_HasBlockTracking_AfterRegisterBlock(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)

	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	tracker := NewBlockCompletionTracker(
		client, "", logrus.New(), &mockStateProviderForTracker{},
		BlockCompletionTrackerConfig{},
	)

	blockNum := uint64(100)
	network := "test_network"
	processor := "test_processor"
	mode := "forwards"

	// Verify block has no tracking initially
	hasTracking, err := tracker.HasBlockTracking(context.Background(), blockNum, network, processor, mode)
	require.NoError(t, err)
	assert.False(t, hasTracking, "block should not have tracking before RegisterBlock")

	// Register the block
	err = tracker.RegisterBlock(context.Background(), blockNum, 5, network, processor, mode, "test_queue")
	require.NoError(t, err)

	// Verify block now has tracking
	hasTracking, err = tracker.HasBlockTracking(context.Background(), blockNum, network, processor, mode)
	require.NoError(t, err)
	assert.True(t, hasTracking, "block should have tracking after RegisterBlock")
}

func TestOrphanedBlockRecoveryFlow(t *testing.T) {
	// This test simulates the full flow:
	// 1. Block is in ClickHouse (complete=0) - simulated by mockStateProvider
	// 2. Redis tracking is missing (orphaned)
	// 3. IsBlockedByIncompleteBlocks returns blocked + blocking block
	// 4. HasBlockTracking returns false
	// 5. RegisterBlock is called (simulating ReprocessBlock)
	// 6. Block now has tracking and will complete normally
	mr, err := miniredis.Run()
	require.NoError(t, err)

	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	orphanedBlockNum := uint64(100)
	nextBlockNum := uint64(110)

	mockState := &mockStateProviderForLimiter{
		oldestIncomplete: &orphanedBlockNum,
	}

	limiter := NewLimiter(&LimiterDeps{
		Log:           logrus.New(),
		StateProvider: mockState,
		Network:       "test",
		Processor:     "test",
	}, LimiterConfig{MaxPendingBlockRange: 5})

	tracker := NewBlockCompletionTracker(
		client, "", logrus.New(), mockState,
		BlockCompletionTrackerConfig{},
	)

	// Step 1: Check if blocked
	blocked, blockingBlock, err := limiter.IsBlockedByIncompleteBlocks(
		context.Background(), nextBlockNum, FORWARDS_MODE,
	)
	require.NoError(t, err)
	assert.True(t, blocked, "should be blocked by incomplete block")
	require.NotNil(t, blockingBlock)
	assert.Equal(t, orphanedBlockNum, *blockingBlock)

	// Step 2: Check if blocking block is orphaned
	hasTracking, err := tracker.HasBlockTracking(
		context.Background(), *blockingBlock,
		"test", "test", FORWARDS_MODE,
	)
	require.NoError(t, err)
	assert.False(t, hasTracking, "blocking block should be orphaned (no Redis tracking)")

	// Step 3: Simulate ReprocessBlock - register the block
	err = tracker.RegisterBlock(
		context.Background(), *blockingBlock, 3,
		"test", "test", FORWARDS_MODE, "test_queue",
	)
	require.NoError(t, err)

	// Step 4: Verify block is no longer orphaned
	hasTracking, err = tracker.HasBlockTracking(
		context.Background(), *blockingBlock,
		"test", "test", FORWARDS_MODE,
	)
	require.NoError(t, err)
	assert.True(t, hasTracking, "block should have tracking after reprocessing")
}

// mockStateProviderForLimiter implements StateProvider for limiter testing.
type mockStateProviderForLimiter struct {
	oldestIncomplete *uint64
	newestIncomplete *uint64
}

func (m *mockStateProviderForLimiter) GetOldestIncompleteBlock(
	_ context.Context, _, _ string, _ uint64,
) (*uint64, error) {
	return m.oldestIncomplete, nil
}

func (m *mockStateProviderForLimiter) GetNewestIncompleteBlock(
	_ context.Context, _, _ string, _ uint64,
) (*uint64, error) {
	return m.newestIncomplete, nil
}

func (m *mockStateProviderForLimiter) MarkBlockComplete(
	_ context.Context, _ uint64, _, _ string,
) error {
	return nil
}
