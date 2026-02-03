package tracker

import (
	"context"
	"fmt"
	"sync"
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

func TestBlockCompletionTracker_PendingBlocksSortedSet(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)

	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	tracker := NewBlockCompletionTracker(
		client, "", logrus.New(), &mockStateProviderForTracker{},
		BlockCompletionTrackerConfig{StaleThreshold: 5 * time.Minute},
	)

	ctx := context.Background()
	network := "test_network"
	processor := "test_processor"
	mode := "forwards"

	t.Run("RegisterBlock adds to sorted set", func(t *testing.T) {
		blockNum := uint64(100)

		err := tracker.RegisterBlock(ctx, blockNum, 5, network, processor, mode, "test_queue")
		require.NoError(t, err)

		// Verify block is in sorted set
		pendingKey := fmt.Sprintf("pending_blocks:%s:%s:%s", processor, network, mode)
		score, err := client.ZScore(ctx, pendingKey, "100").Result()
		require.NoError(t, err)
		assert.Greater(t, score, float64(0), "score should be a positive timestamp")
	})

	t.Run("MarkBlockComplete removes from sorted set", func(t *testing.T) {
		blockNum := uint64(101)

		err := tracker.RegisterBlock(ctx, blockNum, 1, network, processor, mode, "test_queue")
		require.NoError(t, err)

		// Verify block is in sorted set
		pendingKey := fmt.Sprintf("pending_blocks:%s:%s:%s", processor, network, mode)
		exists, err := client.ZScore(ctx, pendingKey, "101").Result()
		require.NoError(t, err)
		assert.Greater(t, exists, float64(0))

		// Mark complete
		err = tracker.MarkBlockComplete(ctx, blockNum, network, processor, mode)
		require.NoError(t, err)

		// Verify block is removed from sorted set
		_, err = client.ZScore(ctx, pendingKey, "101").Result()
		assert.ErrorIs(t, err, redis.Nil, "block should be removed from sorted set")
	})

	t.Run("ClearBlock removes from sorted set", func(t *testing.T) {
		blockNum := uint64(102)

		err := tracker.RegisterBlock(ctx, blockNum, 1, network, processor, mode, "test_queue")
		require.NoError(t, err)

		// Verify block is in sorted set
		pendingKey := fmt.Sprintf("pending_blocks:%s:%s:%s", processor, network, mode)
		_, err = client.ZScore(ctx, pendingKey, "102").Result()
		require.NoError(t, err)

		// Clear block
		err = tracker.ClearBlock(ctx, blockNum, network, processor, mode)
		require.NoError(t, err)

		// Verify block is removed from sorted set
		_, err = client.ZScore(ctx, pendingKey, "102").Result()
		assert.ErrorIs(t, err, redis.Nil, "block should be removed from sorted set")
	})
}

func TestBlockCompletionTracker_GetStaleBlocks_SortedSet(t *testing.T) {
	ctx := context.Background()
	network := "test_network"
	processor := "test_processor"
	mode := "forwards"

	t.Run("returns empty when no blocks registered", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)

		defer mr.Close()

		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		defer client.Close()

		tracker := NewBlockCompletionTracker(
			client, "", logrus.New(), &mockStateProviderForTracker{},
			BlockCompletionTrackerConfig{StaleThreshold: 100 * time.Millisecond},
		)

		staleBlocks, err := tracker.GetStaleBlocks(ctx, network, processor, mode)
		require.NoError(t, err)
		assert.Empty(t, staleBlocks)
	})

	t.Run("returns stale blocks only", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)

		defer mr.Close()

		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		defer client.Close()

		// Use a longer stale threshold to ensure fresh blocks are not marked stale
		staleThreshold := 1 * time.Minute
		tracker := NewBlockCompletionTracker(
			client, "", logrus.New(), &mockStateProviderForTracker{},
			BlockCompletionTrackerConfig{StaleThreshold: staleThreshold},
		)

		// Register blocks with old timestamps directly in Redis
		pendingKey := fmt.Sprintf("pending_blocks:%s:%s:%s", processor, network, mode)
		oldTimestamp := float64(time.Now().Add(-5 * time.Minute).Unix())
		// Fresh timestamp should be well within the stale threshold
		newTimestamp := float64(time.Now().Unix())

		// Add stale block (old timestamp - 5 minutes ago, well past 1 minute threshold)
		err = client.ZAdd(ctx, pendingKey, redis.Z{Score: oldTimestamp, Member: "200"}).Err()
		require.NoError(t, err)

		// Add fresh block (current timestamp - not past 1 minute threshold)
		err = client.ZAdd(ctx, pendingKey, redis.Z{Score: newTimestamp, Member: "201"}).Err()
		require.NoError(t, err)

		staleBlocks, err := tracker.GetStaleBlocks(ctx, network, processor, mode)
		require.NoError(t, err)

		assert.Len(t, staleBlocks, 1)
		assert.Contains(t, staleBlocks, uint64(200))
		assert.NotContains(t, staleBlocks, uint64(201))
	})

	t.Run("blocks become stale after threshold", func(t *testing.T) {
		mr, err := miniredis.Run()
		require.NoError(t, err)

		defer mr.Close()

		client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
		defer client.Close()

		// Use 1 second threshold for reliable testing
		staleThreshold := 1 * time.Second
		tracker := NewBlockCompletionTracker(
			client, "", logrus.New(), &mockStateProviderForTracker{},
			BlockCompletionTrackerConfig{StaleThreshold: staleThreshold},
		)

		blockNum := uint64(300)

		err = tracker.RegisterBlock(ctx, blockNum, 1, network, processor, mode, "test_queue")
		require.NoError(t, err)

		// Initially not stale (we just registered it, well within 1 second)
		staleBlocks, err := tracker.GetStaleBlocks(ctx, network, processor, mode)
		require.NoError(t, err)
		assert.NotContains(t, staleBlocks, blockNum, "block should not be stale immediately after registration")

		// Wait for stale threshold
		time.Sleep(staleThreshold + 100*time.Millisecond)

		// Now stale
		staleBlocks, err = tracker.GetStaleBlocks(ctx, network, processor, mode)
		require.NoError(t, err)
		assert.Contains(t, staleBlocks, blockNum, "block should be stale after threshold")
	})
}

func TestBlockCompletionTracker_TrackTaskCompletion_LuaScript(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)

	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	tracker := NewBlockCompletionTracker(
		client, "", logrus.New(), &mockStateProviderForTracker{},
		BlockCompletionTrackerConfig{},
	)

	ctx := context.Background()
	network := "test_network"
	processor := "test_processor"
	mode := "forwards"
	blockNum := uint64(500)

	t.Run("returns false when block not registered", func(t *testing.T) {
		complete, err := tracker.TrackTaskCompletion(ctx, "task1", blockNum, network, processor, mode)
		require.NoError(t, err)
		assert.False(t, complete)
	})

	t.Run("tracks completion correctly", func(t *testing.T) {
		err := tracker.RegisterBlock(ctx, blockNum, 3, network, processor, mode, "test_queue")
		require.NoError(t, err)

		// First task
		complete, err := tracker.TrackTaskCompletion(ctx, "task1", blockNum, network, processor, mode)
		require.NoError(t, err)
		assert.False(t, complete)

		// Second task
		complete, err = tracker.TrackTaskCompletion(ctx, "task2", blockNum, network, processor, mode)
		require.NoError(t, err)
		assert.False(t, complete)

		// Third task - should complete
		complete, err = tracker.TrackTaskCompletion(ctx, "task3", blockNum, network, processor, mode)
		require.NoError(t, err)
		assert.True(t, complete)
	})

	t.Run("is idempotent", func(t *testing.T) {
		blockNum2 := uint64(501)

		err := tracker.RegisterBlock(ctx, blockNum2, 2, network, processor, mode, "test_queue")
		require.NoError(t, err)

		// Track same task twice
		complete, err := tracker.TrackTaskCompletion(ctx, "task1", blockNum2, network, processor, mode)
		require.NoError(t, err)
		assert.False(t, complete)

		complete, err = tracker.TrackTaskCompletion(ctx, "task1", blockNum2, network, processor, mode)
		require.NoError(t, err)
		assert.False(t, complete, "duplicate task should not increase count")

		// Second unique task should complete
		complete, err = tracker.TrackTaskCompletion(ctx, "task2", blockNum2, network, processor, mode)
		require.NoError(t, err)
		assert.True(t, complete)
	})
}

func TestBlockCompletionTracker_ClearStaleBlocks_Bulk(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)

	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	staleThreshold := 50 * time.Millisecond
	tracker := NewBlockCompletionTracker(
		client, "", logrus.New(), &mockStateProviderForTracker{},
		BlockCompletionTrackerConfig{StaleThreshold: staleThreshold},
	)

	ctx := context.Background()
	network := "test_network"
	processor := "test_processor"
	mode := "forwards"

	t.Run("returns 0 when no stale blocks", func(t *testing.T) {
		cleared, err := tracker.ClearStaleBlocks(ctx, network, processor, mode)
		require.NoError(t, err)
		assert.Equal(t, 0, cleared)
	})

	t.Run("clears stale blocks and their keys", func(t *testing.T) {
		// Register multiple blocks
		for i := uint64(600); i < 605; i++ {
			err := tracker.RegisterBlock(ctx, i, 1, network, processor, mode, "test_queue")
			require.NoError(t, err)
		}

		// Wait for blocks to become stale
		time.Sleep(staleThreshold + 50*time.Millisecond)

		// Verify blocks are stale
		staleBlocks, err := tracker.GetStaleBlocks(ctx, network, processor, mode)
		require.NoError(t, err)
		assert.Len(t, staleBlocks, 5)

		// Clear stale blocks
		cleared, err := tracker.ClearStaleBlocks(ctx, network, processor, mode)
		require.NoError(t, err)
		assert.Equal(t, 5, cleared)

		// Verify blocks are cleared
		staleBlocks, err = tracker.GetStaleBlocks(ctx, network, processor, mode)
		require.NoError(t, err)
		assert.Empty(t, staleBlocks)

		// Verify keys are deleted
		for i := uint64(600); i < 605; i++ {
			hasTracking, err := tracker.HasBlockTracking(ctx, i, network, processor, mode)
			require.NoError(t, err)
			assert.False(t, hasTracking, "block %d should not have tracking", i)
		}
	})
}

func TestBlockCompletionTracker_LuaScript_Concurrent(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)

	defer mr.Close()

	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer client.Close()

	tracker := NewBlockCompletionTracker(
		client, "", logrus.New(), &mockStateProviderForTracker{},
		BlockCompletionTrackerConfig{},
	)

	ctx := context.Background()
	network := "test_network"
	processor := "test_processor"
	mode := "forwards"
	blockNum := uint64(700)
	expectedTasks := 100

	err = tracker.RegisterBlock(ctx, blockNum, expectedTasks, network, processor, mode, "test_queue")
	require.NoError(t, err)

	// Track completions concurrently
	var (
		wg              sync.WaitGroup
		completionCount int
		mu              sync.Mutex
	)

	for i := 0; i < expectedTasks; i++ {
		wg.Add(1)

		go func(taskNum int) {
			defer wg.Done()

			taskID := fmt.Sprintf("task%d", taskNum)

			complete, trackErr := tracker.TrackTaskCompletion(ctx, taskID, blockNum, network, processor, mode)
			require.NoError(t, trackErr)

			if complete {
				mu.Lock()

				completionCount++

				mu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Exactly one goroutine should have seen the completion
	assert.Equal(t, 1, completionCount, "exactly one task should report completion")

	// Verify final state
	completed, expected, _, err := tracker.GetBlockStatus(ctx, blockNum, network, processor, mode)
	require.NoError(t, err)
	assert.Equal(t, int64(expectedTasks), completed)
	assert.Equal(t, int64(expectedTasks), expected)
}

func TestBlockCompletionTracker_PendingBlocksKey_WithPrefix(t *testing.T) {
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

	ctx := context.Background()
	network := "test_network"
	processor := "test_processor"
	mode := "forwards"
	blockNum := uint64(800)

	err = tracker.RegisterBlock(ctx, blockNum, 1, network, processor, mode, "test_queue")
	require.NoError(t, err)

	// Verify the key includes the prefix
	pendingKey := fmt.Sprintf("%s:pending_blocks:%s:%s:%s", prefix, processor, network, mode)
	score, err := client.ZScore(ctx, pendingKey, "800").Result()
	require.NoError(t, err)
	assert.Greater(t, score, float64(0))
}
