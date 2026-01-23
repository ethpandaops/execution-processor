package tracker

import (
	"context"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestRedis(t *testing.T) (*redis.Client, func()) {
	t.Helper()

	mr, err := miniredis.Run()
	require.NoError(t, err)

	client := redis.NewClient(&redis.Options{
		Addr: mr.Addr(),
	})

	return client, func() {
		client.Close()
		mr.Close()
	}
}

func TestPendingTracker_InitBlock(t *testing.T) {
	ctx := context.Background()

	redisClient, cleanup := setupTestRedis(t)
	defer cleanup()

	log := logrus.New()
	tracker := NewPendingTracker(redisClient, "test", log)

	err := tracker.InitBlock(ctx, 100, 5, "mainnet", "test_processor", "forwards")
	require.NoError(t, err)

	// Verify the value was set
	count, err := tracker.GetPendingCount(ctx, 100, "mainnet", "test_processor", "forwards")
	require.NoError(t, err)
	assert.Equal(t, int64(5), count)
}

func TestPendingTracker_DecrementPending(t *testing.T) {
	ctx := context.Background()

	redisClient, cleanup := setupTestRedis(t)
	defer cleanup()

	log := logrus.New()
	tracker := NewPendingTracker(redisClient, "test", log)

	// Initialize with 3 tasks
	err := tracker.InitBlock(ctx, 100, 3, "mainnet", "test_processor", "forwards")
	require.NoError(t, err)

	// Decrement once
	remaining, err := tracker.DecrementPending(ctx, 100, "mainnet", "test_processor", "forwards")
	require.NoError(t, err)
	assert.Equal(t, int64(2), remaining)

	// Decrement again
	remaining, err = tracker.DecrementPending(ctx, 100, "mainnet", "test_processor", "forwards")
	require.NoError(t, err)
	assert.Equal(t, int64(1), remaining)

	// Decrement to zero
	remaining, err = tracker.DecrementPending(ctx, 100, "mainnet", "test_processor", "forwards")
	require.NoError(t, err)
	assert.Equal(t, int64(0), remaining)
}

func TestPendingTracker_DecrementToZero(t *testing.T) {
	ctx := context.Background()

	redisClient, cleanup := setupTestRedis(t)
	defer cleanup()

	log := logrus.New()
	tracker := NewPendingTracker(redisClient, "test", log)

	// Initialize with 1 task
	err := tracker.InitBlock(ctx, 100, 1, "mainnet", "test_processor", "forwards")
	require.NoError(t, err)

	// Decrement to zero
	remaining, err := tracker.DecrementPending(ctx, 100, "mainnet", "test_processor", "forwards")
	require.NoError(t, err)
	assert.Equal(t, int64(0), remaining)
}

func TestPendingTracker_GetPendingCount(t *testing.T) {
	ctx := context.Background()

	redisClient, cleanup := setupTestRedis(t)
	defer cleanup()

	log := logrus.New()
	tracker := NewPendingTracker(redisClient, "test", log)

	// Key doesn't exist - should return 0
	count, err := tracker.GetPendingCount(ctx, 999, "mainnet", "test_processor", "forwards")
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)

	// Initialize and check
	err = tracker.InitBlock(ctx, 100, 10, "mainnet", "test_processor", "forwards")
	require.NoError(t, err)

	count, err = tracker.GetPendingCount(ctx, 100, "mainnet", "test_processor", "forwards")
	require.NoError(t, err)
	assert.Equal(t, int64(10), count)
}

func TestPendingTracker_CleanupBlock(t *testing.T) {
	ctx := context.Background()

	redisClient, cleanup := setupTestRedis(t)
	defer cleanup()

	log := logrus.New()
	tracker := NewPendingTracker(redisClient, "test", log)

	// Initialize
	err := tracker.InitBlock(ctx, 100, 5, "mainnet", "test_processor", "forwards")
	require.NoError(t, err)

	// Verify it exists
	count, err := tracker.GetPendingCount(ctx, 100, "mainnet", "test_processor", "forwards")
	require.NoError(t, err)
	assert.Equal(t, int64(5), count)

	// Cleanup
	err = tracker.CleanupBlock(ctx, 100, "mainnet", "test_processor", "forwards")
	require.NoError(t, err)

	// Verify it's gone
	count, err = tracker.GetPendingCount(ctx, 100, "mainnet", "test_processor", "forwards")
	require.NoError(t, err)
	assert.Equal(t, int64(0), count)
}

func TestPendingTracker_KeyPattern(t *testing.T) {
	log := logrus.New()

	tests := []struct {
		name        string
		prefix      string
		blockNumber uint64
		network     string
		processor   string
		mode        string
		expectedKey string
	}{
		{
			name:        "with prefix",
			prefix:      "myapp",
			blockNumber: 12345,
			network:     "mainnet",
			processor:   "test_processor",
			mode:        "forwards",
			expectedKey: "myapp:block:mainnet:test_processor:forwards:12345",
		},
		{
			name:        "without prefix",
			prefix:      "",
			blockNumber: 12345,
			network:     "mainnet",
			processor:   "test_processor",
			mode:        "backwards",
			expectedKey: "block:mainnet:test_processor:backwards:12345",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tracker := NewPendingTracker(nil, tt.prefix, log)
			key := tracker.blockKey(tt.blockNumber, tt.network, tt.processor, tt.mode)
			assert.Equal(t, tt.expectedKey, key)
		})
	}
}

func TestPendingTracker_MultipleBlocks(t *testing.T) {
	ctx := context.Background()

	redisClient, cleanup := setupTestRedis(t)
	defer cleanup()

	log := logrus.New()
	tracker := NewPendingTracker(redisClient, "test", log)

	// Initialize multiple blocks
	require.NoError(t, tracker.InitBlock(ctx, 100, 5, "mainnet", "test_processor", "forwards"))
	require.NoError(t, tracker.InitBlock(ctx, 101, 3, "mainnet", "test_processor", "forwards"))
	require.NoError(t, tracker.InitBlock(ctx, 102, 7, "mainnet", "test_processor", "forwards"))

	// Verify all blocks have correct counts
	count, err := tracker.GetPendingCount(ctx, 100, "mainnet", "test_processor", "forwards")
	require.NoError(t, err)
	assert.Equal(t, int64(5), count)

	count, err = tracker.GetPendingCount(ctx, 101, "mainnet", "test_processor", "forwards")
	require.NoError(t, err)
	assert.Equal(t, int64(3), count)

	count, err = tracker.GetPendingCount(ctx, 102, "mainnet", "test_processor", "forwards")
	require.NoError(t, err)
	assert.Equal(t, int64(7), count)

	// Decrement one block
	remaining, err := tracker.DecrementPending(ctx, 101, "mainnet", "test_processor", "forwards")
	require.NoError(t, err)
	assert.Equal(t, int64(2), remaining)

	// Other blocks unchanged
	count, err = tracker.GetPendingCount(ctx, 100, "mainnet", "test_processor", "forwards")
	require.NoError(t, err)
	assert.Equal(t, int64(5), count)

	count, err = tracker.GetPendingCount(ctx, 102, "mainnet", "test_processor", "forwards")
	require.NoError(t, err)
	assert.Equal(t, int64(7), count)
}
