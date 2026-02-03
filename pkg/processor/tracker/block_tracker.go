package tracker

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// Default configuration values for BlockCompletionTracker.
const (
	// DefaultBlockMetaTTL is the default TTL for block tracking keys.
	DefaultBlockMetaTTL = 30 * time.Minute

	// DefaultStaleThreshold is the default time after which a block is considered stale.
	DefaultStaleThreshold = 5 * time.Minute
)

// trackTaskCompletionScript is a Lua script for atomic task completion tracking.
// It adds a task to the completed set and returns both the completed count and expected count
// in a single round trip, ensuring atomicity.
//
//nolint:gochecknoglobals // Lua scripts are safely shared across goroutines
var trackTaskCompletionScript = redis.NewScript(`
local completedKey = KEYS[1]
local expectedKey = KEYS[2]
local taskID = ARGV[1]
local ttlSeconds = tonumber(ARGV[2])

redis.call('SADD', completedKey, taskID)
redis.call('EXPIRE', completedKey, ttlSeconds)

local completedCount = redis.call('SCARD', completedKey)
local expectedStr = redis.call('GET', expectedKey)

if expectedStr == false then
    return {completedCount, -1}
end

return {completedCount, tonumber(expectedStr)}
`)

// BlockCompletionTrackerConfig holds configuration for the BlockCompletionTracker.
type BlockCompletionTrackerConfig struct {
	// StaleThreshold is the time after which a block is considered stale.
	StaleThreshold time.Duration

	// AutoRetryStale enables automatic retry of stale blocks.
	AutoRetryStale bool
}

// BlockCompletionTracker tracks block completion using Redis SETs for task deduplication.
// This replaces the counter-based PendingTracker with a SET-based approach that:
// - Uses asynq.TaskID() for deterministic task deduplication
// - Tracks completed taskIDs in a Redis SET (idempotent SADD)
// - Stores expected count and enqueued_at metadata
// - Supports stale block detection for auto-retry.
type BlockCompletionTracker struct {
	redis  *redis.Client
	prefix string
	log    logrus.FieldLogger
	config BlockCompletionTrackerConfig

	stateProvider StateProvider // For ClickHouse writes
}

// NewBlockCompletionTracker creates a new BlockCompletionTracker.
func NewBlockCompletionTracker(
	redisClient *redis.Client,
	prefix string,
	log logrus.FieldLogger,
	stateProvider StateProvider,
	config BlockCompletionTrackerConfig,
) *BlockCompletionTracker {
	// Apply defaults
	if config.StaleThreshold == 0 {
		config.StaleThreshold = DefaultStaleThreshold
	}

	return &BlockCompletionTracker{
		redis:         redisClient,
		prefix:        prefix,
		log:           log.WithField("component", "block_completion_tracker"),
		config:        config,
		stateProvider: stateProvider,
	}
}

// Redis key patterns:
// - {prefix}:completed:{processor}:{network}:{mode}:{blockNum} -> SET of completed taskIDs
// - {prefix}:expected:{processor}:{network}:{mode}:{blockNum}  -> STRING expected count
// - {prefix}:block_meta:{processor}:{network}:{mode}:{blockNum} -> HASH with enqueued_at, queue

func (t *BlockCompletionTracker) completedKey(network, processor, mode string, blockNum uint64) string {
	if t.prefix == "" {
		return fmt.Sprintf("completed:%s:%s:%s:%d", processor, network, mode, blockNum)
	}

	return fmt.Sprintf("%s:completed:%s:%s:%s:%d", t.prefix, processor, network, mode, blockNum)
}

func (t *BlockCompletionTracker) expectedKey(network, processor, mode string, blockNum uint64) string {
	if t.prefix == "" {
		return fmt.Sprintf("expected:%s:%s:%s:%d", processor, network, mode, blockNum)
	}

	return fmt.Sprintf("%s:expected:%s:%s:%s:%d", t.prefix, processor, network, mode, blockNum)
}

func (t *BlockCompletionTracker) metaKey(network, processor, mode string, blockNum uint64) string {
	if t.prefix == "" {
		return fmt.Sprintf("block_meta:%s:%s:%s:%d", processor, network, mode, blockNum)
	}

	return fmt.Sprintf("%s:block_meta:%s:%s:%s:%d", t.prefix, processor, network, mode, blockNum)
}

// pendingBlocksKey returns the key for the sorted set of pending blocks.
// The sorted set uses enqueue timestamps as scores for O(log N) stale detection.
func (t *BlockCompletionTracker) pendingBlocksKey(network, processor, mode string) string {
	if t.prefix == "" {
		return fmt.Sprintf("pending_blocks:%s:%s:%s", processor, network, mode)
	}

	return fmt.Sprintf("%s:pending_blocks:%s:%s:%s", t.prefix, processor, network, mode)
}

// RegisterBlock initializes tracking for a new block.
// Clears any existing completion data (safe for retries).
// This should be called AFTER MarkBlockEnqueued to ensure ClickHouse has the record.
func (t *BlockCompletionTracker) RegisterBlock(
	ctx context.Context,
	blockNum uint64,
	expectedCount int,
	network, processor, mode, queue string,
) error {
	completedKey := t.completedKey(network, processor, mode, blockNum)
	expectedKey := t.expectedKey(network, processor, mode, blockNum)
	metaKey := t.metaKey(network, processor, mode, blockNum)
	pendingKey := t.pendingBlocksKey(network, processor, mode)

	now := time.Now()

	pipe := t.redis.Pipeline()
	pipe.Del(ctx, completedKey)                                    // Clear old completions
	pipe.Set(ctx, expectedKey, expectedCount, DefaultBlockMetaTTL) // Set expected count
	pipe.HSet(ctx, metaKey, map[string]any{
		"enqueued_at": now.Unix(),
		"queue":       queue,
		"expected":    expectedCount,
	})
	pipe.Expire(ctx, metaKey, DefaultBlockMetaTTL)
	// Add to pending blocks sorted set with timestamp as score for O(log N) stale detection
	pipe.ZAdd(ctx, pendingKey, redis.Z{
		Score:  float64(now.Unix()),
		Member: blockNum,
	})

	_, err := pipe.Exec(ctx)
	if err != nil {
		return fmt.Errorf("failed to register block: %w", err)
	}

	t.log.WithFields(logrus.Fields{
		"block_number":   blockNum,
		"expected_count": expectedCount,
		"network":        network,
		"processor":      processor,
		"mode":           mode,
		"queue":          queue,
	}).Debug("Registered block for completion tracking")

	return nil
}

// TrackTaskCompletion records a task completion and checks if block is done.
// Returns true if all tasks are now complete.
// Uses a Lua script for atomic completion tracking in a single round trip.
func (t *BlockCompletionTracker) TrackTaskCompletion(
	ctx context.Context,
	taskID string,
	blockNum uint64,
	network, processor, mode string,
) (bool, error) {
	completedKey := t.completedKey(network, processor, mode, blockNum)
	expectedKey := t.expectedKey(network, processor, mode, blockNum)

	// Execute Lua script for atomic task completion tracking
	result, err := trackTaskCompletionScript.Run(ctx, t.redis,
		[]string{completedKey, expectedKey},
		taskID, int(DefaultBlockMetaTTL.Seconds()),
	).Slice()
	if err != nil {
		return false, fmt.Errorf("failed to track task completion: %w", err)
	}

	if len(result) != 2 {
		return false, fmt.Errorf("unexpected result length from Lua script: %d", len(result))
	}

	completedCount, ok := result[0].(int64)
	if !ok {
		return false, fmt.Errorf("failed to parse completed count from Lua script result")
	}

	expected, ok := result[1].(int64)
	if !ok {
		return false, fmt.Errorf("failed to parse expected count from Lua script result")
	}

	// -1 indicates block not registered (expected key doesn't exist)
	if expected == -1 {
		t.log.WithFields(logrus.Fields{
			"block_number": blockNum,
			"task_id":      taskID,
			"network":      network,
			"processor":    processor,
			"mode":         mode,
		}).Debug("Block not registered for completion tracking (may be old task or already complete)")

		return false, nil
	}

	t.log.WithFields(logrus.Fields{
		"block_number": blockNum,
		"task_id":      taskID,
		"completed":    completedCount,
		"expected":     expected,
		"network":      network,
		"processor":    processor,
		"mode":         mode,
	}).Debug("Tracked task completion")

	return completedCount >= expected, nil
}

// MarkBlockComplete writes to ClickHouse and cleans up Redis.
func (t *BlockCompletionTracker) MarkBlockComplete(
	ctx context.Context,
	blockNum uint64,
	network, processor, mode string,
) error {
	// Write to ClickHouse
	if err := t.stateProvider.MarkBlockComplete(ctx, blockNum, network, processor); err != nil {
		return fmt.Errorf("failed to mark block complete in ClickHouse: %w", err)
	}

	// Cleanup Redis keys
	completedKey := t.completedKey(network, processor, mode, blockNum)
	expectedKey := t.expectedKey(network, processor, mode, blockNum)
	metaKey := t.metaKey(network, processor, mode, blockNum)
	pendingKey := t.pendingBlocksKey(network, processor, mode)

	pipe := t.redis.Pipeline()
	pipe.Del(ctx, completedKey, expectedKey, metaKey)
	pipe.ZRem(ctx, pendingKey, blockNum)

	if _, err := pipe.Exec(ctx); err != nil {
		// Log but don't fail - keys will expire anyway
		t.log.WithError(err).WithFields(logrus.Fields{
			"block_number": blockNum,
			"network":      network,
			"processor":    processor,
			"mode":         mode,
		}).Warn("Failed to cleanup Redis keys after block completion")
	}

	t.log.WithFields(logrus.Fields{
		"block_number": blockNum,
		"network":      network,
		"processor":    processor,
		"mode":         mode,
	}).Debug("Block marked complete - all tasks finished")

	return nil
}

// GetStaleBlocks returns blocks that have been processing longer than the stale threshold.
// Uses ZRANGEBYSCORE on the pending blocks sorted set for O(log N + M) complexity.
func (t *BlockCompletionTracker) GetStaleBlocks(
	ctx context.Context,
	network, processor, mode string,
) ([]uint64, error) {
	pendingKey := t.pendingBlocksKey(network, processor, mode)
	staleThreshold := time.Now().Add(-t.config.StaleThreshold).Unix()

	members, err := t.redis.ZRangeByScore(ctx, pendingKey, &redis.ZRangeBy{
		Min: "-inf",
		Max: fmt.Sprintf("%d", staleThreshold),
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("failed to get stale blocks: %w", err)
	}

	staleBlocks := make([]uint64, 0, len(members))

	for _, member := range members {
		blockNum, err := strconv.ParseUint(member, 10, 64)
		if err != nil {
			t.log.WithError(err).WithField("member", member).Debug("Failed to parse block number")

			continue
		}

		staleBlocks = append(staleBlocks, blockNum)
	}

	return staleBlocks, nil
}

// ClearStaleBlocks removes all stale blocks and their associated tracking data.
// Uses ZRANGEBYSCORE to identify stale blocks and a pipeline to efficiently delete all related keys.
// Returns the number of blocks cleared.
func (t *BlockCompletionTracker) ClearStaleBlocks(
	ctx context.Context,
	network, processor, mode string,
) (int, error) {
	staleBlocks, err := t.GetStaleBlocks(ctx, network, processor, mode)
	if err != nil {
		return 0, fmt.Errorf("failed to get stale blocks: %w", err)
	}

	if len(staleBlocks) == 0 {
		return 0, nil
	}

	pendingKey := t.pendingBlocksKey(network, processor, mode)
	pipe := t.redis.Pipeline()

	// Collect all keys to delete and members to remove from sorted set
	members := make([]any, 0, len(staleBlocks))

	for _, blockNum := range staleBlocks {
		completedKey := t.completedKey(network, processor, mode, blockNum)
		expectedKey := t.expectedKey(network, processor, mode, blockNum)
		metaKey := t.metaKey(network, processor, mode, blockNum)

		pipe.Del(ctx, completedKey, expectedKey, metaKey)

		members = append(members, blockNum)
	}

	// Remove all stale blocks from the sorted set in one operation
	pipe.ZRem(ctx, pendingKey, members...)

	if _, err := pipe.Exec(ctx); err != nil {
		return 0, fmt.Errorf("failed to clear stale blocks: %w", err)
	}

	t.log.WithFields(logrus.Fields{
		"count":     len(staleBlocks),
		"network":   network,
		"processor": processor,
		"mode":      mode,
	}).Debug("Cleared stale blocks")

	return len(staleBlocks), nil
}

// GetBlockStatus returns the completion status of a block.
func (t *BlockCompletionTracker) GetBlockStatus(
	ctx context.Context,
	blockNum uint64,
	network, processor, mode string,
) (completed int64, expected int64, enqueuedAt time.Time, err error) {
	completedKey := t.completedKey(network, processor, mode, blockNum)
	expectedKey := t.expectedKey(network, processor, mode, blockNum)
	metaKey := t.metaKey(network, processor, mode, blockNum)

	pipe := t.redis.Pipeline()
	completedCmd := pipe.SCard(ctx, completedKey)
	expectedCmd := pipe.Get(ctx, expectedKey)
	enqueuedAtCmd := pipe.HGet(ctx, metaKey, "enqueued_at")

	_, err = pipe.Exec(ctx)
	if err != nil && err != redis.Nil {
		return 0, 0, time.Time{}, fmt.Errorf("failed to get block status: %w", err)
	}

	completed, _ = completedCmd.Result()

	expectedStr, _ := expectedCmd.Result()
	if expectedStr != "" {
		expected, _ = strconv.ParseInt(expectedStr, 10, 64)
	}

	enqueuedAtStr, _ := enqueuedAtCmd.Result()
	if enqueuedAtStr != "" {
		enqueuedAtUnix, _ := strconv.ParseInt(enqueuedAtStr, 10, 64)
		enqueuedAt = time.Unix(enqueuedAtUnix, 0)
	}

	return completed, expected, enqueuedAt, nil
}

// ClearBlock removes all tracking data for a block (used when retrying).
func (t *BlockCompletionTracker) ClearBlock(
	ctx context.Context,
	blockNum uint64,
	network, processor, mode string,
) error {
	completedKey := t.completedKey(network, processor, mode, blockNum)
	expectedKey := t.expectedKey(network, processor, mode, blockNum)
	metaKey := t.metaKey(network, processor, mode, blockNum)
	pendingKey := t.pendingBlocksKey(network, processor, mode)

	pipe := t.redis.Pipeline()
	pipe.Del(ctx, completedKey, expectedKey, metaKey)
	pipe.ZRem(ctx, pendingKey, blockNum)

	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to clear block tracking: %w", err)
	}

	t.log.WithFields(logrus.Fields{
		"block_number": blockNum,
		"network":      network,
		"processor":    processor,
		"mode":         mode,
	}).Debug("Cleared block tracking data")

	return nil
}

// HasBlockTracking checks if a block has Redis tracking data.
// Returns true if block_meta key exists (block is being tracked).
// Used to detect orphaned blocks that are in ClickHouse (complete=0) but have no Redis tracking.
func (t *BlockCompletionTracker) HasBlockTracking(
	ctx context.Context,
	blockNum uint64,
	network, processor, mode string,
) (bool, error) {
	metaKey := t.metaKey(network, processor, mode, blockNum)

	exists, err := t.redis.Exists(ctx, metaKey).Result()
	if err != nil {
		return false, fmt.Errorf("failed to check block tracking: %w", err)
	}

	return exists > 0, nil
}

// GenerateTaskID creates a deterministic task ID for deduplication.
// Format: {processor}:{network}:{blockNum}:{identifier}
// For transaction-based processors: identifier = txHash.
// For block-based processors: identifier = "block".
func GenerateTaskID(processor, network string, blockNum uint64, identifier string) string {
	return fmt.Sprintf("%s:%s:%d:%s", processor, network, blockNum, identifier)
}
