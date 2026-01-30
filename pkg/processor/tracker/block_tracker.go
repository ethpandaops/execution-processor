package tracker

import (
	"context"
	"fmt"
	"strconv"
	"strings"
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

// metaKeyPattern returns a pattern for scanning block_meta keys.
func (t *BlockCompletionTracker) metaKeyPattern(network, processor, mode string) string {
	if t.prefix == "" {
		return fmt.Sprintf("block_meta:%s:%s:%s:*", processor, network, mode)
	}

	return fmt.Sprintf("%s:block_meta:%s:%s:%s:*", t.prefix, processor, network, mode)
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

	pipe := t.redis.Pipeline()
	pipe.Del(ctx, completedKey)                                    // Clear old completions
	pipe.Set(ctx, expectedKey, expectedCount, DefaultBlockMetaTTL) // Set expected count
	pipe.HSet(ctx, metaKey, map[string]any{
		"enqueued_at": time.Now().Unix(),
		"queue":       queue,
		"expected":    expectedCount,
	})
	pipe.Expire(ctx, metaKey, DefaultBlockMetaTTL)

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
func (t *BlockCompletionTracker) TrackTaskCompletion(
	ctx context.Context,
	taskID string,
	blockNum uint64,
	network, processor, mode string,
) (bool, error) {
	completedKey := t.completedKey(network, processor, mode, blockNum)
	expectedKey := t.expectedKey(network, processor, mode, blockNum)

	// Add to completed set (idempotent - same task completing twice is fine)
	// Set TTL to ensure cleanup if block never completes
	pipe := t.redis.Pipeline()
	pipe.SAdd(ctx, completedKey, taskID)
	pipe.Expire(ctx, completedKey, DefaultBlockMetaTTL)

	if _, err := pipe.Exec(ctx); err != nil {
		return false, fmt.Errorf("failed to add task to completed set: %w", err)
	}

	// Get counts
	completedCount, err := t.redis.SCard(ctx, completedKey).Result()
	if err != nil {
		return false, fmt.Errorf("failed to get completed count: %w", err)
	}

	expectedStr, err := t.redis.Get(ctx, expectedKey).Result()
	if err == redis.Nil {
		// Block not registered - might be old task from before retry, or already cleaned up
		t.log.WithFields(logrus.Fields{
			"block_number": blockNum,
			"task_id":      taskID,
			"network":      network,
			"processor":    processor,
			"mode":         mode,
		}).Debug("Block not registered for completion tracking (may be old task or already complete)")

		return false, nil
	}

	if err != nil {
		return false, fmt.Errorf("failed to get expected count: %w", err)
	}

	expected, err := strconv.ParseInt(expectedStr, 10, 64)
	if err != nil {
		return false, fmt.Errorf("failed to parse expected count: %w", err)
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

	if err := t.redis.Del(ctx, completedKey, expectedKey, metaKey).Err(); err != nil {
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
func (t *BlockCompletionTracker) GetStaleBlocks(
	ctx context.Context,
	network, processor, mode string,
) ([]uint64, error) {
	pattern := t.metaKeyPattern(network, processor, mode)
	staleBlocks := make([]uint64, 0)

	iter := t.redis.Scan(ctx, 0, pattern, 100).Iterator()
	for iter.Next(ctx) {
		key := iter.Val()

		enqueuedAtStr, err := t.redis.HGet(ctx, key, "enqueued_at").Result()
		if err != nil {
			t.log.WithError(err).WithField("key", key).Debug("Failed to get enqueued_at")

			continue
		}

		enqueuedAt, err := strconv.ParseInt(enqueuedAtStr, 10, 64)
		if err != nil {
			t.log.WithError(err).WithField("key", key).Debug("Failed to parse enqueued_at")

			continue
		}

		if time.Since(time.Unix(enqueuedAt, 0)) > t.config.StaleThreshold {
			// Extract block number from key
			blockNum := extractBlockNumFromKey(key)
			if blockNum != 0 {
				staleBlocks = append(staleBlocks, blockNum)
			}
		}
	}

	if err := iter.Err(); err != nil {
		return nil, fmt.Errorf("failed to scan for stale blocks: %w", err)
	}

	return staleBlocks, nil
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

	if err := t.redis.Del(ctx, completedKey, expectedKey, metaKey).Err(); err != nil {
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

// extractBlockNumFromKey extracts the block number from a Redis key.
// Key format: block_meta:{prefix}:{processor}:{network}:{mode}:{blockNum}
// or: block_meta:{processor}:{network}:{mode}:{blockNum}
func extractBlockNumFromKey(key string) uint64 {
	parts := strings.Split(key, ":")
	if len(parts) < 2 {
		return 0
	}

	// Block number is always the last part
	blockNumStr := parts[len(parts)-1]

	blockNum, err := strconv.ParseUint(blockNumStr, 10, 64)
	if err != nil {
		return 0
	}

	return blockNum
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
