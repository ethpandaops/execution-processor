package tracker

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// PendingTracker tracks pending tasks per block using Redis.
// Used for two-phase completion tracking to know when all tasks for a block are complete.
type PendingTracker struct {
	redis  *redis.Client
	prefix string
	log    logrus.FieldLogger
}

// NewPendingTracker creates a new PendingTracker.
func NewPendingTracker(redisClient *redis.Client, prefix string, log logrus.FieldLogger) *PendingTracker {
	return &PendingTracker{
		redis:  redisClient,
		prefix: prefix,
		log:    log.WithField("component", "pending_tracker"),
	}
}

// blockKey returns the Redis key for tracking a block's pending task count.
// Key pattern: {prefix}:block:{network}:{processor}:{mode}:{block_number}.
func (t *PendingTracker) blockKey(blockNumber uint64, network, processor, mode string) string {
	if t.prefix == "" {
		return fmt.Sprintf("block:%s:%s:%s:%d", network, processor, mode, blockNumber)
	}

	return fmt.Sprintf("%s:block:%s:%s:%s:%d", t.prefix, network, processor, mode, blockNumber)
}

// ErrBlockAlreadyBeingProcessed is returned when attempting to initialize a block that is already being processed.
var ErrBlockAlreadyBeingProcessed = fmt.Errorf("block is already being processed")

// InitBlock initializes tracking for a block with the given task count.
// Uses SetNX to ensure only one processor can claim a block at a time.
// Returns ErrBlockAlreadyBeingProcessed if the block is already being tracked.
// This should be called BEFORE MarkBlockEnqueued to prevent race conditions.
func (t *PendingTracker) InitBlock(ctx context.Context, blockNumber uint64, taskCount int, network, processor, mode string) error {
	key := t.blockKey(blockNumber, network, processor, mode)

	// Use SetNX to atomically check-and-set - only succeeds if key doesn't exist
	// TTL of 30 minutes prevents orphaned keys if processor crashes
	wasSet, err := t.redis.SetNX(ctx, key, taskCount, 30*time.Minute).Result()
	if err != nil {
		return fmt.Errorf("failed to init block tracking: %w", err)
	}

	if !wasSet {
		t.log.WithFields(logrus.Fields{
			"block_number": blockNumber,
			"network":      network,
			"processor":    processor,
			"mode":         mode,
			"key":          key,
		}).Debug("Block already being processed by another worker")

		return ErrBlockAlreadyBeingProcessed
	}

	t.log.WithFields(logrus.Fields{
		"block_number": blockNumber,
		"task_count":   taskCount,
		"network":      network,
		"processor":    processor,
		"mode":         mode,
		"key":          key,
	}).Debug("Initialized block tracking")

	return nil
}

// DecrementPending decrements the pending task count for a block.
// Returns the remaining count after decrement.
func (t *PendingTracker) DecrementPending(ctx context.Context, blockNumber uint64, network, processor, mode string) (int64, error) {
	key := t.blockKey(blockNumber, network, processor, mode)

	remaining, err := t.redis.DecrBy(ctx, key, 1).Result()
	if err != nil {
		return 0, fmt.Errorf("failed to decrement pending count: %w", err)
	}

	t.log.WithFields(logrus.Fields{
		"block_number": blockNumber,
		"remaining":    remaining,
		"network":      network,
		"processor":    processor,
		"mode":         mode,
	}).Trace("Decremented pending task count")

	return remaining, nil
}

// GetPendingCount returns the current pending task count for a block.
// Returns 0 if the key doesn't exist (block not being tracked or already cleaned up).
func (t *PendingTracker) GetPendingCount(ctx context.Context, blockNumber uint64, network, processor, mode string) (int64, error) {
	key := t.blockKey(blockNumber, network, processor, mode)

	val, err := t.redis.Get(ctx, key).Result()
	if err == redis.Nil {
		return 0, nil
	}

	if err != nil {
		return 0, fmt.Errorf("failed to get pending count: %w", err)
	}

	count, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse pending count: %w", err)
	}

	return count, nil
}

// CleanupBlock removes the tracking key for a block.
// Should be called after a block is marked complete.
func (t *PendingTracker) CleanupBlock(ctx context.Context, blockNumber uint64, network, processor, mode string) error {
	key := t.blockKey(blockNumber, network, processor, mode)

	err := t.redis.Del(ctx, key).Err()
	if err != nil {
		return fmt.Errorf("failed to cleanup block tracking: %w", err)
	}

	t.log.WithFields(logrus.Fields{
		"block_number": blockNumber,
		"network":      network,
		"processor":    processor,
		"mode":         mode,
	}).Debug("Cleaned up block tracking")

	return nil
}
