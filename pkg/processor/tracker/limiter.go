package tracker

import (
	"context"
	"fmt"

	"github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/sirupsen/logrus"
)

// StateProvider defines the state manager methods needed by Limiter.
type StateProvider interface {
	GetOldestIncompleteBlock(ctx context.Context, network, processor string, minBlockNumber uint64) (*uint64, error)
	GetNewestIncompleteBlock(ctx context.Context, network, processor string, maxBlockNumber uint64) (*uint64, error)
	MarkBlockComplete(ctx context.Context, blockNumber uint64, network, processor string) error
}

// LimiterConfig holds configuration for the Limiter.
type LimiterConfig struct {
	MaxPendingBlockRange int
}

// LimiterDeps holds dependencies for the Limiter.
type LimiterDeps struct {
	Log           logrus.FieldLogger
	StateProvider StateProvider
	Network       string
	Processor     string
}

// Limiter provides shared blocking and completion functionality for processors.
type Limiter struct {
	log           logrus.FieldLogger
	stateProvider StateProvider
	config        LimiterConfig
	network       string
	processor     string
}

// NewLimiter creates a new Limiter.
func NewLimiter(deps *LimiterDeps, config LimiterConfig) *Limiter {
	return &Limiter{
		log:           deps.Log,
		stateProvider: deps.StateProvider,
		config:        config,
		network:       deps.Network,
		processor:     deps.Processor,
	}
}

// IsBlockedByIncompleteBlocks checks if processing should be blocked based on distance
// from the oldest/newest incomplete block (depending on processing mode).
// Returns: blocked status, blocking block number (if blocked), error.
// The blocking block number can be used to check if the block is orphaned (no Redis tracking).
func (l *Limiter) IsBlockedByIncompleteBlocks(
	ctx context.Context,
	nextBlock uint64,
	mode string,
) (bool, *uint64, error) {
	// Safe conversion: MaxPendingBlockRange is validated to be > 0 during config validation
	if l.config.MaxPendingBlockRange <= 0 {
		return false, nil, nil
	}

	maxPendingBlockRange := uint64(l.config.MaxPendingBlockRange) //nolint:gosec // validated above

	if mode == BACKWARDS_MODE {
		// Backwards mode: check distance from newest incomplete block
		// Search range: nextBlock to nextBlock + maxPendingBlockRange
		searchMaxBlock := nextBlock + maxPendingBlockRange

		newestIncomplete, err := l.stateProvider.GetNewestIncompleteBlock(
			ctx, l.network, l.processor, searchMaxBlock,
		)
		if err != nil {
			return false, nil, err
		}

		if newestIncomplete != nil && (*newestIncomplete-nextBlock) >= maxPendingBlockRange {
			l.log.WithFields(logrus.Fields{
				"next_block":              nextBlock,
				"newest_incomplete":       *newestIncomplete,
				"distance":                *newestIncomplete - nextBlock,
				"max_pending_block_range": maxPendingBlockRange,
			}).Debug("Max pending block range reached (backwards), waiting for tasks to complete")

			common.BlockProcessingSkipped.WithLabelValues(l.network, l.processor, "max_pending_block_range").Inc()

			return true, newestIncomplete, nil
		}
	} else {
		// Forwards mode: check distance from oldest incomplete block
		// Search range: nextBlock - maxPendingBlockRange to nextBlock
		var searchMinBlock uint64
		if nextBlock > maxPendingBlockRange {
			searchMinBlock = nextBlock - maxPendingBlockRange
		}

		oldestIncomplete, err := l.stateProvider.GetOldestIncompleteBlock(
			ctx, l.network, l.processor, searchMinBlock,
		)
		if err != nil {
			return false, nil, err
		}

		if oldestIncomplete != nil && (nextBlock-*oldestIncomplete) >= maxPendingBlockRange {
			l.log.WithFields(logrus.Fields{
				"next_block":              nextBlock,
				"oldest_incomplete":       *oldestIncomplete,
				"distance":                nextBlock - *oldestIncomplete,
				"max_pending_block_range": maxPendingBlockRange,
			}).Debug("Max pending block range reached, waiting for tasks to complete")

			common.BlockProcessingSkipped.WithLabelValues(l.network, l.processor, "max_pending_block_range").Inc()

			return true, oldestIncomplete, nil
		}
	}

	return false, nil, nil
}

// GetAvailableCapacity returns how many more blocks can be enqueued before hitting
// the maxPendingBlockRange limit. Returns 0 if at or over capacity.
func (l *Limiter) GetAvailableCapacity(ctx context.Context, nextBlock uint64, mode string) (int, error) {
	if l.config.MaxPendingBlockRange <= 0 {
		// No limit configured, return max capacity
		return l.config.MaxPendingBlockRange, nil
	}

	maxPendingBlockRange := uint64(l.config.MaxPendingBlockRange) //nolint:gosec // validated above

	if mode == BACKWARDS_MODE {
		// Backwards mode: check distance from newest incomplete block
		searchMaxBlock := nextBlock + maxPendingBlockRange

		newestIncomplete, err := l.stateProvider.GetNewestIncompleteBlock(
			ctx, l.network, l.processor, searchMaxBlock,
		)
		if err != nil {
			return 0, err
		}

		if newestIncomplete == nil {
			// No incomplete blocks, full capacity available
			return l.config.MaxPendingBlockRange, nil
		}

		distance := *newestIncomplete - nextBlock
		if distance >= maxPendingBlockRange {
			return 0, nil
		}

		//nolint:gosec // Result is bounded by MaxPendingBlockRange which is an int
		return int(maxPendingBlockRange - distance), nil
	}

	// Forwards mode: check distance from oldest incomplete block
	var searchMinBlock uint64
	if nextBlock > maxPendingBlockRange {
		searchMinBlock = nextBlock - maxPendingBlockRange
	}

	oldestIncomplete, err := l.stateProvider.GetOldestIncompleteBlock(
		ctx, l.network, l.processor, searchMinBlock,
	)
	if err != nil {
		return 0, err
	}

	if oldestIncomplete == nil {
		// No incomplete blocks, full capacity available
		return l.config.MaxPendingBlockRange, nil
	}

	distance := nextBlock - *oldestIncomplete
	if distance >= maxPendingBlockRange {
		return 0, nil
	}

	//nolint:gosec // Result is bounded by MaxPendingBlockRange which is an int
	return int(maxPendingBlockRange - distance), nil
}

// ValidateBatchWithinLeash ensures a batch of blocks won't exceed the maxPendingBlockRange.
// Returns an error if the batch would violate the constraint.
func (l *Limiter) ValidateBatchWithinLeash(ctx context.Context, startBlock uint64, count int, mode string) error {
	if l.config.MaxPendingBlockRange <= 0 || count <= 0 {
		return nil
	}

	// The batch spans from startBlock to startBlock + count - 1
	// We need to ensure this range doesn't exceed maxPendingBlockRange
	if count > l.config.MaxPendingBlockRange {
		return fmt.Errorf("batch size %d exceeds max pending block range %d", count, l.config.MaxPendingBlockRange)
	}

	// Check available capacity
	capacity, err := l.GetAvailableCapacity(ctx, startBlock, mode)
	if err != nil {
		return fmt.Errorf("failed to get available capacity: %w", err)
	}

	if count > capacity {
		return fmt.Errorf("batch size %d exceeds available capacity %d", count, capacity)
	}

	return nil
}
