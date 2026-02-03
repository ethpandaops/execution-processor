package tracker

import (
	"context"
	"fmt"
	"math/big"

	"github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/sirupsen/logrus"
)

// StateProvider defines the state manager methods needed by Limiter.
type StateProvider interface {
	GetOldestIncompleteBlock(ctx context.Context, network, processor string, minBlockNumber uint64) (*uint64, error)
	GetNewestIncompleteBlock(ctx context.Context, network, processor string, maxBlockNumber uint64) (*uint64, error)
	MarkBlockComplete(ctx context.Context, blockNumber uint64, network, processor string) error
}

// GapStateProvider extends StateProvider with gap detection capabilities.
type GapStateProvider interface {
	StateProvider
	GetIncompleteBlocksInRange(ctx context.Context, network, processor string, minBlock, maxBlock uint64, limit int) ([]uint64, error)
	GetMinMaxStoredBlocks(ctx context.Context, network, processor string) (*big.Int, *big.Int, error)
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

// GetGaps returns incomplete blocks outside the maxPendingBlockRange window.
// If lookbackRange is 0, scans from the oldest stored block.
// This performs a full-range scan for gap detection, excluding the recent window
// that is already handled by IsBlockedByIncompleteBlocks.
func (l *Limiter) GetGaps(ctx context.Context, currentBlock uint64, lookbackRange uint64, limit int) ([]uint64, error) {
	gapProvider, ok := l.stateProvider.(GapStateProvider)
	if !ok {
		return nil, fmt.Errorf("state provider does not support gap detection")
	}

	var minBlock uint64

	if lookbackRange == 0 {
		// Unlimited: scan from oldest stored block
		minStored, _, err := gapProvider.GetMinMaxStoredBlocks(ctx, l.network, l.processor)
		if err != nil {
			return nil, fmt.Errorf("failed to get min stored block: %w", err)
		}

		if minStored == nil {
			// No blocks stored yet
			return nil, nil
		}

		minBlock = minStored.Uint64()
	} else {
		// Limited: scan from currentBlock - lookbackRange
		if currentBlock > lookbackRange {
			minBlock = currentBlock - lookbackRange
		}
	}

	// Calculate maxBlock to exclude the window handled by IsBlockedByIncompleteBlocks.
	// The limiter already handles blocks within [currentBlock - maxPendingBlockRange, currentBlock],
	// so we only scan up to (currentBlock - maxPendingBlockRange - 1) to avoid double work.
	maxBlock := currentBlock

	if l.config.MaxPendingBlockRange > 0 {
		exclusionWindow := uint64(l.config.MaxPendingBlockRange) //nolint:gosec // validated in config

		if currentBlock > exclusionWindow {
			maxBlock = currentBlock - exclusionWindow - 1
		} else {
			// Current block is within the exclusion window, nothing to scan
			return nil, nil
		}
	}

	// Ensure minBlock doesn't exceed maxBlock
	if minBlock > maxBlock {
		return nil, nil
	}

	gaps, err := gapProvider.GetIncompleteBlocksInRange(
		ctx, l.network, l.processor,
		minBlock, maxBlock, limit,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get incomplete blocks in range: %w", err)
	}

	if len(gaps) > 0 {
		l.log.WithFields(logrus.Fields{
			"min_block": minBlock,
			"max_block": maxBlock,
			"gap_count": len(gaps),
			"first_gap": gaps[0],
		}).Debug("Found gaps in block range")
	}

	return gaps, nil
}
