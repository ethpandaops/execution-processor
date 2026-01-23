package tracker

import (
	"context"
	"errors"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/ethereum"
)

// IsBlockNotFoundError checks if an error indicates a block was not found.
// Uses errors.Is for sentinel errors, with fallback to string matching for wrapped errors.
func IsBlockNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	// Check for sentinel error first
	if errors.Is(err, ethereum.ErrBlockNotFound) {
		return true
	}

	// Fallback to string matching for errors from external clients
	errStr := strings.ToLower(err.Error())

	return strings.Contains(errStr, "not found") ||
		strings.Contains(errStr, "unknown block") ||
		strings.Contains(errStr, "block not found") ||
		strings.Contains(errStr, "header not found")
}

// TrackBlockCompletion decrements the pending task count and marks the block complete when all tasks finish.
func (l *Limiter) TrackBlockCompletion(ctx context.Context, blockNumber uint64, mode string) {
	remaining, err := l.pendingTracker.DecrementPending(ctx, blockNumber, l.network, l.processor, mode)
	if err != nil {
		l.log.WithError(err).WithFields(logrus.Fields{
			"block_number": blockNumber,
			"mode":         mode,
		}).Warn("Failed to decrement pending count")

		return
	}

	// If all tasks are complete, mark the block as complete
	if remaining <= 0 {
		if err := l.stateProvider.MarkBlockComplete(ctx, blockNumber, l.network, l.processor); err != nil {
			l.log.WithError(err).WithFields(logrus.Fields{
				"block_number": blockNumber,
			}).Error("Failed to mark block complete")

			return
		}

		// Cleanup Redis tracking key
		if err := l.pendingTracker.CleanupBlock(ctx, blockNumber, l.network, l.processor, mode); err != nil {
			l.log.WithError(err).WithFields(logrus.Fields{
				"block_number": blockNumber,
			}).Warn("Failed to cleanup block tracking")
		}

		l.log.WithFields(logrus.Fields{
			"block_number": blockNumber,
			"mode":         mode,
		}).Debug("Block marked complete - all tasks finished")
	}
}
