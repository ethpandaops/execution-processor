package structlog

import (
	"context"
	"fmt"

	"github.com/0xsequence/ethkit/go-ethereum/core/types"
	"github.com/sirupsen/logrus"
)

// processTransactionWithLargeTxHandling processes a transaction with large transaction lock management
func (p *Processor) processTransactionWithLargeTxHandling(ctx context.Context, block *types.Block, index int, tx *types.Transaction, txHash string) (int, error) {
	if p.largeTxLock == nil || !p.largeTxLock.config.Enabled {
		// Large transaction handling not enabled, process normally
		return p.ProcessSingleTransaction(ctx, block, index, tx)
	}

	// First, we need to check the size by doing a quick trace extraction
	// This is not ideal but necessary to determine if it's a large transaction
	// In production, you might want to cache this or estimate based on gas usage
	node := p.pool.GetHealthyExecutionNode()
	if node == nil {
		return 0, fmt.Errorf("no healthy execution node available")
	}

	// Get trace to count structlogs
	trace, err := node.DebugTraceTransaction(ctx, tx.Hash().String(), block.Number())
	if err != nil {
		return 0, fmt.Errorf("failed to get trace for size check: %w", err)
	}

	structlogCount := len(trace.Structlogs)
	isLargeTx := p.largeTxLock.IsLargeTransaction(structlogCount)

	p.log.WithFields(logrus.Fields{
		"tx_hash":         txHash,
		"structlog_count": structlogCount,
		"is_large_tx":     isLargeTx,
		"threshold":       p.largeTxLock.config.StructlogThreshold,
	}).Debug("Checked transaction size")

	if isLargeTx && p.largeTxLock.config.EnableSequentialMode {
		// This is a large transaction, acquire lock
		if err := p.largeTxLock.AcquireLock(ctx, txHash, structlogCount); err != nil {
			// Failed to acquire lock within timeout
			p.log.WithError(err).WithFields(logrus.Fields{
				"tx_hash":         txHash,
				"structlog_count": structlogCount,
			}).Warn("Failed to acquire large transaction lock")

			return 0, err
		}
		defer p.largeTxLock.ReleaseLock(txHash)

		// Process the large transaction
		return p.ProcessSingleTransaction(ctx, block, index, tx)
	} else if !isLargeTx && p.largeTxLock.active.Load() {
		// Normal transaction but a large transaction is being processed
		// Wait for it to complete
		if err := p.largeTxLock.WaitForLargeTransaction(ctx, txHash); err != nil {
			// Failed to wait, return error to retry later
			p.log.WithError(err).WithFields(logrus.Fields{
				"tx_hash":         txHash,
				"structlog_count": structlogCount,
			}).Warn("Failed to wait for large transaction")

			return 0, err
		}

		// Large transaction completed, process normally
		return p.ProcessSingleTransaction(ctx, block, index, tx)
	}

	// Normal transaction with no large transaction active
	return p.ProcessSingleTransaction(ctx, block, index, tx)
}
