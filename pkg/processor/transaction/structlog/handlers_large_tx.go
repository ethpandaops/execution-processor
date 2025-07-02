package structlog

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/0xsequence/ethkit/go-ethereum/core/types"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

// processTransactionWithLargeTxHandling processes a transaction with large transaction lock management
func (p *Processor) processTransactionWithLargeTxHandling(ctx context.Context, block *types.Block, index int, tx *types.Transaction, txHash string) (int, error) {
	if p.largeTxLock == nil || !p.largeTxLock.config.Enabled {
		// Large transaction handling not enabled, process normally
		return p.ProcessSingleTransaction(ctx, block, index, tx)
	}

	// Check if this transaction already holds the lock (for retries)
	if p.largeTxLock.HasLock(txHash) {
		// This transaction already has the lock, process it
		defer p.largeTxLock.ReleaseLock(txHash)

		return p.ProcessSingleTransaction(ctx, block, index, tx)
	}

	// Check if we need to determine the transaction size first
	if p.largeTxLock.config.EnableSequentialMode {
		// We need to check the size before deciding whether to wait or acquire lock
		// Get a quick trace to count structlogs
		node := p.pool.GetHealthyExecutionNode()
		if node == nil {
			return 0, fmt.Errorf("no healthy execution node available")
		}

		// Use a short timeout for the size check
		sizeCheckCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		trace, err := node.DebugTraceTransaction(sizeCheckCtx, tx.Hash().String(), block.Number(), execution.DefaultTraceOptions())
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

		if isLargeTx {
			// This is a large transaction, acquire lock
			if err := p.largeTxLock.AcquireLock(ctx, txHash, structlogCount, "process"); err != nil {
				p.log.WithError(err).WithFields(logrus.Fields{
					"tx_hash":         txHash,
					"structlog_count": structlogCount,
				}).Warn("Failed to acquire large transaction lock")

				return 0, err
			}
			defer p.largeTxLock.ReleaseLock(txHash)

			// Process the large transaction
			return p.ProcessSingleTransaction(ctx, block, index, tx)
		}

		// Not a large transaction, but check if we need to wait for an active large tx
		if p.largeTxLock.active.Load() {
			// Wait for the large transaction to complete before processing
			if err := p.largeTxLock.WaitForLargeTransaction(ctx, txHash); err != nil {
				p.log.WithError(err).WithFields(logrus.Fields{
					"tx_hash": txHash,
				}).Warn("Failed to wait for large transaction")

				return 0, err
			}
		}
	}

	// Process the transaction normally
	return p.ProcessSingleTransaction(ctx, block, index, tx)
}

// verifyTransactionWithLargeTxHandling verifies a transaction with large transaction lock management
func (p *Processor) verifyTransactionWithLargeTxHandling(ctx context.Context, blockNumber *big.Int, transactionHash string, transactionIndex uint32, networkName string, insertedCount int) error {
	if p.largeTxLock == nil || !p.largeTxLock.config.Enabled {
		// Large transaction handling not enabled, verify normally
		return p.VerifyTransaction(ctx, blockNumber, transactionHash, transactionIndex, networkName, insertedCount)
	}

	// Path 1: Early detection based on insertedCount
	isPotentiallyLarge := insertedCount >= p.largeTxLock.config.StructlogThreshold

	p.log.WithFields(logrus.Fields{
		"tx_hash":              transactionHash,
		"inserted_count":       insertedCount,
		"is_potentially_large": isPotentiallyLarge,
		"threshold":            p.largeTxLock.config.StructlogThreshold,
	}).Debug("Checking if transaction is potentially large for verification")

	if isPotentiallyLarge && p.largeTxLock.config.EnableSequentialMode {
		// This is potentially a large transaction, acquire lock before verification
		if err := p.largeTxLock.AcquireLock(ctx, transactionHash, insertedCount, "verify"); err != nil {
			// Failed to acquire lock within timeout
			p.log.WithError(err).WithFields(logrus.Fields{
				"tx_hash":        transactionHash,
				"inserted_count": insertedCount,
			}).Warn("Failed to acquire large transaction lock for verification")

			return err
		}
		defer p.largeTxLock.ReleaseLock(transactionHash)

		// Verify the transaction (will re-trace)
		return p.VerifyTransaction(ctx, blockNumber, transactionHash, transactionIndex, networkName, insertedCount)
	} else if !isPotentiallyLarge && p.largeTxLock.active.Load() {
		// Normal transaction but a large transaction is being processed
		// Wait for it to complete
		if err := p.largeTxLock.WaitForLargeTransaction(ctx, transactionHash); err != nil {
			// Failed to wait, return error to retry later
			p.log.WithError(err).WithFields(logrus.Fields{
				"tx_hash":        transactionHash,
				"inserted_count": insertedCount,
			}).Warn("Failed to wait for large transaction during verification")

			return err
		}

		// Large transaction completed, verify normally
		return p.VerifyTransaction(ctx, blockNumber, transactionHash, transactionIndex, networkName, insertedCount)
	}

	// Normal transaction with no large transaction active
	// OR potentially large but sequential mode disabled
	return p.VerifyTransaction(ctx, blockNumber, transactionHash, transactionIndex, networkName, insertedCount)
}
