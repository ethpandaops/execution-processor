package structlog

import (
	"context"
	"math/big"

	"github.com/0xsequence/ethkit/go-ethereum/core/types"
	"github.com/sirupsen/logrus"
)

// processTransactionWithLargeTxHandling processes a transaction with large transaction lock management
func (p *Processor) processTransactionWithLargeTxHandling(ctx context.Context, block *types.Block, index int, tx *types.Transaction, txHash string) (int, error) {
	if p.largeTxLock == nil || !p.largeTxLock.config.Enabled {
		// Large transaction handling not enabled, process normally
		return p.ProcessSingleTransaction(ctx, block, index, tx)
	}

	// Check if a large transaction is currently being processed
	if p.largeTxLock.active.Load() {
		// Wait for the large transaction to complete before processing
		if err := p.largeTxLock.WaitForLargeTransaction(ctx, txHash); err != nil {
			p.log.WithError(err).WithFields(logrus.Fields{
				"tx_hash": txHash,
			}).Warn("Failed to wait for large transaction")

			return 0, err
		}
	}

	// Process the transaction and get size info
	structlogCount, isLarge, err := p.ProcessSingleTransactionWithSizeInfo(ctx, block, index, tx)
	if err != nil {
		return 0, err
	}

	// If it turned out to be a large transaction and we haven't acquired the lock yet
	if isLarge && p.largeTxLock.config.EnableSequentialMode && !p.largeTxLock.HasLock(txHash) {
		// This is a large transaction that we just discovered
		// For now, we've already processed it, but log this for monitoring
		p.log.WithFields(logrus.Fields{
			"tx_hash":         txHash,
			"structlog_count": structlogCount,
			"threshold":       p.largeTxLock.config.StructlogThreshold,
		}).Info("Processed large transaction without pre-acquired lock - consider implementing retry logic")
	}

	return structlogCount, nil
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
