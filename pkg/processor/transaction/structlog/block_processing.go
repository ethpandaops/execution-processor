package structlog

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
	"github.com/ethpandaops/execution-processor/pkg/processor/tracker"
	"github.com/ethpandaops/execution-processor/pkg/state"
)

// ProcessNextBlock processes the next available block.
func (p *Processor) ProcessNextBlock(ctx context.Context) error {
	p.log.WithField("network", p.network.Name).Debug("Querying for next block to process")

	// Get current chain head for state manager (used when no history exists)
	var chainHead *big.Int

	node := p.pool.GetHealthyExecutionNode()

	if node != nil {
		if latestBlockNum, err := node.BlockNumber(ctx); err == nil && latestBlockNum != nil {
			chainHead = new(big.Int).SetUint64(*latestBlockNum)
			p.log.WithFields(logrus.Fields{
				"chain_head": chainHead.String(),
			}).Debug("Retrieved chain head for state manager")
		}
	}

	// Get next block to process from admin.execution_block table
	nextBlock, err := p.stateManager.NextBlock(ctx, p.Name(), p.network.Name, p.processingMode, chainHead)
	if err != nil {
		// Check if this is the "no more blocks" condition
		if errors.Is(err, state.ErrNoMoreBlocks) {
			p.log.Debug("no more blocks to process")

			return nil
		}

		p.log.WithError(err).WithField("network", p.network.Name).Error("could not get next block")

		return err
	}

	if nextBlock == nil {
		p.log.Debug("no more blocks to process")

		return nil
	}

	// Distance-based pending block range check
	// Only allow processing if distance between oldest incomplete and next block < maxPendingBlockRange
	blocked, err := p.IsBlockedByIncompleteBlocks(ctx, nextBlock.Uint64(), p.processingMode)
	if err != nil {
		p.log.WithError(err).Warn("Failed to check incomplete blocks distance, proceeding anyway")
	} else if blocked {
		return nil
	}

	p.log.WithFields(logrus.Fields{
		"block_number": nextBlock.String(),
		"network":      p.network.Name,
	}).Debug("Found next block to process")

	// Check if this block was recently processed to avoid rapid reprocessing
	if recentlyProcessed, checkErr := p.stateManager.IsBlockRecentlyProcessed(ctx, nextBlock.Uint64(), p.network.Name, p.Name(), 30); checkErr == nil && recentlyProcessed {
		p.log.WithFields(logrus.Fields{
			"block_number": nextBlock.String(),
			"network":      p.network.Name,
		}).Debug("Block was recently processed, skipping to prevent rapid reprocessing")

		common.BlockProcessingSkipped.WithLabelValues(p.network.Name, p.Name(), "recently_processed").Inc()

		return nil
	}

	// Update lightweight block height metric only (expensive metrics moved to background worker)
	common.BlockHeight.WithLabelValues(p.network.Name, ProcessorName).Set(float64(nextBlock.Int64()))

	// Get healthy execution node
	node = p.pool.GetHealthyExecutionNode()
	if node == nil {
		p.log.WithField("network", p.network.Name).Error("could not get healthy node")

		return fmt.Errorf("no healthy execution node available")
	}

	// Get block data
	block, err := node.BlockByNumber(ctx, nextBlock)
	if err != nil {
		// Check if this is a "not found" error indicating we're at head
		if tracker.IsBlockNotFoundError(err) {
			// Check if we're close to chain tip to determine if this is expected
			if latestBlock, latestErr := node.BlockNumber(ctx); latestErr == nil && latestBlock != nil {
				chainTip := new(big.Int).SetUint64(*latestBlock)
				diff := new(big.Int).Sub(nextBlock, chainTip).Int64()

				if diff <= 5 { // Within 5 blocks of chain tip
					p.log.WithFields(logrus.Fields{
						"network":      p.network.Name,
						"block_number": nextBlock,
						"chain_tip":    chainTip,
						"blocks_ahead": diff,
					}).Info("Waiting for block to be available on execution node")

					return fmt.Errorf("block %s not yet available (chain tip: %s)", nextBlock, chainTip)
				}
			}
		}

		// Log as error for non-head cases or when we can't determine chain tip
		p.log.WithError(err).WithFields(logrus.Fields{
			"network":      p.network.Name,
			"block_number": nextBlock,
		}).Error("could not get block")

		return err
	}

	// Handle empty blocks - mark complete immediately (no task tracking needed)
	if len(block.Transactions()) == 0 {
		p.log.WithFields(logrus.Fields{
			"network":      p.network.Name,
			"block_number": nextBlock,
		}).Debug("skipping empty block")

		// Mark the block as complete immediately (no tasks to track)
		if markErr := p.stateManager.MarkBlockComplete(ctx, nextBlock.Uint64(), p.network.Name, p.Name()); markErr != nil {
			p.log.WithError(markErr).WithFields(logrus.Fields{
				"network":      p.network.Name,
				"block_number": nextBlock,
			}).Error("could not mark empty block as complete")

			return markErr
		}

		return nil
	}

	// Calculate expected task count before enqueueing
	// This MUST happen before tasks are enqueued to avoid race conditions where
	// tasks complete before the block is registered in ClickHouse
	expectedTaskCount := len(block.Transactions())

	// Acquire exclusive lock on this block via Redis FIRST
	// This prevents multiple workers from processing the same block concurrently
	if initErr := p.pendingTracker.InitBlock(ctx, nextBlock.Uint64(), expectedTaskCount, p.network.Name, p.Name(), p.processingMode); initErr != nil {
		// If block is already being processed by another worker, skip gracefully
		if errors.Is(initErr, tracker.ErrBlockAlreadyBeingProcessed) {
			p.log.WithFields(logrus.Fields{
				"network":      p.network.Name,
				"block_number": nextBlock,
			}).Debug("Block already being processed by another worker, skipping")

			common.BlockProcessingSkipped.WithLabelValues(p.network.Name, p.Name(), "already_processing").Inc()

			return nil
		}

		p.log.WithError(initErr).WithFields(logrus.Fields{
			"network":      p.network.Name,
			"block_number": nextBlock,
		}).Error("could not init block tracking in Redis")

		return initErr
	}

	// Mark the block as enqueued AFTER acquiring Redis lock (phase 1 of two-phase completion)
	// This ensures the complete=0 record exists before any task can complete
	if markErr := p.stateManager.MarkBlockEnqueued(ctx, nextBlock.Uint64(), expectedTaskCount, p.network.Name, p.Name()); markErr != nil {
		p.log.WithError(markErr).WithFields(logrus.Fields{
			"network":      p.network.Name,
			"block_number": nextBlock,
		}).Error("could not mark block as enqueued")

		// Clean up Redis lock since we failed to mark in ClickHouse
		_ = p.pendingTracker.CleanupBlock(ctx, nextBlock.Uint64(), p.network.Name, p.Name(), p.processingMode)

		return markErr
	}

	// Enqueue tasks for each transaction LAST
	// Tasks may start processing immediately, but the block is already registered
	taskCount, err := p.EnqueueTransactionTasks(ctx, block)
	if err != nil {
		return fmt.Errorf("failed to enqueue transaction tasks: %w", err)
	}

	// Log warning if actual count differs from expected (some tasks failed to enqueue)
	if taskCount != expectedTaskCount {
		p.log.WithFields(logrus.Fields{
			"network":             p.network.Name,
			"block_number":        nextBlock,
			"expected_task_count": expectedTaskCount,
			"actual_task_count":   taskCount,
		}).Warn("task count mismatch - some tasks may have failed to enqueue")
	}

	p.log.WithFields(logrus.Fields{
		"network":      p.network.Name,
		"block_number": nextBlock,
		"tx_count":     len(block.Transactions()),
		"task_count":   taskCount,
	}).Info("enqueued block for processing")

	return nil
}

// enqueueTransactionTasks enqueues tasks for all transactions in a block.
// EnqueueTransactionTasks enqueues transaction processing tasks for a given block.
func (p *Processor) EnqueueTransactionTasks(ctx context.Context, block execution.Block) (int, error) {
	var enqueuedCount int

	var errs []error

	for index, tx := range block.Transactions() {
		// Create process task payload
		payload := &ProcessPayload{
			BlockNumber:      *block.Number(),
			TransactionHash:  tx.Hash().String(),
			TransactionIndex: uint32(index), //nolint:gosec // index is bounded by block.Transactions() length
			NetworkName:      p.network.Name,
			Network:          p.network.Name,
		}

		// Create the task based on processing mode
		var task *asynq.Task

		var queue string

		var taskType string

		var err error

		if p.processingMode == tracker.BACKWARDS_MODE {
			task, err = NewProcessBackwardsTask(payload)
			queue = p.getProcessBackwardsQueue()
			taskType = ProcessBackwardsTaskType
		} else {
			task, err = NewProcessForwardsTask(payload)
			queue = p.getProcessForwardsQueue()
			taskType = ProcessForwardsTaskType
		}

		if err != nil {
			errs = append(errs, fmt.Errorf("failed to create task for tx %s: %w", tx.Hash().String(), err))

			continue
		}

		// Enqueue the task
		if err := p.EnqueueTask(ctx, task, asynq.Queue(queue)); err != nil {
			errs = append(errs, fmt.Errorf("failed to enqueue task for tx %s: %w", tx.Hash().String(), err))

			continue
		}

		enqueuedCount++

		common.TasksEnqueued.WithLabelValues(p.network.Name, ProcessorName, queue, taskType).Inc()
	}

	p.log.WithFields(logrus.Fields{
		"block_number":   block.Number(),
		"total_txs":      len(block.Transactions()),
		"enqueued_count": enqueuedCount,
		"error_count":    len(errs),
	}).Info("Enqueued transaction processing tasks")

	if len(errs) > 0 {
		return enqueuedCount, fmt.Errorf("failed to enqueue %d tasks: %v", len(errs), errs[0])
	}

	return enqueuedCount, nil
}
