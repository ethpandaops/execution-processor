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

// ErrTaskIDConflict is returned when a task with the same ID already exists.
var ErrTaskIDConflict = asynq.ErrTaskIDConflict

// ProcessNextBlock processes the next available block(s).
// In zero-interval mode, this attempts to fetch and process multiple blocks
// up to the available capacity for improved throughput.
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

	// Get next block to determine starting point
	nextBlock, err := p.stateManager.NextBlock(ctx, p.Name(), p.network.Name, p.processingMode, chainHead)
	if err != nil {
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

	// Distance-based pending block range check with orphan detection
	blocked, blockingBlock, err := p.IsBlockedByIncompleteBlocks(ctx, nextBlock.Uint64(), p.processingMode)
	if err != nil {
		p.log.WithError(err).Warn("Failed to check incomplete blocks distance, proceeding anyway")
	} else if blocked {
		// Check if the blocking block is orphaned (no Redis tracking)
		if blockingBlock != nil {
			hasTracking, trackErr := p.completionTracker.HasBlockTracking(
				ctx, *blockingBlock, p.network.Name, p.Name(), p.processingMode)
			if trackErr != nil {
				p.log.WithError(trackErr).Warn("Failed to check block tracking")
			} else if !hasTracking {
				// Orphaned block - reprocess it
				p.log.WithFields(logrus.Fields{
					"blocking_block": *blockingBlock,
					"next_block":     nextBlock,
				}).Warn("Detected orphaned block blocking progress, reprocessing")

				if reprocessErr := p.ReprocessBlock(ctx, *blockingBlock); reprocessErr != nil {
					p.log.WithError(reprocessErr).Error("Failed to reprocess orphaned block")
				}
			}
		}

		return nil
	}

	// Get available capacity for batch processing
	capacity, err := p.GetAvailableCapacity(ctx, nextBlock.Uint64(), p.processingMode)
	if err != nil {
		p.log.WithError(err).Warn("Failed to get available capacity, falling back to single block")

		capacity = 1
	}

	if capacity <= 0 {
		p.log.Debug("No capacity available, waiting for tasks to complete")

		return nil
	}

	// Get batch of block numbers
	blockNumbers, err := p.stateManager.NextBlocks(ctx, p.Name(), p.network.Name, p.processingMode, chainHead, capacity)
	if err != nil {
		p.log.WithError(err).Warn("Failed to get batch of block numbers, falling back to single block")

		blockNumbers = []*big.Int{nextBlock}
	}

	if len(blockNumbers) == 0 {
		p.log.Debug("No blocks to process")

		return nil
	}

	// Validate batch won't exceed leash
	if validateErr := p.ValidateBatchWithinLeash(ctx, blockNumbers[0].Uint64(), len(blockNumbers), p.processingMode); validateErr != nil {
		p.log.WithError(validateErr).Warn("Batch validation failed, reducing to single block")

		blockNumbers = blockNumbers[:1]
	}

	// Fetch blocks using batch RPC
	blocks, err := node.BlocksByNumbers(ctx, blockNumbers)
	if err != nil {
		p.log.WithError(err).WithField("network", p.network.Name).Error("could not fetch blocks")

		return err
	}

	if len(blocks) == 0 {
		// No blocks returned - might be at chain tip
		return p.handleBlockNotFound(ctx, node, nextBlock)
	}

	p.log.WithFields(logrus.Fields{
		"requested": len(blockNumbers),
		"received":  len(blocks),
		"network":   p.network.Name,
	}).Debug("Fetched batch of blocks")

	// Process each block, stopping on first error
	for _, block := range blocks {
		if processErr := p.ProcessBlock(ctx, block); processErr != nil {
			return processErr
		}
	}

	return nil
}

// handleBlockNotFound handles the case when a block is not found.
func (p *Processor) handleBlockNotFound(ctx context.Context, node execution.Node, nextBlock *big.Int) error {
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

	return fmt.Errorf("block %s not found", nextBlock)
}

// ProcessBlock processes a single block - fetches, marks enqueued, and enqueues tasks.
// This is used for both normal processing and gap filling of missing blocks.
func (p *Processor) ProcessBlock(ctx context.Context, block execution.Block) error {
	blockNumber := block.Number()

	// Check if this block was recently processed to avoid rapid reprocessing
	if recentlyProcessed, checkErr := p.stateManager.IsBlockRecentlyProcessed(ctx, blockNumber.Uint64(), p.network.Name, p.Name(), 30); checkErr == nil && recentlyProcessed {
		p.log.WithFields(logrus.Fields{
			"block_number": blockNumber.String(),
			"network":      p.network.Name,
		}).Debug("Block was recently processed, skipping to prevent rapid reprocessing")

		common.BlockProcessingSkipped.WithLabelValues(p.network.Name, p.Name(), "recently_processed").Inc()

		return nil
	}

	// Update lightweight block height metric
	common.BlockHeight.WithLabelValues(p.network.Name, ProcessorName).Set(float64(blockNumber.Int64()))

	p.log.WithFields(logrus.Fields{
		"block_number": blockNumber.String(),
		"network":      p.network.Name,
	}).Debug("Processing block")

	// Handle empty blocks - mark complete immediately (no task tracking needed)
	if len(block.Transactions()) == 0 {
		p.log.WithFields(logrus.Fields{
			"network":      p.network.Name,
			"block_number": blockNumber,
		}).Debug("skipping empty block")

		// Mark the block as complete immediately (no tasks to track)
		if markErr := p.stateManager.MarkBlockComplete(ctx, blockNumber.Uint64(), p.network.Name, p.Name()); markErr != nil {
			p.log.WithError(markErr).WithFields(logrus.Fields{
				"network":      p.network.Name,
				"block_number": blockNumber,
			}).Error("could not mark empty block as complete")

			return markErr
		}

		return nil
	}

	// Calculate expected task count before enqueueing
	expectedTaskCount := len(block.Transactions())

	// 1. Mark block as enqueued in ClickHouse FIRST (complete=0)
	// This records that we started processing this block
	if markErr := p.stateManager.MarkBlockEnqueued(ctx, blockNumber.Uint64(), expectedTaskCount, p.network.Name, p.Name()); markErr != nil {
		p.log.WithError(markErr).WithFields(logrus.Fields{
			"network":      p.network.Name,
			"block_number": blockNumber,
		}).Error("could not mark block as enqueued")

		return markErr
	}

	// 2. Register block for completion tracking in Redis (clears any old state)
	// This uses SET-based tracking instead of counter-based
	queue := p.getProcessForwardsQueue()
	if p.processingMode == tracker.BACKWARDS_MODE {
		queue = p.getProcessBackwardsQueue()
	}

	if regErr := p.completionTracker.RegisterBlock(ctx, blockNumber.Uint64(), expectedTaskCount, p.network.Name, p.Name(), p.processingMode, queue); regErr != nil {
		p.log.WithError(regErr).WithFields(logrus.Fields{
			"network":      p.network.Name,
			"block_number": blockNumber,
		}).Error("could not register block for completion tracking")

		return regErr
	}

	// 3. Enqueue tasks with TaskID deduplication
	// Tasks may start processing immediately, but the block is already registered
	taskCount, err := p.EnqueueTransactionTasks(ctx, block)
	if err != nil {
		return fmt.Errorf("failed to enqueue transaction tasks: %w", err)
	}

	// Log warning if actual count differs from expected
	if taskCount != expectedTaskCount {
		p.log.WithFields(logrus.Fields{
			"network":             p.network.Name,
			"block_number":        blockNumber,
			"expected_task_count": expectedTaskCount,
			"actual_task_count":   taskCount,
		}).Warn("task count mismatch - some tasks may have failed to enqueue")
	}

	p.log.WithFields(logrus.Fields{
		"network":      p.network.Name,
		"block_number": blockNumber,
		"tx_count":     len(block.Transactions()),
		"task_count":   taskCount,
	}).Info("enqueued block for processing")

	return nil
}

// EnqueueTransactionTasks enqueues transaction processing tasks for a given block.
// Uses TaskID for deduplication - tasks with conflicting IDs are skipped but still count.
func (p *Processor) EnqueueTransactionTasks(ctx context.Context, block execution.Block) (int, error) {
	var enqueuedCount int

	var skippedCount int

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

		var taskID string

		var queue string

		var taskType string

		var err error

		if p.processingMode == tracker.BACKWARDS_MODE {
			task, taskID, err = NewProcessBackwardsTask(payload)
			queue = p.getProcessBackwardsQueue()
			taskType = ProcessBackwardsTaskType
		} else {
			task, taskID, err = NewProcessForwardsTask(payload)
			queue = p.getProcessForwardsQueue()
			taskType = ProcessForwardsTaskType
		}

		if err != nil {
			errs = append(errs, fmt.Errorf("failed to create task for tx %s: %w", tx.Hash().String(), err))

			continue
		}

		// Enqueue the task with TaskID for deduplication
		err = p.EnqueueTask(ctx, task,
			asynq.Queue(queue),
			asynq.TaskID(taskID),
		)

		if errors.Is(err, ErrTaskIDConflict) {
			// Task already exists - this is fine, it will still complete and be tracked
			skippedCount++

			p.log.WithFields(logrus.Fields{
				"task_id": taskID,
				"tx_hash": tx.Hash().String(),
			}).Debug("Task already exists (TaskID conflict), skipping")

			continue
		}

		if err != nil {
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
		"skipped_count":  skippedCount,
		"error_count":    len(errs),
	}).Info("Enqueued transaction processing tasks")

	if len(errs) > 0 {
		return enqueuedCount + skippedCount, fmt.Errorf("failed to enqueue %d tasks: %v", len(errs), errs[0])
	}

	// Return total count including skipped (already existing) tasks
	return enqueuedCount + skippedCount, nil
}

// ReprocessBlock re-enqueues tasks for an orphaned block.
// Used when a block is in ClickHouse (complete=0) but has no Redis tracking.
// TaskID deduplication ensures no duplicate tasks are created.
func (p *Processor) ReprocessBlock(ctx context.Context, blockNum uint64) error {
	p.log.WithFields(logrus.Fields{
		"block_number": blockNum,
		"network":      p.network.Name,
	}).Info("Reprocessing orphaned block")

	// Get execution node
	node := p.pool.GetHealthyExecutionNode()
	if node == nil {
		return fmt.Errorf("no healthy execution node available")
	}

	// Fetch block
	block, err := node.BlockByNumber(ctx, new(big.Int).SetUint64(blockNum))
	if err != nil {
		return fmt.Errorf("failed to fetch block %d: %w", blockNum, err)
	}

	// Handle empty blocks - mark complete immediately
	if len(block.Transactions()) == 0 {
		p.log.WithField("block", blockNum).Debug("Empty orphaned block, marking as complete")

		return p.stateManager.MarkBlockComplete(ctx, blockNum, p.network.Name, p.Name())
	}

	expectedCount := len(block.Transactions())

	// Use the high-priority reprocess queue for orphaned/stale blocks (mode-specific)
	var queue string
	if p.processingMode == tracker.BACKWARDS_MODE {
		queue = p.getProcessReprocessBackwardsQueue()
	} else {
		queue = p.getProcessReprocessForwardsQueue()
	}

	// Register in Redis (clears any partial state)
	if regErr := p.completionTracker.RegisterBlock(
		ctx, blockNum, expectedCount, p.network.Name, p.Name(), p.processingMode, queue,
	); regErr != nil {
		return fmt.Errorf("failed to register block: %w", regErr)
	}

	// Enqueue tasks directly to the reprocess queue (high priority)
	var enqueuedCount int

	var skippedCount int

	for index, tx := range block.Transactions() {
		payload := &ProcessPayload{
			BlockNumber:      *block.Number(),
			TransactionHash:  tx.Hash().String(),
			TransactionIndex: uint32(index), //nolint:gosec // index is bounded by block.Transactions() length
			NetworkName:      p.network.Name,
			Network:          p.network.Name,
		}

		// Create task based on processing mode but enqueue to reprocess queue
		var task *asynq.Task

		var taskID string

		if p.processingMode == tracker.BACKWARDS_MODE {
			task, taskID, err = NewProcessBackwardsTask(payload)
		} else {
			task, taskID, err = NewProcessForwardsTask(payload)
		}

		if err != nil {
			return fmt.Errorf("failed to create task for tx %s: %w", tx.Hash().String(), err)
		}

		// Enqueue to the high-priority reprocess queue
		err = p.EnqueueTask(ctx, task,
			asynq.Queue(queue),
			asynq.TaskID(taskID),
		)

		if errors.Is(err, ErrTaskIDConflict) {
			skippedCount++

			continue
		}

		if err != nil {
			return fmt.Errorf("failed to enqueue task for tx %s: %w", tx.Hash().String(), err)
		}

		enqueuedCount++
	}

	p.log.WithFields(logrus.Fields{
		"block_number":   blockNum,
		"expected_count": expectedCount,
		"enqueued_count": enqueuedCount,
		"skipped_count":  skippedCount,
		"queue":          queue,
	}).Info("Reprocessed orphaned block to high-priority queue")

	return nil
}
