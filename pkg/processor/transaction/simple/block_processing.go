package simple

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

	// Get current chain head for state manager
	var chainHead *big.Int

	node := p.pool.GetHealthyExecutionNode()
	if node == nil {
		return fmt.Errorf("no healthy execution node available")
	}

	latestBlockNum, err := node.BlockNumber(ctx)
	if err == nil && latestBlockNum != nil {
		chainHead = new(big.Int).SetUint64(*latestBlockNum)
	}

	// Get next block to determine starting point
	nextBlock, err := p.stateManager.NextBlock(ctx, p.Name(), p.network.Name, p.processingMode, chainHead)
	if err != nil {
		if errors.Is(err, state.ErrNoMoreBlocks) {
			p.log.Debug("no more blocks to process")

			return nil
		}

		return fmt.Errorf("failed to get next block: %w", err)
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
		return p.handleBlockNotFound(ctx, nextBlock)
	}

	p.log.WithFields(logrus.Fields{
		"requested": len(blockNumbers),
		"received":  len(blocks),
		"network":   p.network.Name,
	}).Debug("Fetched batch of blocks")

	// Process each block, stopping on first error
	processedCount := 0

	for _, block := range blocks {
		if processErr := p.processBlock(ctx, block); processErr != nil {
			// Record blocks that were successfully processed before the error
			if processedCount > 0 {
				common.BlocksProcessed.WithLabelValues(p.network.Name, p.Name()).Add(float64(processedCount))
			}

			return processErr
		}

		processedCount++
	}

	// Record all successfully processed blocks
	if processedCount > 0 {
		common.BlocksProcessed.WithLabelValues(p.network.Name, p.Name()).Add(float64(processedCount))
	}

	return nil
}

// handleBlockNotFound handles the case when a block is not found.
func (p *Processor) handleBlockNotFound(_ context.Context, nextBlock *big.Int) error {
	p.log.WithFields(logrus.Fields{
		"block_number": nextBlock.String(),
		"network":      p.network.Name,
	}).Debug("Block not yet available")

	return fmt.Errorf("block %s not yet available", nextBlock.String())
}

// processBlock processes a single block - the core logic extracted from the original ProcessNextBlock.
func (p *Processor) processBlock(ctx context.Context, block execution.Block) error {
	blockNumber := block.Number()

	p.log.WithFields(logrus.Fields{
		"block_number": blockNumber.String(),
		"network":      p.network.Name,
	}).Debug("Processing block")

	// Check if this block was recently processed
	recentlyProcessed, err := p.stateManager.IsBlockRecentlyProcessed(
		ctx,
		blockNumber.Uint64(),
		p.network.Name,
		p.Name(),
		10,
	)
	if err != nil {
		p.log.WithError(err).Warn("Failed to check if block was recently processed")
	}

	if recentlyProcessed {
		p.log.WithField("block", blockNumber.Uint64()).Debug("Block was recently processed, skipping")
		common.BlockProcessingSkipped.WithLabelValues(p.network.Name, p.Name(), "recently_processed").Inc()

		return fmt.Errorf("block %d was recently processed", blockNumber.Uint64())
	}

	// Handle empty blocks - mark complete immediately (no task tracking needed)
	if len(block.Transactions()) == 0 {
		p.log.WithField("block", blockNumber.Uint64()).Debug("Empty block, marking as complete")

		return p.stateManager.MarkBlockComplete(ctx, blockNumber.Uint64(), p.network.Name, p.Name())
	}

	// 1. Mark block as enqueued in ClickHouse FIRST (complete=0)
	// This records that we started processing this block (1 task per block for simple processor)
	if markErr := p.stateManager.MarkBlockEnqueued(ctx, blockNumber.Uint64(), 1, p.network.Name, p.Name()); markErr != nil {
		p.log.WithError(markErr).WithFields(logrus.Fields{
			"network":      p.network.Name,
			"block_number": blockNumber,
		}).Error("could not mark block as enqueued")

		return markErr
	}

	// 2. Register block for completion tracking in Redis (clears any old state)
	queue := p.getProcessForwardsQueue()
	if p.processingMode == tracker.BACKWARDS_MODE {
		queue = p.getProcessBackwardsQueue()
	}

	if regErr := p.completionTracker.RegisterBlock(ctx, blockNumber.Uint64(), 1, p.network.Name, p.Name(), p.processingMode, queue); regErr != nil {
		p.log.WithError(regErr).WithFields(logrus.Fields{
			"network":      p.network.Name,
			"block_number": blockNumber,
		}).Error("could not register block for completion tracking")

		return regErr
	}

	// 3. Create and enqueue block processing task with TaskID deduplication
	payload := &ProcessPayload{
		BlockNumber:    *blockNumber,
		NetworkName:    p.network.Name,
		ProcessingMode: p.processingMode,
	}

	var task *asynq.Task

	var taskID string

	if p.processingMode == tracker.BACKWARDS_MODE {
		task, taskID, err = NewProcessBackwardsTask(payload)
	} else {
		task, taskID, err = NewProcessForwardsTask(payload)
	}

	if err != nil {
		return fmt.Errorf("failed to create task: %w", err)
	}

	// Enqueue with TaskID for deduplication
	err = p.EnqueueTask(ctx, task,
		asynq.Queue(queue),
		asynq.TaskID(taskID),
	)

	if errors.Is(err, ErrTaskIDConflict) {
		// Task already exists - this is fine, it will still complete and be tracked
		p.log.WithFields(logrus.Fields{
			"task_id":      taskID,
			"block_number": blockNumber.Uint64(),
		}).Debug("Task already exists (TaskID conflict), skipping")
	} else if err != nil {
		return fmt.Errorf("failed to enqueue task: %w", err)
	} else {
		common.TasksEnqueued.WithLabelValues(p.network.Name, ProcessorName, queue, task.Type()).Inc()
	}

	p.log.WithFields(logrus.Fields{
		"block_number": blockNumber.Uint64(),
		"tx_count":     len(block.Transactions()),
	}).Info("Enqueued block for processing")

	return nil
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

	// Use the high-priority reprocess queue for orphaned/stale blocks (mode-specific)
	var queue string
	if p.processingMode == tracker.BACKWARDS_MODE {
		queue = p.getProcessReprocessBackwardsQueue()
	} else {
		queue = p.getProcessReprocessForwardsQueue()
	}

	// Register in Redis (clears any partial state)
	if regErr := p.completionTracker.RegisterBlock(
		ctx, blockNum, 1, p.network.Name, p.Name(), p.processingMode, queue,
	); regErr != nil {
		return fmt.Errorf("failed to register block: %w", regErr)
	}

	// Create and enqueue block processing task with TaskID deduplication
	payload := &ProcessPayload{
		BlockNumber:    *new(big.Int).SetUint64(blockNum),
		NetworkName:    p.network.Name,
		ProcessingMode: p.processingMode,
	}

	var task *asynq.Task

	var taskID string

	// Create task based on processing mode but enqueue to reprocess queue
	if p.processingMode == tracker.BACKWARDS_MODE {
		task, taskID, err = NewProcessBackwardsTask(payload)
	} else {
		task, taskID, err = NewProcessForwardsTask(payload)
	}

	if err != nil {
		return fmt.Errorf("failed to create task: %w", err)
	}

	// Enqueue to the high-priority reprocess queue
	err = p.EnqueueTask(ctx, task,
		asynq.Queue(queue),
		asynq.TaskID(taskID),
	)

	if errors.Is(err, ErrTaskIDConflict) {
		// Task already exists - this is fine, it will still complete and be tracked
		p.log.WithFields(logrus.Fields{
			"task_id":      taskID,
			"block_number": blockNum,
		}).Debug("Task already exists (TaskID conflict) during reprocess")
	} else if err != nil {
		return fmt.Errorf("failed to enqueue task: %w", err)
	} else {
		common.TasksEnqueued.WithLabelValues(p.network.Name, ProcessorName, queue, task.Type()).Inc()
	}

	p.log.WithFields(logrus.Fields{
		"block_number": blockNum,
		"tx_count":     len(block.Transactions()),
		"queue":        queue,
	}).Info("Reprocessed orphaned block to high-priority queue")

	return nil
}
