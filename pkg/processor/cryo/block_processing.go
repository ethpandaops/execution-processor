package cryo

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

// ProcessNextBlock discovers and enqueues the next block(s) for this group.
func (p *Processor) ProcessNextBlock(ctx context.Context) error {
	node := p.pool.GetHealthyExecutionNode()
	if node == nil {
		return fmt.Errorf("no healthy execution node available")
	}

	var chainHead *big.Int

	if latest, err := node.BlockNumber(ctx); err == nil && latest != nil {
		chainHead = new(big.Int).SetUint64(*latest)
	}

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

	// A group cannot collect below the highest floor among its datasets, so
	// descending past it in backwards mode is the end of the run rather than
	// a stall.
	if p.processingMode == tracker.BACKWARDS_MODE && nextBlock.Uint64() < p.group.MinBlock {
		p.log.WithFields(logrus.Fields{
			"next_block": nextBlock.Uint64(),
			"min_block":  p.group.MinBlock,
		}).Debug("Reached the group's floor, backwards processing complete")

		return nil
	}

	blocked, err := p.handleBackpressure(ctx, nextBlock.Uint64())
	if err != nil {
		return err
	}

	if blocked {
		return nil
	}

	capacity, err := p.GetAvailableCapacity(ctx, nextBlock.Uint64(), p.processingMode)
	if err != nil {
		p.log.WithError(err).Warn("Failed to get available capacity, falling back to single block")

		capacity = 1
	}

	if capacity <= 0 {
		p.log.Debug("No capacity available, waiting for tasks to complete")

		return nil
	}

	blockNumbers, err := p.stateManager.NextBlocks(ctx, p.Name(), p.network.Name, p.processingMode, chainHead, capacity)
	if err != nil {
		p.log.WithError(err).Warn("Failed to get batch of block numbers, falling back to single block")

		blockNumbers = []*big.Int{nextBlock}
	}

	if len(blockNumbers) == 0 {
		return nil
	}

	if leashErr := p.ValidateBatchWithinLeash(ctx, blockNumbers[0].Uint64(), len(blockNumbers), p.processingMode); leashErr != nil {
		p.log.WithError(leashErr).Warn("Batch validation failed, reducing to single block")

		blockNumbers = blockNumbers[:1]
	}

	blocks, err := node.BlocksByNumbers(ctx, blockNumbers)
	if err != nil {
		p.log.WithError(err).WithField("network", p.network.Name).Error("could not fetch blocks")

		return err
	}

	if len(blocks) == 0 {
		return fmt.Errorf("block %s not yet available", nextBlock.String())
	}

	for _, block := range blocks {
		if err := p.ProcessBlock(ctx, block); err != nil {
			return err
		}
	}

	return nil
}

// handleBackpressure reports whether the pending-block window is full, and
// re-enqueues the blocking block if it has lost its Redis tracking.
func (p *Processor) handleBackpressure(ctx context.Context, nextBlock uint64) (bool, error) {
	blocked, blockingBlock, err := p.IsBlockedByIncompleteBlocks(ctx, nextBlock, p.processingMode)
	if err != nil {
		p.log.WithError(err).Warn("Failed to check incomplete blocks distance, proceeding anyway")

		return false, nil
	}

	if !blocked {
		return false, nil
	}

	if blockingBlock == nil {
		return true, nil
	}

	hasTracking, err := p.completionTracker.HasBlockTracking(ctx, *blockingBlock, p.network.Name, p.Name(), p.processingMode)
	if err != nil {
		p.log.WithError(err).Warn("Failed to check block tracking")

		return true, nil
	}

	if hasTracking {
		return true, nil
	}

	p.log.WithFields(logrus.Fields{
		"blocking_block": *blockingBlock,
		"next_block":     nextBlock,
	}).Warn("Detected orphaned block blocking progress, reprocessing")

	if err := p.ReprocessBlock(ctx, *blockingBlock); err != nil {
		p.log.WithError(err).Error("Failed to reprocess orphaned block")
	}

	return true, nil
}

// ProcessBlock marks a block enqueued and queues its single collection task.
func (p *Processor) ProcessBlock(ctx context.Context, block execution.Block) error {
	blockNumber := block.Number().Uint64()

	recentlyProcessed, err := p.stateManager.IsBlockRecentlyProcessed(ctx, blockNumber, p.network.Name, p.Name(), 10)
	if err != nil {
		p.log.WithError(err).Warn("Failed to check if block was recently processed")
	}

	if recentlyProcessed {
		common.BlockProcessingSkipped.WithLabelValues(p.network.Name, p.Name(), "recently_processed").Inc()

		return fmt.Errorf("block %d was recently processed", blockNumber)
	}

	// Unlike the transaction processors there is no empty-block shortcut: cryo
	// emits per-block rows regardless of transaction count, and a block with no
	// transactions still has a header, reward traces and state diffs.
	return p.enqueue(ctx, blockNumber, p.queueFor(p.processingMode), "Enqueued block for processing")
}

// ReprocessBlock re-enqueues a block that is recorded as incomplete but has no
// Redis tracking, using the high-priority queue.
func (p *Processor) ReprocessBlock(ctx context.Context, blockNum uint64) error {
	return p.enqueue(ctx, blockNum, p.reprocessQueueFor(p.processingMode), "Reprocessed orphaned block to high-priority queue")
}

// enqueue records the block as in flight and queues its task. The ledger row is
// written before the task so that a crash between the two leaves a block the
// gap detector can see, rather than a task with nothing tracking it.
func (p *Processor) enqueue(ctx context.Context, blockNumber uint64, queue, logMessage string) error {
	if err := p.stateManager.MarkBlockEnqueued(ctx, blockNumber, 1, p.network.Name, p.Name()); err != nil {
		p.log.WithError(err).WithFields(logrus.Fields{
			"network":      p.network.Name,
			"block_number": blockNumber,
		}).Error("could not mark block as enqueued")

		return err
	}

	if err := p.completionTracker.RegisterBlock(
		ctx, blockNumber, 1, p.network.Name, p.Name(), p.processingMode, queue,
	); err != nil {
		return fmt.Errorf("failed to register block %d for completion tracking: %w", blockNumber, err)
	}

	payload := &ProcessPayload{
		BlockNumber: *new(big.Int).SetUint64(blockNumber),
		NetworkName: p.network.Name,
	}

	task, taskID, err := p.newTask(payload, p.processingMode)
	if err != nil {
		return err
	}

	p.deleteTaskFromMainQueue(taskID)

	err = p.EnqueueTask(ctx, task, asynq.Queue(queue), asynq.TaskID(taskID))

	switch {
	case errors.Is(err, asynq.ErrTaskIDConflict):
		p.log.WithFields(logrus.Fields{
			"task_id":      taskID,
			"block_number": blockNumber,
		}).Debug("Task already exists (TaskID conflict), skipping")
	case err != nil:
		return fmt.Errorf("failed to enqueue task: %w", err)
	default:
		common.TasksEnqueued.WithLabelValues(p.network.Name, p.Name(), queue, task.Type()).Inc()
	}

	common.BlocksProcessed.WithLabelValues(p.network.Name, p.Name()).Inc()

	p.log.WithFields(logrus.Fields{
		"block_number": blockNumber,
		"queue":        queue,
	}).Info(logMessage)

	return nil
}

// deleteTaskFromMainQueue drops any pending copy of a task before it is
// re-enqueued elsewhere. An active task cannot be deleted, which is fine: it
// will finish and the TaskID deduplication keeps the outcome idempotent.
func (p *Processor) deleteTaskFromMainQueue(taskID string) {
	if p.asynqInspector == nil {
		return
	}

	mainQueue := p.queueFor(p.processingMode)

	err := p.asynqInspector.DeleteTask(mainQueue, taskID)
	if err == nil || errors.Is(err, asynq.ErrTaskNotFound) || errors.Is(err, asynq.ErrQueueNotFound) {
		return
	}

	p.log.WithFields(logrus.Fields{
		"task_id": taskID,
		"queue":   mainQueue,
		"error":   err,
	}).Debug("Could not delete task from main queue (may be active)")
}
