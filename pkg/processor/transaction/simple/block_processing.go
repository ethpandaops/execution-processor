package simple

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/ethpandaops/execution-processor/pkg/processor/tracker"
	"github.com/ethpandaops/execution-processor/pkg/state"
)

// ProcessNextBlock processes the next available block.
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

	// Get next block to process from state manager
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

	// Check if this block was recently processed
	recentlyProcessed, err := p.stateManager.IsBlockRecentlyProcessed(
		ctx,
		nextBlock.Uint64(),
		p.network.Name,
		p.Name(),
		10,
	)
	if err != nil {
		p.log.WithError(err).Warn("Failed to check if block was recently processed")
	}

	if recentlyProcessed {
		p.log.WithField("block", nextBlock.Uint64()).Debug("Block was recently processed, skipping")
		common.BlockProcessingSkipped.WithLabelValues(p.network.Name, p.Name(), "recently_processed").Inc()

		return fmt.Errorf("block %d was recently processed", nextBlock.Uint64())
	}

	// Get block data
	block, err := node.BlockByNumber(ctx, nextBlock)
	if err != nil {
		if tracker.IsBlockNotFoundError(err) {
			p.log.WithFields(logrus.Fields{
				"block_number": nextBlock.String(),
				"network":      p.network.Name,
			}).Debug("Block not yet available")

			return fmt.Errorf("block %s not yet available", nextBlock.String())
		}

		return fmt.Errorf("failed to get block %d: %w", nextBlock.Uint64(), err)
	}

	// Handle empty blocks - mark complete immediately (no task tracking needed)
	if len(block.Transactions()) == 0 {
		p.log.WithField("block", nextBlock.Uint64()).Debug("Empty block, marking as complete")

		return p.stateManager.MarkBlockComplete(ctx, nextBlock.Uint64(), p.network.Name, p.Name())
	}

	// Enqueue block processing task
	payload := &ProcessPayload{
		BlockNumber:    *nextBlock,
		NetworkName:    p.network.Name,
		ProcessingMode: p.processingMode,
	}

	var task *asynq.Task

	var queue string

	if p.processingMode == tracker.BACKWARDS_MODE {
		task, err = NewProcessBackwardsTask(payload)
		queue = p.getProcessBackwardsQueue()
	} else {
		task, err = NewProcessForwardsTask(payload)
		queue = p.getProcessForwardsQueue()
	}

	if err != nil {
		return fmt.Errorf("failed to create task: %w", err)
	}

	if err := p.EnqueueTask(ctx, task, asynq.Queue(queue)); err != nil {
		return fmt.Errorf("failed to enqueue task: %w", err)
	}

	common.TasksEnqueued.WithLabelValues(p.network.Name, ProcessorName, queue, task.Type()).Inc()

	// Initialize block tracking in Redis (1 task per block for simple processor)
	if err := p.pendingTracker.InitBlock(ctx, nextBlock.Uint64(), 1, p.network.Name, p.Name(), p.processingMode); err != nil {
		p.log.WithError(err).WithFields(logrus.Fields{
			"network":      p.network.Name,
			"block_number": nextBlock,
		}).Error("could not init block tracking in Redis")

		return err
	}

	// Mark block as enqueued (phase 1 of two-phase completion)
	if err := p.stateManager.MarkBlockEnqueued(ctx, nextBlock.Uint64(), 1, p.network.Name, p.Name()); err != nil {
		p.log.WithError(err).WithFields(logrus.Fields{
			"network":      p.network.Name,
			"block_number": nextBlock,
		}).Error("could not mark block as enqueued")

		return err
	}

	p.log.WithFields(logrus.Fields{
		"block_number": nextBlock.Uint64(),
		"tx_count":     len(block.Transactions()),
	}).Info("Enqueued block for processing")

	return nil
}
