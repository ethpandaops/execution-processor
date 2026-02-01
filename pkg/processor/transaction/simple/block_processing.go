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
	for _, block := range blocks {
		if processErr := p.processBlock(ctx, block); processErr != nil {
			return processErr
		}
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

	// Enqueue block processing task
	payload := &ProcessPayload{
		BlockNumber:    *blockNumber,
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
	if err := p.pendingTracker.InitBlock(ctx, blockNumber.Uint64(), 1, p.network.Name, p.Name(), p.processingMode); err != nil {
		p.log.WithError(err).WithFields(logrus.Fields{
			"network":      p.network.Name,
			"block_number": blockNumber,
		}).Error("could not init block tracking in Redis")

		return err
	}

	// Mark block as enqueued (phase 1 of two-phase completion)
	if err := p.stateManager.MarkBlockEnqueued(ctx, blockNumber.Uint64(), 1, p.network.Name, p.Name()); err != nil {
		p.log.WithError(err).WithFields(logrus.Fields{
			"network":      p.network.Name,
			"block_number": blockNumber,
		}).Error("could not mark block as enqueued")

		return err
	}

	p.log.WithFields(logrus.Fields{
		"block_number": blockNumber.Uint64(),
		"tx_count":     len(block.Transactions()),
	}).Info("Enqueued block for processing")

	return nil
}
