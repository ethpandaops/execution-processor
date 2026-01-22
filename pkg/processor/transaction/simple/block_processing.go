package simple

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/common"
	c "github.com/ethpandaops/execution-processor/pkg/processor/common"
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
		if isBlockNotFoundError(err) {
			p.log.WithFields(logrus.Fields{
				"block_number": nextBlock.String(),
				"network":      p.network.Name,
			}).Debug("Block not yet available")

			return fmt.Errorf("block %s not yet available", nextBlock.String())
		}

		return fmt.Errorf("failed to get block %d: %w", nextBlock.Uint64(), err)
	}

	// Handle empty blocks
	if len(block.Transactions()) == 0 {
		p.log.WithField("block", nextBlock.Uint64()).Debug("Empty block, marking as processed")

		return p.stateManager.MarkBlockProcessed(ctx, nextBlock.Uint64(), p.network.Name, p.Name())
	}

	// Enqueue block processing task
	payload := &ProcessPayload{
		BlockNumber:    *nextBlock,
		NetworkName:    p.network.Name,
		ProcessingMode: p.processingMode,
	}

	var task *asynq.Task

	var queue string

	if p.processingMode == c.BACKWARDS_MODE {
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

	p.log.WithFields(logrus.Fields{
		"block_number": nextBlock.Uint64(),
		"tx_count":     len(block.Transactions()),
	}).Info("Enqueued block for processing")

	// Mark block as processed (task is enqueued)
	return p.stateManager.MarkBlockProcessed(ctx, nextBlock.Uint64(), p.network.Name, p.Name())
}

// isBlockNotFoundError checks if an error indicates a block was not found.
func isBlockNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	return strings.Contains(errStr, "not found") ||
		strings.Contains(errStr, "unknown block") ||
		strings.Contains(errStr, "block not found") ||
		strings.Contains(errStr, "header not found")
}
