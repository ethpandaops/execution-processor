package structlog

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/0xsequence/ethkit/go-ethereum/core/types"
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/common"
	c "github.com/ethpandaops/execution-processor/pkg/processor/common"
	"github.com/ethpandaops/execution-processor/pkg/state"
)

// ProcessNextBlock processes the next available block
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

	p.log.WithFields(logrus.Fields{
		"block_number": nextBlock.String(),
		"network":      p.network.Name,
	}).Debug("Found next block to process")

	// Update block metrics
	p.updateBlockMetrics(ctx, nextBlock)

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
		if isBlockNotFoundError(err) {
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

	// Handle empty blocks efficiently
	if len(block.Transactions()) == 0 {
		p.log.WithFields(logrus.Fields{
			"network":      p.network.Name,
			"block_number": nextBlock,
		}).Debug("skipping empty block")

		// Mark the block as processed
		if err := p.stateManager.MarkBlockProcessed(ctx, nextBlock.Uint64(), p.network.Name, p.Name()); err != nil {
			p.log.WithError(err).WithFields(logrus.Fields{
				"network":      p.network.Name,
				"block_number": nextBlock,
			}).Error("could not mark empty block as processed")

			return err
		}

		return nil
	}

	// Enqueue tasks for each transaction
	if err := p.enqueueTransactionTasks(ctx, block); err != nil {
		return fmt.Errorf("failed to enqueue transaction tasks: %w", err)
	}

	// Mark the block as processed
	if err := p.stateManager.MarkBlockProcessed(ctx, nextBlock.Uint64(), p.network.Name, p.Name()); err != nil {
		p.log.WithError(err).WithFields(logrus.Fields{
			"network":      p.network.Name,
			"block_number": nextBlock,
		}).Error("could not mark block as processed")

		return err
	}

	p.log.WithFields(logrus.Fields{
		"network":      p.network.Name,
		"block_number": nextBlock,
		"tx_count":     len(block.Transactions()),
	}).Info("processed block with transactions")

	return nil
}

// isBlockNotFoundError checks if an error indicates a block was not found
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

// enqueueTransactionTasks enqueues tasks for all transactions in a block
func (p *Processor) enqueueTransactionTasks(ctx context.Context, block *types.Block) error {
	var enqueuedCount int

	var errs []error

	for index, tx := range block.Transactions() {
		// Create process task payload
		payload := &ProcessPayload{
			BlockNumber:      *block.Number(),
			TransactionHash:  tx.Hash().String(),
			TransactionIndex: uint32(index), //nolint:gosec // index is bounded by block.Transactions() length
			NetworkID:        p.network.ID,
			NetworkName:      p.network.Name,
			Network:          p.network.Name,
		}

		// Create the task based on processing mode
		var task *asynq.Task

		var queue string

		var taskType string

		var err error

		if p.processingMode == c.BACKWARDS_MODE {
			task, err = NewProcessBackwardsTask(payload)
			queue = c.ProcessBackwardsQueue(ProcessorName)
			taskType = ProcessBackwardsTaskType
		} else {
			task, err = NewProcessForwardsTask(payload)
			queue = c.ProcessForwardsQueue(ProcessorName)
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
		return fmt.Errorf("failed to enqueue %d tasks: %v", len(errs), errs[0])
	}

	return nil
}

// updateBlockMetrics updates block-related metrics
func (p *Processor) updateBlockMetrics(ctx context.Context, blockNumber *big.Int) {
	// Update block height
	common.BlockHeight.WithLabelValues(p.network.Name, ProcessorName).Set(float64(blockNumber.Int64()))

	// Get chain tip to calculate depth
	node := p.pool.GetHealthyExecutionNode()
	if node != nil {
		latestBlock, err := node.BlockNumber(ctx)
		if err == nil && latestBlock != nil {
			latestBlockBig := new(big.Int).SetUint64(*latestBlock)
			depth := new(big.Int).Sub(latestBlockBig, blockNumber).Int64()
			common.BlockDepth.WithLabelValues(p.network.Name, ProcessorName).Set(float64(depth))
		}
	}
}
