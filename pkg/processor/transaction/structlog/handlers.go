package structlog

import (
	"context"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/ethpandaops/execution-processor/pkg/processor/tracker"
)

// handleProcessForwardsTask handles the forwards processing of a single transaction.
func (p *Processor) handleProcessForwardsTask(ctx context.Context, task *asynq.Task) error {
	start := time.Now()

	defer func() {
		duration := time.Since(start)
		common.TaskProcessingDuration.WithLabelValues(p.network.Name, ProcessorName, tracker.ProcessForwardsQueue(ProcessorName), ProcessForwardsTaskType).Observe(duration.Seconds())
	}()

	var payload ProcessPayload
	if err := payload.UnmarshalBinary(task.Payload()); err != nil {
		common.TasksErrored.WithLabelValues(p.network.Name, ProcessorName, tracker.ProcessForwardsQueue(ProcessorName), ProcessForwardsTaskType, "unmarshal_error").Inc()

		return fmt.Errorf("failed to unmarshal process payload: %w", err)
	}

	// Get healthy execution node
	node := p.pool.GetHealthyExecutionNode()
	if node == nil {
		return fmt.Errorf("no healthy execution node available")
	}

	// Get block data
	blockNumber := &payload.BlockNumber

	block, err := node.BlockByNumber(ctx, blockNumber)
	if err != nil {
		return fmt.Errorf("failed to get block: %w", err)
	}

	// Find the transaction in the block
	if int(payload.TransactionIndex) >= len(block.Transactions()) {
		return fmt.Errorf("transaction index %d out of range for block %s", payload.TransactionIndex, payload.BlockNumber.String())
	}

	tx := block.Transactions()[payload.TransactionIndex]
	if tx.Hash().String() != payload.TransactionHash {
		return fmt.Errorf("transaction hash mismatch: expected %s, got %s", payload.TransactionHash, tx.Hash().String())
	}

	// Process transaction using ch-go streaming
	structlogCount, err := p.ProcessTransaction(ctx, block, int(payload.TransactionIndex), tx)
	if err != nil {
		common.TasksErrored.WithLabelValues(p.network.Name, ProcessorName, tracker.ProcessForwardsQueue(ProcessorName), ProcessForwardsTaskType, "processing_error").Inc()

		return fmt.Errorf("failed to process transaction: %w", err)
	}

	// Record successful processing
	common.TasksProcessed.WithLabelValues(p.network.Name, ProcessorName, tracker.ProcessForwardsQueue(ProcessorName), ProcessForwardsTaskType, "success").Inc()

	// Track task completion using SET-based tracker
	taskID := GenerateTaskID(p.network.Name, blockNumber.Uint64(), payload.TransactionHash)

	allComplete, trackErr := p.completionTracker.TrackTaskCompletion(ctx, taskID, blockNumber.Uint64(), p.network.Name, p.Name(), tracker.FORWARDS_MODE)
	if trackErr != nil {
		p.log.WithError(trackErr).WithFields(logrus.Fields{
			"block_number": blockNumber.Uint64(),
			"task_id":      taskID,
		}).Warn("Failed to track task completion")
		// Non-fatal - stale detection will catch it
	}

	if allComplete {
		if markErr := p.completionTracker.MarkBlockComplete(ctx, blockNumber.Uint64(), p.network.Name, p.Name(), tracker.FORWARDS_MODE); markErr != nil {
			p.log.WithError(markErr).WithFields(logrus.Fields{
				"block_number": blockNumber.Uint64(),
			}).Error("Failed to mark block complete")
		}
	}

	p.log.WithFields(logrus.Fields{
		"transaction_hash": payload.TransactionHash,
		"structlog_count":  structlogCount,
	}).Debug("Processed transaction")

	return nil
}

// handleProcessBackwardsTask handles the backwards processing of a single transaction.
func (p *Processor) handleProcessBackwardsTask(ctx context.Context, task *asynq.Task) error {
	start := time.Now()

	defer func() {
		duration := time.Since(start)
		common.TaskProcessingDuration.WithLabelValues(p.network.Name, ProcessorName, tracker.ProcessBackwardsQueue(ProcessorName), ProcessBackwardsTaskType).Observe(duration.Seconds())
	}()

	var payload ProcessPayload
	if err := payload.UnmarshalBinary(task.Payload()); err != nil {
		common.TasksErrored.WithLabelValues(p.network.Name, ProcessorName, tracker.ProcessBackwardsQueue(ProcessorName), ProcessBackwardsTaskType, "unmarshal_error").Inc()

		return fmt.Errorf("failed to unmarshal process payload: %w", err)
	}

	// Get healthy execution node
	node := p.pool.GetHealthyExecutionNode()
	if node == nil {
		return fmt.Errorf("no healthy execution node available")
	}

	// Get block data
	blockNumber := &payload.BlockNumber

	block, err := node.BlockByNumber(ctx, blockNumber)
	if err != nil {
		return fmt.Errorf("failed to get block: %w", err)
	}

	// Find the transaction in the block
	if int(payload.TransactionIndex) >= len(block.Transactions()) {
		return fmt.Errorf("transaction index %d out of range for block %s", payload.TransactionIndex, payload.BlockNumber.String())
	}

	tx := block.Transactions()[payload.TransactionIndex]
	if tx.Hash().String() != payload.TransactionHash {
		return fmt.Errorf("transaction hash mismatch: expected %s, got %s", payload.TransactionHash, tx.Hash().String())
	}

	// Process transaction using ch-go streaming
	structlogCount, err := p.ProcessTransaction(ctx, block, int(payload.TransactionIndex), tx)
	if err != nil {
		common.TasksErrored.WithLabelValues(p.network.Name, ProcessorName, tracker.ProcessBackwardsQueue(ProcessorName), ProcessBackwardsTaskType, "processing_error").Inc()

		return fmt.Errorf("failed to process transaction: %w", err)
	}

	// Record successful processing
	common.TasksProcessed.WithLabelValues(p.network.Name, ProcessorName, tracker.ProcessBackwardsQueue(ProcessorName), ProcessBackwardsTaskType, "success").Inc()

	// Track task completion using SET-based tracker
	taskID := GenerateTaskID(p.network.Name, blockNumber.Uint64(), payload.TransactionHash)

	allComplete, trackErr := p.completionTracker.TrackTaskCompletion(ctx, taskID, blockNumber.Uint64(), p.network.Name, p.Name(), tracker.BACKWARDS_MODE)
	if trackErr != nil {
		p.log.WithError(trackErr).WithFields(logrus.Fields{
			"block_number": blockNumber.Uint64(),
			"task_id":      taskID,
		}).Warn("Failed to track task completion")
		// Non-fatal - stale detection will catch it
	}

	if allComplete {
		if markErr := p.completionTracker.MarkBlockComplete(ctx, blockNumber.Uint64(), p.network.Name, p.Name(), tracker.BACKWARDS_MODE); markErr != nil {
			p.log.WithError(markErr).WithFields(logrus.Fields{
				"block_number": blockNumber.Uint64(),
			}).Error("Failed to mark block complete")
		}
	}

	p.log.WithFields(logrus.Fields{
		"transaction_hash": payload.TransactionHash,
		"structlog_count":  structlogCount,
	}).Debug("Processed transaction")

	return nil
}
