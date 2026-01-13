package structlog

import (
	"context"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/common"
	c "github.com/ethpandaops/execution-processor/pkg/processor/common"
)

// handleProcessForwardsTask handles the forwards processing of a single transaction.
func (p *Processor) handleProcessForwardsTask(ctx context.Context, task *asynq.Task) error {
	start := time.Now()

	defer func() {
		duration := time.Since(start)
		common.TaskProcessingDuration.WithLabelValues(p.network.Name, ProcessorName, c.ProcessForwardsQueue(ProcessorName), ProcessForwardsTaskType).Observe(duration.Seconds())
	}()

	var payload ProcessPayload
	if err := payload.UnmarshalBinary(task.Payload()); err != nil {
		common.TasksErrored.WithLabelValues(p.network.Name, ProcessorName, c.ProcessForwardsQueue(ProcessorName), ProcessForwardsTaskType, "unmarshal_error").Inc()

		return fmt.Errorf("failed to unmarshal process payload: %w", err)
	}

	// Wait for any active big transactions before starting
	p.waitForBigTransactions("process_forwards")

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

	// Extract structlogs from the transaction
	structlogs, err := p.ExtractStructlogs(ctx, block, int(payload.TransactionIndex), tx)
	if err != nil {
		common.TasksErrored.WithLabelValues(p.network.Name, ProcessorName, c.ProcessForwardsQueue(ProcessorName), ProcessForwardsTaskType, "extraction_error").Inc()

		return fmt.Errorf("failed to extract structlogs: %w", err)
	}

	structlogCount := int64(len(structlogs))

	// Check if transaction should be batched
	if p.ShouldBatch(structlogCount) {
		// Route to batch manager
		if addErr := p.batchManager.Add(structlogs, task, &payload); addErr != nil {
			common.TasksErrored.WithLabelValues(p.network.Name, ProcessorName, c.ProcessForwardsQueue(ProcessorName), ProcessForwardsTaskType, "batch_add_error").Inc()

			return fmt.Errorf("failed to add to batch: %w", addErr)
		}
		// Note: Task completion handled by batch manager
		return nil
	}

	// Process large transaction using existing logic
	_, err = p.ProcessTransaction(ctx, block, int(payload.TransactionIndex), tx)
	if err != nil {
		common.TasksErrored.WithLabelValues(p.network.Name, ProcessorName, c.ProcessForwardsQueue(ProcessorName), ProcessForwardsTaskType, "processing_error").Inc()

		return fmt.Errorf("failed to process transaction: %w", err)
	}

	// Record successful processing
	common.TasksProcessed.WithLabelValues(p.network.Name, ProcessorName, c.ProcessForwardsQueue(ProcessorName), ProcessForwardsTaskType, "success").Inc()

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
		common.TaskProcessingDuration.WithLabelValues(p.network.Name, ProcessorName, c.ProcessBackwardsQueue(ProcessorName), ProcessBackwardsTaskType).Observe(duration.Seconds())
	}()

	var payload ProcessPayload
	if err := payload.UnmarshalBinary(task.Payload()); err != nil {
		common.TasksErrored.WithLabelValues(p.network.Name, ProcessorName, c.ProcessBackwardsQueue(ProcessorName), ProcessBackwardsTaskType, "unmarshal_error").Inc()

		return fmt.Errorf("failed to unmarshal process payload: %w", err)
	}

	// Wait for any active big transactions before starting
	p.waitForBigTransactions("process_backwards")

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

	// Extract structlogs from the transaction
	structlogs, err := p.ExtractStructlogs(ctx, block, int(payload.TransactionIndex), tx)
	if err != nil {
		common.TasksErrored.WithLabelValues(p.network.Name, ProcessorName, c.ProcessBackwardsQueue(ProcessorName), ProcessBackwardsTaskType, "extraction_error").Inc()

		return fmt.Errorf("failed to extract structlogs: %w", err)
	}

	structlogCount := int64(len(structlogs))

	// Check if transaction should be batched
	if p.ShouldBatch(structlogCount) {
		// Route to batch manager
		if addErr := p.batchManager.Add(structlogs, task, &payload); addErr != nil {
			common.TasksErrored.WithLabelValues(p.network.Name, ProcessorName, c.ProcessBackwardsQueue(ProcessorName), ProcessBackwardsTaskType, "batch_add_error").Inc()

			return fmt.Errorf("failed to add to batch: %w", addErr)
		}
		// Note: Task completion handled by batch manager
		return nil
	}

	// Process large transaction using existing logic
	_, processErr := p.ProcessTransaction(ctx, block, int(payload.TransactionIndex), tx)
	if processErr != nil {
		common.TasksErrored.WithLabelValues(p.network.Name, ProcessorName, c.ProcessBackwardsQueue(ProcessorName), ProcessBackwardsTaskType, "processing_error").Inc()

		return fmt.Errorf("failed to process transaction: %w", processErr)
	}

	// Record successful processing
	common.TasksProcessed.WithLabelValues(p.network.Name, ProcessorName, c.ProcessBackwardsQueue(ProcessorName), ProcessBackwardsTaskType, "success").Inc()

	p.log.WithFields(logrus.Fields{
		"transaction_hash": payload.TransactionHash,
		"structlog_count":  structlogCount,
	}).Debug("Processed transaction")

	return nil
}
