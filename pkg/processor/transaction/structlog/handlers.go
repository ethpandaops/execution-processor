package structlog

import (
	"context"
	"errors"
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

	// After successful processing, enqueue verify task
	p.log.WithFields(logrus.Fields{
		"transaction_hash": payload.TransactionHash,
		"structlog_count":  structlogCount,
	}).Debug("Preparing to enqueue verify task")

	verifyPayload := &VerifyPayload{
		BlockNumber:      payload.BlockNumber,
		TransactionHash:  payload.TransactionHash,
		TransactionIndex: payload.TransactionIndex,
		NetworkID:        payload.NetworkID,
		NetworkName:      payload.NetworkName,
		Network:          payload.Network,
		InsertedCount:    int(structlogCount),
	}

	p.log.WithFields(logrus.Fields{
		"transaction_hash":       payload.TransactionHash,
		"actual_structlog_count": structlogCount,
	}).Debug("Created verify payload with correct expected count")

	verifyTask, err := NewVerifyForwardsTask(verifyPayload)
	if err != nil {
		p.log.WithError(err).Error("Failed to create verify task")

		return fmt.Errorf("failed to create verify task: %w", err)
	}

	queue := p.getVerifyForwardsQueue()
	p.log.WithFields(logrus.Fields{
		"queue":            queue,
		"transaction_hash": payload.TransactionHash,
		"expected_count":   structlogCount,
	}).Debug("Enqueueing verify task")

	if err := p.EnqueueTask(ctx, verifyTask, asynq.Queue(queue), asynq.ProcessIn(10*time.Second)); err != nil {
		p.log.WithError(err).WithField("queue", queue).Error("Failed to enqueue verify task")
		common.TasksErrored.WithLabelValues(p.network.Name, ProcessorName, c.ProcessForwardsQueue(ProcessorName), ProcessForwardsTaskType, "enqueue_verify_error").Inc()

		return fmt.Errorf("failed to enqueue verify task: %w", err)
	}

	common.TasksEnqueued.WithLabelValues(p.network.Name, ProcessorName, queue, VerifyForwardsTaskType).Inc()

	p.log.WithFields(logrus.Fields{
		"transaction_hash": payload.TransactionHash,
		"queue":            queue,
	}).Debug("Successfully enqueued verify task")

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

	// After successful processing, enqueue verify task
	p.log.WithFields(logrus.Fields{
		"transaction_hash": payload.TransactionHash,
		"structlog_count":  structlogCount,
	}).Debug("Preparing to enqueue verify task")

	verifyPayload := &VerifyPayload{
		BlockNumber:      payload.BlockNumber,
		TransactionHash:  payload.TransactionHash,
		TransactionIndex: payload.TransactionIndex,
		NetworkID:        payload.NetworkID,
		NetworkName:      payload.NetworkName,
		Network:          payload.Network,
		InsertedCount:    int(structlogCount),
	}

	p.log.WithFields(logrus.Fields{
		"transaction_hash":       payload.TransactionHash,
		"actual_structlog_count": structlogCount,
	}).Debug("Created verify payload with correct expected count")

	verifyTask, err := NewVerifyBackwardsTask(verifyPayload)
	if err != nil {
		p.log.WithError(err).Error("Failed to create verify task")

		return fmt.Errorf("failed to create verify task: %w", err)
	}

	queue := p.getVerifyBackwardsQueue()
	p.log.WithFields(logrus.Fields{
		"queue":            queue,
		"transaction_hash": payload.TransactionHash,
		"expected_count":   structlogCount,
	}).Debug("Enqueueing verify task")

	if err := p.EnqueueTask(ctx, verifyTask, asynq.Queue(queue), asynq.ProcessIn(10*time.Second)); err != nil {
		p.log.WithError(err).WithField("queue", queue).Error("Failed to enqueue verify task")
		common.TasksErrored.WithLabelValues(p.network.Name, ProcessorName, c.ProcessBackwardsQueue(ProcessorName), ProcessBackwardsTaskType, "enqueue_verify_error").Inc()

		return fmt.Errorf("failed to enqueue verify task: %w", err)
	}

	common.TasksEnqueued.WithLabelValues(p.network.Name, ProcessorName, queue, VerifyBackwardsTaskType).Inc()

	p.log.WithFields(logrus.Fields{
		"transaction_hash": payload.TransactionHash,
		"queue":            queue,
	}).Debug("Successfully enqueued verify task")

	return nil
}

// handleVerifyForwardsTask handles the verification of a forwards processed transaction.
func (p *Processor) handleVerifyForwardsTask(ctx context.Context, task *asynq.Task) error {
	start := time.Now()

	defer func() {
		duration := time.Since(start)
		common.TaskProcessingDuration.WithLabelValues(p.network.Name, ProcessorName, c.VerifyForwardsQueue(ProcessorName), VerifyForwardsTaskType).Observe(duration.Seconds())
	}()

	// Wait for any active big transactions before starting verify
	p.waitForBigTransactions("verify_forwards")

	p.log.Debug("Received verify task")

	var payload VerifyPayload
	if err := payload.UnmarshalBinary(task.Payload()); err != nil {
		p.log.WithError(err).Error("Failed to unmarshal verify payload")
		common.TasksErrored.WithLabelValues(p.network.Name, ProcessorName, c.VerifyForwardsQueue(ProcessorName), VerifyForwardsTaskType, "unmarshal_error").Inc()

		return fmt.Errorf("failed to unmarshal verify payload: %w", err)
	}

	p.log.WithFields(logrus.Fields{
		"block_number":      payload.BlockNumber.String(),
		"transaction_hash":  payload.TransactionHash,
		"transaction_index": payload.TransactionIndex,
		"network":           payload.NetworkName,
	}).Debug("Processing verify task")

	// Verify the transaction
	if err := p.VerifyTransaction(ctx, &payload.BlockNumber, payload.TransactionHash, payload.TransactionIndex, payload.NetworkName, payload.InsertedCount); err != nil {
		// Check if it's a count mismatch error
		var countMismatchErr *CountMismatchError
		if errors.As(err, &countMismatchErr) {
			// Re-enqueue the original process task
			processPayload := &ProcessPayload{
				BlockNumber:      payload.BlockNumber,
				TransactionHash:  payload.TransactionHash,
				TransactionIndex: payload.TransactionIndex,
				NetworkID:        payload.NetworkID,
				NetworkName:      payload.NetworkName,
				Network:          payload.Network,
			}

			// Create the process task
			processTask, createErr := NewProcessForwardsTask(processPayload)
			if createErr != nil {
				p.log.WithError(createErr).Error("Failed to create reprocess task for count mismatch")
				common.TasksErrored.WithLabelValues(p.network.Name, ProcessorName, c.VerifyForwardsQueue(ProcessorName), VerifyForwardsTaskType, "create_reprocess_error").Inc()

				return fmt.Errorf("failed to create reprocess task: %w", createErr)
			}

			// Enqueue with 5-minute delay to allow ClickHouse to settle
			queue := p.getProcessForwardsQueue()
			if enqueueErr := p.EnqueueTask(ctx, processTask,
				asynq.Queue(queue),
				asynq.ProcessIn(5*time.Minute)); enqueueErr != nil {
				p.log.WithError(enqueueErr).WithField("queue", queue).Error("Failed to enqueue reprocess task")
				common.TasksErrored.WithLabelValues(p.network.Name, ProcessorName, c.VerifyForwardsQueue(ProcessorName), VerifyForwardsTaskType, "enqueue_reprocess_error").Inc()

				return fmt.Errorf("failed to enqueue reprocess task: %w", enqueueErr)
			}

			// Log the re-enqueue with delay info
			p.log.WithFields(logrus.Fields{
				"transaction_hash": payload.TransactionHash,
				"block_number":     payload.BlockNumber.String(),
				"expected_count":   countMismatchErr.Expected,
				"actual_count":     countMismatchErr.Actual,
				"retry_delay":      "5m",
				"queue":            queue,
			}).Info("Re-enqueuing process task due to count mismatch (5-minute delay)")

			// Track re-enqueue metric
			common.TasksEnqueued.WithLabelValues(p.network.Name, ProcessorName, queue, ProcessForwardsTaskType).Inc()
			common.RetryCount.WithLabelValues(p.network.Name, ProcessorName, "count_mismatch").Inc()
			common.VerificationMismatchRate.WithLabelValues(p.network.Name, ProcessorName, payload.TransactionHash).Inc()

			// Record successful handling (re-enqueue)
			common.TasksProcessed.WithLabelValues(p.network.Name, ProcessorName, c.VerifyForwardsQueue(ProcessorName), VerifyForwardsTaskType, "reenqueued").Inc()

			// Return nil - task succeeded in re-enqueueing
			return nil
		}

		// For other errors, handle normally
		p.log.WithError(err).WithFields(logrus.Fields{
			"transaction_hash": payload.TransactionHash,
			"block_number":     payload.BlockNumber.String(),
		}).Error("Transaction verification failed")
		common.TasksErrored.WithLabelValues(p.network.Name, ProcessorName, c.VerifyForwardsQueue(ProcessorName), VerifyForwardsTaskType, "verification_error").Inc()

		return fmt.Errorf("failed to verify transaction: %w", err)
	}

	// Record successful verification
	common.TasksProcessed.WithLabelValues(p.network.Name, ProcessorName, c.VerifyForwardsQueue(ProcessorName), VerifyForwardsTaskType, "success").Inc()

	p.log.WithFields(logrus.Fields{
		"transaction_hash": payload.TransactionHash,
		"block_number":     payload.BlockNumber.String(),
	}).Debug("Verify task completed successfully")

	return nil
}

// handleVerifyBackwardsTask handles the verification of a backwards processed transaction.
func (p *Processor) handleVerifyBackwardsTask(ctx context.Context, task *asynq.Task) error {
	start := time.Now()

	defer func() {
		duration := time.Since(start)
		common.TaskProcessingDuration.WithLabelValues(p.network.Name, ProcessorName, c.VerifyBackwardsQueue(ProcessorName), VerifyBackwardsTaskType).Observe(duration.Seconds())
	}()

	// Wait for any active big transactions before starting verify
	p.waitForBigTransactions("verify_backwards")

	p.log.Debug("Received verify task")

	var payload VerifyPayload
	if err := payload.UnmarshalBinary(task.Payload()); err != nil {
		p.log.WithError(err).Error("Failed to unmarshal verify payload")
		common.TasksErrored.WithLabelValues(p.network.Name, ProcessorName, c.VerifyBackwardsQueue(ProcessorName), VerifyBackwardsTaskType, "unmarshal_error").Inc()

		return fmt.Errorf("failed to unmarshal verify payload: %w", err)
	}

	p.log.WithFields(logrus.Fields{
		"block_number":      payload.BlockNumber.String(),
		"transaction_hash":  payload.TransactionHash,
		"transaction_index": payload.TransactionIndex,
		"network":           payload.NetworkName,
	}).Debug("Processing verify task")

	// Verify the transaction
	if err := p.VerifyTransaction(ctx, &payload.BlockNumber, payload.TransactionHash, payload.TransactionIndex, payload.NetworkName, payload.InsertedCount); err != nil {
		// Check if it's a count mismatch error
		var countMismatchErr *CountMismatchError
		if errors.As(err, &countMismatchErr) {
			// Re-enqueue the original process task
			processPayload := &ProcessPayload{
				BlockNumber:      payload.BlockNumber,
				TransactionHash:  payload.TransactionHash,
				TransactionIndex: payload.TransactionIndex,
				NetworkID:        payload.NetworkID,
				NetworkName:      payload.NetworkName,
				Network:          payload.Network,
			}

			// Create the process task
			processTask, createErr := NewProcessBackwardsTask(processPayload)
			if createErr != nil {
				p.log.WithError(createErr).Error("Failed to create reprocess task for count mismatch")
				common.TasksErrored.WithLabelValues(p.network.Name, ProcessorName, c.VerifyBackwardsQueue(ProcessorName), VerifyBackwardsTaskType, "create_reprocess_error").Inc()

				return fmt.Errorf("failed to create reprocess task: %w", createErr)
			}

			// Enqueue with 5-minute delay to allow ClickHouse to settle
			queue := p.getProcessBackwardsQueue()
			if enqueueErr := p.EnqueueTask(ctx, processTask,
				asynq.Queue(queue),
				asynq.ProcessIn(5*time.Minute)); enqueueErr != nil {
				p.log.WithError(enqueueErr).WithField("queue", queue).Error("Failed to enqueue reprocess task")
				common.TasksErrored.WithLabelValues(p.network.Name, ProcessorName, c.VerifyBackwardsQueue(ProcessorName), VerifyBackwardsTaskType, "enqueue_reprocess_error").Inc()

				return fmt.Errorf("failed to enqueue reprocess task: %w", enqueueErr)
			}

			// Log the re-enqueue with delay info
			p.log.WithFields(logrus.Fields{
				"transaction_hash": payload.TransactionHash,
				"block_number":     payload.BlockNumber.String(),
				"expected_count":   countMismatchErr.Expected,
				"actual_count":     countMismatchErr.Actual,
				"retry_delay":      "5m",
				"queue":            queue,
			}).Info("Re-enqueuing process task due to count mismatch (5-minute delay)")

			// Track re-enqueue metric
			common.TasksEnqueued.WithLabelValues(p.network.Name, ProcessorName, queue, ProcessBackwardsTaskType).Inc()

			// Record successful handling (re-enqueue)
			common.TasksProcessed.WithLabelValues(p.network.Name, ProcessorName, c.VerifyBackwardsQueue(ProcessorName), VerifyBackwardsTaskType, "reenqueued").Inc()

			// Return nil - task succeeded in re-enqueueing
			return nil
		}

		// For other errors, handle normally
		p.log.WithError(err).WithFields(logrus.Fields{
			"transaction_hash": payload.TransactionHash,
			"block_number":     payload.BlockNumber.String(),
		}).Error("Transaction verification failed")
		common.TasksErrored.WithLabelValues(p.network.Name, ProcessorName, c.VerifyBackwardsQueue(ProcessorName), VerifyBackwardsTaskType, "verification_error").Inc()

		return fmt.Errorf("failed to verify transaction: %w", err)
	}

	// Record successful verification
	common.TasksProcessed.WithLabelValues(p.network.Name, ProcessorName, c.VerifyBackwardsQueue(ProcessorName), VerifyBackwardsTaskType, "success").Inc()

	p.log.WithFields(logrus.Fields{
		"transaction_hash": payload.TransactionHash,
		"block_number":     payload.BlockNumber.String(),
	}).Debug("Verify task completed successfully")

	return nil
}
