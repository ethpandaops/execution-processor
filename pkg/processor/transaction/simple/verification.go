package simple

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/common"
	c "github.com/ethpandaops/execution-processor/pkg/processor/common"
)

// CountMismatchError represents a transaction count mismatch.
type CountMismatchError struct {
	Expected int
	Actual   int
}

// Error implements the error interface.
func (e *CountMismatchError) Error() string {
	return fmt.Sprintf("count mismatch: expected %d, got %d", e.Expected, e.Actual)
}

// handleVerifyTask handles verification tasks for both forwards and backwards modes.
func (p *Processor) handleVerifyTask(ctx context.Context, task *asynq.Task) error {
	start := time.Now()

	defer func() {
		duration := time.Since(start)
		common.TaskProcessingDuration.WithLabelValues(
			p.network.Name,
			ProcessorName,
			c.VerifyForwardsQueue(ProcessorName),
			task.Type(),
		).Observe(duration.Seconds())
	}()

	var payload VerifyPayload
	if err := payload.UnmarshalBinary(task.Payload()); err != nil {
		common.TasksErrored.WithLabelValues(
			p.network.Name,
			ProcessorName,
			c.VerifyForwardsQueue(ProcessorName),
			task.Type(),
			"unmarshal_error",
		).Inc()

		return fmt.Errorf("failed to unmarshal payload: %w", err)
	}

	blockNumber := payload.BlockNumber.Uint64()

	p.log.WithFields(logrus.Fields{
		"block":    blockNumber,
		"expected": payload.InsertedCount,
	}).Debug("Verifying block")

	// Get expected count from execution node
	node := p.pool.GetHealthyExecutionNode()
	if node == nil {
		return fmt.Errorf("no healthy execution node available")
	}

	block, err := node.BlockByNumber(ctx, &payload.BlockNumber)
	if err != nil {
		return fmt.Errorf("failed to get block: %w", err)
	}

	expectedCount := len(block.Transactions())

	// Query ClickHouse for actual count
	db := p.clickhouse.GetDB()
	if db == nil {
		return fmt.Errorf("clickhouse connection not available")
	}

	//nolint:gosec // table name is from config, not user input
	query := fmt.Sprintf(`
		SELECT COUNT(*) as count
		FROM %s FINAL
		WHERE block_number = %d
		  AND meta_network_name = '%s'
	`, p.config.Table, blockNumber, payload.NetworkName)

	var actualCount int

	row := db.QueryRowContext(ctx, query)
	if scanErr := row.Scan(&actualCount); scanErr != nil {
		if errors.Is(scanErr, sql.ErrNoRows) {
			actualCount = 0
		} else {
			return fmt.Errorf("failed to query count: %w", scanErr)
		}
	}

	// Check if counts match
	if actualCount != expectedCount {
		p.log.WithFields(logrus.Fields{
			"block":    blockNumber,
			"expected": expectedCount,
			"actual":   actualCount,
		}).Warn("Transaction count mismatch, re-queuing")

		// Re-enqueue process task with delay
		processPayload := &ProcessPayload{
			BlockNumber: payload.BlockNumber,
			NetworkID:   payload.NetworkID,
			NetworkName: payload.NetworkName,
		}

		var processTask *asynq.Task

		var queue string

		// Determine queue based on task type
		if task.Type() == VerifyBackwardsTaskType {
			processPayload.ProcessingMode = c.BACKWARDS_MODE
			processTask, err = NewProcessBackwardsTask(processPayload)
			queue = p.getProcessBackwardsQueue()
		} else {
			processPayload.ProcessingMode = c.FORWARDS_MODE
			processTask, err = NewProcessForwardsTask(processPayload)
			queue = p.getProcessForwardsQueue()
		}

		if err != nil {
			return fmt.Errorf("failed to create process task: %w", err)
		}

		if err := p.EnqueueTask(ctx, processTask,
			asynq.Queue(queue),
			asynq.ProcessIn(5*time.Minute),
		); err != nil {
			return fmt.Errorf("failed to re-enqueue process task: %w", err)
		}

		common.TasksEnqueued.WithLabelValues(p.network.Name, ProcessorName, queue, processTask.Type()).Inc()
		common.RetryCount.WithLabelValues(p.network.Name, ProcessorName, "count_mismatch").Inc()

		// Return nil - task succeeded in re-enqueueing
		common.TasksProcessed.WithLabelValues(
			p.network.Name,
			ProcessorName,
			c.VerifyForwardsQueue(ProcessorName),
			task.Type(),
			"reenqueued",
		).Inc()

		return nil
	}

	p.log.WithFields(logrus.Fields{
		"block": blockNumber,
		"count": actualCount,
	}).Debug("Block verified")

	common.TasksProcessed.WithLabelValues(
		p.network.Name,
		ProcessorName,
		c.VerifyForwardsQueue(ProcessorName),
		task.Type(),
		"success",
	).Inc()

	return nil
}
