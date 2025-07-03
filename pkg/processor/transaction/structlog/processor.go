package structlog

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
	"github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/ethpandaops/execution-processor/pkg/ethereum"
	c "github.com/ethpandaops/execution-processor/pkg/processor/common"
	"github.com/ethpandaops/execution-processor/pkg/state"
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
)

// Dependencies contains the dependencies needed for the processor
type Dependencies struct {
	Log         logrus.FieldLogger
	Pool        *ethereum.Pool
	Network     *ethereum.Network
	State       *state.Manager
	AsynqClient *asynq.Client
	RedisPrefix string
}

// Processor handles transaction structlog processing
type Processor struct {
	log            logrus.FieldLogger
	pool           *ethereum.Pool
	stateManager   *state.Manager
	clickhouse     *clickhouse.Client
	config         *Config
	network        *ethereum.Network
	asynqClient    *asynq.Client
	processingMode string
	redisPrefix    string
	batchCollector *BatchCollector
	largeTxLock    *LargeTxLockManager
}

// New creates a new transaction structlog processor
func New(ctx context.Context, deps *Dependencies, config *Config) (*Processor, error) {
	clickhouseClient, err := clickhouse.NewClient(ctx, deps.Log.WithField("processor", "transaction_structlog"), &clickhouse.Config{
		DSN:          config.DSN,
		MaxOpenConns: config.MaxOpenConns,
		MaxIdleConns: config.MaxIdleConns,
		Network:      deps.Network.Name,
		Processor:    ProcessorName,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create clickhouse client for transaction_structlog: %w", err)
	}

	processor := &Processor{
		log:            deps.Log.WithField("processor", ProcessorName),
		pool:           deps.Pool,
		stateManager:   deps.State,
		clickhouse:     clickhouseClient,
		config:         config,
		asynqClient:    deps.AsynqClient,
		processingMode: c.FORWARDS_MODE, // Default mode
		redisPrefix:    deps.RedisPrefix,
	}

	processor.network = deps.Network

	processor.log.WithFields(logrus.Fields{
		"network":  processor.network.Name,
		"chain_id": processor.network.ID,
	}).Info("Detected network")

	// Initialize batch collector if enabled
	if config.BatchConfig.Enabled {
		processor.batchCollector = NewBatchCollector(processor, config.BatchConfig)
		processor.log.Info("Batch collector enabled")
	} else {
		processor.log.Info("Batch collector disabled")
	}

	// Initialize large transaction lock manager
	if config.LargeTransactionConfig != nil && config.LargeTransactionConfig.Enabled {
		processor.largeTxLock = NewLargeTxLockManager(processor.log, config.LargeTransactionConfig)
		processor.log.WithFields(logrus.Fields{
			"threshold":           config.LargeTransactionConfig.StructlogThreshold,
			"worker_wait_timeout": config.LargeTransactionConfig.WorkerWaitTimeout,
			"max_processing_time": config.LargeTransactionConfig.MaxProcessingTime,
			"sequential_mode":     config.LargeTransactionConfig.EnableSequentialMode,
		}).Info("Large transaction handling enabled")
	} else {
		processor.log.Info("Large transaction handling disabled")
	}

	return processor, nil
}

// Start starts the processor
func (p *Processor) Start(ctx context.Context) error {
	// Start the ClickHouse client
	if err := p.clickhouse.Start(ctx); err != nil {
		return fmt.Errorf("failed to start ClickHouse client: %w", err)
	}

	// Start batch collector if enabled
	if p.batchCollector != nil {
		if err := p.batchCollector.Start(ctx); err != nil {
			return fmt.Errorf("failed to start batch collector: %w", err)
		}
	}

	p.log.Info("Transaction structlog processor ready")

	return nil
}

// Stop stops the processor
func (p *Processor) Stop(ctx context.Context) error {
	p.log.Info("Stopping transaction structlog processor")

	// Stop batch collector first to flush remaining data
	if p.batchCollector != nil {
		if err := p.batchCollector.Stop(ctx); err != nil {
			p.log.WithError(err).Error("Failed to stop batch collector")
		}
	}

	// Stop the ClickHouse client
	return p.clickhouse.Stop(ctx)
}

// Name returns the processor name
func (p *Processor) Name() string {
	return ProcessorName
}

// GetQueues returns the queues used by this processor
func (p *Processor) GetQueues() []c.QueueInfo {
	return []c.QueueInfo{
		{
			Name:     c.PrefixedProcessForwardsQueue(ProcessorName, p.redisPrefix),
			Priority: 10, // Highest priority for forwards processing
		},
		{
			Name:     c.PrefixedProcessBackwardsQueue(ProcessorName, p.redisPrefix),
			Priority: 5, // Medium priority for backwards processing
		},
		{
			Name:     c.PrefixedVerifyForwardsQueue(ProcessorName, p.redisPrefix),
			Priority: 1, // Low priority for forwards verification
		},
		{
			Name:     c.PrefixedVerifyBackwardsQueue(ProcessorName, p.redisPrefix),
			Priority: 1, // Low priority for backwards verification
		},
	}
}

// GetHandlers returns the task handlers for this processor
func (p *Processor) GetHandlers() map[string]asynq.HandlerFunc {
	return map[string]asynq.HandlerFunc{
		ProcessForwardsTaskType:  p.handleProcessForwardsTask,
		ProcessBackwardsTaskType: p.handleProcessBackwardsTask,
		VerifyForwardsTaskType:   p.handleVerifyForwardsTask,
		VerifyBackwardsTaskType:  p.handleVerifyBackwardsTask,
	}
}

// EnqueueTask enqueues a task to the specified queue
func (p *Processor) EnqueueTask(ctx context.Context, task *asynq.Task, opts ...asynq.Option) error {
	_, err := p.asynqClient.EnqueueContext(ctx, task, opts...)

	return err
}

// SetProcessingMode sets the processing mode for the processor
func (p *Processor) SetProcessingMode(mode string) {
	p.processingMode = mode
	p.log.WithField("mode", mode).Info("Processing mode updated")
}

// getProcessForwardsQueue returns the prefixed process forwards queue name
func (p *Processor) getProcessForwardsQueue() string {
	return c.PrefixedProcessForwardsQueue(ProcessorName, p.redisPrefix)
}

// getProcessBackwardsQueue returns the prefixed process backwards queue name
func (p *Processor) getProcessBackwardsQueue() string {
	return c.PrefixedProcessBackwardsQueue(ProcessorName, p.redisPrefix)
}

// getVerifyForwardsQueue returns the prefixed verify forwards queue name
func (p *Processor) getVerifyForwardsQueue() string {
	return c.PrefixedVerifyForwardsQueue(ProcessorName, p.redisPrefix)
}

// getVerifyBackwardsQueue returns the prefixed verify backwards queue name
func (p *Processor) getVerifyBackwardsQueue() string {
	return c.PrefixedVerifyBackwardsQueue(ProcessorName, p.redisPrefix)
}

// GetLargeTxStatus returns the current status of large transaction processing
func (p *Processor) GetLargeTxStatus() map[string]interface{} {
	if p.largeTxLock == nil {
		return map[string]interface{}{
			"enabled": false,
		}
	}

	return p.largeTxLock.GetStatus()
}

// sendToBatchCollector sends structlog rows to the batch collector for aggregated insertion
func (p *Processor) sendToBatchCollector(ctx context.Context, structlogs []Structlog) error {
	// Short-circuit for empty structlog arrays - no need to process
	if len(structlogs) == 0 {
		return nil
	}

	if p.batchCollector == nil {
		// Batch collector not enabled, fallback to direct insert
		// Create timeout context for direct insert using configured timeout
		timeout := 5 * time.Minute
		if p.config.BatchConfig.FlushTimeout > 0 {
			timeout = p.config.BatchConfig.FlushTimeout
		}

		insertCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		return p.insertStructlogBatch(insertCtx, structlogs)
	}

	// Create task batch with unique ID
	taskBatch := TaskBatch{
		Rows:         structlogs,
		ResponseChan: make(chan error, 1),
		TaskID:       fmt.Sprintf("%d-%d", time.Now().UnixNano(), len(structlogs)),
	}

	// Ensure response channel is cleaned up on any exit path
	defer func() {
		// Drain the channel if it has any pending data
		select {
		case <-taskBatch.ResponseChan:
		default:
		}
		close(taskBatch.ResponseChan)
	}()

	// Submit to batch collector
	if err := p.batchCollector.SubmitBatch(ctx, taskBatch); err != nil {
		// Let Asynq handle retries - don't fallback to direct insert
		p.log.WithFields(logrus.Fields{
			"task_id": taskBatch.TaskID,
			"rows":    len(taskBatch.Rows),
			"error":   err.Error(),
		}).Debug("Failed to submit batch to collector, channel will be cleaned up")

		return fmt.Errorf("failed to submit batch: %w", err)
	}

	// Wait for batch completion - no timeout
	select {
	case err := <-taskBatch.ResponseChan:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// insertStructlogBatch performs the actual batch insert to ClickHouse
func (p *Processor) insertStructlogBatch(ctx context.Context, structlogs []Structlog) error {
	if len(structlogs) == 0 {
		return nil
	}
	// Begin transaction
	tx, err := p.clickhouse.GetDB().BeginTx(ctx, nil)
	if err != nil {
		// Check if this is a connection error
		if isConnectionError(err) {
			p.log.WithFields(logrus.Fields{
				"error":           err.Error(),
				"structlog_count": len(structlogs),
			}).Error("Connection error when beginning transaction - reconnecting ClickHouse client")

			// Nuclear option - recreate the entire connection
			reconnectCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			if reconnectErr := p.clickhouse.Reconnect(reconnectCtx); reconnectErr != nil {
				p.log.WithError(reconnectErr).Error("Failed to reconnect ClickHouse client")
			}
		}

		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	// Track transaction state properly to avoid double rollback
	var (
		committed  bool
		rolledBack bool
	)

	defer func() {
		if !committed && !rolledBack {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				p.log.WithError(rollbackErr).Warn("Failed to rollback transaction (may already be closed)")
			}

			rolledBack = true
		}
	}()

	//nolint:gosec // safe to use user input in query
	query := fmt.Sprintf(`INSERT INTO %s (
		updated_date_time, block_number, transaction_hash, transaction_index,
		transaction_gas, transaction_failed, transaction_return_value,
		index, program_counter, operation, gas, gas_cost, depth,
		return_data, refund, error, call_to_address, meta_network_id, meta_network_name
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, p.config.Table)

	start := time.Now()
	stmt, err := tx.PrepareContext(ctx, query)
	duration := time.Since(start)

	if err != nil {
		code := clickhouse.ParseErrorCode(err)
		common.ClickHouseOperationDuration.WithLabelValues(p.network.Name, ProcessorName, "prepare_batch_insert", p.config.Table, "failed", code).Observe(duration.Seconds())
		common.ClickHouseOperationTotal.WithLabelValues(p.network.Name, ProcessorName, "prepare_batch_insert", p.config.Table, "failed", code).Inc()

		// Check if this is a column metadata error
		if isColumnMetadataError(err) {
			p.log.WithFields(logrus.Fields{
				"table":           p.config.Table,
				"error":           err.Error(),
				"structlog_count": len(structlogs),
			}).Error("Column metadata error - reconnecting ClickHouse client")

			// Nuclear option - recreate the entire connection
			reconnectCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			if reconnectErr := p.clickhouse.Reconnect(reconnectCtx); reconnectErr != nil {
				p.log.WithError(reconnectErr).Error("Failed to reconnect ClickHouse client")
			}
		} else {
			p.log.WithFields(logrus.Fields{
				"table":           p.config.Table,
				"error":           err.Error(),
				"structlog_count": len(structlogs),
			}).Error("Failed to prepare ClickHouse batch statement")
		}

		return fmt.Errorf("failed to prepare batch statement for table %s: %w", p.config.Table, err)
	}

	// Ensure statement is closed to release resources
	defer func() {
		if closeErr := stmt.Close(); closeErr != nil {
			p.log.WithError(closeErr).Warn("Failed to close prepared statement")
		}
	}()

	common.ClickHouseOperationDuration.WithLabelValues(p.network.Name, ProcessorName, "prepare_batch_insert", p.config.Table, "success", "").Observe(duration.Seconds())
	common.ClickHouseOperationTotal.WithLabelValues(p.network.Name, ProcessorName, "prepare_batch_insert", p.config.Table, "success", "").Inc()

	// Insert all structlogs
	for i := range structlogs {
		row := &structlogs[i]
		_, err := stmt.ExecContext(ctx,
			row.UpdatedDateTime,
			row.BlockNumber,
			row.TransactionHash,
			row.TransactionIndex,
			row.TransactionGas,
			row.TransactionFailed,
			row.TransactionReturnValue,
			row.Index,
			row.ProgramCounter,
			row.Operation,
			row.Gas,
			row.GasCost,
			row.Depth,
			row.ReturnData,
			row.Refund,
			row.Error,
			row.CallToAddress,
			row.MetaNetworkID,
			row.MetaNetworkName,
		)

		if err != nil {
			p.log.WithFields(logrus.Fields{
				"table":           p.config.Table,
				"structlog_index": i,
				"transaction":     row.TransactionHash,
				"block":           row.BlockNumber,
				"error":           err.Error(),
			}).Error("Failed to insert structlog row in batch")

			return fmt.Errorf("failed to insert structlog row %d in batch: %w", i, err)
		}
	}

	start = time.Now()
	err = tx.Commit()
	duration = time.Since(start)

	if err != nil {
		code := clickhouse.ParseErrorCode(err)
		common.ClickHouseOperationDuration.WithLabelValues(p.network.Name, ProcessorName, "batch_insert", p.config.Table, "failed", code).Observe(duration.Seconds())
		common.ClickHouseOperationTotal.WithLabelValues(p.network.Name, ProcessorName, "batch_insert", p.config.Table, "failed", code).Inc()
		common.ClickHouseInsertsRows.WithLabelValues(p.network.Name, ProcessorName, p.config.Table, "failed", code).Add(float64(len(structlogs)))

		// Check if this is a connection error
		if isConnectionError(err) {
			p.log.WithFields(logrus.Fields{
				"error":           err.Error(),
				"structlog_count": len(structlogs),
			}).Error("Connection error during commit - reconnecting ClickHouse client")

			// Nuclear option - recreate the entire connection
			reconnectCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			if reconnectErr := p.clickhouse.Reconnect(reconnectCtx); reconnectErr != nil {
				p.log.WithError(reconnectErr).Error("Failed to reconnect ClickHouse client after connection error")
			}
		}

		return fmt.Errorf("failed to commit batch transaction: %w", err)
	}

	committed = true

	common.ClickHouseOperationTotal.WithLabelValues(p.network.Name, ProcessorName, "batch_insert", p.config.Table, "success", "").Inc()
	common.ClickHouseInsertsRows.WithLabelValues(p.network.Name, ProcessorName, p.config.Table, "success", "").Add(float64(len(structlogs)))
	common.ClickHouseOperationDuration.WithLabelValues(p.network.Name, ProcessorName, "batch_insert", p.config.Table, "success", "").Observe(duration.Seconds())

	return nil
}

// isColumnMetadataError checks if the error is related to column metadata issues
func isColumnMetadataError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	return strings.Contains(errStr, "column") && strings.Contains(errStr, "is not present in the table")
}

// isConnectionError checks if the error indicates a connection problem
func isConnectionError(err error) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()

	return strings.Contains(errStr, "driver: bad connection") ||
		strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "broken pipe")
}
