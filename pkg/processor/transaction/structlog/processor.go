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

// Dependencies contains the dependencies needed for the processor.
type Dependencies struct {
	Log         logrus.FieldLogger
	Pool        *ethereum.Pool
	Network     *ethereum.Network
	State       *state.Manager
	AsynqClient *asynq.Client
	RedisPrefix string
}

// Processor handles transaction structlog processing.
type Processor struct {
	log            logrus.FieldLogger
	pool           *ethereum.Pool
	stateManager   *state.Manager
	clickhouse     clickhouse.ClientInterface
	config         *Config
	network        *ethereum.Network
	asynqClient    *asynq.Client
	processingMode string
	redisPrefix    string
	bigTxManager   *BigTransactionManager
	batchManager   *BatchManager
}

// New creates a new transaction structlog processor.
func New(ctx context.Context, deps *Dependencies, config *Config) (*Processor, error) {
	// Create a copy of the embedded config and set processor-specific values
	clickhouseConfig := config.Config
	clickhouseConfig.Network = deps.Network.Name
	clickhouseConfig.Processor = ProcessorName

	clickhouseClient, err := clickhouse.New(&clickhouseConfig)
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
		"network": processor.network.Name,
	}).Info("Detected network")

	return processor, nil
}

// Start starts the processor.
func (p *Processor) Start(ctx context.Context) error {
	// Use configured value or default
	threshold := p.config.BigTransactionThreshold
	if threshold == 0 {
		threshold = 500_000 // Default
	}

	p.bigTxManager = NewBigTransactionManager(threshold, p.log)

	p.log.WithFields(logrus.Fields{
		"big_transaction_threshold": threshold,
	}).Info("Initialized big transaction manager")

	// Create and start batch manager
	p.batchManager = NewBatchManager(p, p.config)
	if err := p.batchManager.Start(); err != nil {
		return fmt.Errorf("failed to start batch manager: %w", err)
	}

	// Start the ClickHouse client
	if err := p.clickhouse.Start(); err != nil {
		return fmt.Errorf("failed to start ClickHouse client: %w", err)
	}

	p.log.Info("Transaction structlog processor ready")

	return nil
}

// Stop stops the processor.
func (p *Processor) Stop(ctx context.Context) error {
	p.log.Info("Stopping transaction structlog processor")

	// Stop the batch manager first to flush any pending batches
	if p.batchManager != nil {
		p.batchManager.Stop()
	}

	// Stop the ClickHouse client
	return p.clickhouse.Stop()
}

// Name returns the processor name.
func (p *Processor) Name() string {
	return ProcessorName
}

// GetQueues returns the queues used by this processor.
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
	}
}

// GetHandlers returns the task handlers for this processor.
func (p *Processor) GetHandlers() map[string]asynq.HandlerFunc {
	return map[string]asynq.HandlerFunc{
		ProcessForwardsTaskType:  p.handleProcessForwardsTask,
		ProcessBackwardsTaskType: p.handleProcessBackwardsTask,
	}
}

// EnqueueTask enqueues a task to the specified queue.
func (p *Processor) EnqueueTask(ctx context.Context, task *asynq.Task, opts ...asynq.Option) error {
	_, err := p.asynqClient.EnqueueContext(ctx, task, opts...)

	return err
}

// SetProcessingMode sets the processing mode for the processor.
func (p *Processor) SetProcessingMode(mode string) {
	p.processingMode = mode
	p.log.WithField("mode", mode).Info("Processing mode updated")
}

// getProcessForwardsQueue returns the prefixed process forwards queue name.
func (p *Processor) getProcessForwardsQueue() string {
	return c.PrefixedProcessForwardsQueue(ProcessorName, p.redisPrefix)
}

// getProcessBackwardsQueue returns the prefixed process backwards queue name.
func (p *Processor) getProcessBackwardsQueue() string {
	return c.PrefixedProcessBackwardsQueue(ProcessorName, p.redisPrefix)
}

// insertStructlogs inserts structlog rows directly into ClickHouse.
func (p *Processor) insertStructlogs(ctx context.Context, structlogs []Structlog) error {
	// Short-circuit for empty structlog arrays
	if len(structlogs) == 0 {
		return nil
	}

	// Create timeout context for insert
	timeout := 5 * time.Minute
	insertCtx, cancel := context.WithTimeout(ctx, timeout)

	defer cancel()

	// Perform the insert with metrics tracking
	start := time.Now()
	err := p.clickhouse.BulkInsert(insertCtx, p.config.Table, structlogs)
	duration := time.Since(start)

	if err != nil {
		code := parseErrorCode(err)
		common.ClickHouseOperationDuration.WithLabelValues(p.network.Name, ProcessorName, "bulk_insert", p.config.Table, "failed", code).Observe(duration.Seconds())
		common.ClickHouseOperationTotal.WithLabelValues(p.network.Name, ProcessorName, "bulk_insert", p.config.Table, "failed", code).Inc()
		common.ClickHouseInsertsRows.WithLabelValues(p.network.Name, ProcessorName, p.config.Table, "failed", code).Add(float64(len(structlogs)))

		p.log.WithFields(logrus.Fields{
			"table":           p.config.Table,
			"error":           err.Error(),
			"structlog_count": len(structlogs),
		}).Error("Failed to insert structlogs")

		return fmt.Errorf("failed to insert %d structlogs: %w", len(structlogs), err)
	}

	common.ClickHouseOperationDuration.WithLabelValues(p.network.Name, ProcessorName, "bulk_insert", p.config.Table, "success", "").Observe(duration.Seconds())
	common.ClickHouseOperationTotal.WithLabelValues(p.network.Name, ProcessorName, "bulk_insert", p.config.Table, "success", "").Inc()
	common.ClickHouseInsertsRows.WithLabelValues(p.network.Name, ProcessorName, p.config.Table, "success", "").Add(float64(len(structlogs)))

	return nil
}

// waitForBigTransactions waits for big transactions to complete before proceeding.
func (p *Processor) waitForBigTransactions(taskType string) {
	if p.bigTxManager.ShouldPauseNewWork() {
		p.log.WithFields(logrus.Fields{
			"task_type":      taskType,
			"active_big_txs": p.bigTxManager.currentBigCount.Load(),
		}).Debug("Task waiting for big transactions to complete")

		startWait := time.Now()

		for p.bigTxManager.ShouldPauseNewWork() {
			time.Sleep(100 * time.Millisecond)
		}

		waitDuration := time.Since(startWait)
		p.log.WithFields(logrus.Fields{
			"task_type":     taskType,
			"wait_duration": waitDuration.String(),
		}).Debug("Task resumed after big transactions completed")
	}
}

// parseErrorCode extracts error code from error message.
func parseErrorCode(err error) string {
	if err == nil {
		return ""
	}
	// For HTTP client errors, we don't have specific error codes
	// Return a generic code based on error type
	errStr := err.Error()

	if strings.Contains(errStr, "timeout") {
		return "TIMEOUT"
	}

	if strings.Contains(errStr, "connection") {
		return "CONNECTION"
	}

	return "UNKNOWN"
}

// ShouldBatch determines if a transaction should be batched based on structlog count.
func (p *Processor) ShouldBatch(structlogCount int64) bool {
	return structlogCount > 0 && structlogCount < p.config.BatchInsertThreshold
}
