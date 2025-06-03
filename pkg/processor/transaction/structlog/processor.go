package structlog

import (
	"context"
	"fmt"
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

	return processor, nil
}

// Start starts the processor
func (p *Processor) Start(ctx context.Context) error {
	// Start the ClickHouse client
	if err := p.clickhouse.Start(ctx); err != nil {
		return fmt.Errorf("failed to start ClickHouse client: %w", err)
	}

	p.log.Info("Transaction structlog processor ready")

	return nil
}

// Stop stops the processor
func (p *Processor) Stop(ctx context.Context) error {
	p.log.Info("Stopping transaction structlog processor")

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

// BatchInsertStructlogs inserts structlog data in batch to ClickHouse
func (p *Processor) BatchInsertStructlogs(ctx context.Context, blockNumber uint64, transactionHash string, transactionIndex uint32, structlogs []Structlog) error {
	if len(structlogs) == 0 {
		return nil
	}

	// Process in smaller chunks to avoid memory buildup
	chunkSize := p.config.BatchSize

	for i := 0; i < len(structlogs); i += chunkSize {
		end := i + chunkSize
		if end > len(structlogs) {
			end = len(structlogs)
		}

		chunk := structlogs[i:end]
		if err := p.insertStructlogChunk(ctx, blockNumber, transactionHash, transactionIndex, chunk); err != nil {
			return fmt.Errorf("failed to insert chunk %d-%d: %w", i, end-1, err)
		}
	}

	return nil
}

// insertStructlogChunk inserts a small chunk of structlog data with proper resource management
func (p *Processor) insertStructlogChunk(ctx context.Context, blockNumber uint64, transactionHash string, transactionIndex uint32, structlogs []Structlog) error {
	if len(structlogs) == 0 {
		return nil
	}

	// Begin transaction
	tx, err := p.clickhouse.GetDB().BeginTx(ctx, nil)
	if err != nil {
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
		return_data, refund, error, meta_network_id, meta_network_name
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, p.config.Table)

	start := time.Now()

	stmt, err := tx.PrepareContext(ctx, query)

	duration := time.Since(start)

	if err != nil {
		code := clickhouse.ParseErrorCode(err)
		common.ClickHouseOperationDuration.WithLabelValues(p.network.Name, ProcessorName, "prepare_insert", p.config.Table, "failed", code).Observe(duration.Seconds())
		common.ClickHouseOperationTotal.WithLabelValues(p.network.Name, ProcessorName, "prepare_insert", p.config.Table, "failed", code).Inc()

		p.log.WithFields(logrus.Fields{
			"table": p.config.Table,
			"error": err.Error(),
		}).Error("Failed to prepare ClickHouse statement")

		return fmt.Errorf("failed to prepare statement for table %s: %w", p.config.Table, err)
	}

	// Ensure statement is closed to release resources
	defer func() {
		if closeErr := stmt.Close(); closeErr != nil {
			p.log.WithError(closeErr).Warn("Failed to close prepared statement")
		}
	}()

	common.ClickHouseOperationDuration.WithLabelValues(p.network.Name, ProcessorName, "prepare_insert", p.config.Table, "success", "").Observe(duration.Seconds())
	common.ClickHouseOperationTotal.WithLabelValues(p.network.Name, ProcessorName, "prepare_insert", p.config.Table, "success", "").Inc()

	// Insert structlogs in this chunk
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
			row.MetaNetworkID,
			row.MetaNetworkName,
		)

		if err != nil {
			p.log.WithFields(logrus.Fields{
				"table":             p.config.Table,
				"block_number":      blockNumber,
				"transaction_hash":  transactionHash,
				"transaction_index": transactionIndex,
				"structlog_index":   i,
				"error":             err.Error(),
			}).Error("Failed to insert structlog row")

			return fmt.Errorf("failed to insert structlog row %d: %w", i, err)
		}
	}

	start = time.Now()

	err = tx.Commit()

	duration = time.Since(start)

	if err != nil {
		code := clickhouse.ParseErrorCode(err)
		common.ClickHouseOperationDuration.WithLabelValues(p.network.Name, ProcessorName, "insert", p.config.Table, "failed", code).Observe(duration.Seconds())
		common.ClickHouseOperationTotal.WithLabelValues(p.network.Name, ProcessorName, "insert", p.config.Table, "failed", code).Inc()
		common.ClickHouseInsertsRows.WithLabelValues(p.network.Name, ProcessorName, p.config.Table, "failed", code).Add(float64(len(structlogs)))

		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	committed = true

	common.ClickHouseOperationTotal.WithLabelValues(p.network.Name, ProcessorName, "insert", p.config.Table, "success", "").Inc()
	common.ClickHouseInsertsRows.WithLabelValues(p.network.Name, ProcessorName, p.config.Table, "success", "").Add(float64(len(structlogs)))
	common.ClickHouseOperationDuration.WithLabelValues(p.network.Name, ProcessorName, "insert", p.config.Table, "success", "").Observe(duration.Seconds())

	return nil
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
