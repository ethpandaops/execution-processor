package structlog

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"sync"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
	"github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/ethpandaops/execution-processor/pkg/ethereum"
	"github.com/ethpandaops/execution-processor/pkg/processor/tracker"
	"github.com/ethpandaops/execution-processor/pkg/rowbuffer"
	"github.com/ethpandaops/execution-processor/pkg/state"
)

const (
	metricsUpdateInterval = 15 * time.Second
)

// Compile-time interface compliance check.
var _ tracker.BlockProcessor = (*Processor)(nil)

// Dependencies contains the dependencies needed for the processor.
type Dependencies struct {
	Log         logrus.FieldLogger
	Pool        *ethereum.Pool
	Network     *ethereum.Network
	State       *state.Manager
	AsynqClient *asynq.Client
	RedisClient *redis.Client
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
	pendingTracker *tracker.PendingTracker

	// Row buffer for batched ClickHouse inserts
	rowBuffer *rowbuffer.Buffer[Structlog]

	// Embedded limiter for shared blocking/completion logic
	*tracker.Limiter

	// Background metrics worker fields
	metricsStop      chan struct{}
	metricsWg        sync.WaitGroup
	metricsStarted   bool
	metricsStartedMu sync.Mutex
}

// New creates a new transaction structlog processor.
func New(deps *Dependencies, config *Config) (*Processor, error) {
	// Create a copy of the embedded config and set processor-specific values
	clickhouseConfig := config.Config
	clickhouseConfig.Network = deps.Network.Name
	clickhouseConfig.Processor = ProcessorName

	clickhouseClient, err := clickhouse.New(&clickhouseConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create clickhouse client for transaction_structlog: %w", err)
	}

	// Set default for MaxPendingBlockRange
	if config.MaxPendingBlockRange <= 0 {
		config.MaxPendingBlockRange = tracker.DefaultMaxPendingBlockRange
	}

	// Set buffer defaults
	if config.BufferMaxRows <= 0 {
		config.BufferMaxRows = DefaultBufferMaxRows
	}

	if config.BufferFlushInterval <= 0 {
		config.BufferFlushInterval = DefaultBufferFlushInterval
	}

	if config.BufferMaxConcurrentFlushes <= 0 {
		config.BufferMaxConcurrentFlushes = DefaultBufferMaxConcurrentFlushes
	}

	if config.BufferCircuitBreakerMaxFailures <= 0 {
		config.BufferCircuitBreakerMaxFailures = DefaultBufferCircuitBreakerMaxFailures
	}

	if config.BufferCircuitBreakerTimeout <= 0 {
		config.BufferCircuitBreakerTimeout = DefaultBufferCircuitBreakerTimeout
	}

	log := deps.Log.WithField("processor", ProcessorName)
	pendingTracker := tracker.NewPendingTracker(deps.RedisClient, deps.RedisPrefix, log)

	// Create the limiter for shared functionality
	limiter := tracker.NewLimiter(
		&tracker.LimiterDeps{
			Log:            log,
			StateProvider:  deps.State,
			PendingTracker: pendingTracker,
			Network:        deps.Network.Name,
			Processor:      ProcessorName,
		},
		tracker.LimiterConfig{
			MaxPendingBlockRange: config.MaxPendingBlockRange,
		},
	)

	processor := &Processor{
		log:            log,
		pool:           deps.Pool,
		stateManager:   deps.State,
		clickhouse:     clickhouseClient,
		config:         config,
		asynqClient:    deps.AsynqClient,
		processingMode: tracker.FORWARDS_MODE, // Default mode
		redisPrefix:    deps.RedisPrefix,
		pendingTracker: pendingTracker,
		Limiter:        limiter,
	}

	processor.network = deps.Network

	// Create the row buffer with the flush function
	processor.rowBuffer = rowbuffer.New(
		rowbuffer.Config{
			MaxRows:                   config.BufferMaxRows,
			FlushInterval:             config.BufferFlushInterval,
			MaxConcurrentFlushes:      config.BufferMaxConcurrentFlushes,
			CircuitBreakerMaxFailures: config.BufferCircuitBreakerMaxFailures,
			CircuitBreakerTimeout:     config.BufferCircuitBreakerTimeout,
			Network:                   deps.Network.Name,
			Processor:                 ProcessorName,
			Table:                     config.Table,
		},
		processor.flushRows,
		log,
	)

	processor.log.WithFields(logrus.Fields{
		"network":                       processor.network.Name,
		"max_pending_block_range":       config.MaxPendingBlockRange,
		"buffer_max_rows":               config.BufferMaxRows,
		"buffer_flush_interval":         config.BufferFlushInterval,
		"buffer_max_concurrent_flushes": config.BufferMaxConcurrentFlushes,
	}).Info("Detected network")

	return processor, nil
}

// Start starts the processor.
func (p *Processor) Start(ctx context.Context) error {
	// Start the ClickHouse client
	if err := p.clickhouse.Start(); err != nil {
		return fmt.Errorf("failed to start ClickHouse client: %w", err)
	}

	// Start the row buffer
	if err := p.rowBuffer.Start(ctx); err != nil {
		return fmt.Errorf("failed to start row buffer: %w", err)
	}

	// Start the background metrics worker
	p.startMetricsWorker()

	p.log.Info("Transaction structlog processor ready")

	return nil
}

// Stop stops the processor.
func (p *Processor) Stop(ctx context.Context) error {
	p.log.Info("Stopping transaction structlog processor")

	// Stop the background metrics worker
	p.stopMetricsWorker()

	// Stop the row buffer first (flushes remaining rows)
	if err := p.rowBuffer.Stop(ctx); err != nil {
		p.log.WithError(err).Error("Failed to stop row buffer")
	}

	// Stop the ClickHouse client
	return p.clickhouse.Stop()
}

// Name returns the processor name.
func (p *Processor) Name() string {
	return ProcessorName
}

// GetQueues returns the queues used by this processor.
func (p *Processor) GetQueues() []tracker.QueueInfo {
	return []tracker.QueueInfo{
		{
			Name:     tracker.PrefixedProcessForwardsQueue(ProcessorName, p.redisPrefix),
			Priority: 10, // Highest priority for forwards processing
		},
		{
			Name:     tracker.PrefixedProcessBackwardsQueue(ProcessorName, p.redisPrefix),
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

// EnqueueTask enqueues a task to the specified queue with infinite retries.
func (p *Processor) EnqueueTask(ctx context.Context, task *asynq.Task, opts ...asynq.Option) error {
	opts = append(opts, asynq.MaxRetry(math.MaxInt32))

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
	return tracker.PrefixedProcessForwardsQueue(ProcessorName, p.redisPrefix)
}

// getProcessBackwardsQueue returns the prefixed process backwards queue name.
func (p *Processor) getProcessBackwardsQueue() string {
	return tracker.PrefixedProcessBackwardsQueue(ProcessorName, p.redisPrefix)
}

// flushRows is the flush function for the row buffer.
func (p *Processor) flushRows(ctx context.Context, rows []Structlog) error {
	if len(rows) == 0 {
		return nil
	}

	// Add timeout for ClickHouse operation
	insertCtx, cancel := context.WithTimeout(ctx, tracker.DefaultClickHouseTimeout)
	defer cancel()

	cols := NewColumns()

	for _, sl := range rows {
		cols.Append(
			sl.UpdatedDateTime.Time(),
			sl.BlockNumber,
			sl.TransactionHash,
			sl.TransactionIndex,
			sl.TransactionGas,
			sl.TransactionFailed,
			sl.TransactionReturnValue,
			sl.Index,
			sl.Operation,
			sl.Gas,
			sl.GasCost,
			sl.GasUsed,
			sl.GasSelf,
			sl.Depth,
			sl.ReturnData,
			sl.Refund,
			sl.Error,
			sl.CallToAddress,
			sl.CallFrameID,
			sl.CallFramePath,
			sl.MetaNetworkName,
		)
	}

	input := cols.Input()

	if err := p.clickhouse.Do(insertCtx, ch.Query{
		Body:  input.Into(p.config.Table),
		Input: input,
	}); err != nil {
		common.ClickHouseInsertsRows.WithLabelValues(
			p.network.Name, ProcessorName, p.config.Table, "failed", "",
		).Add(float64(len(rows)))

		return fmt.Errorf("failed to insert structlogs: %w", err)
	}

	common.ClickHouseInsertsRows.WithLabelValues(
		p.network.Name, ProcessorName, p.config.Table, "success", "",
	).Add(float64(len(rows)))

	return nil
}

// insertStructlogs submits structlogs to the row buffer for batched insertion.
func (p *Processor) insertStructlogs(ctx context.Context, structlogs []Structlog) error {
	return p.rowBuffer.Submit(ctx, structlogs)
}

// startMetricsWorker starts the background metrics update worker.
func (p *Processor) startMetricsWorker() {
	p.metricsStartedMu.Lock()
	defer p.metricsStartedMu.Unlock()

	if p.metricsStarted {
		return
	}

	p.metricsStarted = true
	p.metricsStop = make(chan struct{})
	p.metricsWg.Add(1)

	go p.runMetricsWorker()
}

// stopMetricsWorker stops the background metrics update worker.
func (p *Processor) stopMetricsWorker() {
	p.metricsStartedMu.Lock()
	defer p.metricsStartedMu.Unlock()

	if !p.metricsStarted {
		return
	}

	close(p.metricsStop)
	p.metricsWg.Wait()
	p.metricsStarted = false
}

// runMetricsWorker runs the background metrics update loop.
func (p *Processor) runMetricsWorker() {
	defer p.metricsWg.Done()

	ticker := time.NewTicker(metricsUpdateInterval)
	defer ticker.Stop()

	// Do initial update
	p.updateMetricsBackground()

	for {
		select {
		case <-p.metricsStop:
			return
		case <-ticker.C:
			p.updateMetricsBackground()
		}
	}
}

// updateMetricsBackground updates expensive metrics in the background.
func (p *Processor) updateMetricsBackground() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Update blocks stored min/max
	minBlock, maxBlock, err := p.stateManager.GetMinMaxStoredBlocks(ctx, p.network.Name, ProcessorName)
	if err != nil {
		p.log.WithError(err).WithField("network", p.network.Name).Debug("failed to get min/max stored blocks")
	} else if minBlock != nil && maxBlock != nil {
		common.BlocksStored.WithLabelValues(p.network.Name, ProcessorName, "min").Set(float64(minBlock.Int64()))
		common.BlocksStored.WithLabelValues(p.network.Name, ProcessorName, "max").Set(float64(maxBlock.Int64()))
	}

	// Update head distance metric
	node := p.pool.GetHealthyExecutionNode()

	if node != nil {
		if latestBlockNum, err := node.BlockNumber(ctx); err == nil && latestBlockNum != nil {
			executionHead := new(big.Int).SetUint64(*latestBlockNum)

			distance, headType, err := p.stateManager.GetHeadDistance(ctx, ProcessorName, p.network.Name, p.processingMode, executionHead)
			if err != nil {
				p.log.WithError(err).Debug("Failed to calculate head distance in background metrics")
				common.HeadDistance.WithLabelValues(p.network.Name, ProcessorName, "error").Set(-1)
			} else {
				common.HeadDistance.WithLabelValues(p.network.Name, ProcessorName, headType).Set(float64(distance))
			}
		}
	}
}
