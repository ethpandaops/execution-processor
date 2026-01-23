package structlog

import (
	"context"
	"fmt"
	"math"

	"github.com/ClickHouse/ch-go"
	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
	"github.com/ethpandaops/execution-processor/pkg/ethereum"
	"github.com/ethpandaops/execution-processor/pkg/processor/tracker"
	"github.com/ethpandaops/execution-processor/pkg/state"
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

	// Embedded limiter for shared blocking/completion logic
	*tracker.Limiter
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

	processor.log.WithFields(logrus.Fields{
		"network":                 processor.network.Name,
		"max_pending_block_range": config.MaxPendingBlockRange,
	}).Info("Detected network")

	return processor, nil
}

// Start starts the processor.
func (p *Processor) Start(ctx context.Context) error {
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

// insertStructlogs inserts structlogs into ClickHouse using columnar protocol.
func (p *Processor) insertStructlogs(ctx context.Context, structlogs []Structlog) error {
	if len(structlogs) == 0 {
		return nil
	}

	// Add timeout for ClickHouse operation
	insertCtx, cancel := context.WithTimeout(ctx, tracker.DefaultClickHouseTimeout)
	defer cancel()

	cols := NewColumns()
	for _, sl := range structlogs {
		cols.Append(
			sl.UpdatedDateTime.Time(),
			sl.BlockNumber,
			sl.TransactionHash,
			sl.TransactionIndex,
			sl.TransactionGas,
			sl.TransactionFailed,
			sl.TransactionReturnValue,
			sl.Index,
			sl.ProgramCounter,
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
		return fmt.Errorf("failed to insert structlogs: %w", err)
	}

	return nil
}
