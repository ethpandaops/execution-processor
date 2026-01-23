package simple

import (
	"context"
	"fmt"
	"math"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
	"github.com/ethpandaops/execution-processor/pkg/ethereum"
	"github.com/ethpandaops/execution-processor/pkg/processor/tracker"
	"github.com/ethpandaops/execution-processor/pkg/state"
)

// ProcessorName is the name of the simple transaction processor.
const ProcessorName = "transaction_simple"

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

// Processor handles simple transaction processing.
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

// New creates a new simple transaction processor.
func New(deps *Dependencies, config *Config) (*Processor, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	// Create a copy of the embedded config and set processor-specific values
	clickhouseConfig := config.Config
	clickhouseConfig.Network = deps.Network.Name
	clickhouseConfig.Processor = ProcessorName

	clickhouseClient, err := clickhouse.New(&clickhouseConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create clickhouse client: %w", err)
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

	return &Processor{
		log:            log,
		pool:           deps.Pool,
		stateManager:   deps.State,
		clickhouse:     clickhouseClient,
		config:         config,
		network:        deps.Network,
		asynqClient:    deps.AsynqClient,
		processingMode: tracker.FORWARDS_MODE, // Default mode
		redisPrefix:    deps.RedisPrefix,
		pendingTracker: pendingTracker,
		Limiter:        limiter,
	}, nil
}

// Name returns the processor name.
func (p *Processor) Name() string {
	return ProcessorName
}

// Start starts the processor.
func (p *Processor) Start(ctx context.Context) error {
	p.log.Info("Starting transaction simple processor")

	if err := p.clickhouse.Start(); err != nil {
		return fmt.Errorf("failed to start ClickHouse client: %w", err)
	}

	p.log.WithFields(logrus.Fields{
		"network": p.network.Name,
	}).Info("Transaction simple processor ready")

	return nil
}

// Stop stops the processor.
func (p *Processor) Stop(ctx context.Context) error {
	p.log.Info("Stopping transaction simple processor")

	return p.clickhouse.Stop()
}

// SetProcessingMode sets the processing mode for the processor.
func (p *Processor) SetProcessingMode(mode string) {
	p.processingMode = mode
	p.log.WithField("mode", mode).Info("Processing mode updated")
}

// EnqueueTask enqueues a task to the specified queue with infinite retries.
func (p *Processor) EnqueueTask(ctx context.Context, task *asynq.Task, opts ...asynq.Option) error {
	opts = append(opts, asynq.MaxRetry(math.MaxInt32))

	_, err := p.asynqClient.EnqueueContext(ctx, task, opts...)

	return err
}

// GetQueues returns the queues used by this processor.
func (p *Processor) GetQueues() []tracker.QueueInfo {
	return []tracker.QueueInfo{
		{
			Name:     tracker.PrefixedProcessForwardsQueue(ProcessorName, p.redisPrefix),
			Priority: 10,
		},
		{
			Name:     tracker.PrefixedProcessBackwardsQueue(ProcessorName, p.redisPrefix),
			Priority: 5,
		},
	}
}

// getProcessForwardsQueue returns the prefixed process forwards queue name.
func (p *Processor) getProcessForwardsQueue() string {
	return tracker.PrefixedProcessForwardsQueue(ProcessorName, p.redisPrefix)
}

// getProcessBackwardsQueue returns the prefixed process backwards queue name.
func (p *Processor) getProcessBackwardsQueue() string {
	return tracker.PrefixedProcessBackwardsQueue(ProcessorName, p.redisPrefix)
}
