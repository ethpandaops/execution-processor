package simple

import (
	"context"
	"fmt"

	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
	"github.com/ethpandaops/execution-processor/pkg/ethereum"
	c "github.com/ethpandaops/execution-processor/pkg/processor/common"
	"github.com/ethpandaops/execution-processor/pkg/state"
)

// ProcessorName is the name of the simple transaction processor
const ProcessorName = "transaction_simple"

// Dependencies contains the dependencies needed for the processor
type Dependencies struct {
	Log         logrus.FieldLogger
	Pool        *ethereum.Pool
	Network     *ethereum.Network
	State       *state.Manager
	AsynqClient *asynq.Client
	RedisPrefix string
}

// Processor handles simple transaction processing
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

// New creates a new simple transaction processor
func New(ctx context.Context, deps *Dependencies, config *Config) (*Processor, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	clickhouseClient, err := clickhouse.NewClient(
		ctx,
		deps.Log.WithField("processor", ProcessorName),
		&clickhouse.Config{
			DSN:          config.DSN,
			MaxOpenConns: config.MaxOpenConns,
			MaxIdleConns: config.MaxIdleConns,
			Network:      deps.Network.Name,
			Processor:    ProcessorName,
		},
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create clickhouse client: %w", err)
	}

	return &Processor{
		log:            deps.Log.WithField("processor", ProcessorName),
		pool:           deps.Pool,
		stateManager:   deps.State,
		clickhouse:     clickhouseClient,
		config:         config,
		network:        deps.Network,
		asynqClient:    deps.AsynqClient,
		processingMode: c.FORWARDS_MODE, // Default mode
		redisPrefix:    deps.RedisPrefix,
	}, nil
}

// Name returns the processor name
func (p *Processor) Name() string {
	return ProcessorName
}

// Start starts the processor
func (p *Processor) Start(ctx context.Context) error {
	p.log.Info("Starting transaction simple processor")

	if err := p.clickhouse.Start(ctx); err != nil {
		return fmt.Errorf("failed to start ClickHouse client: %w", err)
	}

	p.log.WithFields(logrus.Fields{
		"network":  p.network.Name,
		"chain_id": p.network.ID,
	}).Info("Transaction simple processor ready")

	return nil
}

// Stop stops the processor
func (p *Processor) Stop(ctx context.Context) error {
	p.log.Info("Stopping transaction simple processor")

	return p.clickhouse.Stop(ctx)
}

// SetProcessingMode sets the processing mode for the processor
func (p *Processor) SetProcessingMode(mode string) {
	p.processingMode = mode
	p.log.WithField("mode", mode).Info("Processing mode updated")
}

// EnqueueTask enqueues a task to the specified queue
func (p *Processor) EnqueueTask(ctx context.Context, task *asynq.Task, opts ...asynq.Option) error {
	_, err := p.asynqClient.EnqueueContext(ctx, task, opts...)

	return err
}

// GetQueues returns the queues used by this processor
func (p *Processor) GetQueues() []c.QueueInfo {
	return []c.QueueInfo{
		{
			Name:     c.PrefixedProcessForwardsQueue(ProcessorName, p.redisPrefix),
			Priority: 10,
		},
		{
			Name:     c.PrefixedProcessBackwardsQueue(ProcessorName, p.redisPrefix),
			Priority: 5,
		},
		{
			Name:     c.PrefixedVerifyForwardsQueue(ProcessorName, p.redisPrefix),
			Priority: 1,
		},
		{
			Name:     c.PrefixedVerifyBackwardsQueue(ProcessorName, p.redisPrefix),
			Priority: 1,
		},
	}
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
