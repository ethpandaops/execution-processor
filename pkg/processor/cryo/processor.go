package cryo

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

// Compile-time interface compliance check.
var _ tracker.BlockProcessor = (*Processor)(nil)

// Dependencies contains the dependencies needed for the processor.
type Dependencies struct {
	Log            logrus.FieldLogger
	Pool           *ethereum.Pool
	Network        *ethereum.Network
	State          *state.Manager
	AsynqClient    *asynq.Client
	AsynqInspector *asynq.Inspector
	RedisClient    *redis.Client
	RedisPrefix    string
}

// Processor collects one cryo group per block and writes each dataset it
// produces to its own table.
type Processor struct {
	log            logrus.FieldLogger
	name           string
	group          *Group
	pool           *ethereum.Pool
	stateManager   *state.Manager
	clickhouse     clickhouse.ClientInterface
	config         *Config
	network        *ethereum.Network
	asynqClient    *asynq.Client
	asynqInspector *asynq.Inspector
	processingMode string
	redisPrefix    string

	fetcher Fetcher

	// One sink per dataset: a single invocation writes several tables whose
	// volumes differ by orders of magnitude, so they flush independently.
	sinks []sink

	*tracker.Limiter

	completionTracker *tracker.BlockCompletionTracker
}

// New creates a processor for one enabled group.
func New(deps *Dependencies, config *Config, groupCfg *GroupConfig) (*Processor, error) {
	group, err := newGroup(groupCfg)
	if err != nil {
		return nil, fmt.Errorf("invalid group %q: %w", groupCfg.Name, err)
	}

	name := group.ProcessorName()

	clickhouseConfig := config.Config
	clickhouseConfig.Network = deps.Network.Name
	clickhouseConfig.Processor = name

	clickhouseClient, err := clickhouse.New(&clickhouseConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create clickhouse client: %w", err)
	}

	maxPending := groupCfg.MaxPendingBlockRange
	if maxPending <= 0 {
		maxPending = tracker.DefaultMaxPendingBlockRange
	}

	log := deps.Log.WithField("processor", name)

	limiter := tracker.NewLimiter(
		&tracker.LimiterDeps{
			Log:           log,
			StateProvider: deps.State,
			Network:       deps.Network.Name,
			Processor:     name,
		},
		tracker.LimiterConfig{
			MaxPendingBlockRange: maxPending,
		},
	)

	completionTracker := tracker.NewBlockCompletionTracker(
		deps.RedisClient,
		deps.RedisPrefix,
		log,
		deps.State,
		tracker.BlockCompletionTrackerConfig{
			StaleThreshold: tracker.DefaultStaleThreshold,
			AutoRetryStale: true,
		},
	)

	p := &Processor{
		log:               log,
		name:              name,
		group:             group,
		pool:              deps.Pool,
		stateManager:      deps.State,
		clickhouse:        clickhouseClient,
		config:            config,
		network:           deps.Network,
		asynqClient:       deps.AsynqClient,
		asynqInspector:    deps.AsynqInspector,
		processingMode:    tracker.FORWARDS_MODE,
		redisPrefix:       deps.RedisPrefix,
		Limiter:           limiter,
		completionTracker: completionTracker,
	}

	p.fetcher = newExecFetcher(log, config, p.rpcEndpoint)

	p.sinks = make([]sink, 0, len(group.Datasets))
	for _, ds := range group.Datasets {
		p.sinks = append(p.sinks, ds.newSink(sinkDeps{
			log:                 log,
			clickhouse:          clickhouseClient,
			dataset:             ds,
			table:               group.TableFor(ds),
			network:             deps.Network.Name,
			processor:           name,
			bufferMaxRows:       config.BufferMaxRows,
			bufferFlushInterval: config.BufferFlushInterval,
		}))
	}

	log.WithFields(logrus.Fields{
		"network":                 deps.Network.Name,
		"datatypes":               group.Datatypes,
		"datasets":                len(group.Datasets),
		"min_block":               group.MinBlock,
		"max_pending_block_range": maxPending,
	}).Info("Cryo processor initialized")

	return p, nil
}

// Name returns the processor name, which is the checkpoint key in the block
// ledger and must stay stable for the lifetime of the data.
func (p *Processor) Name() string { return p.name }

// Group returns the group this processor collects.
func (p *Processor) Group() *Group { return p.group }

// rpcEndpoint picks a healthy node for cryo to dial. Nodes that expose no
// endpoint cannot drive a subprocess, so they are not candidates.
func (p *Processor) rpcEndpoint() (string, error) {
	for _, node := range p.pool.GetHealthyExecutionNodes() {
		if endpoint := node.RPCEndpoint(); endpoint != "" {
			return endpoint, nil
		}
	}

	return "", fmt.Errorf("no healthy execution node exposes an RPC endpoint for cryo")
}

// Start starts the processor, verifying the cryo binary and every target table
// before accepting work.
func (p *Processor) Start(ctx context.Context) error {
	p.log.Info("Starting cryo processor")

	sweepTempDirs(p.log, p.config.TempDir)

	if err := p.clickhouse.Start(); err != nil {
		return fmt.Errorf("failed to start ClickHouse client: %w", err)
	}

	version, err := p.cryoVersion(ctx)
	if err != nil {
		return err
	}

	for _, s := range p.sinks {
		if err := s.ValidateSchema(ctx); err != nil {
			return fmt.Errorf("schema check failed: %w", err)
		}

		if err := s.Start(ctx); err != nil {
			return fmt.Errorf("failed to start %s row buffer: %w", s.Dataset(), err)
		}
	}

	p.log.WithFields(logrus.Fields{
		"network":      p.network.Name,
		"cryo_version": version,
	}).Info("Cryo processor ready")

	return nil
}

// cryoVersion resolves the bundled cryo's version, failing startup if the
// binary is missing or unrunnable rather than at the first task.
func (p *Processor) cryoVersion(ctx context.Context) (string, error) {
	versioner, ok := p.fetcher.(interface {
		Version(context.Context) (string, error)
	})
	if !ok {
		return "", nil
	}

	version, err := versioner.Version(ctx)
	if err != nil {
		return "", fmt.Errorf("cryo binary %q is not runnable: %w", p.config.BinaryPath, err)
	}

	CryoBuildInfo.WithLabelValues(p.network.Name, version).Set(1)

	return version, nil
}

// Stop stops the processor, flushing every dataset's buffered rows.
func (p *Processor) Stop(ctx context.Context) error {
	p.log.Info("Stopping cryo processor")

	for _, s := range p.sinks {
		if err := s.Stop(ctx); err != nil {
			p.log.WithError(err).WithField("dataset", s.Dataset()).Error("Failed to stop row buffer")
		}
	}

	return p.clickhouse.Stop()
}

// SetProcessingMode sets the processing mode for the processor.
func (p *Processor) SetProcessingMode(mode string) {
	p.processingMode = mode
	p.log.WithField("mode", mode).Info("Processing mode updated")
}

// GetCompletionTracker returns the block completion tracker.
func (p *Processor) GetCompletionTracker() *tracker.BlockCompletionTracker {
	return p.completionTracker
}

// GetLimiter returns the block completion limiter.
func (p *Processor) GetLimiter() *tracker.Limiter {
	return p.Limiter
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
		{Name: p.getProcessReprocessForwardsQueue(), Priority: 20},
		{Name: p.getProcessReprocessBackwardsQueue(), Priority: 15},
		{Name: p.getProcessForwardsQueue(), Priority: 10},
		{Name: p.getProcessBackwardsQueue(), Priority: 5},
	}
}

func (p *Processor) getProcessForwardsQueue() string {
	return tracker.PrefixedProcessForwardsQueue(p.name, p.redisPrefix)
}

func (p *Processor) getProcessBackwardsQueue() string {
	return tracker.PrefixedProcessBackwardsQueue(p.name, p.redisPrefix)
}

func (p *Processor) getProcessReprocessForwardsQueue() string {
	return tracker.PrefixedProcessReprocessForwardsQueue(p.name, p.redisPrefix)
}

func (p *Processor) getProcessReprocessBackwardsQueue() string {
	return tracker.PrefixedProcessReprocessBackwardsQueue(p.name, p.redisPrefix)
}

// queueFor returns the main queue for a processing mode.
func (p *Processor) queueFor(mode string) string {
	if mode == tracker.BACKWARDS_MODE {
		return p.getProcessBackwardsQueue()
	}

	return p.getProcessForwardsQueue()
}

// reprocessQueueFor returns the high-priority queue for a processing mode.
func (p *Processor) reprocessQueueFor(mode string) string {
	if mode == tracker.BACKWARDS_MODE {
		return p.getProcessReprocessBackwardsQueue()
	}

	return p.getProcessReprocessForwardsQueue()
}
