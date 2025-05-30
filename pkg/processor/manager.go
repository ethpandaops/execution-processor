package processor

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/ethpandaops/execution-processor/pkg/ethereum"
	"github.com/ethpandaops/execution-processor/pkg/leaderelection"
	c "github.com/ethpandaops/execution-processor/pkg/processor/common"
	transaction_structlog "github.com/ethpandaops/execution-processor/pkg/processor/transaction/structlog"
	"github.com/ethpandaops/execution-processor/pkg/state"
	"github.com/hibiken/asynq"
	r "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

/*
 * Leader election;
 *   - on startup, check if this processor is the leader for the given network via redis
 * If leader;
 *   - for each processor, get the next block number from admin.execution_block filtered by processor name and network
 *     - pass the block number to single processor entry point. Struct logs would request the full block and then enqueue trace_logs task, then finally a verify task.
 *   - must monitor each queue for archived items
 *   - metrics for block height/depth
 * processor;
 *   - init with chain id + network name + ethereum pool + logger + processor config
 *   - must expose it's queues and handler for each queue
 *   - has internal access to be able to enqueue items (only on its own queues?)
 *   - has shared metrics between all processors with labels the only difference
 */

// Manager coordinates multiple processors with distributed task processing
type Manager struct {
	log        logrus.FieldLogger
	config     *Config
	pool       *ethereum.Pool
	state      *state.Manager
	processors map[string]c.BlockProcessor

	// Redis/Asynq for distributed processing
	redisClient *r.Client
	asynqClient *asynq.Client
	asynqServer *asynq.Server

	network *ethereum.Network

	// Leader election
	leaderElector    leaderelection.Elector
	isLeader         bool
	leadershipChange chan bool

	stopChan         chan struct{}
	blockProcessStop chan struct{}

	// Synchronization for goroutine management
	blockProcessMutex sync.RWMutex
	monitorMutex      sync.Mutex
	isMonitoring      bool
	monitorCancel     context.CancelFunc
	stopped           bool
	stopMutex         sync.Mutex

	// WaitGroup for tracking all goroutines
	wg sync.WaitGroup
}

func NewManager(log logrus.FieldLogger, config *Config, pool *ethereum.Pool, state *state.Manager, redis *r.Client) (*Manager, error) {
	// Get Redis options from the existing client
	redisOpt := redis.Options()

	// Create separate Redis clients for Asynq to avoid shutdown issues
	asynqRedisOpt := asynq.RedisClientOpt{
		Addr:     redisOpt.Addr,
		Password: redisOpt.Password,
		DB:       redisOpt.DB,
	}

	// Initialize Asynq client with its own Redis connection
	asynqClient := asynq.NewClient(asynqRedisOpt)

	var asynqServer *asynq.Server
	// Setup queue priorities dynamically based on processors
	// This will be populated after processors are initialized
	queues := make(map[string]int)

	asynqServer = asynq.NewServer(asynqRedisOpt, asynq.Config{
		Concurrency: config.Concurrency,
		Queues:      queues,
		LogLevel:    asynq.InfoLevel,
		Logger:      log,
	})

	return &Manager{
		log:              log.WithField("component", "processor"),
		config:           config,
		pool:             pool,
		state:            state,
		processors:       make(map[string]c.BlockProcessor),
		redisClient:      redis,
		asynqClient:      asynqClient,
		asynqServer:      asynqServer,
		leadershipChange: make(chan bool, 1),
		stopChan:         make(chan struct{}),
		blockProcessStop: make(chan struct{}),
	}, nil
}

func (m *Manager) Start(ctx context.Context) error {
	m.log.Info("Starting processor manager")

	// wait for execution node to be healthy
	node, err := m.pool.WaitForHealthyExecutionNode(ctx)
	if err != nil {
		return fmt.Errorf("failed to wait for healthy execution node: %w", err)
	}

	if node == nil {
		return fmt.Errorf("no healthy execution node available")
	}

	m.network, err = m.pool.GetNetworkByChainID(node.Metadata().ChainID())
	if err != nil {
		return fmt.Errorf("failed to get network by chain ID: %w", err)
	}

	// Initialize processors
	if err := m.initializeProcessors(ctx); err != nil {
		return fmt.Errorf("failed to initialize processors: %w", err)
	}

	// Update asynq server with processor queues
	if err := m.updateAsynqQueues(); err != nil {
		return fmt.Errorf("failed to update asynq queues: %w", err)
	}

	// Initialize leader election if enabled
	if m.config.LeaderElection.Enabled {
		leaderKey := fmt.Sprintf("execution-processor:leader:%s:%s", m.network.Name, m.config.Mode)
		leaderConfig := &leaderelection.Config{
			TTL:             m.config.LeaderElection.TTL,
			RenewalInterval: m.config.LeaderElection.RenewalInterval,
			NodeID:          m.config.LeaderElection.NodeID,
		}

		m.log.WithFields(logrus.Fields{
			"leader_key":       leaderKey,
			"ttl":              leaderConfig.TTL,
			"renewal_interval": leaderConfig.RenewalInterval,
			"network":          m.network.Name,
			"mode":             m.config.Mode,
		}).Debug("Creating leader elector")

		m.leaderElector, err = leaderelection.NewRedisElector(
			m.redisClient,
			m.log,
			leaderKey,
			leaderConfig,
		)
		if err != nil {
			return fmt.Errorf("failed to create leader elector: %w", err)
		}

		// Start leader election
		if err := m.leaderElector.Start(ctx); err != nil {
			return fmt.Errorf("failed to start leader election: %w", err)
		}

		// Monitor leadership changes
		go m.monitorLeadership(ctx)

		m.log.Debug("Leader election started, monitoring for leadership changes")
	} else {
		// If leader election is disabled, always act as leader
		m.isLeader = true

		go m.runBlockProcessing(ctx)

		m.log.Info("Leader election disabled - running as standalone processor")
	}

	// Start Asynq server (worker) - always runs regardless of leadership
	if m.asynqServer != nil {
		mux, err := m.setupWorkerHandlers()
		if err != nil {
			return fmt.Errorf("failed to setup worker handlers: %w", err)
		}

		m.wg.Add(1)

		go func() {
			defer m.wg.Done()

			if err := m.asynqServer.Start(mux); err != nil {
				m.log.WithError(err).Error("Asynq server failed")
			}
		}()
		m.log.Info("Worker started for distributed task processing")
	}

	// Wait for stop signal
	<-m.stopChan

	m.log.Info("Stop signal received")

	return nil
}

func (m *Manager) Stop(ctx context.Context) error {
	m.stopMutex.Lock()
	if m.stopped {
		m.stopMutex.Unlock()

		return nil // Already stopped
	}

	m.stopped = true
	m.stopMutex.Unlock()

	m.log.Info("Stopping processor manager")
	close(m.stopChan)

	// Stop queue monitoring
	m.stopQueueMonitoring()

	// Stop block processing if running
	m.blockProcessMutex.Lock()
	if m.isLeader && m.blockProcessStop != nil {
		select {
		case <-m.blockProcessStop:
			// Already closed
		default:
			close(m.blockProcessStop)
		}

		m.blockProcessStop = nil
	}
	m.blockProcessMutex.Unlock()

	// Stop Asynq server first (before stopping Redis-dependent services)
	if m.asynqServer != nil {
		m.asynqServer.Stop()
		m.asynqServer.Shutdown() // Wait for graceful shutdown
		m.log.Info("Asynq server stopped")
	}

	// Stop leader election
	if m.leaderElector != nil {
		// Create a timeout context for leader election stop
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := m.leaderElector.Stop(stopCtx); err != nil {
			m.log.WithError(err).Error("Failed to stop leader election")
		}
	}

	// Stop all processors
	for name, processor := range m.processors {
		if err := processor.Stop(ctx); err != nil {
			m.log.WithError(err).WithField("processor", name).Error("Failed to stop processor")
		}
	}

	// Close Asynq client
	if m.asynqClient != nil {
		if err := m.asynqClient.Close(); err != nil {
			m.log.WithError(err).Error("Failed to close Asynq client")
		}
	}

	return nil
}

func (m *Manager) initializeProcessors(ctx context.Context) error {
	m.log.Debug("Initializing processors")

	// Initialize transaction structlog processor if enabled
	if m.config.TransactionStructlog.Enabled {
		m.log.Debug("Transaction structlog processor is enabled, initializing...")

		processor, err := transaction_structlog.New(ctx, &transaction_structlog.Dependencies{
			Log:         m.log.WithField("processor", "transaction_structlog"),
			Pool:        m.pool,
			State:       m.state,
			AsynqClient: m.asynqClient,
			Network:     m.network,
		}, &m.config.TransactionStructlog)

		if err != nil {
			return fmt.Errorf("failed to create transaction_structlog processor: %w", err)
		}

		m.processors["transaction_structlog"] = processor

		// Set processing mode from config
		processor.SetProcessingMode(m.config.Mode)

		m.log.WithField("processor", "transaction_structlog").Info("Initialized processor")

		if err := processor.Start(ctx); err != nil {
			return fmt.Errorf("failed to start transaction_structlog processor: %w", err)
		}
	} else {
		m.log.Debug("Transaction structlog processor is disabled")
	}

	m.log.WithField("total_processors", len(m.processors)).Info("Completed processor initialization")

	return nil
}

func (m *Manager) processBlocks(ctx context.Context) {
	m.log.WithField("processor_count", len(m.processors)).Debug("Starting to process blocks")

	for name, processor := range m.processors {
		m.log.WithField("processor", name).Debug("Processing next block for processor")

		startTime := time.Now()

		if err := processor.ProcessNextBlock(ctx); err != nil {
			// Check if this is a "waiting for block" case to reduce log noise
			if isWaitingForBlockError(err) {
				m.log.WithError(err).WithField("processor", name).Debug("Processor waiting for new block")
			} else {
				m.log.WithError(err).WithField("processor", name).Error("Failed to process block")
			}

			common.ProcessorErrors.WithLabelValues(m.network.Name, name, "process_block", "processing").Inc()
		} else {
			// Track processing duration
			duration := time.Since(startTime)

			common.BlockProcessingDuration.WithLabelValues(m.network.Name, name).Observe(duration.Seconds())
			common.BlocksProcessed.WithLabelValues(m.network.Name, name).Inc()

			m.log.WithFields(logrus.Fields{
				"processor": name,
				"duration":  duration,
			}).Debug("Successfully processed block")
		}
	}
}

func (m *Manager) setupWorkerHandlers() (*asynq.ServeMux, error) {
	mux := asynq.NewServeMux()

	// Register handlers from all processors
	for name, processor := range m.processors {
		handlers := processor.GetHandlers()
		for taskType, handler := range handlers {
			mux.HandleFunc(taskType, handler)
			m.log.WithFields(logrus.Fields{
				"processor": name,
				"task_type": taskType,
			}).Info("Registered task handler")
		}
	}

	return mux, nil
}

func (m *Manager) monitorLeadership(ctx context.Context) {
	m.log.Debug("Started monitoring leadership changes")

	leadershipChan := m.leaderElector.LeadershipChannel()

	for {
		select {
		case <-ctx.Done():
			m.log.Debug("Context cancelled in monitorLeadership")

			return
		case isLeader, ok := <-leadershipChan:
			if !ok {
				m.log.Debug("Leadership channel closed")

				return
			}

			m.log.WithField("isLeader", isLeader).Debug("Received leadership change event")

			if isLeader {
				m.handleLeadershipGain(ctx)
			} else {
				m.handleLeadershipLoss()
			}
		}
	}
}

func (m *Manager) handleLeadershipGain(ctx context.Context) {
	m.blockProcessMutex.Lock()
	defer m.blockProcessMutex.Unlock()

	m.isLeader = true
	m.log.Info("Gained leadership - starting block processing")

	// Reset the stop channel for block processing
	m.blockProcessStop = make(chan struct{})

	// Log network info
	if m.network != nil {
		m.log.WithFields(logrus.Fields{
			"network": m.network.Name,
			"chainId": m.network.ID,
		}).Debug("Starting block processing for network")
	}

	// Start block processing loop
	m.log.Debug("Starting runBlockProcessing goroutine")

	go func() {
		m.log.Debug("runBlockProcessing goroutine started")
		m.runBlockProcessing(ctx)
	}()
}

func (m *Manager) handleLeadershipLoss() {
	m.blockProcessMutex.Lock()
	defer m.blockProcessMutex.Unlock()

	m.isLeader = false
	m.log.Info("Lost leadership - stopping block processing")

	// Stop queue monitoring first
	m.stopQueueMonitoring()

	// Stop block processing
	if m.blockProcessStop != nil {
		select {
		case <-m.blockProcessStop:
			// Already closed
		default:
			close(m.blockProcessStop)
		}

		m.blockProcessStop = nil
	}
}

func (m *Manager) runBlockProcessing(ctx context.Context) {
	defer func() {
		// Recovery from panics to prevent goroutine leaks
		if r := recover(); r != nil {
			m.log.WithField("panic", r).Error("Block processing panic recovered")
		}

		m.log.Debug("Block processing goroutine exiting")
	}()

	m.log.Debug("Entered runBlockProcessing function")

	// Check if context is already cancelled
	select {
	case <-ctx.Done():
		m.log.Warn("Context already cancelled when entering runBlockProcessing")

		return
	default:
		m.log.Debug("Context is active, proceeding with block processing")
	}

	blockTicker := time.NewTicker(m.config.Interval)
	queueMonitorTicker := time.NewTicker(30 * time.Second) // Monitor queues every 30s

	defer blockTicker.Stop()
	defer queueMonitorTicker.Stop()

	m.log.WithFields(logrus.Fields{
		"interval":   m.config.Interval,
		"processors": len(m.processors),
	}).Info("Started block processing loop")

	// Check if we have any processors
	if len(m.processors) == 0 {
		m.log.Error("No processors available! Cannot process blocks")

		return
	}

	// Log available processors
	for name := range m.processors {
		m.log.WithField("processor", name).Debug("Processor available for block processing")
	}

	// Start initial queue monitoring
	m.startQueueMonitoring(ctx)

	// Process blocks immediately on becoming leader
	m.log.Debug("Processing initial blocks after leadership gain")
	m.processBlocks(ctx)

	for {
		select {
		case <-ctx.Done():
			m.log.Debug("Context cancelled in block processing loop")

			return
		case <-m.blockProcessStop:
			m.log.Info("Block processing stopped")

			return
		case <-blockTicker.C:
			// Only process if we're still the leader
			if m.isLeader {
				m.log.Debug("Block processing ticker fired")
				m.processBlocks(ctx)
			} else {
				m.log.Warn("No longer leader but block processing still running - stopping")

				return
			}
		case <-queueMonitorTicker.C:
			// Monitor queue health
			m.log.Debug("Queue monitoring ticker fired")
			m.startQueueMonitoring(ctx)
		}
	}
}

// monitorQueues monitors queue health and archived items
func (m *Manager) monitorQueues(ctx context.Context) {
	// Get Redis options for Asynq Inspector
	redisOpt := m.redisClient.Options()
	asynqRedisOpt := asynq.RedisClientOpt{
		Addr:     redisOpt.Addr,
		Password: redisOpt.Password,
		DB:       redisOpt.DB,
	}

	// Get queue info from Asynq Inspector
	inspector := asynq.NewInspector(asynqRedisOpt)
	defer func() {
		if err := inspector.Close(); err != nil {
			m.log.WithError(err).Error("Failed to close asynq inspector")
		}
	}()

	// Monitor each processor's queues
	for name, processor := range m.processors {
		// Check for context cancellation between processors
		select {
		case <-ctx.Done():
			m.log.Debug("Context cancelled during queue monitoring")

			return
		default:
		}

		queues := processor.GetQueues()
		for _, queue := range queues {
			// Check for context cancellation between queues
			select {
			case <-ctx.Done():
				m.log.Debug("Context cancelled during queue monitoring")

				return
			default:
			}
			// Get queue info
			info, err := inspector.GetQueueInfo(queue.Name)
			if err != nil {
				m.log.WithError(err).WithFields(logrus.Fields{
					"processor": name,
					"queue":     queue.Name,
				}).Warn("Failed to get queue info")

				common.ProcessorErrors.WithLabelValues(m.network.Name, name, "queue_monitor", "get_info").Inc()

				continue
			}

			// Update metrics
			common.QueueDepth.WithLabelValues(m.network.Name, name, queue.Name).Set(float64(info.Size))
			common.QueueArchivedItems.WithLabelValues(m.network.Name, name, queue.Name).Set(float64(info.Archived))

			// Log warning if archived items exceed threshold
			if info.Archived > 100 {
				m.log.WithFields(logrus.Fields{
					"processor": name,
					"queue":     queue.Name,
					"archived":  info.Archived,
					"pending":   info.Pending,
					"active":    info.Active,
				}).Warn("High number of archived items in queue")
			}
		}
	}
}

// startQueueMonitoring starts queue monitoring with proper goroutine lifecycle management
func (m *Manager) startQueueMonitoring(ctx context.Context) {
	m.monitorMutex.Lock()
	defer m.monitorMutex.Unlock()

	// If monitoring is already running, don't start another one
	if m.isMonitoring {
		m.log.Debug("Queue monitoring already running, skipping")

		return
	}

	// Create a new context for monitoring that can be cancelled
	monitorCtx, cancel := context.WithCancel(ctx)

	m.monitorCancel = cancel
	m.isMonitoring = true

	go func() {
		defer func() {
			// Recovery from panics to prevent goroutine leaks
			if r := recover(); r != nil {
				m.log.WithField("panic", r).Error("Queue monitoring panic recovered")
			}

			// Clean up monitoring state
			m.monitorMutex.Lock()
			m.isMonitoring = false
			m.monitorCancel = nil
			m.monitorMutex.Unlock()
		}()

		m.monitorQueues(monitorCtx)
	}()
}

// stopQueueMonitoring stops the queue monitoring goroutine
func (m *Manager) stopQueueMonitoring() {
	m.monitorMutex.Lock()
	defer m.monitorMutex.Unlock()

	if m.monitorCancel != nil {
		m.monitorCancel()
		m.monitorCancel = nil
	}

	m.isMonitoring = false
}

// updateAsynqQueues updates the asynq server with processor queues
func (m *Manager) updateAsynqQueues() error {
	// Collect all queues from processors
	queues := make(map[string]int)

	for _, processor := range m.processors {
		for _, queue := range processor.GetQueues() {
			queues[queue.Name] = queue.Priority
			m.log.WithFields(logrus.Fields{
				"queue":    queue.Name,
				"priority": queue.Priority,
			}).Debug("Added queue to asynq server")
		}
	}

	// Recreate asynq server with updated queues
	if m.asynqServer != nil {
		m.asynqServer.Stop()
	}

	// Get Redis options from the existing client
	redisOpt := m.redisClient.Options()
	asynqRedisOpt := asynq.RedisClientOpt{
		Addr:     redisOpt.Addr,
		Password: redisOpt.Password,
		DB:       redisOpt.DB,
	}

	m.asynqServer = asynq.NewServer(asynqRedisOpt, asynq.Config{
		Concurrency: m.config.Concurrency,
		Queues:      queues,
		LogLevel:    asynq.InfoLevel,
		Logger:      m.log,
	})

	return nil
}

// isWaitingForBlockError checks if an error indicates we're waiting for a new block
func isWaitingForBlockError(err error) bool {
	if err == nil {
		return false
	}

	errStr := strings.ToLower(err.Error())

	return strings.Contains(errStr, "not yet available") ||
		strings.Contains(errStr, "waiting for block") ||
		strings.Contains(errStr, "chain tip")
}
