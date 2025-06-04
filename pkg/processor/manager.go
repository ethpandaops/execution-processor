package processor

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/ethpandaops/execution-processor/pkg/ethereum"
	"github.com/ethpandaops/execution-processor/pkg/leaderelection"
	c "github.com/ethpandaops/execution-processor/pkg/processor/common"
	transaction_structlog "github.com/ethpandaops/execution-processor/pkg/processor/transaction/structlog"
	s "github.com/ethpandaops/execution-processor/pkg/state"
	"github.com/hibiken/asynq"
	r "github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// Manager coordinates multiple processors with distributed task processing
type Manager struct {
	log        logrus.FieldLogger
	config     *Config
	pool       *ethereum.Pool
	state      *s.Manager
	processors map[string]c.BlockProcessor

	// Redis/Asynq for distributed processing
	redisClient *r.Client
	redisPrefix string
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

	// Track queue high water marks
	queueHighWaterMarks map[string]int
	queueMetricsMutex   sync.RWMutex
}

func NewManager(log logrus.FieldLogger, config *Config, pool *ethereum.Pool, state *s.Manager, redis *r.Client, redisPrefix string) (*Manager, error) {
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
		log:                 log.WithField("component", "processor"),
		config:              config,
		pool:                pool,
		state:               state,
		processors:          make(map[string]c.BlockProcessor),
		redisClient:         redis,
		redisPrefix:         redisPrefix,
		asynqClient:         asynqClient,
		asynqServer:         asynqServer,
		leadershipChange:    make(chan bool, 1),
		stopChan:            make(chan struct{}),
		blockProcessStop:    make(chan struct{}),
		queueHighWaterMarks: make(map[string]int),
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

	// Set network on state manager for metrics
	m.state.SetNetwork(m.network.Name)

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
		leaderKey := fmt.Sprintf("leader:%s:%s", m.network.Name, m.config.Mode)
		if m.redisPrefix != "" {
			leaderKey = fmt.Sprintf("%s:%s", m.redisPrefix, leaderKey)
		}

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
			RedisPrefix: m.redisPrefix,
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

	// Check if we should skip due to queue backpressure
	if shouldSkip, reason := m.shouldSkipBlockProcessing(ctx); shouldSkip {
		m.log.WithFields(logrus.Fields{
			"reason":         reason,
			"max_queue_size": m.config.MaxProcessQueueSize,
		}).Warn("Skipping block processing due to queue backpressure")

		return
	}

	// Get execution node head for head distance calculation
	var executionHead *big.Int

	node := m.pool.GetHealthyExecutionNode()
	if node != nil {
		if latestBlockNum, err := node.BlockNumber(ctx); err == nil && latestBlockNum != nil {
			executionHead = new(big.Int).SetUint64(*latestBlockNum)
		}
	}

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

		// Update head distance metric (regardless of success/failure to track current distance)
		m.updateHeadDistanceMetric(ctx, name, executionHead)
	}
}

// updateHeadDistanceMetric calculates and updates the head distance metric for a processor
func (m *Manager) updateHeadDistanceMetric(ctx context.Context, processorName string, executionHead *big.Int) {
	distance, headType, err := m.state.GetHeadDistance(ctx, processorName, m.network.Name, m.config.Mode, executionHead)
	if err != nil {
		m.log.WithError(err).WithFields(logrus.Fields{
			"processor": processorName,
			"network":   m.network.Name,
		}).Debug("Failed to calculate head distance")

		// Set metric to -1 to indicate calculation error
		common.HeadDistance.WithLabelValues(m.network.Name, processorName, "error").Set(-1)

		return
	}

	// Update the metric
	common.HeadDistance.WithLabelValues(m.network.Name, processorName, headType).Set(float64(distance))

	m.log.WithFields(logrus.Fields{
		"processor": processorName,
		"network":   m.network.Name,
		"distance":  distance,
		"head_type": headType,
	}).Debug("Updated head distance metric")
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
		if recovered := recover(); recovered != nil {
			m.log.WithField("panic", recovered).Error("Block processing panic recovered")
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

			// Only monitor queues that match the current processing mode
			shouldMonitor := false

			switch m.config.Mode {
			case c.FORWARDS_MODE:
				shouldMonitor = strings.Contains(queue.Name, "forwards")
			case c.BACKWARDS_MODE:
				shouldMonitor = strings.Contains(queue.Name, "backwards")
			}

			if !shouldMonitor {
				m.log.WithFields(logrus.Fields{
					"queue": queue.Name,
					"mode":  m.config.Mode,
				}).Debug("Skipped queue monitoring - does not match processing mode")

				continue
			}

			// Queue name is already prefixed from GetQueues()
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

			// Track high water marks
			m.queueMetricsMutex.Lock()
			if info.Size > m.queueHighWaterMarks[queue.Name] {
				m.queueHighWaterMarks[queue.Name] = info.Size
				common.QueueHighWaterMark.WithLabelValues(
					m.network.Name, name, queue.Name,
				).Set(float64(info.Size))
			}
			m.queueMetricsMutex.Unlock()

			// Log warning if archived items exceed threshold
			if info.Archived > 100 {
				m.log.WithFields(logrus.Fields{
					"processor": name,
					"queue":     queue.Name,
					"archived":  info.Archived,
					"pending":   info.Pending,
					"active":    info.Active,
					"size":      info.Size,
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
			if recovered := recover(); recovered != nil {
				m.log.WithField("panic", recovered).Error("Queue monitoring panic recovered")
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
			// Only register queues that match the current processing mode
			shouldInclude := false

			switch m.config.Mode {
			case c.FORWARDS_MODE:
				shouldInclude = strings.Contains(queue.Name, "forwards")
			case c.BACKWARDS_MODE:
				shouldInclude = strings.Contains(queue.Name, "backwards")
			}

			if shouldInclude {
				// Queue names are already prefixed by GetQueues()
				queues[queue.Name] = queue.Priority
				m.log.WithFields(logrus.Fields{
					"queue":    queue.Name,
					"priority": queue.Priority,
					"mode":     m.config.Mode,
				}).Debug("Added queue to asynq server")
			} else {
				m.log.WithFields(logrus.Fields{
					"queue": queue.Name,
					"mode":  m.config.Mode,
				}).Debug("Skipped queue - does not match processing mode")
			}
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

// shouldSkipBlockProcessing checks if block processing should be skipped due to queue backpressure
func (m *Manager) shouldSkipBlockProcessing(ctx context.Context) (bool, string) {
	// Get Redis options for Asynq Inspector
	redisOpt := m.redisClient.Options()
	asynqRedisOpt := asynq.RedisClientOpt{
		Addr:     redisOpt.Addr,
		Password: redisOpt.Password,
		DB:       redisOpt.DB,
	}

	inspector := asynq.NewInspector(asynqRedisOpt)
	defer func() {
		if err := inspector.Close(); err != nil {
			m.log.WithError(err).Error("Failed to close asynq inspector")
		}
	}()

	skipReasons := []string{}
	shouldSkip := false
	anyBackpressure := false

	for name := range m.processors {
		// Check all queues based on mode
		var queuesToCheck []string
		if m.config.Mode == c.FORWARDS_MODE {
			queuesToCheck = []string{
				c.PrefixedProcessForwardsQueue(name, m.redisPrefix),
				c.PrefixedVerifyForwardsQueue(name, m.redisPrefix),
			}
		} else {
			queuesToCheck = []string{
				c.PrefixedProcessBackwardsQueue(name, m.redisPrefix),
				c.PrefixedVerifyBackwardsQueue(name, m.redisPrefix),
			}
		}

		for _, queueName := range queuesToCheck {
			info, err := inspector.GetQueueInfo(queueName)
			if err != nil {
				m.log.WithError(err).WithFields(logrus.Fields{
					"processor": name,
					"queue":     queueName,
				}).Warn("Failed to get queue info for backpressure check")

				continue
			}

			// Update metrics
			common.QueueDepth.WithLabelValues(m.network.Name, name, queueName).Set(float64(info.Size))

			// Check threshold
			if info.Size > m.config.MaxProcessQueueSize {
				shouldSkip = true

				anyBackpressure = true

				skipReasons = append(skipReasons,
					fmt.Sprintf("%s: %d/%d", queueName, info.Size, m.config.MaxProcessQueueSize))

				// Set backpressure metric
				common.QueueBackpressureActive.WithLabelValues(
					m.network.Name, name,
				).Set(1)

				m.log.WithFields(logrus.Fields{
					"processor":  name,
					"queue":      queueName,
					"queue_size": info.Size,
					"max_size":   m.config.MaxProcessQueueSize,
					"pending":    info.Pending,
					"active":     info.Active,
				}).Warn("Queue backpressure active - will skip block processing")
			} else if info.Size < int(float64(m.config.MaxProcessQueueSize)*m.config.BackpressureHysteresis) {
				// Clear backpressure if below hysteresis threshold
				common.QueueBackpressureActive.WithLabelValues(
					m.network.Name, name,
				).Set(0)
			}
		}
	}

	reason := strings.Join(skipReasons, ", ")

	if shouldSkip {
		common.BlockProcessingSkipped.WithLabelValues(
			m.network.Name, "all", "queue_backpressure",
		).Inc()

		m.log.WithFields(logrus.Fields{
			"reasons":        reason,
			"backpressure":   anyBackpressure,
			"max_queue_size": m.config.MaxProcessQueueSize,
		}).Info("Skipping block processing due to queue backpressure")
	}

	return shouldSkip, reason
}
