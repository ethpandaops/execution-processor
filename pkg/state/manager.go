package state

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
	"github.com/ethpandaops/execution-processor/pkg/processor/tracker"
	"github.com/sirupsen/logrus"
)

const (
	limiterCacheRefreshInterval = 6 * time.Second
)

// Sentinel errors.
var (
	ErrNoMoreBlocks = errors.New("no more blocks to process")
)

type Manager struct {
	log            logrus.FieldLogger
	storageClient  clickhouse.ClientInterface
	limiterClient  clickhouse.ClientInterface
	storageTable   string
	limiterTable   string
	limiterEnabled bool
	network        string

	// Limiter cache fields
	limiterCacheMu      sync.RWMutex
	limiterCacheValue   map[string]*big.Int // network -> max block
	limiterCacheStop    chan struct{}
	limiterCacheStarted bool
}

func NewManager(log logrus.FieldLogger, config *Config) (*Manager, error) {
	// Create storage client
	storageConfig := config.Storage.Config
	storageConfig.Processor = "state"

	storageClient, err := clickhouse.New(&storageConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to create storage clickhouse client: %w", err)
	}

	manager := &Manager{
		log:            log.WithField("component", "state"),
		storageClient:  storageClient,
		storageTable:   config.Storage.Table,
		limiterEnabled: config.Limiter.Enabled,
	}

	// Create limiter client if enabled
	if config.Limiter.Enabled {
		limiterConfig := config.Limiter.Config
		limiterConfig.Processor = "state-limiter"

		limiterClient, err := clickhouse.New(&limiterConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create limiter clickhouse client: %w", err)
		}

		manager.limiterClient = limiterClient
		manager.limiterTable = config.Limiter.Table
	}

	return manager, nil
}

// SetNetwork sets the network name for metrics labeling.
func (s *Manager) SetNetwork(network string) {
	s.network = network

	// Update network in ClickHouse clients
	s.storageClient.SetNetwork(network)

	if s.limiterEnabled && s.limiterClient != nil {
		s.limiterClient.SetNetwork(network)
	}
}

func (s *Manager) Start(ctx context.Context) error {
	// Start storage client with infinite retry
	if err := s.startClientWithRetry(ctx, s.storageClient, "storage"); err != nil {
		return fmt.Errorf("failed to start storage client: %w", err)
	}

	// Start limiter client with infinite retry if enabled
	if s.limiterEnabled && s.limiterClient != nil {
		if err := s.startClientWithRetry(ctx, s.limiterClient, "limiter"); err != nil {
			return fmt.Errorf("failed to start limiter client: %w", err)
		}

		// Start the limiter cache refresh goroutine
		s.startLimiterCacheRefresh()
	}

	return nil
}

// startClientWithRetry starts a ClickHouse client with infinite retry and capped exponential backoff.
// This ensures the state manager can wait for ClickHouse to become available at startup.
func (s *Manager) startClientWithRetry(ctx context.Context, client clickhouse.ClientInterface, name string) error {
	const (
		baseDelay = 100 * time.Millisecond
		maxDelay  = 10 * time.Second
	)

	attempt := 0

	for {
		err := client.Start()
		if err == nil {
			if attempt > 0 {
				s.log.WithField("client", name).Info("Successfully connected after retries")
			}

			return nil
		}

		// Calculate delay with exponential backoff, capped at maxDelay
		delay := baseDelay * time.Duration(1<<attempt)
		if delay > maxDelay {
			delay = maxDelay
		}

		s.log.WithFields(logrus.Fields{
			"client":  name,
			"attempt": attempt + 1,
			"delay":   delay,
			"error":   err,
		}).Warn("Failed to start client, retrying...")

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}

		attempt++
	}
}

func (s *Manager) Stop(ctx context.Context) error {
	var err error

	// Stop the limiter cache refresh goroutine
	s.limiterCacheMu.Lock()

	if s.limiterCacheStarted && s.limiterCacheStop != nil {
		close(s.limiterCacheStop)
		s.limiterCacheStarted = false
	}

	s.limiterCacheMu.Unlock()

	if stopErr := s.storageClient.Stop(); stopErr != nil {
		err = fmt.Errorf("failed to stop storage client: %w", stopErr)
	}

	if s.limiterEnabled && s.limiterClient != nil {
		if stopErr := s.limiterClient.Stop(); stopErr != nil {
			if err != nil {
				err = fmt.Errorf("%w; failed to stop limiter client: %w", err, stopErr)
			} else {
				err = fmt.Errorf("failed to stop limiter client: %w", stopErr)
			}
		}
	}

	return err
}

func (s *Manager) NextBlock(ctx context.Context, processor, network, mode string, chainHead *big.Int) (*big.Int, error) {
	// Get progressive next block from storage based on mode
	var progressiveNext *big.Int

	var isEmpty bool

	var err error

	if mode == tracker.BACKWARDS_MODE {
		progressiveNext, err = s.getProgressiveNextBlockBackwards(ctx, processor, network, chainHead)
	} else {
		// Default to forwards mode
		progressiveNext, isEmpty, err = s.getProgressiveNextBlock(ctx, processor, network, chainHead)
	}

	if err != nil {
		return nil, err
	}

	// If limiter is disabled or backwards mode, return progressive next block
	// Limiter only applies to forwards processing to prevent exceeding beacon chain
	if !s.limiterEnabled || mode == tracker.BACKWARDS_MODE {
		return progressiveNext, nil
	}

	// Get maximum allowed block from limiter (forwards mode only)
	maxAllowed, err := s.getLimiterMaxBlock(ctx, network)
	if err != nil {
		s.log.WithFields(logrus.Fields{
			"processor": processor,
			"network":   network,
			"error":     err,
		}).Warn("Failed to get limiter max block, falling back to progressive next block")

		// Intentionally fall back to progressive processing when limiter fails
		// This provides resilience when beacon chain data is unavailable
		return progressiveNext, nil //nolint:nilerr // we want to continue processing even if limiter fails
	}

	// Use isEmpty from getProgressiveNextBlock
	isStorageEmpty := isEmpty

	// If storage is empty, start with max(0, maxAllowed - 1) to allow initial processing
	if isStorageEmpty && maxAllowed.Int64() > 0 {
		startBlockValue := maxAllowed.Int64() - 1
		if startBlockValue < 0 {
			startBlockValue = 0
		}

		startBlock := big.NewInt(startBlockValue)

		s.log.WithFields(logrus.Fields{
			"processor":   processor,
			"network":     network,
			"limiter_max": maxAllowed.String(),
			"start_block": startBlock.String(),
			"chain_head":  chainHead.String(),
		}).Info("Storage table empty with limiter enabled, starting processing from limiter max - 1")

		return startBlock, nil
	}

	// Return minimum of progressive next and limiter max
	if progressiveNext.Cmp(maxAllowed) <= 0 {
		s.log.WithFields(logrus.Fields{
			"processor":   processor,
			"network":     network,
			"progressive": progressiveNext.String(),
			"limiter_max": maxAllowed.String(),
			"next_block":  progressiveNext.String(),
		}).Debug("Progressive next block is within limiter bounds")

		return progressiveNext, nil
	}

	// Check if we've already processed the limiter max block to avoid infinite reprocessing
	// If progressive next is exactly limiter_max + 1, it means we've already processed limiter_max
	if progressiveNext.Cmp(big.NewInt(maxAllowed.Int64()+1)) == 0 {
		s.log.WithFields(logrus.Fields{
			"processor":   processor,
			"network":     network,
			"limiter_max": maxAllowed.String(),
			"progressive": progressiveNext.String(),
		}).Info("Caught up to beacon chain execution payload limit, no new blocks to process")

		return nil, ErrNoMoreBlocks
	}

	// Progressive next is greater than limiter max but not caught up
	// This happens when limiter advances - continue sequential processing to avoid gaps
	s.log.WithFields(logrus.Fields{
		"processor":   processor,
		"network":     network,
		"progressive": progressiveNext.String(),
		"limiter_max": maxAllowed.String(),
		"next_block":  progressiveNext.String(),
	}).Warn("Progressive next block exceeds current limiter max, continuing sequential processing to avoid gaps")

	return progressiveNext, nil
}

func (s *Manager) getProgressiveNextBlock(ctx context.Context, processor, network string, chainHead *big.Int) (*big.Int, bool, error) {
	query := fmt.Sprintf(`
		SELECT block_number
		FROM %s FINAL
		WHERE processor = '%s'
		  AND meta_network_name = '%s'
		ORDER BY block_number DESC
		LIMIT 1
	`, s.storageTable, processor, network)

	s.log.WithFields(logrus.Fields{
		"processor": processor,
		"network":   network,
		"table":     s.storageTable,
	}).Debug("Querying for last processed block")

	blockNumber, err := s.storageClient.QueryUInt64(ctx, query, "block_number")
	if err != nil {
		return nil, false, fmt.Errorf("failed to get next block from %s: %w", s.storageTable, err)
	}

	// Check if we got a result
	if blockNumber == nil {
		// Double-check if this is actually empty or no data
		isEmpty, err := s.storageClient.IsStorageEmpty(ctx, s.storageTable, map[string]interface{}{
			"processor":         processor,
			"meta_network_name": network,
		})
		if err != nil {
			s.log.WithError(err).Warn("Failed to verify if storage is empty, assuming no data")

			isEmpty = true
		}

		if isEmpty {
			// No entries in table - return appropriate starting point
			if chainHead != nil && chainHead.Int64() > 0 {
				s.log.WithFields(logrus.Fields{
					"processor":  processor,
					"network":    network,
					"chain_head": chainHead.String(),
				}).Info("Storage table is empty, will determine starting point based on mode")

				return chainHead, true, nil
			} else {
				s.log.WithFields(logrus.Fields{
					"processor": processor,
					"network":   network,
				}).Info("Storage table is empty and no chain head available, will start from genesis")

				return big.NewInt(0), true, nil
			}
		}

		// Block 0 was actually processed but blockNumber is nil (shouldn't happen but be defensive)
		s.log.WithFields(logrus.Fields{
			"processor": processor,
			"network":   network,
		}).Warn("Unexpected state: block_number is nil but storage is not empty")

		return big.NewInt(0), false, nil
	}

	nextBlock := new(big.Int).SetUint64(*blockNumber + 1)
	s.log.WithFields(logrus.Fields{
		"processor":        processor,
		"network":          network,
		"last_processed":   *blockNumber,
		"progressive_next": nextBlock.String(),
	}).Debug("Found last processed block, calculated progressive next block")

	return nextBlock, false, nil
}

func (s *Manager) getProgressiveNextBlockBackwards(ctx context.Context, processor, network string, chainHead *big.Int) (*big.Int, error) {
	query := fmt.Sprintf(`
		SELECT block_number
		FROM %s FINAL
		WHERE processor = '%s'
		  AND meta_network_name = '%s'
		ORDER BY block_number ASC
		LIMIT 1
	`, s.storageTable, processor, network)

	s.log.WithFields(logrus.Fields{
		"processor": processor,
		"network":   network,
		"table":     s.storageTable,
	}).Debug("Querying for earliest processed block (backwards mode)")

	blockNumber, err := s.storageClient.QueryUInt64(ctx, query, "block_number")
	if err != nil {
		return nil, fmt.Errorf("failed to get earliest block from %s: %w", s.storageTable, err)
	}

	// Check if we got a result
	if blockNumber == nil {
		// No entry in table, need to start from chain tip for backwards processing
		if chainHead != nil && chainHead.Int64() > 0 {
			s.log.WithFields(logrus.Fields{
				"processor":  processor,
				"network":    network,
				"chain_head": chainHead.String(),
			}).Info("No blocks found in storage table, starting backwards processing from chain head")

			return chainHead, nil
		} else {
			s.log.WithFields(logrus.Fields{
				"processor": processor,
				"network":   network,
			}).Debug("No blocks found in storage table, backwards mode needs chain tip")

			// Return error to signal need for chain tip discovery
			return nil, fmt.Errorf("backwards mode requires chain tip discovery when no processed blocks exist")
		}
	}

	// Calculate previous block (go backwards)
	if *blockNumber == 0 {
		// Already at genesis, no more blocks to process backwards
		s.log.WithFields(logrus.Fields{
			"processor": processor,
			"network":   network,
		}).Info("Backwards processing reached genesis block, no more blocks to backfill")

		return nil, ErrNoMoreBlocks
	}

	prevBlock := new(big.Int).SetUint64(*blockNumber - 1)
	s.log.WithFields(logrus.Fields{
		"processor":          processor,
		"network":            network,
		"earliest_processed": *blockNumber,
		"progressive_prev":   prevBlock.String(),
	}).Debug("Found earliest processed block, calculated previous block for backwards processing")

	return prevBlock, nil
}

func (s *Manager) getLimiterMaxBlock(ctx context.Context, network string) (*big.Int, error) {
	// Try to get from cache first
	s.limiterCacheMu.RLock()
	cachedValue, ok := s.limiterCacheValue[network]
	s.limiterCacheMu.RUnlock()

	if ok && cachedValue != nil {
		s.log.WithFields(logrus.Fields{
			"network":     network,
			"limiter_max": cachedValue.String(),
		}).Debug("Returning limiter max block from cache")

		return cachedValue, nil
	}

	// Cache miss - query directly (this should be rare after startup)
	s.log.WithFields(logrus.Fields{
		"network": network,
	}).Debug("Limiter cache miss, querying directly")

	return s.queryLimiterMaxBlock(ctx, network)
}

func (s *Manager) MarkBlockProcessed(ctx context.Context, blockNumber uint64, network, processor string) error {
	// Insert using direct string substitution for table name
	// Table name is validated during config initialization
	query := fmt.Sprintf("INSERT INTO %s (updated_date_time, block_number, processor, meta_network_name) VALUES ('%s', %d, '%s', '%s')", s.storageTable, time.Now().Format("2006-01-02 15:04:05.000"), blockNumber, processor, network)

	err := s.storageClient.Execute(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to mark block as processed in %s: %w", s.storageTable, err)
	}

	s.log.WithFields(logrus.Fields{
		"block_number": blockNumber,
		"processor":    processor,
		"network":      network,
	}).Debug("Marked block as processed in " + s.storageTable)

	return nil
}

// MarkBlockEnqueued inserts a block with complete=false to track that tasks have been enqueued.
// This is the first phase of two-phase completion tracking.
func (s *Manager) MarkBlockEnqueued(ctx context.Context, blockNumber uint64, taskCount int, network, processor string) error {
	query := fmt.Sprintf(
		"INSERT INTO %s (updated_date_time, block_number, processor, meta_network_name, complete, task_count) VALUES ('%s', %d, '%s', '%s', 0, %d)",
		s.storageTable,
		time.Now().Format("2006-01-02 15:04:05.000"),
		blockNumber,
		processor,
		network,
		taskCount,
	)

	err := s.storageClient.Execute(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to mark block as enqueued in %s: %w", s.storageTable, err)
	}

	s.log.WithFields(logrus.Fields{
		"block_number": blockNumber,
		"processor":    processor,
		"network":      network,
		"task_count":   taskCount,
	}).Debug("Marked block as enqueued (complete=false)")

	return nil
}

// MarkBlockComplete inserts a block with complete=true to indicate all tasks finished.
// This is the second phase of two-phase completion tracking.
// ReplacingMergeTree will keep the latest row per (processor, network, block_number).
func (s *Manager) MarkBlockComplete(ctx context.Context, blockNumber uint64, network, processor string) error {
	query := fmt.Sprintf(
		"INSERT INTO %s (updated_date_time, block_number, processor, meta_network_name, complete, task_count) VALUES ('%s', %d, '%s', '%s', 1, 0)",
		s.storageTable,
		time.Now().Format("2006-01-02 15:04:05.000"),
		blockNumber,
		processor,
		network,
	)

	err := s.storageClient.Execute(ctx, query)
	if err != nil {
		return fmt.Errorf("failed to mark block as complete in %s: %w", s.storageTable, err)
	}

	s.log.WithFields(logrus.Fields{
		"block_number": blockNumber,
		"processor":    processor,
		"network":      network,
	}).Debug("Marked block as complete")

	return nil
}

// CountIncompleteBlocks returns the count of blocks that are not yet complete.
func (s *Manager) CountIncompleteBlocks(ctx context.Context, network, processor string) (int, error) {
	query := fmt.Sprintf(`
		SELECT count(*) as count
		FROM %s FINAL
		WHERE processor = '%s'
		  AND meta_network_name = '%s'
		  AND complete = 0
	`, s.storageTable, processor, network)

	count, err := s.storageClient.QueryUInt64(ctx, query, "count")
	if err != nil {
		return 0, fmt.Errorf("failed to count incomplete blocks: %w", err)
	}

	if count == nil {
		return 0, nil
	}

	// Safe conversion - count of incomplete blocks will never exceed int max
	if *count > uint64(^uint(0)>>1) {
		return 0, fmt.Errorf("incomplete block count exceeds int max: %d", *count)
	}

	return int(*count), nil
}

// GetOldestIncompleteBlock returns the oldest incomplete block >= minBlockNumber.
// Returns nil if no incomplete blocks exist within the range.
// The minBlockNumber parameter enables startup optimization by limiting the search range.
func (s *Manager) GetOldestIncompleteBlock(ctx context.Context, network, processor string, minBlockNumber uint64) (*uint64, error) {
	query := fmt.Sprintf(`
		SELECT block_number
		FROM %s FINAL
		WHERE processor = '%s'
		  AND meta_network_name = '%s'
		  AND complete = 0
		  AND block_number >= %d
		ORDER BY block_number ASC
		LIMIT 1
	`, s.storageTable, processor, network, minBlockNumber)

	s.log.WithFields(logrus.Fields{
		"processor":        processor,
		"network":          network,
		"min_block_number": minBlockNumber,
	}).Debug("Querying for oldest incomplete block")

	blockNumber, err := s.storageClient.QueryUInt64(ctx, query, "block_number")
	if err != nil {
		return nil, fmt.Errorf("failed to get oldest incomplete block: %w", err)
	}

	return blockNumber, nil
}

// GetNewestIncompleteBlock returns the newest incomplete block <= maxBlockNumber.
// Returns nil if no incomplete blocks exist within the range.
// The maxBlockNumber parameter enables startup optimization by limiting the search range.
// This method is used for backwards processing mode.
func (s *Manager) GetNewestIncompleteBlock(ctx context.Context, network, processor string, maxBlockNumber uint64) (*uint64, error) {
	query := fmt.Sprintf(`
		SELECT block_number
		FROM %s FINAL
		WHERE processor = '%s'
		  AND meta_network_name = '%s'
		  AND complete = 0
		  AND block_number <= %d
		ORDER BY block_number DESC
		LIMIT 1
	`, s.storageTable, processor, network, maxBlockNumber)

	s.log.WithFields(logrus.Fields{
		"processor":        processor,
		"network":          network,
		"max_block_number": maxBlockNumber,
	}).Debug("Querying for newest incomplete block")

	blockNumber, err := s.storageClient.QueryUInt64(ctx, query, "block_number")
	if err != nil {
		return nil, fmt.Errorf("failed to get newest incomplete block: %w", err)
	}

	return blockNumber, nil
}

// GetIncompleteBlocks returns block numbers that are not yet complete, ordered by block_number.
func (s *Manager) GetIncompleteBlocks(ctx context.Context, network, processor string, limit int) ([]uint64, error) {
	query := fmt.Sprintf(`
		SELECT block_number
		FROM %s FINAL
		WHERE processor = '%s'
		  AND meta_network_name = '%s'
		  AND complete = 0
		ORDER BY block_number ASC
		LIMIT %d
	`, s.storageTable, processor, network, limit)

	s.log.WithFields(logrus.Fields{
		"processor": processor,
		"network":   network,
		"limit":     limit,
	}).Debug("Querying for incomplete blocks")

	blocks, err := s.storageClient.QueryUInt64Slice(ctx, query, "block_number")
	if err != nil {
		return nil, fmt.Errorf("failed to get incomplete blocks: %w", err)
	}

	return blocks, nil
}

func (s *Manager) GetMinMaxStoredBlocks(ctx context.Context, network, processor string) (minBlock, maxBlock *big.Int, err error) {
	query := fmt.Sprintf(`
		SELECT min(block_number) as min, max(block_number) as max
		FROM %s FINAL
		WHERE meta_network_name = '%s' AND processor = '%s'
	`, s.storageTable, network, processor)

	s.log.WithFields(logrus.Fields{
		"network":   network,
		"processor": processor,
		"table":     s.storageTable,
	}).Debug("Querying for min/max stored blocks")

	minResult, maxResult, err := s.storageClient.QueryMinMaxUInt64(ctx, query)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get min/max blocks: %w", err)
	}

	// Handle case where no blocks are stored
	if minResult == nil || maxResult == nil {
		s.log.WithFields(logrus.Fields{
			"network":   network,
			"processor": processor,
			"table":     s.storageTable,
		}).Debug("No blocks found in database (min or max is nil)")

		return nil, nil, nil
	}

	s.log.WithFields(logrus.Fields{
		"network":   network,
		"processor": processor,
		"table":     s.storageTable,
		"min":       *minResult,
		"max":       *maxResult,
	}).Debug("Found min/max blocks")

	return new(big.Int).SetUint64(*minResult), new(big.Int).SetUint64(*maxResult), nil
}

// IsBlockRecentlyProcessed checks if a block was processed within the specified number of seconds.
func (s *Manager) IsBlockRecentlyProcessed(ctx context.Context, blockNumber uint64, network, processor string, withinSeconds int) (bool, error) {
	query := fmt.Sprintf(`
		SELECT COUNT(*) as count
		FROM %s FINAL
		WHERE processor = '%s'
		  AND meta_network_name = '%s'
		  AND block_number = %d
		  AND updated_date_time >= now() - INTERVAL %d SECOND
	`, s.storageTable, processor, network, blockNumber, withinSeconds)

	s.log.WithFields(logrus.Fields{
		"network":        network,
		"processor":      processor,
		"block_number":   blockNumber,
		"within_seconds": withinSeconds,
		"table":          s.storageTable,
	}).Debug("Checking if block was recently processed")

	count, err := s.storageClient.QueryUInt64(ctx, query, "count")
	if err != nil {
		return false, fmt.Errorf("failed to check recent block processing: %w", err)
	}

	return count != nil && *count > 0, nil
}

// GetHeadDistance calculates the distance between current processing block and the relevant head.
func (s *Manager) GetHeadDistance(ctx context.Context, processor, network, mode string, executionHead *big.Int) (distance int64, headType string, err error) {
	// Get the current processing block (what would be next to process)
	currentBlock, err := s.NextBlock(ctx, processor, network, mode, executionHead)
	if err != nil {
		if errors.Is(err, ErrNoMoreBlocks) {
			// If no more blocks, distance is 0
			return 0, "", nil
		}

		return 0, "", fmt.Errorf("failed to get current processing block: %w", err)
	}

	if currentBlock == nil {
		return 0, "", nil
	}

	// Determine which head to use based on limiter status and mode
	if !s.limiterEnabled || mode == tracker.BACKWARDS_MODE {
		// Use execution node head
		if executionHead == nil {
			return 0, "", fmt.Errorf("execution head not available")
		}

		distance = executionHead.Int64() - currentBlock.Int64()
		headType = "execution_head"

		s.log.WithFields(logrus.Fields{
			"processor":       processor,
			"network":         network,
			"mode":            mode,
			"current_block":   currentBlock.String(),
			"execution_head":  executionHead.String(),
			"distance":        distance,
			"limiter_enabled": s.limiterEnabled,
		}).Debug("Calculated head distance using execution node head")

		return distance, headType, nil
	}

	// Use beacon chain limiter head (forwards mode with limiter enabled)
	beaconHead, err := s.getLimiterMaxBlock(ctx, network)
	if err != nil {
		s.log.WithFields(logrus.Fields{
			"processor": processor,
			"network":   network,
			"error":     err,
		}).Warn("Failed to get beacon chain head for distance calculation, falling back to execution head")

		// Fallback to execution head if beacon chain is unavailable
		if executionHead == nil {
			return 0, "", fmt.Errorf("both beacon and execution heads unavailable")
		}

		distance = executionHead.Int64() - currentBlock.Int64()
		headType = "execution_head_fallback"

		return distance, headType, nil
	}

	distance = beaconHead.Int64() - currentBlock.Int64()
	headType = "beacon_chain_head"

	s.log.WithFields(logrus.Fields{
		"processor":       processor,
		"network":         network,
		"mode":            mode,
		"current_block":   currentBlock.String(),
		"beacon_head":     beaconHead.String(),
		"distance":        distance,
		"limiter_enabled": s.limiterEnabled,
	}).Debug("Calculated head distance using beacon chain head")

	return distance, headType, nil
}

// startLimiterCacheRefresh starts the background goroutine for limiter cache refresh.
// It ensures only one refresh goroutine runs even if called multiple times.
func (s *Manager) startLimiterCacheRefresh() {
	s.limiterCacheMu.Lock()

	if s.limiterCacheStarted {
		s.limiterCacheMu.Unlock()

		return
	}

	s.limiterCacheStarted = true
	s.limiterCacheValue = make(map[string]*big.Int, 1)
	s.limiterCacheStop = make(chan struct{})
	s.limiterCacheMu.Unlock()

	go s.refreshLimiterCacheLoop()
}

// refreshLimiterCacheLoop runs the cache refresh loop in a background goroutine.
func (s *Manager) refreshLimiterCacheLoop() {
	ticker := time.NewTicker(limiterCacheRefreshInterval)
	defer ticker.Stop()

	// Do initial refresh immediately
	s.refreshLimiterCache()

	for {
		select {
		case <-s.limiterCacheStop:
			return
		case <-ticker.C:
			s.refreshLimiterCache()
		}
	}
}

// refreshLimiterCache queries ClickHouse and updates the cache.
func (s *Manager) refreshLimiterCache() {
	if s.network == "" {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	maxBlock, err := s.queryLimiterMaxBlock(ctx, s.network)
	if err != nil {
		s.log.WithError(err).Warn("Failed to refresh limiter cache")

		return
	}

	s.limiterCacheMu.Lock()
	s.limiterCacheValue[s.network] = maxBlock
	s.limiterCacheMu.Unlock()

	s.log.WithFields(logrus.Fields{
		"network":     s.network,
		"limiter_max": maxBlock.String(),
	}).Debug("Refreshed limiter cache")
}

// queryLimiterMaxBlock performs the actual ClickHouse query.
// Uses ORDER BY slot_start_date_time DESC LIMIT 1 to leverage the table's index.
func (s *Manager) queryLimiterMaxBlock(ctx context.Context, network string) (*big.Int, error) {
	// Optimized query: uses ORDER BY index instead of MAX() which requires full table scan
	query := fmt.Sprintf(`
		SELECT toUInt64(ifNull(execution_payload_block_number, 0)) AS block_number
		FROM %s
		WHERE meta_network_name = '%s'
		  AND execution_payload_block_number IS NOT NULL
		ORDER BY slot_start_date_time DESC
		LIMIT 1
	`, s.limiterTable, network)

	s.log.WithFields(logrus.Fields{
		"network": network,
		"table":   s.limiterTable,
	}).Debug("Querying for maximum execution payload block number")

	blockNumber, err := s.limiterClient.QueryUInt64(ctx, query, "block_number")
	if err != nil {
		return nil, fmt.Errorf("failed to query max execution payload block from %s: %w", s.limiterTable, err)
	}

	if blockNumber == nil || *blockNumber == 0 {
		return big.NewInt(0), nil
	}

	return new(big.Int).SetUint64(*blockNumber), nil
}
