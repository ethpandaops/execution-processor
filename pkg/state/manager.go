package state

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
	"github.com/ethpandaops/execution-processor/pkg/processor/common"
	"github.com/sirupsen/logrus"
)

// Sentinel errors.
var (
	ErrNoMoreBlocks = errors.New("no more blocks to process")
)

type Manager struct {
	log            logrus.FieldLogger
	storageClient  *clickhouse.Client
	limiterClient  *clickhouse.Client
	storageTable   string
	limiterTable   string
	limiterEnabled bool
	network        string
}

func NewManager(ctx context.Context, log logrus.FieldLogger, config *Config) (*Manager, error) {
	// Create storage client
	storageClient, err := clickhouse.NewClient(ctx, log, &clickhouse.Config{
		DSN:          config.Storage.DSN,
		MaxOpenConns: config.Storage.MaxOpenConns,
		MaxIdleConns: config.Storage.MaxIdleConns,
	})
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
		limiterClient, err := clickhouse.NewClient(ctx, log, &clickhouse.Config{
			DSN:          config.Limiter.DSN,
			MaxOpenConns: config.Limiter.MaxOpenConns,
			MaxIdleConns: config.Limiter.MaxIdleConns,
		})
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
	// Update ClickHouse clients with network label
	if s.storageClient != nil {
		s.storageClient.WithLabels(network, "")
	}

	if s.limiterClient != nil {
		s.limiterClient.WithLabels(network, "")
	}
}

func (s *Manager) Start(ctx context.Context) error {
	if err := s.storageClient.Start(ctx); err != nil {
		return fmt.Errorf("failed to start storage client: %w", err)
	}

	if s.limiterEnabled && s.limiterClient != nil {
		if err := s.limiterClient.Start(ctx); err != nil {
			return fmt.Errorf("failed to start limiter client: %w", err)
		}
	}

	return nil
}

func (s *Manager) Stop(ctx context.Context) error {
	var err error

	if stopErr := s.storageClient.Stop(ctx); stopErr != nil {
		err = fmt.Errorf("failed to stop storage client: %w", stopErr)
	}

	if s.limiterEnabled && s.limiterClient != nil {
		if stopErr := s.limiterClient.Stop(ctx); stopErr != nil {
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

	var err error

	if mode == common.BACKWARDS_MODE {
		progressiveNext, err = s.getProgressiveNextBlockBackwards(ctx, processor, network, chainHead)
	} else {
		// Default to forwards mode
		progressiveNext, err = s.getProgressiveNextBlock(ctx, processor, network, chainHead)
	}

	if err != nil {
		return nil, err
	}

	// If limiter is disabled or backwards mode, return progressive next block
	// Limiter only applies to forwards processing to prevent exceeding beacon chain
	if !s.limiterEnabled || mode == common.BACKWARDS_MODE {
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

	// Check if admin.execution_block table is empty by seeing if progressive next equals chain head
	// This happens when getProgressiveNextBlock returns chain head due to no entries in storage table
	isStorageEmpty := chainHead != nil && progressiveNext.Cmp(chainHead) == 0

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

func (s *Manager) getProgressiveNextBlock(ctx context.Context, processor, network string, chainHead *big.Int) (*big.Int, error) {
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

	var blockNumber sql.NullInt64

	row := s.storageClient.QueryRow(ctx, s.storageTable, query)
	if row == nil {
		// ClickHouse client not started, return genesis block
		s.log.WithFields(logrus.Fields{
			"processor": processor,
			"network":   network,
		}).Warn("Storage ClickHouse client not available, starting from genesis block")

		return big.NewInt(0), nil
	}

	err := row.Scan(&blockNumber)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get next block from %s: %w", s.storageTable, err)
	}

	if !blockNumber.Valid {
		// No entry in table - start from chain head if available, otherwise genesis
		if chainHead != nil && chainHead.Int64() > 0 {
			s.log.WithFields(logrus.Fields{
				"processor":  processor,
				"network":    network,
				"chain_head": chainHead.String(),
			}).Info("No blocks found in storage table, starting forwards processing from chain head")

			return chainHead, nil
		} else {
			s.log.WithFields(logrus.Fields{
				"processor": processor,
				"network":   network,
			}).Debug("No blocks found in storage table, starting from genesis (block 0)")

			return big.NewInt(0), nil
		}
	}

	nextBlock := big.NewInt(blockNumber.Int64 + 1)
	s.log.WithFields(logrus.Fields{
		"processor":        processor,
		"network":          network,
		"last_processed":   blockNumber.Int64,
		"progressive_next": nextBlock.String(),
	}).Debug("Found last processed block, calculated progressive next block")

	return nextBlock, nil
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

	var blockNumber sql.NullInt64

	row := s.storageClient.QueryRow(ctx, s.storageTable, query)
	if row == nil {
		// ClickHouse client not started, need to find chain tip
		s.log.WithFields(logrus.Fields{
			"processor": processor,
			"network":   network,
		}).Warn("Storage ClickHouse client not available for backwards mode")

		return nil, fmt.Errorf("storage client not available for backwards processing")
	}

	err := row.Scan(&blockNumber)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get earliest block from %s: %w", s.storageTable, err)
	}

	if !blockNumber.Valid {
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
	if blockNumber.Int64 <= 0 {
		// Already at genesis, no more blocks to process backwards
		s.log.WithFields(logrus.Fields{
			"processor": processor,
			"network":   network,
		}).Info("Backwards processing reached genesis block, no more blocks to backfill")

		return nil, ErrNoMoreBlocks
	}

	prevBlock := big.NewInt(blockNumber.Int64 - 1)
	s.log.WithFields(logrus.Fields{
		"processor":          processor,
		"network":            network,
		"earliest_processed": blockNumber.Int64,
		"progressive_prev":   prevBlock.String(),
	}).Debug("Found earliest processed block, calculated previous block for backwards processing")

	return prevBlock, nil
}

func (s *Manager) getLimiterMaxBlock(ctx context.Context, network string) (*big.Int, error) {
	query := fmt.Sprintf(`
		SELECT max(execution_payload_block_number)
		FROM %s FINAL
		WHERE meta_network_name = '%s'
	`, s.limiterTable, network)

	s.log.WithFields(logrus.Fields{
		"network": network,
		"table":   s.limiterTable,
	}).Debug("Querying for maximum execution payload block number")

	var maxBlockNumber sql.NullInt64

	row := s.limiterClient.QueryRow(ctx, s.limiterTable, query)
	if row == nil {
		return nil, fmt.Errorf("limiter ClickHouse client not available")
	}

	err := row.Scan(&maxBlockNumber)
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("failed to get max execution payload block from %s: %w", s.limiterTable, err)
	}

	if !maxBlockNumber.Valid {
		// No blocks in limiter table, return genesis
		s.log.WithFields(logrus.Fields{
			"network": network,
		}).Debug("No blocks found in limiter table, returning genesis block as max")

		return big.NewInt(0), nil
	}

	maxBlock := big.NewInt(maxBlockNumber.Int64)
	s.log.WithFields(logrus.Fields{
		"network":     network,
		"limiter_max": maxBlock.String(),
	}).Debug("Found maximum execution payload block number")

	return maxBlock, nil
}

func (s *Manager) MarkBlockProcessed(ctx context.Context, blockNumber uint64, network, processor string) error {
	// Insert using direct string substitution for table name
	// Table name is validated during config initialization
	query := fmt.Sprintf("INSERT INTO %s (updated_date_time, block_number, processor, meta_network_name) VALUES (?, ?, ?, ?)", s.storageTable)

	_, err := s.storageClient.GetDB().ExecContext(ctx, query, time.Now(), blockNumber, processor, network)
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

func (s *Manager) GetMinMaxStoredBlocks(ctx context.Context, network, processor string) (minBlock, maxBlock *big.Int, err error) {
	query := fmt.Sprintf(`
		SELECT min(block_number), max(block_number)
		FROM %s FINAL
		WHERE meta_network_name = '%s' AND processor = '%s'
	`, s.storageTable, network, processor)

	s.log.WithFields(logrus.Fields{
		"network":   network,
		"processor": processor,
		"table":     s.storageTable,
	}).Debug("Querying for min/max stored blocks")

	var minVal, maxVal sql.NullInt64

	row := s.storageClient.QueryRow(ctx, s.storageTable, query)
	if row == nil {
		return nil, nil, fmt.Errorf("storage ClickHouse client not available")
	}

	err = row.Scan(&minVal, &maxVal)
	if err != nil && err != sql.ErrNoRows {
		return nil, nil, fmt.Errorf("failed to get min/max blocks: %w", err)
	}

	// Handle case where no blocks are stored
	if !minVal.Valid || !maxVal.Valid {
		return nil, nil, nil
	}

	return big.NewInt(minVal.Int64), big.NewInt(maxVal.Int64), nil
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

	var count int64

	row := s.storageClient.QueryRow(ctx, s.storageTable, query)
	if row == nil {
		return false, fmt.Errorf("storage ClickHouse client not available")
	}

	err := row.Scan(&count)
	if err != nil && err != sql.ErrNoRows {
		return false, fmt.Errorf("failed to check recent block processing: %w", err)
	}

	return count > 0, nil
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
	if !s.limiterEnabled || mode == common.BACKWARDS_MODE {
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
