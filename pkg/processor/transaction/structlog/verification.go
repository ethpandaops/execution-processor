package structlog

import (
	"context"
	"fmt"
	"math/big"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
	"github.com/ethpandaops/execution-processor/pkg/state"
)

// CountMismatchError represents a structlog count mismatch between expected and actual counts.
type CountMismatchError struct {
	Expected int
	Actual   int
	Message  string
}

func (e *CountMismatchError) Error() string {
	return e.Message
}

// VerifyTransaction verifies that a transaction has been processed correctly.
func (p *Processor) VerifyTransaction(ctx context.Context, blockNumber *big.Int, transactionHash string, transactionIndex uint32, networkName string, insertedCount int) error {
	p.log.WithFields(logrus.Fields{
		"block_number":      blockNumber.String(),
		"transaction_hash":  transactionHash,
		"transaction_index": transactionIndex,
		"network":           networkName,
	}).Debug("Starting transaction verification")

	// Get execution node
	node := p.pool.GetHealthyExecutionNode()

	if node == nil {
		return fmt.Errorf("no healthy execution node available")
	}

	// Process transaction with timeout
	processCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Get transaction trace
	trace, err := node.DebugTraceTransaction(processCtx, transactionHash, blockNumber, execution.DefaultTraceOptions())
	if err != nil {
		return fmt.Errorf("failed to trace transaction: %w", err)
	}

	expectedCount := len(trace.Structlogs)

	// Query ClickHouse to count the structlog entries for this transaction
	query := fmt.Sprintf(`
		SELECT COUNT(*) as count
		FROM %s FINAL
		WHERE block_number = %d
		  AND transaction_hash = '%s'
		  AND transaction_index = %d
		  AND meta_network_name = '%s'
	`, p.config.Table, blockNumber.Uint64(), transactionHash, transactionIndex, networkName)

	p.log.WithField("query", query).Debug("Executing verification query")

	// Define result struct for count query
	type countResult struct {
		Count state.JSONInt `json:"count"`
	}

	var result countResult
	if err := p.clickhouse.QueryOne(ctx, query, &result); err != nil {
		p.log.WithError(err).WithFields(logrus.Fields{
			"table":             p.config.Table,
			"block_number":      blockNumber.Uint64(),
			"transaction_hash":  transactionHash,
			"transaction_index": transactionIndex,
			"network":           networkName,
		}).Error("Failed to query structlog count")

		return fmt.Errorf("failed to query structlog count: %w", err)
	}

	actualCount := result.Count.Int()

	p.log.WithFields(logrus.Fields{
		"actual_count":   actualCount,
		"expected_count": expectedCount,
		"match":          actualCount == expectedCount,
	}).Debug("Verification query completed")

	if actualCount != expectedCount {
		p.log.WithFields(logrus.Fields{
			"block_number":      blockNumber.Uint64(),
			"transaction_hash":  transactionHash,
			"transaction_index": transactionIndex,
			"network":           networkName,
			"expected_count":    expectedCount,
			"actual_count":      actualCount,
			"inserted_count":    insertedCount,
		}).Error("Structlog count mismatch")

		return &CountMismatchError{
			Expected: expectedCount,
			Actual:   actualCount,
			Message:  fmt.Sprintf("structlog count mismatch: expected %d, got %d", expectedCount, actualCount),
		}
	}

	p.log.WithFields(logrus.Fields{
		"transaction_hash": transactionHash,
		"count":            actualCount,
	}).Debug("Transaction verification successful")

	return nil
}
