package structlog

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/0xsequence/ethkit/go-ethereum/core/types"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

type Structlog struct {
	UpdatedDateTime        time.Time `json:"updated_date_time"`
	BlockNumber            uint64    `json:"block_number"`
	TransactionHash        string    `json:"transaction_hash"`
	TransactionIndex       uint32    `json:"transaction_index"`
	TransactionGas         uint64    `json:"transaction_gas"`
	TransactionFailed      bool      `json:"transaction_failed"`
	TransactionReturnValue *string   `json:"transaction_return_value"`
	Index                  uint32    `json:"index"`
	ProgramCounter         uint32    `json:"program_counter"`
	Operation              string    `json:"operation"`
	Gas                    uint64    `json:"gas"`
	GasCost                uint64    `json:"gas_cost"`
	Depth                  uint64    `json:"depth"`
	ReturnData             *string   `json:"return_data"`
	Refund                 *uint64   `json:"refund"`
	Error                  *string   `json:"error"`
	CallToAddress          *string   `json:"call_to_address"`
	MetaNetworkID          int32     `json:"meta_network_id"`
	MetaNetworkName        string    `json:"meta_network_name"`
}

// ProcessSingleTransaction processes a single transaction using batch collector (exposed for worker handlers)
func (p *Processor) ProcessSingleTransaction(ctx context.Context, block *types.Block, index int, tx *types.Transaction) (int, error) {
	// Extract structlog data with optimized memory handling
	structlogCount, err := p.ExtractAndProcessStructlogs(ctx, block, index, tx)
	if err != nil {
		return 0, err
	}

	// Record success metrics
	common.TransactionsProcessed.WithLabelValues(p.network.Name, "structlog", "success").Inc()

	return structlogCount, nil
}

// ProcessSingleTransactionWithSizeInfo processes a transaction and returns size info for large tx handling
func (p *Processor) ProcessSingleTransactionWithSizeInfo(ctx context.Context, block *types.Block, index int, tx *types.Transaction) (int, bool, error) {
	// This will be called from handlers_large_tx.go to get size info during processing
	structlogCount, err := p.ExtractAndProcessStructlogs(ctx, block, index, tx)
	if err != nil {
		return 0, false, err
	}

	isLarge := false
	if p.largeTxLock != nil && p.largeTxLock.config.Enabled {
		isLarge = p.largeTxLock.IsLargeTransaction(structlogCount)
	}

	// Record success metrics
	common.TransactionsProcessed.WithLabelValues(p.network.Name, "structlog", "success").Inc()

	return structlogCount, isLarge, nil
}

// ExtractAndProcessStructlogs extracts and processes structlog data with optimized memory handling
func (p *Processor) ExtractAndProcessStructlogs(ctx context.Context, block *types.Block, index int, tx *types.Transaction) (int, error) {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		common.TransactionProcessingDuration.WithLabelValues(p.network.Name, "structlog").Observe(duration.Seconds())
	}()

	// Get execution node
	node := p.pool.GetHealthyExecutionNode()
	if node == nil {
		return 0, fmt.Errorf("no healthy execution node available")
	}

	// Process transaction with timeout
	processCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Get transaction trace with stack enabled for CALL operations
	trace, err := node.DebugTraceTransaction(processCtx, tx.Hash().String(), block.Number(), execution.StackTraceOptions())
	if err != nil {
		return 0, fmt.Errorf("failed to trace transaction: %w", err)
	}

	if trace == nil || len(trace.Structlogs) == 0 {
		return 0, nil
	}

	// Store the count for return
	structlogCount := len(trace.Structlogs)

	// Pre-extract call addresses into a map to avoid keeping stack data
	callAddresses := make(map[int]string)
	for i, structLog := range trace.Structlogs {
		if structLog.Op == "CALL" && structLog.Stack != nil && len(*structLog.Stack) > 1 {
			callAddresses[i] = (*structLog.Stack)[len(*structLog.Stack)-2]
		}
		// Clear stack data immediately after extraction
		structLog.Stack = nil
	}

	// Process in batches for better memory efficiency
	const batchSize = 10000
	uIndex := uint32(index) //nolint:gosec // index is bounded by block.Transactions() length
	
	for batchStart := 0; batchStart < len(trace.Structlogs); batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > len(trace.Structlogs) {
			batchEnd = len(trace.Structlogs)
		}

		// Create batch of structlogs
		batch := make([]Structlog, 0, batchEnd-batchStart)
		
		for i := batchStart; i < batchEnd; i++ {
			structLog := trace.Structlogs[i]
			
			var callToAddress *string
			if addr, exists := callAddresses[i]; exists {
				callToAddress = &addr
			}

			row := Structlog{
				UpdatedDateTime:        time.Now(),
				BlockNumber:            block.Number().Uint64(),
				TransactionHash:        tx.Hash().String(),
				TransactionIndex:       uIndex,
				TransactionGas:         trace.Gas,
				TransactionFailed:      trace.Failed,
				TransactionReturnValue: trace.ReturnValue,
				Index:                  uint32(i), //nolint:gosec // index is bounded by structlogs length
				ProgramCounter:         structLog.PC,
				Operation:              structLog.Op,
				Gas:                    structLog.Gas,
				GasCost:                structLog.GasCost,
				Depth:                  structLog.Depth,
				ReturnData:             structLog.ReturnData,
				Refund:                 structLog.Refund,
				Error:                  structLog.Error,
				CallToAddress:          callToAddress,
				MetaNetworkID:          p.network.ID,
				MetaNetworkName:        p.network.Name,
			}

			batch = append(batch, row)
		}

		// Send batch to collector
		if err := p.sendToBatchCollector(ctx, batch); err != nil {
			common.TransactionsProcessed.WithLabelValues(p.network.Name, "structlog", "failed").Inc()
			
			// Log memory cleanup for large failed batches
			if structlogCount > 10000 {
				p.log.WithFields(logrus.Fields{
					"transaction_hash": tx.Hash().String(),
					"structlog_count":  structlogCount,
					"batch_start":      batchStart,
					"batch_end":        batchEnd,
					"error":            err.Error(),
				}).Info("Failed to insert batch")
			}

			// Clear batch and force GC on error
			batch = nil
			if structlogCount > 100000 {
				runtime.GC()
			}

			return 0, fmt.Errorf("failed to insert structlogs via batch collector: %w", err)
		}

		// Clear batch after successful insertion
		batch = nil

		// Log progress for very large traces
		if batchEnd > 100000 && batchEnd%100000 == 0 {
			p.log.WithFields(logrus.Fields{
				"transaction_hash": tx.Hash().String(),
				"progress":         fmt.Sprintf("%d/%d", batchEnd, structlogCount),
			}).Debug("Processing large trace")
		}

		// Force GC periodically for extremely large traces
		if batchEnd > 500000 && batchEnd%500000 == 0 {
			runtime.GC()
		}
	}

	// Clear all remaining data
	trace.Structlogs = nil
	trace = nil
	callAddresses = nil

	// Final GC for very large transactions
	if structlogCount > 100000 {
		runtime.GC()
	}

	return structlogCount, nil
}

