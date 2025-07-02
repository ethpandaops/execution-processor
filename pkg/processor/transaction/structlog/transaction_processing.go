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
	// Extract structlog data
	structlogs, err := p.ExtractStructlogs(ctx, block, index, tx)
	if err != nil {
		return 0, err
	}

	// Store count before processing
	structlogCount := len(structlogs)

	// Ensure we clear the slice on exit to allow GC, especially important for failed inserts
	defer func() {
		// Clear the slice to release memory
		structlogs = nil
		// Force GC for large transactions or on errors
		if structlogCount > 1000 {
			runtime.GC()
		}
	}()

	// Send to batch collector for insertion
	if err := p.sendToBatchCollector(ctx, structlogs); err != nil {
		common.TransactionsProcessed.WithLabelValues(p.network.Name, "structlog", "failed").Inc()
		// Log memory cleanup for large failed batches
		if structlogCount > 10000 {
			p.log.WithFields(logrus.Fields{
				"transaction_hash": tx.Hash().String(),
				"structlog_count":  structlogCount,
				"error":            err.Error(),
			}).Info("Cleaning up memory after failed batch insert")
		}

		runtime.GC()

		return 0, fmt.Errorf("failed to insert structlogs via batch collector: %w", err)
	}

	// Record success metrics
	common.TransactionsProcessed.WithLabelValues(p.network.Name, "structlog", "success").Inc()

	return structlogCount, nil
}

// ExtractStructlogs extracts structlog data from a transaction without inserting to database
func (p *Processor) ExtractStructlogs(ctx context.Context, block *types.Block, index int, tx *types.Transaction) ([]Structlog, error) {
	start := time.Now()
	defer func() {
		duration := time.Since(start)
		common.TransactionProcessingDuration.WithLabelValues(p.network.Name, "structlog").Observe(duration.Seconds())
	}()

	// Get execution node
	node := p.pool.GetHealthyExecutionNode()
	if node == nil {
		return nil, fmt.Errorf("no healthy execution node available")
	}

	// Process transaction with timeout
	processCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Get transaction trace
	trace, err := node.DebugTraceTransaction(processCtx, tx.Hash().String(), block.Number(), execution.StackTraceOptions())
	if err != nil {
		return nil, fmt.Errorf("failed to trace transaction: %w", err)
	}

	// Convert trace to structlog rows
	var structlogs []Structlog

	uIndex := uint32(index) //nolint:gosec // index is bounded by block.Transactions() length

	if trace != nil {
		// Pre-allocate slice for better memory efficiency
		structlogs = make([]Structlog, 0, len(trace.Structlogs))

		// For extremely large traces, log memory usage periodically
		logInterval := 100000

		for i, structLog := range trace.Structlogs {
			var callToAddress *string

			if structLog.Op == "CALL" && structLog.Stack != nil && len(*structLog.Stack) > 1 {
				stackValue := (*structLog.Stack)[len(*structLog.Stack)-2]
				callToAddress = &stackValue
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

			structlogs = append(structlogs, row)

			// For very large traces, periodically log memory usage
			if i > 0 && i%logInterval == 0 && len(trace.Structlogs) > 200000 {
				p.log.WithFields(logrus.Fields{
					"transaction_hash": tx.Hash().String(),
					"progress":         fmt.Sprintf("%d/%d", i, len(trace.Structlogs)),
				}).Debug("Processing large trace")

				// Force GC during processing of extremely large traces
				if i%200000 == 0 {
					runtime.GC()
				}
			}
		}

		// Clear the original trace data to free memory
		trace.Structlogs = nil
	}

	return structlogs, nil
}
