package structlog

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/0xsequence/ethkit/go-ethereum/core/types"

	"github.com/ethpandaops/execution-processor/pkg/common"
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

	// Send to batch collector for insertion
	if err := p.sendToBatchCollector(ctx, structlogs); err != nil {
		common.TransactionsProcessed.WithLabelValues(p.network.Name, "structlog", "failed").Inc()
		runtime.GC()

		return 0, fmt.Errorf("failed to insert structlogs via batch collector: %w", err)
	}

	// Force garbage collection for large data processing to prevent memory buildup
	if len(structlogs) > 1000 {
		runtime.GC()
	}

	// Record success metrics
	common.TransactionsProcessed.WithLabelValues(p.network.Name, "structlog", "success").Inc()

	return len(structlogs), nil
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
	trace, err := node.DebugTraceTransaction(processCtx, tx.Hash().String(), block.Number())
	if err != nil {
		return nil, fmt.Errorf("failed to trace transaction: %w", err)
	}

	// Convert trace to structlog rows
	var structlogs []Structlog

	uIndex := uint32(index) //nolint:gosec // index is bounded by block.Transactions() length

	if trace != nil {
		for i, structLog := range trace.Structlogs {
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
				MetaNetworkID:          p.network.ID,
				MetaNetworkName:        p.network.Name,
			}

			structlogs = append(structlogs, row)
		}
	}

	return structlogs, nil
}
