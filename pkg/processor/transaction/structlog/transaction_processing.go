package structlog

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
	"github.com/ethpandaops/execution-processor/pkg/processor/tracker"
)

// ProcessTransaction processes a transaction using ch-go columnar streaming.
func (p *Processor) ProcessTransaction(ctx context.Context, block *types.Block, index int, tx *types.Transaction) (int, error) {
	trace, err := p.getTransactionTrace(ctx, tx, block)
	if err != nil {
		return 0, fmt.Errorf("failed to get trace: %w", err)
	}

	totalCount := len(trace.Structlogs)
	if totalCount == 0 {
		return 0, nil
	}

	// Compute actual gas used for each structlog
	gasUsed := ComputeGasUsed(trace.Structlogs)

	chunkSize := p.config.ChunkSize
	if chunkSize == 0 {
		chunkSize = tracker.DefaultChunkSize
	}

	cols := NewColumns()
	input := cols.Input()

	currentIdx := 0
	blockNum := block.Number().Uint64()
	txHash := tx.Hash().String()
	txIndex := uint32(index) //nolint:gosec // index is bounded by block.Transactions() length
	now := time.Now()

	// Use Do with OnInput for streaming insert
	err = p.clickhouse.Do(ctx, ch.Query{
		Body:  input.Into(p.config.Table),
		Input: input,
		OnInput: func(ctx context.Context) error {
			// Reset columns for next chunk
			cols.Reset()

			if currentIdx >= totalCount {
				return io.EOF
			}

			// Fill columns with next chunk
			end := currentIdx + chunkSize
			if end > totalCount {
				end = totalCount
			}

			for i := currentIdx; i < end; i++ {
				sl := &trace.Structlogs[i]
				cols.Append(
					now,
					blockNum,
					txHash,
					txIndex,
					trace.Gas,
					trace.Failed,
					trace.ReturnValue,
					uint32(i), //nolint:gosec // index is bounded by structlogs length
					sl.PC,
					sl.Op,
					sl.Gas,
					sl.GasCost,
					gasUsed[i],
					sl.Depth,
					sl.ReturnData,
					sl.Refund,
					sl.Error,
					p.extractCallAddress(sl),
					p.network.Name,
				)

				// Free original trace data immediately to help GC
				trace.Structlogs[i] = execution.StructLog{}
			}

			// Log progress for large transactions
			progressThreshold := p.config.ProgressLogThreshold
			if progressThreshold == 0 {
				progressThreshold = tracker.DefaultProgressLogThreshold
			}

			if totalCount > progressThreshold && end%progressThreshold < chunkSize {
				p.log.WithFields(logrus.Fields{
					"tx_hash":  txHash,
					"progress": fmt.Sprintf("%d/%d", end, totalCount),
				}).Debug("Processing large transaction")
			}

			currentIdx = end

			return nil
		},
	})
	if err != nil {
		return 0, fmt.Errorf("insert failed: %w", err)
	}

	// Record success metrics
	common.TransactionsProcessed.WithLabelValues(p.network.Name, "structlog", "success").Inc()
	common.ClickHouseInsertsRows.WithLabelValues(p.network.Name, ProcessorName, p.config.Table, "success", "").Add(float64(totalCount))

	return totalCount, nil
}

// getTransactionTrace gets the trace for a transaction.
func (p *Processor) getTransactionTrace(ctx context.Context, tx *types.Transaction, block *types.Block) (*execution.TraceTransaction, error) {
	// Get execution node
	node := p.pool.GetHealthyExecutionNode()
	if node == nil {
		return nil, fmt.Errorf("no healthy execution node available")
	}

	// Process transaction with timeout
	processCtx, cancel := context.WithTimeout(ctx, tracker.DefaultTraceTimeout)
	defer cancel()

	// Get transaction trace
	trace, err := node.DebugTraceTransaction(processCtx, tx.Hash().String(), block.Number(), execution.StackTraceOptions())
	if err != nil {
		return nil, fmt.Errorf("failed to trace transaction: %w", err)
	}

	return trace, nil
}

// extractCallAddress extracts the call address from a structlog if it's a CALL operation.
func (p *Processor) extractCallAddress(structLog *execution.StructLog) *string {
	if structLog.Op == "CALL" && structLog.Stack != nil && len(*structLog.Stack) > 1 {
		stackValue := (*structLog.Stack)[len(*structLog.Stack)-2]

		return &stackValue
	}

	return nil
}
