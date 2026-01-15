package structlog

import (
	"context"
	"fmt"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

// Structlog represents a single EVM opcode execution within a transaction trace.
// See gas_cost.go for detailed documentation on the gas fields.
//
//nolint:tagliatelle // ClickHouse uses snake_case column names
type Structlog struct {
	UpdatedDateTime        ClickHouseTime `json:"updated_date_time"`
	BlockNumber            uint64         `json:"block_number"`
	TransactionHash        string         `json:"transaction_hash"`
	TransactionIndex       uint32         `json:"transaction_index"`
	TransactionGas         uint64         `json:"transaction_gas"`
	TransactionFailed      bool           `json:"transaction_failed"`
	TransactionReturnValue *string        `json:"transaction_return_value"`
	Index                  uint32         `json:"index"`
	ProgramCounter         uint32         `json:"program_counter"`
	Operation              string         `json:"operation"`

	// Gas is the remaining gas before this opcode executes.
	Gas uint64 `json:"gas"`

	// GasCost is from the execution node trace. For CALL/CREATE opcodes, this is the
	// gas stipend passed to the child frame, not the call overhead.
	GasCost uint64 `json:"gas_cost"`

	// GasUsed is computed as gas[i] - gas[i+1] at the same depth level.
	// For CALL/CREATE opcodes, this includes the call overhead plus all child frame gas.
	// Summing across all opcodes will double count child frame gas.
	GasUsed uint64 `json:"gas_used"`

	// GasSelf excludes child frame gas. For CALL/CREATE opcodes, this is just the call
	// overhead (warm/cold access, memory expansion). For other opcodes, equals GasUsed.
	// Summing across all opcodes gives total execution gas without double counting.
	GasSelf uint64 `json:"gas_self"`

	Depth           uint64   `json:"depth"`
	ReturnData      *string  `json:"return_data"`
	Refund          *uint64  `json:"refund"`
	Error           *string  `json:"error"`
	CallToAddress   *string  `json:"call_to_address"`
	CallFrameID     uint32   `json:"call_frame_id"`
	CallFramePath   []uint32 `json:"call_frame_path"`
	MetaNetworkID   int32    `json:"meta_network_id"`
	MetaNetworkName string   `json:"meta_network_name"`
}

// ProcessSingleTransaction processes a single transaction and inserts its structlogs directly to ClickHouse.
func (p *Processor) ProcessSingleTransaction(ctx context.Context, block *types.Block, index int, tx *types.Transaction) (int, error) {
	// Extract structlog data
	structlogs, err := p.ExtractStructlogs(ctx, block, index, tx)
	if err != nil {
		return 0, err
	}

	// Store count before processing
	structlogCount := len(structlogs)

	// Ensure we clear the slice on exit to allow GC
	defer func() {
		// Clear the slice to release memory
		structlogs = nil
	}()

	// Send for direct insertion
	if err := p.insertStructlogs(ctx, structlogs); err != nil {
		common.TransactionsProcessed.WithLabelValues(p.network.Name, "structlog", "failed").Inc()

		return 0, fmt.Errorf("failed to insert structlogs: %w", err)
	}

	// Record success metrics
	common.TransactionsProcessed.WithLabelValues(p.network.Name, "structlog", "success").Inc()

	return structlogCount, nil
}

// ProcessTransaction processes a transaction using memory-efficient channel-based batching.
func (p *Processor) ProcessTransaction(ctx context.Context, block *types.Block, index int, tx *types.Transaction) (int, error) {
	// Get trace from execution node
	trace, err := p.getTransactionTrace(ctx, tx, block)
	if err != nil {
		return 0, fmt.Errorf("failed to get trace: %w", err)
	}

	totalCount := len(trace.Structlogs)

	// Compute actual gas used for each structlog
	gasUsed := ComputeGasUsed(trace.Structlogs)

	// Compute self gas (excludes child frame gas for CALL/CREATE opcodes)
	gasSelf := ComputeGasSelf(trace.Structlogs, gasUsed)

	// Initialize call frame tracker
	callTracker := NewCallTracker()

	// Pre-compute CREATE/CREATE2 addresses from trace stack
	createAddresses := ComputeCreateAddresses(trace.Structlogs)

	// Check if this is a big transaction and register if needed
	if totalCount >= p.bigTxManager.GetThreshold() {
		p.bigTxManager.RegisterBigTransaction(tx.Hash().String(), p)
		defer p.bigTxManager.UnregisterBigTransaction(tx.Hash().String())

		p.log.WithFields(logrus.Fields{
			"tx_hash":           tx.Hash().String(),
			"structlog_count":   totalCount,
			"current_big_count": p.bigTxManager.currentBigCount.Load(),
		}).Info("Processing big transaction")
	}

	chunkSize := p.config.ChunkSize
	if chunkSize == 0 {
		chunkSize = 10_000 // Default
	}

	// Buffered channel holds configured number of chunks
	bufferSize := p.config.ChannelBufferSize
	if bufferSize == 0 {
		bufferSize = 2 // Default
	}

	batchChan := make(chan []Structlog, bufferSize)
	errChan := make(chan error, 1)

	// Consumer goroutine - inserts to ClickHouse
	go func() {
		inserted := 0

		for batch := range batchChan {
			if err := p.insertStructlogs(ctx, batch); err != nil {
				errChan <- fmt.Errorf("failed to insert at %d: %w", inserted, err)

				return
			}

			inserted += len(batch)

			// Log progress for large transactions
			progressThreshold := p.config.ProgressLogThreshold
			if progressThreshold == 0 {
				progressThreshold = 100_000 // Default
			}

			if totalCount > progressThreshold && inserted%progressThreshold < chunkSize {
				p.log.WithFields(logrus.Fields{
					"tx_hash":  tx.Hash(),
					"progress": fmt.Sprintf("%d/%d", inserted, totalCount),
				}).Debug("Processing large transaction")
			}
		}

		errChan <- nil
	}()

	// Producer - convert and send batches
	batch := make([]Structlog, 0, chunkSize)

	for i := 0; i < totalCount; i++ {
		// Track call frame based on depth changes
		frameID, framePath := callTracker.ProcessDepthChange(trace.Structlogs[i].Depth)

		// Convert structlog
		batch = append(batch, Structlog{
			UpdatedDateTime:        NewClickHouseTime(time.Now()),
			BlockNumber:            block.Number().Uint64(),
			TransactionHash:        tx.Hash().String(),
			TransactionIndex:       uint32(index), //nolint:gosec // index is bounded by block.Transactions() length
			TransactionGas:         trace.Gas,
			TransactionFailed:      trace.Failed,
			TransactionReturnValue: trace.ReturnValue,
			Index:                  uint32(i), //nolint:gosec // index is bounded by structlogs length
			ProgramCounter:         trace.Structlogs[i].PC,
			Operation:              trace.Structlogs[i].Op,
			Gas:                    trace.Structlogs[i].Gas,
			GasCost:                trace.Structlogs[i].GasCost,
			GasUsed:                gasUsed[i],
			GasSelf:                gasSelf[i],
			Depth:                  trace.Structlogs[i].Depth,
			ReturnData:             trace.Structlogs[i].ReturnData,
			Refund:                 trace.Structlogs[i].Refund,
			Error:                  trace.Structlogs[i].Error,
			CallToAddress:          p.extractCallAddressWithCreate(&trace.Structlogs[i], i, createAddresses),
			CallFrameID:            frameID,
			CallFramePath:          framePath,
			MetaNetworkID:          p.network.ID,
			MetaNetworkName:        p.network.Name,
		})

		// CRITICAL: Free original trace data immediately
		trace.Structlogs[i] = execution.StructLog{}

		// Send full batch
		if len(batch) == chunkSize {
			select {
			case batchChan <- batch:
				batch = make([]Structlog, 0, chunkSize)
			case <-ctx.Done():
				close(batchChan)

				return 0, ctx.Err()
			}
		}
	}

	// Clear trace reference to help GC
	trace = nil

	// Send final batch if any
	if len(batch) > 0 {
		select {
		case batchChan <- batch:
		case <-ctx.Done():
			close(batchChan)

			return 0, ctx.Err()
		}
	}

	// Signal completion and wait
	close(batchChan)

	// Wait for consumer to finish
	if err := <-errChan; err != nil {
		return 0, err
	}

	// Record success metrics
	common.TransactionsProcessed.WithLabelValues(p.network.Name, "structlog", "success").Inc()

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
	processCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Get transaction trace
	trace, err := node.DebugTraceTransaction(processCtx, tx.Hash().String(), block.Number(), execution.StackTraceOptions())
	if err != nil {
		return nil, fmt.Errorf("failed to trace transaction: %w", err)
	}

	return trace, nil
}

// extractCallAddress extracts the call address from a structlog if it's a CALL-type operation.
// Handles CALL, CALLCODE, DELEGATECALL, and STATICCALL opcodes.
// For CREATE/CREATE2, use extractCallAddressWithCreate instead.
func (p *Processor) extractCallAddress(structLog *execution.StructLog) *string {
	if structLog.Stack == nil || len(*structLog.Stack) < 2 {
		return nil
	}

	switch structLog.Op {
	case "CALL", "CALLCODE":
		// Stack: [gas, addr, value, argsOffset, argsSize, retOffset, retSize]
		stackValue := (*structLog.Stack)[len(*structLog.Stack)-2]

		return &stackValue
	case "DELEGATECALL", "STATICCALL":
		// Stack: [gas, addr, argsOffset, argsSize, retOffset, retSize]
		stackValue := (*structLog.Stack)[len(*structLog.Stack)-2]

		return &stackValue
	default:
		return nil
	}
}

// extractCallAddressWithCreate extracts the call address, using createAddresses map for CREATE/CREATE2 opcodes.
func (p *Processor) extractCallAddressWithCreate(structLog *execution.StructLog, index int, createAddresses map[int]*string) *string {
	// For CREATE/CREATE2, use the pre-computed address from the trace
	if structLog.Op == "CREATE" || structLog.Op == "CREATE2" {
		if createAddresses != nil {
			return createAddresses[index]
		}

		return nil
	}

	return p.extractCallAddress(structLog)
}

// ComputeCreateAddresses pre-computes the created contract addresses for all CREATE/CREATE2 opcodes.
// It scans the trace and extracts addresses from the stack when each CREATE's constructor returns.
// The returned map contains opcode index -> created address (only for CREATE/CREATE2 opcodes).
func ComputeCreateAddresses(structlogs []execution.StructLog) map[int]*string {
	result := make(map[int]*string)

	// Track pending CREATE operations: (index, depth)
	type pendingCreate struct {
		index int
		depth uint64
	}

	var pending []pendingCreate

	for i, log := range structlogs {
		// Resolve pending CREATEs that have completed.
		// A CREATE at depth D completes when we see an opcode at depth <= D
		// (either immediately if CREATE failed, or after constructor returns).
		for len(pending) > 0 {
			last := pending[len(pending)-1]

			// If current opcode is at or below CREATE's depth and it's not the CREATE itself
			if log.Depth <= last.depth && i > last.index {
				// Extract address from top of stack (created address or 0 if failed)
				if log.Stack != nil && len(*log.Stack) > 0 {
					addr := (*log.Stack)[len(*log.Stack)-1]
					result[last.index] = &addr
				}

				pending = pending[:len(pending)-1]
			} else {
				break
			}
		}

		// Track new CREATE/CREATE2
		if log.Op == "CREATE" || log.Op == "CREATE2" {
			pending = append(pending, pendingCreate{index: i, depth: log.Depth})
		}
	}

	return result
}

// ExtractStructlogs extracts structlog data from a transaction without inserting to database.
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
		// Compute actual gas used for each structlog
		gasUsed := ComputeGasUsed(trace.Structlogs)

		// Compute self gas (excludes child frame gas for CALL/CREATE opcodes)
		gasSelf := ComputeGasSelf(trace.Structlogs, gasUsed)

		// Initialize call frame tracker
		callTracker := NewCallTracker()

		// Pre-compute CREATE/CREATE2 addresses from trace stack
		createAddresses := ComputeCreateAddresses(trace.Structlogs)

		// Pre-allocate slice for better memory efficiency
		structlogs = make([]Structlog, 0, len(trace.Structlogs))

		for i, structLog := range trace.Structlogs {
			// Track call frame based on depth changes
			frameID, framePath := callTracker.ProcessDepthChange(structLog.Depth)

			row := Structlog{
				UpdatedDateTime:        NewClickHouseTime(time.Now()),
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
				GasUsed:                gasUsed[i],
				GasSelf:                gasSelf[i],
				Depth:                  structLog.Depth,
				ReturnData:             structLog.ReturnData,
				Refund:                 structLog.Refund,
				Error:                  structLog.Error,
				CallToAddress:          p.extractCallAddressWithCreate(&structLog, i, createAddresses),
				CallFrameID:            frameID,
				CallFramePath:          framePath,
				MetaNetworkID:          p.network.ID,
				MetaNetworkName:        p.network.Name,
			}

			structlogs = append(structlogs, row)
		}

		// Clear the original trace data to free memory
		trace.Structlogs = nil
	}

	return structlogs, nil
}
