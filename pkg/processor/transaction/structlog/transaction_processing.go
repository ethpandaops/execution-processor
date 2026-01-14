package structlog

import (
	"context"
	"fmt"
	"time"

	ethcommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

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
	Gas                    uint64         `json:"gas"`
	GasCost                uint64         `json:"gas_cost"`
	GasUsed                uint64         `json:"gas_used"`
	Depth                  uint64         `json:"depth"`
	ReturnData             *string        `json:"return_data"`
	Refund                 *uint64        `json:"refund"`
	Error                  *string        `json:"error"`
	CallToAddress          *string        `json:"call_to_address"`
	CallFrameID            uint32         `json:"call_frame_id"`
	CallFramePath          []uint32       `json:"call_frame_path"`
	MetaNetworkID          int32          `json:"meta_network_id"`
	MetaNetworkName        string         `json:"meta_network_name"`
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

	// Initialize call frame tracker
	callTracker := NewCallTracker()

	// Fetch CREATE address from receipt if trace contains CREATE/CREATE2 opcodes
	var createAddress *string

	if hasCreateOpcode(trace.Structlogs) {
		var err error

		createAddress, err = p.fetchCreateAddress(ctx, tx.Hash().String())
		if err != nil {
			// Continue without CREATE address - not fatal.
			p.log.WithError(err).Warn("Failed to fetch CREATE address from receipt")
		}
	}

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
			Depth:                  trace.Structlogs[i].Depth,
			ReturnData:             trace.Structlogs[i].ReturnData,
			Refund:                 trace.Structlogs[i].Refund,
			Error:                  trace.Structlogs[i].Error,
			CallToAddress:          p.extractCallAddressWithCreate(&trace.Structlogs[i], createAddress),
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

// extractCallAddressWithCreate extracts the call address, using createAddress for CREATE/CREATE2 opcodes.
func (p *Processor) extractCallAddressWithCreate(structLog *execution.StructLog, createAddress *string) *string {
	// For CREATE/CREATE2, use the address from the receipt
	if structLog.Op == "CREATE" || structLog.Op == "CREATE2" {
		return createAddress
	}

	return p.extractCallAddress(structLog)
}

// hasCreateOpcode checks if any structlog contains a CREATE or CREATE2 opcode.
func hasCreateOpcode(structlogs []execution.StructLog) bool {
	for i := range structlogs {
		if structlogs[i].Op == "CREATE" || structlogs[i].Op == "CREATE2" {
			return true
		}
	}

	return false
}

// fetchCreateAddress fetches the contract address from the transaction receipt.
// Returns nil if the receipt has no contract address (not a contract creation tx).
func (p *Processor) fetchCreateAddress(ctx context.Context, txHash string) (*string, error) {
	node := p.pool.GetHealthyExecutionNode()
	if node == nil {
		return nil, fmt.Errorf("no healthy execution node available")
	}

	receipt, err := node.TransactionReceipt(ctx, txHash)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch transaction receipt: %w", err)
	}

	// Check if contract was created (ContractAddress is non-zero)
	if receipt.ContractAddress == (ethcommon.Address{}) {
		return nil, nil //nolint:nilnil // nil address with nil error is valid - means no contract created
	}

	addr := receipt.ContractAddress.Hex()

	return &addr, nil
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

		// Initialize call frame tracker
		callTracker := NewCallTracker()

		// Fetch CREATE address from receipt if trace contains CREATE/CREATE2 opcodes
		var createAddress *string

		if hasCreateOpcode(trace.Structlogs) {
			var err error

			createAddress, err = p.fetchCreateAddress(ctx, tx.Hash().String())
			if err != nil {
				p.log.WithError(err).Warn("Failed to fetch CREATE address from receipt")
				// Continue without CREATE address - not fatal
			}
		}

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
				Depth:                  structLog.Depth,
				ReturnData:             structLog.ReturnData,
				Refund:                 structLog.Refund,
				Error:                  structLog.Error,
				CallToAddress:          p.extractCallAddressWithCreate(&structLog, createAddress),
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
