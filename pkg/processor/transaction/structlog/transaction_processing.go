package structlog

import (
	"context"
	"fmt"
	"strings"
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

// isCallOpcode returns true if the opcode initiates a call that creates a child frame.
// Note: CREATE/CREATE2 always execute code (constructor), so they always increase depth.
// CALL-type opcodes may target EOAs (no code) or precompiles (special handling).
func isCallOpcode(op string) bool {
	switch op {
	case "CALL", "CALLCODE", "DELEGATECALL", "STATICCALL":
		return true
	default:
		return false
	}
}

// isPrecompile returns true if the address is likely a precompile contract.
// Precompile calls don't create trace frames (unlike EOA calls which do).
// This is used to distinguish EOA calls from precompile calls when depth doesn't increase.
//
// Uses a heuristic: any address < 0x10000 is considered a precompile.
// This covers all known precompiles (0x01-0x11, 0x100) and future-proofs for new ones.
//
// This is safe because the probability of an EOA at such a low address is negligible
// (EOA addresses are keccak256 hashes, odds of being < 0x10000 are ≈ 2^-144).
func isPrecompile(addr string) bool {
	// Remove 0x prefix and parse as hex
	hex := strings.TrimPrefix(strings.ToLower(addr), "0x")

	// Pad to 40 chars if needed (shouldn't happen with formatAddress, but be safe)
	for len(hex) < 40 {
		hex = "0" + hex
	}

	// Check if first 36 chars (18 bytes) are all zeros
	// This means the address is < 0x10000 (fits in last 2 bytes)
	for i := 0; i < 36; i++ {
		if hex[i] != '0' {
			return false
		}
	}

	return true
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

	// Helper to send batch when full
	sendBatchIfFull := func() error {
		if len(batch) >= chunkSize {
			select {
			case batchChan <- batch:
				batch = make([]Structlog, 0, chunkSize)
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		return nil
	}

	for i := 0; i < totalCount; i++ {
		structLog := &trace.Structlogs[i]

		// Track call frame based on depth changes
		frameID, framePath := callTracker.ProcessDepthChange(structLog.Depth)

		callToAddr := p.extractCallAddressWithCreate(structLog, i, createAddresses)

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
			CallToAddress:          callToAddr,
			CallFrameID:            frameID,
			CallFramePath:          framePath,
			MetaNetworkID:          p.network.ID,
			MetaNetworkName:        p.network.Name,
		})

		// Check for EOA call: CALL-type opcode where next opcode doesn't increase depth
		// and target is not a precompile (precompiles don't create trace frames)
		if isCallOpcode(structLog.Op) && callToAddr != nil {
			isEOACall := false

			if i+1 < totalCount {
				// Next opcode exists - check if depth increased
				nextDepth := trace.Structlogs[i+1].Depth
				if nextDepth <= structLog.Depth {
					// Depth didn't increase - check if it's a precompile
					if !isPrecompile(*callToAddr) {
						isEOACall = true
					}
				}
			} else {
				// Last opcode is a CALL - if not precompile, must be EOA
				if !isPrecompile(*callToAddr) {
					isEOACall = true
				}
			}

			if isEOACall {
				// Emit synthetic structlog for EOA frame
				eoaFrameID, eoaFramePath := callTracker.IssueFrameID()

				batch = append(batch, Structlog{
					UpdatedDateTime:        NewClickHouseTime(time.Now()),
					BlockNumber:            block.Number().Uint64(),
					TransactionHash:        tx.Hash().String(),
					TransactionIndex:       uint32(index), //nolint:gosec // index is bounded by block.Transactions() length
					TransactionGas:         trace.Gas,
					TransactionFailed:      trace.Failed,
					TransactionReturnValue: trace.ReturnValue,
					Index:                  uint32(i), //nolint:gosec // Same index as parent CALL
					ProgramCounter:         0,         // No PC for EOA
					Operation:              "",        // Empty = synthetic EOA frame
					Gas:                    0,
					GasCost:                0,
					GasUsed:                0,
					GasSelf:                0,
					Depth:                  structLog.Depth + 1, // One level deeper than caller
					ReturnData:             nil,
					Refund:                 nil,
					Error:                  structLog.Error, // Inherit error if CALL failed
					CallToAddress:          callToAddr,      // The EOA address
					CallFrameID:            eoaFrameID,
					CallFramePath:          eoaFramePath,
					MetaNetworkID:          p.network.ID,
					MetaNetworkName:        p.network.Name,
				})
			}
		}

		// CRITICAL: Free original trace data immediately
		trace.Structlogs[i] = execution.StructLog{}

		// Send full batch
		if err := sendBatchIfFull(); err != nil {
			close(batchChan)

			return 0, err
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

// formatAddress normalizes an address to exactly 42 characters (0x + 40 hex).
//
// Background: The EVM is a 256-bit (32-byte) stack machine. ALL stack values are 32 bytes,
// including addresses. When execution clients like Erigon/Geth return debug traces, the
// stack array contains raw 32-byte values as hex strings (66 chars with 0x prefix).
//
// However, Ethereum addresses are only 160 bits (20 bytes, 40 hex chars). In EVM/ABI encoding,
// addresses are stored in the LOWER 160 bits of the 32-byte word (right-aligned, left-padded
// with zeros). For example, address 0x7a250d5630b4cf539739df2c5dacb4c659f2488d on the stack:
//
//	0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d
//	|-------- upper 12 bytes (zeros) --------||---- lower 20 bytes (address) ----|
//
// Some contracts may have non-zero upper bytes in the stack value. The EVM ignores these
// when interpreting the value as an address - only the lower 20 bytes are used.
//
// This function handles three cases:
//  1. Short addresses (e.g., "0x1" for precompiles): left-pad with zeros to 40 hex chars
//  2. Full 32-byte stack values (66 chars): extract rightmost 40 hex chars (lower 160 bits)
//  3. Normal 42-char addresses: return as-is
func formatAddress(addr string) string {
	// Remove 0x prefix if present
	hex := strings.TrimPrefix(addr, "0x")

	// If longer than 40 chars, extract the lower 20 bytes (rightmost 40 hex chars).
	// This handles raw 32-byte stack values from execution client traces.
	if len(hex) > 40 {
		hex = hex[len(hex)-40:]
	}

	// Left-pad with zeros to 40 chars if shorter (handles precompiles like 0x1),
	// then add 0x prefix
	return fmt.Sprintf("0x%040s", hex)
}

// extractCallAddress extracts the target address from a CALL-type opcode's stack.
// Handles CALL, CALLCODE, DELEGATECALL, and STATICCALL opcodes.
// For CREATE/CREATE2, use extractCallAddressWithCreate instead.
//
// Stack layout in Erigon/Geth debug traces:
//   - Array index 0 = bottom of stack (oldest value, first pushed)
//   - Array index len-1 = top of stack (newest value, first to be popped)
//
// When a CALL opcode executes, its arguments are at the top of the stack:
//
//	CALL/CALLCODE:        [..., retSize, retOffset, argsSize, argsOffset, value, addr, gas]
//	DELEGATECALL/STATICCALL: [..., retSize, retOffset, argsSize, argsOffset, addr, gas]
//	                                                                          ^     ^
//	                                                                       len-2  len-1
//
// The address is always at Stack[len-2] (second from top), regardless of how many
// other values exist below the CALL arguments on the stack.
//
// Note: The stack value is a raw 32-byte word. The formatAddress function extracts
// the actual 20-byte address from the lower 160 bits.
func (p *Processor) extractCallAddress(structLog *execution.StructLog) *string {
	if structLog.Stack == nil || len(*structLog.Stack) < 2 {
		return nil
	}

	switch structLog.Op {
	case "CALL", "CALLCODE", "DELEGATECALL", "STATICCALL":
		// Extract the raw 32-byte stack value at the address position (second from top).
		// formatAddress will normalize it to a proper 20-byte address.
		stackValue := (*structLog.Stack)[len(*structLog.Stack)-2]
		addr := formatAddress(stackValue)

		return &addr
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
					addr := formatAddress((*log.Stack)[len(*log.Stack)-1])
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

			callToAddr := p.extractCallAddressWithCreate(&structLog, i, createAddresses)

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
				CallToAddress:          callToAddr,
				CallFrameID:            frameID,
				CallFramePath:          framePath,
				MetaNetworkID:          p.network.ID,
				MetaNetworkName:        p.network.Name,
			}

			structlogs = append(structlogs, row)

			// Check for EOA call: CALL-type opcode where next opcode doesn't increase depth
			// and target is not a precompile (precompiles don't create trace frames)
			if isCallOpcode(structLog.Op) && callToAddr != nil {
				isEOACall := false

				if i+1 < len(trace.Structlogs) {
					// Next opcode exists - check if depth increased
					nextDepth := trace.Structlogs[i+1].Depth
					if nextDepth <= structLog.Depth {
						// Depth didn't increase - check if it's a precompile
						if !isPrecompile(*callToAddr) {
							isEOACall = true
						}
					}
				} else {
					// Last opcode is a CALL - if not precompile, must be EOA
					if !isPrecompile(*callToAddr) {
						isEOACall = true
					}
				}

				if isEOACall {
					// Emit synthetic structlog for EOA frame
					eoaFrameID, eoaFramePath := callTracker.IssueFrameID()

					eoaRow := Structlog{
						UpdatedDateTime:        NewClickHouseTime(time.Now()),
						BlockNumber:            block.Number().Uint64(),
						TransactionHash:        tx.Hash().String(),
						TransactionIndex:       uIndex,
						TransactionGas:         trace.Gas,
						TransactionFailed:      trace.Failed,
						TransactionReturnValue: trace.ReturnValue,
						Index:                  uint32(i), //nolint:gosec // Same index as parent CALL
						ProgramCounter:         0,         // No PC for EOA
						Operation:              "",        // Empty = synthetic EOA frame
						Gas:                    0,
						GasCost:                0,
						GasUsed:                0,
						GasSelf:                0,
						Depth:                  structLog.Depth + 1, // One level deeper than caller
						ReturnData:             nil,
						Refund:                 nil,
						Error:                  structLog.Error, // Inherit error if CALL failed
						CallToAddress:          callToAddr,      // The EOA address
						CallFrameID:            eoaFrameID,
						CallFramePath:          eoaFramePath,
						MetaNetworkID:          p.network.ID,
						MetaNetworkName:        p.network.Name,
					}

					structlogs = append(structlogs, eoaRow)
				}
			}
		}

		// Clear the original trace data to free memory
		trace.Structlogs = nil
	}

	return structlogs, nil
}
