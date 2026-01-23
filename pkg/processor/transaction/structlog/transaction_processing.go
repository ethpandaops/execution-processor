package structlog

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/sirupsen/logrus"

	pcommon "github.com/ethpandaops/execution-processor/pkg/common"
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
	MetaNetworkName string   `json:"meta_network_name"`
}

// isCallOpcode returns true if the opcode initiates a call that creates a child frame.
// Note: CREATE/CREATE2 always execute code (constructor), so they always increase depth.
// CALL-type opcodes may target EOAs (no code) or precompiles (special handling).
func isCallOpcode(op string) bool {
	switch op {
	case OpcodeCALL, OpcodeCALLCODE, OpcodeDELEGATECALL, OpcodeSTATICCALL:
		return true
	default:
		return false
	}
}

// precompileAddresses contains all known EVM precompile addresses.
//
// Precompile calls don't appear in trace_transaction results (unlike EOA calls which do).
// This is used to distinguish EOA calls from precompile calls when depth doesn't increase.
//
// Note: Low addresses like 0x5c, 0x60, etc. are NOT precompiles - they're real EOAs/contracts
// deployed early in Ethereum's history. Only the addresses below are actual precompiles.
//
// Addresses sourced from go-ethereum PrecompiledContractsOsaka (superset of all forks):
// https://github.com/ethereum/go-ethereum/blob/master/core/vm/contracts.go
//
// We cannot import go-ethereum directly because it depends on github.com/holiman/bloomfilter/v2,
// which conflicts with Erigon's fork (github.com/AskAlexSharov/bloomfilter/v2) when this package
// is embedded in Erigon. The two bloomfilter versions have incompatible APIs.
var precompileAddresses = map[string]bool{
	"0x0000000000000000000000000000000000000001": true, // ecrecover
	"0x0000000000000000000000000000000000000002": true, // sha256
	"0x0000000000000000000000000000000000000003": true, // ripemd160
	"0x0000000000000000000000000000000000000004": true, // identity (dataCopy)
	"0x0000000000000000000000000000000000000005": true, // modexp (bigModExp)
	"0x0000000000000000000000000000000000000006": true, // bn256Add (ecAdd)
	"0x0000000000000000000000000000000000000007": true, // bn256ScalarMul (ecMul)
	"0x0000000000000000000000000000000000000008": true, // bn256Pairing (ecPairing)
	"0x0000000000000000000000000000000000000009": true, // blake2f
	"0x000000000000000000000000000000000000000a": true, // kzgPointEvaluation (EIP-4844, Cancun)
	"0x000000000000000000000000000000000000000b": true, // bls12381G1Add (EIP-2537, Osaka)
	"0x000000000000000000000000000000000000000c": true, // bls12381G1MultiExp (EIP-2537, Osaka)
	"0x000000000000000000000000000000000000000d": true, // bls12381G2Add (EIP-2537, Osaka)
	"0x000000000000000000000000000000000000000e": true, // bls12381G2MultiExp (EIP-2537, Osaka)
	"0x000000000000000000000000000000000000000f": true, // bls12381Pairing (EIP-2537, Osaka)
	"0x0000000000000000000000000000000000000010": true, // bls12381MapG1 (EIP-2537, Osaka)
	"0x0000000000000000000000000000000000000011": true, // bls12381MapG2 (EIP-2537, Osaka)
	"0x0000000000000000000000000000000000000100": true, // p256Verify (EIP-7212, Osaka)
}

// isPrecompile returns true if the address is a known EVM precompile.
// Precompile calls don't appear in trace_transaction results (unlike EOA calls which do).
func isPrecompile(addr string) bool {
	// Normalize to lowercase with 0x prefix and full 40 hex chars
	hex := strings.TrimPrefix(strings.ToLower(addr), "0x")

	for len(hex) < 40 {
		hex = "0" + hex
	}

	return precompileAddresses["0x"+hex]
}

// ProcessSingleTransaction processes a single transaction and inserts its structlogs directly to ClickHouse.
func (p *Processor) ProcessSingleTransaction(ctx context.Context, block execution.Block, index int, tx execution.Transaction) (int, error) {
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
		pcommon.TransactionsProcessed.WithLabelValues(p.network.Name, "structlog", "failed").Inc()

		return 0, fmt.Errorf("failed to insert structlogs: %w", err)
	}

	// Record success metrics
	pcommon.TransactionsProcessed.WithLabelValues(p.network.Name, "structlog", "success").Inc()

	return structlogCount, nil
}

// ProcessTransaction processes a transaction using memory-efficient channel-based batching.
func (p *Processor) ProcessTransaction(ctx context.Context, block execution.Block, index int, tx execution.Transaction) (int, error) {
	// Get trace from execution node
	trace, err := p.getTransactionTrace(ctx, tx, block)
	if err != nil {
		return 0, fmt.Errorf("failed to get trace: %w", err)
	}

	totalCount := len(trace.Structlogs)

	// Check if GasUsed is pre-computed by the tracer (embedded mode).
	// In embedded mode, skip the post-processing computation.
	// In RPC mode, compute GasUsed from gas differences.
	precomputedGasUsed := hasPrecomputedGasUsed(trace.Structlogs)

	var gasUsed []uint64
	if !precomputedGasUsed {
		gasUsed = ComputeGasUsed(trace.Structlogs)
	}

	// Compute self gas (excludes child frame gas for CALL/CREATE opcodes)
	gasSelf := ComputeGasSelf(trace.Structlogs, gasUsed)

	// Initialize call frame tracker
	callTracker := NewCallTracker()

	// Check if CREATE/CREATE2 addresses are pre-computed by the tracer (embedded mode).
	// In embedded mode, skip the multi-pass ComputeCreateAddresses scan.
	precomputedCreateAddresses := hasPrecomputedCreateAddresses(trace.Structlogs)

	var createAddresses map[int]*string
	if !precomputedCreateAddresses {
		// Pre-compute CREATE/CREATE2 addresses from trace stack (RPC mode)
		createAddresses = ComputeCreateAddresses(trace.Structlogs)
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

		// Get GasUsed: use pre-computed value from tracer (embedded) or computed value (RPC).
		var gasUsedValue uint64
		if precomputedGasUsed {
			gasUsedValue = structLog.GasUsed
		} else {
			gasUsedValue = gasUsed[i]
		}

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
			GasUsed:                gasUsedValue,
			GasSelf:                gasSelf[i],
			Depth:                  structLog.Depth,
			ReturnData:             structLog.ReturnData,
			Refund:                 structLog.Refund,
			Error:                  structLog.Error,
			CallToAddress:          callToAddr,
			CallFrameID:            frameID,
			CallFramePath:          framePath,
			MetaNetworkName:        p.network.Name,
		})

		// Check for EOA call: CALL-type opcode where depth stays the same (immediate return)
		// and target is not a precompile (precompiles don't create trace frames)
		if isCallOpcode(structLog.Op) && callToAddr != nil {
			isEOACall := false

			if i+1 < totalCount {
				// Next opcode exists - check if depth stayed the same
				// Depth increase = entered contract code (not EOA)
				// Depth decrease = call returned/failed (not EOA)
				// Depth same = called EOA or precompile (immediate return)
				nextDepth := trace.Structlogs[i+1].Depth
				if nextDepth == structLog.Depth && !isPrecompile(*callToAddr) {
					isEOACall = true
				}
			}
			// Note: If last opcode is a CALL, we can't determine if it's EOA
			// because we don't have a next opcode to compare depth with.
			// These are typically failed calls at end of execution.

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
	pcommon.TransactionsProcessed.WithLabelValues(p.network.Name, "structlog", "success").Inc()

	return totalCount, nil
}

// getTransactionTrace gets the trace for a transaction.
func (p *Processor) getTransactionTrace(ctx context.Context, tx execution.Transaction, block execution.Block) (*execution.TraceTransaction, error) {
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

// extractCallAddress extracts the call address from a structlog for CALL-family opcodes.
//
// Supports two modes for backward compatibility:
//   - Embedded mode: CallToAddress is pre-populated by the tracer, use directly.
//   - RPC mode: CallToAddress is nil, extract from Stack[len-2] for CALL-family opcodes.
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
	// Embedded mode: use pre-extracted CallToAddress
	if structLog.CallToAddress != nil {
		return structLog.CallToAddress
	}

	// RPC mode fallback: extract from Stack for CALL-family opcodes
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
func (p *Processor) ExtractStructlogs(ctx context.Context, block execution.Block, index int, tx execution.Transaction) ([]Structlog, error) {
	start := time.Now()

	defer func() {
		duration := time.Since(start)
		pcommon.TransactionProcessingDuration.WithLabelValues(p.network.Name, "structlog").Observe(duration.Seconds())
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
		// Check if GasUsed is pre-computed by the tracer (embedded mode).
		// In embedded mode, extract values from structlogs.
		// In RPC mode, compute GasUsed from gas differences.
		precomputedGasUsed := hasPrecomputedGasUsed(trace.Structlogs)

		var gasUsed []uint64
		if precomputedGasUsed {
			// Extract pre-computed GasUsed values from structlogs (embedded mode)
			gasUsed = make([]uint64, len(trace.Structlogs))
			for i := range trace.Structlogs {
				gasUsed[i] = trace.Structlogs[i].GasUsed
			}
		} else {
			// Compute GasUsed from gas differences (RPC mode)
			gasUsed = ComputeGasUsed(trace.Structlogs)
		}

		// Compute self gas (excludes child frame gas for CALL/CREATE opcodes)
		gasSelf := ComputeGasSelf(trace.Structlogs, gasUsed)

		// Initialize call frame tracker
		callTracker := NewCallTracker()

		// Check if CREATE/CREATE2 addresses are pre-computed by the tracer (embedded mode).
		precomputedCreateAddresses := hasPrecomputedCreateAddresses(trace.Structlogs)

		var createAddresses map[int]*string
		if !precomputedCreateAddresses {
			// Pre-compute CREATE/CREATE2 addresses from trace stack (RPC mode)
			createAddresses = ComputeCreateAddresses(trace.Structlogs)
		}

		// Pre-allocate slice for better memory efficiency
		structlogs = make([]Structlog, 0, len(trace.Structlogs))

		for i, structLog := range trace.Structlogs {
			// Track call frame based on depth changes
			frameID, framePath := callTracker.ProcessDepthChange(structLog.Depth)

			callToAddr := p.extractCallAddressWithCreate(&structLog, i, createAddresses)

			// Get GasUsed: use pre-computed value from tracer (embedded) or computed value (RPC).
			var gasUsedValue uint64
			if precomputedGasUsed {
				gasUsedValue = structLog.GasUsed
			} else {
				gasUsedValue = gasUsed[i]
			}

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
				GasUsed:                gasUsedValue,
				GasSelf:                gasSelf[i],
				Depth:                  structLog.Depth,
				ReturnData:             structLog.ReturnData,
				Refund:                 structLog.Refund,
				Error:                  structLog.Error,
				CallToAddress:          callToAddr,
				CallFrameID:            frameID,
				CallFramePath:          framePath,
				MetaNetworkName:        p.network.Name,
			}

			structlogs = append(structlogs, row)

			// Check for EOA call: CALL-type opcode where depth stays the same (immediate return)
			// and target is not a precompile (precompiles don't create trace frames)
			if isCallOpcode(structLog.Op) && callToAddr != nil {
				isEOACall := false

				if i+1 < len(trace.Structlogs) {
					// Next opcode exists - check if depth stayed the same
					// Depth increase = entered contract code (not EOA)
					// Depth decrease = call returned/failed (not EOA)
					// Depth same = called EOA or precompile (immediate return)
					nextDepth := trace.Structlogs[i+1].Depth
					if nextDepth == structLog.Depth && !isPrecompile(*callToAddr) {
						isEOACall = true
					}
				}
				// Note: If last opcode is a CALL, we can't determine if it's EOA
				// because we don't have a next opcode to compare depth with.
				// These are typically failed calls at end of execution.

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
