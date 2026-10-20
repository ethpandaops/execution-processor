package structlog

import (
	"context"
	"fmt"
	"strings"
	"time"

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

	// Synthetic marks the row added for a CALL to an account without code; it shares the CALL's index.
	Synthetic bool `json:"synthetic"`
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

// IsPrecompile returns true if the address is a known EVM precompile.
// Precompile calls don't appear in trace_transaction results (unlike EOA calls which do).
func IsPrecompile(addr string) bool {
	// Normalize to lowercase with 0x prefix and full 40 hex chars
	hex := strings.TrimPrefix(strings.ToLower(addr), "0x")

	for len(hex) < 40 {
		hex = "0" + hex
	}

	return precompileAddresses["0x"+hex]
}

// ProcessSingleTransaction processes a single transaction and inserts its structlogs directly to ClickHouse.
func (p *Processor) ProcessSingleTransaction(ctx context.Context, block execution.Block, index int, tx execution.Transaction) (int, error) {
	return p.ProcessTransaction(ctx, block, index, tx)
}

// ProcessTransaction traces a transaction and inserts its structlog rows to ClickHouse in bounded batches.
func (p *Processor) ProcessTransaction(ctx context.Context, block execution.Block, index int, tx execution.Transaction) (int, error) {
	batch := newRowBatch(p.insertBatchSize(), func(rows []Structlog) error {
		return p.insertStructlogs(ctx, rows)
	})

	rowCount, err := p.buildRows(ctx, block, index, tx, batch.add)
	if err == nil {
		err = batch.flush()
	}

	if err != nil {
		pcommon.TransactionsProcessed.WithLabelValues(p.network.Name, "structlog", "failed").Inc()

		return 0, err
	}

	pcommon.TransactionsProcessed.WithLabelValues(p.network.Name, "structlog", "success").Inc()

	return rowCount, nil
}

// ExtractStructlogs traces a transaction and returns all of its structlog rows without inserting them.
func (p *Processor) ExtractStructlogs(ctx context.Context, block execution.Block, index int, tx execution.Transaction) ([]Structlog, error) {
	var rows []Structlog

	if _, err := p.buildRows(ctx, block, index, tx, func(row *Structlog) error {
		rows = append(rows, *row)

		return nil
	}); err != nil {
		return nil, err
	}

	return rows, nil
}

// buildRows traces tx and passes each structlog row to emit, streaming the trace when the node supports it.
func (p *Processor) buildRows(ctx context.Context, block execution.Block, index int, tx execution.Transaction, emit func(*Structlog) error) (int, error) {
	node := p.pool.GetHealthyExecutionNode()
	if node == nil {
		return 0, fmt.Errorf("no healthy execution node available")
	}

	traceCtx, cancel := context.WithTimeout(ctx, p.traceTimeout())
	defer cancel()

	base := Structlog{
		UpdatedDateTime:  NewClickHouseTime(time.Now()),
		BlockNumber:      block.Number().Uint64(),
		TransactionHash:  tx.Hash().String(),
		TransactionIndex: uint32(index), //nolint:gosec // index is bounded by block.Transactions() length
		MetaNetworkName:  p.network.Name,
	}

	if streamer, ok := node.(execution.StructLogStreamer); ok {
		return p.buildRowsStreamed(traceCtx, streamer, base, emit)
	}

	trace, err := node.DebugTraceTransaction(traceCtx, base.TransactionHash, block.Number(), execution.StackTraceOptions())
	if err != nil {
		return 0, fmt.Errorf("failed to trace transaction: %w", err)
	}

	base.TransactionGas = trace.Gas
	base.TransactionFailed = trace.Failed
	base.TransactionReturnValue = trace.ReturnValue

	builder := newRowBuilder(rowBuilderConfig{
		base:               base,
		emit:               emit,
		callToAddress:      p.extractCallAddress,
		precomputedGasUsed: hasPrecomputedGasUsed(trace.Structlogs),
	})

	for i := range trace.Structlogs {
		if err := builder.add(&trace.Structlogs[i]); err != nil {
			return 0, err
		}

		trace.Structlogs[i] = execution.StructLog{}
	}

	if err := builder.finish(); err != nil {
		return 0, err
	}

	return builder.emitted, nil
}

// buildRowsStreamed takes call frames from one trace and streams structlogs without return data from another.
func (p *Processor) buildRowsStreamed(ctx context.Context, streamer execution.StructLogStreamer, base Structlog, emit func(*Structlog) error) (int, error) {
	frames, err := streamer.TraceCallFrames(ctx, base.TransactionHash)
	if err != nil {
		return 0, fmt.Errorf("failed to trace call frames: %w", err)
	}

	base.TransactionGas = frames.Gas
	base.TransactionFailed = frames.Failed
	base.TransactionReturnValue = frames.ReturnValue

	builder := newRowBuilder(rowBuilderConfig{
		base:          base,
		emit:          emit,
		callToAddress: p.extractCallAddress,
		frames:        frames.Frames,
		useFrames:     true,
	})

	summary, err := streamer.StreamStructLogs(ctx, base.TransactionHash, execution.CallFrameTraceOptions(), builder.add)
	if err != nil {
		return 0, fmt.Errorf("failed to stream structlogs: %w", err)
	}

	if err := builder.finish(); err != nil {
		return 0, err
	}

	if summary.Gas != frames.Gas || summary.Failed != frames.Failed || !equalStringPtr(summary.ReturnValue, frames.ReturnValue) {
		return 0, fmt.Errorf("%w: structlog trace reports gas=%d failed=%t, call frames gas=%d failed=%t",
			ErrCallFramesMismatch, summary.Gas, summary.Failed, frames.Gas, frames.Failed)
	}

	return builder.emitted, nil
}

func equalStringPtr(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}

	return *a == *b
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
// Supports two modes:
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
