package structlog

import (
	"math"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

// EVM opcode name constants used for opcode identification and benchmarking.
const (
	opCALL         = "CALL"
	opSTATICCALL   = "STATICCALL"
	opDELEGATECALL = "DELEGATECALL"
	opCALLCODE     = "CALLCODE"
	opPUSH1        = "PUSH1"
	opRETURN       = "RETURN"
	opSTOP         = "STOP"
)

// isCallOpcode checks if the opcode is a call-family opcode that has a target address.
//
// Call-family opcodes are:
//   - CALL: Standard external call
//   - STATICCALL: Read-only external call (no state modifications)
//   - DELEGATECALL: Call using caller's context (msg.sender, msg.value preserved)
//   - CALLCODE: Deprecated, similar to DELEGATECALL but uses callee's msg.value
//
// For these opcodes, the target address is at stack position len-2 (second from top).
func isCallOpcode(op string) bool {
	return op == opCALL || op == opSTATICCALL || op == opDELEGATECALL || op == opCALLCODE
}

// hasPrecomputedGasUsed detects whether GasUsed values are pre-computed by the tracer.
//
// In embedded mode, the tracer computes GasUsed inline during trace capture,
// populating this field with non-zero values. In RPC mode, GasUsed is always 0
// and must be computed post-hoc using ComputeGasUsed().
//
// This enables backward compatibility: execution-processor works with both
// embedded mode (optimized, pre-computed) and RPC mode (legacy, post-computed).
func hasPrecomputedGasUsed(structlogs []execution.StructLog) bool {
	if len(structlogs) == 0 {
		return false
	}

	// Check first structlog - if GasUsed > 0, tracer pre-computed values.
	return structlogs[0].GasUsed > 0
}

// ComputeGasUsed calculates the actual gas consumed for each structlog using
// the difference between consecutive gas values at the same depth level.
//
// Returns a slice of gasUsed values corresponding to each structlog index.
// For opcodes that are the last in their call context (before returning to parent),
// the pre-calculated GasCost is returned since we cannot compute actual cost
// across call boundaries.
func ComputeGasUsed(structlogs []execution.StructLog) []uint64 {
	if len(structlogs) == 0 {
		return nil
	}

	gasUsed := make([]uint64, len(structlogs))

	// Initialize all with pre-calculated cost (fallback for last opcodes in each call context)
	for i := range structlogs {
		gasUsed[i] = structlogs[i].GasCost
	}

	// pendingIdx[depth] = index into structlogs for the pending opcode at that depth
	// -1 means no pending opcode at that depth
	pendingIdx := make([]int, 0, 16)

	for i := range structlogs {
		// Safe conversion: EVM depth is capped at 1024, but we guard against overflow
		depthU64 := structlogs[i].Depth
		if depthU64 > math.MaxInt {
			depthU64 = math.MaxInt
		}

		depth := int(depthU64) //nolint:gosec // overflow checked above

		// Ensure slice has enough space for this depth
		for len(pendingIdx) <= depth {
			pendingIdx = append(pendingIdx, -1)
		}

		// Clear pending indices from deeper levels (we've returned from calls).
		// These are the last opcodes in child call contexts - they keep
		// the pre-calculated GasCost as gasUsed since we cannot compute
		// actual cost across call boundaries.
		for d := len(pendingIdx) - 1; d > depth; d-- {
			pendingIdx[d] = -1
		}

		// Update gasUsed for pending log at current depth
		if prevIdx := pendingIdx[depth]; prevIdx >= 0 && prevIdx < len(structlogs) {
			gasUsed[prevIdx] = structlogs[prevIdx].Gas - structlogs[i].Gas
		}

		// Store current log's index as pending at this depth
		pendingIdx[depth] = i
	}

	return gasUsed
}
