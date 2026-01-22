package structlog

import (
	"math"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

// =============================================================================
// GAS FIELDS
// =============================================================================
//
// The structlog contains three gas-related fields:
//
// GasCost
//   Source: Directly from geth/erigon debug_traceTransaction response.
//   For non-CALL opcodes: The static cost charged for the opcode.
//   For CALL/CREATE opcodes: The gas stipend passed to the child frame.
//
// GasUsed
//   Source: Computed as gas[i] - gas[i+1] for consecutive opcodes at same depth.
//   For non-CALL opcodes: Actual gas consumed by the opcode.
//   For CALL/CREATE opcodes: Includes the call overhead PLUS all child frame gas.
//   Note: Summing gas_used across all opcodes double counts because CALL's
//   gas_used includes child gas, and children also report their own gas_used.
//
// GasSelf
//   Source: Computed as gas_used minus the sum of all child frame gas_used.
//   For non-CALL opcodes: Equal to gas_used.
//   For CALL/CREATE opcodes: Only the call overhead (warm/cold access, memory
//   expansion, value transfer) without child frame gas.
//   Summing gas_self across all opcodes gives total execution gas without
//   double counting.
//
// Example for a CALL opcode:
//   gas_cost = 7,351,321 (stipend passed to child)
//   gas_used = 23,858    (overhead 2,600 + child consumed 21,258)
//   gas_self = 2,600     (just the CALL overhead)
//
// =============================================================================

// Opcode constants for call and create operations.
const (
	OpcodeCALL         = "CALL"
	OpcodeCALLCODE     = "CALLCODE"
	OpcodeDELEGATECALL = "DELEGATECALL"
	OpcodeSTATICCALL   = "STATICCALL"
	OpcodeCREATE       = "CREATE"
	OpcodeCREATE2      = "CREATE2"
)

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

// ComputeGasSelf calculates the gas consumed by each opcode excluding child frame gas.
// For CALL/CREATE opcodes, this represents only the call overhead (warm/cold access,
// memory expansion, value transfer), not the gas consumed by child frames.
// For all other opcodes, this equals gasUsed.
//
// This is useful for gas analysis where you want to sum gas without double counting:
// sum(gasSelf) = total transaction execution gas (no double counting).
func ComputeGasSelf(structlogs []execution.StructLog, gasUsed []uint64) []uint64 {
	if len(structlogs) == 0 {
		return nil
	}

	gasSelf := make([]uint64, len(structlogs))
	copy(gasSelf, gasUsed)

	for i := range structlogs {
		op := structlogs[i].Op
		if !isCallOrCreateOpcode(op) {
			continue
		}

		callDepth := structlogs[i].Depth

		var childGasSum uint64

		// Sum gas_used for DIRECT children only (depth == callDepth + 1).
		// We only sum direct children because their gas_used already includes
		// any nested descendants. Summing all descendants would double count.
		for j := i + 1; j < len(structlogs); j++ {
			if structlogs[j].Depth <= callDepth {
				break
			}

			if structlogs[j].Depth == callDepth+1 {
				childGasSum += gasUsed[j]
			}
		}

		// gasSelf = total gas attributed to this CALL minus child execution
		// This gives us just the CALL overhead
		if gasUsed[i] >= childGasSum {
			gasSelf[i] = gasUsed[i] - childGasSum
		} else {
			// Edge case: if child gas exceeds parent (shouldn't happen in valid traces)
			// fall back to 0 to avoid underflow
			gasSelf[i] = 0
		}
	}

	return gasSelf
}

// isCallOrCreateOpcode returns true if the opcode spawns a new call frame.
func isCallOrCreateOpcode(op string) bool {
	switch op {
	case OpcodeCALL, OpcodeCALLCODE, OpcodeDELEGATECALL, OpcodeSTATICCALL, OpcodeCREATE, OpcodeCREATE2:
		return true
	default:
		return false
	}
}
