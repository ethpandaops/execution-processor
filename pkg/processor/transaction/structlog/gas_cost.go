package structlog

import (
	"math"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
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
