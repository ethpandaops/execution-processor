package call_frame

import (
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

// CallFrameRow represents aggregated data for a single call frame.
// This is the output format that gets inserted into ClickHouse.
type CallFrameRow struct {
	CallFrameID       uint32
	ParentCallFrameID *uint32 // nil for root frame
	Depth             uint32
	TargetAddress     *string
	CallType          string // CALL/DELEGATECALL/STATICCALL/CALLCODE/CREATE/CREATE2 (empty for root)
	OpcodeCount       uint64
	ErrorCount        uint64
	Gas               uint64  // Self gas (excludes children)
	GasCumulative     uint64  // Self + all descendants
	GasRefund         *uint64 // Root frame only (max refund from trace)
	IntrinsicGas      *uint64 // Root frame only (computed)
}

// FrameAccumulator tracks data for a single frame during processing.
type FrameAccumulator struct {
	CallFrameID      uint32
	CallFramePath    []uint32 // Path from root to this frame
	FirstOpcodeIndex uint32
	FirstGas         uint64 // Gas at first opcode
	LastGas          uint64 // Gas at last opcode
	LastGasUsed      uint64 // GasUsed of last opcode
	OpcodeCount      uint64
	ErrorCount       uint64
	MaxRefund        uint64
	TargetAddress    *string
	CallType         string
	Depth            uint32
}

// FrameAggregator aggregates structlog data into call frame rows.
type FrameAggregator struct {
	frames    map[uint32]*FrameAccumulator // frameID -> accumulator
	frameList []uint32                     // Ordered list of frame IDs for deterministic output
}

// NewFrameAggregator creates a new FrameAggregator.
func NewFrameAggregator() *FrameAggregator {
	return &FrameAggregator{
		frames:    make(map[uint32]*FrameAccumulator, 16),
		frameList: make([]uint32, 0, 16),
	}
}

// ProcessStructlog processes a single structlog entry and updates frame accumulators.
// Parameters:
//   - sl: The structlog entry
//   - index: Index of this structlog in the trace
//   - frameID: The call frame ID for this structlog
//   - framePath: The path from root to current frame
//   - gasUsed: Pre-computed gas used for this opcode
//   - callToAddr: Target address for CALL/CREATE opcodes (nil otherwise)
//   - prevStructlog: Previous structlog (for detecting frame entry via CALL/CREATE)
func (fa *FrameAggregator) ProcessStructlog(
	sl *execution.StructLog,
	index int,
	frameID uint32,
	framePath []uint32,
	gasUsed uint64,
	callToAddr *string,
	prevStructlog *execution.StructLog,
) {
	acc, exists := fa.frames[frameID]
	if !exists {
		// New frame - initialize accumulator
		acc = &FrameAccumulator{
			CallFrameID:      frameID,
			CallFramePath:    framePath,
			FirstOpcodeIndex: uint32(index), //nolint:gosec // index is bounded
			FirstGas:         sl.Gas,
			Depth:            uint32(sl.Depth), //nolint:gosec // depth is bounded by EVM
		}

		// Determine call type and target address from the initiating opcode (previous structlog)
		if frameID == 0 {
			// Root frame - no initiating CALL opcode, use empty string
			acc.CallType = ""
		} else if prevStructlog != nil {
			// Frame was entered via the previous opcode
			acc.CallType = mapOpcodeToCallType(prevStructlog.Op)

			// Get target address from the CALL/CREATE opcode that initiated this frame
			// This is either from prevStructlog.CallToAddress or passed in via callToAddr
			if prevStructlog.CallToAddress != nil {
				acc.TargetAddress = prevStructlog.CallToAddress
			}
		}

		fa.frames[frameID] = acc
		fa.frameList = append(fa.frameList, frameID)
	}

	// Update accumulator with this opcode's data
	// Only count real opcodes, not synthetic EOA rows (operation = '')
	if sl.Op != "" {
		acc.OpcodeCount++
	}

	acc.LastGas = sl.Gas
	acc.LastGasUsed = gasUsed

	// Track errors
	if sl.Error != nil && *sl.Error != "" {
		acc.ErrorCount++
	}

	// Track max refund
	if sl.Refund != nil && *sl.Refund > acc.MaxRefund {
		acc.MaxRefund = *sl.Refund
	}

	// If this is an empty operation (synthetic EOA frame), capture the target address
	// Note: CallType is already set from the initiating CALL opcode (prevStructlog)
	if sl.Op == "" && callToAddr != nil {
		acc.TargetAddress = callToAddr
	}
}

// Finalize computes final call frame rows from the accumulated data.
// Returns the call frame rows ready for insertion.
func (fa *FrameAggregator) Finalize(trace *execution.TraceTransaction, receiptGas uint64) []CallFrameRow {
	if len(fa.frames) == 0 {
		return nil
	}

	rows := make([]CallFrameRow, 0, len(fa.frames))

	// First pass: compute gas_cumulative for each frame
	gasCumulative := make(map[uint32]uint64, len(fa.frames))
	for _, frameID := range fa.frameList {
		acc := fa.frames[frameID]
		// gas_cumulative = first_gas - last_gas + last_gas_used
		// This accounts for all gas consumed within this frame and its children
		if acc.FirstGas >= acc.LastGas {
			gasCumulative[frameID] = acc.FirstGas - acc.LastGas + acc.LastGasUsed
		} else {
			// Edge case: shouldn't happen in valid traces
			gasCumulative[frameID] = acc.LastGasUsed
		}
	}

	// Second pass: compute gas (self) for each frame by subtracting children's gas_cumulative
	for _, frameID := range fa.frameList {
		acc := fa.frames[frameID]

		// Find direct children of this frame
		var childGasSum uint64

		for _, otherFrameID := range fa.frameList {
			otherAcc := fa.frames[otherFrameID]
			if len(otherAcc.CallFramePath) == len(acc.CallFramePath)+1 {
				// Check if this frame is the parent
				if isParentOf(acc.CallFramePath, otherAcc.CallFramePath) {
					childGasSum += gasCumulative[otherFrameID]
				}
			}
		}

		// gas = gas_cumulative - sum(children.gas_cumulative)
		gasSum := gasCumulative[frameID]

		var gasSelf uint64

		if gasSum >= childGasSum {
			gasSelf = gasSum - childGasSum
		}

		// Determine parent frame ID
		var parentFrameID *uint32

		if len(acc.CallFramePath) >= 2 {
			parent := acc.CallFramePath[len(acc.CallFramePath)-2]
			parentFrameID = &parent
		}

		row := CallFrameRow{
			CallFrameID:       frameID,
			ParentCallFrameID: parentFrameID,
			Depth:             acc.Depth - 1, // Convert from EVM depth (1-based) to 0-based
			TargetAddress:     acc.TargetAddress,
			CallType:          acc.CallType,
			OpcodeCount:       acc.OpcodeCount,
			ErrorCount:        acc.ErrorCount,
			Gas:               gasSelf,
			GasCumulative:     gasCumulative[frameID],
		}

		// Root frame gets gas refund and intrinsic gas
		if frameID == 0 {
			row.GasRefund = &acc.MaxRefund
			row.Depth = 0 // Ensure root is depth 0

			// Compute intrinsic gas for root frame
			// Formula from int_transaction_call_frame.sql
			// Only computed when error_count = 0 (successful transaction)
			if acc.ErrorCount == 0 {
				intrinsicGas := computeIntrinsicGas(gasCumulative[0], acc.MaxRefund, receiptGas)
				if intrinsicGas > 0 {
					row.IntrinsicGas = &intrinsicGas
				}
			}
		}

		rows = append(rows, row)
	}

	return rows
}

// isParentOf checks if parentPath is the direct parent of childPath.
func isParentOf(parentPath, childPath []uint32) bool {
	if len(childPath) != len(parentPath)+1 {
		return false
	}

	for i := range parentPath {
		if parentPath[i] != childPath[i] {
			return false
		}
	}

	return true
}

// mapOpcodeToCallType maps an opcode to a call type string.
func mapOpcodeToCallType(op string) string {
	switch op {
	case "CALL":
		return "CALL"
	case "CALLCODE":
		return "CALLCODE"
	case "DELEGATECALL":
		return "DELEGATECALL"
	case "STATICCALL":
		return "STATICCALL"
	case "CREATE":
		return "CREATE"
	case "CREATE2":
		return "CREATE2"
	default:
		return "UNKNOWN"
	}
}

// computeIntrinsicGas computes the intrinsic gas for a transaction.
// This is the gas consumed before EVM execution begins (21000 base + calldata costs).
//
// Formula from int_transaction_call_frame.sql:
//
//	IF gas_refund >= receipt_gas / 4 THEN
//	  intrinsic = receipt_gas * 5 / 4 - gas_cumulative  (refund was capped)
//	ELSE
//	  intrinsic = receipt_gas - gas_cumulative + gas_refund  (uncapped)
func computeIntrinsicGas(gasCumulative, gasRefund, receiptGas uint64) uint64 {
	if receiptGas == 0 {
		return 0
	}

	var intrinsic uint64

	if gasRefund >= receiptGas/4 {
		// Capped case: refund was limited to receipt_gas/4
		// Actual refund applied = receipt_gas/4
		// So: receipt_gas = intrinsic + gas_cumulative - receipt_gas/4
		// => intrinsic = receipt_gas + receipt_gas/4 - gas_cumulative
		// => intrinsic = receipt_gas * 5/4 - gas_cumulative
		cappedValue := receiptGas * 5 / 4
		if cappedValue >= gasCumulative {
			intrinsic = cappedValue - gasCumulative
		}
	} else {
		// Uncapped case: full refund was applied
		// receipt_gas = intrinsic + gas_cumulative - gas_refund
		// => intrinsic = receipt_gas - gas_cumulative + gas_refund
		if receiptGas+gasRefund >= gasCumulative {
			intrinsic = receiptGas - gasCumulative + gasRefund
		}
	}

	return intrinsic
}

// SetRootTargetAddress sets the target address for the root frame (frame ID 0).
// This should be called after processing all structlogs, as the root frame's
// target address comes from the transaction's to_address, not from an initiating CALL.
func (fa *FrameAggregator) SetRootTargetAddress(addr *string) {
	if acc, exists := fa.frames[0]; exists {
		acc.TargetAddress = addr
	}
}

// Reset clears the aggregator for reuse.
func (fa *FrameAggregator) Reset() {
	clear(fa.frames)
	fa.frameList = fa.frameList[:0]
}
