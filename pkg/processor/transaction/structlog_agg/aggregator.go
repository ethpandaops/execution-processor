package structlog_agg

import (
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

// CallFrameRow represents aggregated data for a single call frame or per-opcode aggregation.
// This is the output format that gets inserted into ClickHouse.
// Two types of rows:
//   - Summary row: Operation="" contains frame-level metadata (call_type, target_address, etc.)
//   - Per-opcode row: Operation="SSTORE" etc. contains gas/count for that specific opcode
type CallFrameRow struct {
	CallFrameID       uint32
	ParentCallFrameID *uint32  // nil for root frame
	CallFramePath     []uint32 // Path from root to this frame
	Depth             uint32
	TargetAddress     *string
	CallType          string // CALL/DELEGATECALL/STATICCALL/CALLCODE/CREATE/CREATE2 (empty for root)
	Operation         string // Empty for summary row, opcode name for per-opcode rows
	OpcodeCount       uint64
	ErrorCount        uint64
	Gas               uint64  // SUM(gas_self) - excludes child frame gas
	GasCumulative     uint64  // For summary: frame gas_cumulative; for per-opcode: SUM(gas_used)
	MinDepth          uint32  // Per-opcode: MIN(depth); summary: same as Depth
	MaxDepth          uint32  // Per-opcode: MAX(depth); summary: same as Depth
	GasRefund         uint64  // Max refund from trace (0 for failed txs, per-opcode rows, and non-root frames)
	IntrinsicGas      *uint64 // Root frame only, from computeIntrinsicGas

	// Resource gas building blocks.
	MemWordsSumBefore   uint64
	MemWordsSumAfter    uint64
	MemWordsSqSumBefore uint64
	MemWordsSqSumAfter  uint64
	MemExpansionGas     uint64 // SUM(memory_expansion_gas) — exact per-opcode memory expansion cost
	ColdAccessCount     uint64
}

// OpcodeStats tracks gas and count for a specific opcode within a frame.
type OpcodeStats struct {
	Count         uint64
	Gas           uint64 // SUM(gas_self) - excludes child frame gas
	GasCumulative uint64 // SUM(gas_used) - includes child frame gas for CALL/CREATE
	ErrorCount    uint64
	MinDepth      uint32
	MaxDepth      uint32

	// Resource gas building blocks for decomposing gas into categories.
	MemWordsSumBefore   uint64 // SUM(memory_words_before)
	MemWordsSumAfter    uint64 // SUM(memory_words_after)
	MemWordsSqSumBefore uint64 // SUM(memory_words_before²)
	MemWordsSqSumAfter  uint64 // SUM(memory_words_after²)
	MemExpansionGas     uint64 // SUM(memory_expansion_gas) per opcode
	ColdCount           uint64 // Number of cold accesses
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

	// Per-opcode tracking
	OpcodeStats map[string]*OpcodeStats // opcode -> stats
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
//   - gasUsed: Pre-computed gas used for this opcode (includes child frame gas for CALL/CREATE)
//   - gasSelf: Pre-computed gas used excluding child frame gas
//   - callToAddr: Target address for CALL/CREATE opcodes (nil otherwise)
//   - prevStructlog: Previous structlog (for detecting frame entry via CALL/CREATE)
//   - memWordsBefore: Memory words before this opcode (0 if unavailable)
//   - memWordsAfter: Memory words after this opcode (0 if unavailable)
//   - memExpansionGas: Memory expansion gas for this opcode (exact per-opcode cost)
//   - coldAccessCount: Number of cold accesses for this opcode (0, 1, or 2)
func (fa *FrameAggregator) ProcessStructlog(
	sl *execution.StructLog,
	index int,
	frameID uint32,
	framePath []uint32,
	gasUsed uint64,
	gasSelf uint64,
	callToAddr *string,
	prevStructlog *execution.StructLog,
	memWordsBefore uint32,
	memWordsAfter uint32,
	memExpansionGas uint64,
	coldAccessCount uint64,
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
			OpcodeStats:      make(map[string]*OpcodeStats),
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

		depth := uint32(sl.Depth) //nolint:gosec // depth is bounded by EVM

		// Track per-opcode stats
		stats, ok := acc.OpcodeStats[sl.Op]
		if !ok {
			stats = &OpcodeStats{
				MinDepth: depth,
				MaxDepth: depth,
			}
			acc.OpcodeStats[sl.Op] = stats
		}

		stats.Count++
		stats.Gas += gasSelf           // SUM(gas_self) - excludes child frame gas
		stats.GasCumulative += gasUsed // SUM(gas_used) - includes child frame gas

		// Accumulate resource gas building blocks.
		wb := uint64(memWordsBefore)
		wa := uint64(memWordsAfter)
		stats.MemWordsSumBefore += wb
		stats.MemWordsSumAfter += wa
		stats.MemWordsSqSumBefore += wb * wb
		stats.MemWordsSqSumAfter += wa * wa
		stats.MemExpansionGas += memExpansionGas
		stats.ColdCount += coldAccessCount

		// Track min/max depth
		if depth < stats.MinDepth {
			stats.MinDepth = depth
		}

		if depth > stats.MaxDepth {
			stats.MaxDepth = depth
		}

		// Track errors per opcode
		if sl.Error != nil && *sl.Error != "" {
			stats.ErrorCount++
		}
	}

	acc.LastGas = sl.Gas
	acc.LastGasUsed = gasUsed

	// Track errors (for frame total)
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
// intrinsicGas is the transaction's intrinsic gas, stored on the root summary row.
// Returns the call frame rows ready for insertion.
// Emits two types of rows per frame:
//   - Summary row: Operation="" with frame-level metadata and totals
//   - Per-opcode rows: Operation="SSTORE" etc. with gas/count for that opcode
func (fa *FrameAggregator) Finalize(trace *execution.TraceTransaction, intrinsicGas uint64) []CallFrameRow {
	if len(fa.frames) == 0 {
		return nil
	}

	// Estimate capacity: 1 summary row per frame + average ~10 unique opcodes per frame
	rows := make([]CallFrameRow, 0, len(fa.frames)*11)

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

	// Second pass: compute gas (self) for each frame and emit rows
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

		depth := acc.Depth - 1 // Convert from EVM depth (1-based) to 0-based
		if frameID == 0 {
			depth = 0 // Ensure root is depth 0
		}

		// Emit summary row (Operation="")
		summaryRow := CallFrameRow{
			CallFrameID:       frameID,
			ParentCallFrameID: parentFrameID,
			CallFramePath:     acc.CallFramePath,
			Depth:             depth,
			TargetAddress:     acc.TargetAddress,
			CallType:          acc.CallType,
			Operation:         "", // Empty for summary row
			OpcodeCount:       acc.OpcodeCount,
			ErrorCount:        acc.ErrorCount,
			Gas:               gasSelf,
			GasCumulative:     gasCumulative[frameID],
			MinDepth:          depth, // For summary row, min/max depth = frame depth
			MaxDepth:          depth,
		}

		// Root frame: compute gas refund and intrinsic gas
		if frameID == 0 {
			// Check trace.Failed to detect transaction failure from REVERT opcodes.
			// REVERT executes successfully (no opcode error), but causes transaction failure.
			// Individual opcode errors (like "out of gas") are already counted via acc.ErrorCount.
			if trace.Failed && summaryRow.ErrorCount == 0 {
				summaryRow.ErrorCount = 1
			}

			// For successful transactions, refunds are applied to the final gas calculation.
			// For failed transactions, refunds are accumulated during execution but NOT applied
			// (all gas up to failure is consumed), so refund stays 0.
			if summaryRow.ErrorCount == 0 {
				summaryRow.GasRefund = acc.MaxRefund
			}

			// Intrinsic gas is charged before execution, whether the transaction succeeds or fails.
			summaryRow.IntrinsicGas = &intrinsicGas
		}

		// Compute summary-level resource gas totals (SUM across all opcodes).
		for _, stats := range acc.OpcodeStats {
			summaryRow.MemWordsSumBefore += stats.MemWordsSumBefore
			summaryRow.MemWordsSumAfter += stats.MemWordsSumAfter
			summaryRow.MemWordsSqSumBefore += stats.MemWordsSqSumBefore
			summaryRow.MemWordsSqSumAfter += stats.MemWordsSqSumAfter
			summaryRow.MemExpansionGas += stats.MemExpansionGas
			summaryRow.ColdAccessCount += stats.ColdCount
		}

		rows = append(rows, summaryRow)

		// Emit per-opcode rows
		for opcode, stats := range acc.OpcodeStats {
			opcodeRow := CallFrameRow{
				CallFrameID:         frameID,
				ParentCallFrameID:   parentFrameID,
				CallFramePath:       acc.CallFramePath,
				Depth:               depth,
				TargetAddress:       acc.TargetAddress,
				CallType:            acc.CallType,
				Operation:           opcode,
				OpcodeCount:         stats.Count,
				ErrorCount:          stats.ErrorCount,
				Gas:                 stats.Gas,
				GasCumulative:       stats.GasCumulative, // SUM(gas_used) for per-opcode rows
				MinDepth:            stats.MinDepth,
				MaxDepth:            stats.MaxDepth,
				GasRefund:           0,
				IntrinsicGas:        nil,
				MemWordsSumBefore:   stats.MemWordsSumBefore,
				MemWordsSumAfter:    stats.MemWordsSumAfter,
				MemWordsSqSumBefore: stats.MemWordsSqSumBefore,
				MemWordsSqSumAfter:  stats.MemWordsSqSumAfter,
				MemExpansionGas:     stats.MemExpansionGas,
				ColdAccessCount:     stats.ColdCount,
			}
			rows = append(rows, opcodeRow)
		}
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

// Intrinsic gas costs, charged before the EVM runs (go-ethereum params names in brackets).
const (
	txGas                     = 21000 // [TxGas]
	txGasContractCreation     = 53000 // [TxGasContractCreation]
	txDataZeroGas             = 4     // [TxDataZeroGas]
	txDataNonZeroGas          = 16    // [TxDataNonZeroGasEIP2028]
	initCodeWordGas           = 2     // [InitCodeWordGas], EIP-3860
	txAccessListAddressGas    = 2400  // [TxAccessListAddressGas], EIP-2930
	txAccessListStorageKeyGas = 1900  // [TxAccessListStorageKeyGas], EIP-2930
	txAuthorizationGas        = 25000 // [CallNewAccountGas], EIP-7702 PER_EMPTY_ACCOUNT_COST
)

// computeIntrinsicGas returns the gas a transaction pays before the EVM runs, from the
// transaction alone: the base cost, calldata, EIP-3860 initcode words, the EIP-2930 access
// list and EIP-7702 authorizations. It assumes Shanghai or later rules.
//
// Refunds and the EIP-7623 calldata floor (Prague and later) apply after execution and are
// not part of it:
//
//	receipt_gas = max(intrinsic + gas_cumulative - min(gas_refund, (intrinsic + gas_cumulative) / 5), floor)
//	floor       = 21000 + 10 * (zero_bytes + 4 * nonzero_bytes)
func computeIntrinsicGas(tx execution.Transaction) uint64 {
	data := tx.Data()

	var nonZero uint64

	for _, b := range data {
		if b != 0 {
			nonZero++
		}
	}

	zero := uint64(len(data)) - nonZero
	gas := zero*txDataZeroGas + nonZero*txDataNonZeroGas

	if tx.To() == nil {
		gas += txGasContractCreation + (uint64(len(data))+31)/32*initCodeWordGas
	} else {
		gas += txGas
	}

	for _, tuple := range tx.AccessList() {
		gas += txAccessListAddressGas + uint64(len(tuple.StorageKeys))*txAccessListStorageKeyGas
	}

	return gas + uint64(tx.AuthorizationCount())*txAuthorizationGas //nolint:gosec // a count is never negative
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
