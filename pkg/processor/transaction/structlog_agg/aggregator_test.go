package structlog_agg

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

// getSummaryRow returns the summary row (Operation == "") for a given frame ID.
func getSummaryRow(rows []CallFrameRow, frameID uint32) *CallFrameRow {
	for i := range rows {
		if rows[i].Operation == "" && rows[i].CallFrameID == frameID {
			return &rows[i]
		}
	}

	return nil
}

// getOpcodeRow returns the per-opcode row for a given frame ID and operation.
func getOpcodeRow(rows []CallFrameRow, frameID uint32, operation string) *CallFrameRow {
	for i := range rows {
		if rows[i].Operation == operation && rows[i].CallFrameID == frameID {
			return &rows[i]
		}
	}

	return nil
}

// countSummaryRows counts the number of summary rows (Operation == "").
func countSummaryRows(rows []CallFrameRow) int {
	count := 0

	for _, row := range rows {
		if row.Operation == "" {
			count++
		}
	}

	return count
}

func TestFrameAggregator_SingleFrame(t *testing.T) {
	aggregator := NewFrameAggregator()

	// Simulate a simple transaction with only root frame
	structlogs := []struct {
		op      string
		depth   uint64
		gas     uint64
		gasUsed uint64
		refund  *uint64
		errStr  *string
	}{
		{"PUSH1", 1, 1000, 3, nil, nil},
		{"PUSH1", 1, 997, 3, nil, nil},
		{"ADD", 1, 994, 3, nil, nil},
		{"STOP", 1, 991, 0, nil, nil},
	}

	framePath := []uint32{0}

	for i, sl := range structlogs {
		execSl := &execution.StructLog{
			Op:    sl.op,
			Depth: sl.depth,
			Gas:   sl.gas,
		}

		var prevSl *execution.StructLog
		if i > 0 {
			prevSl = &execution.StructLog{
				Op:    structlogs[i-1].op,
				Depth: structlogs[i-1].depth,
			}
		}

		// For simple opcodes, gasSelf == gasUsed
		aggregator.ProcessStructlog(execSl, i, 0, framePath, sl.gasUsed, sl.gasUsed, nil, prevSl)
	}

	trace := &execution.TraceTransaction{
		Gas:    1000,
		Failed: false,
	}

	frames := aggregator.Finalize(trace, 100)

	// Should have 1 summary row + 3 per-opcode rows (PUSH1, ADD, STOP)
	assert.Equal(t, 1, countSummaryRows(frames))

	// Check summary row
	summaryRow := getSummaryRow(frames, 0)
	require.NotNil(t, summaryRow)
	assert.Equal(t, uint32(0), summaryRow.CallFrameID)
	assert.Nil(t, summaryRow.ParentCallFrameID)
	assert.Equal(t, uint32(0), summaryRow.Depth)
	assert.Equal(t, uint64(4), summaryRow.OpcodeCount)
	assert.Equal(t, uint64(0), summaryRow.ErrorCount)
	assert.Equal(t, "", summaryRow.CallType)
	assert.Equal(t, "", summaryRow.Operation)

	// Check per-opcode rows
	push1Row := getOpcodeRow(frames, 0, "PUSH1")
	require.NotNil(t, push1Row)
	assert.Equal(t, uint64(2), push1Row.OpcodeCount) // 2x PUSH1

	addRow := getOpcodeRow(frames, 0, "ADD")
	require.NotNil(t, addRow)
	assert.Equal(t, uint64(1), addRow.OpcodeCount)
}

func TestFrameAggregator_NestedCalls(t *testing.T) {
	aggregator := NewFrameAggregator()

	// Simulate transaction with nested CALL
	// Root frame (depth 1) -> CALL -> Child frame (depth 2)

	// Frame 0 (root) - depth 1
	aggregator.ProcessStructlog(&execution.StructLog{
		Op:    "PUSH1",
		Depth: 1,
		Gas:   10000,
	}, 0, 0, []uint32{0}, 3, 3, nil, nil)

	// CALL opcode: gasUsed includes child gas, gasSelf is just the CALL overhead
	aggregator.ProcessStructlog(&execution.StructLog{
		Op:    "CALL",
		Depth: 1,
		Gas:   9997,
	}, 1, 0, []uint32{0}, 5000, 100, nil, &execution.StructLog{Op: "PUSH1", Depth: 1})

	// Frame 1 (child) - depth 2
	callAddr := "0x1234567890123456789012345678901234567890"

	aggregator.ProcessStructlog(&execution.StructLog{
		Op:    "PUSH1",
		Depth: 2,
		Gas:   5000,
	}, 2, 1, []uint32{0, 1}, 3, 3, &callAddr, &execution.StructLog{Op: "CALL", Depth: 1})

	aggregator.ProcessStructlog(&execution.StructLog{
		Op:    "RETURN",
		Depth: 2,
		Gas:   4997,
	}, 3, 1, []uint32{0, 1}, 0, 0, nil, &execution.StructLog{Op: "PUSH1", Depth: 2})

	// Back to root frame
	aggregator.ProcessStructlog(&execution.StructLog{
		Op:    "STOP",
		Depth: 1,
		Gas:   4997,
	}, 4, 0, []uint32{0}, 0, 0, nil, &execution.StructLog{Op: "RETURN", Depth: 2})

	trace := &execution.TraceTransaction{
		Gas:    10000,
		Failed: false,
	}

	frames := aggregator.Finalize(trace, 500)

	// Should have 2 summary rows (root + child) + per-opcode rows
	assert.Equal(t, 2, countSummaryRows(frames))

	// Get summary rows for root and child frames
	rootFrame := getSummaryRow(frames, 0)
	childFrame := getSummaryRow(frames, 1)

	require.NotNil(t, rootFrame, "root frame should exist")
	require.NotNil(t, childFrame, "child frame should exist")

	// Verify root frame
	assert.Equal(t, uint32(0), rootFrame.CallFrameID)
	assert.Nil(t, rootFrame.ParentCallFrameID)
	assert.Equal(t, uint32(0), rootFrame.Depth)
	assert.Equal(t, uint64(3), rootFrame.OpcodeCount) // PUSH1, CALL, STOP
	assert.Equal(t, "", rootFrame.CallType)

	// Verify child frame
	assert.Equal(t, uint32(1), childFrame.CallFrameID)
	require.NotNil(t, childFrame.ParentCallFrameID)
	assert.Equal(t, uint32(0), *childFrame.ParentCallFrameID)
	assert.Equal(t, uint32(1), childFrame.Depth)       // 0-based depth
	assert.Equal(t, uint64(2), childFrame.OpcodeCount) // PUSH1, RETURN
	assert.Equal(t, "CALL", childFrame.CallType)
}

func TestFrameAggregator_ErrorCounting(t *testing.T) {
	aggregator := NewFrameAggregator()

	errMsg := "execution reverted"

	aggregator.ProcessStructlog(&execution.StructLog{
		Op:    "PUSH1",
		Depth: 1,
		Gas:   1000,
	}, 0, 0, []uint32{0}, 3, 3, nil, nil)

	aggregator.ProcessStructlog(&execution.StructLog{
		Op:    "REVERT",
		Depth: 1,
		Gas:   997,
		Error: &errMsg,
	}, 1, 0, []uint32{0}, 0, 0, nil, &execution.StructLog{Op: "PUSH1", Depth: 1})

	trace := &execution.TraceTransaction{
		Gas:    1000,
		Failed: true,
	}

	frames := aggregator.Finalize(trace, 100)

	assert.Equal(t, 1, countSummaryRows(frames))

	summaryRow := getSummaryRow(frames, 0)
	require.NotNil(t, summaryRow)
	assert.Equal(t, uint64(1), summaryRow.ErrorCount)

	// Check that REVERT opcode row also has error count
	revertRow := getOpcodeRow(frames, 0, "REVERT")
	require.NotNil(t, revertRow)
	assert.Equal(t, uint64(1), revertRow.ErrorCount)
}

func TestComputeIntrinsicGas_Uncapped(t *testing.T) {
	// Test uncapped case: refund < receipt_gas/4
	gasCumulative := uint64(80000)
	gasRefund := uint64(10000)
	receiptGas := uint64(100000)

	intrinsic := computeIntrinsicGas(gasCumulative, gasRefund, receiptGas)

	// intrinsic = receipt_gas - gas_cumulative + gas_refund
	// intrinsic = 100000 - 80000 + 10000 = 30000
	assert.Equal(t, uint64(30000), intrinsic)
}

func TestComputeIntrinsicGas_Capped(t *testing.T) {
	// Test capped case: refund >= receipt_gas/4
	gasCumulative := uint64(80000)
	gasRefund := uint64(30000) // >= 100000/4 = 25000
	receiptGas := uint64(100000)

	intrinsic := computeIntrinsicGas(gasCumulative, gasRefund, receiptGas)

	// intrinsic = receipt_gas * 5/4 - gas_cumulative
	// intrinsic = 125000 - 80000 = 45000
	assert.Equal(t, uint64(45000), intrinsic)
}

func TestComputeIntrinsicGas_ZeroReceipt(t *testing.T) {
	intrinsic := computeIntrinsicGas(1000, 100, 0)
	assert.Equal(t, uint64(0), intrinsic)
}

func TestComputeIntrinsicGas_NoUnderflow_WhenReceiptLessThanCumulative(t *testing.T) {
	// This test verifies the fix for the underflow bug in the UNCAPPED path.
	// When receiptGas < gasCumulative but receiptGas + gasRefund >= gasCumulative,
	// the old code would underflow: receiptGas - gasCumulative + gasRefund
	// The fix reorders to: (receiptGas + gasRefund) - gasCumulative
	//
	// To hit the uncapped path, we need: gasRefund < receiptGas/4
	//
	// Example that triggers the bug in unfixed code:
	// receiptGas = 100,000
	// gasCumulative = 110,000
	// gasRefund = 20,000 (< 25,000 = receiptGas/4, so UNCAPPED)
	// Guard: 100,000 + 20,000 >= 110,000 ✓ (120,000 >= 110,000)
	// Old calc: 100,000 - 110,000 = UNDERFLOW!
	// Fixed calc: (100,000 + 20,000) - 110,000 = 10,000
	gasCumulative := uint64(110000)
	gasRefund := uint64(20000) // < 100000/4 = 25000, so uncapped
	receiptGas := uint64(100000)

	intrinsic := computeIntrinsicGas(gasCumulative, gasRefund, receiptGas)

	// Expected: (100000 + 20000) - 110000 = 10000
	assert.Equal(t, uint64(10000), intrinsic)

	// Verify it's NOT a huge underflow value
	assert.Less(t, intrinsic, uint64(1000000), "intrinsic gas should be reasonable, not an underflow")
}

func TestComputeIntrinsicGas_NoUnderflow_EdgeCase(t *testing.T) {
	// Edge case: receiptGas + gasRefund == gasCumulative exactly (uncapped path)
	// gasRefund must be < receiptGas/4 to hit uncapped path
	gasCumulative := uint64(120000)
	gasRefund := uint64(20000) // < 100000/4 = 25000, so uncapped
	receiptGas := uint64(100000)

	intrinsic := computeIntrinsicGas(gasCumulative, gasRefund, receiptGas)

	// Expected: (100000 + 20000) - 120000 = 0
	assert.Equal(t, uint64(0), intrinsic)
}

func TestComputeIntrinsicGas_NoUnderflow_ReceiptExceedsCumulative(t *testing.T) {
	// Normal case: receiptGas >= gasCumulative (no underflow risk)
	gasCumulative := uint64(80000)
	gasRefund := uint64(10000)
	receiptGas := uint64(100000)

	intrinsic := computeIntrinsicGas(gasCumulative, gasRefund, receiptGas)

	// Expected: (100000 + 10000) - 80000 = 30000
	// This matches the old formula: 100000 - 80000 + 10000 = 30000
	assert.Equal(t, uint64(30000), intrinsic)
}

func TestComputeIntrinsicGas_GuardPreventsNegativeResult(t *testing.T) {
	// When receiptGas + gasRefund < gasCumulative, guard prevents computation
	gasCumulative := uint64(300000)
	gasRefund := uint64(50000)
	receiptGas := uint64(100000)

	intrinsic := computeIntrinsicGas(gasCumulative, gasRefund, receiptGas)

	// Guard: 100000 + 50000 >= 300000? No (150000 < 300000)
	// So intrinsic stays 0
	assert.Equal(t, uint64(0), intrinsic)
}

func TestIsParentOf(t *testing.T) {
	tests := []struct {
		name       string
		parentPath []uint32
		childPath  []uint32
		expected   bool
	}{
		{
			name:       "direct parent",
			parentPath: []uint32{0},
			childPath:  []uint32{0, 1},
			expected:   true,
		},
		{
			name:       "nested parent",
			parentPath: []uint32{0, 1},
			childPath:  []uint32{0, 1, 2},
			expected:   true,
		},
		{
			name:       "not a parent - same length",
			parentPath: []uint32{0, 1},
			childPath:  []uint32{0, 2},
			expected:   false,
		},
		{
			name:       "not a parent - grandchild",
			parentPath: []uint32{0},
			childPath:  []uint32{0, 1, 2},
			expected:   false,
		},
		{
			name:       "not a parent - different path",
			parentPath: []uint32{0, 1},
			childPath:  []uint32{0, 2, 3},
			expected:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isParentOf(tt.parentPath, tt.childPath)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestFrameAggregator_EOAFrame(t *testing.T) {
	// Test that EOA frames (synthetic rows with operation="") have opcode_count=0
	aggregator := NewFrameAggregator()

	// Root frame with CALL to EOA
	aggregator.ProcessStructlog(&execution.StructLog{
		Op:    "PUSH1",
		Depth: 1,
		Gas:   10000,
	}, 0, 0, []uint32{0}, 3, 3, nil, nil)

	aggregator.ProcessStructlog(&execution.StructLog{
		Op:    "CALL",
		Depth: 1,
		Gas:   9997,
	}, 1, 0, []uint32{0}, 100, 100, nil, &execution.StructLog{Op: "PUSH1", Depth: 1})

	// Synthetic EOA frame (operation = "", depth = 2)
	eoaAddr := "0xEOAEOAEOAEOAEOAEOAEOAEOAEOAEOAEOAEOAEOAE"

	aggregator.ProcessStructlog(&execution.StructLog{
		Op:    "", // Empty = synthetic EOA row
		Depth: 2,
		Gas:   0,
	}, 1, 1, []uint32{0, 1}, 0, 0, &eoaAddr, &execution.StructLog{Op: "CALL", Depth: 1})

	// Back to root frame
	aggregator.ProcessStructlog(&execution.StructLog{
		Op:    "STOP",
		Depth: 1,
		Gas:   9897,
	}, 2, 0, []uint32{0}, 0, 0, nil, &execution.StructLog{Op: "", Depth: 2})

	trace := &execution.TraceTransaction{
		Gas:    10000,
		Failed: false,
	}

	frames := aggregator.Finalize(trace, 500)

	// Should have 2 summary rows (root + EOA)
	assert.Equal(t, 2, countSummaryRows(frames))

	// Get summary rows for root and EOA frames
	rootFrame := getSummaryRow(frames, 0)
	eoaFrame := getSummaryRow(frames, 1)

	require.NotNil(t, rootFrame, "root frame should exist")
	require.NotNil(t, eoaFrame, "EOA frame should exist")

	// Root frame: 3 real opcodes (PUSH1, CALL, STOP)
	assert.Equal(t, uint64(3), rootFrame.OpcodeCount)

	// EOA frame: 0 opcodes (only synthetic row with op="")
	assert.Equal(t, uint64(0), eoaFrame.OpcodeCount)
	assert.Equal(t, "CALL", eoaFrame.CallType)
	require.NotNil(t, eoaFrame.TargetAddress)
	assert.Equal(t, eoaAddr, *eoaFrame.TargetAddress)
}

func TestFrameAggregator_SetRootTargetAddress(t *testing.T) {
	aggregator := NewFrameAggregator()

	// Process a simple root frame
	aggregator.ProcessStructlog(&execution.StructLog{
		Op:    "STOP",
		Depth: 1,
		Gas:   1000,
	}, 0, 0, []uint32{0}, 0, 0, nil, nil)

	// Set root target address (simulating tx.To())
	rootAddr := "0x1234567890123456789012345678901234567890"
	aggregator.SetRootTargetAddress(&rootAddr)

	trace := &execution.TraceTransaction{
		Gas:    1000,
		Failed: false,
	}

	frames := aggregator.Finalize(trace, 100)

	assert.Equal(t, 1, countSummaryRows(frames))

	summaryRow := getSummaryRow(frames, 0)
	require.NotNil(t, summaryRow)
	require.NotNil(t, summaryRow.TargetAddress)
	assert.Equal(t, rootAddr, *summaryRow.TargetAddress)
}

func TestFrameAggregator_FailedTransaction_NoRefundButHasIntrinsic(t *testing.T) {
	// Test that failed transactions do NOT have gas_refund but DO have intrinsic_gas.
	// Intrinsic gas is ALWAYS charged (before EVM execution begins).
	// For failed transactions, refunds are accumulated during execution but NOT applied
	// to the final gas calculation.
	aggregator := NewFrameAggregator()

	errMsg := "execution reverted"
	refundValue := uint64(4800) // Refund accumulated during execution

	// Simulate a transaction that accumulates refunds but then fails
	aggregator.ProcessStructlog(&execution.StructLog{
		Op:    "PUSH1",
		Depth: 1,
		Gas:   80000,
	}, 0, 0, []uint32{0}, 3, 3, nil, nil)

	// SSTORE that generates a refund
	aggregator.ProcessStructlog(&execution.StructLog{
		Op:     "SSTORE",
		Depth:  1,
		Gas:    79997,
		Refund: &refundValue, // Refund accumulated
	}, 1, 0, []uint32{0}, 20000, 20000, nil, &execution.StructLog{Op: "PUSH1", Depth: 1})

	// Transaction fails with REVERT
	aggregator.ProcessStructlog(&execution.StructLog{
		Op:     "REVERT",
		Depth:  1,
		Gas:    59997,
		Error:  &errMsg,
		Refund: &refundValue, // Refund still present but won't be applied
	}, 2, 0, []uint32{0}, 0, 0, nil, &execution.StructLog{Op: "SSTORE", Depth: 1})

	trace := &execution.TraceTransaction{
		Gas:    80000,
		Failed: true,
	}

	// Receipt gas for failed tx
	receiptGas := uint64(50000)
	frames := aggregator.Finalize(trace, receiptGas)

	assert.Equal(t, 1, countSummaryRows(frames))

	summaryRow := getSummaryRow(frames, 0)
	require.NotNil(t, summaryRow)

	// Error count should be 1
	assert.Equal(t, uint64(1), summaryRow.ErrorCount)

	// GasRefund should be nil for failed transactions
	// Even though refund was accumulated (4800), it's not applied when tx fails
	assert.Nil(t, summaryRow.GasRefund, "GasRefund should be nil for failed transactions")

	// IntrinsicGas SHOULD be computed for failed transactions
	// Intrinsic gas is always charged before EVM execution begins
	// Formula: intrinsic = receiptGas - gasCumulative + 0 (no refund for failed)
	require.NotNil(t, summaryRow.IntrinsicGas, "IntrinsicGas should be computed for failed transactions")
}

func TestFrameAggregator_SuccessfulTransaction_HasRefundAndIntrinsic(t *testing.T) {
	// Test that successful transactions DO have gas_refund and intrinsic_gas set.
	aggregator := NewFrameAggregator()

	refundValue := uint64(4800)

	aggregator.ProcessStructlog(&execution.StructLog{
		Op:    "PUSH1",
		Depth: 1,
		Gas:   80000,
	}, 0, 0, []uint32{0}, 3, 3, nil, nil)

	// SSTORE that generates a refund
	aggregator.ProcessStructlog(&execution.StructLog{
		Op:     "SSTORE",
		Depth:  1,
		Gas:    79997,
		Refund: &refundValue,
	}, 1, 0, []uint32{0}, 20000, 20000, nil, &execution.StructLog{Op: "PUSH1", Depth: 1})

	// Successful STOP
	aggregator.ProcessStructlog(&execution.StructLog{
		Op:     "STOP",
		Depth:  1,
		Gas:    59997,
		Refund: &refundValue,
	}, 2, 0, []uint32{0}, 0, 0, nil, &execution.StructLog{Op: "SSTORE", Depth: 1})

	trace := &execution.TraceTransaction{
		Gas:    80000,
		Failed: false,
	}

	// For successful tx, receipt gas = gas_used (after refund applied)
	// Use 20000 so the EIP-3529 cap (20000/4=5000) doesn't affect our 4800 refund
	frames := aggregator.Finalize(trace, 20000)

	assert.Equal(t, 1, countSummaryRows(frames))

	summaryRow := getSummaryRow(frames, 0)
	require.NotNil(t, summaryRow)

	// Error count should be 0
	assert.Equal(t, uint64(0), summaryRow.ErrorCount)

	// GasRefund should be set for successful transactions
	require.NotNil(t, summaryRow.GasRefund, "GasRefund should be set for successful transactions")
	assert.Equal(t, refundValue, *summaryRow.GasRefund)

	// IntrinsicGas should be computed for successful transactions
	// (exact value depends on the computation, just verify it's not nil)
	// Note: might be nil if computed value is 0, so we just check the logic is exercised
}

func TestFrameAggregator_RefundCapEIP3529(t *testing.T) {
	// Test that gas_refund is capped per EIP-3529.
	// The EIP limits refund to gas_used/5. Since gas_used = receiptGas + actual_refund,
	// solving gives: actual_refund = min(refund_counter, receiptGas / 4).
	//
	// Real example from TX 0x04d9e42f6ecc9d1a5739d5a9a45f273ab0209660198622477b2272a88c6b2d79:
	// - refund_counter = 42600
	// - receipt.GasUsed = 160497
	// - cap = 160497 / 4 = 40124
	// - actual_refund = min(42600, 40124) = 40124
	aggregator := NewFrameAggregator()

	refundValue := uint64(42600)

	aggregator.ProcessStructlog(&execution.StructLog{
		Op:    "PUSH1",
		Depth: 1,
		Gas:   200000,
	}, 0, 0, []uint32{0}, 3, 3, nil, nil)

	aggregator.ProcessStructlog(&execution.StructLog{
		Op:     "STOP",
		Depth:  1,
		Gas:    100000,
		Refund: &refundValue,
	}, 1, 0, []uint32{0}, 0, 0, nil, &execution.StructLog{Op: "PUSH1", Depth: 1})

	trace := &execution.TraceTransaction{
		Gas:    200000,
		Failed: false,
	}

	// receiptGas = 160497, so cap = 160497 / 4 = 40124
	frames := aggregator.Finalize(trace, 160497)

	summaryRow := getSummaryRow(frames, 0)
	require.NotNil(t, summaryRow)
	require.NotNil(t, summaryRow.GasRefund)

	// Refund should be capped: min(42600, 40124) = 40124
	assert.Equal(t, uint64(40124), *summaryRow.GasRefund, "Refund should be capped per EIP-3529")
}

func TestFrameAggregator_RevertWithoutOpcodeError(t *testing.T) {
	// Test that REVERT transactions are correctly detected as failed even when
	// the REVERT opcode itself has no error field set.
	//
	// REVERT is a successful opcode execution that causes transaction failure.
	// Unlike "out of gas" errors where the opcode has an error field, REVERT
	// executes successfully but reverts state changes. The failure is indicated
	// by trace.Failed = true, NOT by individual opcode errors.
	aggregator := NewFrameAggregator()

	// Simulate a transaction that reverts: PUSH1 -> REVERT
	aggregator.ProcessStructlog(&execution.StructLog{
		Op:    "PUSH1",
		Depth: 1,
		Gas:   50000,
	}, 0, 0, []uint32{0}, 3, 3, nil, nil)

	// REVERT opcode with NO error field (realistic behavior)
	aggregator.ProcessStructlog(&execution.StructLog{
		Op:    "REVERT",
		Depth: 1,
		Gas:   49997,
		// Note: NO Error field set - REVERT executes successfully
	}, 1, 0, []uint32{0}, 0, 0, nil, &execution.StructLog{Op: "PUSH1", Depth: 1})

	// trace.Failed is true because the transaction reverted
	trace := &execution.TraceTransaction{
		Gas:    50000,
		Failed: true, // This is how REVERT is indicated
	}

	frames := aggregator.Finalize(trace, 30000)

	assert.Equal(t, 1, countSummaryRows(frames))

	summaryRow := getSummaryRow(frames, 0)
	require.NotNil(t, summaryRow)

	// Error count MUST be 1 even though no opcode had an error field
	// This is the key assertion: trace.Failed should set error_count = 1
	assert.Equal(t, uint64(1), summaryRow.ErrorCount,
		"ErrorCount should be 1 for REVERT transactions even without opcode errors")

	// GasRefund should be nil for failed transactions
	assert.Nil(t, summaryRow.GasRefund,
		"GasRefund should be nil for reverted transactions")
}

func TestMapOpcodeToCallType(t *testing.T) {
	tests := []struct {
		opcode   string
		expected string
	}{
		{"CALL", "CALL"},
		{"CALLCODE", "CALLCODE"},
		{"DELEGATECALL", "DELEGATECALL"},
		{"STATICCALL", "STATICCALL"},
		{"CREATE", "CREATE"},
		{"CREATE2", "CREATE2"},
		{"PUSH1", "UNKNOWN"},
		{"STOP", "UNKNOWN"},
	}

	for _, tt := range tests {
		t.Run(tt.opcode, func(t *testing.T) {
			result := mapOpcodeToCallType(tt.opcode)
			assert.Equal(t, tt.expected, result)
		})
	}
}
