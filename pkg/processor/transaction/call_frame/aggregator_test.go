package call_frame

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

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

		aggregator.ProcessStructlog(execSl, i, 0, framePath, sl.gasUsed, nil, prevSl)
	}

	trace := &execution.TraceTransaction{
		Gas:    1000,
		Failed: false,
	}

	frames := aggregator.Finalize(trace, 100)

	require.Len(t, frames, 1)
	assert.Equal(t, uint32(0), frames[0].CallFrameID)
	assert.Nil(t, frames[0].ParentCallFrameID)
	assert.Equal(t, uint32(0), frames[0].Depth)
	assert.Equal(t, uint64(4), frames[0].OpcodeCount)
	assert.Equal(t, uint64(0), frames[0].ErrorCount)
	assert.Equal(t, "ROOT", frames[0].CallType)
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
	}, 0, 0, []uint32{0}, 3, nil, nil)

	aggregator.ProcessStructlog(&execution.StructLog{
		Op:    "CALL",
		Depth: 1,
		Gas:   9997,
	}, 1, 0, []uint32{0}, 5000, nil, &execution.StructLog{Op: "PUSH1", Depth: 1})

	// Frame 1 (child) - depth 2
	callAddr := "0x1234567890123456789012345678901234567890"

	aggregator.ProcessStructlog(&execution.StructLog{
		Op:    "PUSH1",
		Depth: 2,
		Gas:   5000,
	}, 2, 1, []uint32{0, 1}, 3, &callAddr, &execution.StructLog{Op: "CALL", Depth: 1})

	aggregator.ProcessStructlog(&execution.StructLog{
		Op:    "RETURN",
		Depth: 2,
		Gas:   4997,
	}, 3, 1, []uint32{0, 1}, 0, nil, &execution.StructLog{Op: "PUSH1", Depth: 2})

	// Back to root frame
	aggregator.ProcessStructlog(&execution.StructLog{
		Op:    "STOP",
		Depth: 1,
		Gas:   4997,
	}, 4, 0, []uint32{0}, 0, nil, &execution.StructLog{Op: "RETURN", Depth: 2})

	trace := &execution.TraceTransaction{
		Gas:    10000,
		Failed: false,
	}

	frames := aggregator.Finalize(trace, 500)

	require.Len(t, frames, 2)

	// Find root and child frames
	var rootFrame, childFrame *CallFrameRow

	for i := range frames {
		switch frames[i].CallFrameID {
		case 0:
			rootFrame = &frames[i]
		case 1:
			childFrame = &frames[i]
		}
	}

	require.NotNil(t, rootFrame, "root frame should exist")
	require.NotNil(t, childFrame, "child frame should exist")

	// Verify root frame
	assert.Equal(t, uint32(0), rootFrame.CallFrameID)
	assert.Nil(t, rootFrame.ParentCallFrameID)
	assert.Equal(t, uint32(0), rootFrame.Depth)
	assert.Equal(t, uint64(3), rootFrame.OpcodeCount) // PUSH1, CALL, STOP
	assert.Equal(t, "ROOT", rootFrame.CallType)

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
	}, 0, 0, []uint32{0}, 3, nil, nil)

	aggregator.ProcessStructlog(&execution.StructLog{
		Op:    "REVERT",
		Depth: 1,
		Gas:   997,
		Error: &errMsg,
	}, 1, 0, []uint32{0}, 0, nil, &execution.StructLog{Op: "PUSH1", Depth: 1})

	trace := &execution.TraceTransaction{
		Gas:    1000,
		Failed: true,
	}

	frames := aggregator.Finalize(trace, 100)

	require.Len(t, frames, 1)
	assert.Equal(t, uint64(1), frames[0].ErrorCount)
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
