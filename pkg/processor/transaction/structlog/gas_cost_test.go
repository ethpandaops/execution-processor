package structlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

func TestComputeGasUsed_EmptyLogs(t *testing.T) {
	result := ComputeGasUsed(nil)
	assert.Nil(t, result)

	result = ComputeGasUsed([]execution.StructLog{})
	assert.Nil(t, result)
}

func TestComputeGasUsed_SingleOpcode(t *testing.T) {
	// Single opcode keeps pre-calculated cost since there's no next opcode to diff against
	structlogs := []execution.StructLog{
		{Op: "PUSH1", Gas: 100000, GasCost: 3, Depth: 1},
	}

	result := ComputeGasUsed(structlogs)

	require.Len(t, result, 1)
	assert.Equal(t, uint64(3), result[0], "single opcode should keep pre-calculated cost")
}

func TestComputeGasUsed_SameDepth(t *testing.T) {
	// Sequential opcodes at the same depth:
	// PUSH1 (gas=100000, cost=3) -> PUSH1 (gas=99997, cost=3) -> ADD (gas=99994, cost=3)
	// Expected gasUsed: [3, 3, 3] where first two are computed, last keeps pre-calculated
	structlogs := []execution.StructLog{
		{Op: "PUSH1", Gas: 100000, GasCost: 3, Depth: 1},
		{Op: "PUSH1", Gas: 99997, GasCost: 3, Depth: 1},
		{Op: "ADD", Gas: 99994, GasCost: 3, Depth: 1},
	}

	result := ComputeGasUsed(structlogs)

	require.Len(t, result, 3)
	// First PUSH1: 100000 - 99997 = 3
	assert.Equal(t, uint64(3), result[0])
	// Second PUSH1: 99997 - 99994 = 3
	assert.Equal(t, uint64(3), result[1])
	// ADD: keeps pre-calculated cost (last opcode)
	assert.Equal(t, uint64(3), result[2])
}

func TestComputeGasUsed_DepthChange(t *testing.T) {
	// Simulate: CALL at depth 1, child execution at depth 2, return to depth 1
	// depth=1, gas=100000: CALL opcode
	// depth=2, gas=63000:  child starts (PUSH)
	// depth=2, gas=60000:  child continues (STOP)
	// depth=1, gas=98000:  back to parent (PUSH)
	structlogs := []execution.StructLog{
		{Op: "CALL", Gas: 100000, GasCost: 100, Depth: 1},
		{Op: "PUSH1", Gas: 63000, GasCost: 3, Depth: 2},
		{Op: "STOP", Gas: 60000, GasCost: 0, Depth: 2},
		{Op: "PUSH1", Gas: 98000, GasCost: 3, Depth: 1},
	}

	result := ComputeGasUsed(structlogs)

	require.Len(t, result, 4)

	// CALL at depth 1: gasUsed = 100000 - 98000 = 2000 (includes all child execution)
	assert.Equal(t, uint64(2000), result[0], "CALL should include child execution gas")

	// PUSH at depth 2: gasUsed = 63000 - 60000 = 3000
	assert.Equal(t, uint64(3000), result[1], "child PUSH should compute diff")

	// STOP at depth 2: keeps pre-calculated cost (last in call context)
	assert.Equal(t, uint64(0), result[2], "STOP should keep pre-calculated cost")

	// PUSH at depth 1: keeps pre-calculated cost (last opcode)
	assert.Equal(t, uint64(3), result[3], "last opcode should keep pre-calculated cost")
}

func TestComputeGasUsed_NestedCalls(t *testing.T) {
	// Test 3 levels deep: depth 1 -> depth 2 -> depth 3 -> back
	// depth=1, gas=100000: CALL (outer)
	// depth=2, gas=80000:  PUSH in first child
	// depth=2, gas=79000:  CALL (inner call)
	// depth=3, gas=50000:  PUSH in innermost child
	// depth=3, gas=49000:  STOP in innermost child
	// depth=2, gas=75000:  back to first child
	// depth=2, gas=74000:  STOP in first child
	// depth=1, gas=90000:  back to outer
	structlogs := []execution.StructLog{
		{Op: "CALL", Gas: 100000, GasCost: 100, Depth: 1},
		{Op: "PUSH1", Gas: 80000, GasCost: 3, Depth: 2},
		{Op: "CALL", Gas: 79000, GasCost: 100, Depth: 2},
		{Op: "PUSH1", Gas: 50000, GasCost: 3, Depth: 3},
		{Op: "STOP", Gas: 49000, GasCost: 0, Depth: 3},
		{Op: "PUSH1", Gas: 75000, GasCost: 3, Depth: 2},
		{Op: "STOP", Gas: 74000, GasCost: 0, Depth: 2},
		{Op: "PUSH1", Gas: 90000, GasCost: 3, Depth: 1},
	}

	result := ComputeGasUsed(structlogs)

	require.Len(t, result, 8)

	// Outer CALL: 100000 - 90000 = 10000 (includes all nested execution)
	assert.Equal(t, uint64(10000), result[0], "outer CALL should include all nested gas")

	// First child PUSH: 80000 - 79000 = 1000
	assert.Equal(t, uint64(1000), result[1])

	// Inner CALL: 79000 - 75000 = 4000 (includes innermost execution)
	assert.Equal(t, uint64(4000), result[2], "inner CALL should include nested gas")

	// Innermost PUSH: 50000 - 49000 = 1000
	assert.Equal(t, uint64(1000), result[3])

	// Innermost STOP: keeps pre-calculated (0)
	assert.Equal(t, uint64(0), result[4], "innermost STOP keeps pre-calculated")

	// First child PUSH after return: 75000 - 74000 = 1000
	assert.Equal(t, uint64(1000), result[5])

	// First child STOP: keeps pre-calculated (0)
	assert.Equal(t, uint64(0), result[6], "first child STOP keeps pre-calculated")

	// Outer PUSH: keeps pre-calculated (last opcode)
	assert.Equal(t, uint64(3), result[7])
}

func TestComputeGasUsed_MultipleSiblings(t *testing.T) {
	// Test: depth 1 -> depth 2 (call A) -> depth 1 -> depth 2 (call B) -> depth 1
	// This tests that pending indices are properly cleared between sibling calls
	structlogs := []execution.StructLog{
		// First call to depth 2
		{Op: "CALL", Gas: 100000, GasCost: 100, Depth: 1},
		{Op: "PUSH1", Gas: 60000, GasCost: 3, Depth: 2},
		{Op: "STOP", Gas: 59000, GasCost: 0, Depth: 2},
		{Op: "PUSH1", Gas: 90000, GasCost: 3, Depth: 1},
		// Second call to depth 2 (sibling)
		{Op: "CALL", Gas: 89000, GasCost: 100, Depth: 1},
		{Op: "PUSH1", Gas: 50000, GasCost: 3, Depth: 2},
		{Op: "STOP", Gas: 49000, GasCost: 0, Depth: 2},
		{Op: "PUSH1", Gas: 80000, GasCost: 3, Depth: 1},
	}

	result := ComputeGasUsed(structlogs)

	require.Len(t, result, 8)

	// First CALL: 100000 - 90000 = 10000
	assert.Equal(t, uint64(10000), result[0], "first CALL gas")

	// First child PUSH: 60000 - 59000 = 1000
	assert.Equal(t, uint64(1000), result[1])

	// First child STOP: keeps pre-calculated
	assert.Equal(t, uint64(0), result[2])

	// First return PUSH: 90000 - 89000 = 1000
	assert.Equal(t, uint64(1000), result[3])

	// Second CALL: 89000 - 80000 = 9000
	assert.Equal(t, uint64(9000), result[4], "second CALL gas")

	// Second child PUSH: 50000 - 49000 = 1000
	assert.Equal(t, uint64(1000), result[5])

	// Second child STOP: keeps pre-calculated
	assert.Equal(t, uint64(0), result[6])

	// Last PUSH: keeps pre-calculated (last opcode)
	assert.Equal(t, uint64(3), result[7])
}

func TestComputeGasUsed_CreateOpcode(t *testing.T) {
	// CREATE starts a new contract creation context at higher depth
	structlogs := []execution.StructLog{
		{Op: "CREATE", Gas: 100000, GasCost: 32000, Depth: 1},
		{Op: "PUSH1", Gas: 70000, GasCost: 3, Depth: 2},
		{Op: "RETURN", Gas: 69000, GasCost: 0, Depth: 2},
		{Op: "POP", Gas: 80000, GasCost: 2, Depth: 1},
	}

	result := ComputeGasUsed(structlogs)

	require.Len(t, result, 4)

	// CREATE: 100000 - 80000 = 20000
	assert.Equal(t, uint64(20000), result[0])

	// Constructor PUSH: 70000 - 69000 = 1000
	assert.Equal(t, uint64(1000), result[1])

	// Constructor RETURN: keeps pre-calculated (last in call context)
	assert.Equal(t, uint64(0), result[2])

	// POP: keeps pre-calculated (last opcode)
	assert.Equal(t, uint64(2), result[3])
}

func TestComputeGasUsed_RevertOpcode(t *testing.T) {
	// REVERT is similar to RETURN but indicates failure
	structlogs := []execution.StructLog{
		{Op: "CALL", Gas: 100000, GasCost: 100, Depth: 1},
		{Op: "PUSH1", Gas: 63000, GasCost: 3, Depth: 2},
		{Op: "PUSH1", Gas: 62000, GasCost: 3, Depth: 2},
		{Op: "REVERT", Gas: 61000, GasCost: 5, Depth: 2},
		{Op: "ISZERO", Gas: 96000, GasCost: 3, Depth: 1},
	}

	result := ComputeGasUsed(structlogs)

	require.Len(t, result, 5)

	// CALL: 100000 - 96000 = 4000
	assert.Equal(t, uint64(4000), result[0])

	// First PUSH: 63000 - 62000 = 1000
	assert.Equal(t, uint64(1000), result[1])

	// Second PUSH: 62000 - 61000 = 1000
	assert.Equal(t, uint64(1000), result[2])

	// REVERT: keeps pre-calculated (last in call context)
	assert.Equal(t, uint64(5), result[3])

	// ISZERO: keeps pre-calculated (last opcode)
	assert.Equal(t, uint64(3), result[4])
}

func TestComputeGasUsed_DepthZero(t *testing.T) {
	// Some traces may have depth starting at 0
	structlogs := []execution.StructLog{
		{Op: "PUSH1", Gas: 100000, GasCost: 3, Depth: 0},
		{Op: "PUSH1", Gas: 99997, GasCost: 3, Depth: 0},
		{Op: "ADD", Gas: 99994, GasCost: 3, Depth: 0},
	}

	result := ComputeGasUsed(structlogs)

	require.Len(t, result, 3)
	assert.Equal(t, uint64(3), result[0])
	assert.Equal(t, uint64(3), result[1])
	assert.Equal(t, uint64(3), result[2])
}

func TestComputeGasUsed_DynamicGasCost(t *testing.T) {
	// Simulate SLOAD with varying actual costs (cold vs warm access)
	// Pre-calculated cost might be 2100 (cold) but actual could differ
	structlogs := []execution.StructLog{
		{Op: "PUSH1", Gas: 100000, GasCost: 3, Depth: 1},
		{Op: "SLOAD", Gas: 99997, GasCost: 2100, Depth: 1}, // Pre-calculated cold cost
		{Op: "POP", Gas: 97897, GasCost: 2, Depth: 1},      // Actual: 99997 - 97897 = 2100
	}

	result := ComputeGasUsed(structlogs)

	require.Len(t, result, 3)

	// PUSH1: 100000 - 99997 = 3
	assert.Equal(t, uint64(3), result[0])

	// SLOAD: 99997 - 97897 = 2100 (matches pre-calculated in this case)
	assert.Equal(t, uint64(2100), result[1])

	// POP: keeps pre-calculated (last opcode)
	assert.Equal(t, uint64(2), result[2])
}

func TestComputeGasUsed_LargeDepth(t *testing.T) {
	// Test with deeper nesting (typical max is ~1024)
	structlogs := []execution.StructLog{
		{Op: "CALL", Gas: 100000, GasCost: 100, Depth: 1},
		{Op: "CALL", Gas: 90000, GasCost: 100, Depth: 2},
		{Op: "CALL", Gas: 80000, GasCost: 100, Depth: 3},
		{Op: "CALL", Gas: 70000, GasCost: 100, Depth: 4},
		{Op: "PUSH1", Gas: 60000, GasCost: 3, Depth: 5},
		{Op: "STOP", Gas: 59997, GasCost: 0, Depth: 5},
		{Op: "POP", Gas: 65000, GasCost: 2, Depth: 4},
		{Op: "POP", Gas: 74000, GasCost: 2, Depth: 3},
		{Op: "POP", Gas: 83000, GasCost: 2, Depth: 2},
		{Op: "POP", Gas: 92000, GasCost: 2, Depth: 1},
	}

	result := ComputeGasUsed(structlogs)

	require.Len(t, result, 10)

	// Depth 1 CALL: 100000 - 92000 = 8000
	assert.Equal(t, uint64(8000), result[0])

	// Depth 2 CALL: 90000 - 83000 = 7000
	assert.Equal(t, uint64(7000), result[1])

	// Depth 3 CALL: 80000 - 74000 = 6000
	assert.Equal(t, uint64(6000), result[2])

	// Depth 4 CALL: 70000 - 65000 = 5000
	assert.Equal(t, uint64(5000), result[3])

	// Depth 5 PUSH: 60000 - 59997 = 3
	assert.Equal(t, uint64(3), result[4])

	// Depth 5 STOP: keeps pre-calculated
	assert.Equal(t, uint64(0), result[5])

	// All POPs: keeps pre-calculated (last at their respective depths when returning)
	assert.Equal(t, uint64(2), result[6])
	assert.Equal(t, uint64(2), result[7])
	assert.Equal(t, uint64(2), result[8])
	assert.Equal(t, uint64(2), result[9])
}
