package structlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

// =============================================================================
// hasPrecomputedGasUsed Tests
// =============================================================================

func TestHasPrecomputedGasUsed_Empty(t *testing.T) {
	assert.False(t, hasPrecomputedGasUsed(nil))
	assert.False(t, hasPrecomputedGasUsed([]execution.StructLog{}))
}

func TestHasPrecomputedGasUsed_WithGasUsed(t *testing.T) {
	structlogs := []execution.StructLog{
		{Op: "PUSH1", GasUsed: 3},
	}
	assert.True(t, hasPrecomputedGasUsed(structlogs))
}

func TestHasPrecomputedGasUsed_WithoutGasUsed(t *testing.T) {
	structlogs := []execution.StructLog{
		{Op: "PUSH1", GasUsed: 0},
	}
	assert.False(t, hasPrecomputedGasUsed(structlogs))
}

// =============================================================================
// hasPrecomputedCreateAddresses Tests
// =============================================================================

func TestHasPrecomputedCreateAddresses_Empty(t *testing.T) {
	assert.False(t, hasPrecomputedCreateAddresses(nil))
	assert.False(t, hasPrecomputedCreateAddresses([]execution.StructLog{}))
}

func TestHasPrecomputedCreateAddresses_NoCreate(t *testing.T) {
	structlogs := []execution.StructLog{
		{Op: "PUSH1"},
		{Op: "CALL"},
	}
	assert.False(t, hasPrecomputedCreateAddresses(structlogs))
}

func TestHasPrecomputedCreateAddresses_CreateWithAddress(t *testing.T) {
	addr := "0x1234567890123456789012345678901234567890"
	structlogs := []execution.StructLog{
		{Op: "PUSH1"},
		{Op: "CREATE", CallToAddress: &addr},
	}
	assert.True(t, hasPrecomputedCreateAddresses(structlogs))
}

func TestHasPrecomputedCreateAddresses_CreateWithoutAddress(t *testing.T) {
	structlogs := []execution.StructLog{
		{Op: "PUSH1"},
		{Op: "CREATE", CallToAddress: nil},
	}
	assert.False(t, hasPrecomputedCreateAddresses(structlogs))
}

func TestHasPrecomputedCreateAddresses_Create2WithAddress(t *testing.T) {
	addr := "0x1234567890123456789012345678901234567890"
	structlogs := []execution.StructLog{
		{Op: "CREATE2", CallToAddress: &addr},
	}
	assert.True(t, hasPrecomputedCreateAddresses(structlogs))
}

// =============================================================================
// ComputeGasUsed Tests
// =============================================================================

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

func TestComputeGasUsed_NoUnderflow_CorruptedGasValues(t *testing.T) {
	// Test that corrupted/out-of-order gas values don't cause uint64 underflow.
	// In valid traces, gas[i] < gas[i-1] (gas decreases). But if a trace has
	// corrupted data where gas[i] > gas[i-1], we should NOT underflow.
	//
	// Without the fix, this would produce values like 18,446,744,073,709,551,613
	// (near uint64 max) instead of falling back to GasCost.
	structlogs := []execution.StructLog{
		{Op: "PUSH1", Gas: 100000, GasCost: 3, Depth: 1},
		{Op: "ADD", Gas: 200000, GasCost: 3, Depth: 1}, // CORRUPTED: gas increased!
		{Op: "STOP", Gas: 199997, GasCost: 0, Depth: 1},
	}

	result := ComputeGasUsed(structlogs)

	require.Len(t, result, 3)

	// PUSH1: gas[0]=100000, gas[1]=200000, so 100000 - 200000 would underflow!
	// The fix should fall back to GasCost (3) instead.
	assert.Equal(t, uint64(3), result[0], "should fall back to GasCost, not underflow")

	// Verify it's NOT a huge underflow value
	assert.Less(t, result[0], uint64(1000000), "result should be reasonable, not an underflow")

	// ADD: 200000 - 199997 = 3 (normal case, no underflow)
	assert.Equal(t, uint64(3), result[1])

	// STOP: keeps pre-calculated (last opcode)
	assert.Equal(t, uint64(0), result[2])
}

func TestComputeGasUsed_NoUnderflow_AllCorrupted(t *testing.T) {
	// Test where all gas values are corrupted (increasing instead of decreasing)
	structlogs := []execution.StructLog{
		{Op: "PUSH1", Gas: 100000, GasCost: 3, Depth: 1},
		{Op: "PUSH1", Gas: 110000, GasCost: 3, Depth: 1}, // CORRUPTED
		{Op: "ADD", Gas: 120000, GasCost: 3, Depth: 1},   // CORRUPTED
		{Op: "STOP", Gas: 130000, GasCost: 0, Depth: 1},  // CORRUPTED
	}

	result := ComputeGasUsed(structlogs)

	require.Len(t, result, 4)

	// All should fall back to GasCost since all would underflow
	for i, r := range result {
		assert.Less(t, r, uint64(1000000),
			"result[%d] should be reasonable, not an underflow", i)
	}
}

// =============================================================================
// ComputeGasSelf Tests
// =============================================================================

func TestComputeGasSelf_EmptyLogs(t *testing.T) {
	result := ComputeGasSelf(nil, nil)
	assert.Nil(t, result)

	result = ComputeGasSelf([]execution.StructLog{}, []uint64{})
	assert.Nil(t, result)
}

func TestComputeGasSelf_NonCallOpcodes(t *testing.T) {
	// For non-CALL opcodes, gas_self should equal gas_used
	structlogs := []execution.StructLog{
		{Op: "PUSH1", Gas: 100000, GasCost: 3, Depth: 1},
		{Op: "SLOAD", Gas: 99997, GasCost: 2100, Depth: 1},
		{Op: "ADD", Gas: 97897, GasCost: 3, Depth: 1},
	}

	gasUsed := []uint64{3, 2100, 3}

	result := ComputeGasSelf(structlogs, gasUsed)

	require.Len(t, result, 3)
	assert.Equal(t, uint64(3), result[0], "PUSH1 gas_self should equal gas_used")
	assert.Equal(t, uint64(2100), result[1], "SLOAD gas_self should equal gas_used")
	assert.Equal(t, uint64(3), result[2], "ADD gas_self should equal gas_used")
}

func TestComputeGasSelf_SimpleCall(t *testing.T) {
	// CALL at depth 1 with child opcodes at depth 2
	// gas_self for CALL should be gas_used minus sum of direct children's gas_used
	structlogs := []execution.StructLog{
		{Op: "PUSH1", Gas: 100000, GasCost: 3, Depth: 1}, // index 0
		{Op: "CALL", Gas: 99997, GasCost: 100, Depth: 1}, // index 1: CALL
		{Op: "PUSH1", Gas: 63000, GasCost: 3, Depth: 2},  // index 2: child
		{Op: "ADD", Gas: 62000, GasCost: 3, Depth: 2},    // index 3: child
		{Op: "STOP", Gas: 61000, GasCost: 0, Depth: 2},   // index 4: child
		{Op: "POP", Gas: 97000, GasCost: 2, Depth: 1},    // index 5: back to parent
	}

	// gas_used values (computed by ComputeGasUsed logic):
	// PUSH1[0]: 100000 - 99997 = 3
	// CALL[1]: 99997 - 97000 = 2997 (includes child execution)
	// PUSH1[2]: 63000 - 62000 = 1000
	// ADD[3]: 62000 - 61000 = 1000
	// STOP[4]: 0 (pre-calculated, last at depth 2)
	// POP[5]: 2 (pre-calculated, last opcode)
	gasUsed := []uint64{3, 2997, 1000, 1000, 0, 2}

	result := ComputeGasSelf(structlogs, gasUsed)

	require.Len(t, result, 6)

	// Non-CALL opcodes: gas_self == gas_used
	assert.Equal(t, uint64(3), result[0], "PUSH1 gas_self")
	assert.Equal(t, uint64(1000), result[2], "child PUSH1 gas_self")
	assert.Equal(t, uint64(1000), result[3], "child ADD gas_self")
	assert.Equal(t, uint64(0), result[4], "child STOP gas_self")
	assert.Equal(t, uint64(2), result[5], "POP gas_self")

	// CALL: gas_self = gas_used - sum(direct children)
	// direct children at depth 2: indices 2, 3, 4
	// sum = 1000 + 1000 + 0 = 2000
	// gas_self = 2997 - 2000 = 997
	assert.Equal(t, uint64(997), result[1], "CALL gas_self should be overhead only")
}

func TestComputeGasSelf_NestedCalls(t *testing.T) {
	// This is the critical test: nested CALLs where we must only sum direct children.
	// If we sum ALL descendants, we double count and get incorrect (often 0) values.
	//
	// Structure:
	// CALL A (depth 1) -> child frame at depth 2
	//   ├─ PUSH (depth 2)
	//   ├─ CALL B (depth 2) -> grandchild frame at depth 3
	//   │    ├─ ADD (depth 3)
	//   │    └─ STOP (depth 3)
	//   └─ STOP (depth 2)
	structlogs := []execution.StructLog{
		{Op: "CALL", Gas: 100000, GasCost: 100, Depth: 1}, // index 0: CALL A
		{Op: "PUSH1", Gas: 80000, GasCost: 3, Depth: 2},   // index 1: direct child of A
		{Op: "CALL", Gas: 79000, GasCost: 100, Depth: 2},  // index 2: CALL B (direct child of A)
		{Op: "ADD", Gas: 50000, GasCost: 3, Depth: 3},     // index 3: direct child of B
		{Op: "STOP", Gas: 49000, GasCost: 0, Depth: 3},    // index 4: direct child of B
		{Op: "STOP", Gas: 75000, GasCost: 0, Depth: 2},    // index 5: direct child of A
		{Op: "POP", Gas: 90000, GasCost: 2, Depth: 1},     // index 6: back to depth 1
	}

	// gas_used values:
	// CALL A[0]: 100000 - 90000 = 10000 (includes all nested)
	// PUSH[1]: 80000 - 79000 = 1000
	// CALL B[2]: 79000 - 75000 = 4000 (includes grandchild)
	// ADD[3]: 50000 - 49000 = 1000
	// STOP[4]: 0 (pre-calculated)
	// STOP[5]: 0 (pre-calculated)
	// POP[6]: 2 (pre-calculated)
	gasUsed := []uint64{10000, 1000, 4000, 1000, 0, 0, 2}

	result := ComputeGasSelf(structlogs, gasUsed)

	require.Len(t, result, 7)

	// CALL A: direct children at depth 2 are indices 1, 2, 5
	// sum of direct children = 1000 + 4000 + 0 = 5000
	// gas_self = 10000 - 5000 = 5000
	// Note: We do NOT include indices 3, 4 (depth 3) because they're grandchildren,
	// and CALL B's gas_used (4000) already includes them.
	assert.Equal(t, uint64(5000), result[0], "CALL A gas_self should exclude nested CALL's children")

	// CALL B: direct children at depth 3 are indices 3, 4
	// sum of direct children = 1000 + 0 = 1000
	// gas_self = 4000 - 1000 = 3000
	assert.Equal(t, uint64(3000), result[2], "CALL B gas_self should be its overhead")

	// Non-CALL opcodes: gas_self == gas_used
	assert.Equal(t, uint64(1000), result[1], "PUSH gas_self")
	assert.Equal(t, uint64(1000), result[3], "ADD gas_self")
	assert.Equal(t, uint64(0), result[4], "STOP depth 3 gas_self")
	assert.Equal(t, uint64(0), result[5], "STOP depth 2 gas_self")
	assert.Equal(t, uint64(2), result[6], "POP gas_self")
}

func TestComputeGasSelf_SiblingCalls(t *testing.T) {
	// Two sibling CALLs at the same depth, each with their own children
	structlogs := []execution.StructLog{
		{Op: "CALL", Gas: 100000, GasCost: 100, Depth: 1}, // index 0: first CALL
		{Op: "ADD", Gas: 60000, GasCost: 3, Depth: 2},     // index 1: child of first CALL
		{Op: "STOP", Gas: 59000, GasCost: 0, Depth: 2},    // index 2: child of first CALL
		{Op: "CALL", Gas: 90000, GasCost: 100, Depth: 1},  // index 3: second CALL
		{Op: "MUL", Gas: 50000, GasCost: 5, Depth: 2},     // index 4: child of second CALL
		{Op: "STOP", Gas: 49000, GasCost: 0, Depth: 2},    // index 5: child of second CALL
		{Op: "POP", Gas: 80000, GasCost: 2, Depth: 1},     // index 6
	}

	// gas_used:
	// CALL[0]: 100000 - 90000 = 10000
	// ADD[1]: 60000 - 59000 = 1000
	// STOP[2]: 0
	// CALL[3]: 90000 - 80000 = 10000
	// MUL[4]: 50000 - 49000 = 1000
	// STOP[5]: 0
	// POP[6]: 2
	gasUsed := []uint64{10000, 1000, 0, 10000, 1000, 0, 2}

	result := ComputeGasSelf(structlogs, gasUsed)

	require.Len(t, result, 7)

	// First CALL: direct children = indices 1, 2
	// gas_self = 10000 - (1000 + 0) = 9000
	assert.Equal(t, uint64(9000), result[0], "first CALL gas_self")

	// Second CALL: direct children = indices 4, 5
	// gas_self = 10000 - (1000 + 0) = 9000
	assert.Equal(t, uint64(9000), result[3], "second CALL gas_self")
}

func TestComputeGasSelf_CreateOpcode(t *testing.T) {
	// CREATE should be handled the same as CALL
	structlogs := []execution.StructLog{
		{Op: "CREATE", Gas: 100000, GasCost: 32000, Depth: 1}, // index 0
		{Op: "PUSH1", Gas: 70000, GasCost: 3, Depth: 2},       // index 1: constructor
		{Op: "RETURN", Gas: 69000, GasCost: 0, Depth: 2},      // index 2: constructor
		{Op: "POP", Gas: 80000, GasCost: 2, Depth: 1},         // index 3
	}

	// gas_used:
	// CREATE[0]: 100000 - 80000 = 20000
	// PUSH[1]: 70000 - 69000 = 1000
	// RETURN[2]: 0
	// POP[3]: 2
	gasUsed := []uint64{20000, 1000, 0, 2}

	result := ComputeGasSelf(structlogs, gasUsed)

	require.Len(t, result, 4)

	// CREATE: direct children = indices 1, 2
	// gas_self = 20000 - (1000 + 0) = 19000
	assert.Equal(t, uint64(19000), result[0], "CREATE gas_self should be overhead only")
	assert.Equal(t, uint64(1000), result[1], "PUSH gas_self")
	assert.Equal(t, uint64(0), result[2], "RETURN gas_self")
	assert.Equal(t, uint64(2), result[3], "POP gas_self")
}

func TestComputeGasSelf_DelegateCallAndStaticCall(t *testing.T) {
	// DELEGATECALL and STATICCALL should also be handled
	structlogs := []execution.StructLog{
		{Op: "DELEGATECALL", Gas: 100000, GasCost: 100, Depth: 1},
		{Op: "ADD", Gas: 60000, GasCost: 3, Depth: 2},
		{Op: "STOP", Gas: 59000, GasCost: 0, Depth: 2},
		{Op: "STATICCALL", Gas: 90000, GasCost: 100, Depth: 1},
		{Op: "MUL", Gas: 50000, GasCost: 5, Depth: 2},
		{Op: "STOP", Gas: 49000, GasCost: 0, Depth: 2},
		{Op: "POP", Gas: 80000, GasCost: 2, Depth: 1},
	}

	gasUsed := []uint64{10000, 1000, 0, 10000, 1000, 0, 2}

	result := ComputeGasSelf(structlogs, gasUsed)

	require.Len(t, result, 7)

	// DELEGATECALL: gas_self = 10000 - 1000 = 9000
	assert.Equal(t, uint64(9000), result[0], "DELEGATECALL gas_self")

	// STATICCALL: gas_self = 10000 - 1000 = 9000
	assert.Equal(t, uint64(9000), result[3], "STATICCALL gas_self")
}

func TestComputeGasSelf_CallWithNoChildren(t *testing.T) {
	// CALL to precompile or empty contract - no child opcodes
	// In this case, gas_self should equal gas_used
	structlogs := []execution.StructLog{
		{Op: "CALL", Gas: 100000, GasCost: 100, Depth: 1},
		{Op: "POP", Gas: 97400, GasCost: 2, Depth: 1}, // immediately back at depth 1
	}

	// gas_used:
	// CALL: 100000 - 97400 = 2600 (just the CALL overhead, no child execution)
	// POP: 2
	gasUsed := []uint64{2600, 2}

	result := ComputeGasSelf(structlogs, gasUsed)

	require.Len(t, result, 2)

	// No children, so gas_self = gas_used
	assert.Equal(t, uint64(2600), result[0], "CALL with no children: gas_self == gas_used")
	assert.Equal(t, uint64(2), result[1], "POP gas_self")
}

func TestComputeGasSelf_DeeplyNestedCalls(t *testing.T) {
	// Test 4 levels of nesting to ensure correct handling
	structlogs := []execution.StructLog{
		{Op: "CALL", Gas: 100000, GasCost: 100, Depth: 1}, // index 0: A
		{Op: "CALL", Gas: 90000, GasCost: 100, Depth: 2},  // index 1: B
		{Op: "CALL", Gas: 80000, GasCost: 100, Depth: 3},  // index 2: C
		{Op: "CALL", Gas: 70000, GasCost: 100, Depth: 4},  // index 3: D
		{Op: "ADD", Gas: 60000, GasCost: 3, Depth: 5},     // index 4: innermost
		{Op: "STOP", Gas: 59000, GasCost: 0, Depth: 5},    // index 5
		{Op: "STOP", Gas: 65000, GasCost: 0, Depth: 4},    // index 6
		{Op: "STOP", Gas: 74000, GasCost: 0, Depth: 3},    // index 7
		{Op: "STOP", Gas: 83000, GasCost: 0, Depth: 2},    // index 8
		{Op: "POP", Gas: 92000, GasCost: 2, Depth: 1},     // index 9
	}

	// gas_used:
	// A[0]: 100000 - 92000 = 8000
	// B[1]: 90000 - 83000 = 7000
	// C[2]: 80000 - 74000 = 6000
	// D[3]: 70000 - 65000 = 5000
	// ADD[4]: 60000 - 59000 = 1000
	// STOP[5]: 0
	// STOP[6]: 0
	// STOP[7]: 0
	// STOP[8]: 0
	// POP[9]: 2
	gasUsed := []uint64{8000, 7000, 6000, 5000, 1000, 0, 0, 0, 0, 2}

	result := ComputeGasSelf(structlogs, gasUsed)

	require.Len(t, result, 10)

	// CALL A: direct children at depth 2 = [B, STOP] = indices 1, 8
	// gas_self = 8000 - (7000 + 0) = 1000
	assert.Equal(t, uint64(1000), result[0], "CALL A gas_self")

	// CALL B: direct children at depth 3 = [C, STOP] = indices 2, 7
	// gas_self = 7000 - (6000 + 0) = 1000
	assert.Equal(t, uint64(1000), result[1], "CALL B gas_self")

	// CALL C: direct children at depth 4 = [D, STOP] = indices 3, 6
	// gas_self = 6000 - (5000 + 0) = 1000
	assert.Equal(t, uint64(1000), result[2], "CALL C gas_self")

	// CALL D: direct children at depth 5 = [ADD, STOP] = indices 4, 5
	// gas_self = 5000 - (1000 + 0) = 4000
	assert.Equal(t, uint64(4000), result[3], "CALL D gas_self")
}
