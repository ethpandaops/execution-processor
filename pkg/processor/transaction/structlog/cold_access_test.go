package structlog

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

const testNonPrecompileAddr = "0x1234567890123456789012345678901234567890"

// ---------------------------------------------------------------------------
// Helper unit tests
// ---------------------------------------------------------------------------

func TestIsHexZero(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{"empty string", "", true},
		{"single zero", "0", true},
		{"multiple zeros", "0000", true},
		{"with 0x prefix zero", "0x0", true},
		{"with 0x prefix zeros", "0x0000", true},
		{"just 0x prefix", "0x", true},
		{"non-zero single", "1", false},
		{"non-zero with prefix", "0x1", false},
		{"non-zero mixed", "0x00ff00", false},
		{"large non-zero", "de0b6b3a7640000", false},
		{"trailing non-zero", "00000001", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, isHexZero(tc.input))
		})
	}
}

func TestParseHexUint32(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected uint32
	}{
		{"zero", "0", 0},
		{"zero with prefix", "0x0", 0},
		{"small value", "0x20", 32},
		{"max uint32", "0xffffffff", math.MaxUint32},
		{"overflow clamps", "0x100000000", math.MaxUint32},
		{"large overflow", "0xdeadbeefdeadbeef", math.MaxUint32},
		{"empty string", "", 0},
		{"just prefix", "0x", 0},
		{"no prefix", "ff", 255},
		{"malformed", "0xZZZZ", math.MaxUint32},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, parseHexUint32(tc.input))
		})
	}
}

func TestCallHasValue(t *testing.T) {
	tests := []struct {
		name               string
		callTransfersValue bool
		stack              *[]string
		expected           bool
	}{
		{"embedded true", true, nil, true},
		{"embedded false no stack", false, nil, false},
		{"stack non-zero value", false, &[]string{"0", "0", "0", "0", "1", "addr", "gas"}, true},
		{"stack zero value", false, &[]string{"0", "0", "0", "0", "0", "addr", "gas"}, false},
		{"stack too short", false, &[]string{"gas", "addr"}, false},
		{"stack exactly 3", false, &[]string{"1", "addr", "gas"}, true},
		{"embedded true overrides stack", true, &[]string{"0", "0", "0", "0", "0", "addr", "gas"}, true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sl := &execution.StructLog{
				Op:                 "CALL",
				CallTransfersValue: tc.callTransfersValue,
				Stack:              tc.stack,
			}

			assert.Equal(t, tc.expected, callHasValue(sl))
		})
	}
}

func TestExtCodeCopySize(t *testing.T) {
	tests := []struct {
		name     string
		embedded uint32
		stack    *[]string
		expected uint32
	}{
		{"embedded non-zero", 128, nil, 128},
		{"embedded zero with stack", 0, &[]string{"80", "0", "0", "addr"}, 128},
		{"embedded zero no stack", 0, nil, 0},
		{"stack too short", 0, &[]string{"80", "0", "addr"}, 0},
		{"embedded takes priority", 64, &[]string{"80", "0", "0", "addr"}, 64},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sl := &execution.StructLog{
				Op:              "EXTCODECOPY",
				ExtCodeCopySize: tc.embedded,
				Stack:           tc.stack,
			}

			assert.Equal(t, tc.expected, extCodeCopySize(sl))
		})
	}
}

func TestGetMemExp(t *testing.T) {
	tests := []struct {
		name     string
		slice    []uint64
		index    int
		expected uint64
	}{
		{"nil slice", nil, 0, 0},
		{"index in range", []uint64{10, 20, 30}, 1, 20},
		{"index out of range", []uint64{10, 20}, 5, 0},
		{"index at boundary", []uint64{10, 20}, 2, 0},
		{"first element", []uint64{42}, 0, 42},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, getMemExp(tc.slice, tc.index))
		})
	}
}

// ---------------------------------------------------------------------------
// Boundary precision tests
// ---------------------------------------------------------------------------

func TestClassifySload_Boundaries(t *testing.T) {
	assert.Equal(t, uint64(0), classifySload(2099), "just below cold threshold")
	assert.Equal(t, uint64(1), classifySload(2100), "exact cold threshold")
	assert.Equal(t, uint64(1), classifySload(2101), "just above cold threshold")
}

func TestClassifySstore_Boundaries(t *testing.T) {
	// Cold variants are exact matches: 2200, 5000, 22100.
	assert.Equal(t, uint64(0), classifySstore(2199))
	assert.Equal(t, uint64(1), classifySstore(2200))
	assert.Equal(t, uint64(0), classifySstore(2201))
	assert.Equal(t, uint64(0), classifySstore(4999))
	assert.Equal(t, uint64(1), classifySstore(5000))
	assert.Equal(t, uint64(0), classifySstore(5001))
	assert.Equal(t, uint64(0), classifySstore(22099))
	assert.Equal(t, uint64(1), classifySstore(22100))
	assert.Equal(t, uint64(0), classifySstore(22101))
}

func TestClassifyAccountAccess_Boundaries(t *testing.T) {
	assert.Equal(t, uint64(0), classifyAccountAccess(2599), "just below cold threshold")
	assert.Equal(t, uint64(1), classifyAccountAccess(2600), "exact cold threshold")
	assert.Equal(t, uint64(1), classifyAccountAccess(2601), "just above cold threshold")
}

func TestClassifyCall_Boundaries(t *testing.T) {
	addr := testNonPrecompileAddr

	tests := []struct {
		name     string
		gasSelf  uint64
		expected uint64
	}{
		{"at 200 boundary (warm)", 200, 0},
		{"at 201 (gap)", 201, 0},
		{"at 2599 (gap)", 2599, 0},
		{"at 2600 (cold single)", 2600, 1},
		{"at 5199 (cold single)", 5199, 1},
		{"at 5200 (cold double)", 5200, 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sl := &execution.StructLog{Op: "STATICCALL", CallToAddress: &addr}
			assert.Equal(t, tc.expected, classifyCall(sl, tc.gasSelf, 0))
		})
	}
}

func TestClassifyCall_MemExpAndValueCombined(t *testing.T) {
	// gasSelf=11900, memExp=200, value transfer=9000 → remaining = 11900-200-9000 = 2700 → cold.
	addr := testNonPrecompileAddr
	sl := &execution.StructLog{Op: "CALL", CallToAddress: &addr, CallTransfersValue: true}

	assert.Equal(t, uint64(1), classifyCall(sl, 11900, 200))
}

func TestClassifyExtCodeCopy_Boundaries(t *testing.T) {
	// Boundary: remaining must be >= 2600 after subtracting memExp and copyCost.
	// gasSelf=2603, copyCost=3 (1 word) → remaining = 2603-3 = 2600 → cold.
	sl32 := &execution.StructLog{Op: "EXTCODECOPY", ExtCodeCopySize: 32}
	assert.Equal(t, uint64(1), classifyExtCodeCopy(sl32, 2603, 0))

	// gasSelf=2602, copyCost=3 → remaining = 2599 → warm.
	assert.Equal(t, uint64(0), classifyExtCodeCopy(sl32, 2602, 0))

	// memExp exceeds gasSelf → remaining saturates to 0 → warm.
	assert.Equal(t, uint64(0), classifyExtCodeCopy(sl32, 100, 500))
}

func TestClassifySelfdestruct_Boundaries(t *testing.T) {
	// Exact matches only.
	assert.Equal(t, uint64(0), classifySelfdestruct(7599))
	assert.Equal(t, uint64(1), classifySelfdestruct(7600))
	assert.Equal(t, uint64(0), classifySelfdestruct(7601))
	assert.Equal(t, uint64(0), classifySelfdestruct(32599))
	assert.Equal(t, uint64(1), classifySelfdestruct(32600))
	assert.Equal(t, uint64(0), classifySelfdestruct(32601))
}

func TestClassifyColdAccess_Empty(t *testing.T) {
	result := ClassifyColdAccess(nil, nil, nil)
	assert.Nil(t, result)
}

func TestClassifyColdAccess_SLOAD(t *testing.T) {
	tests := []struct {
		name     string
		gasCost  uint64
		expected uint64
	}{
		{"warm SLOAD (100)", 100, 0},
		{"cold SLOAD (2100)", 2100, 1},
		{"cold SLOAD (2200 - with extra)", 2200, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			structlogs := []execution.StructLog{
				{Op: "SLOAD", GasCost: tc.gasCost, Depth: 1},
			}
			gasSelf := []uint64{tc.gasCost}

			result := ClassifyColdAccess(structlogs, gasSelf, nil)
			assert.Equal(t, tc.expected, result[0])
		})
	}
}

func TestClassifyColdAccess_SSTORE(t *testing.T) {
	tests := []struct {
		name     string
		gasCost  uint64
		expected uint64
	}{
		{"warm noop (100)", 100, 0},
		{"cold noop (2200)", 2200, 1},
		{"warm RESET (2900)", 2900, 0},
		{"cold RESET (5000)", 5000, 1},
		{"warm SET (20000)", 20000, 0},
		{"cold SET (22100)", 22100, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			structlogs := []execution.StructLog{
				{Op: "SSTORE", GasCost: tc.gasCost, Depth: 1},
			}
			gasSelf := []uint64{tc.gasCost}

			result := ClassifyColdAccess(structlogs, gasSelf, nil)
			assert.Equal(t, tc.expected, result[0])
		})
	}
}

func TestClassifyColdAccess_AccountAccess(t *testing.T) {
	opcodes := []string{"BALANCE", "EXTCODESIZE", "EXTCODEHASH"}

	for _, opcode := range opcodes {
		t.Run(opcode+"_warm", func(t *testing.T) {
			structlogs := []execution.StructLog{
				{Op: opcode, GasCost: 100, Depth: 1},
			}

			result := ClassifyColdAccess(structlogs, []uint64{100}, nil)
			assert.Equal(t, uint64(0), result[0])
		})

		t.Run(opcode+"_cold", func(t *testing.T) {
			structlogs := []execution.StructLog{
				{Op: opcode, GasCost: 2600, Depth: 1},
			}

			result := ClassifyColdAccess(structlogs, []uint64{2600}, nil)
			assert.Equal(t, uint64(1), result[0])
		})
	}
}

func TestClassifyColdAccess_CALL_EIP7702(t *testing.T) {
	// Test CALL cold detection with various remaining gas values.
	tests := []struct {
		name     string
		gasSelf  uint64
		expected uint64
	}{
		{"warm (100)", 100, 0},
		{"warm (200)", 200, 0},
		{"cold single (2600)", 2600, 1},
		{"cold single (2700)", 2700, 1},
		{"cold double (5200)", 5200, 2},
		{"cold double (5300)", 5300, 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			addr := testNonPrecompileAddr
			structlogs := []execution.StructLog{
				{Op: "STATICCALL", GasCost: tc.gasSelf, Depth: 1, CallToAddress: &addr},
			}

			result := ClassifyColdAccess(structlogs, []uint64{tc.gasSelf}, nil)
			assert.Equal(t, tc.expected, result[0])
		})
	}
}

func TestClassifyColdAccess_CALL_WithValueTransfer(t *testing.T) {
	// CALL with value transfer: remaining = gasSelf - memExp - 9000.
	addr := testNonPrecompileAddr

	tests := []struct {
		name     string
		gasSelf  uint64
		expected uint64
	}{
		// 9100 - 9000 = 100 (warm)
		{"warm with value", 9100, 0},
		// 11600 - 9000 = 2600 (cold)
		{"cold with value", 11600, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			structlogs := []execution.StructLog{
				{
					Op: "CALL", GasCost: tc.gasSelf, Depth: 1,
					CallToAddress: &addr, CallTransfersValue: true,
				},
			}

			result := ClassifyColdAccess(structlogs, []uint64{tc.gasSelf}, nil)
			assert.Equal(t, tc.expected, result[0])
		})
	}
}

func TestClassifyColdAccess_CALL_NewAccountSubtraction(t *testing.T) {
	// CALL with value > 0 to empty account: gasSelf includes 25000 CallNewAccountGas.
	// After subtracting memExp and 9000, if remaining > 5200, subtract 25000.
	addr := testNonPrecompileAddr

	tests := []struct {
		name     string
		gasSelf  uint64
		expected uint64
	}{
		// 34100 - 9000 = 25100 (> 5200 → subtract 25000) = 100 → warm
		{"warm new account", 34100, 0},
		// 36600 - 9000 = 27600 (> 5200 → subtract 25000) = 2600 → 1 cold
		{"cold new account", 36600, 1},
		// 59200 - 9000 = 50200 (> 5200 → subtract 25000) = 25200 → still > 5200 but this
		// shouldn't happen in practice. The remaining 25200 exceeds all classification
		// ranges, so it falls into >= 5200 → 2.
		{"double cold new account (theoretical)", 59200, 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			structlogs := []execution.StructLog{
				{
					Op: "CALL", GasCost: tc.gasSelf, Depth: 1,
					CallToAddress: &addr, CallTransfersValue: true,
				},
			}

			result := ClassifyColdAccess(structlogs, []uint64{tc.gasSelf}, nil)
			assert.Equal(t, tc.expected, result[0])
		})
	}
}

func TestClassifyColdAccess_CALL_Precompile(t *testing.T) {
	// Precompile targets are always warm.
	precompileAddr := "0x0000000000000000000000000000000000000001"
	structlogs := []execution.StructLog{
		{Op: "CALL", GasCost: 5000, Depth: 1, CallToAddress: &precompileAddr},
	}

	result := ClassifyColdAccess(structlogs, []uint64{5000}, nil)
	assert.Equal(t, uint64(0), result[0])
}

func TestClassifyColdAccess_EXTCODECOPY(t *testing.T) {
	tests := []struct {
		name            string
		gasSelf         uint64
		memExp          uint64
		extCodeCopySize uint32
		expected        uint64
	}{
		// remaining = 100 - 0 - 0 = 100 (warm)
		{"warm no copy", 100, 0, 0, 0},
		// remaining = 2700 - 0 - 3*1 = 2697 (>= 2600 = cold)
		{"cold with small copy", 2700, 0, 32, 1},
		// remaining = 2700 - 100 - 3*1 = 2597 (< 2600 = warm)
		{"warm with memExp", 2700, 100, 32, 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			structlogs := []execution.StructLog{
				{Op: "EXTCODECOPY", GasCost: tc.gasSelf, Depth: 1, ExtCodeCopySize: tc.extCodeCopySize},
			}

			var memExpGas []uint64
			if tc.memExp > 0 {
				memExpGas = []uint64{tc.memExp}
			}

			result := ClassifyColdAccess(structlogs, []uint64{tc.gasSelf}, memExpGas)
			assert.Equal(t, tc.expected, result[0])
		})
	}
}

func TestClassifyColdAccess_SELFDESTRUCT(t *testing.T) {
	tests := []struct {
		name     string
		gasCost  uint64
		expected uint64
	}{
		{"warm (5000)", 5000, 0},
		{"cold target (7600)", 7600, 1},
		{"cold + new account (32600)", 32600, 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			structlogs := []execution.StructLog{
				{Op: "SELFDESTRUCT", GasCost: tc.gasCost, Depth: 1},
			}

			result := ClassifyColdAccess(structlogs, []uint64{tc.gasCost}, nil)
			assert.Equal(t, tc.expected, result[0])
		})
	}
}

func TestClassifyColdAccess_NonAccessOpcodes(t *testing.T) {
	// Non-access opcodes should always return 0.
	opcodes := []string{"ADD", "MUL", "PUSH1", "MSTORE", "JUMP", "RETURN", "STOP"}

	for _, opcode := range opcodes {
		t.Run(opcode, func(t *testing.T) {
			structlogs := []execution.StructLog{
				{Op: opcode, GasCost: 3, Depth: 1},
			}

			result := ClassifyColdAccess(structlogs, []uint64{3}, nil)
			assert.Equal(t, uint64(0), result[0])
		})
	}
}

func TestClassifyColdAccess_CALL_RPCValueFallback(t *testing.T) {
	// RPC mode: CallTransfersValue=false but stack[len-3] is non-zero.
	// Should detect value transfer via stack fallback.
	addr := testNonPrecompileAddr

	tests := []struct {
		name     string
		gasSelf  uint64
		value    string
		expected uint64
	}{
		// 11600 - 9000 = 2600 (cold) — value detected from stack.
		{"cold with stack value", 11600, "de0b6b3a7640000", 1},
		// 11600 - 0 = 11600 (>= 5200 = 2 cold) — zero value, no subtraction.
		{"no value zero stack", 11600, "0", 2},
		// 9100 - 9000 = 100 (warm) — value detected from stack.
		{"warm with stack value", 9100, "1", 0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Build stack: CALL stack is [gas, addr, value, ...] from top.
			// Stack slice is bottom-to-top, so value is at len-3.
			stack := []string{"0", "0", "0", "0", tc.value, addr, "ffffffff"}
			structlogs := []execution.StructLog{
				{
					Op: "CALL", GasCost: tc.gasSelf, Depth: 1,
					CallToAddress: &addr, Stack: &stack,
				},
			}

			result := ClassifyColdAccess(structlogs, []uint64{tc.gasSelf}, nil)
			assert.Equal(t, tc.expected, result[0])
		})
	}
}

func TestClassifyColdAccess_EXTCODECOPY_RPCSizeFallback(t *testing.T) {
	// RPC mode: ExtCodeCopySize=0 but stack[len-4] has the size.
	tests := []struct {
		name     string
		gasSelf  uint64
		sizeHex  string
		expected uint64
	}{
		// remaining = 2700 - 0 - 3*1word = 2697 (>= 2600 = cold).
		// Size 0x20=32 bytes = 1 word, copyCost = 3.
		{"cold with stack size", 2700, "20", 1},
		// remaining = 2700 - 0 - 0 = 2700 (>= 2600 = cold).
		// Size 0 = 0 words, copyCost = 0.
		{"cold zero size", 2700, "0", 1},
		// remaining = 2700 - 0 - 3*4words = 2688 (>= 2600 = cold).
		// Size 0x80=128 bytes = 4 words, copyCost = 12.
		{"cold with larger size", 2700, "80", 1},
		// remaining = 2700 - 0 - 3*32words = 2604 (>= 2600 = cold).
		// Size 0x400=1024 bytes = 32 words, copyCost = 96.
		{"cold with 1024 bytes", 2700, "400", 1},
		// remaining = 2700 - 0 - 3*33words = 2601 (>= 2600 = cold).
		// Size 0x420=1056 bytes = 33 words, copyCost = 99.
		{"borderline cold 1056 bytes", 2700, "420", 1},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// EXTCODECOPY stack: [addr, destOffset, offset, size] from top.
			// Stack slice is bottom-to-top, so size is at len-4.
			stack := []string{tc.sizeHex, "0", "0", "0xaddr"}
			structlogs := []execution.StructLog{
				{Op: "EXTCODECOPY", GasCost: tc.gasSelf, Depth: 1, Stack: &stack},
			}

			result := ClassifyColdAccess(structlogs, []uint64{tc.gasSelf}, nil)
			assert.Equal(t, tc.expected, result[0])
		})
	}
}

func TestIsPrecompile_ColdAccess(t *testing.T) {
	tests := []struct {
		addr     string
		expected bool
	}{
		{"0x0000000000000000000000000000000000000001", true},
		{"0x0000000000000000000000000000000000000009", true},
		{"0x000000000000000000000000000000000000000a", true},
		{"0x0000000000000000000000000000000000000011", true},
		{"0x0000000000000000000000000000000000000100", true},
		{"0x0000000000000000000000000000000000000012", false},
		{testNonPrecompileAddr, false},
		{"0x0000000000000000000000000000000000000000", false},
	}

	for _, tc := range tests {
		t.Run(tc.addr, func(t *testing.T) {
			assert.Equal(t, tc.expected, IsPrecompile(tc.addr))
		})
	}
}
