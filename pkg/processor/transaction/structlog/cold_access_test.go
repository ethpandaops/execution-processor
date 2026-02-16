package structlog

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

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
			addr := "0x1234567890123456789012345678901234567890"
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
	addr := "0x1234567890123456789012345678901234567890"

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
	addr := "0x1234567890123456789012345678901234567890"

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
		{"0x1234567890123456789012345678901234567890", false},
		{"0x0000000000000000000000000000000000000000", false},
	}

	for _, tc := range tests {
		t.Run(tc.addr, func(t *testing.T) {
			assert.Equal(t, tc.expected, IsPrecompile(tc.addr))
		})
	}
}
