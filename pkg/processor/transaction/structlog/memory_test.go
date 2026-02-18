package structlog

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

// ---------------------------------------------------------------------------
// Helper unit tests
// ---------------------------------------------------------------------------

func TestParseHexUint64(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected uint64
	}{
		{"zero", "0", 0},
		{"zero with prefix", "0x0", 0},
		{"small value", "0x20", 32},
		{"large value", "0xde0b6b3a7640000", 1000000000000000000},
		{"max uint64", "0xffffffffffffffff", math.MaxUint64},
		{"overflow returns 0", "0x10000000000000000", 0},
		{"empty string", "", 0},
		{"just prefix", "0x", 0},
		{"no prefix", "ff", 255},
		{"malformed", "0xZZZZ", 0},
		{"single char", "a", 10},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.expected, parseHexUint64(tc.input))
		})
	}
}

func TestReturnEndBytes(t *testing.T) {
	tests := []struct {
		name     string
		stack    *[]string
		expected uint32
	}{
		{"nil stack", nil, 0},
		{"stack too short", &[]string{"100"}, 0},
		{"zero offset+size", &[]string{"0", "0"}, 0},
		{"simple case", &[]string{"0", "80"}, 128},
		{"offset+size", &[]string{"20", "40"}, 96},
		{"large offset+size", &[]string{"100", "100"}, 512},
		{"offset overflow clamps", &[]string{"ffffffffffffffff", "1"}, math.MaxUint32},
		{"size overflow clamps", &[]string{"1", "ffffffffffffffff"}, math.MaxUint32},
		{"both max clamps", &[]string{"ffffffffffffffff", "ffffffffffffffff"}, math.MaxUint32},
		{"exactly max uint32", &[]string{"0", "ffffffff"}, math.MaxUint32},
		{"stack with extra elements", &[]string{"0", "0", "0", "20", "40"}, 96},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sl := &execution.StructLog{
				Op:    "RETURN",
				Stack: tc.stack,
			}

			assert.Equal(t, tc.expected, returnEndBytes(sl))
		})
	}
}

func TestComputeMemoryWords_SingleOpcode(t *testing.T) {
	// Single opcode trace: wordsBefore from MemorySize, wordsAfter = wordsBefore.
	structlogs := []execution.StructLog{
		{Op: "STOP", Depth: 1, MemorySize: 64},
	}

	wb, wa := ComputeMemoryWords(structlogs)
	require.NotNil(t, wb)
	assert.Equal(t, uint32(2), wb[0])
	assert.Equal(t, uint32(2), wa[0])
}

func TestComputeMemoryWords_NestedDepthReturn(t *testing.T) {
	// depth 1 -> 2 -> 3 -> 2 -> 1, RETURN at depth 3 expands memory.
	stack3 := []string{"0", "100"} // offset=0, size=0x100=256

	structlogs := []execution.StructLog{
		{Op: "CALL", Depth: 1, MemorySize: 32},                   // 1 word
		{Op: "CALL", Depth: 2, MemorySize: 0},                    // 0 words
		{Op: "MSTORE", Depth: 3, MemorySize: 0},                  // 0 words
		{Op: "RETURN", Depth: 3, MemorySize: 64, Stack: &stack3}, // 2 words, return needs 8
		{Op: "STOP", Depth: 2, MemorySize: 0},                    // back to depth 2
		{Op: "STOP", Depth: 1, MemorySize: 32},                   // back to depth 1
	}

	wb, wa := ComputeMemoryWords(structlogs)
	require.NotNil(t, wb)

	// Op 3 (RETURN, last at depth 3): wordsBefore=2, wordsAfter=ceil(256/32)=8.
	assert.Equal(t, uint32(2), wb[3])
	assert.Equal(t, uint32(8), wa[3])
}

func TestComputeMemoryWords_Empty(t *testing.T) {
	wb, wa := ComputeMemoryWords(nil)
	assert.Nil(t, wb)
	assert.Nil(t, wa)
}

func TestComputeMemoryWords_NoMemoryData(t *testing.T) {
	// RPC mode: all MemorySize values are 0.
	structlogs := []execution.StructLog{
		{Op: "PUSH1", Depth: 1, MemorySize: 0},
		{Op: "ADD", Depth: 1, MemorySize: 0},
	}

	wb, wa := ComputeMemoryWords(structlogs)
	assert.Nil(t, wb)
	assert.Nil(t, wa)
}

func TestComputeMemoryWords_SameDepthSequence(t *testing.T) {
	// Three opcodes at same depth with growing memory.
	// Memory: 0 -> 32 -> 96 bytes (0 -> 1 -> 3 words).
	structlogs := []execution.StructLog{
		{Op: "PUSH1", Depth: 1, MemorySize: 0},
		{Op: "MSTORE", Depth: 1, MemorySize: 32},
		{Op: "MSTORE", Depth: 1, MemorySize: 96},
	}

	wb, wa := ComputeMemoryWords(structlogs)
	require.NotNil(t, wb)
	require.NotNil(t, wa)

	// Op 0: wordsBefore=0, wordsAfter=ceil(32/32)=1
	assert.Equal(t, uint32(0), wb[0])
	assert.Equal(t, uint32(1), wa[0])

	// Op 1: wordsBefore=1, wordsAfter=ceil(96/32)=3
	assert.Equal(t, uint32(1), wb[1])
	assert.Equal(t, uint32(3), wa[1])

	// Op 2: last opcode, wordsAfter=wordsBefore=3
	assert.Equal(t, uint32(3), wb[2])
	assert.Equal(t, uint32(3), wa[2])
}

func TestComputeMemoryWords_DepthTransition(t *testing.T) {
	// depth 1 -> depth 2 -> back to depth 1
	structlogs := []execution.StructLog{
		{Op: "CALL", Depth: 1, MemorySize: 64},    // 2 words
		{Op: "PUSH1", Depth: 2, MemorySize: 0},    // 0 words (child frame starts fresh)
		{Op: "MSTORE", Depth: 2, MemorySize: 32},  // 1 word
		{Op: "RETURN", Depth: 2, MemorySize: 128}, // 4 words
		{Op: "POP", Depth: 1, MemorySize: 64},     // back to parent: 2 words
	}

	wb, wa := ComputeMemoryWords(structlogs)
	require.NotNil(t, wb)

	// Op 0 (CALL, depth 1): wordsBefore=2, wordsAfter resolved when we return to depth 1 (Op 4)
	assert.Equal(t, uint32(2), wb[0])
	assert.Equal(t, uint32(2), wa[0]) // POP at depth 1 has memSize=64 -> 2 words

	// Op 1 (depth 2): wordsBefore=0, wordsAfter=1
	assert.Equal(t, uint32(0), wb[1])
	assert.Equal(t, uint32(1), wa[1])

	// Op 2 (depth 2): wordsBefore=1, wordsAfter=4
	assert.Equal(t, uint32(1), wb[2])
	assert.Equal(t, uint32(4), wa[2])

	// Op 3 (RETURN, depth 2): last at depth 2, wordsAfter=wordsBefore=4
	assert.Equal(t, uint32(4), wb[3])
	assert.Equal(t, uint32(4), wa[3])

	// Op 4 (POP, depth 1): last opcode, wordsAfter=wordsBefore=2
	assert.Equal(t, uint32(2), wb[4])
	assert.Equal(t, uint32(2), wa[4])
}

func TestComputeMemoryWords_ReturnExpandsMemory(t *testing.T) {
	// RETURN with offset=0, size=256 should expand memory beyond the 128 bytes
	// already allocated. Stack: [offset, size] = top-of-stack first.
	stack := []string{"0", "100"} // offset=0, size=0x100=256

	structlogs := []execution.StructLog{
		{Op: "PUSH1", Depth: 1, MemorySize: 64},                  // 2 words
		{Op: "MSTORE", Depth: 1, MemorySize: 64},                 // 2 words
		{Op: "RETURN", Depth: 1, MemorySize: 128, Stack: &stack}, // 4 words before, RETURN needs ceil(256/32)=8 words
	}

	wb, wa := ComputeMemoryWords(structlogs)
	require.NotNil(t, wb)

	// Op 2 (RETURN, last in frame): wordsBefore=4, wordsAfter=ceil(256/32)=8.
	assert.Equal(t, uint32(4), wb[2])
	assert.Equal(t, uint32(8), wa[2])

	// Verify expansion gas is non-zero.
	gas := MemoryExpansionGas(wb[2], wa[2])
	// cost(8) - cost(4) = (24 + 64/512) - (12 + 16/512) = 24 - 12 = 12
	assert.Equal(t, uint64(12), gas)
}

func TestComputeMemoryWords_RevertExpandsMemory(t *testing.T) {
	// REVERT with offset=32, size=64 should expand to ceil((32+64)/32)=3 words.
	stack := []string{"20", "40"} // offset=0x20=32, size=0x40=64

	structlogs := []execution.StructLog{
		{Op: "PUSH1", Depth: 1, MemorySize: 32}, // 1 word
		{Op: "REVERT", Depth: 1, MemorySize: 32, Stack: &stack},
	}

	wb, wa := ComputeMemoryWords(structlogs)
	require.NotNil(t, wb)

	// Op 1 (REVERT): wordsBefore=1, wordsAfter=ceil(96/32)=3.
	assert.Equal(t, uint32(1), wb[1])
	assert.Equal(t, uint32(3), wa[1])
}

func TestComputeMemoryWords_ReturnNoExpansion(t *testing.T) {
	// RETURN with offset=0, size=32 — memory already has 64 bytes, no expansion.
	stack := []string{"0", "20"} // offset=0, size=0x20=32

	structlogs := []execution.StructLog{
		{Op: "PUSH1", Depth: 1, MemorySize: 64},
		{Op: "RETURN", Depth: 1, MemorySize: 64, Stack: &stack},
	}

	wb, wa := ComputeMemoryWords(structlogs)
	require.NotNil(t, wb)

	// Op 1 (RETURN): wordsBefore=2, wordsAfter=max(2, ceil(32/32)=1) = 2.
	assert.Equal(t, uint32(2), wb[1])
	assert.Equal(t, uint32(2), wa[1])
}

func TestComputeMemoryWords_ReturnNoStack(t *testing.T) {
	// RETURN without stack data (embedded mode) — falls back to wordsBefore.
	structlogs := []execution.StructLog{
		{Op: "PUSH1", Depth: 1, MemorySize: 64},
		{Op: "RETURN", Depth: 1, MemorySize: 128},
	}

	wb, wa := ComputeMemoryWords(structlogs)
	require.NotNil(t, wb)

	// Op 1 (RETURN, no stack): wordsAfter=wordsBefore=4.
	assert.Equal(t, uint32(4), wb[1])
	assert.Equal(t, uint32(4), wa[1])
}

func TestResolveLastInFrame(t *testing.T) {
	tests := []struct {
		name     string
		op       string
		memSize  uint32
		stack    *[]string
		expected uint32
	}{
		{
			name:     "RETURN expands memory",
			op:       "RETURN",
			memSize:  64,                   // 2 words before
			stack:    &[]string{"0", "80"}, // offset=0, size=0x80=128 → ceil(128/32)=4
			expected: 4,
		},
		{
			name:     "REVERT expands memory",
			op:       "REVERT",
			memSize:  32,                    // 1 word before
			stack:    &[]string{"20", "40"}, // offset=0x20=32, size=0x40=64 → ceil(96/32)=3
			expected: 3,
		},
		{
			name:     "RETURN no expansion (within existing)",
			op:       "RETURN",
			memSize:  128,                  // 4 words before
			stack:    &[]string{"0", "20"}, // offset=0, size=0x20=32 → ceil(32/32)=1, max(4,1)=4
			expected: 4,
		},
		{
			name:     "RETURN zero size",
			op:       "RETURN",
			memSize:  64,                  // 2 words before
			stack:    &[]string{"0", "0"}, // offset=0, size=0 → 0, fallback to 2
			expected: 2,
		},
		{
			name:     "RETURN no stack (embedded mode)",
			op:       "RETURN",
			memSize:  96, // 3 words before
			stack:    nil,
			expected: 3,
		},
		{
			name:     "STOP (non-RETURN opcode)",
			op:       "STOP",
			memSize:  64,                   // 2 words before
			stack:    &[]string{"0", "80"}, // stack ignored for non-RETURN
			expected: 2,
		},
		{
			name:     "RETURN offset+size causes expansion",
			op:       "RETURN",
			memSize:  0,                       // 0 words before
			stack:    &[]string{"100", "100"}, // offset=0x100=256, size=0x100=256 → ceil(512/32)=16
			expected: 16,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			sl := &execution.StructLog{
				Op:         tc.op,
				MemorySize: tc.memSize,
				Stack:      tc.stack,
			}

			result := resolveLastInFrame(sl)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestMemoryExpansionGas_NoExpansion(t *testing.T) {
	assert.Equal(t, uint64(0), MemoryExpansionGas(5, 5))
	assert.Equal(t, uint64(0), MemoryExpansionGas(5, 3))
}

func TestMemoryExpansionGas_SmallExpansion(t *testing.T) {
	// 0 -> 1 word: cost(1) - cost(0) = 3*1 + 1/512 - 0 = 3
	assert.Equal(t, uint64(3), MemoryExpansionGas(0, 1))
}

func TestMemoryExpansionGas_LargerExpansion(t *testing.T) {
	// 1 -> 3 words: cost(3) - cost(1) = (9 + 9/512) - (3 + 1/512) = 9 - 3 = 6
	// cost(3) = 3*3 + 9/512 = 9 + 0 = 9
	// cost(1) = 3*1 + 1/512 = 3 + 0 = 3
	assert.Equal(t, uint64(6), MemoryExpansionGas(1, 3))
}

func TestMemoryExpansionGas_QuadraticKicksIn(t *testing.T) {
	// Large expansion where quadratic term matters.
	// 0 -> 100 words: cost(100) = 3*100 + 100*100/512 = 300 + 19 = 319
	assert.Equal(t, uint64(319), MemoryExpansionGas(0, 100))
}

func TestMemoryWords_Rounding(t *testing.T) {
	assert.Equal(t, uint32(0), memoryWords(0))
	assert.Equal(t, uint32(1), memoryWords(1))
	assert.Equal(t, uint32(1), memoryWords(31))
	assert.Equal(t, uint32(1), memoryWords(32))
	assert.Equal(t, uint32(2), memoryWords(33))
	assert.Equal(t, uint32(2), memoryWords(64))
	assert.Equal(t, uint32(3), memoryWords(65))
}
