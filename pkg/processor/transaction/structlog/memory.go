package structlog

import (
	"math"
	"strconv"
	"strings"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

// ComputeMemoryWords computes the EVM memory size in 32-byte words before and
// after each opcode executes. This is derived from the MemorySize field which
// captures the memory length at opcode entry.
//
// For consecutive same-depth opcodes:
//   - wordsBefore[i] = ceil(structlogs[i].MemorySize / 32)
//   - wordsAfter[i]  = ceil(structlogs[i+1].MemorySize / 32)
//
// At depth transitions and for the last opcode in a frame, wordsAfter = wordsBefore
// (we assume no expansion since we can't observe the post-execution state),
// except for RETURN/REVERT which compute wordsAfter from their stack operands
// (offset + size) since these opcodes can expand memory.
//
// Returns (nil, nil) if MemorySize data is unavailable (RPC mode where MemorySize=0).
func ComputeMemoryWords(structlogs []execution.StructLog) (wordsBefore, wordsAfter []uint32) {
	if len(structlogs) == 0 {
		return nil, nil
	}

	// Detect if MemorySize data is available. In RPC mode all values are 0.
	// Heuristic: if the first structlog has MemorySize=0, check if ANY structlog
	// has non-zero MemorySize. If none do, data is unavailable.
	hasData := false

	for i := range structlogs {
		if structlogs[i].MemorySize > 0 {
			hasData = true

			break
		}
	}

	if !hasData {
		return nil, nil
	}

	wordsBefore = make([]uint32, len(structlogs))
	wordsAfter = make([]uint32, len(structlogs))

	// pendingIdx[depth] = index of the pending opcode at that depth.
	// Same pattern as ComputeGasUsed - deferred resolution across same-depth opcodes.
	pendingIdx := make([]int, 0, 16)

	for i := range structlogs {
		depthU64 := structlogs[i].Depth
		if depthU64 > math.MaxInt {
			depthU64 = math.MaxInt
		}

		depth := int(depthU64) //nolint:gosec // overflow checked above

		// Ensure slice has enough space for this depth.
		for len(pendingIdx) <= depth {
			pendingIdx = append(pendingIdx, -1)
		}

		// Clear pending indices from deeper levels (returned from calls).
		for d := len(pendingIdx) - 1; d > depth; d-- {
			if prevIdx := pendingIdx[d]; prevIdx >= 0 {
				wordsAfter[prevIdx] = resolveLastInFrame(&structlogs[prevIdx])
			}

			pendingIdx[d] = -1
		}

		// Resolve pending opcode at current depth: its wordsAfter is our wordsBefore.
		wb := memoryWords(structlogs[i].MemorySize)
		wordsBefore[i] = wb

		if prevIdx := pendingIdx[depth]; prevIdx >= 0 && prevIdx < len(structlogs) {
			wordsAfter[prevIdx] = wb
		}

		pendingIdx[depth] = i
	}

	// Finalize any remaining pending opcodes (last in their depth).
	for d := range pendingIdx {
		if prevIdx := pendingIdx[d]; prevIdx >= 0 {
			wordsAfter[prevIdx] = resolveLastInFrame(&structlogs[prevIdx])
		}
	}

	return wordsBefore, wordsAfter
}

// resolveLastInFrame determines wordsAfter for the last opcode in a call frame.
// For RETURN/REVERT, computes ceil((offset + size) / 32) from the stack operands
// since these opcodes can expand memory. Returns the larger of wordsBefore and
// the stack-derived value to avoid undercounting.
// For all other opcodes, falls back to wordsBefore (no observable expansion).
func resolveLastInFrame(sl *execution.StructLog) uint32 {
	wb := memoryWords(sl.MemorySize)

	if sl.Op != "RETURN" && sl.Op != "REVERT" {
		return wb
	}

	// RETURN/REVERT stack layout: [offset, size, ...] (top-of-stack first).
	// Try embedded ReturnSize field first, then fall back to stack.
	endBytes := returnEndBytes(sl)
	if endBytes == 0 {
		return wb
	}

	wa := memoryWords(endBytes)

	// Memory only grows; use the larger value.
	if wa > wb {
		return wa
	}

	return wb
}

// returnEndBytes computes offset+size for RETURN/REVERT from the stack.
// Returns 0 if stack is unavailable or both operands are zero.
func returnEndBytes(sl *execution.StructLog) uint32 {
	if sl.Stack == nil || len(*sl.Stack) < 2 {
		return 0
	}

	stack := *sl.Stack
	offset := parseHexUint64(stack[len(stack)-1])
	size := parseHexUint64(stack[len(stack)-2])

	end := offset + size
	// Overflow or exceeding uint32 range: clamp.
	if end < offset || end > math.MaxUint32 {
		return math.MaxUint32
	}

	return uint32(end)
}

// parseHexUint64 parses a hex string (with optional 0x prefix) to uint64.
// Returns 0 on parse error.
func parseHexUint64(s string) uint64 {
	hex := strings.TrimPrefix(s, "0x")
	if hex == "" {
		return 0
	}

	v, err := strconv.ParseUint(hex, 16, 64)
	if err != nil {
		return 0
	}

	return v
}

// MemoryExpansionGas computes the gas cost of expanding EVM memory from
// wordsBefore to wordsAfter words. Returns 0 if no expansion occurred.
//
// The EVM memory cost formula is: cost = 3*words + words²/512.
// Memory expansion gas = cost(wordsAfter) - cost(wordsBefore).
func MemoryExpansionGas(wordsBefore, wordsAfter uint32) uint64 {
	if wordsAfter <= wordsBefore {
		return 0
	}

	return memoryCost(uint64(wordsAfter)) - memoryCost(uint64(wordsBefore))
}

// memoryCost computes the EVM memory gas cost for a given number of words.
// Formula: 3 * words + words² / 512.
func memoryCost(words uint64) uint64 {
	return 3*words + (words*words)/512
}

// memoryWords returns ceil(byteSize / 32) — the number of 32-byte words.
func memoryWords(byteSize uint32) uint32 {
	return (byteSize + 31) / 32
}
