package structlog

import (
	"math"
	"strconv"
	"strings"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

// EVM gas constants for cold/warm access detection (EIP-2929).
const (
	warmAccessCost  = 100
	coldSloadCost   = 2100
	coldAccountCost = 2600

	// CALL value transfer: charged when CALL/CALLCODE transfers non-zero ETH.
	// The 2300 CallStipend is added to the callee's gas, NOT subtracted from the caller's cost.
	callValueTransferGas = 9000

	// Minimum word copy cost for EXTCODECOPY (3 gas per word).
	wordCopyCost = 3
)

// ClassifyColdAccess returns per-opcode cold access counts (0, 1, or 2).
// It uses gas values and memory expansion costs to determine whether each
// access-list-affected opcode performed a cold or warm access.
//
// The gasSelf parameter should contain the gas excluding child frame gas
// (as computed by ComputeGasSelf). The memExpGas parameter contains the
// memory expansion gas for each opcode (nil if memory data is unavailable,
// in which case memory expansion is assumed to be 0).
//
// Returns a slice of cold access counts corresponding to each structlog index.
// Returns nil if structlogs is empty.
func ClassifyColdAccess(
	structlogs []execution.StructLog,
	gasSelf []uint64,
	memExpGas []uint64,
) []uint64 {
	if len(structlogs) == 0 {
		return nil
	}

	coldCounts := make([]uint64, len(structlogs))

	for i := range structlogs {
		coldCounts[i] = classifyOpcode(&structlogs[i], gasSelf[i], getMemExp(memExpGas, i))
	}

	return coldCounts
}

// classifyOpcode determines the cold access count for a single opcode.
func classifyOpcode(sl *execution.StructLog, gasSelf, memExp uint64) uint64 {
	switch sl.Op {
	case "SLOAD":
		return classifySload(sl.GasCost)

	case "SSTORE":
		return classifySstore(sl.GasCost)

	case "BALANCE", "EXTCODESIZE", "EXTCODEHASH":
		return classifyAccountAccess(sl.GasCost)

	case OpcodeCALL, OpcodeSTATICCALL, OpcodeDELEGATECALL, OpcodeCALLCODE:
		return classifyCall(sl, gasSelf, memExp)

	case "EXTCODECOPY":
		return classifyExtCodeCopy(sl, gasSelf, memExp)

	case "SELFDESTRUCT":
		return classifySelfdestruct(sl.GasCost)

	default:
		return 0
	}
}

// classifySload: cold if GasCost >= 2100 (cold SLOAD cost).
func classifySload(gasCost uint64) uint64 {
	if gasCost >= coldSloadCost {
		return 1
	}

	return 0
}

// classifySstore: cold if GasCost is one of the cold SSTORE variants.
// Cold SSTORE costs: 22100 (SET+cold), 5000 (RESET+cold), 2200 (noop+cold).
// Warm SSTORE costs: 20000 (SET), 2900 (RESET), 100 (noop/warm).
func classifySstore(gasCost uint64) uint64 {
	switch gasCost {
	case 22100, 5000, 2200:
		return 1
	default:
		return 0
	}
}

// classifyAccountAccess: cold if GasCost >= 2600 (cold account access cost).
func classifyAccountAccess(gasCost uint64) uint64 {
	if gasCost >= coldAccountCost {
		return 1
	}

	return 0
}

// classifyCall determines cold access count for CALL-family opcodes.
// Normalizes gas by subtracting memory expansion and value transfer costs,
// then uses range-based detection:
//   - remaining <= 200: 0 cold (warm access, possibly with warm delegation)
//   - remaining 2600-2700: 1 cold (single cold account access)
//   - remaining >= 5200: 2 cold (cold account + cold EIP-7702 delegation target)
func classifyCall(sl *execution.StructLog, gasSelf, memExp uint64) uint64 {
	remaining := gasSelf

	// Subtract memory expansion cost.
	if remaining > memExp {
		remaining -= memExp
	} else {
		remaining = 0
	}

	// Subtract value transfer cost for CALL/CALLCODE with non-zero value.
	// Use tracer field if set; otherwise fall back to stack in RPC mode.
	if (sl.Op == OpcodeCALL || sl.Op == OpcodeCALLCODE) && callHasValue(sl) {
		if remaining > callValueTransferGas {
			remaining -= callValueTransferGas
		} else {
			remaining = 0
		}

		// Subtract CallNewAccountGas (25000) if remaining is too large.
		// This is charged when value > 0 AND the target account is empty.
		if remaining > 5200 {
			const callNewAccountGas = 25000
			if remaining > callNewAccountGas {
				remaining -= callNewAccountGas
			} else {
				remaining = 0
			}
		}
	}

	// Precompile targets are always warm (EIP-2929 pre-warms them).
	if sl.CallToAddress != nil && IsPrecompile(*sl.CallToAddress) {
		return 0
	}

	// Range-based classification.
	if remaining <= 200 {
		return 0
	}

	if remaining >= 5200 {
		return 2
	}

	if remaining >= 2600 {
		return 1
	}

	return 0
}

// callHasValue returns true if a CALL/CALLCODE transfers non-zero ETH value.
// In embedded mode, uses the pre-computed CallTransfersValue field.
// In RPC mode, falls back to checking stack[len-3] (the value operand).
func callHasValue(sl *execution.StructLog) bool {
	if sl.CallTransfersValue {
		return true
	}

	// RPC fallback: value is the 3rd element from the top of the stack.
	if sl.Stack != nil && len(*sl.Stack) > 2 {
		return !isHexZero((*sl.Stack)[len(*sl.Stack)-3])
	}

	return false
}

// extCodeCopySize returns the EXTCODECOPY size operand.
// In embedded mode, uses the pre-computed ExtCodeCopySize field.
// In RPC mode, falls back to parsing stack[len-4] (the size operand).
func extCodeCopySize(sl *execution.StructLog) uint32 {
	if sl.ExtCodeCopySize > 0 {
		return sl.ExtCodeCopySize
	}

	// RPC fallback: size is the 4th element from the top of the stack.
	if sl.Stack != nil && len(*sl.Stack) > 3 {
		return parseHexUint32((*sl.Stack)[len(*sl.Stack)-4])
	}

	return 0
}

// classifyExtCodeCopy determines cold access count for EXTCODECOPY.
// Normalizes gas by subtracting memory expansion and copy costs.
func classifyExtCodeCopy(sl *execution.StructLog, gasSelf, memExp uint64) uint64 {
	remaining := gasSelf

	// Subtract memory expansion cost.
	if remaining > memExp {
		remaining -= memExp
	} else {
		remaining = 0
	}

	// Subtract copy cost: 3 gas per 32-byte word (rounded up).
	size := extCodeCopySize(sl)
	copyWords := (uint64(size) + 31) / 32
	copyCost := copyWords * wordCopyCost

	if remaining > copyCost {
		remaining -= copyCost
	} else {
		remaining = 0
	}

	if remaining >= coldAccountCost {
		return 1
	}

	return 0
}

// classifySelfdestruct: cold if GasCost indicates cold access.
// Cold SELFDESTRUCT costs: 7600 (cold target), 32600 (cold + new account).
func classifySelfdestruct(gasCost uint64) uint64 {
	switch gasCost {
	case 7600, 32600:
		return 1
	default:
		return 0
	}
}

// getMemExp safely retrieves memory expansion gas, returning 0 if the slice is nil.
func getMemExp(memExpGas []uint64, i int) uint64 {
	if memExpGas == nil || i >= len(memExpGas) {
		return 0
	}

	return memExpGas[i]
}

// isHexZero returns true if the hex string represents zero.
func isHexZero(s string) bool {
	hex := strings.TrimPrefix(s, "0x")
	if hex == "" {
		return true
	}

	for _, c := range hex {
		if c != '0' {
			return false
		}
	}

	return true
}

// parseHexUint32 parses a hex string to uint32, clamping to math.MaxUint32.
func parseHexUint32(s string) uint32 {
	hex := strings.TrimPrefix(s, "0x")
	if hex == "" {
		return 0
	}

	v, err := strconv.ParseUint(hex, 16, 64)
	if err != nil || v > math.MaxUint32 {
		return math.MaxUint32
	}

	return uint32(v)
}
