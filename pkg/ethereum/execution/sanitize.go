package execution

// SanitizeGasCost detects and corrects corrupted gasCost values from Erigon's
// debug_traceTransaction RPC.
//
// Bug: Erigon has an unsigned integer underflow bug in gas.go:callGas() where
// `availableGas - base` underflows when availableGas < base, producing huge
// corrupted values (e.g., 18158513697557845033).
//
// Detection: gasCost can never legitimately exceed the available gas at that
// opcode. If gasCost > Gas, the value is corrupted.
//
// Correction: Set gasCost = Gas (all available gas consumed), matching Reth's
// behavior for failed CALL opcodes.
func SanitizeGasCost(log *StructLog) {
	if log.GasCost > log.Gas {
		log.GasCost = log.Gas
	}
}

// SanitizeStructLogs applies gas cost sanitization to all structlogs.
// This corrects corrupted values from Erigon's unsigned integer underflow bug.
func SanitizeStructLogs(logs []StructLog) {
	for i := range logs {
		SanitizeGasCost(&logs[i])
	}
}
