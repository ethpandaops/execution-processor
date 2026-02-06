package execution

// TraceTransaction holds the result of a debug_traceTransaction call.
type TraceTransaction struct {
	// Gas is the post-refund gas used by this transaction (what the user pays).
	// Set by the data source from the execution result or receipt's GasUsed.
	//
	// EIP-7778 context: After EIP-7778, Ethereum splits gas accounting into:
	//   - ReceiptGasUsed (post-refund): what the user pays, stored in receipts
	//   - BlockGasUsed (pre-refund): used for block gas limit accounting
	// This field carries the receipt (post-refund) value. The computeIntrinsicGas()
	// formula in structlog_agg depends on this being post-refund.
	Gas         uint64  `json:"gas"`
	Failed      bool    `json:"failed"`
	ReturnValue *string `json:"returnValue"`

	Structlogs []StructLog
}

// StructLog represents a single EVM opcode execution trace entry.
//
// This struct supports two operation modes:
//   - RPC mode: Stack is populated for CALL opcodes, CallToAddress/GasUsed computed post-hoc
//   - Embedded mode: CallToAddress/GasUsed pre-computed by tracer, Stack remains nil
//
// The embedded mode optimizations eliminate ~99% of stack-related allocations
// and remove the post-processing GasUsed computation pass.
type StructLog struct {
	// PC is the program counter. Kept for RPC backward compatibility but not
	// populated in embedded mode (always 0).
	PC uint32 `json:"pc"`

	// Op is the opcode name (e.g., "PUSH1", "CALL", "SSTORE").
	Op string `json:"op"`

	// Gas is the remaining gas before this opcode executes.
	Gas uint64 `json:"gas"`

	// GasCost is the static gas cost of the opcode (may differ from actual GasUsed).
	GasCost uint64 `json:"gasCost"`

	// GasUsed is the actual gas consumed by this opcode.
	// In embedded mode: pre-computed by tracer using gas difference to next opcode.
	// In RPC mode: computed post-hoc by ComputeGasUsed(), this field will be 0.
	GasUsed uint64 `json:"gasUsed,omitempty"`

	// Depth is the call stack depth (1 = top-level, increases with CALL/CREATE).
	Depth uint64 `json:"depth"`

	// ReturnData contains the return data from the last CALL/STATICCALL/etc.
	ReturnData *string `json:"returnData"`

	// Refund is the gas refund counter value.
	Refund *uint64 `json:"refund,omitempty"`

	// Error contains any error message if the opcode failed.
	Error *string `json:"error,omitempty"`

	// Stack contains the EVM stack state (RPC mode only).
	// In embedded mode this is nil - use CallToAddress instead.
	Stack *[]string `json:"stack,omitempty"`

	// CallToAddress is the target address for CALL/STATICCALL/DELEGATECALL/CALLCODE.
	// In embedded mode: pre-extracted by tracer from stack[len-2].
	// In RPC mode: nil, extracted post-hoc from Stack by extractCallAddress().
	CallToAddress *string `json:"callToAddress,omitempty"`
}
