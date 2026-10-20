package execution

import "context"

// CallFrame is one CALL/CALLCODE/DELEGATECALL/STATICCALL/CREATE/CREATE2 frame, in the order frames were entered.
type CallFrame struct {
	// Index is the structlog index of the opcode that opened the frame.
	Index uint32
	// Type is the opcode that opened the frame.
	Type string
	// ReturnData is the hex return-data buffer the frame left in its caller, or nil when that buffer is empty.
	ReturnData *string
}

// CallFrameTrace holds a transaction's call frames and outcome without per-opcode data.
type CallFrameTrace struct {
	Frames      []CallFrame
	Gas         uint64
	Failed      bool
	ReturnValue *string
}

// TraceSummary is the transaction outcome reported at the end of a streamed structlog trace.
type TraceSummary struct {
	Gas         uint64
	Failed      bool
	ReturnValue *string
}

// StructLogStreamer is implemented by nodes that deliver a structlog trace incrementally instead of as one value.
type StructLogStreamer interface {
	// TraceCallFrames returns the transaction's call frames and outcome.
	TraceCallFrames(ctx context.Context, hash string) (*CallFrameTrace, error)

	// StreamStructLogs traces hash with opts and calls visit for each structlog in execution order.
	// The *StructLog passed to visit is reused between calls.
	StreamStructLogs(ctx context.Context, hash string, opts TraceOptions, visit func(*StructLog) error) (*TraceSummary, error)
}

// ReturnBufferAfter returns the return-data buffer a frame of frameType leaves in its caller, given its output and error.
// A create only leaves its output on revert; a call leaves it on success or revert; other errors leave the buffer empty.
func ReturnBufferAfter(frameType, output, errMsg string) *string {
	const reverted = "execution reverted"

	isCreate := frameType == "CREATE" || frameType == "CREATE2"

	switch {
	case isCreate && errMsg != reverted:
		return nil
	case !isCreate && errMsg != "" && errMsg != reverted:
		return nil
	}

	output = trimHexPrefix(output)
	if output == "" {
		return nil
	}

	return &output
}

func trimHexPrefix(s string) string {
	if len(s) >= 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X') {
		return s[2:]
	}

	return s
}
