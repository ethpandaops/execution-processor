package execution

// ParityTrace represents a single trace entry from trace_transaction RPC.
// This is the parity/OpenEthereum trace format used by most execution clients.
type ParityTrace struct {
	Action              ParityTraceAction  `json:"action"`
	BlockHash           string             `json:"blockHash"`
	BlockNumber         uint64             `json:"blockNumber"`
	Result              *ParityTraceResult `json:"result"`
	Subtraces           uint32             `json:"subtraces"`
	TraceAddress        []uint32           `json:"traceAddress"`
	TransactionHash     string             `json:"transactionHash"`
	TransactionPosition uint32             `json:"transactionPosition"`
	Type                string             `json:"type"` // "call", "create", "suicide", "reward"
	Error               *string            `json:"error"`
}

// ParityTraceAction contains the action details of a trace.
type ParityTraceAction struct {
	From         string  `json:"from"`
	To           *string `json:"to"`           // nil for CREATE
	CallType     *string `json:"callType"`     // "call", "delegatecall", "staticcall", etc. (nil for CREATE)
	Gas          string  `json:"gas"`          // Hex-encoded gas
	Input        string  `json:"input"`        // Hex-encoded input data
	Value        string  `json:"value"`        // Hex-encoded value
	Init         *string `json:"init"`         // CREATE init code (nil for CALL)
	CreationType *string `json:"creationType"` // "create" or "create2" (nil for CALL)
}

// ParityTraceResult contains the result of a trace execution.
type ParityTraceResult struct {
	GasUsed string  `json:"gasUsed"` // Hex-encoded gas used
	Output  string  `json:"output"`  // Hex-encoded output data
	Code    *string `json:"code"`    // For CREATE: deployed code
	Address *string `json:"address"` // For CREATE: created address
}

// TraceAddressDepth returns the frame depth from a trace address.
// An empty trace address (root call) has depth 1.
// Each level of nesting adds 1 to the depth.
func TraceAddressDepth(traceAddress []uint32) uint64 {
	return uint64(len(traceAddress)) + 1
}
