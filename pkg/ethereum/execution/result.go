package execution

type ErigonResult struct {
	Gas         uint64  `json:"gas"`
	Failed      bool    `json:"failed"`
	ReturnValue *string `json:"returnValue"`
	// empty array on transfer
	StructLogs []ErigonStructLog `json:"structLogs"`
}

type ErigonStructLog struct {
	PC         uint32  `json:"pc"`
	Op         string  `json:"op"`
	Gas        uint64  `json:"gas"`
	GasCost    uint64  `json:"gasCost"`
	Depth      uint64  `json:"depth"`
	ReturnData []byte  `json:"returnData"`
	Refund     *uint64 `json:"refund,omitempty"`
	Error      *string `json:"error,omitempty"`
}
