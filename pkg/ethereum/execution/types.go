package execution

// TraceOptions configures debug_traceTransaction parameters.
type TraceOptions struct {
	DisableStorage   bool
	DisableStack     bool
	DisableMemory    bool
	EnableReturnData bool
}

// DefaultTraceOptions returns standard options.
func DefaultTraceOptions() TraceOptions {
	return TraceOptions{
		DisableStorage:   true,
		DisableStack:     true,
		DisableMemory:    true,
		EnableReturnData: true,
	}
}

// StackTraceOptions returns options with stack enabled.
func StackTraceOptions() TraceOptions {
	return TraceOptions{
		DisableStorage:   true,
		DisableStack:     false,
		DisableMemory:    true,
		EnableReturnData: true,
	}
}
