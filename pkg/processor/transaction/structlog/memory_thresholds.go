package structlog

// MemoryThresholds holds configurable memory thresholds for warnings
type MemoryThresholds struct {
	// BatchCollectorWarningMB is the threshold for batch collector memory warnings
	BatchCollectorWarningMB uint64
	// LargeTraceWarningMB is the threshold for large trace memory warnings
	LargeTraceWarningMB uint64
	// LargeTaskWarningMB is the threshold for large task processing warnings
	LargeTaskWarningMB uint64
}

// DefaultMemoryThresholds returns default memory thresholds
func DefaultMemoryThresholds() MemoryThresholds {
	return MemoryThresholds{
		BatchCollectorWarningMB: 500,
		LargeTraceWarningMB:     800,
		LargeTaskWarningMB:      1000,
	}
}
