package structlog

// CallFrame represents a single call frame in the EVM execution.
type CallFrame struct {
	ID    uint32 // Sequential frame ID within the transaction
	Depth uint64 // EVM depth level
}

// CallTracker tracks call frames during EVM opcode traversal.
// It assigns sequential frame IDs as calls are entered and maintains
// the current path from root to the active frame.
type CallTracker struct {
	stack  []CallFrame // Stack of active call frames
	nextID uint32      // Next frame ID to assign
	path   []uint32    // Current path from root to active frame
}

// NewCallTracker creates a new CallTracker initialized with the root frame.
func NewCallTracker() *CallTracker {
	return &CallTracker{
		stack:  []CallFrame{{ID: 0, Depth: 0}},
		nextID: 1,
		path:   []uint32{0},
	}
}

// ProcessDepthChange processes a depth change and returns the current frame ID and path.
// Call this for each opcode with the opcode's depth value.
func (ct *CallTracker) ProcessDepthChange(newDepth uint64) (frameID uint32, framePath []uint32) {
	currentDepth := ct.stack[len(ct.stack)-1].Depth

	if newDepth > currentDepth {
		// Entering new call frame
		newFrame := CallFrame{ID: ct.nextID, Depth: newDepth}
		ct.stack = append(ct.stack, newFrame)
		ct.path = append(ct.path, ct.nextID)
		ct.nextID++
	} else if newDepth < currentDepth {
		// Returning from call(s) - pop frames until depth matches
		for len(ct.stack) > 1 && ct.stack[len(ct.stack)-1].Depth > newDepth {
			ct.stack = ct.stack[:len(ct.stack)-1]
			ct.path = ct.path[:len(ct.path)-1]
		}
	}

	// Return current frame info (copy path to avoid mutation issues)
	pathCopy := make([]uint32, len(ct.path))
	copy(pathCopy, ct.path)

	return ct.stack[len(ct.stack)-1].ID, pathCopy
}

// CurrentFrameID returns the current frame ID without processing a depth change.
func (ct *CallTracker) CurrentFrameID() uint32 {
	return ct.stack[len(ct.stack)-1].ID
}

// CurrentPath returns a copy of the current path.
func (ct *CallTracker) CurrentPath() []uint32 {
	pathCopy := make([]uint32, len(ct.path))
	copy(pathCopy, ct.path)

	return pathCopy
}
