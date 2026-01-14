package structlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewCallTracker(t *testing.T) {
	ct := NewCallTracker()

	assert.Equal(t, uint32(0), ct.CurrentFrameID())
	assert.Equal(t, []uint32{0}, ct.CurrentPath())
}

func TestCallTracker_SameDepth(t *testing.T) {
	ct := NewCallTracker()

	// All opcodes at depth 1 should stay in frame 0 (root)
	frameID, path := ct.ProcessDepthChange(1)
	assert.Equal(t, uint32(0), frameID)
	assert.Equal(t, []uint32{0}, path)

	frameID, path = ct.ProcessDepthChange(1)
	assert.Equal(t, uint32(0), frameID)
	assert.Equal(t, []uint32{0}, path)

	frameID, path = ct.ProcessDepthChange(1)
	assert.Equal(t, uint32(0), frameID)
	assert.Equal(t, []uint32{0}, path)
}

func TestCallTracker_SingleCall(t *testing.T) {
	ct := NewCallTracker()

	// depth=1: root frame (EVM traces start at depth 1)
	frameID, path := ct.ProcessDepthChange(1)
	assert.Equal(t, uint32(0), frameID)
	assert.Equal(t, []uint32{0}, path)

	// depth=2: entering first call
	frameID, path = ct.ProcessDepthChange(2)
	assert.Equal(t, uint32(1), frameID)
	assert.Equal(t, []uint32{0, 1}, path)

	// depth=2: still in first call
	frameID, path = ct.ProcessDepthChange(2)
	assert.Equal(t, uint32(1), frameID)
	assert.Equal(t, []uint32{0, 1}, path)

	// depth=1: returned from call
	frameID, path = ct.ProcessDepthChange(1)
	assert.Equal(t, uint32(0), frameID)
	assert.Equal(t, []uint32{0}, path)
}

func TestCallTracker_NestedCalls(t *testing.T) {
	ct := NewCallTracker()

	// depth=1: root (EVM traces start at depth 1)
	frameID, path := ct.ProcessDepthChange(1)
	assert.Equal(t, uint32(0), frameID)
	assert.Equal(t, []uint32{0}, path)

	// depth=2: first call
	frameID, path = ct.ProcessDepthChange(2)
	assert.Equal(t, uint32(1), frameID)
	assert.Equal(t, []uint32{0, 1}, path)

	// depth=3: nested call
	frameID, path = ct.ProcessDepthChange(3)
	assert.Equal(t, uint32(2), frameID)
	assert.Equal(t, []uint32{0, 1, 2}, path)

	// depth=4: deeper nested call
	frameID, path = ct.ProcessDepthChange(4)
	assert.Equal(t, uint32(3), frameID)
	assert.Equal(t, []uint32{0, 1, 2, 3}, path)

	// depth=3: return from depth 4
	frameID, path = ct.ProcessDepthChange(3)
	assert.Equal(t, uint32(2), frameID)
	assert.Equal(t, []uint32{0, 1, 2}, path)

	// depth=2: return from depth 3
	frameID, path = ct.ProcessDepthChange(2)
	assert.Equal(t, uint32(1), frameID)
	assert.Equal(t, []uint32{0, 1}, path)

	// depth=1: return to root
	frameID, path = ct.ProcessDepthChange(1)
	assert.Equal(t, uint32(0), frameID)
	assert.Equal(t, []uint32{0}, path)
}

func TestCallTracker_SiblingCalls(t *testing.T) {
	// Tests the scenario from the plan:
	// root -> CALL (0x123) -> CALL (0x456) -> CALL (0x789)
	// root -> CALL (0xabc) -> CALL (0x456) -> CALL (0x789)
	ct := NewCallTracker()

	// depth=1: root (EVM traces start at depth 1)
	frameID, path := ct.ProcessDepthChange(1)
	assert.Equal(t, uint32(0), frameID)
	assert.Equal(t, []uint32{0}, path)

	// First branch: depth=2 (call to 0x123)
	frameID, path = ct.ProcessDepthChange(2)
	assert.Equal(t, uint32(1), frameID)
	assert.Equal(t, []uint32{0, 1}, path)

	// depth=3 (call to 0x456)
	frameID, path = ct.ProcessDepthChange(3)
	assert.Equal(t, uint32(2), frameID)
	assert.Equal(t, []uint32{0, 1, 2}, path)

	// depth=4 (call to 0x789)
	frameID, path = ct.ProcessDepthChange(4)
	assert.Equal(t, uint32(3), frameID)
	assert.Equal(t, []uint32{0, 1, 2, 3}, path)

	// Return all the way to root
	frameID, path = ct.ProcessDepthChange(1)
	assert.Equal(t, uint32(0), frameID)
	assert.Equal(t, []uint32{0}, path)

	// Second branch: depth=2 (call to 0xabc) - NEW frame_id!
	frameID, path = ct.ProcessDepthChange(2)
	assert.Equal(t, uint32(4), frameID, "sibling call should get new frame_id")
	assert.Equal(t, []uint32{0, 4}, path)

	// depth=3 (call to 0x456 again) - NEW frame_id!
	frameID, path = ct.ProcessDepthChange(3)
	assert.Equal(t, uint32(5), frameID, "same contract different call should get new frame_id")
	assert.Equal(t, []uint32{0, 4, 5}, path)

	// depth=4 (call to 0x789 again) - NEW frame_id!
	frameID, path = ct.ProcessDepthChange(4)
	assert.Equal(t, uint32(6), frameID, "same contract different call should get new frame_id")
	assert.Equal(t, []uint32{0, 4, 5, 6}, path)
}

func TestCallTracker_MultipleReturns(t *testing.T) {
	// Test returning multiple levels at once (e.g., REVERT that unwinds multiple frames)
	ct := NewCallTracker()

	// Build up: depth 1 -> 2 -> 3 -> 4 (EVM traces start at depth 1)
	ct.ProcessDepthChange(1)
	ct.ProcessDepthChange(2)
	ct.ProcessDepthChange(3)
	frameID, path := ct.ProcessDepthChange(4)
	assert.Equal(t, uint32(3), frameID)
	assert.Equal(t, []uint32{0, 1, 2, 3}, path)

	// Jump directly from depth 4 to depth 2 (skipping depth 3)
	frameID, path = ct.ProcessDepthChange(2)
	assert.Equal(t, uint32(1), frameID)
	assert.Equal(t, []uint32{0, 1}, path)
}

func TestCallTracker_PathIsCopy(t *testing.T) {
	ct := NewCallTracker()

	ct.ProcessDepthChange(1)
	_, path1 := ct.ProcessDepthChange(2)

	// Modify path1, should not affect tracker's internal state
	path1[0] = 999

	_, path2 := ct.ProcessDepthChange(2)
	require.Len(t, path2, 2)
	assert.Equal(t, uint32(0), path2[0], "modifying returned path should not affect tracker")
}

func TestCallTracker_DepthStartsAtOne(t *testing.T) {
	// EVM traces always start at depth 1, which is the root frame (ID 0)
	ct := NewCallTracker()

	// First opcode at depth 1 - should be frame 0 (root)
	frameID, path := ct.ProcessDepthChange(1)
	assert.Equal(t, uint32(0), frameID)
	assert.Equal(t, []uint32{0}, path)

	// Stay at depth 1
	frameID, path = ct.ProcessDepthChange(1)
	assert.Equal(t, uint32(0), frameID)
	assert.Equal(t, []uint32{0}, path)

	// Go deeper - creates frame 1
	frameID, path = ct.ProcessDepthChange(2)
	assert.Equal(t, uint32(1), frameID)
	assert.Equal(t, []uint32{0, 1}, path)
}

func TestCallTracker_RealWorldExample(t *testing.T) {
	// Simulate a real EVM trace where depth starts at 1:
	// op=PUSH1, depth=1 → frame_id=0, path=[0]        (root execution)
	// op=CALL(A),depth=1 → frame_id=0, path=[0]
	//   op=ADD, depth=2 → frame_id=1, path=[0,1]      (inside A)
	//   op=CALL(B),d=2 → frame_id=1, path=[0,1]
	//     op=MUL, d=3 → frame_id=2, path=[0,1,2]      (inside B)
	//     op=CALL(C),d=3 → frame_id=2, path=[0,1,2]
	//       op=SLOAD,d=4 → frame_id=3, path=[0,1,2,3] (inside C)
	//       op=RETURN,d=4 → frame_id=3, path=[0,1,2,3]
	//     op=ADD, d=3 → frame_id=2, path=[0,1,2]      (back in B)
	//     op=RETURN,d=3 → frame_id=2, path=[0,1,2]
	//   op=POP, depth=2 → frame_id=1, path=[0,1]      (back in A)
	// op=STOP, depth=1 → frame_id=0, path=[0]         (back in root)
	ct := NewCallTracker()

	type expected struct {
		depth   uint64
		frameID uint32
		path    []uint32
	}

	testCases := []expected{
		{1, 0, []uint32{0}},          // PUSH1 (root)
		{1, 0, []uint32{0}},          // CALL(A)
		{2, 1, []uint32{0, 1}},       // ADD (inside A)
		{2, 1, []uint32{0, 1}},       // CALL(B)
		{3, 2, []uint32{0, 1, 2}},    // MUL (inside B)
		{3, 2, []uint32{0, 1, 2}},    // CALL(C)
		{4, 3, []uint32{0, 1, 2, 3}}, // SLOAD (inside C)
		{4, 3, []uint32{0, 1, 2, 3}}, // RETURN (inside C)
		{3, 2, []uint32{0, 1, 2}},    // ADD (back in B)
		{3, 2, []uint32{0, 1, 2}},    // RETURN (inside B)
		{2, 1, []uint32{0, 1}},       // POP (back in A)
		{1, 0, []uint32{0}},          // STOP (back in root)
	}

	for i, tc := range testCases {
		frameID, path := ct.ProcessDepthChange(tc.depth)
		assert.Equal(t, tc.frameID, frameID, "case %d: frame_id mismatch", i)
		assert.Equal(t, tc.path, path, "case %d: path mismatch", i)
	}
}
