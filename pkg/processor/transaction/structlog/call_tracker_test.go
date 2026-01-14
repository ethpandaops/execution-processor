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

	// All opcodes at depth 0 should stay in frame 0
	frameID, path := ct.ProcessDepthChange(0)
	assert.Equal(t, uint32(0), frameID)
	assert.Equal(t, []uint32{0}, path)

	frameID, path = ct.ProcessDepthChange(0)
	assert.Equal(t, uint32(0), frameID)
	assert.Equal(t, []uint32{0}, path)

	frameID, path = ct.ProcessDepthChange(0)
	assert.Equal(t, uint32(0), frameID)
	assert.Equal(t, []uint32{0}, path)
}

func TestCallTracker_SingleCall(t *testing.T) {
	ct := NewCallTracker()

	// depth=0: root frame
	frameID, path := ct.ProcessDepthChange(0)
	assert.Equal(t, uint32(0), frameID)
	assert.Equal(t, []uint32{0}, path)

	// depth=1: entering first call
	frameID, path = ct.ProcessDepthChange(1)
	assert.Equal(t, uint32(1), frameID)
	assert.Equal(t, []uint32{0, 1}, path)

	// depth=1: still in first call
	frameID, path = ct.ProcessDepthChange(1)
	assert.Equal(t, uint32(1), frameID)
	assert.Equal(t, []uint32{0, 1}, path)

	// depth=0: returned from call
	frameID, path = ct.ProcessDepthChange(0)
	assert.Equal(t, uint32(0), frameID)
	assert.Equal(t, []uint32{0}, path)
}

func TestCallTracker_NestedCalls(t *testing.T) {
	ct := NewCallTracker()

	// depth=0: root
	frameID, path := ct.ProcessDepthChange(0)
	assert.Equal(t, uint32(0), frameID)
	assert.Equal(t, []uint32{0}, path)

	// depth=1: first call
	frameID, path = ct.ProcessDepthChange(1)
	assert.Equal(t, uint32(1), frameID)
	assert.Equal(t, []uint32{0, 1}, path)

	// depth=2: nested call
	frameID, path = ct.ProcessDepthChange(2)
	assert.Equal(t, uint32(2), frameID)
	assert.Equal(t, []uint32{0, 1, 2}, path)

	// depth=3: deeper nested call
	frameID, path = ct.ProcessDepthChange(3)
	assert.Equal(t, uint32(3), frameID)
	assert.Equal(t, []uint32{0, 1, 2, 3}, path)

	// depth=2: return from depth 3
	frameID, path = ct.ProcessDepthChange(2)
	assert.Equal(t, uint32(2), frameID)
	assert.Equal(t, []uint32{0, 1, 2}, path)

	// depth=1: return from depth 2
	frameID, path = ct.ProcessDepthChange(1)
	assert.Equal(t, uint32(1), frameID)
	assert.Equal(t, []uint32{0, 1}, path)

	// depth=0: return to root
	frameID, path = ct.ProcessDepthChange(0)
	assert.Equal(t, uint32(0), frameID)
	assert.Equal(t, []uint32{0}, path)
}

func TestCallTracker_SiblingCalls(t *testing.T) {
	// Tests the scenario from the plan:
	// root -> CALL (0x123) -> CALL (0x456) -> CALL (0x789)
	// root -> CALL (0xabc) -> CALL (0x456) -> CALL (0x789)
	ct := NewCallTracker()

	// depth=0: root
	frameID, path := ct.ProcessDepthChange(0)
	assert.Equal(t, uint32(0), frameID)
	assert.Equal(t, []uint32{0}, path)

	// First branch: depth=1 (call to 0x123)
	frameID, path = ct.ProcessDepthChange(1)
	assert.Equal(t, uint32(1), frameID)
	assert.Equal(t, []uint32{0, 1}, path)

	// depth=2 (call to 0x456)
	frameID, path = ct.ProcessDepthChange(2)
	assert.Equal(t, uint32(2), frameID)
	assert.Equal(t, []uint32{0, 1, 2}, path)

	// depth=3 (call to 0x789)
	frameID, path = ct.ProcessDepthChange(3)
	assert.Equal(t, uint32(3), frameID)
	assert.Equal(t, []uint32{0, 1, 2, 3}, path)

	// Return all the way to root
	frameID, path = ct.ProcessDepthChange(0)
	assert.Equal(t, uint32(0), frameID)
	assert.Equal(t, []uint32{0}, path)

	// Second branch: depth=1 (call to 0xabc) - NEW frame_id!
	frameID, path = ct.ProcessDepthChange(1)
	assert.Equal(t, uint32(4), frameID, "sibling call should get new frame_id")
	assert.Equal(t, []uint32{0, 4}, path)

	// depth=2 (call to 0x456 again) - NEW frame_id!
	frameID, path = ct.ProcessDepthChange(2)
	assert.Equal(t, uint32(5), frameID, "same contract different call should get new frame_id")
	assert.Equal(t, []uint32{0, 4, 5}, path)

	// depth=3 (call to 0x789 again) - NEW frame_id!
	frameID, path = ct.ProcessDepthChange(3)
	assert.Equal(t, uint32(6), frameID, "same contract different call should get new frame_id")
	assert.Equal(t, []uint32{0, 4, 5, 6}, path)
}

func TestCallTracker_MultipleReturns(t *testing.T) {
	// Test returning multiple levels at once (e.g., REVERT that unwinds multiple frames)
	ct := NewCallTracker()

	// Build up: depth 0 -> 1 -> 2 -> 3
	ct.ProcessDepthChange(0)
	ct.ProcessDepthChange(1)
	ct.ProcessDepthChange(2)
	frameID, path := ct.ProcessDepthChange(3)
	assert.Equal(t, uint32(3), frameID)
	assert.Equal(t, []uint32{0, 1, 2, 3}, path)

	// Jump directly from depth 3 to depth 1 (skipping depth 2)
	frameID, path = ct.ProcessDepthChange(1)
	assert.Equal(t, uint32(1), frameID)
	assert.Equal(t, []uint32{0, 1}, path)
}

func TestCallTracker_PathIsCopy(t *testing.T) {
	ct := NewCallTracker()

	ct.ProcessDepthChange(0)
	_, path1 := ct.ProcessDepthChange(1)

	// Modify path1, should not affect tracker's internal state
	path1[0] = 999

	_, path2 := ct.ProcessDepthChange(1)
	require.Len(t, path2, 2)
	assert.Equal(t, uint32(0), path2[0], "modifying returned path should not affect tracker")
}

func TestCallTracker_DepthStartsAtOne(t *testing.T) {
	// Some EVM traces start at depth 1 instead of 0
	ct := NewCallTracker()

	// First opcode at depth 1 - should create frame 1
	frameID, path := ct.ProcessDepthChange(1)
	assert.Equal(t, uint32(1), frameID)
	assert.Equal(t, []uint32{0, 1}, path)

	// Stay at depth 1
	frameID, path = ct.ProcessDepthChange(1)
	assert.Equal(t, uint32(1), frameID)
	assert.Equal(t, []uint32{0, 1}, path)

	// Go deeper
	frameID, path = ct.ProcessDepthChange(2)
	assert.Equal(t, uint32(2), frameID)
	assert.Equal(t, []uint32{0, 1, 2}, path)
}

func TestCallTracker_RealWorldExample(t *testing.T) {
	// Simulate the example from the HackMD doc:
	// op=PUSH1, depth=0 → frame_id=0, path=[0]
	// op=CALL(A),depth=0 → frame_id=0, path=[0]
	//   op=ADD, depth=1 → frame_id=1, path=[0,1]
	//   op=CALL(B),d=1 → frame_id=1, path=[0,1]
	//     op=MUL, d=2 → frame_id=2, path=[0,1,2]
	//     op=CALL(C),d=2 → frame_id=2, path=[0,1,2]
	//       op=SLOAD,d=3 → frame_id=3, path=[0,1,2,3]
	//       op=RETURN,d=3 → frame_id=3, path=[0,1,2,3]
	//     op=ADD, d=2 → frame_id=2, path=[0,1,2]
	//     op=RETURN,d=2 → frame_id=2, path=[0,1,2]
	//   op=POP, depth=1 → frame_id=1, path=[0,1]
	// op=STOP, depth=0 → frame_id=0, path=[0]
	ct := NewCallTracker()

	type expected struct {
		depth   uint64
		frameID uint32
		path    []uint32
	}

	testCases := []expected{
		{0, 0, []uint32{0}},          // PUSH1
		{0, 0, []uint32{0}},          // CALL(A)
		{1, 1, []uint32{0, 1}},       // ADD (inside A)
		{1, 1, []uint32{0, 1}},       // CALL(B)
		{2, 2, []uint32{0, 1, 2}},    // MUL (inside B)
		{2, 2, []uint32{0, 1, 2}},    // CALL(C)
		{3, 3, []uint32{0, 1, 2, 3}}, // SLOAD (inside C)
		{3, 3, []uint32{0, 1, 2, 3}}, // RETURN (inside C)
		{2, 2, []uint32{0, 1, 2}},    // ADD (back in B)
		{2, 2, []uint32{0, 1, 2}},    // RETURN (inside B)
		{1, 1, []uint32{0, 1}},       // POP (back in A)
		{0, 0, []uint32{0}},          // STOP (back in root)
	}

	for i, tc := range testCases {
		frameID, path := ct.ProcessDepthChange(tc.depth)
		assert.Equal(t, tc.frameID, frameID, "case %d: frame_id mismatch", i)
		assert.Equal(t, tc.path, path, "case %d: path mismatch", i)
	}
}
