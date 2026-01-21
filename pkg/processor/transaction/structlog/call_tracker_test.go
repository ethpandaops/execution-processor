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

func TestCallTracker_IssueFrameID(t *testing.T) {
	// Tests IssueFrameID for synthetic EOA frames
	ct := NewCallTracker()

	// depth=1: root (EVM traces start at depth 1)
	frameID, path := ct.ProcessDepthChange(1)
	assert.Equal(t, uint32(0), frameID)
	assert.Equal(t, []uint32{0}, path)

	// Issue a synthetic frame for EOA call (no depth increase)
	eoaFrameID, eoaPath := ct.IssueFrameID()
	assert.Equal(t, uint32(1), eoaFrameID, "EOA frame should get next sequential ID")
	assert.Equal(t, []uint32{0, 1}, eoaPath, "EOA path should be parent path + EOA ID")

	// Regular depth increase should get the next ID after the EOA frame
	frameID, path = ct.ProcessDepthChange(2)
	assert.Equal(t, uint32(2), frameID, "next real frame should get ID after EOA frame")
	assert.Equal(t, []uint32{0, 2}, path)

	// Issue another EOA frame from depth 2
	eoaFrameID2, eoaPath2 := ct.IssueFrameID()
	assert.Equal(t, uint32(3), eoaFrameID2)
	assert.Equal(t, []uint32{0, 2, 3}, eoaPath2)

	// Return to depth 1
	frameID, path = ct.ProcessDepthChange(1)
	assert.Equal(t, uint32(0), frameID)
	assert.Equal(t, []uint32{0}, path)

	// Issue EOA from root - should continue sequential numbering
	eoaFrameID3, eoaPath3 := ct.IssueFrameID()
	assert.Equal(t, uint32(4), eoaFrameID3)
	assert.Equal(t, []uint32{0, 4}, eoaPath3)
}

func TestCallTracker_IssueFrameID_MultipleConsecutive(t *testing.T) {
	// Test multiple consecutive EOA calls (e.g., contract sends to multiple EOAs)
	ct := NewCallTracker()

	ct.ProcessDepthChange(1) // root

	// Three consecutive EOA calls
	id1, path1 := ct.IssueFrameID()
	id2, path2 := ct.IssueFrameID()
	id3, path3 := ct.IssueFrameID()

	assert.Equal(t, uint32(1), id1)
	assert.Equal(t, uint32(2), id2)
	assert.Equal(t, uint32(3), id3)

	assert.Equal(t, []uint32{0, 1}, path1)
	assert.Equal(t, []uint32{0, 2}, path2)
	assert.Equal(t, []uint32{0, 3}, path3)

	// Next real call should continue from 4
	frameID, path := ct.ProcessDepthChange(2)
	assert.Equal(t, uint32(4), frameID)
	assert.Equal(t, []uint32{0, 4}, path)
}

func TestIsPrecompile(t *testing.T) {
	tests := []struct {
		name     string
		addr     string
		expected bool
	}{
		// Known precompiles (should return true)
		{"ecrecover", "0x0000000000000000000000000000000000000001", true},
		{"sha256", "0x0000000000000000000000000000000000000002", true},
		{"ripemd160", "0x0000000000000000000000000000000000000003", true},
		{"identity", "0x0000000000000000000000000000000000000004", true},
		{"modexp", "0x0000000000000000000000000000000000000005", true},
		{"bn256Add", "0x0000000000000000000000000000000000000006", true},
		{"bn256ScalarMul", "0x0000000000000000000000000000000000000007", true},
		{"bn256Pairing", "0x0000000000000000000000000000000000000008", true},
		{"blake2f", "0x0000000000000000000000000000000000000009", true},
		{"kzgPointEvaluation", "0x000000000000000000000000000000000000000a", true},
		{"bls12381G1Add", "0x000000000000000000000000000000000000000b", true},
		{"bls12381G1Msm", "0x000000000000000000000000000000000000000c", true},
		{"bls12381G2Add", "0x000000000000000000000000000000000000000d", true},
		{"bls12381G2Msm", "0x000000000000000000000000000000000000000e", true},
		{"bls12381PairingCheck", "0x000000000000000000000000000000000000000f", true},
		{"bls12381MapFpToG1", "0x0000000000000000000000000000000000000010", true},
		{"bls12381MapFp2ToG2", "0x0000000000000000000000000000000000000011", true},
		{"p256Verify", "0x0000000000000000000000000000000000000100", true},

		// Edge cases within threshold (should return true)
		{"max precompile range", "0x000000000000000000000000000000000000ffff", true},
		{"zero address", "0x0000000000000000000000000000000000000000", true},

		// Real contract/EOA addresses (should return false)
		{"WETH", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", false},
		{"Uniswap V2 Router", "0x7a250d5630b4cf539739df2c5dacb4c659f2488d", false},
		{"vitalik.eth", "0xd8da6bf26964af9d7eed9e03e53415d37aa96045", false},
		{"random EOA", "0x1234567890abcdef1234567890abcdef12345678", false},

		// Just above threshold (should return false)
		{"just above threshold", "0x0000000000000000000000000000000000010000", false},
		{"slightly above", "0x0000000000000000000000000000000000010001", false},

		// Case insensitivity
		{"uppercase hex", "0x000000000000000000000000000000000000000A", true},
		{"mixed case", "0x000000000000000000000000000000000000000B", true},
		{"uppercase contract", "0xC02AAA39B223FE8D0A0E5C4F27EAD9083C756CC2", false},

		// Without 0x prefix (edge case)
		{"no prefix precompile", "0000000000000000000000000000000000000001", true},
		{"no prefix contract", "c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", false},

		// Short addresses (should be padded)
		{"short precompile 0x1", "0x1", true},
		{"short precompile 0x9", "0x9", true},
		{"short precompile 0x100", "0x100", true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := isPrecompile(tc.addr)
			assert.Equal(t, tc.expected, result, "isPrecompile(%q) = %v, want %v", tc.addr, result, tc.expected)
		})
	}
}

func TestIsCallOpcode(t *testing.T) {
	tests := []struct {
		opcode   string
		expected bool
	}{
		// CALL-type opcodes (should return true)
		{"CALL", true},
		{"CALLCODE", true},
		{"DELEGATECALL", true},
		{"STATICCALL", true},

		// CREATE opcodes (should return false - they always increase depth)
		{"CREATE", false},
		{"CREATE2", false},

		// Other opcodes (should return false)
		{"ADD", false},
		{"SUB", false},
		{"SLOAD", false},
		{"SSTORE", false},
		{"PUSH1", false},
		{"POP", false},
		{"JUMP", false},
		{"RETURN", false},
		{"REVERT", false},
		{"STOP", false},
		{"", false},
	}

	for _, tc := range tests {
		t.Run(tc.opcode, func(t *testing.T) {
			result := isCallOpcode(tc.opcode)
			assert.Equal(t, tc.expected, result, "isCallOpcode(%q) = %v, want %v", tc.opcode, result, tc.expected)
		})
	}
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
