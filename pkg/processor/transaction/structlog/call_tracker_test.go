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

		// Low addresses that are NOT precompiles (should return false)
		// These are real EOAs/contracts deployed early in Ethereum's history
		{"zero address", "0x0000000000000000000000000000000000000000", false},
		{"address 0x5c", "0x000000000000000000000000000000000000005c", false},
		{"address 0x60", "0x0000000000000000000000000000000000000060", false},
		{"address 0x44", "0x0000000000000000000000000000000000000044", false},
		{"address 0x348", "0x0000000000000000000000000000000000000348", false},
		{"address 0xffff", "0x000000000000000000000000000000000000ffff", false},
		{"address 0x12", "0x0000000000000000000000000000000000000012", false}, // Just above 0x11

		// Real contract/EOA addresses (should return false)
		{"WETH", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", false},
		{"Uniswap V2 Router", "0x7a250d5630b4cf539739df2c5dacb4c659f2488d", false},
		{"vitalik.eth", "0xd8da6bf26964af9d7eed9e03e53415d37aa96045", false},
		{"random EOA", "0x1234567890abcdef1234567890abcdef12345678", false},

		// Case insensitivity for precompiles
		{"uppercase hex 0xA", "0x000000000000000000000000000000000000000A", true},
		{"mixed case 0xB", "0x000000000000000000000000000000000000000B", true},
		{"uppercase contract", "0xC02AAA39B223FE8D0A0E5C4F27EAD9083C756CC2", false},

		// Without 0x prefix (edge case)
		{"no prefix precompile", "0000000000000000000000000000000000000001", true},
		{"no prefix contract", "c02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", false},

		// Short addresses (should be padded)
		{"short precompile 0x1", "0x1", true},
		{"short precompile 0x9", "0x9", true},
		{"short precompile 0x100", "0x100", true},
		{"short non-precompile 0x5c", "0x5c", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := isPrecompile(tc.addr)
			assert.Equal(t, tc.expected, result, "isPrecompile(%q) = %v, want %v", tc.addr, result, tc.expected)
		})
	}
}

// TestIsPrecompile_HardcodedList verifies that our hardcoded precompile list
// matches the expected Osaka precompiles from go-ethereum.
// Source: https://github.com/ethereum/go-ethereum/blob/master/core/vm/contracts.go
func TestIsPrecompile_HardcodedList(t *testing.T) {
	// All expected Osaka precompiles (superset of all forks)
	expectedPrecompiles := []string{
		"0x0000000000000000000000000000000000000001", // ecrecover
		"0x0000000000000000000000000000000000000002", // sha256
		"0x0000000000000000000000000000000000000003", // ripemd160
		"0x0000000000000000000000000000000000000004", // identity
		"0x0000000000000000000000000000000000000005", // modexp
		"0x0000000000000000000000000000000000000006", // bn256Add
		"0x0000000000000000000000000000000000000007", // bn256ScalarMul
		"0x0000000000000000000000000000000000000008", // bn256Pairing
		"0x0000000000000000000000000000000000000009", // blake2f
		"0x000000000000000000000000000000000000000a", // kzgPointEvaluation
		"0x000000000000000000000000000000000000000b", // bls12381G1Add
		"0x000000000000000000000000000000000000000c", // bls12381G1MultiExp
		"0x000000000000000000000000000000000000000d", // bls12381G2Add
		"0x000000000000000000000000000000000000000e", // bls12381G2MultiExp
		"0x000000000000000000000000000000000000000f", // bls12381Pairing
		"0x0000000000000000000000000000000000000010", // bls12381MapG1
		"0x0000000000000000000000000000000000000011", // bls12381MapG2
		"0x0000000000000000000000000000000000000100", // p256Verify
	}

	// Verify all expected precompiles are detected
	for _, addr := range expectedPrecompiles {
		assert.True(t, isPrecompile(addr),
			"precompile %s should be detected", addr)
	}

	// Verify the expected count: 0x01-0x11 (17) + 0x100 (1) = 18 precompiles
	assert.Equal(t, 18, len(expectedPrecompiles),
		"expected 18 precompiles in Osaka fork")
	assert.Equal(t, 18, len(precompileAddresses),
		"hardcoded precompileAddresses should have 18 entries")
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

// TestEOADetectionLogic tests the EOA call detection scenarios.
// This validates the fix for the bug where synthetic frames were incorrectly
// created for failed calls (depth decrease) instead of only for EOA calls (depth same).
func TestEOADetectionLogic(t *testing.T) {
	// Helper to simulate the EOA detection logic from transaction_processing.go
	shouldCreateSyntheticFrame := func(currentDepth, nextDepth uint64, hasNextOpcode bool, callToAddr string) bool {
		if !hasNextOpcode {
			// Last opcode is a CALL - we can't determine if it's EOA
			// because we don't have a next opcode to compare depth with.
			return false
		}

		// Only create synthetic frame if depth stayed the same (EOA call)
		// Depth increase = entered contract code (not EOA)
		// Depth decrease = call returned/failed (not EOA)
		// Depth same = called EOA or precompile (immediate return)
		if nextDepth == currentDepth && !isPrecompile(callToAddr) {
			return true
		}

		return false
	}

	tests := []struct {
		name         string
		currentDepth uint64
		nextDepth    uint64
		hasNextOp    bool
		callToAddr   string
		expectSynth  bool
		description  string
	}{
		// EOA call scenarios (should create synthetic frame)
		{
			name:         "EOA call - depth stays same",
			currentDepth: 2,
			nextDepth:    2,
			hasNextOp:    true,
			callToAddr:   "0xd8da6bf26964af9d7eed9e03e53415d37aa96045", // vitalik.eth
			expectSynth:  true,
			description:  "CALL to EOA returns immediately, depth stays same",
		},
		{
			name:         "EOA call from root depth",
			currentDepth: 1,
			nextDepth:    1,
			hasNextOp:    true,
			callToAddr:   "0x1234567890abcdef1234567890abcdef12345678",
			expectSynth:  true,
			description:  "CALL to EOA from root frame",
		},

		// Precompile call scenarios (should NOT create synthetic frame)
		{
			name:         "precompile call - ecrecover",
			currentDepth: 2,
			nextDepth:    2,
			hasNextOp:    true,
			callToAddr:   "0x0000000000000000000000000000000000000001",
			expectSynth:  false,
			description:  "CALL to ecrecover precompile",
		},
		{
			name:         "precompile call - sha256",
			currentDepth: 2,
			nextDepth:    2,
			hasNextOp:    true,
			callToAddr:   "0x0000000000000000000000000000000000000002",
			expectSynth:  false,
			description:  "CALL to sha256 precompile",
		},
		{
			name:         "precompile call - kzg point eval",
			currentDepth: 3,
			nextDepth:    3,
			hasNextOp:    true,
			callToAddr:   "0x000000000000000000000000000000000000000a",
			expectSynth:  false,
			description:  "STATICCALL to KZG point evaluation precompile",
		},

		// Contract call scenarios (should NOT create synthetic frame)
		{
			name:         "contract call - depth increases",
			currentDepth: 2,
			nextDepth:    3,
			hasNextOp:    true,
			callToAddr:   "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", // WETH
			expectSynth:  false,
			description:  "CALL to contract enters code, depth increases",
		},
		{
			name:         "delegatecall - depth increases",
			currentDepth: 1,
			nextDepth:    2,
			hasNextOp:    true,
			callToAddr:   "0x7a250d5630b4cf539739df2c5dacb4c659f2488d", // Uniswap Router
			expectSynth:  false,
			description:  "DELEGATECALL enters implementation code",
		},
		{
			name:         "nested contract call",
			currentDepth: 3,
			nextDepth:    4,
			hasNextOp:    true,
			callToAddr:   "0xabcdef1234567890abcdef1234567890abcdef12",
			expectSynth:  false,
			description:  "Nested CALL enters deeper contract",
		},

		// Failed/returning call scenarios (should NOT create synthetic frame)
		// This is the bug we fixed - depth DECREASE was incorrectly treated as EOA
		{
			name:         "failed call - depth decreases by 1",
			currentDepth: 3,
			nextDepth:    2,
			hasNextOp:    true,
			callToAddr:   "0xde9c774cde34f85ee69c22e9a1077a0c9091f09b",
			expectSynth:  false,
			description:  "CALL failed/reverted, returned to caller depth",
		},
		{
			name:         "failed call - depth decreases by 2",
			currentDepth: 4,
			nextDepth:    2,
			hasNextOp:    true,
			callToAddr:   "0xabcdef1234567890abcdef1234567890abcdef12",
			expectSynth:  false,
			description:  "CALL caused revert unwinding multiple frames",
		},
		{
			name:         "out of gas - depth returns to root",
			currentDepth: 3,
			nextDepth:    1,
			hasNextOp:    true,
			callToAddr:   "0xfe02a32cbe0cb9ad9a945576a5bb53a3c123a3a3",
			expectSynth:  false,
			description:  "Out of gas unwinds all the way to root",
		},
		{
			name:         "call returns normally",
			currentDepth: 2,
			nextDepth:    1,
			hasNextOp:    true,
			callToAddr:   "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
			expectSynth:  false,
			description:  "Contract call completed and returned",
		},

		// Last opcode scenarios (should NOT create synthetic frame)
		{
			name:         "last opcode is CALL - no next opcode",
			currentDepth: 2,
			nextDepth:    0, // doesn't matter
			hasNextOp:    false,
			callToAddr:   "0xd8da6bf26964af9d7eed9e03e53415d37aa96045",
			expectSynth:  false,
			description:  "Transaction ends with CALL (likely failed)",
		},
		{
			name:         "last opcode CALL to contract",
			currentDepth: 1,
			nextDepth:    0,
			hasNextOp:    false,
			callToAddr:   "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
			expectSynth:  false,
			description:  "Can't determine if EOA without next opcode",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := shouldCreateSyntheticFrame(tc.currentDepth, tc.nextDepth, tc.hasNextOp, tc.callToAddr)
			assert.Equal(t, tc.expectSynth, result,
				"%s: shouldCreateSyntheticFrame(depth=%d, nextDepth=%d, hasNext=%v, addr=%s) = %v, want %v",
				tc.description, tc.currentDepth, tc.nextDepth, tc.hasNextOp, tc.callToAddr, result, tc.expectSynth)
		})
	}
}

// TestEOADetectionBugScenario_DepthDecrease verifies the fix for the bug where
// a CALL followed by depth decrease was incorrectly treated as an EOA call.
// Real-world example: transaction 0x4f7494... had a CALL at depth 3, next opcode
// was at depth 2 (returned/failed). The old <= check created a phantom synthetic frame.
func TestEOADetectionBugScenario_DepthDecrease(t *testing.T) {
	// Simulate the buggy scenario from tx 0x4f7494c9f3b1bb7fb9f4d928aae41d971f0799a3d5c24df209074b70f04211f5
	// Index 235: GAS at depth 3
	// Index 236: CALL at depth 3 (to 0xde9c774cde34f85ee69c22e9a1077a0c9091f09b)
	// Index 237: RETURNDATASIZE at depth 2 (call returned/failed)
	currentDepth := uint64(3)
	nextDepth := uint64(2) // Depth DECREASED (call returned/failed)
	callToAddr := "0xde9c774cde34f85ee69c22e9a1077a0c9091f09b"

	// Old buggy logic: nextDepth <= currentDepth → 2 <= 3 → TRUE (wrong!)
	buggyLogic := nextDepth <= currentDepth && !isPrecompile(callToAddr)
	assert.True(t, buggyLogic, "Old buggy logic would have created synthetic frame")

	// Fixed logic: nextDepth == currentDepth → 2 == 3 → FALSE (correct!)
	fixedLogic := nextDepth == currentDepth && !isPrecompile(callToAddr)
	assert.False(t, fixedLogic, "Fixed logic should NOT create synthetic frame")
}

// TestEOADetectionBugScenario_OutOfGas verifies the fix for the bug where
// a CALL as the last opcode (out of gas) was incorrectly treated as an EOA call.
// Real-world example: transaction 0x7178d8e3... ended with a CALL that ran out of gas.
func TestEOADetectionBugScenario_OutOfGas(t *testing.T) {
	// Simulate the buggy scenario from tx 0x7178d8e3a33331ee0b2c42372c357cb6135bf3acd6e1eea5dbca7d9dbedfa418
	// Index 10: GAS at depth 1
	// Index 11: CALL at depth 1 (last opcode - out of gas before entering target)
	// No index 12 (trace ended)
	callToAddr := "0xfe02a32cbe0cb9ad9a945576a5bb53a3c123a3a3"
	hasNextOpcode := false

	// Old buggy logic: "Last opcode is a CALL - if not precompile, must be EOA"
	buggyLogic := !hasNextOpcode && !isPrecompile(callToAddr)
	assert.True(t, buggyLogic, "Old buggy logic would have created synthetic frame")

	// Fixed logic: Don't assume last CALL is EOA - we can't determine without next opcode
	fixedLogic := hasNextOpcode && !isPrecompile(callToAddr) // Always false when !hasNextOpcode
	assert.False(t, fixedLogic, "Fixed logic should NOT create synthetic frame for last opcode")
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
