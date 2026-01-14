package structlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

const testCreateAddress = "0x1234567890abcdef1234567890abcdef12345678"

func TestComputeCreateAddresses_Empty(t *testing.T) {
	result := ComputeCreateAddresses([]execution.StructLog{})
	assert.Empty(t, result)
}

func TestComputeCreateAddresses_NoCREATE(t *testing.T) {
	structlogs := []execution.StructLog{
		{Op: "PUSH1", Depth: 1},
		{Op: "CALL", Depth: 1},
		{Op: "ADD", Depth: 2},
		{Op: "RETURN", Depth: 2},
		{Op: "STOP", Depth: 1},
	}

	result := ComputeCreateAddresses(structlogs)
	assert.Empty(t, result)
}

func TestComputeCreateAddresses_SingleCREATE(t *testing.T) {
	// Simulate: CREATE at depth 2, constructor runs at depth 3, returns
	createdAddr := "0xdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
	stack := []string{createdAddr}

	structlogs := []execution.StructLog{
		{Op: "PUSH1", Depth: 2},
		{Op: "CREATE", Depth: 2},               // index 1
		{Op: "PUSH1", Depth: 3},                // constructor starts
		{Op: "RETURN", Depth: 3},               // constructor ends
		{Op: "SWAP1", Depth: 2, Stack: &stack}, // back in caller, stack has address
	}

	result := ComputeCreateAddresses(structlogs)

	require.Contains(t, result, 1)
	assert.Equal(t, createdAddr, *result[1])
}

func TestComputeCreateAddresses_CREATE2(t *testing.T) {
	createdAddr := "0xabcdefabcdefabcdefabcdefabcdefabcdefabcd"
	stack := []string{createdAddr}

	structlogs := []execution.StructLog{
		{Op: "PUSH1", Depth: 1},
		{Op: "CREATE2", Depth: 1},            // index 1
		{Op: "ADD", Depth: 2},                // constructor
		{Op: "RETURN", Depth: 2},             // constructor ends
		{Op: "POP", Depth: 1, Stack: &stack}, // back in caller
	}

	result := ComputeCreateAddresses(structlogs)

	require.Contains(t, result, 1)
	assert.Equal(t, createdAddr, *result[1])
}

func TestComputeCreateAddresses_FailedCREATE(t *testing.T) {
	// When CREATE fails immediately, next opcode is at same depth with 0 on stack
	zeroAddr := "0x0"
	stack := []string{zeroAddr}

	structlogs := []execution.StructLog{
		{Op: "PUSH1", Depth: 2},
		{Op: "CREATE", Depth: 2},                // index 1 - fails immediately
		{Op: "ISZERO", Depth: 2, Stack: &stack}, // still at depth 2, stack has 0
	}

	result := ComputeCreateAddresses(structlogs)

	require.Contains(t, result, 1)
	assert.Equal(t, zeroAddr, *result[1])
}

func TestComputeCreateAddresses_NestedCREATEs(t *testing.T) {
	// Outer CREATE at depth 1, inner CREATE at depth 2
	innerAddr := "0x1111111111111111111111111111111111111111"
	outerAddr := "0x2222222222222222222222222222222222222222"
	innerStack := []string{innerAddr}
	outerStack := []string{outerAddr}

	structlogs := []execution.StructLog{
		{Op: "PUSH1", Depth: 1},
		{Op: "CREATE", Depth: 1},                    // index 1 - outer CREATE
		{Op: "PUSH1", Depth: 2},                     // outer constructor starts
		{Op: "CREATE", Depth: 2},                    // index 3 - inner CREATE
		{Op: "ADD", Depth: 3},                       // inner constructor
		{Op: "RETURN", Depth: 3},                    // inner constructor ends
		{Op: "POP", Depth: 2, Stack: &innerStack},   // back in outer constructor
		{Op: "RETURN", Depth: 2},                    // outer constructor ends
		{Op: "SWAP1", Depth: 1, Stack: &outerStack}, // back in original caller
	}

	result := ComputeCreateAddresses(structlogs)

	require.Contains(t, result, 1)
	require.Contains(t, result, 3)
	assert.Equal(t, outerAddr, *result[1])
	assert.Equal(t, innerAddr, *result[3])
}

func TestComputeCreateAddresses_MultipleCREATEsSameDepth(t *testing.T) {
	// Two CREATEs at the same depth (sequential, not nested)
	addr1 := "0x1111111111111111111111111111111111111111"
	addr2 := "0x2222222222222222222222222222222222222222"
	stack1 := []string{addr1}
	stack2 := []string{addr2}

	structlogs := []execution.StructLog{
		{Op: "PUSH1", Depth: 1},
		{Op: "CREATE", Depth: 1},              // index 1 - first CREATE
		{Op: "ADD", Depth: 2},                 // first constructor
		{Op: "RETURN", Depth: 2},              // first constructor ends
		{Op: "POP", Depth: 1, Stack: &stack1}, // back, has first address
		{Op: "PUSH1", Depth: 1},
		{Op: "CREATE", Depth: 1},                // index 6 - second CREATE
		{Op: "MUL", Depth: 2},                   // second constructor
		{Op: "RETURN", Depth: 2},                // second constructor ends
		{Op: "SWAP1", Depth: 1, Stack: &stack2}, // back, has second address
	}

	result := ComputeCreateAddresses(structlogs)

	require.Contains(t, result, 1)
	require.Contains(t, result, 6)
	assert.Equal(t, addr1, *result[1])
	assert.Equal(t, addr2, *result[6])
}

func TestExtractCallAddressWithCreate_CREATE(t *testing.T) {
	p := &Processor{}
	createAddresses := map[int]*string{
		0: ptrString(testCreateAddress),
	}

	result := p.extractCallAddressWithCreate(&execution.StructLog{
		Op: "CREATE",
	}, 0, createAddresses)

	assert.NotNil(t, result)
	assert.Equal(t, testCreateAddress, *result)
}

func TestExtractCallAddressWithCreate_CREATE2(t *testing.T) {
	p := &Processor{}
	addr := "0xabcdef1234567890abcdef1234567890abcdef12"
	createAddresses := map[int]*string{
		5: ptrString(addr),
	}

	result := p.extractCallAddressWithCreate(&execution.StructLog{
		Op: "CREATE2",
	}, 5, createAddresses)

	assert.NotNil(t, result)
	assert.Equal(t, addr, *result)
}

func TestExtractCallAddressWithCreate_CREATEWithNilMap(t *testing.T) {
	p := &Processor{}

	result := p.extractCallAddressWithCreate(&execution.StructLog{
		Op: "CREATE",
	}, 0, nil)

	assert.Nil(t, result)
}

func TestExtractCallAddressWithCreate_CREATENotInMap(t *testing.T) {
	p := &Processor{}
	createAddresses := map[int]*string{
		10: ptrString(testCreateAddress),
	}

	result := p.extractCallAddressWithCreate(&execution.StructLog{
		Op: "CREATE",
	}, 5, createAddresses) // index 5 not in map

	assert.Nil(t, result)
}

func TestExtractCallAddressWithCreate_CALLDelegatesToExtractCallAddress(t *testing.T) {
	p := &Processor{}
	createAddresses := map[int]*string{
		0: ptrString(testCreateAddress),
	}
	stack := []string{"0x5208", "0xdeadbeef"}

	result := p.extractCallAddressWithCreate(&execution.StructLog{
		Op:    "CALL",
		Stack: &stack,
	}, 0, createAddresses)

	// Should use extractCallAddress, not createAddresses
	assert.NotNil(t, result)
	assert.Equal(t, "0x5208", *result) // Second from top of stack
}

func TestExtractCallAddressWithCreate_DELEGATECALLDelegatesToExtractCallAddress(t *testing.T) {
	p := &Processor{}
	createAddresses := map[int]*string{
		0: ptrString(testCreateAddress),
	}
	stack := []string{"0x5208", "0xdeadbeef"}

	result := p.extractCallAddressWithCreate(&execution.StructLog{
		Op:    "DELEGATECALL",
		Stack: &stack,
	}, 0, createAddresses)

	assert.NotNil(t, result)
	assert.Equal(t, "0x5208", *result)
}

func TestExtractCallAddressWithCreate_NonCallOpcodeReturnsNil(t *testing.T) {
	p := &Processor{}
	createAddresses := map[int]*string{
		0: ptrString(testCreateAddress),
	}
	stack := []string{"0x5208", "0xdeadbeef"}

	testCases := []string{
		"PUSH1",
		"ADD",
		"SLOAD",
		"SSTORE",
		"RETURN",
		"REVERT",
		"STOP",
	}

	for _, op := range testCases {
		t.Run(op, func(t *testing.T) {
			result := p.extractCallAddressWithCreate(&execution.StructLog{
				Op:    op,
				Stack: &stack,
			}, 0, createAddresses)

			assert.Nil(t, result, "opcode %s should return nil", op)
		})
	}
}

// ptrString returns a pointer to the given string.
func ptrString(s string) *string {
	return &s
}
