package structlog

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

const testCreateAddress = "0x1234567890abcdef1234567890abcdef12345678"

func TestHasCreateOpcode_Empty(t *testing.T) {
	result := hasCreateOpcode([]execution.StructLog{})
	assert.False(t, result)
}

func TestHasCreateOpcode_NoCREATE(t *testing.T) {
	structlogs := []execution.StructLog{
		{Op: "PUSH1"},
		{Op: "CALL"},
		{Op: "ADD"},
		{Op: "SLOAD"},
		{Op: "RETURN"},
	}

	result := hasCreateOpcode(structlogs)
	assert.False(t, result)
}

func TestHasCreateOpcode_HasCREATE(t *testing.T) {
	structlogs := []execution.StructLog{
		{Op: "PUSH1"},
		{Op: "CREATE"},
		{Op: "POP"},
	}

	result := hasCreateOpcode(structlogs)
	assert.True(t, result)
}

func TestHasCreateOpcode_HasCREATE2(t *testing.T) {
	structlogs := []execution.StructLog{
		{Op: "PUSH1"},
		{Op: "CREATE2"},
		{Op: "POP"},
	}

	result := hasCreateOpcode(structlogs)
	assert.True(t, result)
}

func TestHasCreateOpcode_BothCREATEAndCREATE2(t *testing.T) {
	structlogs := []execution.StructLog{
		{Op: "CREATE"},
		{Op: "CREATE2"},
	}

	result := hasCreateOpcode(structlogs)
	assert.True(t, result)
}

func TestHasCreateOpcode_CREATEAtEnd(t *testing.T) {
	structlogs := []execution.StructLog{
		{Op: "PUSH1"},
		{Op: "PUSH2"},
		{Op: "ADD"},
		{Op: "CREATE"},
	}

	result := hasCreateOpcode(structlogs)
	assert.True(t, result)
}

func TestExtractCallAddressWithCreate_CREATE(t *testing.T) {
	p := &Processor{}
	createAddr := testCreateAddress

	result := p.extractCallAddressWithCreate(&execution.StructLog{
		Op: "CREATE",
	}, &createAddr)

	assert.NotNil(t, result)
	assert.Equal(t, createAddr, *result)
}

func TestExtractCallAddressWithCreate_CREATE2(t *testing.T) {
	p := &Processor{}
	createAddr := "0xabcdef1234567890abcdef1234567890abcdef12"

	result := p.extractCallAddressWithCreate(&execution.StructLog{
		Op: "CREATE2",
	}, &createAddr)

	assert.NotNil(t, result)
	assert.Equal(t, createAddr, *result)
}

func TestExtractCallAddressWithCreate_CREATEWithNilAddress(t *testing.T) {
	p := &Processor{}

	result := p.extractCallAddressWithCreate(&execution.StructLog{
		Op: "CREATE",
	}, nil)

	assert.Nil(t, result)
}

func TestExtractCallAddressWithCreate_CREATE2WithNilAddress(t *testing.T) {
	p := &Processor{}

	result := p.extractCallAddressWithCreate(&execution.StructLog{
		Op: "CREATE2",
	}, nil)

	assert.Nil(t, result)
}

func TestExtractCallAddressWithCreate_CALLDelegatesToExtractCallAddress(t *testing.T) {
	p := &Processor{}
	createAddr := testCreateAddress
	stack := []string{"0x5208", "0xdeadbeef"}

	result := p.extractCallAddressWithCreate(&execution.StructLog{
		Op:    "CALL",
		Stack: &stack,
	}, &createAddr)

	// Should use extractCallAddress, not createAddr
	assert.NotNil(t, result)
	assert.Equal(t, "0x5208", *result) // Second from top of stack
}

func TestExtractCallAddressWithCreate_DELEGATECALLDelegatesToExtractCallAddress(t *testing.T) {
	p := &Processor{}
	createAddr := testCreateAddress
	stack := []string{"0x5208", "0xdeadbeef"}

	result := p.extractCallAddressWithCreate(&execution.StructLog{
		Op:    "DELEGATECALL",
		Stack: &stack,
	}, &createAddr)

	// Should use extractCallAddress, not createAddr
	assert.NotNil(t, result)
	assert.Equal(t, "0x5208", *result)
}

func TestExtractCallAddressWithCreate_STATICCALLDelegatesToExtractCallAddress(t *testing.T) {
	p := &Processor{}
	createAddr := testCreateAddress
	stack := []string{"0x5208", "0xdeadbeef"}

	result := p.extractCallAddressWithCreate(&execution.StructLog{
		Op:    "STATICCALL",
		Stack: &stack,
	}, &createAddr)

	assert.NotNil(t, result)
	assert.Equal(t, "0x5208", *result)
}

func TestExtractCallAddressWithCreate_CALLCODEDelegatesToExtractCallAddress(t *testing.T) {
	p := &Processor{}
	createAddr := testCreateAddress
	stack := []string{"0x5208", "0xdeadbeef"}

	result := p.extractCallAddressWithCreate(&execution.StructLog{
		Op:    "CALLCODE",
		Stack: &stack,
	}, &createAddr)

	assert.NotNil(t, result)
	assert.Equal(t, "0x5208", *result)
}

func TestExtractCallAddressWithCreate_NonCallOpcodeReturnsNil(t *testing.T) {
	p := &Processor{}
	createAddr := testCreateAddress
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
			}, &createAddr)

			assert.Nil(t, result, "opcode %s should return nil", op)
		})
	}
}
