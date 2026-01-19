package structlog

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

func TestExtractCallAddress_NilStack(t *testing.T) {
	p := &Processor{}

	result := p.extractCallAddress(&execution.StructLog{
		Op:    "CALL",
		Stack: nil,
	})

	assert.Nil(t, result)
}

func TestExtractCallAddress_EmptyStack(t *testing.T) {
	p := &Processor{}
	emptyStack := []string{}

	result := p.extractCallAddress(&execution.StructLog{
		Op:    "CALL",
		Stack: &emptyStack,
	})

	assert.Nil(t, result)
}

func TestExtractCallAddress_InsufficientStack(t *testing.T) {
	p := &Processor{}
	stack := []string{"0x1234"} // Only 1 element, need at least 2

	result := p.extractCallAddress(&execution.StructLog{
		Op:    "CALL",
		Stack: &stack,
	})

	assert.Nil(t, result)
}

func TestExtractCallAddress_CALL(t *testing.T) {
	p := &Processor{}
	// CALL stack (index 0 = bottom, len-1 = top):
	// [retSize, retOffset, argsSize, argsOffset, value, addr, gas]
	// Address is at index len-2 (second from top)
	stack := []string{
		"0x0", // retSize (bottom, index 0)
		"0x0", // retOffset
		"0x0", // argsSize
		"0x0", // argsOffset
		"0x0", // value
		"0x7a250d5630b4cf539739df2c5dacb4c659f2488d", // addr (index len-2)
		"0x5208", // gas (top, index len-1)
	}

	result := p.extractCallAddress(&execution.StructLog{
		Op:    "CALL",
		Stack: &stack,
	})

	assert.NotNil(t, result)
	assert.Equal(t, "0x7a250d5630b4cf539739df2c5dacb4c659f2488d", *result)
}

func TestExtractCallAddress_CALL_MinimalStack(t *testing.T) {
	p := &Processor{}
	// Minimal stack with just 2 elements (addr at index 0, gas at index 1)
	stack := []string{
		"0x7a250d5630b4cf539739df2c5dacb4c659f2488d", // addr (index 0 = len-2)
		"0x5208", // gas (index 1 = len-1)
	}

	result := p.extractCallAddress(&execution.StructLog{
		Op:    "CALL",
		Stack: &stack,
	})

	assert.NotNil(t, result)
	assert.Equal(t, "0x7a250d5630b4cf539739df2c5dacb4c659f2488d", *result)
}

func TestExtractCallAddress_CALL_WithExtraStackItemsBelow(t *testing.T) {
	p := &Processor{}
	// Stack with extra items BELOW CALL args (at the bottom)
	// The CALL args are still at the top, so len-2 still gives addr
	stack := []string{
		"0xdeadbeef", // extra item (bottom)
		"0xcafebabe", // another extra item
		"0x0",        // retSize (start of CALL args)
		"0x0",        // retOffset
		"0x0",        // argsSize
		"0x0",        // argsOffset
		"0x0",        // value
		"0x7a250d5630b4cf539739df2c5dacb4c659f2488d", // addr (len-2)
		"0x5208", // gas (top, len-1)
	}

	result := p.extractCallAddress(&execution.StructLog{
		Op:    "CALL",
		Stack: &stack,
	})

	assert.NotNil(t, result)
	assert.Equal(t, "0x7a250d5630b4cf539739df2c5dacb4c659f2488d", *result)
}

func TestExtractCallAddress_CALLCODE(t *testing.T) {
	p := &Processor{}
	// CALLCODE has same stack layout as CALL
	stack := []string{
		"0x7a250d5630b4cf539739df2c5dacb4c659f2488d", // addr
		"0x5208", // gas
	}

	result := p.extractCallAddress(&execution.StructLog{
		Op:    "CALLCODE",
		Stack: &stack,
	})

	assert.NotNil(t, result)
	assert.Equal(t, "0x7a250d5630b4cf539739df2c5dacb4c659f2488d", *result)
}

func TestExtractCallAddress_DELEGATECALL(t *testing.T) {
	p := &Processor{}
	// DELEGATECALL stack (no value parameter, but addr still at len-2):
	// [retSize, retOffset, argsSize, argsOffset, addr, gas]
	stack := []string{
		"0x7a250d5630b4cf539739df2c5dacb4c659f2488d", // addr
		"0x5208", // gas
	}

	result := p.extractCallAddress(&execution.StructLog{
		Op:    "DELEGATECALL",
		Stack: &stack,
	})

	assert.NotNil(t, result)
	assert.Equal(t, "0x7a250d5630b4cf539739df2c5dacb4c659f2488d", *result)
}

func TestExtractCallAddress_STATICCALL(t *testing.T) {
	p := &Processor{}
	// STATICCALL has same stack layout as DELEGATECALL
	stack := []string{
		"0x7a250d5630b4cf539739df2c5dacb4c659f2488d", // addr
		"0x5208", // gas
	}

	result := p.extractCallAddress(&execution.StructLog{
		Op:    "STATICCALL",
		Stack: &stack,
	})

	assert.NotNil(t, result)
	assert.Equal(t, "0x7a250d5630b4cf539739df2c5dacb4c659f2488d", *result)
}

func TestExtractCallAddress_NonCallOpcode(t *testing.T) {
	p := &Processor{}
	stack := []string{"0x1234", "0x5678"}

	testCases := []string{
		"PUSH1",
		"ADD",
		"SLOAD",
		"SSTORE",
		"JUMP",
		"RETURN",
		"REVERT",
		"CREATE",  // CREATE is not handled (address comes from trace)
		"CREATE2", // CREATE2 is not handled (address comes from trace)
	}

	for _, op := range testCases {
		t.Run(op, func(t *testing.T) {
			result := p.extractCallAddress(&execution.StructLog{
				Op:    op,
				Stack: &stack,
			})
			assert.Nil(t, result, "opcode %s should not extract call address", op)
		})
	}
}

func TestExtractCallAddress_ShortAddressPadding(t *testing.T) {
	p := &Processor{}
	// Test that short addresses (like precompiles) get zero-padded
	stack := []string{
		"0x1",    // addr - precompile ecRecover, should be padded
		"0x5208", // gas
	}

	result := p.extractCallAddress(&execution.StructLog{
		Op:    "CALL",
		Stack: &stack,
	})

	assert.NotNil(t, result)
	assert.Equal(t, "0x0000000000000000000000000000000000000001", *result)
	assert.Len(t, *result, 42)
}

func TestExtractCallAddress_Permit2Padding(t *testing.T) {
	p := &Processor{}
	// Test Permit2 address with leading zeros
	stack := []string{
		"0x22d473030f116ddee9f6b43ac78ba3", // Permit2 truncated
		"0x5208",                           // gas
	}

	result := p.extractCallAddress(&execution.StructLog{
		Op:    "CALL",
		Stack: &stack,
	})

	assert.NotNil(t, result)
	assert.Equal(t, "0x000000000022d473030f116ddee9f6b43ac78ba3", *result)
	assert.Len(t, *result, 42)
}

func TestExtractCallAddress_AllCallVariants(t *testing.T) {
	// Table-driven test for all supported CALL variants
	p := &Processor{}

	targetAddr := "0x7a250d5630b4cf539739df2c5dacb4c659f2488d"

	testCases := []struct {
		name  string
		op    string
		stack []string // Stack with addr at len-2 and gas at len-1
	}{
		{
			name:  "CALL with full stack",
			op:    "CALL",
			stack: []string{"0xretSize", "0xretOff", "0xargsSize", "0xargsOff", "0xvalue", targetAddr, "0xgas"},
		},
		{
			name:  "CALLCODE with full stack",
			op:    "CALLCODE",
			stack: []string{"0xretSize", "0xretOff", "0xargsSize", "0xargsOff", "0xvalue", targetAddr, "0xgas"},
		},
		{
			name:  "DELEGATECALL with full stack",
			op:    "DELEGATECALL",
			stack: []string{"0xretSize", "0xretOff", "0xargsSize", "0xargsOff", targetAddr, "0xgas"},
		},
		{
			name:  "STATICCALL with full stack",
			op:    "STATICCALL",
			stack: []string{"0xretSize", "0xretOff", "0xargsSize", "0xargsOff", targetAddr, "0xgas"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := p.extractCallAddress(&execution.StructLog{
				Op:    tc.op,
				Stack: &tc.stack,
			})
			assert.NotNil(t, result)
			assert.Equal(t, targetAddr, *result)
		})
	}
}
