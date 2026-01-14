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
	// CALL stack: [gas, addr, value, argsOffset, argsSize, retOffset, retSize]
	// Address is at position len-2 (second from top)
	stack := []string{
		"0x5208", // gas
		"0x7a250d5630b4cf539739df2c5dacb4c659f2488d", // addr (target)
		"0x0", // value
		"0x0", // argsOffset
		"0x0", // argsSize
		"0x0", // retOffset
		"0x0", // retSize
	}

	result := p.extractCallAddress(&execution.StructLog{
		Op:    "CALL",
		Stack: &stack,
	})

	assert.NotNil(t, result)
	assert.Equal(t, "0x0", *result) // Second from top is retOffset (0x0)
}

func TestExtractCallAddress_CALL_MinimalStack(t *testing.T) {
	p := &Processor{}
	// Minimal stack with just 2 elements
	stack := []string{
		"0x5208", // gas
		"0x7a250d5630b4cf539739df2c5dacb4c659f2488d", // addr
	}

	result := p.extractCallAddress(&execution.StructLog{
		Op:    "CALL",
		Stack: &stack,
	})

	assert.NotNil(t, result)
	assert.Equal(t, "0x5208", *result) // Second from top
}

func TestExtractCallAddress_CALLCODE(t *testing.T) {
	p := &Processor{}
	// CALLCODE has same stack layout as CALL
	stack := []string{
		"0x5208",
		"0xdeadbeef",
	}

	result := p.extractCallAddress(&execution.StructLog{
		Op:    "CALLCODE",
		Stack: &stack,
	})

	assert.NotNil(t, result)
	assert.Equal(t, "0x5208", *result)
}

func TestExtractCallAddress_DELEGATECALL(t *testing.T) {
	p := &Processor{}
	// DELEGATECALL stack: [gas, addr, argsOffset, argsSize, retOffset, retSize]
	// (no value parameter)
	stack := []string{
		"0x5208",
		"0xdeadbeef",
	}

	result := p.extractCallAddress(&execution.StructLog{
		Op:    "DELEGATECALL",
		Stack: &stack,
	})

	assert.NotNil(t, result)
	assert.Equal(t, "0x5208", *result)
}

func TestExtractCallAddress_STATICCALL(t *testing.T) {
	p := &Processor{}
	// STATICCALL has same stack layout as DELEGATECALL
	stack := []string{
		"0x5208",
		"0xdeadbeef",
	}

	result := p.extractCallAddress(&execution.StructLog{
		Op:    "STATICCALL",
		Stack: &stack,
	})

	assert.NotNil(t, result)
	assert.Equal(t, "0x5208", *result)
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
		"CREATE",  // CREATE is not handled (address comes from receipt)
		"CREATE2", // CREATE2 is not handled (address comes from receipt)
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

func TestExtractCallAddress_AllCallVariants(t *testing.T) {
	// Table-driven test for all supported CALL variants
	p := &Processor{}

	testCases := []struct {
		name     string
		op       string
		stack    []string
		expected string
	}{
		{
			name:     "CALL with full stack",
			op:       "CALL",
			stack:    []string{"0xgas", "0xaddr", "0xvalue", "0xargsOff", "0xargsSize", "0xretOff", "0xretSize"},
			expected: "0xretOff", // len-2 position
		},
		{
			name:     "CALLCODE with full stack",
			op:       "CALLCODE",
			stack:    []string{"0xgas", "0xaddr", "0xvalue", "0xargsOff", "0xargsSize", "0xretOff", "0xretSize"},
			expected: "0xretOff",
		},
		{
			name:     "DELEGATECALL with full stack",
			op:       "DELEGATECALL",
			stack:    []string{"0xgas", "0xaddr", "0xargsOff", "0xargsSize", "0xretOff", "0xretSize"},
			expected: "0xretOff",
		},
		{
			name:     "STATICCALL with full stack",
			op:       "STATICCALL",
			stack:    []string{"0xgas", "0xaddr", "0xargsOff", "0xargsSize", "0xretOff", "0xretSize"},
			expected: "0xretOff",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := p.extractCallAddress(&execution.StructLog{
				Op:    tc.op,
				Stack: &tc.stack,
			})
			assert.NotNil(t, result)
			assert.Equal(t, tc.expected, *result)
		})
	}
}
