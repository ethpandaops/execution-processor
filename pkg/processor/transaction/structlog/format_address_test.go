package structlog

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFormatAddress(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "already 40 chars with 0x prefix",
			input:    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
			expected: "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
		},
		{
			name:     "already 40 chars without 0x prefix",
			input:    "7a250d5630b4cf539739df2c5dacb4c659f2488d",
			expected: "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
		},
		{
			name:     "precompile address 0x1",
			input:    "0x1",
			expected: "0x0000000000000000000000000000000000000001",
		},
		{
			name:     "precompile address 0xa",
			input:    "0xa",
			expected: "0x000000000000000000000000000000000000000a",
		},
		{
			name:     "Permit2 with leading zeros truncated",
			input:    "0x22d473030f116ddee9f6b43ac78ba3",
			expected: "0x000000000022d473030f116ddee9f6b43ac78ba3",
		},
		{
			name:     "Uniswap PoolManager with leading zeros truncated",
			input:    "0x4444c5dc75cb358380d2e3de08a90",
			expected: "0x000000000004444c5dc75cb358380d2e3de08a90",
		},
		{
			name:     "zero address",
			input:    "0x0",
			expected: "0x0000000000000000000000000000000000000000",
		},
		{
			name:     "short address without 0x prefix",
			input:    "5208",
			expected: "0x0000000000000000000000000000000000005208",
		},
		{
			name:     "short address with 0x prefix",
			input:    "0x5208",
			expected: "0x0000000000000000000000000000000000005208",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "0x0000000000000000000000000000000000000000",
		},
		{
			name:     "just 0x prefix",
			input:    "0x",
			expected: "0x0000000000000000000000000000000000000000",
		},
		// Full 32-byte stack values (66 chars) - extract lower 20 bytes
		{
			name:     "full 32-byte stack value from XEN Batch Minter",
			input:    "0x661f30bf3a790c8687131ae8fc6e649df9f27275fc286db8f1a0be7e99b24bb2",
			expected: "0xfc6e649df9f27275fc286db8f1a0be7e99b24bb2",
		},
		{
			name:     "full 32-byte stack value - all zeros except address",
			input:    "0x0000000000000000000000007a250d5630b4cf539739df2c5dacb4c659f2488d",
			expected: "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
		},
		{
			name:     "full 32-byte stack value without 0x prefix",
			input:    "661f30bf3a790c8687131ae8fc6e649df9f27275fc286db8f1a0be7e99b24bb2",
			expected: "0xfc6e649df9f27275fc286db8f1a0be7e99b24bb2",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := formatAddress(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestFormatAddress_LengthConsistency(t *testing.T) {
	// All formatted addresses should be exactly 42 characters (0x + 40 hex chars)
	inputs := []string{
		"0x1",
		"0xa",
		"0xdeadbeef",
		"0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
		"1",
		"abcdef",
		"",
	}

	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			result := formatAddress(input)
			assert.Len(t, result, 42, "formatted address should always be 42 chars")
			assert.Equal(t, "0x", result[:2], "formatted address should start with 0x")
		})
	}
}
