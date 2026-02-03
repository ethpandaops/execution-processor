package tracker

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const testProcessorName = "test_processor"

func TestProcessReprocessForwardsQueue(t *testing.T) {
	tests := []struct {
		name          string
		processorName string
		expected      string
	}{
		{
			name:          "transaction_structlog processor",
			processorName: "transaction_structlog",
			expected:      "transaction_structlog:process:reprocess:forwards",
		},
		{
			name:          "transaction_simple processor",
			processorName: "transaction_simple",
			expected:      "transaction_simple:process:reprocess:forwards",
		},
		{
			name:          "transaction_structlog_agg processor",
			processorName: "transaction_structlog_agg",
			expected:      "transaction_structlog_agg:process:reprocess:forwards",
		},
		{
			name:          "custom processor name",
			processorName: "custom_processor",
			expected:      "custom_processor:process:reprocess:forwards",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ProcessReprocessForwardsQueue(tt.processorName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPrefixedProcessReprocessForwardsQueue(t *testing.T) {
	tests := []struct {
		name          string
		processorName string
		prefix        string
		expected      string
	}{
		{
			name:          "with prefix",
			processorName: "transaction_structlog",
			prefix:        "execution-processor",
			expected:      "execution-processor:transaction_structlog:process:reprocess:forwards",
		},
		{
			name:          "with different prefix",
			processorName: "transaction_simple",
			prefix:        "test-prefix",
			expected:      "test-prefix:transaction_simple:process:reprocess:forwards",
		},
		{
			name:          "structlog_agg with prefix",
			processorName: "transaction_structlog_agg",
			prefix:        "my-app",
			expected:      "my-app:transaction_structlog_agg:process:reprocess:forwards",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := PrefixedProcessReprocessForwardsQueue(tt.processorName, tt.prefix)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPrefixedProcessReprocessForwardsQueue_EmptyPrefix(t *testing.T) {
	tests := []struct {
		name          string
		processorName string
		expected      string
	}{
		{
			name:          "transaction_structlog without prefix",
			processorName: "transaction_structlog",
			expected:      "transaction_structlog:process:reprocess:forwards",
		},
		{
			name:          "transaction_simple without prefix",
			processorName: "transaction_simple",
			expected:      "transaction_simple:process:reprocess:forwards",
		},
		{
			name:          "transaction_structlog_agg without prefix",
			processorName: "transaction_structlog_agg",
			expected:      "transaction_structlog_agg:process:reprocess:forwards",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := PrefixedProcessReprocessForwardsQueue(tt.processorName, "")
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestProcessReprocessBackwardsQueue(t *testing.T) {
	tests := []struct {
		name          string
		processorName string
		expected      string
	}{
		{
			name:          "transaction_structlog processor",
			processorName: "transaction_structlog",
			expected:      "transaction_structlog:process:reprocess:backwards",
		},
		{
			name:          "transaction_simple processor",
			processorName: "transaction_simple",
			expected:      "transaction_simple:process:reprocess:backwards",
		},
		{
			name:          "transaction_structlog_agg processor",
			processorName: "transaction_structlog_agg",
			expected:      "transaction_structlog_agg:process:reprocess:backwards",
		},
		{
			name:          "custom processor name",
			processorName: "custom_processor",
			expected:      "custom_processor:process:reprocess:backwards",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ProcessReprocessBackwardsQueue(tt.processorName)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPrefixedProcessReprocessBackwardsQueue(t *testing.T) {
	tests := []struct {
		name          string
		processorName string
		prefix        string
		expected      string
	}{
		{
			name:          "with prefix",
			processorName: "transaction_structlog",
			prefix:        "execution-processor",
			expected:      "execution-processor:transaction_structlog:process:reprocess:backwards",
		},
		{
			name:          "with different prefix",
			processorName: "transaction_simple",
			prefix:        "test-prefix",
			expected:      "test-prefix:transaction_simple:process:reprocess:backwards",
		},
		{
			name:          "structlog_agg with prefix",
			processorName: "transaction_structlog_agg",
			prefix:        "my-app",
			expected:      "my-app:transaction_structlog_agg:process:reprocess:backwards",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := PrefixedProcessReprocessBackwardsQueue(tt.processorName, tt.prefix)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestPrefixedProcessReprocessBackwardsQueue_EmptyPrefix(t *testing.T) {
	tests := []struct {
		name          string
		processorName string
		expected      string
	}{
		{
			name:          "transaction_structlog without prefix",
			processorName: "transaction_structlog",
			expected:      "transaction_structlog:process:reprocess:backwards",
		},
		{
			name:          "transaction_simple without prefix",
			processorName: "transaction_simple",
			expected:      "transaction_simple:process:reprocess:backwards",
		},
		{
			name:          "transaction_structlog_agg without prefix",
			processorName: "transaction_structlog_agg",
			expected:      "transaction_structlog_agg:process:reprocess:backwards",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := PrefixedProcessReprocessBackwardsQueue(tt.processorName, "")
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestQueueConsistency(t *testing.T) {
	// Ensure all queue functions follow consistent naming conventions
	processorName := testProcessorName
	prefix := "prefix"

	forwardsUnprefixed := ProcessForwardsQueue(processorName)
	backwardsUnprefixed := ProcessBackwardsQueue(processorName)
	reprocessForwardsUnprefixed := ProcessReprocessForwardsQueue(processorName)
	reprocessBackwardsUnprefixed := ProcessReprocessBackwardsQueue(processorName)

	// All should have same base pattern
	assert.Contains(t, forwardsUnprefixed, processorName+":process:")
	assert.Contains(t, backwardsUnprefixed, processorName+":process:")
	assert.Contains(t, reprocessForwardsUnprefixed, processorName+":process:reprocess:")
	assert.Contains(t, reprocessBackwardsUnprefixed, processorName+":process:reprocess:")

	// Reprocess queues should contain their mode suffix
	assert.Contains(t, reprocessForwardsUnprefixed, "forwards")
	assert.Contains(t, reprocessBackwardsUnprefixed, "backwards")

	forwardsPrefixed := PrefixedProcessForwardsQueue(processorName, prefix)
	backwardsPrefixed := PrefixedProcessBackwardsQueue(processorName, prefix)
	reprocessForwardsPrefixed := PrefixedProcessReprocessForwardsQueue(processorName, prefix)
	reprocessBackwardsPrefixed := PrefixedProcessReprocessBackwardsQueue(processorName, prefix)

	// All prefixed versions should start with prefix
	assert.Contains(t, forwardsPrefixed, prefix+":")
	assert.Contains(t, backwardsPrefixed, prefix+":")
	assert.Contains(t, reprocessForwardsPrefixed, prefix+":")
	assert.Contains(t, reprocessBackwardsPrefixed, prefix+":")

	// Empty prefix should return unprefixed
	assert.Equal(t, forwardsUnprefixed, PrefixedProcessForwardsQueue(processorName, ""))
	assert.Equal(t, backwardsUnprefixed, PrefixedProcessBackwardsQueue(processorName, ""))
	assert.Equal(t, reprocessForwardsUnprefixed, PrefixedProcessReprocessForwardsQueue(processorName, ""))
	assert.Equal(t, reprocessBackwardsUnprefixed, PrefixedProcessReprocessBackwardsQueue(processorName, ""))
}
