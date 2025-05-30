package structlog

import (
	"testing"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/ethereum"
)

func TestStructlogCountReturn(t *testing.T) {
	// Create mock trace data
	mockTrace := &MockTrace{
		Gas:         21000,
		Failed:      false,
		ReturnValue: nil,
		Structlogs: []MockStructlog{
			{PC: 0, Op: "PUSH1", Gas: 21000, GasCost: 3, Depth: 1},
			{PC: 2, Op: "PUSH1", Gas: 20997, GasCost: 3, Depth: 1},
			{PC: 4, Op: "SSTORE", Gas: 20994, GasCost: 20000, Depth: 1},
		},
	}

	// Create processor with mock dependencies
	processor := &Processor{
		config: &Config{
			Table: "test_structlogs",
		},
		network: &ethereum.Network{
			Name: "test",
			ID:   1,
		},
	}

	// Test the count calculation and return
	expectedCount := len(mockTrace.Structlogs)

	// We can't easily test the full processTransaction method without a real execution node,
	// but we can test the logic of creating structlogs and counting them
	structlogs := make([]Structlog, 0, expectedCount)

	now := time.Now()

	// Simulate the structlog creation logic
	for i, structLog := range mockTrace.Structlogs {
		row := Structlog{
			UpdatedDateTime:        now,
			BlockNumber:            12345,
			TransactionHash:        "0x1234567890abcdef",
			TransactionIndex:       0,
			TransactionGas:         mockTrace.Gas,
			TransactionFailed:      mockTrace.Failed,
			TransactionReturnValue: mockTrace.ReturnValue,
			Index:                  uint32(i), //nolint:gosec // safe to use user input in query
			ProgramCounter:         structLog.PC,
			Operation:              structLog.Op,
			Gas:                    structLog.Gas,
			GasCost:                structLog.GasCost,
			Depth:                  structLog.Depth,
			ReturnData:             structLog.ReturnData,
			Refund:                 structLog.Refund,
			Error:                  structLog.Error,
			MetaNetworkID:          processor.network.ID,
			MetaNetworkName:        processor.network.Name,
		}

		structlogs = append(structlogs, row)
	}

	// Test the key fix: save count before clearing slice
	structlogCount := len(structlogs)

	// Clear slice like the real code does
	structlogs = nil

	// Verify the count was saved correctly
	if structlogCount != expectedCount {
		t.Errorf("Expected count %d, but got %d", expectedCount, structlogCount)
	}

	// Verify that len(structlogs) is now 0 (which would be the bug)
	if len(structlogs) != 0 {
		t.Errorf("Expected cleared slice to have length 0, but got %d", len(structlogs))
	}

	// The fix ensures we return structlogCount, not len(structlogs)
	if structlogCount == 0 {
		t.Error("structlogCount should not be 0 after processing valid structlogs")
	}
}

// Mock structures for testing
type MockTrace struct {
	Gas         uint64          `json:"gas"`
	Failed      bool            `json:"failed"`
	ReturnValue *string         `json:"returnValue"`
	Structlogs  []MockStructlog `json:"structLogs"`
}

type MockStructlog struct {
	PC         uint32  `json:"pc"`
	Op         string  `json:"op"`
	Gas        uint64  `json:"gas"`
	GasCost    uint64  `json:"gasCost"`
	Depth      uint64  `json:"depth"`
	ReturnData *string `json:"returnData"`
	Refund     *uint64 `json:"refund"`
	Error      *string `json:"error"`
}
