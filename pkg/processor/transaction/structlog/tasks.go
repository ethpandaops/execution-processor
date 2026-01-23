package structlog

import (
	"encoding/json"
	"math/big"

	"github.com/hibiken/asynq"

	"github.com/ethpandaops/execution-processor/pkg/processor/tracker"
)

const (
	ProcessorName            = "transaction_structlog"
	ProcessForwardsTaskType  = "transaction_structlog_process_forwards"
	ProcessBackwardsTaskType = "transaction_structlog_process_backwards"
)

// ProcessPayload represents the payload for processing a transaction.
//
//nolint:tagliatelle // Using snake_case for backwards compatibility
type ProcessPayload struct {
	BlockNumber      big.Int `json:"block_number"`
	TransactionHash  string  `json:"transaction_hash"`
	TransactionIndex uint32  `json:"transaction_index"`
	NetworkName      string  `json:"network_name"`
	Network          string  `json:"network"`         // Alias for NetworkName
	ProcessingMode   string  `json:"processing_mode"` // "forwards" or "backwards"
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (p *ProcessPayload) MarshalBinary() ([]byte, error) {
	return json.Marshal(p)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (p *ProcessPayload) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, p)
}

// NewProcessForwardsTask creates a new forwards process task.
func NewProcessForwardsTask(payload *ProcessPayload) (*asynq.Task, error) {
	payload.ProcessingMode = tracker.FORWARDS_MODE

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return asynq.NewTask(ProcessForwardsTaskType, data), nil
}

// NewProcessBackwardsTask creates a new backwards process task.
func NewProcessBackwardsTask(payload *ProcessPayload) (*asynq.Task, error) {
	payload.ProcessingMode = tracker.BACKWARDS_MODE

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return asynq.NewTask(ProcessBackwardsTaskType, data), nil
}
