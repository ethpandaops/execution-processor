package structlog_agg

import (
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/hibiken/asynq"

	"github.com/ethpandaops/execution-processor/pkg/processor/tracker"
)

const (
	ProcessorName            = "transaction_structlog_agg"
	ProcessForwardsTaskType  = "transaction_structlog_agg_process_forwards"
	ProcessBackwardsTaskType = "transaction_structlog_agg_process_backwards"
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

// GenerateTaskID creates a deterministic task ID for deduplication.
// Format: {processor}:{network}:{blockNum}:{txHash}.
func GenerateTaskID(network string, blockNumber uint64, txHash string) string {
	return fmt.Sprintf("%s:%s:%d:%s", ProcessorName, network, blockNumber, txHash)
}

// NewProcessForwardsTask creates a new forwards process task.
// Returns the task, taskID for deduplication, and any error.
func NewProcessForwardsTask(payload *ProcessPayload) (*asynq.Task, string, error) {
	payload.ProcessingMode = tracker.FORWARDS_MODE

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, "", err
	}

	taskID := GenerateTaskID(payload.NetworkName, payload.BlockNumber.Uint64(), payload.TransactionHash)

	return asynq.NewTask(ProcessForwardsTaskType, data), taskID, nil
}

// NewProcessBackwardsTask creates a new backwards process task.
// Returns the task, taskID for deduplication, and any error.
func NewProcessBackwardsTask(payload *ProcessPayload) (*asynq.Task, string, error) {
	payload.ProcessingMode = tracker.BACKWARDS_MODE

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, "", err
	}

	taskID := GenerateTaskID(payload.NetworkName, payload.BlockNumber.Uint64(), payload.TransactionHash)

	return asynq.NewTask(ProcessBackwardsTaskType, data), taskID, nil
}
