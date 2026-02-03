package simple

import (
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/hibiken/asynq"

	"github.com/ethpandaops/execution-processor/pkg/processor/tracker"
)

const (
	// ProcessForwardsTaskType is the task type for forwards processing.
	ProcessForwardsTaskType = "transaction_simple_process_forwards"
	// ProcessBackwardsTaskType is the task type for backwards processing.
	ProcessBackwardsTaskType = "transaction_simple_process_backwards"
)

// ProcessPayload represents the payload for processing a block.
//
//nolint:tagliatelle // snake_case required for backwards compatibility with queued tasks
type ProcessPayload struct {
	BlockNumber    big.Int `json:"block_number"`
	NetworkName    string  `json:"network_name"`
	ProcessingMode string  `json:"processing_mode"`
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
// Format: {processor}:{network}:{blockNum}:block.
// For block-based processors, we use "block" as the identifier since there's one task per block.
func GenerateTaskID(network string, blockNumber uint64) string {
	return fmt.Sprintf("%s:%s:%d:block", ProcessorName, network, blockNumber)
}

// NewProcessForwardsTask creates a new forwards process task.
// Returns the task, taskID for deduplication, and any error.
func NewProcessForwardsTask(payload *ProcessPayload) (*asynq.Task, string, error) {
	payload.ProcessingMode = tracker.FORWARDS_MODE

	data, err := payload.MarshalBinary()
	if err != nil {
		return nil, "", err
	}

	taskID := GenerateTaskID(payload.NetworkName, payload.BlockNumber.Uint64())

	return asynq.NewTask(ProcessForwardsTaskType, data), taskID, nil
}

// NewProcessBackwardsTask creates a new backwards process task.
// Returns the task, taskID for deduplication, and any error.
func NewProcessBackwardsTask(payload *ProcessPayload) (*asynq.Task, string, error) {
	payload.ProcessingMode = tracker.BACKWARDS_MODE

	data, err := payload.MarshalBinary()
	if err != nil {
		return nil, "", err
	}

	taskID := GenerateTaskID(payload.NetworkName, payload.BlockNumber.Uint64())

	return asynq.NewTask(ProcessBackwardsTaskType, data), taskID, nil
}
