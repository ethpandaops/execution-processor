package simple

import (
	"encoding/json"
	"math/big"

	"github.com/hibiken/asynq"

	c "github.com/ethpandaops/execution-processor/pkg/processor/common"
)

const (
	// ProcessForwardsTaskType is the task type for forwards processing.
	ProcessForwardsTaskType = "transaction_simple_process_forwards"
	// ProcessBackwardsTaskType is the task type for backwards processing.
	ProcessBackwardsTaskType = "transaction_simple_process_backwards"
	// VerifyForwardsTaskType is the task type for forwards verification.
	VerifyForwardsTaskType = "transaction_simple_verify_forwards"
	// VerifyBackwardsTaskType is the task type for backwards verification.
	VerifyBackwardsTaskType = "transaction_simple_verify_backwards"
)

// ProcessPayload represents the payload for processing a block.
//
//nolint:tagliatelle // snake_case required for backwards compatibility with queued tasks
type ProcessPayload struct {
	BlockNumber    big.Int `json:"block_number"`
	NetworkID      int32   `json:"network_id"`
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

// VerifyPayload represents the payload for verifying a block.
//
//nolint:tagliatelle // snake_case required for backwards compatibility with queued tasks
type VerifyPayload struct {
	BlockNumber   big.Int `json:"block_number"`
	NetworkID     int32   `json:"network_id"`
	NetworkName   string  `json:"network_name"`
	InsertedCount int     `json:"inserted_count"`
}

// MarshalBinary implements encoding.BinaryMarshaler.
func (v *VerifyPayload) MarshalBinary() ([]byte, error) {
	return json.Marshal(v)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler.
func (v *VerifyPayload) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, v)
}

// NewProcessForwardsTask creates a new forwards process task.
func NewProcessForwardsTask(payload *ProcessPayload) (*asynq.Task, error) {
	payload.ProcessingMode = c.FORWARDS_MODE

	data, err := payload.MarshalBinary()
	if err != nil {
		return nil, err
	}

	return asynq.NewTask(ProcessForwardsTaskType, data), nil
}

// NewProcessBackwardsTask creates a new backwards process task.
func NewProcessBackwardsTask(payload *ProcessPayload) (*asynq.Task, error) {
	payload.ProcessingMode = c.BACKWARDS_MODE

	data, err := payload.MarshalBinary()
	if err != nil {
		return nil, err
	}

	return asynq.NewTask(ProcessBackwardsTaskType, data), nil
}

// NewVerifyForwardsTask creates a new forwards verify task.
func NewVerifyForwardsTask(payload *VerifyPayload) (*asynq.Task, error) {
	data, err := payload.MarshalBinary()
	if err != nil {
		return nil, err
	}

	return asynq.NewTask(VerifyForwardsTaskType, data), nil
}

// NewVerifyBackwardsTask creates a new backwards verify task.
func NewVerifyBackwardsTask(payload *VerifyPayload) (*asynq.Task, error) {
	data, err := payload.MarshalBinary()
	if err != nil {
		return nil, err
	}

	return asynq.NewTask(VerifyBackwardsTaskType, data), nil
}
