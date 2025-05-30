package structlog

import (
	"encoding/json"
	"math/big"

	"github.com/hibiken/asynq"
)

const (
	ProcessorName            = "transaction_structlog"
	ProcessForwardsTaskType  = "transaction_structlog_process_forwards"
	ProcessBackwardsTaskType = "transaction_structlog_process_backwards"
	VerifyForwardsTaskType   = "transaction_structlog_verify_forwards"
	VerifyBackwardsTaskType  = "transaction_structlog_verify_backwards"
)

// ProcessPayload represents the payload for processing a transaction
type ProcessPayload struct {
	BlockNumber      big.Int `json:"block_number"`
	TransactionHash  string  `json:"transaction_hash"`
	TransactionIndex uint32  `json:"transaction_index"`
	NetworkID        int32   `json:"network_id"`
	NetworkName      string  `json:"network_name"`
	Network          string  `json:"network"`         // Alias for NetworkName
	ProcessingMode   string  `json:"processing_mode"` // "forwards" or "backwards"
}

// MarshalBinary implements encoding.BinaryMarshaler
func (p *ProcessPayload) MarshalBinary() ([]byte, error) {
	return json.Marshal(p)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (p *ProcessPayload) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, p)
}

// VerifyPayload represents the payload for verifying a transaction
type VerifyPayload struct {
	BlockNumber      big.Int `json:"block_number"`
	TransactionHash  string  `json:"transaction_hash"`
	TransactionIndex uint32  `json:"transaction_index"`
	NetworkID        int32   `json:"network_id"`
	NetworkName      string  `json:"network_name"`
	Network          string  `json:"network"`         // Alias for NetworkName
	ProcessingMode   string  `json:"processing_mode"` // "forwards" or "backwards"
	InsertedCount    int     `json:"inserted_count"`
}

// MarshalBinary implements encoding.BinaryMarshaler
func (v *VerifyPayload) MarshalBinary() ([]byte, error) {
	return json.Marshal(v)
}

// UnmarshalBinary implements encoding.BinaryUnmarshaler
func (v *VerifyPayload) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, v)
}

// NewProcessForwardsTask creates a new forwards process task
func NewProcessForwardsTask(payload *ProcessPayload) (*asynq.Task, error) {
	payload.ProcessingMode = "forwards"

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return asynq.NewTask(ProcessForwardsTaskType, data), nil
}

// NewProcessBackwardsTask creates a new backwards process task
func NewProcessBackwardsTask(payload *ProcessPayload) (*asynq.Task, error) {
	payload.ProcessingMode = "backwards"

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return asynq.NewTask(ProcessBackwardsTaskType, data), nil
}

// NewVerifyForwardsTask creates a new forwards verify task
func NewVerifyForwardsTask(payload *VerifyPayload) (*asynq.Task, error) {
	payload.ProcessingMode = "forwards"

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return asynq.NewTask(VerifyForwardsTaskType, data), nil
}

// NewVerifyBackwardsTask creates a new backwards verify task
func NewVerifyBackwardsTask(payload *VerifyPayload) (*asynq.Task, error) {
	payload.ProcessingMode = "backwards"

	data, err := json.Marshal(payload)
	if err != nil {
		return nil, err
	}

	return asynq.NewTask(VerifyBackwardsTaskType, data), nil
}
