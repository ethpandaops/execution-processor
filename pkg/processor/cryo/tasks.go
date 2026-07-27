package cryo

import (
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/hibiken/asynq"

	"github.com/ethpandaops/execution-processor/pkg/processor/tracker"
)

// ProcessPayload represents the payload for processing one block of one group.
//
//nolint:tagliatelle // snake_case matches the queued payloads of the other processors
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

// Task types are derived from the group rather than fixed at package level,
// because one config yields one processor per enabled group.
func (p *Processor) processForwardsTaskType() string {
	return p.name + "_process_forwards"
}

func (p *Processor) processBackwardsTaskType() string {
	return p.name + "_process_backwards"
}

// GenerateTaskID creates a deterministic task ID for deduplication.
func GenerateTaskID(processorName, network string, blockNumber uint64) string {
	return fmt.Sprintf("%s:%s:%d:block", processorName, network, blockNumber)
}

// newTask builds the task for a processing mode, returning it with the ID used
// for deduplication.
func (p *Processor) newTask(payload *ProcessPayload, mode string) (*asynq.Task, string, error) {
	payload.ProcessingMode = mode

	taskType := p.processForwardsTaskType()
	if mode == tracker.BACKWARDS_MODE {
		taskType = p.processBackwardsTaskType()
	}

	data, err := payload.MarshalBinary()
	if err != nil {
		return nil, "", fmt.Errorf("marshal payload: %w", err)
	}

	return asynq.NewTask(taskType, data), GenerateTaskID(p.name, payload.NetworkName, payload.BlockNumber.Uint64()), nil
}
