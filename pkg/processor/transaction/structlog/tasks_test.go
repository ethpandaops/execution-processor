package structlog_test

import (
	"encoding/json"
	"math/big"
	"testing"

	transaction_structlog "github.com/ethpandaops/execution-processor/pkg/processor/transaction/structlog"
	"github.com/hibiken/asynq"
)

func TestProcessPayload(t *testing.T) {
	payload := &transaction_structlog.ProcessPayload{
		BlockNumber:      *big.NewInt(12345),
		TransactionHash:  "0x1234567890abcdef",
		TransactionIndex: 5,
		NetworkID:        1,
		NetworkName:      "mainnet",
		Network:          "mainnet",
		ProcessingMode:   "forwards",
	}

	// Test JSON marshaling
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}

	var unmarshaled transaction_structlog.ProcessPayload

	err = json.Unmarshal(data, &unmarshaled)
	if err != nil {
		t.Fatalf("failed to unmarshal payload: %v", err)
	}

	if unmarshaled.TransactionHash != payload.TransactionHash {
		t.Errorf("expected transaction hash %s, got %s", payload.TransactionHash, unmarshaled.TransactionHash)
	}

	if unmarshaled.TransactionIndex != payload.TransactionIndex {
		t.Errorf("expected transaction index %d, got %d", payload.TransactionIndex, unmarshaled.TransactionIndex)
	}

	// Test binary marshaling
	binData, err := payload.MarshalBinary()
	if err != nil {
		t.Fatalf("failed to marshal binary: %v", err)
	}

	var binUnmarshaled transaction_structlog.ProcessPayload

	err = binUnmarshaled.UnmarshalBinary(binData)
	if err != nil {
		t.Fatalf("failed to unmarshal binary: %v", err)
	}

	if binUnmarshaled.TransactionHash != payload.TransactionHash {
		t.Errorf("expected transaction hash %s, got %s", payload.TransactionHash, binUnmarshaled.TransactionHash)
	}
}

func TestVerifyPayload(t *testing.T) {
	payload := &transaction_structlog.VerifyPayload{
		BlockNumber:      *big.NewInt(12345),
		TransactionHash:  "0xabcdef1234567890",
		TransactionIndex: 3,
		NetworkID:        1,
		NetworkName:      "mainnet",
		Network:          "mainnet",
		ProcessingMode:   "forwards",
	}

	// Test JSON marshaling
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}

	var unmarshaled transaction_structlog.VerifyPayload

	err = json.Unmarshal(data, &unmarshaled)
	if err != nil {
		t.Fatalf("failed to unmarshal payload: %v", err)
	}

	if unmarshaled.TransactionHash != payload.TransactionHash {
		t.Errorf("expected transaction hash %s, got %s", payload.TransactionHash, unmarshaled.TransactionHash)
	}

	// Test binary marshaling
	binData, err := payload.MarshalBinary()
	if err != nil {
		t.Fatalf("failed to marshal binary: %v", err)
	}

	var binUnmarshaled transaction_structlog.VerifyPayload

	err = binUnmarshaled.UnmarshalBinary(binData)
	if err != nil {
		t.Fatalf("failed to unmarshal binary: %v", err)
	}

	if binUnmarshaled.TransactionHash != payload.TransactionHash {
		t.Errorf("expected transaction hash %s, got %s", payload.TransactionHash, binUnmarshaled.TransactionHash)
	}
}

func TestNewProcessForwardsTask(t *testing.T) {
	payload := &transaction_structlog.ProcessPayload{
		BlockNumber:      *big.NewInt(12345),
		TransactionHash:  "0x1234567890abcdef",
		TransactionIndex: 5,
		NetworkID:        1,
		NetworkName:      "mainnet",
		Network:          "mainnet",
	}

	task, err := transaction_structlog.NewProcessForwardsTask(payload)
	if err != nil {
		t.Fatalf("failed to create task: %v", err)
	}

	if task.Type() != transaction_structlog.ProcessForwardsTaskType {
		t.Errorf("expected task type %s, got %s", transaction_structlog.ProcessForwardsTaskType, task.Type())
	}

	// Verify payload can be unmarshaled from task
	var unmarshaled transaction_structlog.ProcessPayload

	err = json.Unmarshal(task.Payload(), &unmarshaled)
	if err != nil {
		t.Fatalf("failed to unmarshal task payload: %v", err)
	}

	if unmarshaled.TransactionHash != payload.TransactionHash {
		t.Errorf("expected transaction hash %s, got %s", payload.TransactionHash, unmarshaled.TransactionHash)
	}

	if unmarshaled.ProcessingMode != "forwards" {
		t.Errorf("expected processing mode 'forwards', got %s", unmarshaled.ProcessingMode)
	}
}

func TestNewProcessBackwardsTask(t *testing.T) {
	payload := &transaction_structlog.ProcessPayload{
		BlockNumber:      *big.NewInt(12345),
		TransactionHash:  "0x1234567890abcdef",
		TransactionIndex: 5,
		NetworkID:        1,
		NetworkName:      "mainnet",
		Network:          "mainnet",
	}

	task, err := transaction_structlog.NewProcessBackwardsTask(payload)
	if err != nil {
		t.Fatalf("failed to create task: %v", err)
	}

	if task.Type() != transaction_structlog.ProcessBackwardsTaskType {
		t.Errorf("expected task type %s, got %s", transaction_structlog.ProcessBackwardsTaskType, task.Type())
	}

	// Verify payload can be unmarshaled from task
	var unmarshaled transaction_structlog.ProcessPayload

	err = json.Unmarshal(task.Payload(), &unmarshaled)
	if err != nil {
		t.Fatalf("failed to unmarshal task payload: %v", err)
	}

	if unmarshaled.TransactionHash != payload.TransactionHash {
		t.Errorf("expected transaction hash %s, got %s", payload.TransactionHash, unmarshaled.TransactionHash)
	}

	if unmarshaled.ProcessingMode != "backwards" {
		t.Errorf("expected processing mode 'backwards', got %s", unmarshaled.ProcessingMode)
	}
}

func TestNewVerifyForwardsTask(t *testing.T) {
	payload := &transaction_structlog.VerifyPayload{
		BlockNumber:      *big.NewInt(12345),
		TransactionHash:  "0xabcdef1234567890",
		TransactionIndex: 3,
		NetworkID:        1,
		NetworkName:      "mainnet",
		Network:          "mainnet",
	}

	task, err := transaction_structlog.NewVerifyForwardsTask(payload)
	if err != nil {
		t.Fatalf("failed to create task: %v", err)
	}

	if task.Type() != transaction_structlog.VerifyForwardsTaskType {
		t.Errorf("expected task type %s, got %s", transaction_structlog.VerifyForwardsTaskType, task.Type())
	}

	// Verify payload can be unmarshaled from task
	var unmarshaled transaction_structlog.VerifyPayload

	err = json.Unmarshal(task.Payload(), &unmarshaled)
	if err != nil {
		t.Fatalf("failed to unmarshal task payload: %v", err)
	}

	if unmarshaled.TransactionHash != payload.TransactionHash {
		t.Errorf("expected transaction hash %s, got %s", payload.TransactionHash, unmarshaled.TransactionHash)
	}

	if unmarshaled.ProcessingMode != "forwards" {
		t.Errorf("expected processing mode 'forwards', got %s", unmarshaled.ProcessingMode)
	}
}

func TestNewVerifyBackwardsTask(t *testing.T) {
	payload := &transaction_structlog.VerifyPayload{
		BlockNumber:      *big.NewInt(12345),
		TransactionHash:  "0xabcdef1234567890",
		TransactionIndex: 3,
		NetworkID:        1,
		NetworkName:      "mainnet",
		Network:          "mainnet",
	}

	task, err := transaction_structlog.NewVerifyBackwardsTask(payload)
	if err != nil {
		t.Fatalf("failed to create task: %v", err)
	}

	if task.Type() != transaction_structlog.VerifyBackwardsTaskType {
		t.Errorf("expected task type %s, got %s", transaction_structlog.VerifyBackwardsTaskType, task.Type())
	}

	// Verify payload can be unmarshaled from task
	var unmarshaled transaction_structlog.VerifyPayload

	err = json.Unmarshal(task.Payload(), &unmarshaled)
	if err != nil {
		t.Fatalf("failed to unmarshal task payload: %v", err)
	}

	if unmarshaled.TransactionHash != payload.TransactionHash {
		t.Errorf("expected transaction hash %s, got %s", payload.TransactionHash, unmarshaled.TransactionHash)
	}

	if unmarshaled.ProcessingMode != "backwards" {
		t.Errorf("expected processing mode 'backwards', got %s", unmarshaled.ProcessingMode)
	}
}

func TestTaskTypes(t *testing.T) {
	// Test that task type constants are defined
	if transaction_structlog.ProcessForwardsTaskType == "" {
		t.Error("ProcessForwardsTaskType should not be empty")
	}

	if transaction_structlog.ProcessBackwardsTaskType == "" {
		t.Error("ProcessBackwardsTaskType should not be empty")
	}

	if transaction_structlog.VerifyForwardsTaskType == "" {
		t.Error("VerifyForwardsTaskType should not be empty")
	}

	if transaction_structlog.VerifyBackwardsTaskType == "" {
		t.Error("VerifyBackwardsTaskType should not be empty")
	}

	if transaction_structlog.ProcessorName == "" {
		t.Error("ProcessorName should not be empty")
	}
}

func TestAsynqTaskCreation(t *testing.T) {
	// Test that we can create asynq tasks manually
	payload := map[string]interface{}{
		"block_number":      "12345",
		"transaction_hash":  "0x1234567890abcdef",
		"transaction_index": 5,
		"network_id":        1,
		"network_name":      "mainnet",
		"processing_mode":   "forwards",
	}

	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("failed to marshal payload: %v", err)
	}

	task := asynq.NewTask(transaction_structlog.ProcessForwardsTaskType, data)
	if task == nil {
		t.Fatal("expected task to be created")
	}

	if task.Type() != transaction_structlog.ProcessForwardsTaskType {
		t.Errorf("expected task type %s, got %s", transaction_structlog.ProcessForwardsTaskType, task.Type())
	}
}
