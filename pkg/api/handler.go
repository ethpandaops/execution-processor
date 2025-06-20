package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/ethpandaops/execution-processor/pkg/ethereum"
	"github.com/ethpandaops/execution-processor/pkg/processor"
	"github.com/sirupsen/logrus"
)

const (
	maxBulkBlocks = 1000
)

type Handler struct {
	log       logrus.FieldLogger
	processor *processor.Manager
	pool      *ethereum.Pool
}

func NewHandler(log logrus.FieldLogger, processorManager *processor.Manager, pool *ethereum.Pool) *Handler {
	return &Handler{
		log:       log,
		processor: processorManager,
		pool:      pool,
	}
}

func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /api/v1/queue/block/{processor}/{block_number}", h.queueSingleBlock)
	mux.HandleFunc("POST /api/v1/queue/blocks/{processor}", h.queueMultipleBlocks)
}

type SingleBlockResponse struct {
	Status           string `json:"status"`
	BlockNumber      uint64 `json:"block_number"`
	Processor        string `json:"processor"`
	Queue            string `json:"queue"`
	TransactionCount int    `json:"transaction_count"`
	TasksCreated     int    `json:"tasks_created"`
}

type BlockResult struct {
	BlockNumber      uint64 `json:"block_number"`
	Status           string `json:"status"`
	TransactionCount int    `json:"transaction_count,omitempty"`
	TasksCreated     int    `json:"tasks_created,omitempty"`
	Error            string `json:"error,omitempty"`
}

type BulkBlocksRequest struct {
	Blocks []uint64 `json:"blocks"`
}

type BulkBlocksResponse struct {
	Status    string `json:"status"`
	Processor string `json:"processor"`
	Queue     string `json:"queue"`
	Summary   struct {
		Total   int `json:"total"`
		Queued  int `json:"queued"`
		Skipped int `json:"skipped"`
		Failed  int `json:"failed"`
	} `json:"summary"`
	Results []BlockResult `json:"results"`
}

type ErrorResponse struct {
	Error       string      `json:"error"`
	BlockNumber interface{} `json:"block_number,omitempty"`
	Processor   string      `json:"processor,omitempty"`
}

func (h *Handler) queueSingleBlock(w http.ResponseWriter, r *http.Request) {
	processorName := r.PathValue("processor")
	blockNumberStr := r.PathValue("block_number")

	blockNumber, err := strconv.ParseUint(blockNumberStr, 10, 64)
	if err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid block number format", blockNumberStr, "")

		return
	}

	if !h.isValidProcessor(processorName) {
		h.writeError(w, http.StatusNotFound, "processor not found", nil, processorName)

		return
	}

	ctx := r.Context()

	result, err := h.queueBlock(ctx, processorName, blockNumber)
	if err != nil {
		switch {
		case strings.Contains(err.Error(), "fetch"):
			h.writeError(w, http.StatusBadGateway, "failed to fetch block from execution node", blockNumber, "")
		case strings.Contains(err.Error(), "execution_block"):
			h.writeError(w, http.StatusInternalServerError, "failed to update execution_block table", blockNumber, "")
		default:
			h.writeError(w, http.StatusInternalServerError, err.Error(), blockNumber, "")
		}

		return
	}

	queue := h.processor.GetQueueName()
	response := SingleBlockResponse{
		Status:           "queued",
		BlockNumber:      blockNumber,
		Processor:        processorName,
		Queue:            queue,
		TransactionCount: result.TransactionCount,
		TasksCreated:     result.TasksCreated,
	}

	h.writeJSON(w, http.StatusOK, response)
}

func (h *Handler) queueMultipleBlocks(w http.ResponseWriter, r *http.Request) {
	processorName := r.PathValue("processor")

	if !h.isValidProcessor(processorName) {
		h.writeError(w, http.StatusNotFound, "processor not found", nil, processorName)

		return
	}

	var req BulkBlocksRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.writeError(w, http.StatusBadRequest, "invalid request body", nil, "")

		return
	}

	if len(req.Blocks) == 0 {
		h.writeError(w, http.StatusBadRequest, "no blocks provided", nil, "")

		return
	}

	if len(req.Blocks) > maxBulkBlocks {
		h.writeError(w, http.StatusRequestEntityTooLarge, fmt.Sprintf("too many blocks (limit: %d)", maxBulkBlocks), nil, "")

		return
	}

	ctx := r.Context()
	queue := h.processor.GetQueueName()
	response := BulkBlocksResponse{
		Processor: processorName,
		Queue:     queue,
		Results:   make([]BlockResult, 0, len(req.Blocks)),
	}

	response.Summary.Total = len(req.Blocks)

	for _, blockNumber := range req.Blocks {
		result, err := h.queueBlock(ctx, processorName, blockNumber)
		if err != nil {
			response.Results = append(response.Results, BlockResult{
				BlockNumber: blockNumber,
				Status:      "failed",
				Error:       err.Error(),
			})
			response.Summary.Failed++
		} else {
			response.Results = append(response.Results, BlockResult{
				BlockNumber:      blockNumber,
				Status:           "queued",
				TransactionCount: result.TransactionCount,
				TasksCreated:     result.TasksCreated,
			})
			response.Summary.Queued++
		}
	}

	switch {
	case response.Summary.Failed > 0 && response.Summary.Queued > 0:
		response.Status = "partial"
		h.writeJSON(w, http.StatusMultiStatus, response)
	case response.Summary.Failed > 0:
		response.Status = "failed"
		h.writeJSON(w, http.StatusInternalServerError, response)
	default:
		response.Status = "queued"
		h.writeJSON(w, http.StatusOK, response)
	}
}

func (h *Handler) queueBlock(ctx context.Context, processorName string, blockNumber uint64) (*processor.QueueResult, error) {
	return h.processor.QueueBlockManually(ctx, processorName, blockNumber)
}

func (h *Handler) isValidProcessor(name string) bool {
	return name == "transaction_structlog"
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)

	if err := json.NewEncoder(w).Encode(data); err != nil {
		h.log.WithError(err).Error("failed to encode response")
	}
}

func (h *Handler) writeError(w http.ResponseWriter, status int, message string, blockNumber interface{}, processorName string) {
	resp := ErrorResponse{
		Error: message,
	}

	if blockNumber != nil {
		resp.BlockNumber = blockNumber
	}

	if processorName != "" {
		resp.Processor = processorName
	}

	h.writeJSON(w, status, resp)
}
