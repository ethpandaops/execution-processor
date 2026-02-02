//go:build !embedded

package geth

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/sirupsen/logrus"

	pcommon "github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

const (
	statusError   = "error"
	statusSuccess = "success"
)

func (n *RPCNode) blockNumber(ctx context.Context) (*uint64, error) {
	start := time.Now()

	blockNumber, err := n.client.BlockNumber(ctx)

	duration := time.Since(start)

	// Record RPC metrics
	status := statusSuccess
	if err != nil {
		status = statusError
	}

	network := n.Metadata().ChainID()

	pcommon.RPCCallDuration.WithLabelValues(fmt.Sprintf("%d", network), n.config.Name, "eth_blockNumber", status).Observe(duration.Seconds())
	pcommon.RPCCallsTotal.WithLabelValues(fmt.Sprintf("%d", network), n.config.Name, "eth_blockNumber", status).Inc()

	if err != nil {
		return nil, err
	}

	return &blockNumber, nil
}

func (n *RPCNode) blockByNumber(ctx context.Context, blockNumber *big.Int) (execution.Block, error) {
	start := time.Now()

	block, err := n.client.BlockByNumber(ctx, blockNumber)

	duration := time.Since(start)

	// Record RPC metrics
	status := statusSuccess
	if err != nil {
		status = statusError
	}

	network := n.Metadata().ChainID()

	pcommon.RPCCallDuration.WithLabelValues(fmt.Sprintf("%d", network), n.config.Name, "eth_getBlockByNumber", status).Observe(duration.Seconds())
	pcommon.RPCCallsTotal.WithLabelValues(fmt.Sprintf("%d", network), n.config.Name, "eth_getBlockByNumber", status).Inc()

	if err != nil {
		return nil, err
	}

	return NewBlockAdapter(block), nil
}

// maxBatchSize is the maximum number of blocks to request in a single batch RPC call.
// Most RPC nodes have a default limit of 100 (e.g., Erigon's --rpc.batch.limit).
const maxBatchSize = 100

// blocksByNumbers fetches multiple blocks using batch RPC calls.
// Returns blocks up to the first not-found (contiguous only).
// Large requests are automatically chunked to respect RPC batch limits.
func (n *RPCNode) blocksByNumbers(ctx context.Context, numbers []*big.Int) ([]execution.Block, error) {
	if len(numbers) == 0 {
		return []execution.Block{}, nil
	}

	// If request exceeds batch limit, chunk it
	if len(numbers) > maxBatchSize {
		return n.blocksByNumbersChunked(ctx, numbers)
	}

	return n.blocksByNumbersBatch(ctx, numbers)
}

// blocksByNumbersChunked fetches blocks in chunks to respect RPC batch limits.
func (n *RPCNode) blocksByNumbersChunked(ctx context.Context, numbers []*big.Int) ([]execution.Block, error) {
	allBlocks := make([]execution.Block, 0, len(numbers))

	for i := 0; i < len(numbers); i += maxBatchSize {
		end := i + maxBatchSize
		if end > len(numbers) {
			end = len(numbers)
		}

		chunk := numbers[i:end]

		blocks, err := n.blocksByNumbersBatch(ctx, chunk)
		if err != nil {
			return allBlocks, err
		}

		allBlocks = append(allBlocks, blocks...)

		// If we got fewer blocks than requested, a block was not found - stop for contiguity
		if len(blocks) < len(chunk) {
			break
		}
	}

	return allBlocks, nil
}

// blocksByNumbersBatch fetches a single batch of blocks (must be <= maxBatchSize).
func (n *RPCNode) blocksByNumbersBatch(ctx context.Context, numbers []*big.Int) ([]execution.Block, error) {
	start := time.Now()
	network := n.Metadata().ChainID()

	// Prepare batch calls using json.RawMessage to handle null responses
	batch := make([]rpc.BatchElem, len(numbers))
	results := make([]*json.RawMessage, len(numbers))

	for i, num := range numbers {
		results[i] = new(json.RawMessage)
		batch[i] = rpc.BatchElem{
			Method: "eth_getBlockByNumber",
			Args:   []interface{}{toBlockNumArg(num), true}, // true = include transactions
			Result: results[i],
		}
	}

	// Execute batch call
	err := n.rpcClient.BatchCallContext(ctx, batch)

	duration := time.Since(start)

	// Record batch RPC metrics
	status := statusSuccess
	if err != nil {
		status = statusError
	}

	pcommon.RPCCallDuration.WithLabelValues(
		fmt.Sprintf("%d", network),
		n.config.Name,
		"eth_getBlockByNumber_batch",
		status,
	).Observe(duration.Seconds())

	pcommon.RPCCallsTotal.WithLabelValues(
		fmt.Sprintf("%d", network),
		n.config.Name,
		"eth_getBlockByNumber_batch",
		status,
	).Inc()

	if err != nil {
		return nil, fmt.Errorf("batch call failed: %w", err)
	}

	// Process results, stopping at first not-found (contiguity requirement)
	blocks := make([]execution.Block, 0, len(numbers))

	for i, elem := range batch {
		// Check for individual call error - stop at first error for contiguity
		// We intentionally don't return this error as we want partial results
		if elem.Error != nil {
			n.log.WithError(elem.Error).WithField("block_index", i).Debug("Batch element error, stopping")

			break
		}

		// Check for nil/not-found block (null JSON response)
		if results[i] == nil || len(*results[i]) == 0 || string(*results[i]) == "null" {
			// Block not found - stop here for contiguity
			n.log.WithFields(logrus.Fields{
				"block_index":  i,
				"block_number": numbers[i].String(),
			}).Debug("Block not found in batch, stopping")

			break
		}

		// Parse the block from JSON
		block, parseErr := parseBlockFromJSON(*results[i])
		if parseErr != nil {
			// Parse error - stop here for contiguity
			n.log.WithError(parseErr).WithFields(logrus.Fields{
				"block_index":  i,
				"block_number": numbers[i].String(),
			}).Warn("Failed to parse block from JSON, stopping batch")

			break
		}

		blocks = append(blocks, NewBlockAdapter(block))
	}

	return blocks, nil
}

// toBlockNumArg converts a block number to the RPC argument format.
func toBlockNumArg(number *big.Int) string {
	if number == nil {
		return "latest"
	}

	return fmt.Sprintf("0x%x", number)
}

// parseBlockFromJSON parses a types.Block from JSON-RPC response.
func parseBlockFromJSON(raw json.RawMessage) (*types.Block, error) {
	// Use go-ethereum's internal header structure for unmarshaling
	var head *types.Header
	if err := json.Unmarshal(raw, &head); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block header: %w", err)
	}

	// Parse transactions separately
	var body struct {
		Transactions []*types.Transaction `json:"transactions"`
	}

	if err := json.Unmarshal(raw, &body); err != nil {
		return nil, fmt.Errorf("failed to unmarshal block body: %w", err)
	}

	return types.NewBlockWithHeader(head).WithBody(types.Body{Transactions: body.Transactions}), nil
}

// getTraceParams returns VM trace parameters with configurable options.
func getTraceParams(hash string, options execution.TraceOptions) []any {
	return []any{
		hash,
		map[string]any{
			"disableStorage":   options.DisableStorage,
			"disableStack":     options.DisableStack,
			"disableMemory":    options.DisableMemory,
			"enableReturnData": options.EnableReturnData,
		},
	}
}

// traceTransactionErigon handles tracing for Erigon clients.
func (n *RPCNode) traceTransactionErigon(ctx context.Context, hash string, options execution.TraceOptions) (*execution.TraceTransaction, error) {
	var rsp erigonResult

	start := time.Now()

	err := n.rpcClient.CallContext(ctx, &rsp, "debug_traceTransaction", getTraceParams(hash, options)...)

	duration := time.Since(start)

	// Record RPC metrics
	status := statusSuccess
	if err != nil {
		status = statusError
	}

	network := n.Metadata().ChainID()

	pcommon.RPCCallDuration.WithLabelValues(fmt.Sprintf("%d", network), n.config.Name, "debug_traceTransaction", status).Observe(duration.Seconds())
	pcommon.RPCCallsTotal.WithLabelValues(fmt.Sprintf("%d", network), n.config.Name, "debug_traceTransaction", status).Inc()

	if err != nil {
		return nil, err
	}

	returnValue := rsp.ReturnValue
	if returnValue != nil && (*returnValue == "" || *returnValue == "0x") {
		returnValue = nil
	}

	result := &execution.TraceTransaction{
		Gas:         rsp.Gas,
		Failed:      rsp.Failed,
		ReturnValue: returnValue,
		Structlogs:  make([]execution.StructLog, 0, len(rsp.StructLogs)),
	}

	// Empty array on transfer
	for _, log := range rsp.StructLogs {
		var returnData *string

		if log.ReturnData != nil {
			returnData = new(string)
			*returnData = hex.EncodeToString(log.ReturnData)
		}

		result.Structlogs = append(result.Structlogs, execution.StructLog{
			PC:         log.PC,
			Op:         log.Op,
			Gas:        log.Gas,
			GasCost:    log.GasCost,
			Depth:      log.Depth,
			ReturnData: returnData,
			Refund:     log.Refund,
			Error:      log.Error,
			Stack:      log.Stack,
		})
	}

	// Sanitize gasCost values to fix Erigon's unsigned integer underflow bug.
	execution.SanitizeStructLogs(result.Structlogs)

	return result, nil
}

// blockReceipts fetches all receipts for a block by number (much faster than per-tx).
func (n *RPCNode) blockReceipts(ctx context.Context, blockNumber *big.Int) ([]execution.Receipt, error) {
	start := time.Now()

	blockNrOrHash := rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNumber.Int64()))

	receipts, err := n.client.BlockReceipts(ctx, blockNrOrHash)

	duration := time.Since(start)

	// Record RPC metrics
	status := statusSuccess
	if err != nil {
		status = statusError
	}

	network := n.Metadata().ChainID()

	pcommon.RPCCallDuration.WithLabelValues(
		fmt.Sprintf("%d", network),
		n.config.Name,
		"eth_getBlockReceipts",
		status,
	).Observe(duration.Seconds())

	pcommon.RPCCallsTotal.WithLabelValues(
		fmt.Sprintf("%d", network),
		n.config.Name,
		"eth_getBlockReceipts",
		status,
	).Inc()

	if err != nil {
		return nil, err
	}

	return AdaptReceipts(receipts), nil
}

// transactionReceipt fetches the receipt for a transaction by hash.
func (n *RPCNode) transactionReceipt(ctx context.Context, hash string) (execution.Receipt, error) {
	start := time.Now()

	txHash := common.HexToHash(hash)

	receipt, err := n.client.TransactionReceipt(ctx, txHash)

	duration := time.Since(start)

	// Record RPC metrics
	status := statusSuccess
	if err != nil {
		status = statusError
	}

	network := n.Metadata().ChainID()

	pcommon.RPCCallDuration.WithLabelValues(
		fmt.Sprintf("%d", network),
		n.config.Name,
		"eth_getTransactionReceipt",
		status,
	).Observe(duration.Seconds())

	pcommon.RPCCallsTotal.WithLabelValues(
		fmt.Sprintf("%d", network),
		n.config.Name,
		"eth_getTransactionReceipt",
		status,
	).Inc()

	if err != nil {
		return nil, err
	}

	return NewReceiptAdapter(receipt), nil
}

// debugTraceTransaction traces a transaction execution using the client's debug API.
func (n *RPCNode) debugTraceTransaction(
	ctx context.Context,
	hash string,
	_ *big.Int,
	options execution.TraceOptions,
) (*execution.TraceTransaction, error) {
	// Add a timeout if the context doesn't already have one
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc

		ctx, cancel = context.WithTimeout(ctx, 60*time.Second)

		defer cancel()
	}

	client := n.Metadata().Client(ctx)

	switch client {
	case "geth":
		return nil, fmt.Errorf("geth is not supported")
	case "nethermind":
		return nil, fmt.Errorf("nethermind is not supported")
	case "besu":
		return nil, fmt.Errorf("besu is not supported")
	case "reth":
		return nil, fmt.Errorf("reth is not supported")
	case "erigon":
		return n.traceTransactionErigon(ctx, hash, options)
	default:
		// Default to Erigon format if client is unknown
		return n.traceTransactionErigon(ctx, hash, options)
	}
}

// erigonResult represents the result from an Erigon debug_traceTransaction call.
type erigonResult struct {
	Gas         uint64            `json:"gas"`
	Failed      bool              `json:"failed"`
	ReturnValue *string           `json:"returnValue"`
	StructLogs  []erigonStructLog `json:"structLogs"`
}

// erigonStructLog represents a single structlog entry from Erigon.
type erigonStructLog struct {
	PC         uint32    `json:"pc"`
	Op         string    `json:"op"`
	Gas        uint64    `json:"gas"`
	GasCost    uint64    `json:"gasCost"`
	Depth      uint64    `json:"depth"`
	ReturnData []byte    `json:"returnData"`
	Refund     *uint64   `json:"refund"`
	Error      *string   `json:"error"`
	Stack      *[]string `json:"stack"`
}
