//go:build !embedded

package geth

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/rpc"

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
		Structlogs:  []execution.StructLog{},
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
