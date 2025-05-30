package execution

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"time"

	"github.com/0xsequence/ethkit/ethrpc"
	"github.com/0xsequence/ethkit/go-ethereum/core/types"

	"github.com/ethpandaops/execution-processor/pkg/common"
)

const (
	STATUS_ERROR   = "error"
	STATUS_SUCCESS = "success"
)

func (n *Node) BlockNumber(ctx context.Context) (*uint64, error) {
	var blockNumber uint64

	start := time.Now()
	_, err := n.rpc.Do(ctx, ethrpc.BlockNumber().Into(&blockNumber))
	duration := time.Since(start)

	// Record RPC metrics
	status := STATUS_SUCCESS
	if err != nil {
		status = STATUS_ERROR
	}

	network := n.Metadata().ChainID()

	common.RPCCallDuration.WithLabelValues(fmt.Sprintf("%d", network), n.config.Name, "eth_blockNumber", status).Observe(duration.Seconds())
	common.RPCCallsTotal.WithLabelValues(fmt.Sprintf("%d", network), n.config.Name, "eth_blockNumber", status).Inc()

	if err != nil {
		return nil, err
	}

	return &blockNumber, nil
}

func (n *Node) BlockByNumber(ctx context.Context, blockNumber *big.Int) (*types.Block, error) {
	var block *types.Block

	start := time.Now()
	_, err := n.rpc.Do(ctx, ethrpc.BlockByNumber(blockNumber).Into(&block))
	duration := time.Since(start)

	// Record RPC metrics
	status := STATUS_SUCCESS
	if err != nil {
		status = STATUS_ERROR
	}

	network := n.Metadata().ChainID()

	common.RPCCallDuration.WithLabelValues(fmt.Sprintf("%d", network), n.config.Name, "eth_getBlockByNumber", status).Observe(duration.Seconds())
	common.RPCCallsTotal.WithLabelValues(fmt.Sprintf("%d", network), n.config.Name, "eth_getBlockByNumber", status).Inc()

	if err != nil {
		return nil, err
	}

	return block, nil
}

// getDefaultTraceParams returns standard VM trace parameters
func getTraceParams(hash string) []any {
	return []any{
		hash,
		map[string]any{
			"disableStorage":   true, // Don't include storage changes
			"disableStack":     true, // Don't include stack
			"disableMemory":    true, // Don't include memory
			"enableReturnData": true, // Do include return data
		},
	}
}

// traceTransactionErigon handles tracing for Erigon clients
func (n *Node) traceTransactionErigon(ctx context.Context, hash string) (*TraceTransaction, error) {
	var rsp ErigonResult

	call := ethrpc.NewCallBuilder[ErigonResult]("debug_traceTransaction", nil, getTraceParams(hash)...)

	start := time.Now()
	_, err := n.rpc.Do(ctx, call.Into(&rsp))
	duration := time.Since(start)

	// Record RPC metrics
	status := STATUS_SUCCESS
	if err != nil {
		status = STATUS_ERROR
	}

	network := n.Metadata().ChainID()

	common.RPCCallDuration.WithLabelValues(fmt.Sprintf("%d", network), n.config.Name, "debug_traceTransaction", status).Observe(duration.Seconds())
	common.RPCCallsTotal.WithLabelValues(fmt.Sprintf("%d", network), n.config.Name, "debug_traceTransaction", status).Inc()

	if err != nil {
		return nil, err
	}

	returnValue := rsp.ReturnValue
	if returnValue != nil && *returnValue == "" {
		returnValue = nil
	}

	result := &TraceTransaction{
		Gas:         rsp.Gas,
		Failed:      rsp.Failed,
		ReturnValue: returnValue,
		Structlogs:  []StructLog{},
	}

	// Empty array on transfer
	for _, log := range rsp.StructLogs {
		var returnData *string

		if log.ReturnData != nil {
			returnData = new(string)
			*returnData = hex.EncodeToString(log.ReturnData)
		}

		result.Structlogs = append(result.Structlogs, StructLog{
			PC:         log.PC,
			Op:         log.Op,
			Gas:        log.Gas,
			GasCost:    log.GasCost,
			Depth:      log.Depth,
			ReturnData: returnData,
			Refund:     log.Refund,
			Error:      log.Error,
		})
	}

	return result, nil
}

// DebugTraceTransaction traces a transaction execution using the client's debug API
func (n *Node) DebugTraceTransaction(ctx context.Context, hash string, blockNumber *big.Int) (*TraceTransaction, error) {
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
		return n.traceTransactionErigon(ctx, hash)
	default:
		// Default to Erigon format if client is unknown
		return n.traceTransactionErigon(ctx, hash)
	}
}
