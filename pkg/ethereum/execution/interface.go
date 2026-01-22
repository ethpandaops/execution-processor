package execution

import (
	"context"
	"math/big"

	"github.com/ethereum/go-ethereum/core/types"
)

// Node defines the interface for execution data providers.
//
// Implementations include:
//   - RPCNode: connects to execution clients via JSON-RPC over HTTP
//   - EmbeddedNode: receives data directly from host application via DataSource
//
// All methods must be safe for concurrent use by multiple goroutines.
//
// Lifecycle:
//  1. Create node with appropriate constructor (NewRPCNode or NewEmbeddedNode)
//  2. Register OnReady callbacks before calling Start
//  3. Call Start to begin initialization
//  4. Node signals readiness by executing OnReady callbacks
//  5. Call Stop for graceful shutdown
type Node interface {
	// Start initializes the node and begins any background operations.
	// For RPCNode, this establishes the RPC connection and starts health monitoring.
	// For EmbeddedNode, this is a no-op as the host controls the DataSource lifecycle.
	Start(ctx context.Context) error

	// Stop gracefully shuts down the node and releases resources.
	// Should be called when the node is no longer needed.
	Stop(ctx context.Context) error

	// OnReady registers a callback to be invoked when the node becomes ready.
	// For RPCNode, callbacks execute when the RPC connection is healthy.
	// For EmbeddedNode, callbacks execute when MarkReady is called by the host.
	// Multiple callbacks can be registered and will execute in registration order.
	OnReady(ctx context.Context, callback func(ctx context.Context) error)

	// BlockNumber returns the current block number from the execution client.
	BlockNumber(ctx context.Context) (*uint64, error)

	// BlockByNumber returns the block at the given number.
	BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error)

	// BlockReceipts returns all receipts for the block at the given number.
	BlockReceipts(ctx context.Context, number *big.Int) ([]*types.Receipt, error)

	// TransactionReceipt returns the receipt for the transaction with the given hash.
	TransactionReceipt(ctx context.Context, hash string) (*types.Receipt, error)

	// DebugTraceTransaction returns the execution trace for the transaction.
	DebugTraceTransaction(ctx context.Context, hash string, blockNumber *big.Int, opts TraceOptions) (*TraceTransaction, error)

	// ChainID returns the chain ID reported by the execution client.
	ChainID() int32

	// ClientType returns the client type/version string (e.g., "geth/1.10.0").
	ClientType() string

	// IsSynced returns true if the execution client is fully synced.
	IsSynced() bool

	// Name returns the configured name for this node.
	Name() string
}
