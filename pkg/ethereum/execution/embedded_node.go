package execution

import (
	"context"
	"math/big"
	"sync"

	"github.com/sirupsen/logrus"
)

// DataSource is the interface host applications implement to provide
// execution data directly without JSON-RPC. This enables embedding
// execution-processor as a library within an execution client.
//
// All methods must be safe for concurrent calls from multiple goroutines.
// Context cancellation should be respected for all I/O operations.
//
// The interface uses abstract types (Block, Transaction, Receipt) instead of
// go-ethereum types to avoid CGO dependencies. Host applications should
// implement these interfaces with their own types.
//
// Example implementation:
//
//	type MyDataSource struct {
//	    client *MyExecutionClient
//	}
//
//	func (ds *MyDataSource) BlockNumber(ctx context.Context) (*uint64, error) {
//	    num := ds.client.CurrentBlock()
//	    return &num, nil
//	}
type DataSource interface {
	// BlockNumber returns the current block number.
	BlockNumber(ctx context.Context) (*uint64, error)

	// BlockByNumber returns the block at the given number.
	BlockByNumber(ctx context.Context, number *big.Int) (Block, error)

	// BlockReceipts returns all receipts for the block at the given number.
	BlockReceipts(ctx context.Context, number *big.Int) ([]Receipt, error)

	// TransactionReceipt returns the receipt for the transaction with the given hash.
	TransactionReceipt(ctx context.Context, hash string) (Receipt, error)

	// DebugTraceTransaction returns the execution trace for the transaction.
	DebugTraceTransaction(ctx context.Context, hash string, blockNumber *big.Int, opts TraceOptions) (*TraceTransaction, error)

	// ChainID returns the chain ID.
	ChainID() int64

	// ClientType returns the client type/version string.
	ClientType() string

	// IsSynced returns true if the data source is fully synced.
	IsSynced() bool
}

// Compile-time check that EmbeddedNode implements Node interface.
var _ Node = (*EmbeddedNode)(nil)

// EmbeddedNode implements Node by delegating to a DataSource.
// This allows host applications to provide execution data directly
// without going through JSON-RPC, eliminating serialization overhead.
//
// Lifecycle:
//  1. Create with NewEmbeddedNode(log, name, dataSource)
//  2. Register OnReady callbacks (optional)
//  3. Pool calls Start() (no-op for embedded)
//  4. Host calls MarkReady() when DataSource is ready to serve data
//  5. Callbacks execute in registration order, node becomes healthy in pool
//  6. Pool calls Stop() on shutdown (no-op for embedded)
//
// Thread-safety: All methods are safe for concurrent use.
type EmbeddedNode struct {
	log              logrus.FieldLogger
	name             string
	source           DataSource
	ready            bool
	onReadyCallbacks []func(ctx context.Context) error
	mu               sync.RWMutex
}

// NewEmbeddedNode creates a new EmbeddedNode with the given DataSource.
//
// Parameters:
//   - log: Logger for node operations
//   - name: Human-readable name for this node (used in logs and metrics)
//   - source: DataSource implementation providing execution data
//
// The returned node is not yet ready. Call MarkReady() when the DataSource
// is ready to serve data.
func NewEmbeddedNode(log logrus.FieldLogger, name string, source DataSource) *EmbeddedNode {
	return &EmbeddedNode{
		log:              log.WithFields(logrus.Fields{"type": "execution", "source": name, "mode": "embedded"}),
		name:             name,
		source:           source,
		onReadyCallbacks: make([]func(ctx context.Context) error, 0),
	}
}

// Start is a no-op for EmbeddedNode. The host controls readiness via MarkReady().
func (n *EmbeddedNode) Start(_ context.Context) error {
	n.log.Info("EmbeddedNode started - waiting for host to call MarkReady()")

	return nil
}

// Stop is a no-op for EmbeddedNode. The host manages the DataSource lifecycle.
func (n *EmbeddedNode) Stop(_ context.Context) error {
	n.log.Info("EmbeddedNode stopped")

	return nil
}

// MarkReady is called by the host application when the DataSource is ready.
// This triggers all registered OnReady callbacks.
func (n *EmbeddedNode) MarkReady(ctx context.Context) error {
	n.mu.Lock()
	n.ready = true
	callbacks := n.onReadyCallbacks
	n.mu.Unlock()

	n.log.WithField("callback_count", len(callbacks)).Info("EmbeddedNode marked as ready, executing callbacks")

	for i, cb := range callbacks {
		n.log.WithField("callback_index", i).Info("Executing OnReady callback")

		if err := cb(ctx); err != nil {
			n.log.WithError(err).Error("Failed to execute OnReady callback")

			return err
		}
	}

	return nil
}

// OnReady registers a callback to be called when the node becomes ready.
func (n *EmbeddedNode) OnReady(_ context.Context, callback func(ctx context.Context) error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.onReadyCallbacks = append(n.onReadyCallbacks, callback)
}

// IsReady returns true if the node has been marked as ready.
func (n *EmbeddedNode) IsReady() bool {
	n.mu.RLock()
	defer n.mu.RUnlock()

	return n.ready
}

// BlockNumber delegates to the DataSource.
func (n *EmbeddedNode) BlockNumber(ctx context.Context) (*uint64, error) {
	return n.source.BlockNumber(ctx)
}

// BlockByNumber delegates to the DataSource.
func (n *EmbeddedNode) BlockByNumber(ctx context.Context, number *big.Int) (Block, error) {
	return n.source.BlockByNumber(ctx, number)
}

// BlockReceipts delegates to the DataSource.
func (n *EmbeddedNode) BlockReceipts(ctx context.Context, number *big.Int) ([]Receipt, error) {
	return n.source.BlockReceipts(ctx, number)
}

// TransactionReceipt delegates to the DataSource.
func (n *EmbeddedNode) TransactionReceipt(ctx context.Context, hash string) (Receipt, error) {
	return n.source.TransactionReceipt(ctx, hash)
}

// DebugTraceTransaction delegates to the DataSource.
//
// OPTIMIZATION: In embedded mode, the tracer extracts CallToAddress directly
// for CALL-family opcodes instead of capturing the full stack. We explicitly
// set DisableStack: true to signal this intent, even though the tracer ignores
// this setting (it always uses the optimized path).
func (n *EmbeddedNode) DebugTraceTransaction(
	ctx context.Context,
	hash string,
	blockNumber *big.Int,
	opts TraceOptions,
) (*TraceTransaction, error) {
	// Override DisableStack for embedded mode optimization.
	// The tracer extracts CallToAddress directly, so full stack capture is unnecessary.
	opts.DisableStack = true

	return n.source.DebugTraceTransaction(ctx, hash, blockNumber, opts)
}

// ChainID delegates to the DataSource.
func (n *EmbeddedNode) ChainID() int64 {
	return n.source.ChainID()
}

// ClientType delegates to the DataSource.
func (n *EmbeddedNode) ClientType() string {
	return n.source.ClientType()
}

// IsSynced delegates to the DataSource.
func (n *EmbeddedNode) IsSynced() bool {
	return n.source.IsSynced()
}

// Name returns the configured name for this node.
func (n *EmbeddedNode) Name() string {
	return n.name
}
