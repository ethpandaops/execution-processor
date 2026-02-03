package execution_test

import (
	"context"
	"errors"
	"math/big"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

// MockBlock implements execution.Block for testing.
type MockBlock struct {
	number     *big.Int
	hash       execution.Hash
	parentHash execution.Hash
	baseFee    *big.Int
	txs        []execution.Transaction
}

func (b *MockBlock) Number() *big.Int                      { return b.number }
func (b *MockBlock) Hash() execution.Hash                  { return b.hash }
func (b *MockBlock) ParentHash() execution.Hash            { return b.parentHash }
func (b *MockBlock) BaseFee() *big.Int                     { return b.baseFee }
func (b *MockBlock) Transactions() []execution.Transaction { return b.txs }

// NewMockBlock creates a mock block with the given number.
func NewMockBlock(number *big.Int) *MockBlock {
	return &MockBlock{
		number:     number,
		hash:       execution.Hash{},
		parentHash: execution.Hash{},
		baseFee:    big.NewInt(1000000000),
		txs:        []execution.Transaction{},
	}
}

// MockReceipt implements execution.Receipt for testing.
type MockReceipt struct {
	status  uint64
	txHash  execution.Hash
	gasUsed uint64
}

func (r *MockReceipt) Status() uint64         { return r.status }
func (r *MockReceipt) TxHash() execution.Hash { return r.txHash }
func (r *MockReceipt) GasUsed() uint64        { return r.gasUsed }

// NewMockReceipt creates a mock receipt with the given status.
func NewMockReceipt(status uint64, gasUsed uint64) *MockReceipt {
	return &MockReceipt{
		status:  status,
		txHash:  execution.Hash{},
		gasUsed: gasUsed,
	}
}

// MockDataSource implements execution.DataSource for testing.
type MockDataSource struct {
	mock.Mock
}

func (m *MockDataSource) BlockNumber(ctx context.Context) (*uint64, error) {
	args := m.Called(ctx)

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	val, ok := args.Get(0).(*uint64)
	if !ok {
		return nil, args.Error(1)
	}

	return val, args.Error(1)
}

func (m *MockDataSource) BlockByNumber(ctx context.Context, number *big.Int) (execution.Block, error) {
	args := m.Called(ctx, number)

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	val, ok := args.Get(0).(execution.Block)
	if !ok {
		return nil, args.Error(1)
	}

	return val, args.Error(1)
}

func (m *MockDataSource) BlockReceipts(ctx context.Context, number *big.Int) ([]execution.Receipt, error) {
	args := m.Called(ctx, number)

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	val, ok := args.Get(0).([]execution.Receipt)
	if !ok {
		return nil, args.Error(1)
	}

	return val, args.Error(1)
}

func (m *MockDataSource) TransactionReceipt(ctx context.Context, hash string) (execution.Receipt, error) {
	args := m.Called(ctx, hash)

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	val, ok := args.Get(0).(execution.Receipt)
	if !ok {
		return nil, args.Error(1)
	}

	return val, args.Error(1)
}

func (m *MockDataSource) DebugTraceTransaction(
	ctx context.Context,
	hash string,
	blockNumber *big.Int,
	opts execution.TraceOptions,
) (*execution.TraceTransaction, error) {
	args := m.Called(ctx, hash, blockNumber, opts)

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	val, ok := args.Get(0).(*execution.TraceTransaction)
	if !ok {
		return nil, args.Error(1)
	}

	return val, args.Error(1)
}

func (m *MockDataSource) ChainID() int64 {
	args := m.Called()

	val, ok := args.Get(0).(int64)
	if !ok {
		return 0
	}

	return val
}

func (m *MockDataSource) ClientType() string {
	args := m.Called()

	return args.String(0)
}

func (m *MockDataSource) IsSynced() bool {
	args := m.Called()

	return args.Bool(0)
}

func (m *MockDataSource) BlocksByNumbers(ctx context.Context, numbers []*big.Int) ([]execution.Block, error) {
	args := m.Called(ctx, numbers)

	if args.Get(0) == nil {
		return nil, args.Error(1)
	}

	val, ok := args.Get(0).([]execution.Block)
	if !ok {
		return nil, args.Error(1)
	}

	return val, args.Error(1)
}

func TestEmbeddedNode_Creation(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ds := new(MockDataSource)
	node := execution.NewEmbeddedNode(log, "test-node", ds)

	require.NotNil(t, node)
	assert.Equal(t, "test-node", node.Name())
	assert.False(t, node.IsReady())
}

func TestEmbeddedNode_Start_NoOp(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ds := new(MockDataSource)
	node := execution.NewEmbeddedNode(log, "test-node", ds)

	ctx := context.Background()
	err := node.Start(ctx)

	assert.NoError(t, err)
	// Start should not mark the node as ready
	assert.False(t, node.IsReady())
}

func TestEmbeddedNode_Stop_NoOp(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ds := new(MockDataSource)
	node := execution.NewEmbeddedNode(log, "test-node", ds)

	ctx := context.Background()

	// Start and mark ready first
	err := node.Start(ctx)
	require.NoError(t, err)

	err = node.MarkReady(ctx)
	require.NoError(t, err)

	// Stop should complete without error
	err = node.Stop(ctx)
	assert.NoError(t, err)
}

func TestEmbeddedNode_MarkReady_ExecutesCallbacks(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ds := new(MockDataSource)
	node := execution.NewEmbeddedNode(log, "test-node", ds)

	ctx := context.Background()

	// Track callback execution order
	var order []int

	var mu sync.Mutex

	node.OnReady(ctx, func(_ context.Context) error {
		mu.Lock()
		defer mu.Unlock()

		order = append(order, 1)

		return nil
	})

	node.OnReady(ctx, func(_ context.Context) error {
		mu.Lock()
		defer mu.Unlock()

		order = append(order, 2)

		return nil
	})

	node.OnReady(ctx, func(_ context.Context) error {
		mu.Lock()
		defer mu.Unlock()

		order = append(order, 3)

		return nil
	})

	assert.False(t, node.IsReady())

	err := node.MarkReady(ctx)
	require.NoError(t, err)

	assert.True(t, node.IsReady())
	assert.Equal(t, []int{1, 2, 3}, order)
}

func TestEmbeddedNode_MarkReady_CallbackError(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ds := new(MockDataSource)
	node := execution.NewEmbeddedNode(log, "test-node", ds)

	ctx := context.Background()

	expectedErr := errors.New("callback failed")

	var callbacksCalled int

	node.OnReady(ctx, func(_ context.Context) error {
		callbacksCalled++

		return nil
	})

	node.OnReady(ctx, func(_ context.Context) error {
		callbacksCalled++

		return expectedErr
	})

	node.OnReady(ctx, func(_ context.Context) error {
		callbacksCalled++

		return nil
	})

	err := node.MarkReady(ctx)
	assert.ErrorIs(t, err, expectedErr)
	// Only first two callbacks should have been called (second one failed)
	assert.Equal(t, 2, callbacksCalled)
}

func TestEmbeddedNode_OnReady_MultipleCallbacks(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ds := new(MockDataSource)
	node := execution.NewEmbeddedNode(log, "test-node", ds)

	ctx := context.Background()

	var count atomic.Int32

	const numCallbacks = 10
	for i := 0; i < numCallbacks; i++ {
		node.OnReady(ctx, func(_ context.Context) error {
			count.Add(1)

			return nil
		})
	}

	err := node.MarkReady(ctx)
	require.NoError(t, err)

	assert.Equal(t, int32(numCallbacks), count.Load())
}

func TestEmbeddedNode_IsReady_States(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ds := new(MockDataSource)
	node := execution.NewEmbeddedNode(log, "test-node", ds)

	ctx := context.Background()

	// Initially not ready
	assert.False(t, node.IsReady())

	// Still not ready after Start
	err := node.Start(ctx)
	require.NoError(t, err)

	assert.False(t, node.IsReady())

	// Ready after MarkReady
	err = node.MarkReady(ctx)
	require.NoError(t, err)

	assert.True(t, node.IsReady())

	// Still ready after Stop
	err = node.Stop(ctx)
	require.NoError(t, err)

	assert.True(t, node.IsReady())
}

func TestEmbeddedNode_DelegatesToDataSource_BlockNumber(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ds := new(MockDataSource)
	node := execution.NewEmbeddedNode(log, "test-node", ds)

	ctx := context.Background()
	expectedBlock := uint64(12345)

	ds.On("BlockNumber", ctx).Return(&expectedBlock, nil)

	result, err := node.BlockNumber(ctx)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Equal(t, expectedBlock, *result)

	ds.AssertExpectations(t)
}

func TestEmbeddedNode_DelegatesToDataSource_BlockByNumber(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ds := new(MockDataSource)
	node := execution.NewEmbeddedNode(log, "test-node", ds)

	ctx := context.Background()
	blockNum := big.NewInt(12345)
	expectedBlock := NewMockBlock(blockNum)

	ds.On("BlockByNumber", ctx, blockNum).Return(expectedBlock, nil)

	result, err := node.BlockByNumber(ctx, blockNum)
	require.NoError(t, err)
	assert.Equal(t, expectedBlock.Number(), result.Number())

	ds.AssertExpectations(t)
}

func TestEmbeddedNode_DelegatesToDataSource_BlockReceipts(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ds := new(MockDataSource)
	node := execution.NewEmbeddedNode(log, "test-node", ds)

	ctx := context.Background()
	blockNum := big.NewInt(12345)
	expectedReceipts := []execution.Receipt{
		NewMockReceipt(1, 21000),
		NewMockReceipt(0, 50000),
	}

	ds.On("BlockReceipts", ctx, blockNum).Return(expectedReceipts, nil)

	result, err := node.BlockReceipts(ctx, blockNum)
	require.NoError(t, err)
	assert.Len(t, result, 2)

	ds.AssertExpectations(t)
}

func TestEmbeddedNode_DelegatesToDataSource_TransactionReceipt(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ds := new(MockDataSource)
	node := execution.NewEmbeddedNode(log, "test-node", ds)

	ctx := context.Background()
	txHash := "0x1234567890abcdef"
	expectedReceipt := NewMockReceipt(1, 21000)

	ds.On("TransactionReceipt", ctx, txHash).Return(expectedReceipt, nil)

	result, err := node.TransactionReceipt(ctx, txHash)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), result.Status())
	assert.Equal(t, uint64(21000), result.GasUsed())

	ds.AssertExpectations(t)
}

func TestEmbeddedNode_DelegatesToDataSource_DebugTraceTransaction(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ds := new(MockDataSource)
	node := execution.NewEmbeddedNode(log, "test-node", ds)

	ctx := context.Background()
	txHash := "0x1234567890abcdef"
	blockNum := big.NewInt(12345)
	opts := execution.DefaultTraceOptions()
	expectedTrace := &execution.TraceTransaction{
		Gas:    21000,
		Failed: false,
	}

	ds.On("DebugTraceTransaction", ctx, txHash, blockNum, opts).Return(expectedTrace, nil)

	result, err := node.DebugTraceTransaction(ctx, txHash, blockNum, opts)
	require.NoError(t, err)
	assert.Equal(t, expectedTrace, result)

	ds.AssertExpectations(t)
}

func TestEmbeddedNode_DelegatesToDataSource_ClientType(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ds := new(MockDataSource)
	node := execution.NewEmbeddedNode(log, "test-node", ds)

	ds.On("ClientType").Return("geth/1.10.0")

	result := node.ClientType()
	assert.Equal(t, "geth/1.10.0", result)

	ds.AssertExpectations(t)
}

func TestEmbeddedNode_DelegatesToDataSource_IsSynced(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ds := new(MockDataSource)
	node := execution.NewEmbeddedNode(log, "test-node", ds)

	ds.On("IsSynced").Return(true)

	result := node.IsSynced()
	assert.True(t, result)

	ds.AssertExpectations(t)
}

func TestEmbeddedNode_Name(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	testCases := []struct {
		name         string
		expectedName string
	}{
		{name: "simple-name", expectedName: "simple-name"},
		{name: "with-numbers-123", expectedName: "with-numbers-123"},
		{name: "embedded-erigon", expectedName: "embedded-erigon"},
		{name: "", expectedName: ""},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ds := new(MockDataSource)
			node := execution.NewEmbeddedNode(log, tc.name, ds)

			assert.Equal(t, tc.expectedName, node.Name())
		})
	}
}

func TestEmbeddedNode_ConcurrentMarkReady(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ds := new(MockDataSource)
	node := execution.NewEmbeddedNode(log, "test-node", ds)

	ctx := context.Background()

	var callbackCount atomic.Int32

	node.OnReady(ctx, func(_ context.Context) error {
		callbackCount.Add(1)

		return nil
	})

	// Start multiple goroutines calling MarkReady concurrently
	const numGoroutines = 10

	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			// Ignore errors - only the first MarkReady should execute callbacks
			_ = node.MarkReady(ctx)
		}()
	}

	wg.Wait()

	// Node should be ready
	assert.True(t, node.IsReady())

	// Callback should have been called at least once
	// (implementation may allow multiple calls, but at least one should succeed)
	assert.GreaterOrEqual(t, callbackCount.Load(), int32(1))
}

func TestEmbeddedNode_ConcurrentOnReady(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ds := new(MockDataSource)
	node := execution.NewEmbeddedNode(log, "test-node", ds)

	ctx := context.Background()

	var callbackCount atomic.Int32

	// Register callbacks concurrently
	const numGoroutines = 10

	var wg sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			node.OnReady(ctx, func(_ context.Context) error {
				callbackCount.Add(1)

				return nil
			})
		}()
	}

	wg.Wait()

	// Now mark ready and verify all callbacks execute
	err := node.MarkReady(ctx)
	require.NoError(t, err)

	assert.Equal(t, int32(numGoroutines), callbackCount.Load())
}

func TestEmbeddedNode_InterfaceCompliance(t *testing.T) {
	// Compile-time check that EmbeddedNode implements Node interface
	var _ execution.Node = (*execution.EmbeddedNode)(nil)

	// Create an actual instance and verify it can be used as Node
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ds := new(MockDataSource)

	var node execution.Node = execution.NewEmbeddedNode(log, "test-node", ds)

	require.NotNil(t, node)
	assert.Equal(t, "test-node", node.Name())
}

func TestEmbeddedNode_DataSourceError(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ds := new(MockDataSource)
	node := execution.NewEmbeddedNode(log, "test-node", ds)

	ctx := context.Background()
	expectedErr := errors.New("data source error")

	ds.On("BlockNumber", ctx).Return(nil, expectedErr)

	result, err := node.BlockNumber(ctx)
	assert.ErrorIs(t, err, expectedErr)
	assert.Nil(t, result)

	ds.AssertExpectations(t)
}

func TestEmbeddedNode_ContextCancellation(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ds := new(MockDataSource)
	node := execution.NewEmbeddedNode(log, "test-node", ds)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	// DataSource should receive cancelled context
	ds.On("BlockNumber", ctx).Return(nil, ctx.Err())

	result, err := node.BlockNumber(ctx)
	assert.Error(t, err)
	assert.Nil(t, result)
}

func TestEmbeddedNode_CallbackWithContext(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ds := new(MockDataSource)
	node := execution.NewEmbeddedNode(log, "test-node", ds)

	ctx := context.Background()

	var receivedCtx context.Context

	node.OnReady(ctx, func(cbCtx context.Context) error {
		receivedCtx = cbCtx

		return nil
	})

	err := node.MarkReady(ctx)
	require.NoError(t, err)

	// The callback should receive the context passed to MarkReady
	assert.Equal(t, ctx, receivedCtx)
}

func TestEmbeddedNode_CallbackWithTimeout(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ds := new(MockDataSource)
	node := execution.NewEmbeddedNode(log, "test-node", ds)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	callbackExecuted := make(chan struct{})

	node.OnReady(ctx, func(_ context.Context) error {
		close(callbackExecuted)

		return nil
	})

	err := node.MarkReady(ctx)
	require.NoError(t, err)

	select {
	case <-callbackExecuted:
		// Success
	case <-time.After(1 * time.Second):
		t.Fatal("callback did not execute")
	}
}
