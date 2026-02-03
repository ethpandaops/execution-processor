package ethereum_test

import (
	"context"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/ethereum"
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// MockBlock implements execution.Block for testing.
type MockBlock struct {
	number *big.Int
}

func (b *MockBlock) Number() *big.Int                      { return b.number }
func (b *MockBlock) Hash() execution.Hash                  { return execution.Hash{} }
func (b *MockBlock) ParentHash() execution.Hash            { return execution.Hash{} }
func (b *MockBlock) BaseFee() *big.Int                     { return nil }
func (b *MockBlock) Transactions() []execution.Transaction { return nil }

// MockReceipt implements execution.Receipt for testing.
type MockReceipt struct{}

func (r *MockReceipt) Status() uint64         { return 1 }
func (r *MockReceipt) TxHash() execution.Hash { return execution.Hash{} }
func (r *MockReceipt) GasUsed() uint64        { return 21000 }

// MockNode implements execution.Node for testing.
type MockNode struct {
	name             string
	started          bool
	stopped          bool
	onReadyCallbacks []func(ctx context.Context) error
	mu               sync.Mutex
}

func NewMockNode(name string) *MockNode {
	return &MockNode{
		name:             name,
		onReadyCallbacks: make([]func(ctx context.Context) error, 0),
	}
}

func (m *MockNode) Start(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.started = true

	return nil
}

func (m *MockNode) Stop(_ context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.stopped = true

	return nil
}

func (m *MockNode) OnReady(_ context.Context, callback func(ctx context.Context) error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.onReadyCallbacks = append(m.onReadyCallbacks, callback)
}

// TriggerReady simulates the node becoming ready by calling all OnReady callbacks.
func (m *MockNode) TriggerReady(ctx context.Context) error {
	m.mu.Lock()
	callbacks := m.onReadyCallbacks
	m.mu.Unlock()

	for _, cb := range callbacks {
		if err := cb(ctx); err != nil {
			return err
		}
	}

	return nil
}

func (m *MockNode) BlockNumber(_ context.Context) (*uint64, error) {
	num := uint64(12345)

	return &num, nil
}

func (m *MockNode) BlockByNumber(_ context.Context, number *big.Int) (execution.Block, error) {
	return &MockBlock{number: number}, nil
}

func (m *MockNode) BlocksByNumbers(_ context.Context, numbers []*big.Int) ([]execution.Block, error) {
	blocks := make([]execution.Block, len(numbers))
	for i, num := range numbers {
		blocks[i] = &MockBlock{number: num}
	}

	return blocks, nil
}

func (m *MockNode) BlockReceipts(_ context.Context, _ *big.Int) ([]execution.Receipt, error) {
	return []execution.Receipt{}, nil
}

func (m *MockNode) TransactionReceipt(_ context.Context, _ string) (execution.Receipt, error) {
	return &MockReceipt{}, nil
}

func (m *MockNode) DebugTraceTransaction(
	_ context.Context,
	_ string,
	_ *big.Int,
	_ execution.TraceOptions,
) (*execution.TraceTransaction, error) {
	return &execution.TraceTransaction{}, nil
}

func (m *MockNode) ChainID() int64 {
	return 1
}

func (m *MockNode) ClientType() string {
	return "mock"
}

func (m *MockNode) IsSynced() bool {
	return true
}

func (m *MockNode) Name() string {
	return m.name
}

// Compile-time check that MockNode implements execution.Node.
var _ execution.Node = (*MockNode)(nil)

func TestPool_Creation(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	config := &ethereum.Config{
		Execution: []*execution.Config{
			{
				Name:        "test-node-1",
				NodeAddress: "http://localhost:8545",
			},
			{
				Name:        "test-node-2",
				NodeAddress: "http://localhost:8546",
			},
		},
	}

	pool := ethereum.NewPool(log, "test", config)
	require.NotNil(t, pool)
	assert.True(t, pool.HasExecutionNodes())
}

func TestPool_EmptyConfig(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	config := &ethereum.Config{
		Execution: []*execution.Config{},
	}

	pool := ethereum.NewPool(log, "test", config)
	require.NotNil(t, pool)
	assert.False(t, pool.HasExecutionNodes())
	assert.False(t, pool.HasHealthyExecutionNodes())
	assert.Nil(t, pool.GetHealthyExecutionNode())
	assert.Empty(t, pool.GetHealthyExecutionNodes())
}

func TestPool_StartStop(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	config := &ethereum.Config{
		Execution: []*execution.Config{
			{
				Name:        "test-node",
				NodeAddress: "http://localhost:8545",
			},
		},
	}

	pool := ethereum.NewPool(log, "test", config)
	require.NotNil(t, pool)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Start the pool
	pool.Start(ctx)

	// Let it run briefly
	time.Sleep(100 * time.Millisecond)

	// Stop the pool
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer stopCancel()

	err := pool.Stop(stopCtx)
	assert.NoError(t, err)
}

func TestPool_WaitForHealthyNode_NoNodes(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	config := &ethereum.Config{
		Execution: []*execution.Config{},
	}

	pool := ethereum.NewPool(log, "test", config)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	node, err := pool.WaitForHealthyExecutionNode(ctx)
	assert.Error(t, err)
	assert.Nil(t, node)
	assert.Contains(t, err.Error(), "no execution nodes configured")
}

func TestPool_WaitForHealthyNode_Timeout(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	config := &ethereum.Config{
		Execution: []*execution.Config{
			{
				Name:        "test-node",
				NodeAddress: "http://invalid:8545", // Invalid address
			},
		},
	}

	pool := ethereum.NewPool(log, "test", config)

	// Start pool to begin health checks
	startCtx := context.Background()
	pool.Start(startCtx)

	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		err := pool.Stop(stopCtx)
		assert.NoError(t, err)
	}()

	// Wait for healthy node with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	node, err := pool.WaitForHealthyExecutionNode(ctx)
	duration := time.Since(start)

	assert.Error(t, err)
	assert.Nil(t, node)
	assert.Equal(t, context.DeadlineExceeded, err)
	assert.Greater(t, duration, 150*time.Millisecond)
	assert.Less(t, duration, 1*time.Second)
}

func TestPool_ConcurrentAccess(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	config := &ethereum.Config{
		Execution: []*execution.Config{
			{
				Name:        "test-node-1",
				NodeAddress: "http://localhost:8545",
			},
			{
				Name:        "test-node-2",
				NodeAddress: "http://localhost:8546",
			},
		},
	}

	pool := ethereum.NewPool(log, "test", config)

	// Start pool
	ctx := context.Background()
	pool.Start(ctx)

	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		err := pool.Stop(stopCtx)
		assert.NoError(t, err)
	}()

	// Concurrent access to pool methods
	var wg sync.WaitGroup

	const numGoroutines = 10

	// Test concurrent HasHealthyExecutionNodes calls
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < 5; j++ {
				pool.HasHealthyExecutionNodes()
				time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	// Test concurrent GetHealthyExecutionNode calls
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < 5; j++ {
				pool.GetHealthyExecutionNode()

				time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	// Test concurrent GetHealthyExecutionNodes calls
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for j := 0; j < 5; j++ {
				pool.GetHealthyExecutionNodes()
				time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	wg.Wait()
}

func TestPool_NetworkNameOverride(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	testCases := []struct {
		name         string
		override     *string
		chainID      int64
		expectedName string
		expectError  bool
	}{
		{
			name:         "with override",
			override:     stringPtr("custom-network"),
			chainID:      1,
			expectedName: "custom-network",
			expectError:  false,
		},
		{
			name:         "without override mainnet",
			override:     nil,
			chainID:      1,
			expectedName: "mainnet",
			expectError:  false,
		},
		{
			name:         "without override unknown",
			override:     nil,
			chainID:      999999,
			expectedName: "",
			expectError:  true,
		},
		{
			name:         "empty override",
			override:     stringPtr(""),
			chainID:      1,
			expectedName: "mainnet",
			expectError:  false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := &ethereum.Config{
				OverrideNetworkName: tc.override,
				Execution: []*execution.Config{
					{
						Name:        "test-node",
						NodeAddress: "http://localhost:8545",
					},
				},
			}

			pool := ethereum.NewPool(log, "test", config)

			network, err := pool.GetNetworkByChainID(tc.chainID)

			if tc.expectError {
				assert.Error(t, err)
				assert.Nil(t, network)
			} else {
				assert.NoError(t, err)
				require.NotNil(t, network)
				assert.Equal(t, tc.expectedName, network.Name)
			}
		})
	}
}

func TestPool_GracefulShutdown(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	config := &ethereum.Config{
		Execution: []*execution.Config{
			{
				Name:        "test-node-1",
				NodeAddress: "http://localhost:8545",
			},
			{
				Name:        "test-node-2",
				NodeAddress: "http://localhost:8546",
			},
		},
	}

	pool := ethereum.NewPool(log, "test", config)

	// Start pool
	ctx := context.Background()
	pool.Start(ctx)

	// Let it run for a bit
	time.Sleep(100 * time.Millisecond)

	// Test graceful shutdown
	start := time.Now()
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	defer cancel()

	err := pool.Stop(stopCtx)
	duration := time.Since(start)

	assert.NoError(t, err)
	assert.Less(t, duration, 3*time.Second)
}

func TestPool_MultipleStartStop(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	config := &ethereum.Config{
		Execution: []*execution.Config{
			{
				Name:        "test-node",
				NodeAddress: "http://localhost:8545",
			},
		},
	}

	const cycles = 3
	for i := 0; i < cycles; i++ {
		t.Run("cycle", func(t *testing.T) {
			pool := ethereum.NewPool(log, "test", config)

			// Start
			ctx := context.Background()
			pool.Start(ctx)

			// Brief operation
			time.Sleep(50 * time.Millisecond)

			hasNodes := pool.HasExecutionNodes()
			assert.True(t, hasNodes)

			// Stop
			stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err := pool.Stop(stopCtx)
			assert.NoError(t, err)

			cancel()

			assert.NoError(t, err)
		})
	}
}

func TestPool_ContextCancellation(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	config := &ethereum.Config{
		Execution: []*execution.Config{
			{
				Name:        "test-node",
				NodeAddress: "http://localhost:8545",
			},
		},
	}

	pool := ethereum.NewPool(log, "test", config)

	// Start with context that will be cancelled
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	pool.Start(ctx)

	// Wait for context cancellation
	<-ctx.Done()

	// Stop should still work after context cancellation
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()

	err := pool.Stop(stopCtx)
	assert.NoError(t, err)
}

func TestPool_NodeSelection(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	config := &ethereum.Config{
		Execution: []*execution.Config{
			{
				Name:        "test-node-1",
				NodeAddress: "http://localhost:8545",
			},
			{
				Name:        "test-node-2",
				NodeAddress: "http://localhost:8546",
			},
			{
				Name:        "test-node-3",
				NodeAddress: "http://localhost:8547",
			},
		},
	}

	pool := ethereum.NewPool(log, "test", config)

	// Initially no healthy nodes
	assert.False(t, pool.HasHealthyExecutionNodes())
	assert.Nil(t, pool.GetHealthyExecutionNode())
	assert.Empty(t, pool.GetHealthyExecutionNodes())

	// Test that selection is properly random when we have multiple healthy nodes
	// (This is hard to test deterministically, but we can at least verify the methods work)
	nodes := pool.GetHealthyExecutionNodes()
	assert.Empty(t, nodes)

	node := pool.GetHealthyExecutionNode()
	assert.Nil(t, node)
}

// Helper function to create string pointer.
func stringPtr(s string) *string {
	return &s
}

// Tests for NewPoolWithNodes

func TestPool_NewPoolWithNodes_Basic(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	node1 := NewMockNode("mock-node-1")
	node2 := NewMockNode("mock-node-2")

	nodes := []execution.Node{node1, node2}
	pool := ethereum.NewPoolWithNodes(log, "test", nodes, nil)

	require.NotNil(t, pool)
	assert.True(t, pool.HasExecutionNodes())
	assert.False(t, pool.HasHealthyExecutionNodes())
}

func TestPool_NewPoolWithNodes_NilConfig(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	node := NewMockNode("mock-node")

	nodes := []execution.Node{node}
	pool := ethereum.NewPoolWithNodes(log, "test", nodes, nil)

	require.NotNil(t, pool)
	assert.True(t, pool.HasExecutionNodes())

	// Verify default config behavior - unknown chain ID should error
	network, err := pool.GetNetworkByChainID(999999)
	assert.Error(t, err)
	assert.Nil(t, network)
}

func TestPool_NewPoolWithNodes_EmptyNodes(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	nodes := []execution.Node{}
	pool := ethereum.NewPoolWithNodes(log, "test", nodes, nil)

	require.NotNil(t, pool)
	assert.False(t, pool.HasExecutionNodes())
	assert.False(t, pool.HasHealthyExecutionNodes())
	assert.Nil(t, pool.GetHealthyExecutionNode())
	assert.Empty(t, pool.GetHealthyExecutionNodes())
}

func TestPool_NewPoolWithNodes_MultipleNodes(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	const numNodes = 5

	nodes := make([]execution.Node, numNodes)
	for i := 0; i < numNodes; i++ {
		nodes[i] = NewMockNode("mock-node-" + string(rune('a'+i)))
	}

	pool := ethereum.NewPoolWithNodes(log, "test", nodes, nil)

	require.NotNil(t, pool)
	assert.True(t, pool.HasExecutionNodes())
}

func TestPool_NewPoolWithNodes_WithConfig(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	node := NewMockNode("mock-node")
	overrideName := "custom-network"

	config := &ethereum.Config{
		OverrideNetworkName: &overrideName,
	}

	nodes := []execution.Node{node}
	pool := ethereum.NewPoolWithNodes(log, "test", nodes, config)

	require.NotNil(t, pool)

	// Verify config is used - override name should be returned
	network, err := pool.GetNetworkByChainID(999999)
	require.NoError(t, err)
	assert.Equal(t, "custom-network", network.Name)
}

func TestPool_NewPoolWithNodes_StartStop(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	node := NewMockNode("mock-node")

	nodes := []execution.Node{node}
	pool := ethereum.NewPoolWithNodes(log, "test", nodes, nil)

	ctx := context.Background()
	pool.Start(ctx)

	// Wait for async Start goroutines to execute
	time.Sleep(50 * time.Millisecond)

	// Node should have been started
	node.mu.Lock()
	assert.True(t, node.started)
	node.mu.Unlock()

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err := pool.Stop(stopCtx)
	assert.NoError(t, err)

	// Node should have been stopped
	node.mu.Lock()
	assert.True(t, node.stopped)
	node.mu.Unlock()
}

func TestPool_NewPoolWithNodes_NodeBecomesHealthy(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	node := NewMockNode("mock-node")

	nodes := []execution.Node{node}
	pool := ethereum.NewPoolWithNodes(log, "test", nodes, nil)

	ctx := context.Background()
	pool.Start(ctx)

	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := pool.Stop(stopCtx)
		assert.NoError(t, err)
	}()

	// Wait for async Start goroutines to register callbacks
	time.Sleep(50 * time.Millisecond)

	// Initially no healthy nodes
	assert.False(t, pool.HasHealthyExecutionNodes())

	// Trigger the node to become ready (simulates OnReady callback)
	err := node.TriggerReady(ctx)
	require.NoError(t, err)

	// Now the pool should have a healthy node
	assert.True(t, pool.HasHealthyExecutionNodes())
	assert.NotNil(t, pool.GetHealthyExecutionNode())
}

func TestPool_EmbeddedNodeIntegration(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	// Create an EmbeddedNode with a mock data source
	ds := &testDataSource{}
	embeddedNode := execution.NewEmbeddedNode(log, "embedded-test", ds)

	nodes := []execution.Node{embeddedNode}
	pool := ethereum.NewPoolWithNodes(log, "test", nodes, nil)

	ctx := context.Background()
	pool.Start(ctx)

	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err := pool.Stop(stopCtx)
		assert.NoError(t, err)
	}()

	// Wait for async Start goroutines to register callbacks
	time.Sleep(50 * time.Millisecond)

	// Initially no healthy nodes
	assert.False(t, pool.HasHealthyExecutionNodes())

	// Mark embedded node as ready
	err := embeddedNode.MarkReady(ctx)
	require.NoError(t, err)

	// Now the pool should have a healthy node
	assert.True(t, pool.HasHealthyExecutionNodes())

	healthyNode := pool.GetHealthyExecutionNode()
	require.NotNil(t, healthyNode)
	assert.Equal(t, "embedded-test", healthyNode.Name())
}

// testDataSource is a minimal DataSource implementation for integration tests.
type testDataSource struct{}

func (ds *testDataSource) BlockNumber(_ context.Context) (*uint64, error) {
	num := uint64(12345)

	return &num, nil
}

func (ds *testDataSource) BlockByNumber(_ context.Context, number *big.Int) (execution.Block, error) {
	return &MockBlock{number: number}, nil
}

func (ds *testDataSource) BlockReceipts(_ context.Context, _ *big.Int) ([]execution.Receipt, error) {
	return []execution.Receipt{}, nil
}

func (ds *testDataSource) TransactionReceipt(_ context.Context, _ string) (execution.Receipt, error) {
	return &MockReceipt{}, nil
}

func (ds *testDataSource) DebugTraceTransaction(
	_ context.Context,
	_ string,
	_ *big.Int,
	_ execution.TraceOptions,
) (*execution.TraceTransaction, error) {
	return &execution.TraceTransaction{}, nil
}

func (ds *testDataSource) ChainID() int64 {
	return 1
}

func (ds *testDataSource) ClientType() string {
	return "test"
}

func (ds *testDataSource) IsSynced() bool {
	return true
}

func (ds *testDataSource) BlocksByNumbers(_ context.Context, numbers []*big.Int) ([]execution.Block, error) {
	blocks := make([]execution.Block, len(numbers))
	for i, num := range numbers {
		blocks[i] = &MockBlock{number: num}
	}

	return blocks, nil
}
