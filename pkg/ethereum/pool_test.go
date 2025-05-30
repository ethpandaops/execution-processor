package ethereum_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/ethereum"
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
		chainID      int32
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
				assert.Equal(t, tc.chainID, network.ID)
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

// Helper function to create string pointer
func stringPtr(s string) *string {
	return &s
}
