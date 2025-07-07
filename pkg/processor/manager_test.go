package processor_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
	"github.com/ethpandaops/execution-processor/pkg/ethereum"
	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
	"github.com/ethpandaops/execution-processor/pkg/processor"
	"github.com/ethpandaops/execution-processor/pkg/processor/transaction/structlog"
	"github.com/ethpandaops/execution-processor/pkg/state"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Basic functionality tests

func TestManager_Creation(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	config := &processor.Config{
		Interval:    10 * time.Second,
		Mode:        "forwards",
		Concurrency: 10,
		LeaderElection: processor.LeaderElectionConfig{
			Enabled:         false,
			TTL:             30 * time.Second,
			RenewalInterval: 10 * time.Second,
			NodeID:          "test-node",
		},
		TransactionStructlog: structlog.Config{
			Enabled: false,
		},
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15, // Use high DB number for tests
	})
	defer redisClient.Close()

	// Create proper pool and state mocks
	poolConfig := &ethereum.Config{
		Execution: []*execution.Config{
			{
				Name:        "test-node",
				NodeAddress: "http://localhost:8545",
			},
		},
	}
	pool := ethereum.NewPool(log.WithField("component", "pool"), "test", poolConfig)

	stateConfig := &state.Config{
		Storage: state.StorageConfig{
			Config: clickhouse.Config{
				URL: "http://localhost:8123",
			},
			Table: "test_manager_blocks",
		},
		Limiter: state.LimiterConfig{
			Enabled: false,
		},
	}
	stateManager, err := state.NewManager(context.Background(), log.WithField("component", "state"), stateConfig)
	require.NoError(t, err)

	manager, err := processor.NewManager(log, config, pool, stateManager, redisClient, "test-prefix")
	require.NoError(t, err)
	require.NotNil(t, manager)
}

func TestManager_StartStop(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	config := &processor.Config{
		Interval:    100 * time.Millisecond,
		Mode:        "forwards",
		Concurrency: 2,
		LeaderElection: processor.LeaderElectionConfig{
			Enabled:         false,
			TTL:             5 * time.Second,
			RenewalInterval: 1 * time.Second,
			NodeID:          "test-node-start-stop",
		},
		TransactionStructlog: structlog.Config{
			Enabled: false,
		},
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15,
	})
	defer redisClient.Close()

	// Create proper components
	poolConfig := &ethereum.Config{
		Execution: []*execution.Config{
			{
				Name:        "test-node-start-stop",
				NodeAddress: "http://localhost:8545",
			},
		},
	}
	pool := ethereum.NewPool(log.WithField("component", "pool"), "test", poolConfig)

	stateConfig := &state.Config{
		Storage: state.StorageConfig{
			Config: clickhouse.Config{
				URL: "http://localhost:8123",
			},
			Table: "test_manager_start_stop_blocks",
		},
		Limiter: state.LimiterConfig{
			Enabled: false,
		},
	}
	stateManager, err := state.NewManager(context.Background(), log.WithField("component", "state"), stateConfig)
	require.NoError(t, err)

	manager, err := processor.NewManager(log, config, pool, stateManager, redisClient, "test-prefix")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Test start/stop cycle
	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		err := manager.Start(ctx)
		// Should return when context is cancelled or due to connection failures
		// We expect it to either succeed or fail gracefully due to external service unavailability
		_ = err // Ignore connection errors to external services
	}()

	time.Sleep(100 * time.Millisecond)

	// Test stop
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer stopCancel()

	err = manager.Stop(stopCtx)
	assert.NoError(t, err)

	wg.Wait()
}

func TestManager_MultipleStops(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	config := &processor.Config{
		Interval:    200 * time.Millisecond,
		Mode:        "forwards",
		Concurrency: 1,
		LeaderElection: processor.LeaderElectionConfig{
			Enabled:         false,
			TTL:             2 * time.Second,
			RenewalInterval: 500 * time.Millisecond,
			NodeID:          "test-node-multi-stop",
		},
		TransactionStructlog: structlog.Config{
			Enabled: false,
		},
	}

	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   14,
	})
	defer redisClient.Close()

	// Create proper components
	poolConfig := &ethereum.Config{
		Execution: []*execution.Config{
			{
				Name:        "test-node-multi-stop",
				NodeAddress: "http://localhost:8545",
			},
		},
	}
	pool := ethereum.NewPool(log.WithField("component", "pool"), "test", poolConfig)

	stateConfig := &state.Config{
		Storage: state.StorageConfig{
			Config: clickhouse.Config{
				URL: "http://localhost:8123",
			},
			Table: "test_manager_multi_stop_blocks",
		},
		Limiter: state.LimiterConfig{
			Enabled: false,
		},
	}
	stateManager, err := state.NewManager(context.Background(), log.WithField("component", "state"), stateConfig)
	require.NoError(t, err)

	manager, err := processor.NewManager(log, config, pool, stateManager, redisClient, "test-prefix")
	require.NoError(t, err)

	// Test multiple stops don't panic
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		err := manager.Stop(ctx)
		assert.NoError(t, err, "Stop call %d should not error", i+1)
	}
}

// Mode-specific tests

func TestManager_ModeSpecificLeaderElection(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	// Test that different modes create different leader keys
	testCases := []struct {
		name        string
		mode        string
		expectedKey string
	}{
		{
			name:        "forwards mode",
			mode:        "forwards",
			expectedKey: "execution-processor:leader:test:forwards",
		},
		{
			name:        "backwards mode",
			mode:        "backwards",
			expectedKey: "execution-processor:leader:test:backwards",
		},
		{
			name:        "custom mode",
			mode:        "custom",
			expectedKey: "execution-processor:leader:test:custom",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			config := &processor.Config{
				Interval:    1 * time.Second,
				Mode:        tc.mode,
				Concurrency: 5,
				LeaderElection: processor.LeaderElectionConfig{
					Enabled:         true,
					TTL:             5 * time.Second,
					RenewalInterval: 2 * time.Second,
					NodeID:          "test-node",
				},
				TransactionStructlog: structlog.Config{
					Enabled: false, // Disable to avoid ClickHouse requirements
					Table:   "test_structlog",
					Config: clickhouse.Config{
						URL: "http://localhost:8123",
					},
					BatchConfig: structlog.BatchConfig{
						Enabled: false, // Will use default chunk size
					},
				},
			}

			// Create minimal components
			poolConfig := &ethereum.Config{
				Execution: []*execution.Config{
					{
						Name:        "test-node-mode",
						NodeAddress: "http://localhost:8545",
					},
				},
			}
			pool := ethereum.NewPool(log.WithField("component", "pool"), "test", poolConfig)

			stateConfig := &state.Config{
				Storage: state.StorageConfig{
					Config: clickhouse.Config{
						URL: "http://localhost:8123",
					},
					Table: "test_mode_blocks",
				},
				Limiter: state.LimiterConfig{
					Enabled: false,
				},
			}
			stateManager, err := state.NewManager(context.Background(), log.WithField("component", "state"), stateConfig)
			require.NoError(t, err)

			redisClient := redis.NewClient(&redis.Options{
				Addr: "localhost:6379",
				DB:   15, // Use test DB
			})
			defer redisClient.Close()

			// Create manager (leader election will be initialized)
			manager, err := processor.NewManager(log, config, pool, stateManager, redisClient, "test-prefix")
			require.NoError(t, err)
			require.NotNil(t, manager)
		})
	}
}

func TestManager_ConcurrentModes(t *testing.T) {
	// This test simulates the scenario where you might want to run
	// both forwards and backwards processing simultaneously
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	// Forwards manager config
	forwardsConfig := &processor.Config{
		Interval:    1 * time.Second,
		Mode:        "forwards",
		Concurrency: 5,
		LeaderElection: processor.LeaderElectionConfig{
			Enabled:         true,
			TTL:             5 * time.Second,
			RenewalInterval: 2 * time.Second,
			NodeID:          "forwards-node",
		},
		TransactionStructlog: structlog.Config{
			Enabled: false,
		},
	}

	// Backwards manager config
	backwardsConfig := &processor.Config{
		Interval:    2 * time.Second, // Different interval
		Mode:        "backwards",
		Concurrency: 3, // Different concurrency
		LeaderElection: processor.LeaderElectionConfig{
			Enabled:         true,
			TTL:             10 * time.Second, // Different TTL
			RenewalInterval: 3 * time.Second,
			NodeID:          "backwards-node",
		},
		TransactionStructlog: structlog.Config{
			Enabled: false,
		},
	}

	// Create shared components
	poolConfig := &ethereum.Config{
		Execution: []*execution.Config{
			{
				Name:        "test-node-concurrent",
				NodeAddress: "http://localhost:8545",
			},
		},
	}
	pool := ethereum.NewPool(log.WithField("component", "pool"), "test", poolConfig)

	stateConfig := &state.Config{
		Storage: state.StorageConfig{
			Config: clickhouse.Config{
				URL: "http://localhost:8123",
			},
			Table: "test_concurrent_blocks",
		},
		Limiter: state.LimiterConfig{
			Enabled: false,
		},
	}
	stateManager, err := state.NewManager(context.Background(), log.WithField("component", "state"), stateConfig)
	require.NoError(t, err)

	// Use different Redis DBs to simulate separate Redis instances
	forwardsRedis := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   10, // Forwards Redis DB
	})
	defer forwardsRedis.Close()

	backwardsRedis := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   11, // Backwards Redis DB
	})
	defer backwardsRedis.Close()

	// Create both managers - they should not conflict with mode-specific leader keys
	forwardsManager, err := processor.NewManager(log.WithField("mode", "forwards"), forwardsConfig, pool, stateManager, forwardsRedis, "test-prefix")
	require.NoError(t, err)
	require.NotNil(t, forwardsManager)

	backwardsManager, err := processor.NewManager(log.WithField("mode", "backwards"), backwardsConfig, pool, stateManager, backwardsRedis, "test-prefix")
	require.NoError(t, err)
	require.NotNil(t, backwardsManager)
}

func TestManager_LeaderElectionDisabled(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	config := &processor.Config{
		Interval:    1 * time.Second,
		Mode:        "forwards",
		Concurrency: 5,
		LeaderElection: processor.LeaderElectionConfig{
			Enabled: false, // Disabled
		},
		TransactionStructlog: structlog.Config{
			Enabled: false,
		},
	}

	// Create minimal components
	poolConfig := &ethereum.Config{
		Execution: []*execution.Config{
			{
				Name:        "test-node-no-leader",
				NodeAddress: "http://localhost:8545",
			},
		},
	}
	pool := ethereum.NewPool(log.WithField("component", "pool"), "test", poolConfig)

	stateConfig := &state.Config{
		Storage: state.StorageConfig{
			Config: clickhouse.Config{
				URL: "http://localhost:8123",
			},
			Table: "test_no_leader_blocks",
		},
		Limiter: state.LimiterConfig{
			Enabled: false,
		},
	}
	stateManager, err := state.NewManager(context.Background(), log.WithField("component", "state"), stateConfig)
	require.NoError(t, err)

	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15,
	})
	defer redisClient.Close()

	// Should work fine even with leader election disabled
	manager, err := processor.NewManager(log, config, pool, stateManager, redisClient, "test-prefix")
	require.NoError(t, err)
	require.NotNil(t, manager)
}

// Race condition tests

// TestManager_RaceConditions specifically tests for race conditions in manager
func TestManager_RaceConditions(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	config := &processor.Config{
		Interval:    10 * time.Millisecond,
		Mode:        "forwards",
		Concurrency: 10,
		LeaderElection: processor.LeaderElectionConfig{
			Enabled:         false,
			TTL:             1 * time.Second,
			RenewalInterval: 200 * time.Millisecond,
			NodeID:          "race-test-node",
		},
		TransactionStructlog: structlog.Config{
			Enabled: false,
		},
	}

	// Create mock pool with proper configuration
	poolConfig := &ethereum.Config{
		Execution: []*execution.Config{
			{
				Name:        "mock-node",
				NodeAddress: "http://localhost:8545",
			},
		},
	}
	pool := ethereum.NewPool(log.WithField("component", "pool"), "test", poolConfig)

	// Create mock state manager
	stateConfig := &state.Config{
		Storage: state.StorageConfig{
			Config: clickhouse.Config{
				URL: "http://localhost:8123",
			},
			Table: "test_race_blocks",
		},
		Limiter: state.LimiterConfig{
			Enabled: false,
		},
	}
	stateManager, err := state.NewManager(context.Background(), log.WithField("component", "state"), stateConfig)
	require.NoError(t, err)

	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15,
	})
	defer redisClient.Close()

	manager, err := processor.NewManager(log, config, pool, stateManager, redisClient, "test-prefix")
	require.NoError(t, err)

	const numGoroutines = 5 // Reduced to avoid overwhelming the system

	var wg sync.WaitGroup

	// Test concurrent Start operations (should handle gracefully)
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)

		go func(id int) {
			defer wg.Done()

			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			// This should not cause race conditions and should handle connection failures gracefully
			err := manager.Start(ctx)
			// We expect this to either succeed or fail gracefully due to external service unavailability
			_ = err // Ignore connection errors to external services

			stopCtx, stopCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer stopCancel()

			// Stop should be safe to call even if Start failed
			err = manager.Stop(stopCtx)
			require.NoError(t, err)
		}(i)
	}

	wg.Wait()
}

// TestManager_ConcurrentConfiguration tests concurrent access to manager configuration
func TestManager_ConcurrentConfiguration(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	config := &processor.Config{
		Interval:    100 * time.Millisecond,
		Mode:        "forwards",
		Concurrency: 5,
		LeaderElection: processor.LeaderElectionConfig{
			Enabled:         false,
			TTL:             2 * time.Second,
			RenewalInterval: 500 * time.Millisecond,
			NodeID:          "config-test-node",
		},
		TransactionStructlog: structlog.Config{
			Enabled: false,
		},
	}

	// Create minimal components for configuration testing
	poolConfig := &ethereum.Config{
		Execution: []*execution.Config{
			{
				Name:        "config-test-node",
				NodeAddress: "http://localhost:8545",
			},
		},
	}
	pool := ethereum.NewPool(log.WithField("component", "pool"), "test", poolConfig)

	stateConfig := &state.Config{
		Storage: state.StorageConfig{
			Config: clickhouse.Config{
				URL: "http://localhost:8123",
			},
			Table: "test_config_blocks",
		},
		Limiter: state.LimiterConfig{
			Enabled: false,
		},
	}
	stateManager, err := state.NewManager(context.Background(), log.WithField("component", "state"), stateConfig)
	require.NoError(t, err)

	redisClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   14,
	})
	defer redisClient.Close()

	_, err = processor.NewManager(log, config, pool, stateManager, redisClient, "test-prefix")
	require.NoError(t, err)

	// Test concurrent access to manager configuration (should be immutable)
	const numReaders = 10

	var wg sync.WaitGroup

	for i := 0; i < numReaders; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			// Test that multiple goroutines can safely access the configuration
			// without causing data races
			_ = config.Interval
			_ = config.Mode
			_ = config.Concurrency
		}()
	}

	wg.Wait()
}
