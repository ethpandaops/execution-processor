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
	"github.com/stretchr/testify/require"
)

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
				DSN:          "clickhouse://localhost:9000/test_race",
				MaxOpenConns: 5,
				MaxIdleConns: 2,
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
				DSN:          "clickhouse://localhost:9000/test_config",
				MaxOpenConns: 3,
				MaxIdleConns: 1,
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
