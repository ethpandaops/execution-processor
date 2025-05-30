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
				DSN:          "clickhouse://localhost:9000/test_manager",
				MaxOpenConns: 5,
				MaxIdleConns: 2,
			},
			Table: "test_manager_blocks",
		},
		Limiter: state.LimiterConfig{
			Enabled: false,
		},
	}
	stateManager, err := state.NewManager(context.Background(), log.WithField("component", "state"), stateConfig)
	require.NoError(t, err)

	manager, err := processor.NewManager(log, config, pool, stateManager, redisClient)
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
				DSN:          "clickhouse://localhost:9000/test_manager_start_stop",
				MaxOpenConns: 3,
				MaxIdleConns: 1,
			},
			Table: "test_manager_start_stop_blocks",
		},
		Limiter: state.LimiterConfig{
			Enabled: false,
		},
	}
	stateManager, err := state.NewManager(context.Background(), log.WithField("component", "state"), stateConfig)
	require.NoError(t, err)

	manager, err := processor.NewManager(log, config, pool, stateManager, redisClient)
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
				DSN:          "clickhouse://localhost:9000/test_manager_multi_stop",
				MaxOpenConns: 3,
				MaxIdleConns: 1,
			},
			Table: "test_manager_multi_stop_blocks",
		},
		Limiter: state.LimiterConfig{
			Enabled: false,
		},
	}
	stateManager, err := state.NewManager(context.Background(), log.WithField("component", "state"), stateConfig)
	require.NoError(t, err)

	manager, err := processor.NewManager(log, config, pool, stateManager, redisClient)
	require.NoError(t, err)

	// Test multiple stops don't panic
	ctx := context.Background()
	for i := 0; i < 3; i++ {
		err := manager.Stop(ctx)
		assert.NoError(t, err, "Stop call %d should not error", i+1)
	}
}
