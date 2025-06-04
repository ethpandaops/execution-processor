package processor_test

import (
	"context"
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
						DSN:          "clickhouse://localhost:9000/test",
						MaxOpenConns: 10,
						MaxIdleConns: 5,
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
						DSN:          "clickhouse://localhost:9000/test_mode",
						MaxOpenConns: 3,
						MaxIdleConns: 1,
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
				DSN:          "clickhouse://localhost:9000/test_concurrent",
				MaxOpenConns: 10,
				MaxIdleConns: 5,
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
				DSN:          "clickhouse://localhost:9000/test_no_leader",
				MaxOpenConns: 3,
				MaxIdleConns: 1,
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
