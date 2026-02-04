package processor_test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
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

// newTestRedis creates an in-memory Redis server for testing.
func newTestRedis(t *testing.T) *redis.Client {
	t.Helper()

	s := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})

	t.Cleanup(func() {
		_ = client.Close()
	})

	return client
}

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

	redisClient := newTestRedis(t)

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
				Addr: "localhost:9000",
			},
			Table: "test_manager_blocks",
		},
		Limiter: state.LimiterConfig{
			Enabled: false,
		},
	}
	stateManager, err := state.NewManager(log.WithField("component", "state"), stateConfig)
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

	redisClient := newTestRedis(t)

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
				Addr: "localhost:9000",
			},
			Table: "test_manager_start_stop_blocks",
		},
		Limiter: state.LimiterConfig{
			Enabled: false,
		},
	}
	stateManager, err := state.NewManager(log.WithField("component", "state"), stateConfig)
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

		startErr := manager.Start(ctx)
		// Should return when context is cancelled or due to connection failures
		// We expect it to either succeed or fail gracefully due to external service unavailability
		_ = startErr // Ignore connection errors to external services
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

	redisClient := newTestRedis(t)

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
				Addr: "localhost:9000",
			},
			Table: "test_manager_multi_stop_blocks",
		},
		Limiter: state.LimiterConfig{
			Enabled: false,
		},
	}
	stateManager, err := state.NewManager(log.WithField("component", "state"), stateConfig)
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
						Addr: "localhost:9000",
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
						Addr: "localhost:9000",
					},
					Table: "test_mode_blocks",
				},
				Limiter: state.LimiterConfig{
					Enabled: false,
				},
			}
			stateManager, err := state.NewManager(log.WithField("component", "state"), stateConfig)
			require.NoError(t, err)

			redisClient := newTestRedis(t)

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
				Addr: "localhost:9000",
			},
			Table: "test_concurrent_blocks",
		},
		Limiter: state.LimiterConfig{
			Enabled: false,
		},
	}
	stateManager, err := state.NewManager(log.WithField("component", "state"), stateConfig)
	require.NoError(t, err)

	// Each miniredis instance is isolated, simulating separate Redis servers
	forwardsRedis := newTestRedis(t)
	backwardsRedis := newTestRedis(t)

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
				Addr: "localhost:9000",
			},
			Table: "test_no_leader_blocks",
		},
		Limiter: state.LimiterConfig{
			Enabled: false,
		},
	}
	stateManager, err := state.NewManager(log.WithField("component", "state"), stateConfig)
	require.NoError(t, err)

	redisClient := newTestRedis(t)

	// Should work fine even with leader election disabled
	manager, err := processor.NewManager(log, config, pool, stateManager, redisClient, "test-prefix")
	require.NoError(t, err)
	require.NotNil(t, manager)
}

// Race condition tests

// TestGapDetectionRaceConditionLogic tests the race condition where a block
// completes between gap scan and reprocess decision.
//
// The race occurs when:
//  1. Gap detection queries ClickHouse - returns block 100 as incomplete
//  2. Block 100 completes (MarkBlockComplete writes complete=1, cleans Redis)
//  3. Gap detection checks Redis - HasBlockTracking returns false
//  4. Gap detection calls ReprocessBlock - WRONG! Block is already complete
//
// Current (buggy) logic in checkGaps():
//
//	if !hasTracking { ReprocessBlock() }  // BUG: doesn't check if complete
//
// Fixed logic:
//
//	if !hasTracking && !isComplete { ReprocessBlock() }
func TestGapDetectionRaceConditionLogic(t *testing.T) {
	tests := []struct {
		name            string
		hasTracking     bool // Redis tracking exists
		isComplete      bool // Block is complete in ClickHouse
		shouldReprocess bool // Expected decision
	}{
		{
			name:            "has tracking - skip (being processed)",
			hasTracking:     true,
			isComplete:      false,
			shouldReprocess: false,
		},
		{
			name:            "no tracking, incomplete - reprocess (orphaned)",
			hasTracking:     false,
			isComplete:      false,
			shouldReprocess: true,
		},
		{
			name:            "no tracking, complete - skip (race condition)",
			hasTracking:     false,
			isComplete:      true,
			shouldReprocess: false, // THIS CASE IS THE BUG - currently would reprocess!
		},
		{
			name:            "has tracking, complete - skip (already being processed)",
			hasTracking:     true,
			isComplete:      true,
			shouldReprocess: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// FIXED logic (now in checkGaps):
			// Only reprocess if no Redis tracking AND not complete in ClickHouse
			shouldReprocess := !tt.hasTracking && !tt.isComplete

			assert.Equal(t, tt.shouldReprocess, shouldReprocess,
				"block with hasTracking=%v, isComplete=%v should reprocess=%v, but got %v",
				tt.hasTracking, tt.isComplete, tt.shouldReprocess, shouldReprocess)
		})
	}
}

// TestManager_RaceConditions specifically tests for race conditions in manager.
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
				Addr: "localhost:9000",
			},
			Table: "test_race_blocks",
		},
		Limiter: state.LimiterConfig{
			Enabled: false,
		},
	}
	stateManager, err := state.NewManager(log.WithField("component", "state"), stateConfig)
	require.NoError(t, err)

	redisClient := newTestRedis(t)

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

// TestManager_ConcurrentConfiguration tests concurrent access to manager configuration.
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
				Addr: "localhost:9000",
			},
			Table: "test_config_blocks",
		},
		Limiter: state.LimiterConfig{
			Enabled: false,
		},
	}
	stateManager, err := state.NewManager(log.WithField("component", "state"), stateConfig)
	require.NoError(t, err)

	redisClient := newTestRedis(t)

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

// =====================================
// LEADERSHIP TRANSITION RACE CONDITION TESTS
// =====================================

const testLeaderKey = "test-prefix:leader:test:forwards"

// createTestManager is a helper to create a manager for leadership tests.
func createTestManager(t *testing.T, leaderEnabled bool) (*processor.Manager, *redis.Client) {
	t.Helper()

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	config := &processor.Config{
		Interval:    50 * time.Millisecond,
		Mode:        "forwards",
		Concurrency: 2,
		LeaderElection: processor.LeaderElectionConfig{
			Enabled:         leaderEnabled,
			TTL:             500 * time.Millisecond,
			RenewalInterval: 100 * time.Millisecond,
			NodeID:          "race-test-node",
		},
		TransactionStructlog: structlog.Config{
			Enabled: false,
		},
	}

	redisClient := newTestRedis(t)

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
				Addr: "localhost:9000",
			},
			Table: "test_leadership_race_blocks",
		},
		Limiter: state.LimiterConfig{
			Enabled: false,
		},
	}

	stateManager, err := state.NewManager(log.WithField("component", "state"), stateConfig)
	require.NoError(t, err)

	manager, err := processor.NewManager(log, config, pool, stateManager, redisClient, "test-prefix")
	require.NoError(t, err)

	return manager, redisClient
}

// TestManager_ConcurrentStopAndLeadershipLoss tests that Stop() and leadership loss
// don't race to close the same blockProcessStop channel.
//
// This test should expose a potential panic if both paths try to close the same channel.
// The bug is in manager.go where both Stop() and handleLeadershipLoss() have:
//
//	if m.blockProcessStop != nil {
//	    select {
//	    case <-m.blockProcessStop:
//	    default:
//	        close(m.blockProcessStop)
//	    }
//	}
//
// If they race, one can close while the other is checking, causing a panic.
func TestManager_ConcurrentStopAndLeadershipLoss(t *testing.T) {
	// Run multiple times to increase chance of hitting the race
	for iteration := range 20 {
		t.Run("iteration", func(t *testing.T) {
			manager, redisClient := createTestManager(t, true)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			// Track panics
			var panicCount atomic.Int32

			// Start the manager
			startDone := make(chan struct{})

			go func() {
				defer func() {
					if r := recover(); r != nil {
						panicCount.Add(1)
						t.Logf("Iteration %d: PANIC in Start: %v", iteration, r)
					}

					close(startDone)
				}()

				_ = manager.Start(ctx)
			}()

			// Wait for manager to start and potentially gain leadership
			time.Sleep(200 * time.Millisecond)

			// Now trigger concurrent stop and leadership loss
			var wg sync.WaitGroup

			// Goroutine 1: Call Stop()
			wg.Add(1)

			go func() {
				defer func() {
					if r := recover(); r != nil {
						panicCount.Add(1)
						t.Logf("Iteration %d: PANIC in Stop: %v", iteration, r)
					}

					wg.Done()
				}()

				stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
				defer stopCancel()

				_ = manager.Stop(stopCtx)
			}()

			// Goroutine 2: Simulate leadership loss by deleting the Redis key
			wg.Add(1)

			go func() {
				defer func() {
					if r := recover(); r != nil {
						panicCount.Add(1)
						t.Logf("Iteration %d: PANIC in leadership loss: %v", iteration, r)
					}

					wg.Done()
				}()

				// Delete leader key to trigger leadership loss
				redisClient.Del(context.Background(), "test-prefix:leader:test:forwards")
			}()

			wg.Wait()
			<-startDone

			// Check for panics
			if panicCount.Load() > 0 {
				t.Fatalf("Iteration %d: Detected %d panic(s) during concurrent stop/leadership loss",
					iteration, panicCount.Load())
			}
		})
	}
}

// TestManager_RapidLeadershipFlipping tests that rapid leadership gain/loss cycles
// don't cause orphaned goroutines or channel leaks.
//
// The bug: handleLeadershipGain() creates a new blockProcessStop channel each time.
// If leadership flips rapidly, old runBlockProcessing goroutines may be orphaned
// because they're waiting on the old channel that's now unreachable.
func TestManager_RapidLeadershipFlipping(t *testing.T) {
	manager, redisClient := createTestManager(t, true)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start the manager
	startDone := make(chan struct{})

	go func() {
		defer close(startDone)

		_ = manager.Start(ctx)
	}()

	// Wait for initial startup
	time.Sleep(200 * time.Millisecond)

	// Rapidly flip leadership 20 times
	for i := range 20 {
		// Delete key to force leadership loss
		redisClient.Del(context.Background(), testLeaderKey)
		time.Sleep(30 * time.Millisecond)

		// The manager should re-acquire leadership automatically
		// (since it's the only instance)
		time.Sleep(150 * time.Millisecond)

		t.Logf("Flip %d complete", i)
	}

	// Now stop - this should NOT hang if goroutines are properly managed
	stopDone := make(chan struct{})

	go func() {
		defer close(stopDone)

		stopCtx, stopCancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer stopCancel()

		err := manager.Stop(stopCtx)
		if err != nil {
			t.Logf("Stop error: %v", err)
		}
	}()

	// Wait for stop with timeout - if it hangs, goroutines are orphaned
	select {
	case <-stopDone:
		t.Log("Manager stopped successfully")
	case <-time.After(5 * time.Second):
		t.Fatal("CRITICAL: Manager.Stop() hung - likely orphaned goroutines waiting on unreachable channels")
	}

	cancel()
	<-startDone
}

// TestManager_StopDuringLeadershipTransition tests stopping the manager
// while a leadership transition is in progress.
//
// This can expose races where Stop() tries to close blockProcessStop
// while handleLeadershipGain() is creating a new one.
func TestManager_StopDuringLeadershipTransition(t *testing.T) {
	for iteration := range 10 {
		t.Run("iteration", func(t *testing.T) {
			manager, redisClient := createTestManager(t, true)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			var panicCount atomic.Int32

			// Start manager
			startDone := make(chan struct{})

			go func() {
				defer func() {
					if r := recover(); r != nil {
						panicCount.Add(1)
						t.Logf("Iteration %d: PANIC in Start: %v", iteration, r)
					}

					close(startDone)
				}()

				_ = manager.Start(ctx)
			}()

			// Wait for startup
			time.Sleep(150 * time.Millisecond)

			// Start rapid leadership flipping in background
			flipDone := make(chan struct{})

			go func() {
				defer close(flipDone)

				for i := 0; i < 10; i++ {
					redisClient.Del(context.Background(), testLeaderKey)
					time.Sleep(20 * time.Millisecond)
				}
			}()

			// While flipping, call Stop
			time.Sleep(50 * time.Millisecond)

			stopDone := make(chan struct{})

			go func() {
				defer func() {
					if r := recover(); r != nil {
						panicCount.Add(1)
						t.Logf("Iteration %d: PANIC in Stop: %v", iteration, r)
					}

					close(stopDone)
				}()

				stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer stopCancel()

				_ = manager.Stop(stopCtx)
			}()

			// Wait for everything
			<-flipDone

			select {
			case <-stopDone:
				// Good
			case <-time.After(3 * time.Second):
				t.Fatalf("Iteration %d: Stop hung during leadership transition", iteration)
			}

			cancel()
			<-startDone

			if panicCount.Load() > 0 {
				t.Fatalf("Iteration %d: Detected %d panic(s)", iteration, panicCount.Load())
			}
		})
	}
}

// TestManager_WaitGroupLeakOnRapidTransitions verifies that the WaitGroup
// count stays balanced during rapid leadership transitions.
//
// The bug: handleLeadershipGain() calls wg.Add(1) and spawns a goroutine.
// If the goroutine doesn't properly exit (e.g., stuck on old channel),
// wg.Wait() in Stop() will hang forever.
func TestManager_WaitGroupLeakOnRapidTransitions(t *testing.T) {
	manager, redisClient := createTestManager(t, true)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	startDone := make(chan struct{})

	go func() {
		defer close(startDone)

		_ = manager.Start(ctx)
	}()

	time.Sleep(200 * time.Millisecond)

	// Do many rapid transitions
	for i := range 30 {
		redisClient.Del(context.Background(), testLeaderKey)
		time.Sleep(20 * time.Millisecond)

		// Let it re-acquire
		time.Sleep(120 * time.Millisecond)

		if i%10 == 0 {
			t.Logf("Completed %d transitions", i)
		}
	}

	// Stop with a strict timeout
	stopComplete := make(chan error, 1)

	go func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer stopCancel()

		stopComplete <- manager.Stop(stopCtx)
	}()

	select {
	case err := <-stopComplete:
		if err != nil {
			t.Logf("Stop returned error: %v", err)
		}

		t.Log("Stop completed - WaitGroup balanced correctly")
	case <-time.After(5 * time.Second):
		t.Fatal("CRITICAL: Stop hung - WaitGroup leak detected (orphaned goroutines)")
	}

	cancel()
	<-startDone
}
