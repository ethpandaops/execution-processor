package leaderelection_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/leaderelection"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// skipIfRedisUnavailable checks if Redis is available and skips the test if not.
func skipIfRedisUnavailable(t *testing.T) *redis.Client {
	t.Helper()

	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   15, // Use high DB number for tests
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		client.Close()
		t.Skipf("Redis unavailable at localhost:6379: %v", err)

		return nil
	}

	return client
}

// =====================================
// BASIC FUNCTIONALITY TESTS
// =====================================

func TestNewRedisElector(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	if client == nil {
		return
	}
	defer client.Close()

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	tests := []struct {
		name      string
		keyName   string
		config    *leaderelection.Config
		expectErr bool
	}{
		{
			name:    "valid config",
			keyName: "test:leader:network1",
			config: &leaderelection.Config{
				TTL:             10 * time.Second,
				RenewalInterval: 3 * time.Second,
				NodeID:          "test-node-1",
			},
			expectErr: false,
		},
		{
			name:      "nil config uses defaults",
			keyName:   "test:leader:network2",
			config:    nil,
			expectErr: false,
		},
		{
			name:    "empty node ID generates random",
			keyName: "test:leader:network3",
			config: &leaderelection.Config{
				TTL:             5 * time.Second,
				RenewalInterval: 1 * time.Second,
				NodeID:          "",
			},
			expectErr: false,
		},
		{
			name:    "simple key name",
			keyName: "simple-key",
			config: &leaderelection.Config{
				TTL:             5 * time.Second,
				RenewalInterval: 1 * time.Second,
				NodeID:          "node1",
			},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			elector, err := leaderelection.NewRedisElector(client, log, tt.keyName, tt.config)

			if tt.expectErr && err == nil {
				t.Error("expected error but got none")
			}

			if !tt.expectErr && err != nil {
				t.Errorf("expected no error but got: %v", err)
			}

			if !tt.expectErr && elector == nil {
				t.Error("expected elector to be created")
			}

			if elector != nil {
				// Test initial state
				if elector.IsLeader() {
					t.Error("new elector should not be leader initially")
				}

				// Test that channel is created
				ch := elector.LeadershipChannel()
				if ch == nil {
					t.Error("leadership channel should not be nil")
				}
			}
		})
	}
}

func TestRedisElector_GetLeaderID_NoLeader(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	if client == nil {
		return
	}
	defer client.Close()

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	// Clean up any existing keys
	ctx := context.Background()
	client.Del(ctx, "test:leader:no-leader")

	config := &leaderelection.Config{
		TTL:             5 * time.Second,
		RenewalInterval: 1 * time.Second,
		NodeID:          "test-node",
	}

	elector, err := leaderelection.NewRedisElector(client, log, "test:leader:no-leader", config)
	if err != nil {
		t.Fatalf("failed to create elector: %v", err)
	}

	leaderID, err := elector.GetLeaderID()
	if err == nil {
		t.Error("expected error when no leader exists")
	}

	if leaderID != "" {
		t.Errorf("expected empty leader ID, got %s", leaderID)
	}
}

func TestRedisElector_StartStop(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	if client == nil {
		return
	}
	defer client.Close()

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	// Clean up
	ctx := context.Background()
	client.Del(ctx, "test:leader:start-stop")

	config := &leaderelection.Config{
		TTL:             2 * time.Second,
		RenewalInterval: 500 * time.Millisecond,
		NodeID:          "test-node-start-stop",
	}

	elector, err := leaderelection.NewRedisElector(client, log, "test:leader:start-stop", config)
	if err != nil {
		t.Fatalf("failed to create elector: %v", err)
	}

	// Test start
	startCtx, startCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer startCancel()

	err = elector.Start(startCtx)
	if err != nil {
		t.Errorf("failed to start elector: %v", err)
	}

	// Give some time for leadership acquisition
	time.Sleep(100 * time.Millisecond)

	// Test stop
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()

	err = elector.Stop(stopCtx)
	if err != nil {
		t.Errorf("failed to stop elector: %v", err)
	}

	// Clean up
	client.Del(ctx, "test:leader:start-stop")
}

func TestRedisElector_LeadershipAcquisition(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	if client == nil {
		return
	}
	defer client.Close()

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	// Clean up
	ctx := context.Background()
	client.Del(ctx, "test:leader:acquisition")

	config := &leaderelection.Config{
		TTL:             2 * time.Second,
		RenewalInterval: 500 * time.Millisecond,
		NodeID:          "test-node-acquisition",
	}

	elector, err := leaderelection.NewRedisElector(client, log, "test:leader:acquisition", config)
	if err != nil {
		t.Fatalf("failed to create elector: %v", err)
	}

	startCtx, startCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer startCancel()

	// Start election
	err = elector.Start(startCtx)
	if err != nil {
		t.Fatalf("failed to start elector: %v", err)
	}

	// Wait for leadership acquisition
	leadershipChan := elector.LeadershipChannel()

	select {
	case isLeader := <-leadershipChan:
		if !isLeader {
			t.Error("expected to gain leadership")
		}
	case <-time.After(1 * time.Second):
		t.Error("timeout waiting for leadership acquisition")
	}

	// Verify leadership status
	if !elector.IsLeader() {
		t.Error("elector should be leader")
	}

	// Verify leader ID
	leaderID, err := elector.GetLeaderID()
	if err != nil {
		t.Errorf("failed to get leader ID: %v", err)
	}

	if leaderID != config.NodeID {
		t.Errorf("expected leader ID %s, got %s", config.NodeID, leaderID)
	}

	// Stop and clean up
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()

	err = elector.Stop(stopCtx)
	if err != nil {
		t.Errorf("failed to stop elector: %v", err)
	}

	client.Del(ctx, "test:leader:acquisition")
}

func TestRedisElector_MultipleNodes(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	if client == nil {
		return
	}
	defer client.Close()

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	// Clean up
	ctx := context.Background()
	client.Del(ctx, "test:leader:multi-node")

	// Create two electors
	config1 := &leaderelection.Config{
		TTL:             2 * time.Second,
		RenewalInterval: 500 * time.Millisecond,
		NodeID:          "node-1",
	}

	config2 := &leaderelection.Config{
		TTL:             2 * time.Second,
		RenewalInterval: 500 * time.Millisecond,
		NodeID:          "node-2",
	}

	elector1, err := leaderelection.NewRedisElector(client, log, "test:leader:multi-node", config1)
	if err != nil {
		t.Fatalf("failed to create elector1: %v", err)
	}

	elector2, err := leaderelection.NewRedisElector(client, log, "test:leader:multi-node", config2)
	if err != nil {
		t.Fatalf("failed to create elector2: %v", err)
	}

	startCtx, startCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer startCancel()

	// Start both electors
	err = elector1.Start(startCtx)
	if err != nil {
		t.Fatalf("failed to start elector1: %v", err)
	}

	err = elector2.Start(startCtx)
	if err != nil {
		t.Fatalf("failed to start elector2: %v", err)
	}

	// Give time for election
	time.Sleep(200 * time.Millisecond)

	// Exactly one should be leader
	leader1 := elector1.IsLeader()
	leader2 := elector2.IsLeader()

	if leader1 && leader2 {
		t.Error("both nodes cannot be leader")
	}

	if !leader1 && !leader2 {
		t.Error("at least one node should be leader")
	}

	// Stop and clean up
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()

	err = elector1.Stop(stopCtx)
	if err != nil {
		t.Errorf("failed to stop elector1: %v", err)
	}

	err = elector2.Stop(stopCtx)
	if err != nil {
		t.Errorf("failed to stop elector2: %v", err)
	}

	client.Del(ctx, "test:leader:multi-node")
}

func TestRedisElector_LeadershipTransition(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	if client == nil {
		return
	}
	defer client.Close()

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	// Clean up
	ctx := context.Background()
	client.Del(ctx, "test:leader:transition")

	// Create first elector with short TTL
	config1 := &leaderelection.Config{
		TTL:             1 * time.Second,
		RenewalInterval: 300 * time.Millisecond,
		NodeID:          "node-1",
	}

	elector1, err := leaderelection.NewRedisElector(client, log, "test:leader:transition", config1)
	if err != nil {
		t.Fatalf("failed to create elector1: %v", err)
	}

	startCtx, startCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer startCancel()

	// Start first elector
	err = elector1.Start(startCtx)
	if err != nil {
		t.Fatalf("failed to start elector1: %v", err)
	}

	// Wait for leadership
	time.Sleep(200 * time.Millisecond)

	if !elector1.IsLeader() {
		t.Error("elector1 should be leader")
	}

	// Stop first elector (simulates failure)
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()

	err = elector1.Stop(stopCtx)
	if err != nil {
		t.Errorf("failed to stop elector1: %v", err)
	}

	// Create second elector
	config2 := &leaderelection.Config{
		TTL:             2 * time.Second,
		RenewalInterval: 500 * time.Millisecond,
		NodeID:          "node-2",
	}

	elector2, err := leaderelection.NewRedisElector(client, log, "test:leader:transition", config2)
	if err != nil {
		t.Fatalf("failed to create elector2: %v", err)
	}

	// Start second elector
	err = elector2.Start(startCtx)
	if err != nil {
		t.Fatalf("failed to start elector2: %v", err)
	}

	// Wait for leadership transition
	leadershipChan := elector2.LeadershipChannel()

	select {
	case isLeader := <-leadershipChan:
		if !isLeader {
			t.Error("elector2 should gain leadership")
		}
	case <-time.After(3 * time.Second):
		t.Error("timeout waiting for leadership transition")
	}

	// Verify new leader
	if !elector2.IsLeader() {
		t.Error("elector2 should be leader")
	}

	leaderID, err := elector2.GetLeaderID()
	if err != nil {
		t.Errorf("failed to get leader ID: %v", err)
	}

	if leaderID != "node-2" {
		t.Errorf("expected leader ID node-2, got %s", leaderID)
	}

	// Clean up
	stopCtx2, stopCancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel2()

	err = elector2.Stop(stopCtx2)
	if err != nil {
		t.Errorf("failed to stop elector2: %v", err)
	}

	client.Del(ctx, "test:leader:transition")
}

// =====================================
// ADVANCED EDGE CASE TESTS
// =====================================

func TestRedisElector_RenewalFailure(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	if client == nil {
		return
	}
	defer client.Close()

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	// Clean up
	ctx := context.Background()
	client.Del(ctx, "test:leader:renewal-failure")

	config := &leaderelection.Config{
		TTL:             500 * time.Millisecond, // Very short TTL
		RenewalInterval: 100 * time.Millisecond, // Fast renewal
		NodeID:          "test-node-renewal",
	}

	elector, err := leaderelection.NewRedisElector(client, log, "test:leader:renewal-failure", config)
	require.NoError(t, err)

	startCtx, startCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer startCancel()

	// Start election
	err = elector.Start(startCtx)
	require.NoError(t, err)

	// Wait for leadership acquisition
	leadershipChan := elector.LeadershipChannel()
	select {
	case isLeader := <-leadershipChan:
		assert.True(t, isLeader, "Should gain leadership")
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for leadership")
	}

	// Verify leadership
	assert.True(t, elector.IsLeader())

	// Simulate external interference - delete the key
	client.Del(ctx, "test:leader:renewal-failure")

	// Wait for leadership loss detection
	select {
	case isLeader := <-leadershipChan:
		assert.False(t, isLeader, "Should lose leadership")
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for leadership loss")
	}

	// Verify leadership loss
	assert.False(t, elector.IsLeader())

	// Clean up
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer stopCancel()

	err = elector.Stop(stopCtx)
	assert.NoError(t, err)

	client.Del(ctx, "test:leader:renewal-failure")
}

func TestRedisElector_ConcurrentElectors(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	if client == nil {
		return
	}
	defer client.Close()

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	// Clean up
	ctx := context.Background()
	client.Del(ctx, "test:leader:concurrent")

	const numElectors = 5

	electors := make([]*leaderelection.RedisElector, numElectors)

	config := &leaderelection.Config{
		TTL:             2 * time.Second,
		RenewalInterval: 500 * time.Millisecond,
	}

	// Create multiple electors
	for i := 0; i < numElectors; i++ {
		nodeConfig := *config
		nodeConfig.NodeID = fmt.Sprintf("node-%d", i)

		elector, err := leaderelection.NewRedisElector(client, log, "test:leader:concurrent", &nodeConfig)
		require.NoError(t, err)

		electors[i] = elector
	}

	startCtx, startCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer startCancel()

	// Start all electors simultaneously
	var wg sync.WaitGroup
	for i, elector := range electors {
		wg.Add(1)

		go func(idx int, e *leaderelection.RedisElector) {
			defer wg.Done()

			err := e.Start(startCtx)
			if err != nil {
				t.Errorf("Failed to start elector %d: %v", idx, err)
			}
		}(i, elector)
	}

	wg.Wait()

	// Give time for election
	time.Sleep(1 * time.Second)

	// Exactly one should be leader
	leaderCount := 0

	var leader *leaderelection.RedisElector

	for i, elector := range electors {
		if elector.IsLeader() {
			leaderCount++
			leader = elector

			t.Logf("Elector %d is leader", i)
		}
	}

	assert.Equal(t, 1, leaderCount, "Exactly one elector should be leader")
	require.NotNil(t, leader)

	// Verify leader ID
	leaderID, err := leader.GetLeaderID()
	assert.NoError(t, err)
	assert.NotEmpty(t, leaderID)

	// Stop all electors
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer stopCancel()

	for i, elector := range electors {
		err := elector.Stop(stopCtx)
		if err != nil {
			t.Errorf("Failed to stop elector %d: %v", i, err)
		}
	}

	client.Del(ctx, "test:leader:concurrent")
}

func TestRedisElector_StopWithoutStart(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	if client == nil {
		return
	}
	defer client.Close()

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	config := &leaderelection.Config{
		TTL:             2 * time.Second,
		RenewalInterval: 500 * time.Millisecond,
		NodeID:          "test-node-stop-without-start",
	}

	elector, err := leaderelection.NewRedisElector(client, log, "test:leader:stop-without-start", config)
	require.NoError(t, err)

	// Stop without starting should not error
	stopCtx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	err = elector.Stop(stopCtx)
	assert.NoError(t, err)
}

func TestRedisElector_MultipleStops(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	if client == nil {
		return
	}
	defer client.Close()

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	// Clean up
	ctx := context.Background()
	client.Del(ctx, "test:leader:multiple-stops")

	config := &leaderelection.Config{
		TTL:             2 * time.Second,
		RenewalInterval: 500 * time.Millisecond,
		NodeID:          "test-node-multiple-stops",
	}

	elector, err := leaderelection.NewRedisElector(client, log, "test:leader:multiple-stops", config)
	require.NoError(t, err)

	// Start elector
	startCtx, startCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer startCancel()

	err = elector.Start(startCtx)
	require.NoError(t, err)

	// Multiple stops should not error
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer stopCancel()

	err = elector.Stop(stopCtx)
	assert.NoError(t, err)

	// Second stop should also not error
	err = elector.Stop(stopCtx)
	assert.NoError(t, err)

	client.Del(ctx, "test:leader:multiple-stops")
}

func TestRedisElector_ContextCancellation(t *testing.T) {
	client := skipIfRedisUnavailable(t)
	if client == nil {
		return
	}
	defer client.Close()

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	// Clean up
	ctx := context.Background()
	client.Del(ctx, "test:leader:context-cancel")

	config := &leaderelection.Config{
		TTL:             2 * time.Second,
		RenewalInterval: 200 * time.Millisecond,
		NodeID:          "test-node-context-cancel",
	}

	elector, err := leaderelection.NewRedisElector(client, log, "test:leader:context-cancel", config)
	require.NoError(t, err)

	// Start with context that will be cancelled
	startCtx, startCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer startCancel()

	err = elector.Start(startCtx)
	require.NoError(t, err)

	// Wait for context cancellation
	<-startCtx.Done()

	// Stop should still work after context cancellation
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer stopCancel()

	err = elector.Stop(stopCtx)
	assert.NoError(t, err)

	client.Del(ctx, "test:leader:context-cancel")
}

func TestRedisElector_InvalidRedisAddress(t *testing.T) {
	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	// Client with invalid address
	client := redis.NewClient(&redis.Options{
		Addr: "invalid:9999",
		DB:   15,
	})
	defer client.Close()

	config := &leaderelection.Config{
		TTL:             2 * time.Second,
		RenewalInterval: 500 * time.Millisecond,
		NodeID:          "test-node-invalid",
	}

	elector, err := leaderelection.NewRedisElector(client, log, "test:leader:invalid", config)
	require.NoError(t, err)

	// Should be able to start even with invalid Redis
	startCtx, startCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer startCancel()

	err = elector.Start(startCtx)
	assert.NoError(t, err) // Start should succeed

	// Operations should handle connection errors gracefully
	isLeader := elector.IsLeader()
	assert.False(t, isLeader)

	leaderID, err := elector.GetLeaderID()
	assert.Error(t, err) // Should error due to connection failure
	assert.Empty(t, leaderID)

	// Stop should work even with connection issues
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer stopCancel()

	err = elector.Stop(stopCtx)
	assert.NoError(t, err)
}
