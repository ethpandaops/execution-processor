package leaderelection_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ethpandaops/execution-processor/pkg/leaderelection"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTestRedis creates an in-memory Redis server for testing.
// The server and client are automatically cleaned up when the test completes.
func newTestRedis(t *testing.T) *redis.Client {
	t.Helper()

	s := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})

	t.Cleanup(func() {
		_ = client.Close()
	})

	return client
}

// =====================================
// BASIC FUNCTIONALITY TESTS
// =====================================

func TestNewRedisElector(t *testing.T) {
	client := newTestRedis(t)

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
	client := newTestRedis(t)

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
	client := newTestRedis(t)

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
	client := newTestRedis(t)

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
	client := newTestRedis(t)

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
	client := newTestRedis(t)

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
	client := newTestRedis(t)

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
	client := newTestRedis(t)

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
	client := newTestRedis(t)

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
	client := newTestRedis(t)

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
	client := newTestRedis(t)

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

// =====================================
// CHANNEL OVERFLOW / GUARANTEED DELIVERY TESTS
// =====================================

// TestLeadershipChannelOverflow_LossEventDelivery tests that leadership loss events
// are NOT dropped when the channel buffer is full.
//
// This test should FAIL with the current implementation because:
// 1. The leadership channel has a buffer of 10.
// 2. When the buffer is full, events are silently dropped with a warning log.
// 3. A consumer that is slow (or blocked) can miss critical leadership loss events.
// 4. This causes the consumer to think it's still leader when it's not.
func TestLeadershipChannelOverflow_LossEventDelivery(t *testing.T) {
	client := newTestRedis(t)

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel) // Show warnings about dropped events

	ctx := context.Background()
	client.Del(ctx, "test:leader:overflow")

	config := &leaderelection.Config{
		TTL:             500 * time.Millisecond,
		RenewalInterval: 100 * time.Millisecond,
		NodeID:          "overflow-test-node",
	}

	elector, err := leaderelection.NewRedisElector(client, log, "test:leader:overflow", config)
	require.NoError(t, err)

	// Start the elector
	startCtx, startCancel := context.WithCancel(context.Background())
	defer startCancel()

	err = elector.Start(startCtx)
	require.NoError(t, err)

	// Wait for initial leadership gain
	leadershipChan := elector.LeadershipChannel()

	select {
	case isLeader := <-leadershipChan:
		require.True(t, isLeader, "Should gain leadership initially")
	case <-time.After(2 * time.Second):
		t.Fatal("Timeout waiting for initial leadership")
	}

	// Verify we're leader
	require.True(t, elector.IsLeader(), "Should be leader after gaining leadership")

	// Now simulate a slow consumer by NOT reading from the channel
	// We'll set a different node as leader to force a leadership loss event
	// The loss event should be delivered even if we haven't been reading

	// Set a different node as leader to simulate another node taking over
	client.Set(ctx, "test:leader:overflow", "different-node", config.TTL)

	// Wait for the renewal cycle to detect the loss
	// With 100ms renewal interval, this should happen within 200ms
	time.Sleep(300 * time.Millisecond)

	// At this point, IsLeader() should return false (internal state updated)
	assert.False(t, elector.IsLeader(), "IsLeader() should return false after another node took over")

	// CRITICAL: The leadership loss event MUST be available on the channel
	// If it was dropped, this will timeout and the test fails
	select {
	case isLeader := <-leadershipChan:
		assert.False(t, isLeader, "Should receive leadership LOSS event")
	case <-time.After(1 * time.Second):
		t.Fatal("CRITICAL: Leadership loss event was DROPPED - consumer never notified!")
	}

	// Cleanup
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer stopCancel()

	_ = elector.Stop(stopCtx)
}

// TestLeadershipChannelOverflow_BufferExhaustion specifically tests what happens
// when the channel buffer (size 10) is completely filled.
//
// This simulates rapid leadership transitions that exhaust the buffer,
// causing subsequent events to be dropped.
func TestLeadershipChannelOverflow_BufferExhaustion(t *testing.T) {
	client := newTestRedis(t)

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	ctx := context.Background()
	keyName := "test:leader:buffer-exhaust"
	client.Del(ctx, keyName)

	// Very fast renewal to generate many events
	config := &leaderelection.Config{
		TTL:             200 * time.Millisecond,
		RenewalInterval: 50 * time.Millisecond,
		NodeID:          "buffer-test-node",
	}

	elector, err := leaderelection.NewRedisElector(client, log, keyName, config)
	require.NoError(t, err)

	startCtx, startCancel := context.WithCancel(context.Background())
	defer startCancel()

	err = elector.Start(startCtx)
	require.NoError(t, err)

	// DON'T read from the channel - simulate a completely blocked consumer
	// This will cause the buffer to fill up

	// Rapidly toggle leadership by deleting and letting it re-acquire
	// Each cycle generates gain+loss events
	for range 15 { // More than buffer size (10)
		client.Del(ctx, keyName)
		time.Sleep(60 * time.Millisecond) // Let it detect loss
		// It will auto-reacquire on next renewal tick
		time.Sleep(60 * time.Millisecond)
	}

	// Now delete one final time to ensure we're not leader
	client.Del(ctx, keyName)
	time.Sleep(100 * time.Millisecond)

	// Internal state should show not leader
	finalIsLeader := elector.IsLeader()

	// Count how many events we can actually read from the channel
	leadershipChan := elector.LeadershipChannel()
	eventCount := 0

	var lastEvent *bool

drainLoop:
	for {
		select {
		case isLeader, ok := <-leadershipChan:
			if !ok {
				break drainLoop
			}

			eventCount++
			lastEvent = &isLeader
		default:
			break drainLoop
		}
	}

	t.Logf("Events in buffer: %d, Final IsLeader(): %v", eventCount, finalIsLeader)

	// CRITICAL ASSERTION:
	// If the final state is "not leader", the last event we receive MUST be "false"
	// If it's not, events were dropped and state is inconsistent
	if !finalIsLeader {
		if lastEvent == nil {
			t.Fatal("CRITICAL: IsLeader()=false but NO events in channel - consumer has no way to know!")
		}

		if *lastEvent {
			t.Fatal("CRITICAL: IsLeader()=false but last channel event was 'true' - events were dropped!")
		}
	}

	// Cleanup
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer stopCancel()

	_ = elector.Stop(stopCtx)
}

// TestLeadershipChannelOverflow_SlowConsumer tests that a slow consumer
// eventually receives all leadership events without any being dropped.
func TestLeadershipChannelOverflow_SlowConsumer(t *testing.T) {
	client := newTestRedis(t)

	log := logrus.New()
	log.SetLevel(logrus.WarnLevel)

	ctx := context.Background()
	keyName := "test:leader:slow-consumer"
	client.Del(ctx, keyName)

	config := &leaderelection.Config{
		TTL:             300 * time.Millisecond,
		RenewalInterval: 100 * time.Millisecond,
		NodeID:          "slow-consumer-node",
	}

	elector, err := leaderelection.NewRedisElector(client, log, keyName, config)
	require.NoError(t, err)

	startCtx, startCancel := context.WithCancel(context.Background())
	defer startCancel()

	err = elector.Start(startCtx)
	require.NoError(t, err)

	leadershipChan := elector.LeadershipChannel()

	// Track events received by a slow consumer
	var (
		eventsReceived []bool
		eventsMu       sync.Mutex
		consumerDone   = make(chan struct{})
	)

	// Start a SLOW consumer that takes 200ms to process each event
	go func() {
		defer close(consumerDone)

		for {
			select {
			case isLeader, ok := <-leadershipChan:
				if !ok {
					return
				}

				eventsMu.Lock()

				eventsReceived = append(eventsReceived, isLeader)

				eventsMu.Unlock()

				time.Sleep(200 * time.Millisecond) // Slow processing
			case <-startCtx.Done():
				return
			}
		}
	}()

	// Wait for initial leadership
	time.Sleep(200 * time.Millisecond)
	require.True(t, elector.IsLeader(), "Should be leader")

	// Trigger leadership loss by setting a different node as leader
	client.Set(ctx, keyName, "different-node", config.TTL)
	time.Sleep(200 * time.Millisecond)

	// Should have lost leadership
	assert.False(t, elector.IsLeader(), "Should have lost leadership")

	// Give slow consumer time to catch up
	time.Sleep(500 * time.Millisecond)

	// Stop and wait for consumer
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer stopCancel()

	_ = elector.Stop(stopCtx)

	startCancel()
	<-consumerDone

	// Check that we received the loss event
	eventsMu.Lock()
	defer eventsMu.Unlock()

	t.Logf("Events received by slow consumer: %v", eventsReceived)

	// Must have received at least 2 events: gain (true) and loss (false)
	require.GreaterOrEqual(t, len(eventsReceived), 2,
		"Slow consumer should receive both gain and loss events")

	// The last event must be false (loss)
	lastEvent := eventsReceived[len(eventsReceived)-1]

	assert.False(t, lastEvent,
		"CRITICAL: Last event should be loss (false), but slow consumer missed it!")
}

// =====================================
// CALLBACK-BASED NOTIFICATION TESTS
// =====================================

// TestOnLeadershipChange_CallbackInvocation verifies that registered callbacks
// are invoked when leadership status changes.
func TestOnLeadershipChange_CallbackInvocation(t *testing.T) {
	client := newTestRedis(t)

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ctx := context.Background()
	keyName := "test:leader:callback-invocation"
	client.Del(ctx, keyName)

	config := &leaderelection.Config{
		TTL:             500 * time.Millisecond,
		RenewalInterval: 100 * time.Millisecond,
		NodeID:          "callback-test-node",
	}

	elector, err := leaderelection.NewRedisElector(client, log, keyName, config)
	require.NoError(t, err)

	// Track callback invocations
	var (
		callbackInvocations []bool
		callbackMu          sync.Mutex
	)

	elector.OnLeadershipChange(func(_ context.Context, isLeader bool) {
		callbackMu.Lock()
		defer callbackMu.Unlock()

		callbackInvocations = append(callbackInvocations, isLeader)
	})

	startCtx, startCancel := context.WithCancel(context.Background())
	defer startCancel()

	err = elector.Start(startCtx)
	require.NoError(t, err)

	// Wait for leadership acquisition
	time.Sleep(200 * time.Millisecond)

	// Should have gained leadership
	require.True(t, elector.IsLeader(), "Should be leader")

	// Force leadership loss by setting a different node as leader
	// This prevents the current node from re-acquiring
	client.Set(ctx, keyName, "different-node", config.TTL)
	time.Sleep(300 * time.Millisecond) // Wait for renewal cycle to detect loss

	// Should have lost leadership
	require.False(t, elector.IsLeader(), "Should have lost leadership")

	// Check callback invocations
	callbackMu.Lock()
	defer callbackMu.Unlock()

	require.GreaterOrEqual(t, len(callbackInvocations), 2,
		"Should have at least 2 callback invocations (gain + loss)")

	// First should be gain (true), last should be loss (false)
	assert.True(t, callbackInvocations[0], "First callback should be leadership gain")
	assert.False(t, callbackInvocations[len(callbackInvocations)-1], "Last callback should be leadership loss")

	// Cleanup
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer stopCancel()

	_ = elector.Stop(stopCtx)
}

// TestOnLeadershipChange_MultipleCallbacks verifies that multiple callbacks
// are all invoked in registration order.
func TestOnLeadershipChange_MultipleCallbacks(t *testing.T) {
	client := newTestRedis(t)

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ctx := context.Background()
	keyName := "test:leader:multiple-callbacks"
	client.Del(ctx, keyName)

	config := &leaderelection.Config{
		TTL:             500 * time.Millisecond,
		RenewalInterval: 100 * time.Millisecond,
		NodeID:          "multi-callback-node",
	}

	elector, err := leaderelection.NewRedisElector(client, log, keyName, config)
	require.NoError(t, err)

	// Track callback order
	var (
		invocationOrder []int
		orderMu         sync.Mutex
	)

	// Register multiple callbacks
	for i := range 3 {
		callbackID := i

		elector.OnLeadershipChange(func(_ context.Context, _ bool) {
			orderMu.Lock()
			defer orderMu.Unlock()

			invocationOrder = append(invocationOrder, callbackID)
		})
	}

	startCtx, startCancel := context.WithCancel(context.Background())
	defer startCancel()

	err = elector.Start(startCtx)
	require.NoError(t, err)

	// Wait for leadership acquisition
	time.Sleep(200 * time.Millisecond)

	// Check invocation order
	orderMu.Lock()
	defer orderMu.Unlock()

	// Should have 3 invocations for the leadership gain
	require.GreaterOrEqual(t, len(invocationOrder), 3, "All 3 callbacks should be invoked")

	// First 3 should be in order 0, 1, 2
	assert.Equal(t, 0, invocationOrder[0], "First callback should be invoked first")
	assert.Equal(t, 1, invocationOrder[1], "Second callback should be invoked second")
	assert.Equal(t, 2, invocationOrder[2], "Third callback should be invoked third")

	// Cleanup
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer stopCancel()

	_ = elector.Stop(stopCtx)
}

// TestOnLeadershipChange_GuaranteedDelivery verifies that callbacks receive
// ALL leadership events, even when they would overflow a channel buffer.
func TestOnLeadershipChange_GuaranteedDelivery(t *testing.T) {
	client := newTestRedis(t)

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ctx := context.Background()
	keyName := "test:leader:callback-guaranteed"
	client.Del(ctx, keyName)

	// Very fast renewal to generate many events
	config := &leaderelection.Config{
		TTL:             200 * time.Millisecond,
		RenewalInterval: 50 * time.Millisecond,
		NodeID:          "guaranteed-callback-node",
	}

	elector, err := leaderelection.NewRedisElector(client, log, keyName, config)
	require.NoError(t, err)

	// Track ALL callback invocations
	var (
		callbackEvents []bool
		eventsMu       sync.Mutex
	)

	elector.OnLeadershipChange(func(_ context.Context, isLeader bool) {
		eventsMu.Lock()
		defer eventsMu.Unlock()

		callbackEvents = append(callbackEvents, isLeader)
	})

	startCtx, startCancel := context.WithCancel(context.Background())
	defer startCancel()

	err = elector.Start(startCtx)
	require.NoError(t, err)

	// Wait for initial leadership
	time.Sleep(100 * time.Millisecond)

	// Rapidly toggle leadership by deleting key multiple times
	// This generates more events than a channel buffer (10) could hold
	for range 15 {
		client.Del(ctx, keyName)
		time.Sleep(60 * time.Millisecond) // Let it detect loss
		time.Sleep(60 * time.Millisecond) // Let it re-acquire
	}

	// Final deletion to ensure not leader
	client.Del(ctx, keyName)
	time.Sleep(100 * time.Millisecond)

	// Check final state
	finalIsLeader := elector.IsLeader()

	eventsMu.Lock()
	defer eventsMu.Unlock()

	t.Logf("Callback events received: %d, Final IsLeader(): %v", len(callbackEvents), finalIsLeader)

	// CRITICAL: Callback should have received many events
	// More than the channel buffer size (10) proves guaranteed delivery
	assert.Greater(t, len(callbackEvents), 10,
		"Callbacks should receive more events than channel buffer size")

	// If final state is not leader, the last callback event must be false
	if !finalIsLeader && len(callbackEvents) > 0 {
		lastEvent := callbackEvents[len(callbackEvents)-1]
		assert.False(t, lastEvent,
			"CRITICAL: IsLeader()=false but last callback event was 'true' - events were lost!")
	}

	// Cleanup
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer stopCancel()

	_ = elector.Stop(stopCtx)
}

// TestOnLeadershipChange_SlowCallback verifies that slow callbacks work correctly
// (though they may delay leadership renewal).
func TestOnLeadershipChange_SlowCallback(t *testing.T) {
	client := newTestRedis(t)

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ctx := context.Background()
	keyName := "test:leader:slow-callback"
	client.Del(ctx, keyName)

	config := &leaderelection.Config{
		TTL:             1 * time.Second,
		RenewalInterval: 200 * time.Millisecond,
		NodeID:          "slow-callback-node",
	}

	elector, err := leaderelection.NewRedisElector(client, log, keyName, config)
	require.NoError(t, err)

	// Track callback invocations with a slow callback
	var (
		callbackEvents []bool
		eventsMu       sync.Mutex
	)

	elector.OnLeadershipChange(func(_ context.Context, isLeader bool) {
		// Slow callback - takes 150ms (less than renewal interval for safety)
		// This simulates a callback that does significant work
		time.Sleep(150 * time.Millisecond)

		eventsMu.Lock()
		defer eventsMu.Unlock()

		callbackEvents = append(callbackEvents, isLeader)
	})

	startCtx, startCancel := context.WithCancel(context.Background())
	defer startCancel()

	err = elector.Start(startCtx)
	require.NoError(t, err)

	// Wait for leadership (callback takes 150ms, so wait longer)
	time.Sleep(500 * time.Millisecond)

	// Should have gained leadership despite slow callback
	require.True(t, elector.IsLeader(), "Should be leader")

	// Force leadership loss by setting a different node as leader
	client.Set(ctx, keyName, "different-node", config.TTL)
	time.Sleep(600 * time.Millisecond) // Wait for renewal + slow callback

	// Should have lost leadership
	require.False(t, elector.IsLeader(), "Should have lost leadership")

	// Check callback invocations
	eventsMu.Lock()
	defer eventsMu.Unlock()

	require.GreaterOrEqual(t, len(callbackEvents), 2,
		"Should have at least 2 callback invocations despite slow callback")

	assert.True(t, callbackEvents[0], "First event should be leadership gain")
	assert.False(t, callbackEvents[len(callbackEvents)-1], "Last event should be leadership loss")

	// Cleanup
	stopCtx, stopCancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer stopCancel()

	_ = elector.Stop(stopCtx)
}

// TestLeadershipChannel_GuaranteedDelivery verifies that leadership loss events
// are ALWAYS delivered, even under contention.
//
// This is the contract that distributed systems depend on:
// If IsLeader() returns false, the consumer MUST have been notified.
func TestLeadershipChannel_GuaranteedDelivery(t *testing.T) {
	client := newTestRedis(t)

	log := logrus.New()
	log.SetLevel(logrus.ErrorLevel)

	ctx := context.Background()

	// Run multiple iterations to catch race conditions
	for iteration := range 10 {
		keyName := "test:leader:guaranteed"
		client.Del(ctx, keyName)

		config := &leaderelection.Config{
			TTL:             300 * time.Millisecond,
			RenewalInterval: 50 * time.Millisecond,
			NodeID:          "guaranteed-node",
		}

		elector, err := leaderelection.NewRedisElector(client, log, keyName, config)
		require.NoError(t, err)

		startCtx, startCancel := context.WithCancel(context.Background())

		err = elector.Start(startCtx)
		require.NoError(t, err)

		leadershipChan := elector.LeadershipChannel()

		// Track whether we've been notified of loss
		var notifiedOfLoss atomic.Bool

		// Consumer goroutine
		consumerDone := make(chan struct{})

		go func() {
			defer close(consumerDone)

			for {
				select {
				case isLeader, ok := <-leadershipChan:
					if !ok {
						return
					}

					if !isLeader {
						notifiedOfLoss.Store(true)
					}
				case <-startCtx.Done():
					return
				}
			}
		}()

		// Wait for leadership
		time.Sleep(100 * time.Millisecond)

		if elector.IsLeader() {
			// Force leadership loss
			client.Del(ctx, keyName)
			time.Sleep(150 * time.Millisecond)

			// If we're no longer leader, we MUST have been notified
			if !elector.IsLeader() {
				// Give consumer a moment to process
				time.Sleep(50 * time.Millisecond)

				if !notifiedOfLoss.Load() {
					t.Fatalf("CRITICAL (iteration %d): IsLeader()=false but consumer was NOT notified of loss!",
						iteration)
				}
			}
		}

		// Cleanup
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)

		_ = elector.Stop(stopCtx)

		stopCancel()
		startCancel()

		<-consumerDone

		client.Del(ctx, keyName)
	}
}
