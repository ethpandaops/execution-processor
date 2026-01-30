package leaderelection

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/go-redsync/redsync/v4"
	"github.com/go-redsync/redsync/v4/redis/goredis/v9"
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// RedisElector implements leader election using Redis with redsync.
type RedisElector struct {
	rs      *redsync.Redsync
	mutex   *redsync.Mutex
	log     logrus.FieldLogger
	config  *Config
	nodeID  string
	keyName string
	network string

	mu                  sync.RWMutex
	isLeader            bool
	leadershipStartTime time.Time
	stopped             bool

	callbacksMu sync.RWMutex
	callbacks   []LeadershipCallback

	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewRedisElector creates a new Redis-based leader elector using redsync.
func NewRedisElector(
	client *redis.Client,
	log logrus.FieldLogger,
	keyName string,
	config *Config,
) (*RedisElector, error) {
	if config == nil {
		config = DefaultConfig()
	}

	nodeID := config.NodeID
	if nodeID == "" {
		bytes := make([]byte, 16)
		if _, err := rand.Read(bytes); err != nil {
			return nil, fmt.Errorf("failed to generate node ID: %w", err)
		}

		nodeID = hex.EncodeToString(bytes)
	}

	// Extract network name from key (format: "execution-processor:leader:network_name")
	parts := strings.Split(keyName, ":")
	network := "unknown"

	if len(parts) >= 3 {
		network = parts[2]
	}

	pool := goredis.NewPool(client)

	return &RedisElector{
		rs:       redsync.New(pool),
		log:      log.WithField("component", "leader-election").WithField("node_id", nodeID),
		config:   config,
		nodeID:   nodeID,
		keyName:  keyName,
		network:  network,
		stopChan: make(chan struct{}),
	}, nil
}

// Start begins the leader election process.
func (e *RedisElector) Start(ctx context.Context) error {
	e.log.Info("Starting leader election")

	common.LeaderElectionStatus.WithLabelValues(e.network, e.nodeID).Set(0)

	e.wg.Add(1)

	go e.run(ctx)

	return nil
}

// Stop gracefully stops the leader election.
func (e *RedisElector) Stop(ctx context.Context) error {
	e.mu.Lock()

	if e.stopped {
		e.mu.Unlock()

		return nil
	}

	e.stopped = true
	e.mu.Unlock()

	e.log.Info("Stopping leader election")

	close(e.stopChan)
	e.wg.Wait()

	if e.IsLeader() {
		e.mu.RLock()
		duration := time.Since(e.leadershipStartTime).Seconds()
		e.mu.RUnlock()

		common.LeaderElectionDuration.WithLabelValues(e.network, e.nodeID).Observe(duration)
		common.LeaderElectionStatus.WithLabelValues(e.network, e.nodeID).Set(0)

		if _, err := e.mutex.UnlockContext(ctx); err != nil {
			e.log.WithError(err).Error("Failed to release leadership on stop")
			common.LeaderElectionErrors.WithLabelValues(e.network, e.nodeID, "release").Inc()
		}
	}

	return nil
}

// IsLeader returns true if this node is currently the leader.
func (e *RedisElector) IsLeader() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.isLeader
}

// OnLeadershipChange registers a callback for guaranteed leadership notification.
func (e *RedisElector) OnLeadershipChange(callback LeadershipCallback) {
	e.callbacksMu.Lock()
	defer e.callbacksMu.Unlock()

	e.callbacks = append(e.callbacks, callback)
}

// run is the main election loop.
func (e *RedisElector) run(ctx context.Context) {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.RenewalInterval)
	defer ticker.Stop()

	if e.tryAcquire(ctx) {
		e.handleGain(ctx)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.stopChan:
			return
		case <-ticker.C:
			if e.IsLeader() {
				if !e.tryExtend(ctx) {
					e.handleLoss(ctx)
				}
			} else {
				if e.tryAcquire(ctx) {
					e.handleGain(ctx)
				}
			}
		}
	}
}

// tryAcquire attempts to acquire the leadership lock.
func (e *RedisElector) tryAcquire(ctx context.Context) bool {
	e.mutex = e.rs.NewMutex(
		e.keyName,
		redsync.WithExpiry(e.config.TTL),
		redsync.WithTries(1),
		redsync.WithDriftFactor(0.01),
		redsync.WithSetNXOnExtend(),
	)

	if err := e.mutex.TryLockContext(ctx); err != nil {
		e.log.WithError(err).Info("Failed to acquire leadership")

		return false
	}

	e.mu.Lock()
	e.isLeader = true
	e.leadershipStartTime = time.Now()
	e.mu.Unlock()

	common.LeaderElectionStatus.WithLabelValues(e.network, e.nodeID).Set(1)
	common.LeaderElectionTransitions.WithLabelValues(e.network, e.nodeID, "gained").Inc()
	e.log.Info("Acquired leadership")

	return true
}

// tryExtend attempts to extend the leadership lock TTL.
func (e *RedisElector) tryExtend(ctx context.Context) bool {
	ok, err := e.mutex.ExtendContext(ctx)
	if err != nil || !ok {
		e.log.WithError(err).Warn("Failed to extend leadership")
		common.LeaderElectionErrors.WithLabelValues(e.network, e.nodeID, "renew").Inc()

		return false
	}

	return true
}

// handleGain is called when leadership is acquired.
func (e *RedisElector) handleGain(ctx context.Context) {
	e.callbacksMu.RLock()
	callbacks := make([]LeadershipCallback, len(e.callbacks))
	copy(callbacks, e.callbacks)
	e.callbacksMu.RUnlock()

	for _, cb := range callbacks {
		cb(ctx, true)
	}
}

// handleLoss is called when leadership is lost.
func (e *RedisElector) handleLoss(ctx context.Context) {
	e.mu.Lock()
	wasLeader := e.isLeader
	e.isLeader = false
	duration := time.Since(e.leadershipStartTime).Seconds()
	e.mu.Unlock()

	if wasLeader {
		common.LeaderElectionStatus.WithLabelValues(e.network, e.nodeID).Set(0)
		common.LeaderElectionTransitions.WithLabelValues(e.network, e.nodeID, "lost").Inc()
		common.LeaderElectionDuration.WithLabelValues(e.network, e.nodeID).Observe(duration)
	}

	e.log.Info("Lost leadership")

	e.callbacksMu.RLock()
	callbacks := make([]LeadershipCallback, len(e.callbacks))
	copy(callbacks, e.callbacks)
	e.callbacksMu.RUnlock()

	for _, cb := range callbacks {
		cb(ctx, false)
	}
}
