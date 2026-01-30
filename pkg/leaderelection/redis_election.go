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
	"github.com/redis/go-redis/v9"
	"github.com/sirupsen/logrus"
)

// RedisElector implements leader election using Redis.
type RedisElector struct {
	client  *redis.Client
	log     logrus.FieldLogger
	config  *Config
	nodeID  string
	keyName string
	network string // Network name for metrics

	mu                  sync.RWMutex
	isLeader            bool
	leadershipStartTime time.Time
	stopped             bool

	// Callback-based notification (guaranteed delivery)
	callbacksMu sync.RWMutex
	callbacks   []LeadershipCallback

	leadershipChan chan bool
	stopChan       chan struct{}
	wg             sync.WaitGroup
}

// NewRedisElector creates a new Redis-based leader elector.
func NewRedisElector(client *redis.Client, log logrus.FieldLogger, keyName string, config *Config) (*RedisElector, error) {
	if config == nil {
		config = DefaultConfig()
	}

	// Generate random node ID if not provided
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

	return &RedisElector{
		client:         client,
		log:            log.WithField("component", "leader-election").WithField("node_id", nodeID),
		config:         config,
		nodeID:         nodeID,
		keyName:        keyName,
		network:        network,
		leadershipChan: make(chan bool, 10),
		stopChan:       make(chan struct{}),
	}, nil
}

// Start begins the leader election process.
func (e *RedisElector) Start(ctx context.Context) error {
	e.log.Info("Starting leader election")

	// Initialize metrics
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

	// Release leadership if we have it
	if e.IsLeader() {
		// Record final leadership duration
		e.mu.RLock()
		leadershipDuration := time.Since(e.leadershipStartTime).Seconds()
		e.mu.RUnlock()

		common.LeaderElectionDuration.WithLabelValues(e.network, e.nodeID).Observe(leadershipDuration)
		common.LeaderElectionStatus.WithLabelValues(e.network, e.nodeID).Set(0)

		if err := e.releaseLeadership(ctx); err != nil {
			e.log.WithError(err).Error("Failed to release leadership on stop")
			common.LeaderElectionErrors.WithLabelValues(e.network, e.nodeID, "release").Inc()
		}
	}

	close(e.leadershipChan)

	return nil
}

// IsLeader returns true if this node is currently the leader.
func (e *RedisElector) IsLeader() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()

	return e.isLeader
}

// LeadershipChannel returns a channel that receives leadership changes.
func (e *RedisElector) LeadershipChannel() <-chan bool {
	return e.leadershipChan
}

// GetLeaderID returns the current leader's ID.
func (e *RedisElector) GetLeaderID() (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	val, err := e.client.Get(ctx, e.keyName).Result()
	if err == redis.Nil {
		return "", fmt.Errorf("no leader elected")
	}

	if err != nil {
		return "", fmt.Errorf("failed to get leader ID: %w", err)
	}

	return val, nil
}

// OnLeadershipChange registers a callback for guaranteed leadership notification.
// The callback is invoked synchronously when leadership status changes.
// Multiple callbacks can be registered and will be invoked in registration order.
func (e *RedisElector) OnLeadershipChange(callback LeadershipCallback) {
	e.callbacksMu.Lock()
	defer e.callbacksMu.Unlock()

	e.callbacks = append(e.callbacks, callback)
}

// notifyLeadershipChange invokes all registered callbacks with the leadership status.
// Callbacks are invoked synchronously in registration order.
func (e *RedisElector) notifyLeadershipChange(ctx context.Context, isLeader bool) {
	e.callbacksMu.RLock()
	callbacks := make([]LeadershipCallback, len(e.callbacks))
	copy(callbacks, e.callbacks)
	e.callbacksMu.RUnlock()

	for _, callback := range callbacks {
		callback(ctx, isLeader)
	}
}

// run is the main election loop.
func (e *RedisElector) run(ctx context.Context) {
	defer e.wg.Done()

	ticker := time.NewTicker(e.config.RenewalInterval)
	defer ticker.Stop()

	// Try to acquire leadership immediately
	if e.tryAcquireLeadership(ctx) {
		e.handleLeadershipGain(ctx)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-e.stopChan:
			return
		case <-ticker.C:
			if e.IsLeader() {
				// Try to renew leadership
				if !e.renewLeadership(ctx) {
					e.handleLeadershipLoss(ctx)
				}
			} else {
				// Try to acquire leadership
				if e.tryAcquireLeadership(ctx) {
					e.handleLeadershipGain(ctx)
				}
			}
		}
	}
}

// tryAcquireLeadership attempts to become the leader.
func (e *RedisElector) tryAcquireLeadership(ctx context.Context) bool {
	e.log.WithFields(logrus.Fields{
		"key": e.keyName,
		"ttl": e.config.TTL,
	}).Debug("Attempting to acquire leadership")

	// Use SET with NX (only set if not exists) and PX (expire in milliseconds)
	ok, err := e.client.SetNX(ctx, e.keyName, e.nodeID, e.config.TTL).Result()
	if err != nil {
		e.log.WithError(err).Error("Failed to acquire leadership")

		common.LeaderElectionErrors.WithLabelValues(e.network, e.nodeID, "acquire").Inc()

		return false
	}

	if ok {
		e.mu.Lock()
		e.isLeader = true
		e.leadershipStartTime = time.Now()
		e.mu.Unlock()

		common.LeaderElectionStatus.WithLabelValues(e.network, e.nodeID).Set(1)
		common.LeaderElectionTransitions.WithLabelValues(e.network, e.nodeID, "gained").Inc()

		e.log.Info("Acquired leadership")

		return true
	}

	// Check who is the current leader
	currentLeader, _ := e.client.Get(ctx, e.keyName).Result()
	e.log.WithFields(logrus.Fields{
		"current_leader": currentLeader,
		"our_node_id":    e.nodeID,
	}).Debug("Failed to acquire leadership, another node is leader")

	return false
}

// renewLeadership attempts to extend the leadership lock.
func (e *RedisElector) renewLeadership(ctx context.Context) bool {
	// Lua script to atomically check ownership and extend TTL
	script := `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("pexpire", KEYS[1], ARGV[2])
		else
			return 0
		end
	`

	result, err := e.client.Eval(ctx, script, []string{e.keyName}, e.nodeID, e.config.TTL.Milliseconds()).Result()
	if err != nil {
		e.log.WithError(err).Error("Failed to renew leadership")

		common.LeaderElectionErrors.WithLabelValues(e.network, e.nodeID, "renew").Inc()

		return false
	}

	val, ok := result.(int64)
	if !ok {
		e.log.WithError(fmt.Errorf("failed to renew leadership: %w", err)).Error("Failed to renew leadership")

		common.LeaderElectionErrors.WithLabelValues(e.network, e.nodeID, "renew").Inc()

		return false
	}

	success := val == 1

	if !success {
		e.log.Warn("Failed to renew leadership - lock not owned by this node")

		common.LeaderElectionErrors.WithLabelValues(e.network, e.nodeID, "renew").Inc()
	}

	return success
}

// releaseLeadership voluntarily gives up leadership.
func (e *RedisElector) releaseLeadership(ctx context.Context) error {
	// Lua script to atomically check ownership and delete
	script := `
		if redis.call("get", KEYS[1]) == ARGV[1] then
			return redis.call("del", KEYS[1])
		else
			return 0
		end
	`

	result, err := e.client.Eval(ctx, script, []string{e.keyName}, e.nodeID).Result()
	if err != nil {
		return fmt.Errorf("failed to release leadership: %w", err)
	}

	val, ok := result.(int64)
	if !ok {
		return fmt.Errorf("failed to release leadership: %w", err)
	}

	if val == 0 {
		e.log.Warn("Could not release leadership - lock not owned by this node")
	} else {
		e.log.Info("Released leadership")
	}

	e.mu.Lock()
	e.isLeader = false
	e.mu.Unlock()

	return nil
}

// handleLeadershipGain is called when leadership is acquired.
func (e *RedisElector) handleLeadershipGain(ctx context.Context) {
	e.log.Info("Gained leadership")

	// Notify callbacks first (guaranteed delivery)
	e.notifyLeadershipChange(ctx, true)

	// Send to channel for backward compatibility (best-effort)
	select {
	case e.leadershipChan <- true:
	default:
		e.log.Warn("Leadership channel full, dropping leadership gain event")
	}
}

// handleLeadershipLoss is called when leadership is lost.
func (e *RedisElector) handleLeadershipLoss(ctx context.Context) {
	e.mu.Lock()
	wasLeader := e.isLeader
	e.isLeader = false
	leadershipDuration := time.Since(e.leadershipStartTime).Seconds()
	e.mu.Unlock()

	if wasLeader {
		common.LeaderElectionStatus.WithLabelValues(e.network, e.nodeID).Set(0)
		common.LeaderElectionTransitions.WithLabelValues(e.network, e.nodeID, "lost").Inc()
		common.LeaderElectionDuration.WithLabelValues(e.network, e.nodeID).Observe(leadershipDuration)
	}

	e.log.Info("Lost leadership")

	// Notify callbacks first (guaranteed delivery)
	e.notifyLeadershipChange(ctx, false)

	// Send to channel for backward compatibility (best-effort)
	select {
	case e.leadershipChan <- false:
	default:
		e.log.Warn("Leadership channel full, dropping leadership loss event")
	}
}
