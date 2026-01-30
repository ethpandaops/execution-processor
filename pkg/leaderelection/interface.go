package leaderelection

import (
	"context"
	"time"
)

// LeadershipCallback is a function invoked when leadership status changes.
// The callback is invoked synchronously - implementations should return quickly (< 100ms)
// to avoid delaying leadership renewal. Long-running operations should be spawned
// in a separate goroutine.
type LeadershipCallback func(ctx context.Context, isLeader bool)

// Elector defines the interface for leader election implementations.
type Elector interface {
	// Start begins the leader election process
	Start(ctx context.Context) error

	// Stop gracefully stops the leader election
	Stop(ctx context.Context) error

	// IsLeader returns true if this node is currently the leader
	IsLeader() bool

	// LeadershipChannel returns a channel that receives leadership changes
	// true = gained leadership, false = lost leadership
	//
	// Deprecated: Use OnLeadershipChange for guaranteed delivery.
	// This channel may drop events if the buffer is full.
	LeadershipChannel() <-chan bool

	// OnLeadershipChange registers a callback for guaranteed leadership notification.
	// The callback is invoked synchronously when leadership status changes.
	// Multiple callbacks can be registered and will be invoked in registration order.
	//
	// Important: Keep callbacks fast (< 100ms) to avoid delaying leadership renewal.
	// For long-running operations, spawn a goroutine within the callback.
	OnLeadershipChange(callback LeadershipCallback)

	// GetLeaderID returns the current leader's ID
	GetLeaderID() (string, error)
}

// Config holds configuration for leader election.
type Config struct {
	// TTL is the time-to-live for the leader lock
	TTL time.Duration

	// RenewalInterval is how often to renew the leader lock
	RenewalInterval time.Duration

	// NodeID is the unique identifier for this node
	// If empty, a random ID will be generated
	NodeID string
}

// DefaultConfig returns a default configuration.
func DefaultConfig() *Config {
	return &Config{
		TTL:             10 * time.Second,
		RenewalInterval: 3 * time.Second,
		NodeID:          "", // Will be generated
	}
}
