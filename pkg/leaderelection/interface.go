package leaderelection

import (
	"context"
	"time"
)

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
	LeadershipChannel() <-chan bool

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
