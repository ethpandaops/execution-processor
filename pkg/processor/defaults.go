package processor

import "time"

// Default configuration values for the processor manager.
// Processor-specific defaults are in the tracker package.
const (
	// DefaultInterval is the default interval between processing cycles.
	DefaultInterval = 10 * time.Second

	// DefaultConcurrency is the default number of concurrent workers for task processing.
	DefaultConcurrency = 20

	// DefaultMaxProcessQueue is the default max queue size for asynq.
	DefaultMaxProcessQueue = 1000

	// DefaultBackpressureHysteresis is the default hysteresis factor for backpressure.
	// When backpressure is triggered, it won't be released until queue size drops
	// to this fraction of the threshold.
	DefaultBackpressureHysteresis = 0.8

	// DefaultLeaderTTL is the default TTL for leader election locks.
	DefaultLeaderTTL = 10 * time.Second

	// DefaultLeaderRenewalInterval is the default renewal interval for leader election.
	DefaultLeaderRenewalInterval = 3 * time.Second
)
