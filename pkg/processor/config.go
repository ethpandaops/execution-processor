package processor

import (
	"fmt"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/processor/tracker"
	"github.com/ethpandaops/execution-processor/pkg/processor/transaction/simple"
	"github.com/ethpandaops/execution-processor/pkg/processor/transaction/structlog"
	"github.com/ethpandaops/execution-processor/pkg/processor/transaction/structlog_agg"
)

// Config holds the unified processor configuration.
type Config struct {
	// Processing interval
	Interval time.Duration `yaml:"interval"`

	// Processing mode: forwards, backwards
	Mode string `yaml:"mode"`

	// Maximum concurrent transactions to process
	Concurrency int `yaml:"concurrency"`

	// Leader election configuration
	LeaderElection LeaderElectionConfig `yaml:"leaderElection"`

	// Queue control configuration
	MaxProcessQueueSize    int     `yaml:"maxProcessQueueSize"`
	BackpressureHysteresis float64 `yaml:"backpressureHysteresis"`

	// Processor configurations
	TransactionStructlog    structlog.Config     `yaml:"transactionStructlog"`
	TransactionSimple       simple.Config        `yaml:"transactionSimple"`
	TransactionStructlogAgg structlog_agg.Config `yaml:"transactionStructlogAgg"`
}

// LeaderElectionConfig holds configuration for leader election.
type LeaderElectionConfig struct {
	// Enable leader election (default: true)
	Enabled bool `yaml:"enabled"`

	// TTL for leader lock (default: 10s)
	TTL time.Duration `yaml:"ttl"`

	// Renewal interval (default: 3s)
	RenewalInterval time.Duration `yaml:"renewalInterval"`

	// Optional node ID (auto-generated if empty)
	NodeID string `yaml:"nodeId"`
}

// WorkerConfig holds worker configuration.
type WorkerConfig struct {
	Enabled     bool `yaml:"enabled"`
	Concurrency int  `yaml:"concurrency"`
}

func (c *Config) Validate() error {
	// Interval 0 = no delay (default), >0 = fixed interval between processing cycles
	if c.Mode == "" {
		c.Mode = tracker.FORWARDS_MODE
	}

	if c.Mode != tracker.FORWARDS_MODE && c.Mode != tracker.BACKWARDS_MODE {
		return fmt.Errorf("invalid mode %s, must be '%s' or '%s'", c.Mode, tracker.FORWARDS_MODE, tracker.BACKWARDS_MODE)
	}

	if c.Concurrency == 0 {
		c.Concurrency = DefaultConcurrency
	}

	// Queue control defaults
	if c.MaxProcessQueueSize == 0 {
		c.MaxProcessQueueSize = DefaultMaxProcessQueue
	}

	if c.BackpressureHysteresis == 0 {
		c.BackpressureHysteresis = DefaultBackpressureHysteresis
	}

	// Set leader election defaults
	// Enable by default unless explicitly disabled
	if !c.LeaderElection.Enabled && c.LeaderElection.TTL == 0 && c.LeaderElection.RenewalInterval == 0 {
		c.LeaderElection.Enabled = true
	}

	if c.LeaderElection.TTL == 0 {
		c.LeaderElection.TTL = DefaultLeaderTTL
	}

	if c.LeaderElection.RenewalInterval == 0 {
		c.LeaderElection.RenewalInterval = DefaultLeaderRenewalInterval
	}

	// Validate leader election settings
	if c.LeaderElection.RenewalInterval >= c.LeaderElection.TTL {
		return fmt.Errorf("leader election renewal interval must be less than TTL")
	}

	if c.TransactionStructlog.Enabled {
		if c.TransactionStructlog.Addr == "" {
			return fmt.Errorf("transaction structlog addr is required when enabled")
		}

		if c.TransactionStructlog.Table == "" {
			return fmt.Errorf("transaction structlog table is required when enabled")
		}
	}

	if err := c.TransactionSimple.Validate(); err != nil {
		return fmt.Errorf("transaction simple config validation failed: %w", err)
	}

	if err := c.TransactionStructlogAgg.Validate(); err != nil {
		return fmt.Errorf("transaction structlog_agg config validation failed: %w", err)
	}

	return nil
}
