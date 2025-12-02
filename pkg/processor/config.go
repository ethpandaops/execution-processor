package processor

import (
	"fmt"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/processor/common"
	"github.com/ethpandaops/execution-processor/pkg/processor/transaction/simple"
	"github.com/ethpandaops/execution-processor/pkg/processor/transaction/structlog"
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
	TransactionStructlog structlog.Config `yaml:"transactionStructlog"`
	TransactionSimple    simple.Config    `yaml:"transactionSimple"`
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
	if c.Interval == 0 {
		c.Interval = 10 * time.Second
	}

	if c.Mode == "" {
		c.Mode = common.FORWARDS_MODE
	}

	if c.Mode != common.FORWARDS_MODE && c.Mode != common.BACKWARDS_MODE {
		return fmt.Errorf("invalid mode %s, must be '%s' or '%s'", c.Mode, common.FORWARDS_MODE, common.BACKWARDS_MODE)
	}

	if c.Concurrency == 0 {
		c.Concurrency = 20
	}

	// Queue control defaults
	if c.MaxProcessQueueSize == 0 {
		c.MaxProcessQueueSize = 1000
	}

	if c.BackpressureHysteresis == 0 {
		c.BackpressureHysteresis = 0.8
	}

	// Set leader election defaults
	// Enable by default unless explicitly disabled
	if !c.LeaderElection.Enabled && c.LeaderElection.TTL == 0 && c.LeaderElection.RenewalInterval == 0 {
		c.LeaderElection.Enabled = true
	}

	if c.LeaderElection.TTL == 0 {
		c.LeaderElection.TTL = 10 * time.Second
	}

	if c.LeaderElection.RenewalInterval == 0 {
		c.LeaderElection.RenewalInterval = 3 * time.Second
	}

	// Validate leader election settings
	if c.LeaderElection.RenewalInterval >= c.LeaderElection.TTL {
		return fmt.Errorf("leader election renewal interval must be less than TTL")
	}

	if c.TransactionStructlog.Enabled {
		if c.TransactionStructlog.DSN == "" {
			return fmt.Errorf("transaction structlog DSN is required when enabled")
		}

		if c.TransactionStructlog.Table == "" {
			return fmt.Errorf("transaction structlog table is required when enabled")
		}
	}

	if err := c.TransactionSimple.Validate(); err != nil {
		return fmt.Errorf("transaction simple config validation failed: %w", err)
	}

	return nil
}
