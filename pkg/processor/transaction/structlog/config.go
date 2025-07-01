package structlog

import (
	"fmt"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
)

// BatchConfig holds configuration for the batch aggregation system
type BatchConfig struct {
	Enabled           bool          `yaml:"enabled"`           // Enable batch aggregation
	MaxRows           int           `yaml:"maxRows"`           // Max rows before forced flush
	FlushInterval     time.Duration `yaml:"flushInterval"`     // Max time to wait before flush
	ChannelBufferSize int           `yaml:"channelBufferSize"` // Max tasks that can be queued
	FlushTimeout      time.Duration `yaml:"flushTimeout"`      // Timeout for ClickHouse flush operations
}

// LargeTransactionConfig holds configuration for handling large transactions
type LargeTransactionConfig struct {
	Enabled              bool          `yaml:"enabled"`              // Enable large transaction handling
	StructlogThreshold   int           `yaml:"structlogThreshold"`   // Number of structlogs to consider a transaction "large"
	WorkerWaitTimeout    time.Duration `yaml:"workerWaitTimeout"`    // How long workers wait before returning task to queue
	MaxProcessingTime    time.Duration `yaml:"maxProcessingTime"`    // Maximum time allowed for processing a large transaction
	EnableSequentialMode bool          `yaml:"enableSequentialMode"` // If true, large transactions are processed one at a time
}

// TransactionStructlogConfig holds configuration for transaction structlog processor
type Config struct {
	clickhouse.Config      `yaml:",inline"`
	Enabled                bool                    `yaml:"enabled"`
	Table                  string                  `yaml:"table"`
	BatchConfig            BatchConfig             `yaml:"batchConfig"`
	MemoryThresholds       *MemoryThresholds       `yaml:"memoryThresholds"`
	LargeTransactionConfig *LargeTransactionConfig `yaml:"largeTransactionConfig"`
}

func (c *Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	if c.DSN == "" {
		return fmt.Errorf("transaction structlog DSN is required when enabled")
	}

	if c.Table == "" {
		return fmt.Errorf("transaction structlog table is required when enabled")
	}

	// Validate batch config
	if c.BatchConfig.Enabled {
		if c.BatchConfig.MaxRows <= 0 {
			return fmt.Errorf("batch config max rows must be greater than 0")
		}

		if c.BatchConfig.FlushInterval <= 0 {
			return fmt.Errorf("batch config flush interval must be greater than 0")
		}

		if c.BatchConfig.ChannelBufferSize <= 0 {
			return fmt.Errorf("batch config channel buffer size must be greater than 0")
		}

		if c.BatchConfig.FlushTimeout <= 0 {
			c.BatchConfig.FlushTimeout = 5 * time.Minute
		}
	}

	// Set default memory thresholds if not provided
	if c.MemoryThresholds == nil {
		defaultThresholds := DefaultMemoryThresholds()
		c.MemoryThresholds = &defaultThresholds
	}

	// Set default large transaction config if not provided
	if c.LargeTransactionConfig == nil {
		c.LargeTransactionConfig = &LargeTransactionConfig{
			Enabled:              false,
			StructlogThreshold:   100000, // 100K structlogs
			WorkerWaitTimeout:    60 * time.Second,
			MaxProcessingTime:    5 * time.Minute,
			EnableSequentialMode: true,
		}
	} else if c.LargeTransactionConfig.Enabled {
		// Validate large transaction config
		if c.LargeTransactionConfig.StructlogThreshold <= 0 {
			return fmt.Errorf("large transaction structlog threshold must be greater than 0")
		}

		if c.LargeTransactionConfig.WorkerWaitTimeout <= 0 {
			return fmt.Errorf("large transaction worker wait timeout must be greater than 0")
		}

		if c.LargeTransactionConfig.MaxProcessingTime <= 0 {
			return fmt.Errorf("large transaction max processing time must be greater than 0")
		}
	}

	return nil
}
