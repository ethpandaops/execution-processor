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
	ChunkSize         int           `yaml:"chunkSize"`         // Chunk size for large single transactions (fallback)
}

// TransactionStructlogConfig holds configuration for transaction structlog processor
type Config struct {
	clickhouse.Config `yaml:",inline"`
	Enabled           bool        `yaml:"enabled"`
	Table             string      `yaml:"table"`
	BatchConfig       BatchConfig `yaml:"batchConfig"`
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

		if c.BatchConfig.ChunkSize <= 0 {
			return fmt.Errorf("batch config chunk size must be greater than 0")
		}
	} else if c.BatchConfig.ChunkSize <= 0 {
		// When batch aggregation is disabled, we still need chunk size for fallback
		return fmt.Errorf("batch config chunk size must be greater than 0")
	}

	return nil
}
