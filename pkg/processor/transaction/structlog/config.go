package structlog

import (
	"fmt"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
)

// Default buffer configuration values.
const (
	DefaultBufferMaxRows       = 100000
	DefaultBufferFlushInterval = time.Second
)

// Config holds configuration for transaction structlog processor.
type Config struct {
	clickhouse.Config `yaml:",inline"`
	Enabled           bool   `yaml:"enabled"`
	Table             string `yaml:"table"`

	// Row buffer settings for batched ClickHouse inserts
	BufferMaxRows       int           `yaml:"bufferMaxRows"`       // Max rows before flush. Default: 100000
	BufferFlushInterval time.Duration `yaml:"bufferFlushInterval"` // Max time before flush. Default: 1s

	// Block completion tracking
	MaxPendingBlockRange int `yaml:"maxPendingBlockRange"` // Max distance between oldest incomplete and current block. Default: 2
}

// Validate validates the configuration.
func (c *Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	// Validate the embedded clickhouse config
	if err := c.Config.Validate(); err != nil {
		return fmt.Errorf("clickhouse config validation failed: %w", err)
	}

	if c.Table == "" {
		return fmt.Errorf("transaction structlog table is required when enabled")
	}

	return nil
}
