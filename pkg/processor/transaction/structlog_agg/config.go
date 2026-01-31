package structlog_agg

import (
	"fmt"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
)

// Config holds configuration for transaction structlog_agg processor.
type Config struct {
	clickhouse.Config `yaml:",inline"`
	Enabled           bool   `yaml:"enabled"`
	Table             string `yaml:"table"`

	// Async insert settings for ClickHouse (pointers to distinguish omitted from explicit false)
	AsyncInsert        *bool `yaml:"asyncInsert"`        // Enable async inserts to reduce part creation. Default: true
	WaitForAsyncInsert *bool `yaml:"waitForAsyncInsert"` // Wait for async insert to complete. Default: true

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
		return fmt.Errorf("transaction structlog_agg table is required when enabled")
	}

	return nil
}
