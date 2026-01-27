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

	// Streaming settings
	ChunkSize            int `yaml:"chunkSize"`            // Default: 10,000 rows per OnInput iteration
	ProgressLogThreshold int `yaml:"progressLogThreshold"` // Default: 100,000 - log progress for large txs

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
