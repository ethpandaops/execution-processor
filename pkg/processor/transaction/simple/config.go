package simple

import (
	"fmt"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
)

// Config holds configuration for the simple transaction processor.
type Config struct {
	clickhouse.Config `yaml:",inline"`
	Enabled           bool   `yaml:"enabled"`
	Table             string `yaml:"table"`
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
		return fmt.Errorf("transaction simple table is required when enabled")
	}

	return nil
}
