package simple

import (
	"fmt"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
)

// Config holds configuration for the simple transaction processor
type Config struct {
	clickhouse.Config `yaml:",inline"`
	Enabled           bool   `yaml:"enabled"`
	Table             string `yaml:"table"`
}

// Validate validates the configuration
func (c *Config) Validate() error {
	if !c.Enabled {
		return nil
	}

	if c.DSN == "" {
		return fmt.Errorf("transaction simple DSN is required when enabled")
	}

	if c.Table == "" {
		return fmt.Errorf("transaction simple table is required when enabled")
	}

	return nil
}
