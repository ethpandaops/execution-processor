package state

import (
	"fmt"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
)

type StorageConfig struct {
	clickhouse.Config `yaml:",inline"`
	Table             string `yaml:"table"`
}

type LimiterConfig struct {
	Enabled           bool `yaml:"enabled"`
	clickhouse.Config `yaml:",inline"`
	Table             string `yaml:"table"`
}

type Config struct {
	Storage StorageConfig `yaml:"storage"`
	Limiter LimiterConfig `yaml:"limiter"`
}

func (c *Config) Validate() error {
	// Validate storage config
	if c.Storage.Table == "" {
		return fmt.Errorf("storage.table is required")
	}

	if err := c.Storage.Validate(); err != nil {
		return fmt.Errorf("storage config validation failed: %w", err)
	}

	// Validate limiter config if enabled
	if c.Limiter.Enabled {
		if c.Limiter.Table == "" {
			return fmt.Errorf("limiter.table is required when limiter is enabled")
		}

		if err := c.Limiter.Validate(); err != nil {
			return fmt.Errorf("limiter config validation failed: %w", err)
		}
	}

	return nil
}
