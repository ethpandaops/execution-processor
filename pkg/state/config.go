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

	if c.Storage.DSN == "" {
		return fmt.Errorf("storage.dsn is required")
	}

	if c.Storage.MaxOpenConns <= 0 {
		return fmt.Errorf("storage.maxOpenConns must be greater than 0")
	}

	if c.Storage.MaxIdleConns <= 0 {
		return fmt.Errorf("storage.maxIdleConns must be greater than 0")
	}

	// Validate limiter config if enabled
	if c.Limiter.Enabled {
		if c.Limiter.Table == "" {
			return fmt.Errorf("limiter.table is required when limiter is enabled")
		}

		if c.Limiter.DSN == "" {
			return fmt.Errorf("limiter.dsn is required when limiter is enabled")
		}

		if c.Limiter.MaxOpenConns <= 0 {
			return fmt.Errorf("limiter.maxOpenConns must be greater than 0")
		}

		if c.Limiter.MaxIdleConns <= 0 {
			return fmt.Errorf("limiter.maxIdleConns must be greater than 0")
		}
	}

	return nil
}
