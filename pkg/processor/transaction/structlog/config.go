package structlog

import (
	"fmt"

	"github.com/ethpandaops/execution-processor/pkg/clickhouse"
)

// TransactionStructlogConfig holds configuration for transaction structlog processor
type Config struct {
	clickhouse.Config `yaml:",inline"`
	Enabled           bool   `yaml:"enabled"`
	Table             string `yaml:"table"`

	// Big transaction handling
	BigTransactionThreshold int `yaml:"bigTransactionThreshold"` // Default: 500,000
	ChunkSize               int `yaml:"chunkSize"`               // Default: 10,000
	ChannelBufferSize       int `yaml:"channelBufferSize"`       // Default: 2
	ProgressLogThreshold    int `yaml:"progressLogThreshold"`    // Default: 100,000
}

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
