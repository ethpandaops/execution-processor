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
	BatchSize         int    `yaml:"batchSize" default:"1000000"`
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

	if c.BatchSize <= 0 {
		return fmt.Errorf("transaction structlog batch size must be greater than 0")
	}

	return nil
}
