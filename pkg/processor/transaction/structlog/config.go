package structlog

import (
	"fmt"
	"time"

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

	// Batch processing configuration
	// BatchInsertThreshold is the minimum number of structlogs to accumulate before batch insert
	// Transactions with more structlogs than this will bypass batching
	BatchInsertThreshold int64 `yaml:"batchInsertThreshold" default:"50000"`

	// BatchFlushInterval is the maximum time to wait before flushing a batch
	BatchFlushInterval time.Duration `yaml:"batchFlushInterval" default:"5s"`

	// BatchMaxSize is the maximum number of structlogs to accumulate in a batch
	BatchMaxSize int64 `yaml:"batchMaxSize" default:"100000"`
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

	// Validate batch configuration
	if c.BatchInsertThreshold <= 0 {
		return fmt.Errorf("batchInsertThreshold must be greater than 0")
	}

	if c.BatchInsertThreshold >= int64(c.BigTransactionThreshold) {
		return fmt.Errorf("batchInsertThreshold must be less than bigTransactionThreshold to prevent routing conflicts")
	}

	if c.BatchFlushInterval <= 0 {
		return fmt.Errorf("batchFlushInterval must be greater than 0")
	}

	if c.BatchMaxSize < c.BatchInsertThreshold {
		return fmt.Errorf("batchMaxSize must be greater than or equal to batchInsertThreshold")
	}

	return nil
}
