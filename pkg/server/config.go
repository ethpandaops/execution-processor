package server

import (
	"fmt"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/ethereum"
	"github.com/ethpandaops/execution-processor/pkg/processor"
	"github.com/ethpandaops/execution-processor/pkg/redis"
	"github.com/ethpandaops/execution-processor/pkg/state"
)

type Config struct { // MetricsAddr is the address to listen on for metrics.
	MetricsAddr string `yaml:"metricsAddr" default:":9090"`
	// HealthCheckAddr is the address to listen on for healthcheck.
	HealthCheckAddr *string `yaml:"healthCheckAddr"`
	// PProfAddr is the address to listen on for pprof.
	PProfAddr *string `yaml:"pprofAddr"`
	// APIAddr is the address to listen on for the API server.
	APIAddr *string `yaml:"apiAddr"`
	// LoggingLevel is the logging level to use.
	LoggingLevel string `yaml:"logging" default:"info"`
	// Ethereum is the ethereum network configuration.
	Ethereum ethereum.Config `yaml:"ethereum"`
	// Redis is the redis configuration.
	Redis *redis.Config `yaml:"redis"`
	// StateManager is the state manager configuration.
	StateManager state.Config `yaml:"stateManager"`
	// Processors is the processor configuration.
	Processors processor.Config `yaml:"processors"`
	// ShutdownTimeout is the timeout for shutting down the server.
	ShutdownTimeout time.Duration `yaml:"shutdownTimeout" default:"10s"`
	// MemoryMonitor is the memory monitoring configuration.
	MemoryMonitor MemoryMonitorConfig `yaml:"memoryMonitor"`
}

// MemoryMonitorConfig holds configuration for memory monitoring
type MemoryMonitorConfig struct {
	// Enabled enables memory monitoring
	Enabled bool `yaml:"enabled" default:"true"`
	// Interval is the interval to collect memory stats
	Interval time.Duration `yaml:"interval" default:"30s"`
	// WarningThresholdMB is the memory threshold in MB to log warnings
	WarningThresholdMB uint64 `yaml:"warningThresholdMB" default:"1024"`
	// CriticalThresholdMB is the memory threshold in MB to log critical warnings
	CriticalThresholdMB uint64 `yaml:"criticalThresholdMB" default:"2048"`
}

func (c *Config) Validate() error {
	if c.Redis == nil {
		return fmt.Errorf("redis configuration is required")
	}

	if err := c.Redis.Validate(); err != nil {
		return fmt.Errorf("invalid redis configuration: %w", err)
	}

	if err := c.Ethereum.Validate(); err != nil {
		return fmt.Errorf("invalid ethereum configuration: %w", err)
	}

	if err := c.StateManager.Validate(); err != nil {
		return fmt.Errorf("invalid state manager configuration: %w", err)
	}

	if err := c.Processors.Validate(); err != nil {
		return fmt.Errorf("invalid processor configuration: %w", err)
	}

	return nil
}
