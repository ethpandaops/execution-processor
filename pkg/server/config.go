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
