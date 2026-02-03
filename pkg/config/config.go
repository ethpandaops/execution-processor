// Package config provides configuration types for execution-processor.
// This package is designed to be imported without pulling in go-ethereum dependencies,
// making it suitable for embedded mode integrations.
package config

import (
	"fmt"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
	"github.com/ethpandaops/execution-processor/pkg/processor"
	"github.com/ethpandaops/execution-processor/pkg/redis"
	"github.com/ethpandaops/execution-processor/pkg/state"
)

// EthereumConfig is the ethereum network configuration.
// This is a copy of ethereum.Config to avoid importing pkg/ethereum
// which would pull in go-ethereum dependencies.
type EthereumConfig struct {
	// Execution configuration
	Execution []*execution.Config `yaml:"execution"`
	// Override network name for custom networks (bypasses networkMap)
	OverrideNetworkName *string `yaml:"overrideNetworkName"`
}

// Validate validates the ethereum configuration.
func (c *EthereumConfig) Validate() error {
	for i, exec := range c.Execution {
		if err := exec.Validate(); err != nil {
			return fmt.Errorf("invalid execution configuration at index %d: %w", i, err)
		}
	}

	return nil
}

// Config is the main configuration for execution-processor.
type Config struct {
	// MetricsAddr is the address to listen on for metrics.
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
	Ethereum EthereumConfig `yaml:"ethereum"`
	// Redis is the redis configuration.
	Redis *redis.Config `yaml:"redis"`
	// StateManager is the state manager configuration.
	StateManager state.Config `yaml:"stateManager"`
	// Processors is the processor configuration.
	Processors processor.Config `yaml:"processors"`
	// ShutdownTimeout is the timeout for shutting down the server.
	ShutdownTimeout time.Duration `yaml:"shutdownTimeout" default:"10s"`
}

// Validate validates the configuration.
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
