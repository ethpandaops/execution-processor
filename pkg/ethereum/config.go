package ethereum

import (
	"fmt"

	"github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

type Config struct {
	// Execution configuration
	Execution []*execution.Config `yaml:"execution"`
	// Override network name for custom networks (bypasses networkMap)
	OverrideNetworkName *string `yaml:"overrideNetworkName"`
}

func (c *Config) Validate() error {
	for i, execution := range c.Execution {
		if err := execution.Validate(); err != nil {
			return fmt.Errorf("invalid execution configuration at index %d: %w", i, err)
		}
	}

	return nil
}
