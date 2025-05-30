package redis

import (
	"fmt"
)

type Config struct {
	Address string `yaml:"address"`
	Prefix  string `yaml:"prefix"`
}

func (c *Config) Validate() error {
	if c.Address == "" {
		return fmt.Errorf("redis address is required")
	}

	if c.Prefix == "" {
		c.Prefix = "execution-processor"
	}

	return nil
}
