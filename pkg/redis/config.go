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

// PrefixKey adds the configured prefix to a Redis key
func (c *Config) PrefixKey(key string) string {
	if c.Prefix == "" {
		return key
	}

	return fmt.Sprintf("%s:%s", c.Prefix, key)
}

// PrefixQueue adds the configured prefix to an Asynq queue name
func (c *Config) PrefixQueue(queue string) string {
	if c.Prefix == "" {
		return queue
	}

	return fmt.Sprintf("%s:%s", c.Prefix, queue)
}
