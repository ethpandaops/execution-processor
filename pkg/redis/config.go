package redis

import (
	"errors"
	"fmt"
)

type Config struct {
	Address  string          `yaml:"address"`
	Prefix   string          `yaml:"prefix"`
	Sentinel *SentinelConfig `yaml:"sentinel"`
}

// SentinelConfig makes the client find the master through Redis Sentinel; Address then only supplies credentials and DB.
type SentinelConfig struct {
	MasterName string   `yaml:"masterName"`
	Addrs      []string `yaml:"addrs"`
	// Password authenticates to the sentinels; it defaults to the password in Address.
	Password string `yaml:"password"`
}

func (c *Config) Validate() error {
	if c.Address == "" {
		return fmt.Errorf("redis address is required")
	}

	if c.Prefix == "" {
		c.Prefix = "execution-processor"
	}

	if c.Sentinel != nil {
		if err := c.Sentinel.Validate(); err != nil {
			return fmt.Errorf("invalid sentinel config: %w", err)
		}
	}

	return nil
}

func (c *SentinelConfig) Validate() error {
	if c.MasterName == "" {
		return errors.New("masterName is required")
	}

	if len(c.Addrs) == 0 {
		return errors.New("at least one sentinel address is required")
	}

	return nil
}

// PrefixKey adds the configured prefix to a Redis key.
func (c *Config) PrefixKey(key string) string {
	if c.Prefix == "" {
		return key
	}

	return fmt.Sprintf("%s:%s", c.Prefix, key)
}

// PrefixQueue adds the configured prefix to an Asynq queue name.
func (c *Config) PrefixQueue(queue string) string {
	if c.Prefix == "" {
		return queue
	}

	return fmt.Sprintf("%s:%s", c.Prefix, queue)
}
