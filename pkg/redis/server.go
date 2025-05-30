package redis

import (
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

// New creates a new Redis client from configuration
func New(config *Config) (*redis.Client, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid redis config: %w", err)
	}

	// Parse Redis URL
	addr := config.Address
	addr = strings.TrimPrefix(addr, "redis://")

	client := redis.NewClient(&redis.Options{
		Addr: addr,
	})

	return client, nil
}
