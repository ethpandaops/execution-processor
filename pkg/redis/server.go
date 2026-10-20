package redis

import (
	"fmt"

	"github.com/hibiken/asynq"
	"github.com/redis/go-redis/v9"
)

// New creates a new Redis client from configuration. With Sentinel set, the client always talks to the current master.
func New(config *Config) (*redis.Client, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid redis config: %w", err)
	}

	options, err := redis.ParseURL(config.Address)
	if err != nil {
		return nil, fmt.Errorf("failed to parse redis url: %w", err)
	}

	if config.Sentinel == nil {
		return redis.NewClient(options), nil
	}

	sentinelPassword := config.Sentinel.Password
	if sentinelPassword == "" {
		sentinelPassword = options.Password
	}

	return redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:       config.Sentinel.MasterName,
		SentinelAddrs:    config.Sentinel.Addrs,
		SentinelPassword: sentinelPassword,
		Username:         options.Username,
		Password:         options.Password,
		DB:               options.DB,
		TLSConfig:        options.TLSConfig,
	}), nil
}

// ConnOpt returns asynq connection options that open new clients with client's options, so they reach the same server.
func ConnOpt(client *redis.Client) asynq.RedisConnOpt {
	return connOpt{options: client.Options()}
}

type connOpt struct {
	options *redis.Options
}

func (o connOpt) MakeRedisClient() any {
	options := *o.options

	return redis.NewClient(&options)
}
