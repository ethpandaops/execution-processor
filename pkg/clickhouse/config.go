package clickhouse

import (
	"fmt"
	"time"
)

// Config holds configuration for ClickHouse ch-go native client.
//
//nolint:tagliatelle // YAML config uses snake_case by convention
type Config struct {
	// Connection
	Addr     string `yaml:"addr"`     // Native protocol address, e.g., "localhost:9000"
	Database string `yaml:"database"` // Database name, default: "default"
	Username string `yaml:"username"`
	Password string `yaml:"password"`

	// Pool settings
	MaxConns          int32         `yaml:"max_conns"`           // Maximum connections in pool, default: 10
	MinConns          int32         `yaml:"min_conns"`           // Minimum connections in pool, default: 2
	ConnMaxLifetime   time.Duration `yaml:"conn_max_lifetime"`   // Maximum connection lifetime, default: 1h
	ConnMaxIdleTime   time.Duration `yaml:"conn_max_idle_time"`  // Maximum idle time, default: 30m
	HealthCheckPeriod time.Duration `yaml:"health_check_period"` // Health check period, default: 1m
	DialTimeout       time.Duration `yaml:"dial_timeout"`        // Dial timeout, default: 10s

	// Performance
	Compression string `yaml:"compression"` // Compression: lz4, zstd, none (default: lz4)

	// Retry settings
	MaxRetries     int           `yaml:"max_retries"`      // Maximum retry attempts, default: 3
	RetryBaseDelay time.Duration `yaml:"retry_base_delay"` // Base delay for exponential backoff, default: 100ms
	RetryMaxDelay  time.Duration `yaml:"retry_max_delay"`  // Max delay between retries, default: 10s

	// Timeout settings
	QueryTimeout time.Duration `yaml:"query_timeout"` // Query timeout per attempt, default: 60s

	// Metrics labels
	Network   string `yaml:"network"`
	Processor string `yaml:"processor"`
	Debug     bool   `yaml:"debug"`
}

// Validate checks if the config is valid.
func (c *Config) Validate() error {
	if c.Addr == "" {
		return fmt.Errorf("addr is required")
	}

	return nil
}

// SetDefaults sets default values for unset fields.
func (c *Config) SetDefaults() {
	if c.Database == "" {
		c.Database = "default"
	}

	if c.MaxConns == 0 {
		c.MaxConns = 10
	}

	if c.DialTimeout == 0 {
		c.DialTimeout = 10 * time.Second
	}

	if c.MinConns == 0 {
		c.MinConns = 2
	}

	if c.ConnMaxLifetime == 0 {
		c.ConnMaxLifetime = time.Hour
	}

	if c.ConnMaxIdleTime == 0 {
		c.ConnMaxIdleTime = 30 * time.Minute
	}

	if c.HealthCheckPeriod == 0 {
		c.HealthCheckPeriod = time.Minute
	}

	if c.Compression == "" {
		c.Compression = "lz4"
	}

	if c.MaxRetries == 0 {
		c.MaxRetries = 3
	}

	if c.RetryBaseDelay == 0 {
		c.RetryBaseDelay = 100 * time.Millisecond
	}

	if c.RetryMaxDelay == 0 {
		c.RetryMaxDelay = 10 * time.Second
	}

	if c.QueryTimeout == 0 {
		c.QueryTimeout = 60 * time.Second
	}
}
