package clickhouse

import "fmt"

type Config struct {
	DSN          string `yaml:"dsn" default:"http://localhost:8123/default"`
	MaxOpenConns int    `yaml:"maxOpenConns" default:"10"`
	MaxIdleConns int    `yaml:"maxIdleConns" default:"10"`
	Network      string // Network name for metrics
	Processor    string // Processor name for metrics
}

func (c *Config) Validate() error {
	if c.DSN == "" {
		return fmt.Errorf("DSN is required")
	}

	return nil
}
