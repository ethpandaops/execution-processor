package clickhouse

import (
	"fmt"
	"time"
)

type Config struct {
	URL           string        `yaml:"url"`
	QueryTimeout  time.Duration `yaml:"query_timeout"`
	InsertTimeout time.Duration `yaml:"insert_timeout"`
	Network       string        `yaml:"network"`
	Processor     string        `yaml:"processor"`
	Debug         bool          `yaml:"debug"`
	KeepAlive     time.Duration `yaml:"keep_alive"`
}

func (c *Config) Validate() error {
	if c.URL == "" {
		return fmt.Errorf("URL is required")
	}

	return nil
}

func (c *Config) SetDefaults() {
	if c.QueryTimeout == 0 {
		c.QueryTimeout = 30 * time.Second
	}

	if c.InsertTimeout == 0 {
		c.InsertTimeout = 5 * time.Minute
	}

	if c.KeepAlive == 0 {
		c.KeepAlive = 30 * time.Second
	}
}
