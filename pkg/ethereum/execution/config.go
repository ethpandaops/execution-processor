package execution

import "errors"

// DefaultMaxResponseBytes bounds how much of one JSON-RPC response an RPC node will read.
const DefaultMaxResponseBytes int64 = 1 << 30

// ErrResponseTooLarge is returned when a JSON-RPC response exceeds Config.MaxResponseBytes.
var ErrResponseTooLarge = errors.New("execution node response exceeds the size limit")

type Config struct {
	// The address of the Execution node to connect to
	NodeAddress string `yaml:"nodeAddress"`
	// NodeHeaders is a map of headers to send to the execution node.
	NodeHeaders map[string]string `yaml:"nodeHeaders"`
	// Name is the name of the execution node
	Name string `yaml:"name"`
	// MaxResponseBytes caps the size of a single JSON-RPC response; 0 uses DefaultMaxResponseBytes.
	MaxResponseBytes int64 `yaml:"maxResponseBytes"`
}

func (c *Config) Validate() error {
	if c.NodeAddress == "" {
		return errors.New("nodeAddress is required")
	}

	if c.MaxResponseBytes < 0 {
		return errors.New("maxResponseBytes must not be negative")
	}

	return nil
}

// ResponseLimit returns the effective response size limit in bytes.
func (c *Config) ResponseLimit() int64 {
	if c.MaxResponseBytes == 0 {
		return DefaultMaxResponseBytes
	}

	return c.MaxResponseBytes
}
