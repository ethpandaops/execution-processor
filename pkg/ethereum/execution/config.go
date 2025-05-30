package execution

import "errors"

type Config struct {
	// The address of the Execution node to connect to
	NodeAddress string `yaml:"nodeAddress"`
	// NodeHeaders is a map of headers to send to the execution node.
	NodeHeaders map[string]string `yaml:"nodeHeaders"`
	// Name is the name of the execution node
	Name string `yaml:"name"`
}

func (c *Config) Validate() error {
	if c.NodeAddress == "" {
		return errors.New("nodeAddress is required")
	}

	return nil
}
