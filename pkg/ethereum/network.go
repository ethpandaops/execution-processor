package ethereum

import (
	"fmt"
)

type Network struct {
	Name string
}

var networkMap = map[int64]Network{
	1:        {Name: "mainnet"},
	11155111: {Name: "sepolia"},
	17000:    {Name: "holesky"},
	560048:   {Name: "hoodi"},
}

// GetNetworkByChainID returns the network information for the given chain ID.
func GetNetworkByChainID(chainID int64) (*Network, error) {
	network, exists := networkMap[chainID]
	if !exists {
		return nil, fmt.Errorf("unsupported chain ID: %d - set 'overrideNetworkName' in config for custom networks", chainID)
	}

	return &network, nil
}
