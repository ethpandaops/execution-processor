package ethereum

import (
	"fmt"
)

type Network struct {
	ID   int32
	Name string
}

var networkMap = map[int32]Network{
	1:        {ID: 1, Name: "mainnet"},
	11155111: {ID: 11155111, Name: "sepolia"},
	17000:    {ID: 17000, Name: "holesky"},
	560048:   {ID: 560048, Name: "hoodi"},
}

// GetNetworkByChainID returns the network information for the given chain ID
func GetNetworkByChainID(chainID int32) (*Network, error) {
	network, exists := networkMap[chainID]
	if !exists {
		return nil, fmt.Errorf("unsupported chain ID: %d", chainID)
	}

	return &network, nil
}
