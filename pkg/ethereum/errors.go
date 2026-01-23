package ethereum

import "errors"

// Sentinel errors for Ethereum client operations.
var (
	// ErrNoHealthyNode indicates no healthy execution node is available.
	ErrNoHealthyNode = errors.New("no healthy execution node available")

	// ErrBlockNotFound indicates a block was not found on the execution client.
	ErrBlockNotFound = errors.New("block not found")

	// ErrTransactionNotFound indicates a transaction was not found.
	ErrTransactionNotFound = errors.New("transaction not found")

	// ErrUnsupportedChainID indicates an unsupported chain ID was provided.
	ErrUnsupportedChainID = errors.New("unsupported chain ID")
)
