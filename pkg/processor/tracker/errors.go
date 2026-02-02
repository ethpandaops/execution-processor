package tracker

import (
	"errors"
	"strings"

	"github.com/ethpandaops/execution-processor/pkg/ethereum"
)

// IsBlockNotFoundError checks if an error indicates a block was not found.
// Uses errors.Is for sentinel errors, with fallback to string matching for wrapped errors.
func IsBlockNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	// Check for sentinel error first
	if errors.Is(err, ethereum.ErrBlockNotFound) {
		return true
	}

	// Fallback to string matching for errors from external clients
	errStr := strings.ToLower(err.Error())

	return strings.Contains(errStr, "not found") ||
		strings.Contains(errStr, "unknown block") ||
		strings.Contains(errStr, "block not found") ||
		strings.Contains(errStr, "header not found")
}
