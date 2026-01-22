package clickhouse

import (
	"context"

	"github.com/ClickHouse/ch-go"
)

// ClientInterface defines the methods for interacting with ClickHouse.
type ClientInterface interface {
	// QueryOne executes a query and returns a single result
	QueryOne(ctx context.Context, query string, dest any) error
	// QueryMany executes a query and returns multiple results
	QueryMany(ctx context.Context, query string, dest any) error
	// Execute runs a query without expecting results
	Execute(ctx context.Context, query string) error
	// IsStorageEmpty checks if a table has any records matching the given conditions
	IsStorageEmpty(ctx context.Context, table string, conditions map[string]any) (bool, error)
	// SetNetwork updates the network name for metrics labeling
	SetNetwork(network string)
	// Start initializes the client
	Start() error
	// Stop closes the client
	Stop() error

	// Do executes a ch-go query directly for streaming operations
	Do(ctx context.Context, query ch.Query) error
}
