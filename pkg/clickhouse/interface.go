package clickhouse

import (
	"context"

	"github.com/ClickHouse/ch-go"
)

// ClientInterface defines the methods for interacting with ClickHouse.
type ClientInterface interface {
	// Start initializes the client
	Start() error
	// Stop closes the client
	Stop() error
	// SetNetwork updates the network name for metrics labeling
	SetNetwork(network string)
	// Do executes a ch-go query directly for streaming operations
	Do(ctx context.Context, query ch.Query) error
	// Execute runs a query without expecting results
	Execute(ctx context.Context, query string) error
	// IsStorageEmpty checks if a table has any records matching the given conditions
	IsStorageEmpty(ctx context.Context, table string, conditions map[string]any) (bool, error)

	// QueryUInt64 executes a query and returns a single UInt64 value from the specified column.
	// Returns nil if no rows are found.
	QueryUInt64(ctx context.Context, query string, columnName string) (*uint64, error)
	// QueryMinMaxUInt64 executes a query that returns min and max UInt64 values.
	// The query must return columns named "min" and "max".
	// Returns nil for both values if no rows are found.
	QueryMinMaxUInt64(ctx context.Context, query string) (minVal, maxVal *uint64, err error)
}
