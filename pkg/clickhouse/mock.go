// Package clickhouse provides test mocks for the ClickHouse client.
// This file should only be imported in test files.
package clickhouse

import (
	"context"

	"github.com/ClickHouse/ch-go"
)

// MockClient is a mock implementation of ClientInterface for testing.
// It should only be used in test files, not in production code.
type MockClient struct {
	// Function fields that can be set by tests
	ExecuteFunc           func(ctx context.Context, query string) error
	IsStorageEmptyFunc    func(ctx context.Context, table string, conditions map[string]any) (bool, error)
	StartFunc             func() error
	StopFunc              func() error
	DoFunc                func(ctx context.Context, query ch.Query) error
	QueryUInt64Func       func(ctx context.Context, query string, columnName string) (*uint64, error)
	QueryMinMaxUInt64Func func(ctx context.Context, query string) (minVal, maxVal *uint64, err error)

	// Track calls for assertions
	Calls []MockCall
}

// MockCall represents a method call made to the mock.
type MockCall struct {
	Method string
	Args   []any
}

// NewMockClient creates a new mock client with default implementations.
func NewMockClient() *MockClient {
	return &MockClient{
		ExecuteFunc: func(ctx context.Context, query string) error {
			return nil
		},
		IsStorageEmptyFunc: func(ctx context.Context, table string, conditions map[string]any) (bool, error) {
			return true, nil
		},
		StartFunc: func() error {
			return nil
		},
		StopFunc: func() error {
			return nil
		},
		DoFunc: func(ctx context.Context, query ch.Query) error {
			return nil
		},
		QueryUInt64Func: func(ctx context.Context, query string, columnName string) (*uint64, error) {
			return nil, nil
		},
		QueryMinMaxUInt64Func: func(ctx context.Context, query string) (minVal, maxVal *uint64, err error) {
			return nil, nil, nil
		},
		Calls: make([]MockCall, 0),
	}
}

// QueryUInt64 implements ClientInterface.
func (m *MockClient) QueryUInt64(ctx context.Context, query string, columnName string) (*uint64, error) {
	m.Calls = append(m.Calls, MockCall{
		Method: "QueryUInt64",
		Args:   []any{ctx, query, columnName},
	})

	if m.QueryUInt64Func != nil {
		return m.QueryUInt64Func(ctx, query, columnName)
	}

	return nil, nil //nolint:nilnil // valid for mock: nil means no value found
}

// QueryMinMaxUInt64 implements ClientInterface.
func (m *MockClient) QueryMinMaxUInt64(ctx context.Context, query string) (minVal, maxVal *uint64, err error) {
	m.Calls = append(m.Calls, MockCall{
		Method: "QueryMinMaxUInt64",
		Args:   []any{ctx, query},
	})

	if m.QueryMinMaxUInt64Func != nil {
		return m.QueryMinMaxUInt64Func(ctx, query)
	}

	return nil, nil, nil
}

// Execute implements ClientInterface.
func (m *MockClient) Execute(ctx context.Context, query string) error {
	m.Calls = append(m.Calls, MockCall{
		Method: "Execute",
		Args:   []any{ctx, query},
	})

	if m.ExecuteFunc != nil {
		return m.ExecuteFunc(ctx, query)
	}

	return nil
}

// IsStorageEmpty implements ClientInterface.
func (m *MockClient) IsStorageEmpty(ctx context.Context, table string, conditions map[string]any) (bool, error) {
	m.Calls = append(m.Calls, MockCall{
		Method: "IsStorageEmpty",
		Args:   []any{ctx, table, conditions},
	})

	if m.IsStorageEmptyFunc != nil {
		return m.IsStorageEmptyFunc(ctx, table, conditions)
	}

	return true, nil
}

// SetNetwork implements ClientInterface.
func (m *MockClient) SetNetwork(network string) {
	m.Calls = append(m.Calls, MockCall{
		Method: "SetNetwork",
		Args:   []any{network},
	})
}

// Start implements ClientInterface.
func (m *MockClient) Start() error {
	m.Calls = append(m.Calls, MockCall{
		Method: "Start",
		Args:   []any{},
	})

	if m.StartFunc != nil {
		return m.StartFunc()
	}

	return nil
}

// Stop implements ClientInterface.
func (m *MockClient) Stop() error {
	m.Calls = append(m.Calls, MockCall{
		Method: "Stop",
		Args:   []any{},
	})

	if m.StopFunc != nil {
		return m.StopFunc()
	}

	return nil
}

// Do implements ClientInterface.
func (m *MockClient) Do(ctx context.Context, query ch.Query) error {
	m.Calls = append(m.Calls, MockCall{
		Method: "Do",
		Args:   []any{ctx, query},
	})

	if m.DoFunc != nil {
		return m.DoFunc(ctx, query)
	}

	return nil
}

// GetCallCount returns the number of times a method was called.
func (m *MockClient) GetCallCount(method string) int {
	count := 0

	for _, call := range m.Calls {
		if call.Method == method {
			count++
		}
	}

	return count
}

// WasCalled returns true if the specified method was called.
func (m *MockClient) WasCalled(method string) bool {
	return m.GetCallCount(method) > 0
}

// Reset clears all recorded calls.
func (m *MockClient) Reset() {
	m.Calls = make([]MockCall, 0)
}

// Helper functions for common test scenarios

// SetQueryUInt64Response sets up the mock to return a specific value for QueryUInt64.
func (m *MockClient) SetQueryUInt64Response(value *uint64) {
	m.QueryUInt64Func = func(ctx context.Context, query string, columnName string) (*uint64, error) {
		return value, nil
	}
}

// SetQueryMinMaxUInt64Response sets up the mock to return specific values for QueryMinMaxUInt64.
func (m *MockClient) SetQueryMinMaxUInt64Response(minVal, maxVal *uint64) {
	m.QueryMinMaxUInt64Func = func(ctx context.Context, query string) (*uint64, *uint64, error) {
		return minVal, maxVal, nil
	}
}

// SetError sets all functions to return the specified error.
func (m *MockClient) SetError(err error) {
	m.ExecuteFunc = func(ctx context.Context, query string) error {
		return err
	}
	m.IsStorageEmptyFunc = func(ctx context.Context, table string, conditions map[string]any) (bool, error) {
		return false, err
	}
	m.StartFunc = func() error {
		return err
	}
	m.StopFunc = func() error {
		return err
	}
	m.DoFunc = func(ctx context.Context, query ch.Query) error {
		return err
	}
	m.QueryUInt64Func = func(ctx context.Context, query string, columnName string) (*uint64, error) {
		return nil, err
	}
	m.QueryMinMaxUInt64Func = func(ctx context.Context, query string) (*uint64, *uint64, error) {
		return nil, nil, err
	}
}
