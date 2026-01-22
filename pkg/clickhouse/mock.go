// Package clickhouse provides test mocks for the ClickHouse client.
// This file should only be imported in test files.
package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"

	"github.com/ClickHouse/ch-go"
)

// MockClient is a mock implementation of ClientInterface for testing.
// It should only be used in test files, not in production code.
type MockClient struct {
	// Function fields that can be set by tests
	QueryOneFunc       func(ctx context.Context, query string, dest any) error
	QueryManyFunc      func(ctx context.Context, query string, dest any) error
	ExecuteFunc        func(ctx context.Context, query string) error
	IsStorageEmptyFunc func(ctx context.Context, table string, conditions map[string]any) (bool, error)
	StartFunc          func() error
	StopFunc           func() error
	DoFunc             func(ctx context.Context, query ch.Query) error

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
		QueryOneFunc: func(ctx context.Context, query string, dest any) error {
			return nil
		},
		QueryManyFunc: func(ctx context.Context, query string, dest any) error {
			return nil
		},
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
		Calls: make([]MockCall, 0),
	}
}

// QueryOne implements ClientInterface.
func (m *MockClient) QueryOne(ctx context.Context, query string, dest any) error {
	m.Calls = append(m.Calls, MockCall{
		Method: "QueryOne",
		Args:   []any{ctx, query, dest},
	})

	if m.QueryOneFunc != nil {
		return m.QueryOneFunc(ctx, query, dest)
	}

	return nil
}

// QueryMany implements ClientInterface.
func (m *MockClient) QueryMany(ctx context.Context, query string, dest any) error {
	m.Calls = append(m.Calls, MockCall{
		Method: "QueryMany",
		Args:   []any{ctx, query, dest},
	})

	if m.QueryManyFunc != nil {
		return m.QueryManyFunc(ctx, query, dest)
	}

	return nil
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

// SetQueryOneResponse sets up the mock to return specific data for QueryOne.
func (m *MockClient) SetQueryOneResponse(data any) {
	m.QueryOneFunc = func(ctx context.Context, query string, dest any) error {
		// Marshal the data to JSON and then unmarshal into dest
		jsonData, err := json.Marshal(data)
		if err != nil {
			return err
		}

		return json.Unmarshal(jsonData, dest)
	}
}

// SetQueryManyResponse sets up the mock to return specific data for QueryMany.
func (m *MockClient) SetQueryManyResponse(data any) {
	m.QueryManyFunc = func(ctx context.Context, query string, dest any) error {
		// Use reflection to set the slice
		destValue := reflect.ValueOf(dest).Elem()
		srcValue := reflect.ValueOf(data)

		if destValue.Kind() != reflect.Slice || srcValue.Kind() != reflect.Slice {
			return fmt.Errorf("both dest and data must be slices")
		}

		destValue.Set(srcValue)

		return nil
	}
}

// SetError sets all functions to return the specified error.
func (m *MockClient) SetError(err error) {
	m.QueryOneFunc = func(ctx context.Context, query string, dest any) error {
		return err
	}
	m.QueryManyFunc = func(ctx context.Context, query string, dest any) error {
		return err
	}
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
}
