// Package testutil provides test helper utilities for unit and integration tests.
package testutil

import (
	"context"
	"fmt"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/testcontainers/testcontainers-go"
	tcclickhouse "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	tcredis "github.com/testcontainers/testcontainers-go/modules/redis"
)

// NewMiniredis creates an in-memory Redis server for unit tests.
// The server is automatically cleaned up when the test completes.
func NewMiniredis(t *testing.T) *miniredis.Miniredis {
	t.Helper()

	s := miniredis.RunT(t)

	return s
}

// NewMiniredisClient creates a Redis client connected to an in-memory miniredis server.
// Both the server and client are automatically cleaned up when the test completes.
func NewMiniredisClient(t *testing.T) (*redis.Client, *miniredis.Miniredis) {
	t.Helper()

	s := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: s.Addr()})

	t.Cleanup(func() {
		_ = client.Close()
	})

	return client, s
}

// NewRedisContainer creates a real Redis container for integration tests.
// The container is automatically cleaned up when the test completes.
func NewRedisContainer(t *testing.T) (*redis.Client, string) {
	t.Helper()

	ctx := context.Background()

	c, err := tcredis.Run(ctx, "redis:7-alpine")
	if err != nil {
		t.Fatalf("failed to start redis container: %v", err)
	}

	testcontainers.CleanupContainer(t, c)

	connStr, err := c.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get redis connection string: %v", err)
	}

	// Parse the connection string to get the address
	// The connection string is in the format "redis://host:port"
	opts, err := redis.ParseURL(connStr)
	if err != nil {
		t.Fatalf("failed to parse redis connection string: %v", err)
	}

	client := redis.NewClient(opts)

	t.Cleanup(func() {
		_ = client.Close()
	})

	return client, connStr
}

// ClickHouseConnection holds ClickHouse connection details.
type ClickHouseConnection struct {
	Host     string
	Port     int
	Database string
	Username string
	Password string
}

// Addr returns the ClickHouse address in host:port format.
func (c ClickHouseConnection) Addr() string {
	return fmt.Sprintf("%s:%d", c.Host, c.Port)
}

// NewClickHouseContainer creates a real ClickHouse container for integration tests.
// The container is automatically cleaned up when the test completes.
func NewClickHouseContainer(t *testing.T) ClickHouseConnection {
	t.Helper()

	ctx := context.Background()

	c, err := tcclickhouse.Run(ctx, "clickhouse/clickhouse-server:latest",
		tcclickhouse.WithUsername("default"),
		tcclickhouse.WithPassword(""),
		tcclickhouse.WithDatabase("default"),
	)
	if err != nil {
		t.Fatalf("failed to start clickhouse container: %v", err)
	}

	testcontainers.CleanupContainer(t, c)

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get clickhouse host: %v", err)
	}

	port, err := c.MappedPort(ctx, "9000/tcp")
	if err != nil {
		t.Fatalf("failed to get clickhouse port: %v", err)
	}

	return ClickHouseConnection{
		Host:     host,
		Port:     port.Int(),
		Database: "default",
		Username: "default",
		Password: "",
	}
}
