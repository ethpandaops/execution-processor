package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sync"
	"time"

	_ "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/common"
)

const (
	statusSuccess = "success"
	statusFailed  = "failed"
)

// Client represents a ClickHouse client for data storage.
type Client struct {
	log       logrus.FieldLogger
	config    *Config
	network   string
	processor string

	db *sql.DB

	lock sync.Mutex
}

// NewClient creates a new ClickHouse client.
func NewClient(ctx context.Context, log logrus.FieldLogger, config *Config) (*Client, error) {
	return &Client{
		log:       log.WithField("component", "clickhouse"),
		config:    config,
		network:   config.Network,
		processor: config.Processor,
		lock:      sync.Mutex{},
	}, nil
}

func (c *Client) Start(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	db, err := sql.Open("clickhouse", c.config.DSN)
	if err != nil {
		return fmt.Errorf("failed to open ClickHouse connection: %w", err)
	}

	c.db = db

	// Configure connection pool
	c.db.SetMaxOpenConns(c.config.MaxOpenConns)
	c.db.SetMaxIdleConns(c.config.MaxIdleConns)
	c.db.SetConnMaxLifetime(5 * time.Minute)

	// Test connection
	start := time.Now()
	err = c.db.PingContext(ctx)
	duration := time.Since(start)

	// Record ClickHouse connection metrics
	status := statusSuccess
	code := ""

	if err != nil {
		status = statusFailed
		code = ParseErrorCode(err)
	}

	common.ClickHouseOperationDuration.WithLabelValues(c.network, c.processor, "ping", "connection", status, code).Observe(duration.Seconds())
	common.ClickHouseConnectionsActive.WithLabelValues(c.network, c.processor).Set(float64(c.db.Stats().OpenConnections))

	if err != nil {
		return fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	return nil
}

// WithLabels sets the network and processor labels for metrics.
func (c *Client) WithLabels(network, processor string) *Client {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.network = network
	c.processor = processor

	return c
}

func (c *Client) Stop(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.db != nil {
		return c.db.Close()
	}

	return nil
}

// Reconnect closes the existing connection and creates a new one.
func (c *Client) Reconnect(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.log.Warn("Reconnecting ClickHouse client due to metadata issues")

	// Close existing connection if it exists
	if c.db != nil {
		if err := c.db.Close(); err != nil {
			c.log.WithError(err).Warn("Error closing existing ClickHouse connection")
		}

		c.db = nil
	}

	// Create new connection
	db, err := sql.Open("clickhouse", c.config.DSN)
	if err != nil {
		return fmt.Errorf("failed to open new ClickHouse connection: %w", err)
	}

	c.db = db

	// Configure connection pool
	c.db.SetMaxOpenConns(c.config.MaxOpenConns)
	c.db.SetMaxIdleConns(c.config.MaxIdleConns)
	c.db.SetConnMaxLifetime(5 * time.Minute)

	// Test connection
	start := time.Now()
	err = c.db.PingContext(ctx)
	duration := time.Since(start)

	// Record ClickHouse connection metrics
	status := statusSuccess
	code := ""

	if err != nil {
		status = statusFailed
		code = ParseErrorCode(err)
	}

	common.ClickHouseOperationDuration.WithLabelValues(c.network, c.processor, "ping", "reconnection", status, code).Observe(duration.Seconds())
	common.ClickHouseConnectionsActive.WithLabelValues(c.network, c.processor).Set(float64(c.db.Stats().OpenConnections))

	if err != nil {
		return fmt.Errorf("failed to ping ClickHouse after reconnection: %w", err)
	}

	c.log.Info("Successfully reconnected to ClickHouse")

	return nil
}

// QueryRow executes a query and returns a single row.
func (c *Client) QueryRow(ctx context.Context, table, query string) *sql.Row {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.db == nil {
		c.log.Error("ClickHouse client not started - database connection is nil")

		return nil
	}

	start := time.Now()

	row := c.db.QueryRowContext(ctx, query)

	duration := time.Since(start)

	code := ""
	status := statusSuccess

	if row.Err() != nil {
		code = ParseErrorCode(row.Err())
		status = statusFailed
	}

	common.ClickHouseOperationDuration.WithLabelValues(c.network, c.processor, "query", table, status, code).Observe(duration.Seconds())
	common.ClickHouseOperationTotal.WithLabelValues(c.network, c.processor, "query", table, status, code).Inc()
	common.ClickHouseConnectionsActive.WithLabelValues(c.network, c.processor).Set(float64(c.db.Stats().OpenConnections))

	return row
}

// GetDB returns the underlying database connection.
func (c *Client) GetDB() *sql.DB {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.db == nil {
		c.log.Error("ClickHouse client not started - database connection is nil")

		return nil
	}

	return c.db
}

func ParseErrorCode(err error) string {
	re := regexp.MustCompile(`(\d+) code:`)

	if matches := re.FindStringSubmatch(err.Error()); len(matches) > 1 {
		return matches[1]
	}

	return "unknown"
}
