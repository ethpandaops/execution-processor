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

// Client represents a ClickHouse client for data storage
type Client struct {
	log    logrus.FieldLogger
	config *Config

	db *sql.DB

	lock sync.Mutex
}

// NewClient creates a new ClickHouse client
func NewClient(ctx context.Context, log logrus.FieldLogger, config *Config) (*Client, error) {
	return &Client{
		log:    log.WithField("component", "clickhouse"),
		config: config,
		lock:   sync.Mutex{},
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
	c.db.SetConnMaxLifetime(time.Hour)

	// Test connection
	start := time.Now()
	err = c.db.PingContext(ctx)
	duration := time.Since(start)

	// Record ClickHouse connection metrics
	status := "success"
	code := ""

	if err != nil {
		status = "failed"
		code = ParseErrorCode(err)
	}

	common.ClickHouseOperationDuration.WithLabelValues("ping", "connection", status, code).Observe(duration.Seconds())
	common.ClickHouseConnectionsActive.WithLabelValues().Set(float64(c.db.Stats().OpenConnections))

	if err != nil {
		return fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	return nil
}

func (c *Client) Stop(ctx context.Context) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	if c.db != nil {
		return c.db.Close()
	}

	return nil
}

// QueryRow executes a query and returns a single row
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
	status := "success"

	if row.Err() != nil {
		code = ParseErrorCode(row.Err())
		status = "failed"
	}

	common.ClickHouseOperationDuration.WithLabelValues("query", table, status, code).Observe(duration.Seconds())
	common.ClickHouseOperationTotal.WithLabelValues("query", table, status, code).Inc()
	common.ClickHouseConnectionsActive.WithLabelValues().Set(float64(c.db.Stats().OpenConnections))

	return row
}

// GetDB returns the underlying database connection
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
