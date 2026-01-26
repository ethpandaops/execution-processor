package clickhouse

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/ClickHouse/ch-go"
	"github.com/ClickHouse/ch-go/chpool"
	"github.com/ClickHouse/ch-go/compress"
	"github.com/ClickHouse/ch-go/proto"
	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/common"
)

const (
	statusSuccess = "success"
	statusFailed  = "failed"
)

// Client implements the ClientInterface using ch-go native protocol.
type Client struct {
	pool        *chpool.Pool
	config      *Config
	compression ch.Compression
	network     string
	processor   string
	log         logrus.FieldLogger
	lock        sync.RWMutex
	started     atomic.Bool

	// Metrics collection
	metricsDone chan struct{}
	metricsWg   sync.WaitGroup
}

// isRetryableError checks if an error is transient and can be retried.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for context errors - these should not be retried
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}

	// Check for ch-go sentinel errors - client permanently closed
	if errors.Is(err, ch.ErrClosed) {
		return false
	}

	// Check for ch-go server exceptions
	if exc, ok := ch.AsException(err); ok {
		// Retryable server-side errors
		if exc.IsCode(
			proto.ErrTimeoutExceeded,            // 159 - server timeout
			proto.ErrNoFreeConnection,           // 203 - server pool exhausted
			proto.ErrTooManySimultaneousQueries, // 202 - rate limited
			proto.ErrSocketTimeout,              // 209 - network timeout
			proto.ErrNetworkError,               // 210 - generic network
		) {
			return true
		}
		// All other server exceptions are non-retryable (syntax, data errors, etc.)
		return false
	}

	// Check for data corruption - never retry
	var corruptedErr *compress.CorruptedDataErr
	if errors.As(err, &corruptedErr) {
		return false
	}

	// Check for connection reset/refused errors (before net.Error since syscall.Errno implements net.Error)
	if errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) {
		return true
	}

	// Check for network timeout errors
	var netErr net.Error
	if errors.As(err, &netErr) && netErr.Timeout() {
		return true
	}

	// Check for common transient error messages (fallback for edge cases)
	errStr := err.Error()
	transientPatterns := []string{
		"connection reset",
		"connection refused",
		"broken pipe",
		"EOF",
		"timeout",
		"temporary failure",
		"server is overloaded",
		"too many connections",
	}

	for _, pattern := range transientPatterns {
		if strings.Contains(strings.ToLower(errStr), strings.ToLower(pattern)) {
			return true
		}
	}

	return false
}

// withQueryTimeout returns a context with the configured query timeout applied.
// If the context already has a deadline, the original context is returned unchanged.
func (c *Client) withQueryTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if c.config.QueryTimeout == 0 {
		return ctx, func() {}
	}

	if _, hasDeadline := ctx.Deadline(); hasDeadline {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, c.config.QueryTimeout)
}

// doWithRetry executes a function with exponential backoff retry logic.
// The function receives a context with the configured query timeout applied per attempt.
func (c *Client) doWithRetry(ctx context.Context, operation string, fn func(ctx context.Context) error) error {
	var lastErr error

	for attempt := 0; attempt <= c.config.MaxRetries; attempt++ {
		if attempt > 0 {
			// Calculate backoff delay with exponential increase, capped at RetryMaxDelay
			delay := c.config.RetryBaseDelay * time.Duration(1<<(attempt-1))
			if c.config.RetryMaxDelay > 0 && delay > c.config.RetryMaxDelay {
				delay = c.config.RetryMaxDelay
			}

			c.log.WithFields(logrus.Fields{
				"attempt":   attempt,
				"max":       c.config.MaxRetries,
				"delay":     delay,
				"operation": operation,
				"error":     lastErr,
			}).Debug("Retrying after transient error")

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		attemptCtx, cancel := c.withQueryTimeout(ctx)
		err := fn(attemptCtx)

		cancel()

		if err == nil {
			return nil
		}

		lastErr = err

		if !isRetryableError(err) {
			return err
		}
	}

	return fmt.Errorf("max retries (%d) exceeded: %w", c.config.MaxRetries, lastErr)
}

// New creates a new ch-go native ClickHouse client.
// The client is not connected until Start() is called.
func New(cfg *Config) (ClientInterface, error) {
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid config: %w", err)
	}

	cfg.SetDefaults()

	compression := ch.CompressionLZ4

	switch cfg.Compression {
	case "zstd":
		compression = ch.CompressionZSTD
	case "none":
		compression = ch.CompressionDisabled
	}

	log := logrus.WithField("component", "clickhouse-native")

	return &Client{
		config:      cfg,
		compression: compression,
		network:     cfg.Network,
		processor:   cfg.Processor,
		log:         log,
	}, nil
}

// dialWithRetry wraps the dial operation with exponential backoff retry logic.
func dialWithRetry(ctx context.Context, log logrus.FieldLogger, cfg *Config, fn func() error) error {
	var lastErr error

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			// Calculate backoff delay with exponential increase, capped at RetryMaxDelay
			delay := cfg.RetryBaseDelay * time.Duration(1<<(attempt-1))
			if cfg.RetryMaxDelay > 0 && delay > cfg.RetryMaxDelay {
				delay = cfg.RetryMaxDelay
			}

			log.WithFields(logrus.Fields{
				"attempt":   attempt,
				"max":       cfg.MaxRetries,
				"delay":     delay,
				"operation": "dial",
				"error":     lastErr,
			}).Debug("Retrying dial after transient error")

			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}

		err := fn()
		if err == nil {
			return nil
		}

		lastErr = err

		if !isRetryableError(err) {
			return err
		}
	}

	return fmt.Errorf("max retries (%d) exceeded: %w", cfg.MaxRetries, lastErr)
}

// Start initializes the client by dialing ClickHouse with retry logic.
func (c *Client) Start() error {
	// Idempotency guard - prevent multiple successful Start() calls from leaking goroutines.
	// We check if pool is already set to allow retries after failures.
	c.lock.Lock()

	if c.pool != nil {
		c.lock.Unlock()
		c.log.Debug("Start() already completed successfully, skipping")

		return nil
	}

	c.lock.Unlock()

	// Dial with startup retry logic for transient connection failures
	ctx, cancel := context.WithTimeout(context.Background(), c.config.DialTimeout*time.Duration(c.config.MaxRetries+1))
	defer cancel()

	var pool *chpool.Pool

	err := dialWithRetry(ctx, c.log, c.config, func() error {
		var dialErr error

		pool, dialErr = chpool.Dial(ctx, chpool.Options{
			ClientOptions: ch.Options{
				Address:     c.config.Addr,
				Database:    c.config.Database,
				User:        c.config.Username,
				Password:    c.config.Password,
				Compression: c.compression,
				DialTimeout: c.config.DialTimeout,
			},
			MaxConns:          c.config.MaxConns,
			MinConns:          c.config.MinConns,
			MaxConnLifetime:   c.config.ConnMaxLifetime,
			MaxConnIdleTime:   c.config.ConnMaxIdleTime,
			HealthCheckPeriod: c.config.HealthCheckPeriod,
		})

		return dialErr
	})
	if err != nil {
		return fmt.Errorf("failed to dial clickhouse: %w", err)
	}

	c.lock.Lock()
	c.pool = pool
	c.lock.Unlock()

	c.log.Info("Connected to ClickHouse native interface")

	// Start pool metrics collection goroutine - use started flag to prevent duplicate goroutines
	if !c.started.Swap(true) {
		c.metricsDone = make(chan struct{})
		c.metricsWg.Add(1)

		go c.collectPoolMetrics()
	}

	return nil
}

// Stop closes the connection pool.
func (c *Client) Stop() error {
	// Stop metrics collection goroutine
	if c.metricsDone != nil {
		close(c.metricsDone)
		c.metricsWg.Wait()
	}

	if c.pool != nil {
		c.pool.Close()
		c.log.Info("Closed ClickHouse connection pool")
	}

	return nil
}

// SetNetwork updates the network name for metrics labeling.
func (c *Client) SetNetwork(network string) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.network = network
}

// Do executes a query using the pool.
func (c *Client) Do(ctx context.Context, query ch.Query) error {
	return c.pool.Do(ctx, query)
}

// QueryUInt64 executes a query and returns a single UInt64 value from the specified column.
// Returns nil if no rows are found.
func (c *Client) QueryUInt64(ctx context.Context, query string, columnName string) (*uint64, error) {
	start := time.Now()
	operation := "query_uint64"
	status := statusSuccess

	defer func() {
		c.recordMetrics(operation, status, time.Since(start), query)
	}()

	var result *uint64

	col := new(proto.ColUInt64)

	err := c.doWithRetry(ctx, operation, func(attemptCtx context.Context) error {
		col.Reset()

		result = nil

		return c.pool.Do(attemptCtx, ch.Query{
			Body: query,
			Result: proto.Results{
				{Name: columnName, Data: col},
			},
			OnResult: func(ctx context.Context, block proto.Block) error {
				if col.Rows() > 0 {
					val := col.Row(0)
					result = &val
				}

				return nil
			},
		})
	})
	if err != nil {
		status = statusFailed

		return nil, fmt.Errorf("query failed: %w", err)
	}

	return result, nil
}

// QueryMinMaxUInt64 executes a query that returns min and max UInt64 values.
// The query must return columns named "min" and "max".
// Returns nil for both values if no rows are found.
func (c *Client) QueryMinMaxUInt64(ctx context.Context, query string) (minVal, maxVal *uint64, err error) {
	start := time.Now()
	operation := "query_min_max"
	status := statusSuccess

	defer func() {
		c.recordMetrics(operation, status, time.Since(start), query)
	}()

	colMin := new(proto.ColUInt64)
	colMax := new(proto.ColUInt64)

	err = c.doWithRetry(ctx, operation, func(attemptCtx context.Context) error {
		colMin.Reset()
		colMax.Reset()

		minVal = nil
		maxVal = nil

		return c.pool.Do(attemptCtx, ch.Query{
			Body: query,
			Result: proto.Results{
				{Name: "min", Data: colMin},
				{Name: "max", Data: colMax},
			},
			OnResult: func(ctx context.Context, block proto.Block) error {
				if colMin.Rows() > 0 {
					v := colMin.Row(0)
					minVal = &v
				}

				if colMax.Rows() > 0 {
					v := colMax.Row(0)
					maxVal = &v
				}

				return nil
			},
		})
	})
	if err != nil {
		status = statusFailed

		return nil, nil, fmt.Errorf("query failed: %w", err)
	}

	return minVal, maxVal, nil
}

// QueryUInt64Slice executes a query and returns all UInt64 values from the specified column.
// Returns an empty slice if no rows are found.
func (c *Client) QueryUInt64Slice(ctx context.Context, query string, columnName string) ([]uint64, error) {
	start := time.Now()
	operation := "query_uint64_slice"
	status := statusSuccess

	defer func() {
		c.recordMetrics(operation, status, time.Since(start), query)
	}()

	result := make([]uint64, 0)
	col := new(proto.ColUInt64)

	err := c.doWithRetry(ctx, operation, func(attemptCtx context.Context) error {
		col.Reset()

		result = result[:0]

		return c.pool.Do(attemptCtx, ch.Query{
			Body: query,
			Result: proto.Results{
				{Name: columnName, Data: col},
			},
			OnResult: func(ctx context.Context, block proto.Block) error {
				for i := 0; i < col.Rows(); i++ {
					result = append(result, col.Row(i))
				}

				return nil
			},
		})
	})
	if err != nil {
		status = statusFailed

		return nil, fmt.Errorf("query failed: %w", err)
	}

	return result, nil
}

// Execute runs a query without expecting results.
func (c *Client) Execute(ctx context.Context, query string) error {
	start := time.Now()
	operation := "execute"
	status := statusSuccess

	defer func() {
		c.recordMetrics(operation, status, time.Since(start), query)
	}()

	err := c.doWithRetry(ctx, operation, func(attemptCtx context.Context) error {
		return c.pool.Do(attemptCtx, ch.Query{
			Body: query,
		})
	})
	if err != nil {
		status = statusFailed

		return fmt.Errorf("execution failed: %w", err)
	}

	return nil
}

// IsStorageEmpty checks if a table has any records matching the given conditions.
func (c *Client) IsStorageEmpty(ctx context.Context, table string, conditions map[string]any) (bool, error) {
	start := time.Now()
	operation := "is_storage_empty"
	status := statusSuccess

	defer func() {
		c.recordMetrics(operation, status, time.Since(start), table)
	}()

	// Build the query
	query := fmt.Sprintf("SELECT count() as count FROM %s FINAL", table)

	if len(conditions) > 0 {
		query += " WHERE "

		conditionParts := make([]string, 0, len(conditions))

		for key, value := range conditions {
			switch v := value.(type) {
			case string:
				conditionParts = append(conditionParts, fmt.Sprintf("%s = '%s'", key, v))
			case int, int64, uint64:
				conditionParts = append(conditionParts, fmt.Sprintf("%s = %v", key, v))
			default:
				conditionParts = append(conditionParts, fmt.Sprintf("%s = '%v'", key, v))
			}
		}

		query += strings.Join(conditionParts, " AND ")
	}

	// Execute and get count
	var count uint64

	err := c.doWithRetry(ctx, operation, func(attemptCtx context.Context) error {
		colCount := new(proto.ColUInt64)

		if err := c.pool.Do(attemptCtx, ch.Query{
			Body: query,
			Result: proto.Results{
				{Name: "count", Data: colCount},
			},
		}); err != nil {
			return err
		}

		if colCount.Rows() > 0 {
			count = colCount.Row(0)
		}

		return nil
	})
	if err != nil {
		status = statusFailed

		return false, fmt.Errorf("failed to check if table is empty: %w", err)
	}

	return count == 0, nil
}

// extractTableName attempts to extract the table name from various SQL query patterns.
func extractTableName(query string) string {
	trimmedQuery := strings.TrimSpace(query)
	upperQuery := strings.ToUpper(trimmedQuery)

	// Handle INSERT INTO queries
	if strings.HasPrefix(upperQuery, "INSERT INTO") {
		parts := strings.Fields(trimmedQuery)
		if len(parts) >= 3 {
			return strings.Trim(parts[2], "`'\"")
		}
	}

	// Handle SELECT ... FROM queries
	if idx := strings.Index(upperQuery, "FROM"); idx != -1 {
		afterFrom := strings.TrimSpace(trimmedQuery[idx+4:])
		parts := strings.Fields(afterFrom)

		if len(parts) > 0 {
			tableName := parts[0]
			if !strings.EqualFold(tableName, "FINAL") {
				return strings.Trim(tableName, "`'\"")
			}
		}
	}

	// Handle CREATE TABLE queries
	if strings.HasPrefix(upperQuery, "CREATE TABLE") {
		parts := strings.Fields(trimmedQuery)
		if len(parts) >= 3 {
			return strings.Trim(parts[2], "`'\"")
		}
	}

	// Handle DROP TABLE queries
	if strings.HasPrefix(upperQuery, "DROP TABLE") {
		parts := strings.Fields(trimmedQuery)
		if len(parts) >= 3 {
			return strings.Trim(parts[2], "`'\"")
		}
	}

	return ""
}

func (c *Client) recordMetrics(operation, status string, duration time.Duration, tableOrQuery string) {
	table := extractTableName(tableOrQuery)

	c.lock.RLock()
	network := c.network
	c.lock.RUnlock()

	common.ClickHouseOperationDuration.WithLabelValues(network, c.processor, operation, table, status, "").Observe(duration.Seconds())
	common.ClickHouseOperationTotal.WithLabelValues(network, c.processor, operation, table, status, "").Inc()
}

// collectPoolMetrics periodically collects pool statistics and updates Prometheus metrics.
func (c *Client) collectPoolMetrics() {
	defer c.metricsWg.Done()

	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	// Track previous counter values for delta calculation
	var prevAcquireCount, prevEmptyAcquireCount, prevCanceledAcquireCount int64

	for {
		select {
		case <-c.metricsDone:
			return
		case <-ticker.C:
			c.lock.RLock()
			network := c.network
			c.lock.RUnlock()

			if network == "" {
				continue
			}

			stat := c.pool.Stat()

			// Set gauge values (current state)
			common.ClickHousePoolAcquiredResources.WithLabelValues(network, c.processor).Set(float64(stat.AcquiredResources()))
			common.ClickHousePoolIdleResources.WithLabelValues(network, c.processor).Set(float64(stat.IdleResources()))
			common.ClickHousePoolConstructingResources.WithLabelValues(network, c.processor).Set(float64(stat.ConstructingResources()))
			common.ClickHousePoolTotalResources.WithLabelValues(network, c.processor).Set(float64(stat.TotalResources()))
			common.ClickHousePoolMaxResources.WithLabelValues(network, c.processor).Set(float64(stat.MaxResources()))

			// Set cumulative duration gauges
			common.ClickHousePoolAcquireDuration.WithLabelValues(network, c.processor).Set(stat.AcquireDuration().Seconds())
			common.ClickHousePoolEmptyAcquireWaitDuration.WithLabelValues(network, c.processor).Set(stat.EmptyAcquireWaitTime().Seconds())

			// Calculate deltas and add to counters
			currentAcquireCount := stat.AcquireCount()
			currentEmptyAcquireCount := stat.EmptyAcquireCount()
			currentCanceledAcquireCount := stat.CanceledAcquireCount()

			if prevAcquireCount > 0 {
				delta := currentAcquireCount - prevAcquireCount
				if delta > 0 {
					common.ClickHousePoolAcquireTotal.WithLabelValues(network, c.processor).Add(float64(delta))
				}
			}

			if prevEmptyAcquireCount > 0 {
				delta := currentEmptyAcquireCount - prevEmptyAcquireCount
				if delta > 0 {
					common.ClickHousePoolEmptyAcquireTotal.WithLabelValues(network, c.processor).Add(float64(delta))
				}
			}

			if prevCanceledAcquireCount > 0 {
				delta := currentCanceledAcquireCount - prevCanceledAcquireCount
				if delta > 0 {
					common.ClickHousePoolCanceledAcquireTotal.WithLabelValues(network, c.processor).Add(float64(delta))
				}
			}

			prevAcquireCount = currentAcquireCount
			prevEmptyAcquireCount = currentEmptyAcquireCount
			prevCanceledAcquireCount = currentCanceledAcquireCount
		}
	}
}
