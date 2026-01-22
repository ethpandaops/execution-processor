package clickhouse

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"reflect"
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
	pool      *chpool.Pool
	config    *Config
	network   string
	processor string
	log       logrus.FieldLogger
	lock      sync.RWMutex
	started   atomic.Bool

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
			// Calculate backoff delay with exponential increase
			delay := c.config.RetryBaseDelay * time.Duration(1<<(attempt-1))

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
func New(ctx context.Context, cfg *Config) (ClientInterface, error) {
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

	// Dial with startup retry logic for transient connection failures
	var pool *chpool.Pool

	err := dialWithRetry(ctx, log, cfg, func() error {
		var dialErr error

		pool, dialErr = chpool.Dial(ctx, chpool.Options{
			ClientOptions: ch.Options{
				Address:     cfg.Addr,
				Database:    cfg.Database,
				User:        cfg.Username,
				Password:    cfg.Password,
				Compression: compression,
				DialTimeout: cfg.DialTimeout,
			},
			MaxConns:          cfg.MaxConns,
			MinConns:          cfg.MinConns,
			MaxConnLifetime:   cfg.ConnMaxLifetime,
			MaxConnIdleTime:   cfg.ConnMaxIdleTime,
			HealthCheckPeriod: cfg.HealthCheckPeriod,
		})

		return dialErr
	})
	if err != nil {
		return nil, fmt.Errorf("failed to dial clickhouse: %w", err)
	}

	return &Client{
		pool:      pool,
		config:    cfg,
		network:   cfg.Network,
		processor: cfg.Processor,
		log:       log,
	}, nil
}

// dialWithRetry wraps the dial operation with exponential backoff retry logic.
func dialWithRetry(ctx context.Context, log logrus.FieldLogger, cfg *Config, fn func() error) error {
	var lastErr error

	for attempt := 0; attempt <= cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := cfg.RetryBaseDelay * time.Duration(1<<(attempt-1))

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

// Start initializes the client and tests connectivity.
func (c *Client) Start() error {
	// Idempotency guard - prevent multiple Start() calls from leaking goroutines.
	// Once Start() is called, the client is considered "started" regardless of outcome.
	// On failure, the client is effectively dead and Stop() should be called.
	if c.started.Swap(true) {
		c.log.Debug("Start() already called, skipping")

		return nil
	}

	if c.network == "" {
		c.log.Debug("Skipping ClickHouse connectivity test - network not yet determined")

		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := c.pool.Ping(ctx); err != nil {
		// Don't reset started - the client is now in a failed state.
		// Caller should call Stop() to clean up.
		return fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	c.log.Info("Connected to ClickHouse native interface")

	// Start pool metrics collection goroutine
	c.metricsDone = make(chan struct{})
	c.metricsWg.Add(1)

	go c.collectPoolMetrics()

	return nil
}

// Stop closes the connection pool.
func (c *Client) Stop() error {
	// Stop metrics collection goroutine
	if c.metricsDone != nil {
		close(c.metricsDone)
		c.metricsWg.Wait()
	}

	c.pool.Close()
	c.log.Info("Closed ClickHouse connection pool")

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

// QueryOne executes a query and returns a single result.
func (c *Client) QueryOne(ctx context.Context, query string, dest any) error {
	start := time.Now()
	operation := "query_one"
	status := statusSuccess

	defer func() {
		c.recordMetrics(operation, status, time.Since(start), query)
	}()

	var rows []string

	err := c.doWithRetry(ctx, operation, func(attemptCtx context.Context) error {
		// Reset rows for each retry attempt
		rows = nil
		colStr := new(proto.ColStr)

		return c.pool.Do(attemptCtx, ch.Query{
			Body: query + " FORMAT JSONEachRow",
			Result: proto.Results{
				{Name: "", Data: colStr},
			},
			OnResult: func(ctx context.Context, block proto.Block) error {
				for i := 0; i < colStr.Rows(); i++ {
					rows = append(rows, colStr.Row(i))
				}

				return nil
			},
		})
	})
	if err != nil {
		status = statusFailed

		return fmt.Errorf("query execution failed: %w", err)
	}

	if len(rows) == 0 {
		return nil
	}

	if err := json.Unmarshal([]byte(rows[0]), dest); err != nil {
		status = statusFailed

		return fmt.Errorf("failed to unmarshal result: %w", err)
	}

	return nil
}

// QueryMany executes a query and returns multiple results.
func (c *Client) QueryMany(ctx context.Context, query string, dest any) error {
	start := time.Now()
	operation := "query_many"
	status := statusSuccess

	defer func() {
		c.recordMetrics(operation, status, time.Since(start), query)
	}()

	// Validate that dest is a pointer to a slice
	destValue := reflect.ValueOf(dest)
	if destValue.Kind() != reflect.Pointer || destValue.Elem().Kind() != reflect.Slice {
		status = statusFailed

		return fmt.Errorf("dest must be a pointer to a slice")
	}

	var rows []string

	err := c.doWithRetry(ctx, operation, func(attemptCtx context.Context) error {
		// Reset rows for each retry attempt
		rows = nil
		colStr := new(proto.ColStr)

		return c.pool.Do(attemptCtx, ch.Query{
			Body: query + " FORMAT JSONEachRow",
			Result: proto.Results{
				{Name: "", Data: colStr},
			},
			OnResult: func(ctx context.Context, block proto.Block) error {
				for i := 0; i < colStr.Rows(); i++ {
					rows = append(rows, colStr.Row(i))
				}

				return nil
			},
		})
	})
	if err != nil {
		status = statusFailed

		return fmt.Errorf("query execution failed: %w", err)
	}

	// Create a slice of the appropriate type
	sliceType := destValue.Elem().Type()
	elemType := sliceType.Elem()
	newSlice := reflect.MakeSlice(sliceType, len(rows), len(rows))

	// Unmarshal each row
	for i, row := range rows {
		elem := reflect.New(elemType)
		if err := json.Unmarshal([]byte(row), elem.Interface()); err != nil {
			status = statusFailed

			return fmt.Errorf("failed to unmarshal row %d: %w", i, err)
		}

		newSlice.Index(i).Set(elem.Elem())
	}

	// Set the result
	destValue.Elem().Set(newSlice)

	return nil
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
