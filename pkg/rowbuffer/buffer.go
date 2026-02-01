// Package rowbuffer provides thread-safe row batching for ClickHouse inserts.
// It pools rows in memory across concurrent tasks and flushes when hitting
// a row limit or timer interval.
package rowbuffer

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/ethpandaops/execution-processor/pkg/common"
)

// FlushFunc is the function called to flush rows to the database.
type FlushFunc[R any] func(ctx context.Context, rows []R) error

// Config holds configuration for the row buffer.
type Config struct {
	MaxRows       int           // Flush threshold (default: 100000)
	FlushInterval time.Duration // Max wait before flush (default: 1s)
	Network       string        // For metrics
	Processor     string        // For metrics
	Table         string        // For metrics
}

// waiter represents a task waiting for its rows to be flushed.
type waiter struct {
	resultCh chan<- error
	rowCount int
}

// Buffer provides thread-safe row batching for ClickHouse inserts.
type Buffer[R any] struct {
	mu      sync.Mutex
	rows    []R
	waiters []waiter

	config  Config
	flushFn FlushFunc[R]
	log     logrus.FieldLogger

	stopChan    chan struct{}
	stoppedChan chan struct{}
	wg          sync.WaitGroup
	started     bool
}

// New creates a new Buffer with the given configuration and flush function.
func New[R any](cfg Config, flushFn FlushFunc[R], log logrus.FieldLogger) *Buffer[R] {
	// Set defaults
	if cfg.MaxRows <= 0 {
		cfg.MaxRows = 100000
	}

	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = time.Second
	}

	return &Buffer[R]{
		rows:        make([]R, 0, cfg.MaxRows),
		waiters:     make([]waiter, 0, 64),
		config:      cfg,
		flushFn:     flushFn,
		log:         log.WithField("component", "rowbuffer"),
		stopChan:    make(chan struct{}),
		stoppedChan: make(chan struct{}),
	}
}

// Start starts the flush timer goroutine.
func (b *Buffer[R]) Start(ctx context.Context) error {
	b.mu.Lock()

	if b.started {
		b.mu.Unlock()

		return nil
	}

	b.started = true
	b.mu.Unlock()

	// Go 1.25: cleaner goroutine spawning with WaitGroup.Go
	b.wg.Go(func() { b.runFlushTimer(ctx) })

	b.log.WithFields(logrus.Fields{
		"max_rows":       b.config.MaxRows,
		"flush_interval": b.config.FlushInterval,
	}).Debug("Row buffer started")

	return nil
}

// Stop stops the buffer, flushing any remaining rows.
func (b *Buffer[R]) Stop(ctx context.Context) error {
	b.mu.Lock()

	if !b.started {
		b.mu.Unlock()

		return nil
	}

	b.started = false
	b.mu.Unlock()

	// Signal the flush timer to stop
	close(b.stopChan)

	// Wait for the flush timer to exit
	b.wg.Wait()

	// Flush any remaining rows
	b.mu.Lock()

	if len(b.rows) > 0 {
		rows := b.rows
		waiters := b.waiters
		b.rows = make([]R, 0, b.config.MaxRows)
		b.waiters = make([]waiter, 0, 64)
		b.mu.Unlock()

		err := b.doFlush(ctx, rows, waiters, "shutdown")
		if err != nil {
			b.log.WithError(err).Error("Failed to flush remaining rows on shutdown")

			return fmt.Errorf("failed to flush remaining rows: %w", err)
		}
	} else {
		b.mu.Unlock()
	}

	close(b.stoppedChan)
	b.log.Debug("Row buffer stopped")

	return nil
}

// Submit adds rows to the buffer and blocks until they are successfully flushed.
// Returns an error if the flush fails or the context is cancelled.
func (b *Buffer[R]) Submit(ctx context.Context, rows []R) error {
	if len(rows) == 0 {
		return nil
	}

	// Create result channel
	resultCh := make(chan error, 1)

	b.mu.Lock()

	// Check if buffer is stopped
	if !b.started {
		b.mu.Unlock()

		return fmt.Errorf("buffer is not started")
	}

	// Add rows and waiter
	b.rows = append(b.rows, rows...)
	b.waiters = append(b.waiters, waiter{resultCh: resultCh, rowCount: len(rows)})

	// Update pending metrics
	common.RowBufferPendingRows.WithLabelValues(
		b.config.Network, b.config.Processor, b.config.Table,
	).Set(float64(len(b.rows)))

	common.RowBufferPendingTasks.WithLabelValues(
		b.config.Network, b.config.Processor, b.config.Table,
	).Set(float64(len(b.waiters)))

	// Check if we should flush (size trigger)
	shouldFlush := len(b.rows) >= b.config.MaxRows

	var flushRows []R

	var flushWaiters []waiter

	if shouldFlush {
		flushRows = b.rows
		flushWaiters = b.waiters
		b.rows = make([]R, 0, b.config.MaxRows)
		b.waiters = make([]waiter, 0, 64)
	}

	b.mu.Unlock()

	// Perform flush outside of lock if triggered by size
	if shouldFlush {
		go func() {
			_ = b.doFlush(context.Background(), flushRows, flushWaiters, "size")
		}()
	}

	// Wait for result or context cancellation
	select {
	case err := <-resultCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	case <-b.stopChan:
		// Buffer is stopping, wait for flush result
		select {
		case err := <-resultCh:
			return err
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// runFlushTimer runs the periodic flush timer.
func (b *Buffer[R]) runFlushTimer(ctx context.Context) {
	ticker := time.NewTicker(b.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-b.stopChan:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.flushOnTimer(ctx)
		}
	}
}

// flushOnTimer attempts to flush buffered rows on timer trigger.
func (b *Buffer[R]) flushOnTimer(ctx context.Context) {
	b.mu.Lock()

	if len(b.rows) == 0 {
		b.mu.Unlock()

		return
	}

	rows := b.rows
	waiters := b.waiters
	b.rows = make([]R, 0, b.config.MaxRows)
	b.waiters = make([]waiter, 0, 64)
	b.mu.Unlock()

	_ = b.doFlush(ctx, rows, waiters, "timer")
}

// doFlush performs the actual flush and notifies all waiters.
func (b *Buffer[R]) doFlush(ctx context.Context, rows []R, waiters []waiter, trigger string) error {
	if len(rows) == 0 {
		return nil
	}

	start := time.Now()
	rowCount := len(rows)

	// Call the flush function
	err := b.flushFn(ctx, rows)

	duration := time.Since(start)

	// Record metrics
	status := "success"
	if err != nil {
		status = "failed"
	}

	common.RowBufferFlushTotal.WithLabelValues(
		b.config.Network, b.config.Processor, b.config.Table, trigger, status,
	).Inc()

	common.RowBufferFlushDuration.WithLabelValues(
		b.config.Network, b.config.Processor, b.config.Table,
	).Observe(duration.Seconds())

	common.RowBufferFlushSize.WithLabelValues(
		b.config.Network, b.config.Processor, b.config.Table,
	).Observe(float64(rowCount))

	// Update pending metrics (now zero after flush)
	b.mu.Lock()
	common.RowBufferPendingRows.WithLabelValues(
		b.config.Network, b.config.Processor, b.config.Table,
	).Set(float64(len(b.rows)))

	common.RowBufferPendingTasks.WithLabelValues(
		b.config.Network, b.config.Processor, b.config.Table,
	).Set(float64(len(b.waiters)))
	b.mu.Unlock()

	// Log the flush
	if err != nil {
		b.log.WithError(err).WithFields(logrus.Fields{
			"rows":      rowCount,
			"waiters":   len(waiters),
			"trigger":   trigger,
			"duration":  duration,
			"processor": b.config.Processor,
			"table":     b.config.Table,
		}).Error("ClickHouse flush failed")
	} else {
		b.log.WithFields(logrus.Fields{
			"rows":      rowCount,
			"waiters":   len(waiters),
			"trigger":   trigger,
			"duration":  duration,
			"processor": b.config.Processor,
			"table":     b.config.Table,
		}).Debug("ClickHouse flush completed")
	}

	// Notify all waiters
	for _, w := range waiters {
		select {
		case w.resultCh <- err:
		default:
			// Waiter may have timed out, skip
		}
	}

	return err
}

// Len returns the current number of buffered rows.
func (b *Buffer[R]) Len() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.rows)
}

// WaiterCount returns the current number of waiting tasks.
func (b *Buffer[R]) WaiterCount() int {
	b.mu.Lock()
	defer b.mu.Unlock()

	return len(b.waiters)
}
