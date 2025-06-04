package structlog

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/sirupsen/logrus"
)

// TaskBatch represents a batch of structlog rows from a single task
type TaskBatch struct {
	Rows         []Structlog // All rows from one task
	ResponseChan chan error  // Channel to send result back to task
	TaskID       string      // Unique identifier for this task
}

// BatchCollector aggregates structlog rows from multiple tasks and flushes them in batches
type BatchCollector struct {
	processor     *Processor
	taskChannel   chan TaskBatch
	maxBatchSize  int
	flushInterval time.Duration
	shutdown      chan struct{}
	wg            sync.WaitGroup
	log           logrus.FieldLogger

	// State tracking (protected by mu)
	mu              sync.Mutex
	accumulatedRows []Structlog
	pendingTasks    []TaskBatch
}

// NewBatchCollector creates a new batch collector
func NewBatchCollector(processor *Processor, config BatchConfig) *BatchCollector {
	return &BatchCollector{
		processor:     processor,
		taskChannel:   make(chan TaskBatch, config.ChannelBufferSize),
		maxBatchSize:  config.MaxRows,
		flushInterval: config.FlushInterval,
		shutdown:      make(chan struct{}),
		log:           processor.log.WithField("component", "batch_collector"),
	}
}

// Start begins the batch collection process
func (bc *BatchCollector) Start(ctx context.Context) error {
	bc.log.WithFields(logrus.Fields{
		"max_batch_size":      bc.maxBatchSize,
		"flush_interval":      bc.flushInterval,
		"channel_buffer_size": cap(bc.taskChannel),
	}).Info("Starting batch collector")

	bc.wg.Add(1)
	go bc.run(ctx)

	return nil
}

// Stop gracefully shuts down the batch collector
func (bc *BatchCollector) Stop(ctx context.Context) error {
	bc.log.Info("Stopping batch collector")

	close(bc.shutdown)
	bc.wg.Wait()

	bc.log.Info("Batch collector stopped")

	return nil
}

// SubmitBatch submits a task batch for processing
func (bc *BatchCollector) SubmitBatch(taskBatch TaskBatch) error {
	select {
	case bc.taskChannel <- taskBatch:
		return nil
	case <-bc.shutdown:
		return context.Canceled
	default:
		// Channel is full, caller should fallback to direct insert
		return ErrChannelFull
	}
}

// run is the main goroutine that processes batches
func (bc *BatchCollector) run(ctx context.Context) {
	defer bc.wg.Done()
	defer bc.flushRemaining()

	bc.log.Debug("Batch collector started")

	// Create a ticker for periodic flushes
	ticker := time.NewTicker(bc.flushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-bc.shutdown:
			bc.log.Debug("Batch collector received shutdown signal")

			return

		case taskBatch := <-bc.taskChannel:
			bc.mu.Lock()
			bc.processPendingTask(taskBatch)
			bc.mu.Unlock()

		case <-ticker.C:
			bc.mu.Lock()
			if len(bc.accumulatedRows) > 0 {
				bc.log.WithField("trigger", "timer").Debug("Flushing batch due to timer")
				bc.flushBatch(ctx)
			}
			bc.mu.Unlock()
		}
	}
}

// processPendingTask adds a task to the current batch and flushes if needed
func (bc *BatchCollector) processPendingTask(taskBatch TaskBatch) {
	// Add rows to accumulated batch
	bc.accumulatedRows = append(bc.accumulatedRows, taskBatch.Rows...)
	bc.pendingTasks = append(bc.pendingTasks, taskBatch)

	bc.log.WithFields(logrus.Fields{
		"task_id":          taskBatch.TaskID,
		"task_rows":        len(taskBatch.Rows),
		"accumulated_rows": len(bc.accumulatedRows),
		"pending_tasks":    len(bc.pendingTasks),
	}).Debug("Added task to batch")

	// Check if we should flush due to size
	if len(bc.accumulatedRows) >= bc.maxBatchSize {
		bc.log.WithField("trigger", "size").Debug("Flushing batch due to size limit")
		bc.flushBatch(context.Background())
	}
}

// flushBatch performs the actual database insert and responds to all pending tasks
func (bc *BatchCollector) flushBatch(ctx context.Context) {
	if len(bc.accumulatedRows) == 0 {
		return
	}

	start := time.Now()
	rowCount := len(bc.accumulatedRows)
	taskCount := len(bc.pendingTasks)

	bc.log.WithFields(logrus.Fields{
		"rows":  rowCount,
		"tasks": taskCount,
	}).Debug("Starting batch flush")

	// Perform the batch insert
	err := bc.processor.insertStructlogBatch(ctx, bc.accumulatedRows)

	duration := time.Since(start)

	// Update metrics
	if err != nil {
		common.BatchCollectorFlushes.WithLabelValues(bc.processor.network.Name, ProcessorName, "failed").Inc()
		common.BatchCollectorFlushDuration.WithLabelValues(bc.processor.network.Name, ProcessorName, "failed").Observe(duration.Seconds())
		bc.log.WithError(err).WithFields(logrus.Fields{
			"rows":     rowCount,
			"tasks":    taskCount,
			"duration": duration,
		}).Error("Batch flush failed")
	} else {
		common.BatchCollectorFlushes.WithLabelValues(bc.processor.network.Name, ProcessorName, "success").Inc()
		common.BatchCollectorFlushDuration.WithLabelValues(bc.processor.network.Name, ProcessorName, "success").Observe(duration.Seconds())
		common.BatchCollectorRowsFlushed.WithLabelValues(bc.processor.network.Name, ProcessorName).Add(float64(rowCount))
		bc.log.WithFields(logrus.Fields{
			"rows":     rowCount,
			"tasks":    taskCount,
			"duration": duration,
		}).Info("Batch flush completed successfully")
	}

	// Respond to all pending tasks with the same result
	for _, task := range bc.pendingTasks {
		select {
		case task.ResponseChan <- err:
			// Successfully sent response
		default:
			// Response channel might be closed, log warning
			bc.log.WithField("task_id", task.TaskID).Warn("Failed to send response to task")
		}
	}

	// Reset state
	bc.accumulatedRows = nil
	bc.pendingTasks = nil
}

// flushRemaining flushes any remaining rows during shutdown
func (bc *BatchCollector) flushRemaining() {
	bc.mu.Lock()
	defer bc.mu.Unlock()

	if len(bc.accumulatedRows) > 0 {
		bc.log.WithFields(logrus.Fields{
			"rows":  len(bc.accumulatedRows),
			"tasks": len(bc.pendingTasks),
		}).Info("Flushing remaining rows during shutdown")

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		bc.flushBatch(ctx)
	}
}

// Custom error for when channel is full
var ErrChannelFull = fmt.Errorf("batch collector channel is full")
