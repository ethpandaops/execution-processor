package structlog

import (
	"context"
	"sync"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/hibiken/asynq"
	"github.com/sirupsen/logrus"
)

// BatchItem represents a single transaction's data within a batch.
type BatchItem struct {
	structlogs []Structlog
	task       *asynq.Task
	payload    *ProcessPayload
}

// BatchManager accumulates small transactions and flushes them in batches.
type BatchManager struct {
	mu             sync.Mutex
	items          []BatchItem
	currentSize    int64
	flushThreshold int64
	maxSize        int64
	flushInterval  time.Duration
	flushTimer     *time.Timer
	flushCh        chan struct{}
	processor      *Processor
	cancel         context.CancelFunc
	wg             sync.WaitGroup
	stopCh         chan struct{}
}

// NewBatchManager creates a new batch manager.
func NewBatchManager(processor *Processor, config *Config) *BatchManager {
	_, cancel := context.WithCancel(context.Background())

	return &BatchManager{
		items:          make([]BatchItem, 0, 100),
		flushThreshold: config.BatchInsertThreshold,
		maxSize:        config.BatchMaxSize,
		flushInterval:  config.BatchFlushInterval,
		flushCh:        make(chan struct{}, 1),
		processor:      processor,
		cancel:         cancel,
		stopCh:         make(chan struct{}),
	}
}

// Start begins the batch manager's flush goroutine.
func (bm *BatchManager) Start() error {
	bm.wg.Add(1)

	go bm.flushLoop()

	bm.processor.log.WithFields(logrus.Fields{
		"flush_threshold": bm.flushThreshold,
		"max_size":        bm.maxSize,
		"flush_interval":  bm.flushInterval,
	}).Info("batch manager started")

	return nil
}

// Stop gracefully shuts down the batch manager.
func (bm *BatchManager) Stop() {
	bm.cancel()
	close(bm.stopCh)

	// Flush any remaining items
	bm.Flush()

	// Wait for flush loop to exit
	bm.wg.Wait()

	bm.processor.log.Info("batch manager stopped")
}

// Add adds a transaction's structlogs to the batch.
func (bm *BatchManager) Add(structlogs []Structlog, task *asynq.Task, payload *ProcessPayload) error {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	// Check if adding this would exceed max size
	newSize := bm.currentSize + int64(len(structlogs))
	if newSize > bm.maxSize {
		// Flush current batch first
		bm.flushLocked()
	}

	// Add to batch
	bm.items = append(bm.items, BatchItem{
		structlogs: structlogs,
		task:       task,
		payload:    payload,
	})
	bm.currentSize += int64(len(structlogs))

	// Reset flush timer
	if bm.flushTimer != nil {
		bm.flushTimer.Stop()
	}

	bm.flushTimer = time.AfterFunc(bm.flushInterval, func() {
		select {
		case bm.flushCh <- struct{}{}:
		default:
		}
	})

	// Check if we should flush
	bm.FlushIfNeeded()

	return nil
}

// FlushIfNeeded checks if the batch should be flushed based on size threshold.
func (bm *BatchManager) FlushIfNeeded() {
	if bm.currentSize >= bm.flushThreshold {
		bm.flushLocked()
	}
}

// Flush performs a batch flush (thread-safe).
func (bm *BatchManager) Flush() {
	bm.mu.Lock()
	defer bm.mu.Unlock()

	bm.flushLocked()
}

// flushLocked performs the actual flush (must be called with lock held).
func (bm *BatchManager) flushLocked() {
	if len(bm.items) == 0 {
		return
	}

	// Stop the flush timer
	if bm.flushTimer != nil {
		bm.flushTimer.Stop()
		bm.flushTimer = nil
	}

	// Collect all structlogs
	var allStructlogs []Structlog
	for _, item := range bm.items {
		allStructlogs = append(allStructlogs, item.structlogs...)
	}

	// Log batch details
	bm.processor.log.WithFields(logrus.Fields{
		"batch_items":      len(bm.items),
		"batch_structlogs": len(allStructlogs),
	}).Debug("flushing batch")

	// Perform bulk insert
	startTime := time.Now()

	err := bm.processor.insertStructlogs(context.Background(), allStructlogs)
	if err != nil {
		// Failed - fail all tasks in batch
		bm.processor.log.WithError(err).Error("batch insert failed")

		for _, item := range bm.items {
			// Increment error metrics
			common.TasksErrored.WithLabelValues(
				bm.processor.network.Name,
				ProcessorName,
				item.payload.ProcessingMode,
				ProcessForwardsTaskType,
				"batch_insert_error",
			).Inc()
		}
	} else {
		// Success - complete all tasks
		for _, item := range bm.items {
			// Increment success metrics
			common.TasksProcessed.WithLabelValues(
				bm.processor.network.Name,
				ProcessorName,
				item.payload.ProcessingMode,
				ProcessForwardsTaskType,
				"success",
			).Inc()
		}

		// Record batch metrics using ClickHouse metrics
		common.ClickHouseInsertsRows.WithLabelValues(
			bm.processor.network.Name,
			ProcessorName,
			"structlogs_batch",
			"success",
			"",
		).Add(float64(len(allStructlogs)))

		common.ClickHouseOperationDuration.WithLabelValues(
			bm.processor.network.Name,
			ProcessorName,
			"batch_insert",
			"structlogs_batch",
			"success",
			"",
		).Observe(time.Since(startTime).Seconds())
	}

	// Clear the batch
	bm.items = bm.items[:0]
	bm.currentSize = 0
}

// flushLoop runs the time-based flush goroutine.
func (bm *BatchManager) flushLoop() {
	defer bm.wg.Done()

	for {
		select {
		case <-bm.stopCh:
			return
		case <-bm.flushCh:
			bm.Flush()
		}
	}
}
