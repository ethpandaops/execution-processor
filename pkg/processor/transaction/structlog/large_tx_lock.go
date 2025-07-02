package structlog

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

// LargeTxLockManager manages sequential processing of large transactions
type LargeTxLockManager struct {
	mu                    sync.Mutex
	active                atomic.Bool
	currentTx             string
	currentSize           int
	currentOperation      string
	startTime             time.Time
	waitingWorkers        atomic.Int32
	waitingLargeTx        atomic.Int32
	log                   logrus.FieldLogger
	config                *LargeTransactionConfig
	lastCompletedTx       string
	lastCompletionTime    time.Time
	totalLargeTxProcessed atomic.Int64
}

// NewLargeTxLockManager creates a new large transaction lock manager
func NewLargeTxLockManager(log logrus.FieldLogger, config *LargeTransactionConfig) *LargeTxLockManager {
	return &LargeTxLockManager{
		log:    log.WithField("component", "large_tx_lock"),
		config: config,
	}
}

// IsLargeTransaction checks if a transaction qualifies as large
func (m *LargeTxLockManager) IsLargeTransaction(structlogCount int) bool {
	return m.config.Enabled && structlogCount >= m.config.StructlogThreshold
}

// AcquireLock attempts to acquire the lock for processing a large transaction
func (m *LargeTxLockManager) AcquireLock(ctx context.Context, txHash string, size int, operation string) error {
	// Increment waiting counter
	m.waitingLargeTx.Add(1)
	defer m.waitingLargeTx.Add(-1)

	// Log if multiple large transactions are waiting
	if waiting := m.waitingLargeTx.Load(); waiting > 1 {
		m.log.WithFields(logrus.Fields{
			"waiting_large_tx": waiting,
			"current_tx":       m.currentTx,
			"tx_hash":          txHash,
			"size":             size,
			"operation":        operation,
		}).Info("Multiple large transactions queued")
	}

	waitStart := time.Now()

	// Try to acquire lock with proper cleanup
	lockChan := make(chan struct{})
	lockAcquired := make(chan bool, 1)

	go func() {
		// Use TryLock first to check if we can acquire immediately
		if m.mu.TryLock() {
			lockAcquired <- true

			close(lockChan)

			return
		}

		// Otherwise wait for the lock, but with ability to cancel
		lockDone := make(chan struct{})
		go func() {
			m.mu.Lock()
			close(lockDone)
		}()

		select {
		case <-lockDone:
			lockAcquired <- true

			close(lockChan)
		case <-ctx.Done():
			// Context cancelled, abandon lock acquisition
			lockAcquired <- false
			// Note: the inner goroutine might still be waiting, but it will
			// eventually get the lock and unlock it when this function returns
			go func() {
				<-lockDone
				m.mu.Unlock()
			}()
		}
	}()

	// Periodic logging ticker
	logTicker := time.NewTicker(10 * time.Second)
	defer logTicker.Stop()

	// Create timeout channel
	timeoutChan := time.After(m.config.WorkerWaitTimeout)

	for {
		select {
		case <-ctx.Done():
			// Context cancelled
			return fmt.Errorf("context cancelled while waiting for large tx lock: %w", ctx.Err())

		case <-timeoutChan:
			// Timeout waiting for lock
			m.log.WithFields(logrus.Fields{
				"tx_hash":       txHash,
				"size":          size,
				"wait_duration": time.Since(waitStart),
				"timeout":       m.config.WorkerWaitTimeout,
				"current_tx":    m.currentTx,
			}).Warn("Timeout waiting for large transaction lock")

			return fmt.Errorf("timeout waiting for large tx lock after %v", m.config.WorkerWaitTimeout)

		case <-logTicker.C:
			// Periodic status log
			m.log.WithFields(logrus.Fields{
				"tx_hash":          txHash,
				"waiting_duration": time.Since(waitStart),
				"current_tx":       m.currentTx,
				"waiting_workers":  m.waitingLargeTx.Load(),
				"operation":        operation,
			}).Info("Still waiting for large transaction lock")
			// Continue waiting

		case <-lockChan:
			// Check if we actually got the lock or if it was cancelled
			if !<-lockAcquired {
				return fmt.Errorf("lock acquisition cancelled")
			}

			// Got the lock!
			m.active.Store(true)
			m.currentTx = txHash
			m.currentSize = size
			m.currentOperation = operation
			m.startTime = time.Now()

			logFields := logrus.Fields{
				"tx_hash":       txHash,
				"size":          size,
				"wait_duration": time.Since(waitStart),
				"operation":     operation,
			}

			// Only add last_completed and time_since_last if there was a previous completion
			if m.lastCompletedTx != "" {
				logFields["last_completed"] = m.lastCompletedTx
				logFields["time_since_last"] = time.Since(m.lastCompletionTime)
			}

			m.log.WithFields(logFields).Info("Large transaction acquired lock")

			return nil
		}
	}
}

// ReleaseLock releases the lock after processing a large transaction
func (m *LargeTxLockManager) ReleaseLock(txHash string) {
	processingDuration := time.Since(m.startTime)
	operation := m.currentOperation

	// Update completion tracking
	m.lastCompletedTx = txHash
	m.lastCompletionTime = time.Now()
	m.totalLargeTxProcessed.Add(1)

	// Clear current state
	m.active.Store(false)
	m.currentTx = ""
	m.currentSize = 0
	m.currentOperation = ""
	m.startTime = time.Time{}

	// Release the lock
	m.mu.Unlock()

	m.log.WithFields(logrus.Fields{
		"tx_hash":             txHash,
		"processing_duration": processingDuration,
		"total_processed":     m.totalLargeTxProcessed.Load(),
		"waiting_large_tx":    m.waitingLargeTx.Load(),
		"operation":           operation,
	}).Info("Large transaction released lock")
}

// WaitForLargeTransaction waits for any active large transaction to complete
func (m *LargeTxLockManager) WaitForLargeTransaction(ctx context.Context, workerTask string) error {
	if !m.active.Load() {
		return nil // No large transaction active
	}

	// Increment waiting counter
	m.waitingWorkers.Add(1)
	defer m.waitingWorkers.Add(-1)

	waitStart := time.Now()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	logTicker := time.NewTicker(10 * time.Second)
	defer logTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("context cancelled while waiting: %w", ctx.Err())

		case <-logTicker.C:
			// Periodic status log
			m.log.WithFields(logrus.Fields{
				"worker_task":      workerTask,
				"current_large_tx": m.currentTx,
				"waiting_duration": time.Since(waitStart),
				"waiting_workers":  m.waitingWorkers.Load(),
			}).Debug("Worker waiting for large transaction")

		case <-ticker.C:
			// Check if large transaction completed
			if !m.active.Load() {
				return nil // Can proceed
			}

			// Check for timeout
			if time.Since(waitStart) > m.config.WorkerWaitTimeout {
				m.log.WithFields(logrus.Fields{
					"worker_task":      workerTask,
					"wait_duration":    time.Since(waitStart),
					"timeout":          m.config.WorkerWaitTimeout,
					"current_large_tx": m.currentTx,
				}).Warn("Worker wait timeout for large transaction")

				return fmt.Errorf("timeout waiting for large tx to complete after %v", m.config.WorkerWaitTimeout)
			}

			// Check if the large transaction itself is taking too long (safety check)
			if m.startTime != (time.Time{}) && time.Since(m.startTime) > m.config.MaxProcessingTime {
				m.log.WithFields(logrus.Fields{
					"current_large_tx":    m.currentTx,
					"processing_duration": time.Since(m.startTime),
					"max_allowed":         m.config.MaxProcessingTime,
				}).Error("Large transaction exceeded max processing time")
			}
		}
	}
}

// HasLock checks if the given transaction currently holds the lock
func (m *LargeTxLockManager) HasLock(txHash string) bool {
	return m.active.Load() && m.currentTx == txHash
}

// GetStatus returns current status of the lock manager
func (m *LargeTxLockManager) GetStatus() map[string]interface{} {
	status := map[string]interface{}{
		"enabled":           m.config.Enabled,
		"threshold":         m.config.StructlogThreshold,
		"active":            m.active.Load(),
		"waiting_workers":   m.waitingWorkers.Load(),
		"waiting_large_tx":  m.waitingLargeTx.Load(),
		"total_processed":   m.totalLargeTxProcessed.Load(),
		"last_completed_tx": m.lastCompletedTx,
		"time_since_last":   time.Since(m.lastCompletionTime).String(),
	}

	if m.active.Load() {
		status["current_tx"] = m.currentTx
		status["current_size"] = m.currentSize
		status["current_operation"] = m.currentOperation
		status["processing_duration"] = time.Since(m.startTime).String()
	}

	return status
}
