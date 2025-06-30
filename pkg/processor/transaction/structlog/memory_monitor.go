package structlog

import (
	"runtime"
	"time"

	"github.com/sirupsen/logrus"
)

// MemoryStats captures current memory usage
type MemoryStats struct {
	AllocMB      uint64
	TotalAllocMB uint64
	SysMB        uint64
	NumGC        uint32
	NumGoroutine int
}

// GetMemoryStats returns current memory statistics
func GetMemoryStats() MemoryStats {
	var m runtime.MemStats

	runtime.ReadMemStats(&m)

	return MemoryStats{
		AllocMB:      m.Alloc / 1024 / 1024,
		TotalAllocMB: m.TotalAlloc / 1024 / 1024,
		SysMB:        m.Sys / 1024 / 1024,
		NumGC:        m.NumGC,
		NumGoroutine: runtime.NumGoroutine(),
	}
}

// LogMemoryWarning logs a warning when memory usage is high
func LogMemoryWarning(log logrus.FieldLogger, component string, threshold uint64) {
	stats := GetMemoryStats()

	if stats.AllocMB > threshold {
		log.WithFields(logrus.Fields{
			"component":     component,
			"alloc_mb":      stats.AllocMB,
			"sys_mb":        stats.SysMB,
			"num_goroutine": stats.NumGoroutine,
			"num_gc":        stats.NumGC,
			"threshold_mb":  threshold,
		}).Warn("High memory usage detected")
	}
}

// MemoryMonitor monitors memory usage periodically
type MemoryMonitor struct {
	log         logrus.FieldLogger
	interval    time.Duration
	thresholdMB uint64
	stopCh      chan struct{}
}

// NewMemoryMonitor creates a new memory monitor
func NewMemoryMonitor(log logrus.FieldLogger, interval time.Duration, thresholdMB uint64) *MemoryMonitor {
	return &MemoryMonitor{
		log:         log.WithField("component", "memory_monitor"),
		interval:    interval,
		thresholdMB: thresholdMB,
		stopCh:      make(chan struct{}),
	}
}

// Start begins monitoring memory usage
func (m *MemoryMonitor) Start() {
	go func() {
		ticker := time.NewTicker(m.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				stats := GetMemoryStats()

				// Log warning if memory usage is high
				if stats.AllocMB > m.thresholdMB {
					m.log.WithFields(logrus.Fields{
						"alloc_mb":       stats.AllocMB,
						"total_alloc_mb": stats.TotalAllocMB,
						"sys_mb":         stats.SysMB,
						"num_goroutine":  stats.NumGoroutine,
						"num_gc":         stats.NumGC,
					}).Warn("Memory usage exceeds threshold")
				}

				// Always log periodic stats at debug level
				m.log.WithFields(logrus.Fields{
					"alloc_mb":      stats.AllocMB,
					"sys_mb":        stats.SysMB,
					"num_goroutine": stats.NumGoroutine,
				}).Debug("Memory usage stats")

			case <-m.stopCh:
				return
			}
		}
	}()
}

// Stop stops the memory monitor
func (m *MemoryMonitor) Stop() {
	close(m.stopCh)
}
