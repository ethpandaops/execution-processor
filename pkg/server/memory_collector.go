package server

import (
	"context"
	"fmt"
	"runtime"
	"time"

	"github.com/ethpandaops/execution-processor/pkg/common"
	"github.com/sirupsen/logrus"
)

// MemoryStatsCollector collects memory statistics periodically
type MemoryStatsCollector struct {
	log            logrus.FieldLogger
	config         MemoryMonitorConfig
	stopCh         chan struct{}
	lastAllocBytes uint64
	maxAllocBytes  uint64
}

// NewMemoryStatsCollector creates a new memory stats collector
func NewMemoryStatsCollector(log logrus.FieldLogger, config MemoryMonitorConfig) *MemoryStatsCollector {
	return &MemoryStatsCollector{
		log:    log.WithField("component", "memory_stats_collector"),
		config: config,
		stopCh: make(chan struct{}),
	}
}

// Start begins collecting memory statistics
func (m *MemoryStatsCollector) Start(ctx context.Context) error {
	if !m.config.Enabled {
		m.log.Info("Memory stats collector is disabled")

		return nil
	}

	m.log.WithFields(logrus.Fields{
		"interval":              m.config.Interval,
		"warning_threshold_mb":  m.config.WarningThresholdMB,
		"critical_threshold_mb": m.config.CriticalThresholdMB,
	}).Info("Starting memory stats collector")

	go m.run(ctx)

	return nil
}

// Stop stops the memory stats collector
func (m *MemoryStatsCollector) Stop(ctx context.Context) error {
	m.log.Info("Stopping memory stats collector")

	close(m.stopCh)

	return nil
}

func (m *MemoryStatsCollector) run(ctx context.Context) {
	ticker := time.NewTicker(m.config.Interval)
	defer ticker.Stop()

	// Collect initial stats
	m.collectStats()

	for {
		select {
		case <-ctx.Done():
			return
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.collectStats()
		}
	}
}

func (m *MemoryStatsCollector) collectStats() {
	var memStats runtime.MemStats

	runtime.ReadMemStats(&memStats)

	// Update memory metrics
	common.MemoryUsage.WithLabelValues("alloc").Set(float64(memStats.Alloc))
	common.MemoryUsage.WithLabelValues("sys").Set(float64(memStats.Sys))
	common.MemoryUsage.WithLabelValues("heap_alloc").Set(float64(memStats.HeapAlloc))
	common.MemoryUsage.WithLabelValues("heap_sys").Set(float64(memStats.HeapSys))

	// Update goroutine count
	common.GoroutineCount.Set(float64(runtime.NumGoroutine()))

	// Convert to MB for logging
	allocMB := memStats.Alloc / 1024 / 1024
	sysMB := memStats.Sys / 1024 / 1024
	heapAllocMB := memStats.HeapAlloc / 1024 / 1024
	heapSysMB := memStats.HeapSys / 1024 / 1024

	// Track memory spikes
	var spikeMB int64 = 0

	if m.lastAllocBytes > 0 {
		// Calculate spike in MB directly to avoid overflow
		allocMBNow := int64(memStats.Alloc / 1024 / 1024)    // #nosec G115 - division prevents overflow
		allocMBLast := int64(m.lastAllocBytes / 1024 / 1024) // #nosec G115 - division prevents overflow
		spikeMB = allocMBNow - allocMBLast
	}

	m.lastAllocBytes = memStats.Alloc

	// Track max allocation
	if memStats.Alloc > m.maxAllocBytes {
		m.maxAllocBytes = memStats.Alloc
	}

	// Log memory summary at info level
	fields := logrus.Fields{
		"alloc_mb":      allocMB,
		"sys_mb":        sysMB,
		"heap_alloc_mb": heapAllocMB,
		"heap_sys_mb":   heapSysMB,
		"goroutines":    runtime.NumGoroutine(),
		"num_gc":        memStats.NumGC,
		"gc_cpu_pct":    fmt.Sprintf("%.2f", memStats.GCCPUFraction*100),
		"max_alloc_mb":  m.maxAllocBytes / 1024 / 1024,
	}

	// Add spike info if significant
	if spikeMB != 0 {
		fields["spike_mb"] = spikeMB

		// Log warning for large spikes (>100MB increase)
		if spikeMB > 100 {
			m.log.WithFields(fields).Warnf("Large memory spike detected: +%d MB", spikeMB)

			return
		}
	}

	m.log.WithFields(fields).Info("Memory usage summary")

	// Log high memory usage based on configured thresholds
	allocMB = memStats.Alloc / 1024 / 1024
	if allocMB > m.config.WarningThresholdMB {
		logFields := logrus.Fields{
			"alloc_mb":       allocMB,
			"sys_mb":         memStats.Sys / 1024 / 1024,
			"heap_alloc_mb":  memStats.HeapAlloc / 1024 / 1024,
			"heap_sys_mb":    memStats.HeapSys / 1024 / 1024,
			"num_gc":         memStats.NumGC,
			"num_goroutines": runtime.NumGoroutine(),
			"threshold_mb":   m.config.WarningThresholdMB,
		}

		// Check if it's critical
		if allocMB > m.config.CriticalThresholdMB {
			logFields["threshold_mb"] = m.config.CriticalThresholdMB
			m.log.WithFields(logFields).Error("Critical memory usage detected")
			common.MemoryPressureEvents.WithLabelValues("system", "critical").Inc()
		} else {
			m.log.WithFields(logFields).Warn("High memory usage detected")
			common.MemoryPressureEvents.WithLabelValues("system", "warning").Inc()
		}
	}
}
