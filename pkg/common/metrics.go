package common

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	BlockHeight = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "execution_processor_block_height",
		Help: "Current block height being processed",
	}, []string{"network", "processor"})

	BlocksStored = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "execution_processor_blocks_stored",
		Help: "Range of blocks stored in database",
	}, []string{"network", "processor", "boundary"})

	HeadDistance = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "execution_processor_head_distance",
		Help: "Distance between current processing block and head (execution node head when limiter disabled, beacon chain head when enabled)",
	}, []string{"network", "processor", "head_type"})

	BlocksProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "execution_processor_blocks_processed_total",
		Help: "Total number of blocks processed",
	}, []string{"network", "processor"})

	BlockProcessingDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "execution_processor_block_processing_duration_seconds",
		Help:    "Time taken to process a block",
		Buckets: prometheus.ExponentialBuckets(0.1, 2, 10),
	}, []string{"network", "processor"})

	TasksEnqueued = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "execution_processor_tasks_enqueued_total",
		Help: "Total number of tasks enqueued",
	}, []string{"network", "processor", "queue", "task_type"})

	TasksProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "execution_processor_tasks_processed_total",
		Help: "Total number of tasks processed",
	}, []string{"network", "processor", "queue", "task_type", "status"})

	TaskProcessingDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "execution_processor_task_processing_duration_seconds",
		Help:    "Time taken to process a task",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
	}, []string{"network", "processor", "queue", "task_type"})

	QueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "execution_processor_queue_depth",
		Help: "Current number of tasks in queue",
	}, []string{"network", "processor", "queue"})

	QueueArchivedItems = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "execution_processor_queue_archived_items",
		Help: "Number of archived items in queue",
	}, []string{"network", "processor", "queue"})

	ProcessorErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "execution_processor_errors_total",
		Help: "Total number of processor errors",
	}, []string{"network", "processor", "operation", "error_type"})

	TasksErrored = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "execution_processor_tasks_errored_total",
		Help: "Total number of tasks that encountered errors",
	}, []string{"network", "processor", "queue", "task_type", "error_type"})

	RPCCallDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "execution_processor_rpc_call_duration_seconds",
		Help:    "Duration of RPC calls to Ethereum nodes",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
	}, []string{"chain_id", "node", "method", "status"})

	RPCCallsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "execution_processor_rpc_calls_total",
		Help: "Total RPC calls made to Ethereum nodes",
	}, []string{"chain_id", "node", "method", "status"})

	TransactionsProcessed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "execution_processor_transactions_processed_total",
		Help: "Total transactions processed",
	}, []string{"network", "processor", "status"})

	TransactionProcessingDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "execution_processor_transaction_processing_duration_seconds",
		Help:    "Time to process individual transactions",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 15),
	}, []string{"network", "processor"})

	ClickHouseOperationDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "execution_processor_clickhouse_operation_duration_seconds",
		Help:    "Duration of ClickHouse operations",
		Buckets: prometheus.ExponentialBuckets(0.001, 2, 12),
	}, []string{"network", "processor", "operation", "table", "status", "error_code"})

	ClickHouseOperationTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "execution_processor_clickhouse_operation_total",
		Help: "Total number of ClickHouse operations",
	}, []string{"network", "processor", "operation", "table", "status", "error_code"})

	ClickHouseConnectionsActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "execution_processor_clickhouse_connections_active",
		Help: "Active ClickHouse connections",
	}, []string{"network", "processor"})

	ClickHouseInsertsRows = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "execution_processor_clickhouse_inserted_rows_total",
		Help: "Total number of rows inserted into ClickHouse",
	}, []string{"network", "processor", "table", "status", "error_code"})

	LeaderElectionStatus = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "execution_processor_leader_election_status",
		Help: "Current leader election status (1 = leader, 0 = follower)",
	}, []string{"network", "node_id"})

	LeaderElectionTransitions = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "execution_processor_leader_election_transitions_total",
		Help: "Total number of leader election transitions",
	}, []string{"network", "node_id", "transition"})

	LeaderElectionDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "execution_processor_leader_election_duration_seconds",
		Help:    "Duration in seconds this node held leadership",
		Buckets: prometheus.ExponentialBuckets(1, 2, 15),
	}, []string{"network", "node_id"})

	LeaderElectionErrors = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "execution_processor_leader_election_errors_total",
		Help: "Total number of errors during leader election",
	}, []string{"network", "node_id", "operation"})

	// Queue control metrics
	QueueBackpressureActive = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "execution_processor_queue_backpressure_active",
		Help: "Whether backpressure is active (1) or not (0) for a processor",
	}, []string{"network", "processor"})

	QueueHighWaterMark = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "execution_processor_queue_high_water_mark",
		Help: "Highest queue depth observed",
	}, []string{"network", "processor", "queue"})

	BlockProcessingSkipped = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "execution_processor_block_processing_skipped_total",
		Help: "Total number of times block processing was skipped",
	}, []string{"network", "processor", "reason"})

	// Batch collector metrics
	BatchCollectorFlushes = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "execution_processor_batch_collector_flushes_total",
		Help: "Total number of batch flushes",
	}, []string{"network", "processor", "status"})

	BatchCollectorFlushDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "execution_processor_batch_collector_flush_duration_seconds",
		Help:    "Time taken to flush a batch",
		Buckets: prometheus.ExponentialBuckets(0.01, 2, 12),
	}, []string{"network", "processor", "status"})

	BatchCollectorRowsFlushed = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "execution_processor_batch_collector_rows_flushed_total",
		Help: "Total number of rows flushed in batches",
	}, []string{"network", "processor"})

	BatchCollectorQueueDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "execution_processor_batch_collector_queue_depth",
		Help: "Number of tasks waiting in batch collector queue",
	}, []string{"network", "processor"})

	BatchCollectorBatchSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "execution_processor_batch_collector_batch_size",
		Help:    "Size of batches being flushed",
		Buckets: prometheus.ExponentialBuckets(10, 2, 15),
	}, []string{"network", "processor"})

	// Memory monitoring metrics
	MemoryUsage = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "execution_processor_memory_usage_bytes",
		Help: "Current memory usage in bytes",
	}, []string{"type"}) // type: alloc, sys, heap_alloc, heap_sys

	GoroutineCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "execution_processor_goroutines",
		Help: "Current number of goroutines",
	})

	BatchCollectorSize = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "execution_processor_batch_collector_size",
		Help: "Current size of batch collector accumulation",
	}, []string{"network", "processor", "type"}) // type: rows, tasks, memory_estimate_mb

	TransactionTraceSize = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "execution_processor_transaction_trace_size",
		Help:    "Size of transaction traces in structlog count",
		Buckets: []float64{10, 50, 100, 500, 1000, 5000, 10000, 50000, 100000},
	}, []string{"network", "processor"})

	RetryCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "execution_processor_retry_count_total",
		Help: "Total number of retry attempts",
	}, []string{"network", "processor", "reason"})

	MemoryPressureEvents = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "execution_processor_memory_pressure_events_total",
		Help: "Total number of memory pressure events",
	}, []string{"component", "severity"}) // severity: warning, critical

	VerificationMismatchRate = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "execution_processor_verification_mismatch_total",
		Help: "Total number of verification mismatches",
	}, []string{"network", "processor", "transaction_hash"})

	LargeTransactionCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "execution_processor_large_transaction_total",
		Help: "Total number of large transactions processed",
	}, []string{"network", "processor", "size_category"}) // size_category: 1k-10k, 10k-50k, 50k+
)
