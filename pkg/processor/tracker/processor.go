// Package tracker provides block processing coordination and tracking.
//
// # Processing Pipeline
//
// The execution-processor uses a multi-stage pipeline for block processing:
//
//	┌─────────────────┐
//	│  Block Source   │  Execution node provides blocks
//	└────────┬────────┘
//	         │
//	         ▼
//	┌─────────────────┐
//	│ ProcessNextBlock│  Manager calls processor to discover next block
//	└────────┬────────┘
//	         │
//	         ▼
//	┌─────────────────┐
//	│  Task Creation  │  Processor creates tasks for each transaction
//	└────────┬────────┘
//	         │
//	         ▼
//	┌─────────────────┐
//	│  Redis/Asynq    │  Tasks enqueued to distributed queue
//	└────────┬────────┘
//	         │
//	         ▼
//	┌─────────────────┐
//	│  Worker Handler │  Asynq workers process tasks (may be distributed)
//	└────────┬────────┘
//	         │
//	         ▼
//	┌─────────────────┐
//	│   ClickHouse    │  Data inserted using columnar protocol
//	└────────┬────────┘
//	         │
//	         ▼
//	┌─────────────────┐
//	│Block Completion │  Pending tracker marks block complete when all tasks finish
//	└─────────────────┘
//
// # Processing Modes
//
//   - FORWARDS_MODE: Process blocks from current head forward (real-time)
//   - BACKWARDS_MODE: Process blocks from start point backward (backfill)
//
// # Backpressure Control
//
// The pipeline uses MaxPendingBlockRange to control backpressure:
//   - Limits concurrent incomplete blocks
//   - Prevents memory exhaustion during slow ClickHouse inserts
//   - Uses Redis to track pending tasks per block
package tracker

import (
	"context"
	"time"

	"github.com/hibiken/asynq"
)

// Default configuration values for processors.
// These provide a single source of truth for default configuration.
const (
	// DefaultMaxPendingBlockRange is the maximum distance between the oldest
	// incomplete block and the current block before blocking new block processing.
	DefaultMaxPendingBlockRange = 2

	// DefaultClickHouseTimeout is the default timeout for ClickHouse operations.
	DefaultClickHouseTimeout = 30 * time.Second

	// DefaultTraceTimeout is the default timeout for trace fetching operations.
	DefaultTraceTimeout = 30 * time.Second
)

// Processor defines the base interface for all processors.
// Processors are responsible for transforming blockchain data
// into a format suitable for storage and analysis.
type Processor interface {
	// Start initializes the processor and its dependencies (e.g., ClickHouse).
	Start(ctx context.Context) error

	// Stop gracefully shuts down the processor.
	Stop(ctx context.Context) error

	// Name returns the unique identifier for this processor.
	Name() string
}

// BlockProcessor extends Processor with block-level processing capabilities.
// It coordinates the full pipeline from block discovery through task completion.
//
// Pipeline stages managed by BlockProcessor:
//  1. Block Discovery: ProcessNextBlock identifies the next block to process
//  2. Task Creation: Creates distributed tasks for each unit of work
//  3. Queue Management: Manages Asynq queues for forwards/backwards processing
//  4. Task Handling: Worker handlers process individual tasks
//  5. Completion Tracking: Marks blocks complete when all tasks finish
type BlockProcessor interface {
	Processor

	// ProcessNextBlock discovers and enqueues tasks for the next block.
	// Returns an error if no block is available or processing fails.
	ProcessNextBlock(ctx context.Context) error

	// GetQueues returns the Asynq queues used by this processor.
	// Queues have priorities to control processing order.
	GetQueues() []QueueInfo

	// GetHandlers returns task handlers for Asynq worker registration.
	// These handlers process the distributed tasks.
	GetHandlers() map[string]asynq.HandlerFunc

	// EnqueueTask adds a task to the distributed queue.
	// Uses infinite retries to ensure eventual processing.
	EnqueueTask(ctx context.Context, task *asynq.Task, opts ...asynq.Option) error

	// SetProcessingMode configures forwards or backwards processing.
	SetProcessingMode(mode string)
}

// QueueInfo contains information about a processor queue.
type QueueInfo struct {
	Name     string
	Priority int
}

const (
	BACKWARDS_MODE = "backwards"
	FORWARDS_MODE  = "forwards"
)
