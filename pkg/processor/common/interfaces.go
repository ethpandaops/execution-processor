package common

import (
	"context"

	"github.com/hibiken/asynq"
)

// Processor defines the main interface for block processors
type Processor interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Name() string
}

// BlockProcessor handles block discovery and processing
type BlockProcessor interface {
	Processor
	ProcessNextBlock(ctx context.Context) error

	// Queue management
	GetQueues() []QueueInfo
	GetHandlers() map[string]asynq.HandlerFunc

	// Task enqueueing (for internal use)
	EnqueueTask(ctx context.Context, task *asynq.Task, opts ...asynq.Option) error

	// Processing mode configuration
	SetProcessingMode(mode string)
}

// QueueInfo contains information about a processor queue
type QueueInfo struct {
	Name     string
	Priority int
}
