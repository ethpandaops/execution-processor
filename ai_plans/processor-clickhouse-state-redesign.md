# Processor, ClickHouse, and State Redesign Implementation Plan

## Executive Summary
> The current execution processor architecture suffers from memory management issues, complex queuing mechanisms, and inefficient database interactions. This redesign simplifies the entire system by:
> - Replacing clickhouse-go with direct HTTP requests for better control and simplicity
> - Streamlining state management to simply return the next block to process
> - Optimizing the processor for memory-efficient structlog processing with streaming capabilities
> - Implementing proper backpressure and memory bounds throughout the system
> 
> The new architecture will handle transaction traces ranging from 10MB to 20GB efficiently, with predictable memory usage and higher throughput.

## Goals & Objectives
### Primary Goals
- **Memory Efficiency**: Process 20GB transaction traces without OOM, maintaining <4GB memory usage
- **Simplicity**: Reduce codebase complexity by 50% while maintaining functionality
- **Performance**: Achieve 10x improvement in structlog processing throughput

### Secondary Objectives
- Remove clickhouse-go dependency in favor of simple HTTP
- Implement proper streaming architecture
- Add comprehensive memory monitoring
- Enable graceful degradation under load
- Maintain distributed processing with Redis/Asynq

## Solution Overview
### Approach
The redesign focuses on three core components with clear boundaries:
1. **HTTP-based ClickHouse client** with generic query/insert capabilities
2. **Simplified state manager** that only determines the next block
3. **Memory-efficient processor** with streaming structlog processing

### Key Components
1. **ClickHouse HTTP Client**: Direct HTTP API usage with JSON format, generic type support, simple error handling
2. **State Manager**: Lightweight next-block resolver with configurable backends
3. **Processor Manager**: Simplified block handler with processor lifecycle management
4. **Structlog Processor**: Streaming architecture with bounded memory usage

### Architecture Diagram
```
[Execution Client] → [Processor Manager] → [Structlog Processor]
                            ↓                      ↓
                     [State Manager]        [Redis/Asynq Queue]
                            ↓                    ↙        ↘
                     [ClickHouse]      [Process Tasks]  [Verify Tasks]
                                              ↓              ↓
                                         [Workers]      [Workers]
                                              ↓              ↓
                                          [ClickHouse HTTP API]
```

### Data Flow
```
Block → Transaction → Stream Structlogs → Batch → Insert
                           ↓
                    Memory Pool ← Release
```

### Expected Outcomes
- Process 1M structlogs/second sustained throughput
- Handle 20GB traces with <4GB memory usage
- 99.9% success rate for transaction processing
- Sub-second block processing latency

## Implementation Tasks

### CRITICAL IMPLEMENTATION RULES
1. **NO PLACEHOLDER CODE**: Every implementation must be production-ready. NEVER write "TODO", "in a real implementation", or similar placeholders unless explicitly requested by the user.
2. **CROSS-DIRECTORY TASKS**: Group related changes across directories into single tasks to ensure consistency. Never create isolated changes that require follow-up work in sibling directories.
3. **COMPLETE IMPLEMENTATIONS**: Each task must fully implement its feature including all consumers, type updates, and integration points.
4. **DETAILED SPECIFICATIONS**: Each task must include EXACTLY what to implement, including specific functions, types, and integration points to avoid "breaking change" confusion.
5. **CONTEXT AWARENESS**: Each task is part of a larger system - specify how it connects to other parts.
6. **MAKE BREAKING CHANGES**: Unless explicitly requested by the user, you MUST make breaking changes.

### Visual Dependency Tree
```
pkg/
├── clickhouse/
│   ├── http.go (Task #0: HTTP client implementation)
│   ├── types.go (Task #0: Query/Insert types and generics)
│   └── config.go (Task #0: HTTP client configuration)
│
├── state/
│   ├── manager.go (Task #1: Next block resolver)
│   ├── store.go (Task #1: ClickHouse/memory backend)
│   └── config.go (Task #1: State configuration)
│
├── processor/
│   ├── manager.go (Task #2: Simplified processor manager)
│   ├── config.go (Task #2: Processor configuration)
│   │
│   └── transaction/
│       └── structlog/
│           ├── processor.go (Task #3: Main processor with Asynq integration)
│           ├── tasks.go (Task #3: Task definitions and handlers)
│           ├── stream.go (Task #4: Streaming JSON parser)
│           ├── worker.go (Task #5: Process/verify workers)
│           ├── memory.go (Task #4: Memory pool management)
│           └── metrics.go (Task #3: Performance metrics)
│
├── ethereum/
│   └── execution/
│       └── rpc.go (Task #6: Optimize DebugTraceTransaction)
│
└── common/
    ├── http.go (Task #0: Shared HTTP utilities)
    └── metrics.go (Task #0: Shared metric helpers)
```

### Execution Plan

#### Group A: Foundation (Execute all in parallel)
- [x] **Task #0**: Create ClickHouse HTTP client foundation
  - Folder: `pkg/clickhouse/`, `pkg/common/`
  - Files: `http.go`, `types.go`, `config.go`, `common/http.go`
  - Implements:
    - `type Client[T any] struct` with HTTP client, config, metrics
    - `func NewClient(config Config) (*Client[T], error)`
    - `func (c *Client[T]) Query(ctx context.Context, query string, args ...any) ([]T, error)`
    - `func (c *Client[T]) Insert(ctx context.Context, table string, data []T) error`
    - `func (c *Client[T]) Execute(ctx context.Context, query string) error`
    - Generic JSON marshaling/unmarshaling with FORMAT JSON
    - Connection pooling, retry logic, timeout handling
    - Error types: `QueryError`, `InsertError`, `ConnectionError`
  - Config struct:
    ```go
    type Config struct {
        URL             string
        Database        string
        Username        string
        Password        string
        MaxConnections  int
        RequestTimeout  time.Duration
        RetryAttempts   int
        RetryDelay      time.Duration
    }
    ```
  - Exports: Client, Config, error types
  - Integration: Used by state manager and processor for all DB operations

#### Group B: Core Components (Execute after Group A)
- [x] **Task #1**: Create simplified state manager
  - Folder: `pkg/state/`
  - Files: `manager.go`, `store.go`, `config.go`
  - Implements:
    - `type Manager interface` with `GetNextBlock(ctx, direction) (uint64, error)`
    - `type clickhouseStore` implementing block queries
    - `type memoryStore` for non-DB mode
    - Direction handling (forwards/backwards) with proper defaults
    - Chain head fetching from execution client
  - Queries:
    - Find next unprocessed block with direction awareness
    - Default to 0 (backwards) or chain head (forwards) when no data
  - Exports: Manager, NewManager, Config
  - Integration: Used by processor manager to determine next block
  - **Review feedback**: Removed caching layer for simplicity

- [x] **Task #2**: Simplify processor manager
  - Folder: `pkg/processor/`
  - Files: `manager.go`, `config.go`
  - Implements:
    - `type Manager struct` with processor lifecycle and Asynq client
    - `func (m *Manager) Start(ctx)` main processing loop
    - `func (m *Manager) processNextBlock(ctx)` simplified flow
    - `func (m *Manager) enqueueTransactions(block)` for task creation
    - Processor enable/disable based on config
    - Clean shutdown with context cancellation
    - Asynq client and server management
    - Manual block queue API handling
    - Leader election for distributed coordination
  - Processing flow:
    - Get next block from state manager (only if leader)
    - Fetch block from execution client
    - Create Asynq tasks for each transaction
    - Enqueue tasks with proper priority and retry settings
    - Handle errors with exponential backoff
  - Exports: Manager, Config
  - Integration: Main entry point for block processing
  - **Review feedback**: Preserve manual block queue API and leader election

#### Group C: Structlog Processing (Execute after Group B)
- [ ] **Task #3**: Redesign structlog processor with Asynq
  - Folder: `pkg/processor/transaction/structlog/`
  - Files: `processor.go`, `tasks.go`, `metrics.go`
  - Implements:
    - `type Processor struct` with Asynq handlers
    - `func (p *Processor) GetHandlers() map[string]asynq.HandlerFunc`
    - Task types for process/verify with direction support
    - Queue configuration (priorities, retries, timeouts)
    - Metrics for queue depth, processing rate, memory usage
    - Graceful shutdown with Asynq server stop
  - Task definitions:
    ```go
    type ProcessTask struct {
        BlockNumber      uint64
        TransactionHash  string
        TransactionIndex uint32
        Direction        string // "forwards" or "backwards"
    }
    type VerifyTask struct {
        // Same as ProcessTask
    }
    ```
  - Queue names: `structlog:process:forwards`, `structlog:process:backwards`, etc.
  - Exports: Processor, NewProcessor, task types
  - Integration: Registers handlers with Asynq, processes enqueued tasks

- [ ] **Task #4**: Implement streaming structlog parser
  - Folder: `pkg/processor/transaction/structlog/`
  - Files: `stream.go`, `memory.go`
  - Implements:
    - `type StreamParser` using `json.Decoder` for incremental parsing
    - `type MemoryPool` for reusable buffers
    - `func StreamStructlogs(reader, callback)` for chunked processing
    - Bounded memory usage with configurable chunk size
    - Efficient JSON parsing with standard library
  - Memory management:
    ```go
    type MemoryPool struct {
        buffers sync.Pool
        maxSize int
    }
    ```
  - Exports: StreamParser, MemoryPool
  - Integration: Used by workers to process large traces
  - **Review feedback**: Use json.Decoder for robust streaming

- [ ] **Task #5**: Create process and verify handlers
  - Folder: `pkg/processor/transaction/structlog/`
  - File: `worker.go`
  - Implements:
    - `func handleProcessTask(ctx, task *asynq.Task) error`
    - `func handleVerifyTask(ctx, task *asynq.Task) error`
    - Streaming trace fetching with memory bounds
    - Batch accumulation and flushing
    - Progress tracking for large transactions
    - Asynq task result reporting
  - Processing flow:
    - Unmarshal task payload
    - Fetch trace with streaming
    - Parse structlogs in chunks
    - Batch insert to ClickHouse
    - Clear memory between chunks
    - Return success/failure to Asynq
  - Verification flow:
    - Unmarshal task payload
    - Count structlogs in trace (lightweight)
    - Query ClickHouse for count
    - Compare and report discrepancies
    - Re-enqueue process task if mismatch
  - Exports: Handler functions for Asynq
  - Integration: Registered with processor, handle Asynq tasks

#### Group D: Optimizations (Execute after Group C)
- [ ] **Task #6**: Optimize DebugTraceTransaction for memory
  - Folder: `pkg/ethereum/execution/`
  - File: `rpc.go`
  - Implements:
    - Add streaming support if possible
    - Memory-efficient JSON unmarshaling
    - Options to skip unnecessary data
    - Connection reuse optimizations
  - Trace options enhancement:
    ```go
    type TraceOptions struct {
        DisableStorage   bool
        DisableStack     bool
        DisableMemory    bool
        EnableReturnData bool
        StreamingMode    bool  // New
        SkipStackData    bool  // New for verification
    }
    ```
  - Exports: Enhanced DebugTraceTransaction
  - Integration: Used by workers for trace fetching

---

## Implementation Workflow

This plan file serves as the authoritative checklist for implementation. When implementing:

### Required Process
1. **Load Plan**: Read this entire plan file before starting
2. **Sync Tasks**: Create TodoWrite tasks matching the checkboxes below
3. **Execute & Update**: For each task:
   - Mark TodoWrite as `in_progress` when starting
   - Update checkbox `[ ]` to `[x]` when completing
   - Mark TodoWrite as `completed` when done
4. **Maintain Sync**: Keep this file and TodoWrite synchronized throughout

### Review Feedback Summary
The following modifications were approved during review:
- **Task #1**: Remove caching layer from state manager for simplicity
- **Task #2**: Preserve manual block queue API and leader election functionality
- **Task #4**: Use json.Decoder instead of custom streaming parser
- All other tasks approved as originally specified

### Critical Rules
- This plan file is the source of truth for progress
- Update checkboxes in real-time as work progresses
- Never lose synchronization between plan file and TodoWrite
- Mark tasks complete only when fully implemented (no placeholders)
- Tasks should be run in parallel, unless there are dependencies, using subtasks, to avoid context bloat.

### Progress Tracking
The checkboxes above represent the authoritative status of each task. Keep them updated as you work.

## Technical Specifications

### ClickHouse HTTP API Usage
```go
// Query example
query := "SELECT * FROM structlogs WHERE block_number = ? FORMAT JSON"
response := httpPost("/", query, blockNumber)
json.Unmarshal(response.Data, &results)

// Insert example  
data := []Structlog{...}
query := "INSERT INTO structlogs FORMAT JSONEachRow"
body := jsonlines.Marshal(data)
httpPost("/", query + "\n" + body)
```

### Asynq Task Management
```go
// Task creation
task := asynq.NewTask("structlog:process:forwards", payload)
info, err := client.Enqueue(task, 
    asynq.Queue("structlog:process:forwards"),
    asynq.MaxRetry(3),
    asynq.Timeout(10*time.Minute),
)

// Handler registration
mux := asynq.NewServeMux()
mux.HandleFunc("structlog:process:forwards", handleProcessTask)
mux.HandleFunc("structlog:verify:forwards", handleVerifyTask)
```

### Memory Bounds Configuration
```yaml
processor:
  structlog:
    memory:
      max_trace_size: 4GB
      chunk_size: 100MB
      buffer_pool_size: 10
    asynq:
      redis_url: "redis://localhost:6379"
      max_concurrent_tasks: 100
      queue_priorities:
        "structlog:process:forwards": 6
        "structlog:process:backwards": 4
        "structlog:verify:forwards": 3
        "structlog:verify:backwards": 2
      retry_max: 3
      retry_timeout: 5m
```

### Streaming Architecture
```
RPC Response → Stream Reader → JSON Parser → Chunk Processor → Batch Insert
                                    ↓
                              Memory Pool ← Buffer Return
```

## Migration Strategy

1. **Phase 1**: Implement new components alongside existing ones
2. **Phase 2**: Add feature flags for gradual rollout
3. **Phase 3**: Run in shadow mode to validate correctness
4. **Phase 4**: Switch traffic with ability to rollback
5. **Phase 5**: Remove old implementation

## Testing Requirements

Each component must include:
- Unit tests with mocked dependencies
- Integration tests with real ClickHouse
- Memory usage tests with large payloads
- Benchmark tests for performance validation
- Chaos tests for failure scenarios

## Success Metrics

- **Memory Usage**: <4GB for 20GB traces
- **Throughput**: 1M+ structlogs/second
- **Latency**: <1s block processing time
- **Reliability**: 99.9% success rate
- **Simplicity**: 50% code reduction