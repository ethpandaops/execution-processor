# ClickHouse HTTP Client Refactor Implementation Plan

## Executive Summary
> The current ClickHouse integration uses the `clickhouse-go` driver with `database/sql` interface, providing connection pooling, prepared statements, and transaction support. This plan refactors the package to use direct HTTP requests with JSON formatting, creating a simple, modern API without compatibility constraints. The refactor will:
> - Replace `database/sql` with HTTP client using ClickHouse's HTTP interface
> - Use `FORMAT JSON` for both queries and inserts
> - Implement generic query methods with automatic JSON unmarshaling
> - Create a fresh, type-safe API without backward compatibility
> - Keep the clickhouse package as a "dumb" HTTP client with business logic in consuming packages

## Goals & Objectives
### Primary Goals
- Replace `clickhouse-go` dependency with standard library HTTP client
- Create a clean, modern API without backward compatibility constraints
- Design a "dumb" HTTP client with business logic in consuming packages
- Implement type-safe generic methods for queries and inserts

### Secondary Objectives
- Reduce binary size by removing large driver dependency
- Improve debuggability with plain HTTP requests and optional logging
- Maintain comprehensive metrics integration
- Provide testable interfaces and mock helpers

## Solution Overview
### Approach
The refactor replaces the SQL driver approach with direct HTTP requests to ClickHouse's HTTP interface. All queries and inserts will use JSON formatting for simplified parsing and data exchange.

### Key Components
1. **HTTP Client**: Reusable standard library HTTP client with configurable timeouts
2. **Modern Query Methods**: 
   - `QueryOne[T](ctx, query) (*T, error)` for single results
   - `QueryMany[T](ctx, query) ([]T, error)` for multiple results
3. **Simple Insert Method**: `BulkInsert[T](ctx, table, data) error` for batch inserts
4. **Execute Method**: `Execute(ctx, query) error` for non-query operations
5. **Clean Config**: HTTP endpoint URL, timeout, and metrics labels
6. **Debug Logging**: Optional request/response logging (excluding large payloads)

### Data Flow
```
Application → HTTP Client → ClickHouse HTTP API
     ↓              ↓                 ↓
   Query      POST Request      JSON Response
     ↓              ↓                 ↓
  Generic[T]   FORMAT JSON      Unmarshal to []T
```

### Expected Outcomes
- Clean, modern API requiring updates to consuming code
- Simpler "dumb" HTTP client without business logic
- Direct HTTP requests with optional debug logging
- Batch inserts as simple HTTP POST requests with JSON arrays
- Clear separation between HTTP transport and business logic

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
│   ├── client.go (Task #1: Replace with HTTP client implementation)
│   └── config.go (Task #1: Simplify config for HTTP client)
│
├── state/
│   └── manager.go (Task #2: Update to use new Query method)
│
└── processor/
    ├── transaction/
    │   └── structlog/
    │       ├── processor.go (Task #3: Replace batch insert with simplified Insert)
    │       ├── config.go (No changes - embeds clickhouse.Config)
    │       └── processor_test.go (Task #4: Update tests for new client)
    │
    ├── manager_test.go (Task #4: Update test setup)
    ├── manager_mode_test.go (Task #4: Update test setup)
    └── manager_race_test.go (Task #4: Update test setup)
```

### Execution Plan

#### Group A: Core HTTP Client Implementation
- [x] **Task #1**: Refactor ClickHouse package to HTTP client
  - Folder: `pkg/clickhouse/`
  - Files: `client.go`, `config.go`, `interface.go` (new)
  - Implements in `interface.go`:
    ```go
    type Client interface {
        QueryOne[T any](ctx context.Context, query string) (*T, error)
        QueryMany[T any](ctx context.Context, query string) ([]T, error)
        Execute(ctx context.Context, query string) error
        BulkInsert[T any](ctx context.Context, table string, data []T) error
        Start() error
        Stop() error
    }
    ```
  - Implements in `client.go`:
    ```go
    type client struct {
        httpClient *http.Client  // Reused across requests
        baseURL    string
        metrics    struct {
            operationDuration *prometheus.HistogramVec
            operationTotal    *prometheus.CounterVec
        }
        network   string
        processor string
        debug     bool  // Enable request/response logging
    }
    
    func New(cfg Config) (Client, error)
    func (c *client) Start() error
    func (c *client) Stop() error
    func (c *client) QueryOne[T any](ctx context.Context, query string) (*T, error)
    func (c *client) QueryMany[T any](ctx context.Context, query string) ([]T, error)
    func (c *client) Execute(ctx context.Context, query string) error
    func (c *client) BulkInsert[T any](ctx context.Context, table string, data []T) error
    ```
  - Implements in `config.go`:
    ```go
    type Config struct {
        URL           string        `yaml:"url"`
        QueryTimeout  time.Duration `yaml:"query_timeout"`   // Timeout for query operations
        InsertTimeout time.Duration `yaml:"insert_timeout"`  // Timeout for bulk insert operations
        Network       string        `yaml:"network"`
        Processor     string        `yaml:"processor"`
        Debug         bool          `yaml:"debug"`  // Enable logging
        KeepAlive     time.Duration `yaml:"keep_alive"`  // HTTP connection keep-alive duration
    }
    func (c *Config) Validate() error
    func (c *Config) SetDefaults()
    ```
  - Key Implementation Details:
    - Reuse single HTTP client instance (not per-request) with connection keep-alive settings
    - Configure separate timeouts for query vs insert operations
    - Debug logging for requests/responses (exclude large insert payloads)
    - No retry logic - keep it simple
    - No compatibility methods - fresh API
    - HTTP client configuration should include keep-alive settings for connection reuse
  - HTTP request format:
    - Queries: `POST /` with body: `query + " FORMAT JSON"`
    - Inserts: `POST /` with body: `INSERT INTO table FORMAT JSONEachRow` + JSON lines
  - Error handling: Return raw errors from HTTP/JSON operations
  - Metrics: Track operation duration, count, and payload size with labels
  - Context: This is the core refactor that all other tasks depend on

#### Group B: Query Usage Updates (Execute after Group A)
- [x] **Task #2**: Update state manager to use new Query method
  - Folder: `pkg/state/`
  - File: `manager.go`
  - Changes:
    - Replace `QueryRow().Scan()` pattern with `QueryOne[T]()` method
    - Update all query methods to use clean struct patterns
    - Change `ExecContext()` to `Execute()` for insert operations
    - Handle empty results as `nil` (not error)
  - Example transformations:
    ```go
    // Old: 
    // row := client.QueryRow(ctx, query)
    // err := row.Scan(&blockNumber)
    // if err == sql.ErrNoRows { ... }
    
    // New (using anonymous struct for simple queries):
    result, err := s.storageClient.QueryOne[struct {
        BlockNumber int64 `json:"block_number"`
    }](ctx, query)
    if err != nil {
        return 0, err
    }
    if result == nil {
        return 0, nil  // No rows found
    }
    return result.BlockNumber, nil
    ```
  - Implementation approach:
    - Use anonymous structs for one-off queries
    - Define named structs for commonly used result types (e.g., BlockNumberResult, MinMaxBlockResult)
    - Keep code concise and readable
    - Ensure dynamic table names via string formatting continue to work in queries
    - Define common result structs in state package for reusable query patterns
  - Context: State manager tracks processing progress in ClickHouse

#### Group C: Batch Insert Updates (Execute after Group A)
- [x] **Task #3**: Refactor structlog processor batch inserts
  - Folder: `pkg/processor/transaction/structlog/`
  - File: `processor.go`
  - Major changes:
    - Remove all transaction-based batching code
    - Replace with simple `BulkInsert` call per batch
    - Update `flushBatch()` to use new API
    - Remove prepared statement logic
    - Simplify error handling (no rollback needed)
  - Implementation approach:
    - Keep ALL existing batch collection logic in processor
    - Processor decides batch sizes and when to flush
    - At flush time, call `BulkInsert` with full batch
    - Remove transaction begin/commit/rollback code
    - Keep `runtime.GC()` optimization in processor (not in clickhouse package)
  - Key principles:
    - ClickHouse package remains "dumb" - just sends what it receives
    - All batching intelligence stays in structlog processor
    - Memory management stays in processor layer
    - Clear separation of concerns
  - Context: Main processor that handles high-volume transaction log inserts

#### Group D: Test Updates (Execute after Groups B and C)
- [x] **Task #4**: Update all tests for new HTTP client
  - Files:
    - `pkg/clickhouse/client_test.go` (new - add unit tests)
    - `pkg/clickhouse/mock.go` (new - provide mock implementation)
    - `pkg/processor/transaction/structlog/processor_test.go`
    - `pkg/processor/manager_test.go`
    - `pkg/processor/manager_mode_test.go`
    - `pkg/processor/manager_race_test.go`
  - Changes:
    - Create interface-based mocks for easy testing that implement the Client interface
    - Add unit tests for HTTP client using `httptest`
    - Include HTTP-specific tests for timeouts, connection errors, and keep-alive behavior
    - Provide common test helpers for typical scenarios (empty results, errors, large responses)
    - Update all test setup to use new API
    - Remove any SQL-specific test patterns
  - Mock implementation approach:
    ```go
    // pkg/clickhouse/mock.go
    type MockClient struct {
        QueryOneFunc    func(ctx, query) (interface{}, error)
        QueryManyFunc   func(ctx, query) (interface{}, error)
        ExecuteFunc     func(ctx, query) error
        BulkInsertFunc  func(ctx, table string, data interface{}) error
        StartFunc       func() error
        StopFunc        func() error
    }
    
    // Implement Client interface methods that call the func fields
    ```
  - Benefits:
    - Clean test setup without SQL complexity
    - Better test isolation
    - Can test HTTP behavior directly
    - Easy to mock different scenarios
  - Context: Complete test rewrite for new clean API

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

### Critical Rules
- This plan file is the source of truth for progress
- Update checkboxes in real-time as work progresses
- Never lose synchronization between plan file and TodoWrite
- Mark tasks complete only when fully implemented (no placeholders)
- Tasks should be run in parallel, unless there are dependencies, using subtasks, to avoid context bloat.

### Progress Tracking
The checkboxes above represent the authoritative status of each task. Keep them updated as you work.