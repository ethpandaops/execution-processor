# Execution Processor

A distributed system for processing Ethereum execution layer data with support for transaction structlogs, leader election, and horizontal scaling.

## Features

- **Transaction Structlog Processing**: Extract and store detailed execution traces for every transaction
- **Structlog Aggregation**: Aggregate per-opcode gas data into call frame rows with resource gas decomposition
- **Distributed Processing**: Redis-backed task queues with priority-based processing
- **Leader Election**: Built-in leader election for coordinated block processing
- **Dual Processing Modes**: Forwards (real-time) and backwards (backfill) processing
- **State Management**: Track processing progress with ClickHouse storage
- **Resource Management**: Memory-optimized chunked processing with leak prevention
- **Queue Prioritization**: Separate queues for forwards/backwards processing

## Quick Start

1. **Prerequisites**
   - Go 1.21+
   - ClickHouse database
   - Redis server
   - Ethereum execution node (e.g., Geth, Nethermind)

2. **Configuration**
   ```bash
   cp example_config.yaml config.yaml
   # Edit config.yaml with your database and node URLs
   ```

3. **Run**
   ```bash
   go build -o execution-processor
   ./execution-processor --config config.yaml
   ```

## Configuration

### Core Components

- **Ethereum Nodes**: Configure execution node endpoints
- **Redis**: Task queue and leader election coordination
- **State Manager**: Track processing progress in ClickHouse
- **Processors**: Configure structlog extraction settings

### Processing Modes

- **Forwards**: Process new blocks as they arrive (priority 10)
- **Backwards**: Backfill historical blocks (priority 5)

### Queue Architecture

```
┌─────────────────────────────────────────┐
│  Priority Queue System                  │
├─────────────────────────────────────────┤
│  1. Forwards Processing    (Priority 10)│
│  2. Backwards Processing   (Priority 5) │
└─────────────────────────────────────────┘
```

## Embedded Mode (Library Usage)

The execution-processor can be embedded as a library within an execution client, providing direct data access without JSON-RPC overhead.

### Implementing DataSource

```go
import (
    "context"
    "math/big"

    "github.com/ethereum/go-ethereum/core/types"
    "github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

type MyDataSource struct {
    client *MyExecutionClient
}

func (ds *MyDataSource) BlockNumber(ctx context.Context) (*uint64, error) {
    num := ds.client.CurrentBlock()
    return &num, nil
}

func (ds *MyDataSource) BlockByNumber(ctx context.Context, number *big.Int) (*types.Block, error) {
    return ds.client.GetBlock(number), nil
}

func (ds *MyDataSource) BlockReceipts(ctx context.Context, number *big.Int) ([]*types.Receipt, error) {
    return ds.client.GetBlockReceipts(number), nil
}

func (ds *MyDataSource) TransactionReceipt(ctx context.Context, hash string) (*types.Receipt, error) {
    return ds.client.GetReceipt(hash), nil
}

func (ds *MyDataSource) DebugTraceTransaction(
    ctx context.Context,
    hash string,
    blockNumber *big.Int,
    opts execution.TraceOptions,
) (*execution.TraceTransaction, error) {
    return ds.client.TraceTransaction(hash, opts), nil
}

func (ds *MyDataSource) ChainID() int64 {
    return ds.client.ChainID()
}

func (ds *MyDataSource) ClientType() string {
    return "my-client/1.0.0"
}

func (ds *MyDataSource) IsSynced() bool {
    return ds.client.IsSynced()
}
```

### Creating an Embedded Pool

```go
import (
    "github.com/ethpandaops/execution-processor/pkg/ethereum"
    "github.com/ethpandaops/execution-processor/pkg/ethereum/execution"
)

// Create embedded node with your data source
dataSource := &MyDataSource{client: myClient}
node := execution.NewEmbeddedNode(log, "embedded", dataSource)

// Create pool with the embedded node
pool := ethereum.NewPoolWithNodes(log, "processor", []execution.Node{node}, nil)
pool.Start(ctx)

// Mark ready when your client is synced and ready to serve data
node.MarkReady(ctx)
```

### Embedded vs RPC Mode

| Aspect | RPC Mode | Embedded Mode |
|--------|----------|---------------|
| Data Access | JSON-RPC over HTTP | Direct function calls |
| Readiness | Auto-detected via RPC health checks | Host calls MarkReady() |
| Performance | Network + serialization overhead | Zero serialization overhead |
| Use Case | External execution clients | Library integration |

## Manual Block Queue API

The execution processor provides an HTTP API for manually queuing blocks for reprocessing. This is useful for fixing data issues or reprocessing specific blocks.

### Enable the API

Add the API server address to your configuration:

```yaml
apiAddr: ":8080"  # Optional API server address
```

### Queue a Single Block

```bash
# Queue block 12345 for transaction_structlog processing
curl -X POST http://localhost:8080/api/v1/queue/block/transaction_structlog/12345

# Response:
{
  "status": "queued",
  "block_number": 12345,
  "processor": "transaction_structlog",
  "queue": "process:forwards",
  "transaction_count": 150,
  "tasks_created": 150
}
```

### Queue Multiple Blocks

```bash
# Queue blocks 12345-12350 for reprocessing
curl -X POST http://localhost:8080/api/v1/queue/blocks/transaction_structlog \
  -H "Content-Type: application/json" \
  -d '{
    "blocks": [12345, 12346, 12347, 12348, 12349, 12350]
  }'

# Response:
{
  "status": "queued",
  "processor": "transaction_structlog",
  "queue": "process:forwards",
  "summary": {
    "total": 6,
    "queued": 6,
    "skipped": 0,
    "failed": 0
  },
  "results": [
    {
      "block_number": 12345,
      "status": "queued",
      "transaction_count": 150,
      "tasks_created": 150
    },
    ...
  ]
}
```

### Important Notes

- The API works on any node (leader or non-leader)
- Blocks are queued using the node's current processing mode (forwards/backwards)
- Maximum 1000 blocks per bulk request
- Allows reprocessing of already processed blocks
- Each API call creates new tasks (calling multiple times will create duplicates)

## Structlog Aggregation

The `structlog_agg` processor aggregates per-opcode structlog data into call frame rows suitable for ClickHouse storage. It produces two types of rows per call frame:

- **Summary row** (`operation=""`): Frame-level metadata including gas totals, call type, target address, intrinsic gas, and gas refund
- **Per-opcode rows** (`operation="SLOAD"` etc.): Gas and count aggregated by opcode within each frame

### Resource Gas Decomposition

The aggregator computes building-block columns that enable downstream SQL to decompose EVM gas into resource categories (compute, memory, storage access):

| Column | Description |
|--------|-------------|
| `memory_words_sum_before` | SUM(ceil(memory_bytes/32)) before each opcode |
| `memory_words_sum_after` | SUM(ceil(memory_bytes/32)) after each opcode |
| `memory_words_sq_sum_before` | SUM(words_before^2) for quadratic cost extraction |
| `memory_words_sq_sum_after` | SUM(words_after^2) for quadratic cost extraction |
| `cold_access_count` | Number of cold storage/account accesses (EIP-2929) |

These columns are computed by two functions in the `structlog` package:

- **`ComputeMemoryWords`**: Derives per-opcode memory size in 32-byte words using the pending-index technique. Handles depth transitions and RETURN/REVERT last-in-frame expansion via stack operands.
- **`ClassifyColdAccess`**: Classifies each opcode's cold vs warm access using gas values, memory expansion costs, and range-based detection. Supports both embedded mode (pre-computed tracer fields) and RPC mode (stack-based fallbacks).

### Gas Computation Pipeline

```
StructLogs -> ComputeGasUsed -> ComputeGasSelf -> ComputeMemoryWords -> ClassifyColdAccess
                                                                              |
                                                                              v
                                                                     ProcessStructlog (per opcode)
                                                                              |
                                                                              v
                                                                     Finalize -> CallFrameRows
```

## Architecture

### Leader Election
- Redis-based distributed leader election
- Automatic failover and coordination
- Mode-specific leader keys (`execution-processor:leader:{network}:{mode}`)

### Resource Management
- Chunked processing (100 items per transaction)
- Memory leak prevention with explicit GC
- Connection pooling and proper cleanup
- Transaction atomicity with rollback support

### Error Handling
- Graceful handling of chain head scenarios
- Retry logic for transient failures
- Comprehensive logging and metrics

## Monitoring

- **Metrics**: Prometheus metrics on `:9090` (default)
- **Health Check**: Optional health endpoint
- **Profiling**: Optional pprof server
- **Logging**: Configurable log levels (trace, debug, info, warn, error)

## Development

```bash
# Run tests
go test ./...

# Run with race detector
go test ./... --race

# Build
go build .
```

## License

See LICENSE file.
