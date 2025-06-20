# Execution Processor

A distributed system for processing Ethereum execution layer data with support for transaction structlogs, leader election, and horizontal scaling.

## Features

- **Transaction Structlog Processing**: Extract and store detailed execution traces for every transaction
- **Distributed Processing**: Redis-backed task queues with priority-based processing
- **Leader Election**: Built-in leader election for coordinated block processing
- **Dual Processing Modes**: Forwards (real-time) and backwards (backfill) processing
- **State Management**: Track processing progress with ClickHouse storage
- **Resource Management**: Memory-optimized chunked processing with leak prevention
- **Queue Prioritization**: Separate queues for forwards/backwards processing and verification

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

- **Forwards**: Process new blocks as they arrive (priority 100)
- **Backwards**: Backfill historical blocks (priority 50)
- **Verification**: Validate processed data (priority 10)

### Queue Architecture

```
┌─────────────────────────────────────────┐
│  Priority Queue System                  │
├─────────────────────────────────────────┤
│  1. Forwards Processing    (Priority 10)│
│  2. Backwards Processing   (Priority 5) │
│  3. Forwards Verification  (Priority 1) │
│  4. Backwards Verification (Priority 1) │
└─────────────────────────────────────────┘
```

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
- Verification system with mismatch detection

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