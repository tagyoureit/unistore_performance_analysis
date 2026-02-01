# Log Streaming Architecture

This document describes the end-to-end flow of logs from workers to the UI,
current limitations, and recommended improvements.

## Architecture Overview

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Worker-0      │    │   Worker-1      │    │   Worker-N      │
│ (subprocess)    │    │ (subprocess)    │    │ (subprocess)    │
│                 │    │                 │    │                 │
│  Python Logger  │    │  Python Logger  │    │  Python Logger  │
└───────┬─────────┘    └───────┬─────────┘    └───────┬─────────┘
        │                      │                      │
        └──────────────────────┼──────────────────────┘
                               │
                    ┌──────────▼──────────┐
                    │  TestLogQueueHandler │
                    │  (asyncio.Queue)     │
                    │  max_size: 10,000    │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │   _drain_log_queue   │
                    │  (batch: 100/sec)    │
                    └──────────┬──────────┘
                               │
               ┌───────────────┼───────────────┐
               │               │               │
     ┌─────────▼────┐  ┌───────▼──────┐  ┌─────▼─────┐
     │ Snowflake DB │  │  WebSocket   │  │  HTTP API │
     │  TEST_LOGS   │  │ (1 sec poll) │  │ (fallback)│
     └──────────────┘  └──────────────┘  └───────────┘
```

## Component Details

### 1. Worker Log Capture

**Location**: `backend/core/test_log_stream.py`

Workers use Python's standard `logging` module with a custom handler that
intercepts log records and converts them to structured events.

**Log Event Schema**:
```python
{
    "kind": "log",
    "log_id": str,        # UUID for deduplication
    "test_id": str,       # Parent test run ID
    "seq": int,           # Sequence number for ordering
    "timestamp": str,     # ISO 8601 timestamp
    "level": str,         # INFO, WARNING, ERROR, DEBUG
    "logger": str,        # Module path (e.g., "backend.core.orchestrator")
    "message": str,       # Log message (truncated to 20K chars)
    "exception": str,     # Formatted traceback if present
    "worker_id": str,     # CONTROLLER, ORCHESTRATOR, worker-id, or UNKNOWN
}
```

**Key Behaviors**:
- Uses `CURRENT_TEST_ID` contextvar for per-test isolation (`test_log_stream.py:18`)
- Uses `CURRENT_WORKER_ID` contextvar and `record.worker_id` to label sources
- Non-blocking: drops logs if queue full (`test_log_stream.py:84-86`)
- Message truncation: 20,000 char limit (`test_log_stream.py:59-60`)

### 2. Log Queue & Aggregation

**Location**: `backend/core/orchestrator.py:575-591`

The orchestrator creates a shared asyncio.Queue for all workers:
```python
log_queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)
log_handler = TestLogQueueHandler(test_id=run_id, queue=log_queue)
```

**Background Drain Task** (`orchestrator.py:727-767`):
- Batches logs: 100 events per batch
- Polling interval: 1 second
- Persists to Snowflake `TEST_LOGS` table
- Graceful shutdown: drains remaining queue on cancellation

### 3. Snowflake Persistence

**Location**: `backend/core/results_store.py:1004-1053`

Logs are persisted to `TEST_RESULTS.TEST_LOGS` table:

| Column | Type | Description |
|--------|------|-------------|
| LOG_ID | VARCHAR | UUID for deduplication |
| TEST_ID | VARCHAR | Parent test run ID |
| WORKER_ID | VARCHAR | CONTROLLER, ORCHESTRATOR, or worker identifier |
| SEQ | INTEGER | Sequence number |
| TIMESTAMP | TIMESTAMP_NTZ | Event timestamp |
| LEVEL | VARCHAR | Log level |
| LOGGER | VARCHAR | Logger name (module path) |
| MESSAGE | VARCHAR | Log message |
| EXCEPTION | VARCHAR | Exception traceback |

**Insert behavior**: Chunks of 500 rows per INSERT statement.

### 4. WebSocket Streaming

**Location**: `backend/main.py:1184-1320`

The WebSocket endpoint streams logs to the UI:
- Polling interval: 1 second (`main.py:1193`)
- Incremental streaming: tracks `last_log_seq` to avoid re-sending
- Batch size: up to 100 logs per message (`main.py:1306`)

**Message format**:
```json
{
  "event": "RUN_UPDATE",
  "data": {
    "test_id": "...",
    "logs": [
      {"kind": "log", "log_id": "...", "seq": 123, ...}
    ]
  }
}
```

### 5. HTTP Fallback API

**Location**: `backend/api/routes/test_results.py:3098-3198`

Endpoint: `GET /api/tests/{test_id}/logs`

- Fetches from in-memory buffer for running tests
- Falls back to Snowflake for historical logs
- Supports multi-worker scenarios with worker selection

### 6. Frontend Display

**Location**: `backend/templates/pages/dashboard.html:425-449`

The UI displays logs with:
- Worker test selector dropdown (for multi-worker runs)
- Source filter (worker/controller/orchestrator), level filter, and verbose toggle
- Worker badges that can be clicked to filter
- Monospace font, dark theme, max 1000 lines displayed

**Log formatting** (`backend/static/js/dashboard/logs.js`):
```javascript
// Filters apply before rendering.
const logs = this.filteredLogs();
```

## Rate Limiting & Batching Summary

| Stage | Mechanism | Limit |
|-------|-----------|-------|
| Log Capture | Queue overflow protection | Drop if > 10,000 queued |
| Message Truncation | Per-message limit | 20,000 chars |
| Queue Drain | Batch processing | 100 events/batch |
| Queue Drain | Polling interval | 1 second |
| Snowflake Insert | Chunk size | 500 rows/chunk |
| WebSocket Fetch | Query limit | 100 logs/query |
| WebSocket Send | Polling interval | 1 second |
| Frontend Buffer | Display limit | 1000 lines |

## Remaining Limitations

### 1. Single Log Stream

**Problem**: All workers share a single log queue and stream per test.

**Impact**:
- Logs from different workers can still interleave
- No per-worker log tabs (planned follow-up)

## Related Documents

- [plan/log-improvements.md](plan/log-improvements.md) - Implementation plan
- [metrics-streaming-debug.md](metrics-streaming-debug.md) - Metrics flow (similar pattern)
- [ui-architecture.md](ui-architecture.md) - UI contracts and behavior
