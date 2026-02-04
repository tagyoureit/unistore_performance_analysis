# File-Based Query Logging

**Status**: Not Started  
**Created**: 2026-02-04  
**Supersedes**: [query-execution-streaming.md](query-execution-streaming.md) (streaming approach)

## Problem Statement

The `QueryExecutionStreamer` causes periodic drops in benchmark concurrency (from 100 down to 30-60 concurrent queries) when streaming query execution records to Snowflake during high-throughput tests (~900 QPS).

### Root Cause

The async event loop is blocked by the streamer's operations:
- `asyncio.Lock` contention on buffer access
- Even with `run_in_executor()` for CPU work, lock acquisition blocks the event loop
- At 900 QPS, small amounts of contention accumulate and cause visible concurrency dips

### Evidence

| Configuration | Concurrency Behavior |
|---------------|----------------------|
| Logging enabled | Dips to 30-60 periodically |
| Logging disabled | Stable 95-100 |

### What We Tried (All Failed to Eliminate Dips)

1. Threshold-based flushing (2000/5000 records)
2. `run_in_executor()` for `asdict()` conversion
3. O(1) atomic buffer swap
4. Concurrent flushes with semaphore (4 parallel)

## Solution: File-Based Logging with Deferred COPY INTO

**Key insight**: Complete decoupling from the async event loop during benchmark execution.

### Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│ DURING BENCHMARK (WARMUP + MEASUREMENT)                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Query Execution                                                    │
│       │                                                             │
│       ▼                                                             │
│  FileBasedQueryLogger                                               │
│  ├─ buffer: list[dict]          (plain Python list, no lock)        │
│  ├─ file_index: int             (current file sequence)             │
│  ├─ rows_in_current_file: int                                       │
│  └─ temp_dir: /tmp/qe_{test_id}_{worker_id}/                        │
│       │                                                             │
│       ▼ (when buffer >= BUFFER_SIZE or rows >= MAX_ROWS_PER_FILE)   │
│  Write to Parquet file (sync, in-process, ~50ms for 10K rows)       │
│       │                                                             │
│       ▼                                                             │
│  qe_{test_id}_{worker_id}_{sequence:04d}.parquet                    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ DURING PROCESSING PHASE                                             │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  Worker calls finalize()                                            │
│       │                                                             │
│       ▼                                                             │
│  1. Flush remaining buffer to final Parquet file                    │
│       │                                                             │
│       ▼                                                             │
│  2. PUT files to internal stage                                     │
│     PUT file:///tmp/qe_{test_id}_{worker_id}/*.parquet              │
│         @QUERY_EXECUTIONS_STAGE/{test_id}/{worker_id}/              │
│       │                                                             │
│       ▼                                                             │
│  3. COPY INTO QUERY_EXECUTIONS                                      │
│     (bulk load, fastest possible)                                   │
│       │                                                             │
│       ▼                                                             │
│  4. Cleanup: REMOVE staged files + delete local files               │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Why Parquet?

| Format | COPY INTO Speed | Compression | Type Safety |
|--------|-----------------|-------------|-------------|
| **Parquet** | Fastest (columnar) | Best (~5:1) | Preserves types |
| CSV | Slower (parsing) | Good (~3:1) | Type coercion needed |
| JSON | Slowest | Poor | Schema inference |

**Decision**: Use Parquet via PyArrow (already a dependency via pandas).

## Configuration

### Constants

```python
# File splitting thresholds
MAX_ROWS_PER_FILE = 500_000        # ~100-250MB per file (Snowflake optimal)
BUFFER_SIZE = 10_000               # Flush to disk every 10K records
MIN_ROWS_FOR_SPLIT = 100_000       # Don't split files smaller than this

# Calculated sizing
# At 500 bytes/row compressed:
#   500K rows × 500 bytes = 250MB per file (ideal for COPY INTO)
#   10K rows × 500 bytes = 5MB buffer (acceptable memory)
```

### Scale Examples

| Scenario | Duration | QPS/Worker | Rows/Worker | Files/Worker |
|----------|----------|------------|-------------|--------------|
| Short test | 5 min | 100 | 30K | 1 |
| Standard test | 30 min | 100 | 180K | 1 |
| Long test | 1 hour | 100 | 360K | 1 |
| High-QPS short | 30 min | 500 | 900K | 2 |
| Multi-hour high | 4 hours | 500 | 7.2M | 15 |
| Extreme | 8 hours | 1000 | 28.8M | 58 |

**Key insight**: Most tests produce a single file per worker. File splitting only matters for multi-hour high-QPS runs.

## Implementation

### FileBasedQueryLogger Class

**Location**: `backend/core/file_query_logger.py`

```python
from __future__ import annotations

import logging
import os
import shutil
import tempfile
from dataclasses import asdict
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow as pa
import pyarrow.parquet as pq

if TYPE_CHECKING:
    from backend.connectors.snowflake_pool import SnowflakeConnectionPool

logger = logging.getLogger(__name__)

# Configuration
MAX_ROWS_PER_FILE = 500_000
BUFFER_SIZE = 10_000


class FileBasedQueryLogger:
    """
    Logs query execution records to local Parquet files during benchmark,
    then bulk loads to Snowflake during the PROCESSING phase.
    
    This approach eliminates event loop contention by:
    1. Using plain Python list for buffer (no async lock)
    2. Writing to local disk (no network I/O during benchmark)
    3. Deferring Snowflake writes to PROCESSING phase
    
    Usage:
        logger = FileBasedQueryLogger(test_id, worker_id, results_prefix)
        
        # During benchmark
        logger.append(record)  # Fast, no async
        
        # During PROCESSING phase
        await logger.finalize(pool)  # PUT + COPY INTO + cleanup
    """
    
    def __init__(
        self,
        test_id: str,
        worker_id: str,
        results_prefix: str,
    ) -> None:
        self.test_id = test_id
        self.worker_id = worker_id
        self.results_prefix = results_prefix
        
        # In-memory buffer (plain list, no lock needed)
        self._buffer: list[dict[str, Any]] = []
        
        # File management
        self._temp_dir = Path(tempfile.mkdtemp(prefix=f"qe_{test_id}_{worker_id}_"))
        self._file_index = 0
        self._rows_in_current_file = 0
        self._total_rows = 0
        self._files_written: list[Path] = []
        
        # Schema for Parquet files
        self._schema = self._build_schema()
        
        logger.info(
            f"FileBasedQueryLogger initialized: test_id={test_id}, "
            f"worker_id={worker_id}, temp_dir={self._temp_dir}"
        )
    
    def _build_schema(self) -> pa.Schema:
        """Build PyArrow schema for QUERY_EXECUTIONS table."""
        return pa.schema([
            ("execution_id", pa.string()),
            ("test_id", pa.string()),
            ("query_id", pa.string()),
            ("query_text", pa.string()),
            ("start_time", pa.timestamp("us")),
            ("end_time", pa.timestamp("us")),
            ("duration_ms", pa.float64()),
            ("rows_affected", pa.int64()),
            ("bytes_scanned", pa.int64()),
            ("warehouse", pa.string()),
            ("success", pa.bool_()),
            ("error", pa.string()),
            ("connection_id", pa.string()),
            ("custom_metadata", pa.string()),  # JSON string
            ("query_kind", pa.string()),
            ("worker_id", pa.string()),
            ("warmup", pa.bool_()),
            ("app_elapsed_ms", pa.float64()),
            ("sf_rows_inserted", pa.int64()),
            ("sf_rows_updated", pa.int64()),
            ("sf_rows_deleted", pa.int64()),
        ])
    
    @property
    def stats(self) -> dict[str, Any]:
        """Return current logger statistics."""
        return {
            "total_rows": self._total_rows,
            "buffered_rows": len(self._buffer),
            "files_written": len(self._files_written),
            "current_file_rows": self._rows_in_current_file,
        }
    
    def append(self, record: Any) -> None:
        """
        Add a record to the buffer.
        
        This is designed to be extremely fast - no locks, no I/O.
        
        Args:
            record: QueryExecutionRecord dataclass or dict.
        """
        # Convert dataclass to dict if needed
        if hasattr(record, "__dataclass_fields__"):
            record_dict = asdict(record)
        else:
            record_dict = record
        
        # Add derived fields
        query_kind = (record_dict.get("query_kind") or "").strip().upper()
        rows_affected = record_dict.get("rows_affected")
        record_dict["sf_rows_inserted"] = rows_affected if query_kind == "INSERT" else None
        record_dict["sf_rows_updated"] = rows_affected if query_kind == "UPDATE" else None
        record_dict["sf_rows_deleted"] = rows_affected if query_kind == "DELETE" else None
        
        # Serialize custom_metadata to JSON string
        import json
        record_dict["custom_metadata"] = json.dumps(record_dict.get("custom_metadata") or {})
        
        self._buffer.append(record_dict)
        self._total_rows += 1
        
        # Check if we need to flush to disk
        if len(self._buffer) >= BUFFER_SIZE:
            self._flush_buffer_to_disk()
        
        # Check if we need to start a new file
        if self._rows_in_current_file >= MAX_ROWS_PER_FILE:
            self._start_new_file()
    
    def _flush_buffer_to_disk(self) -> None:
        """Flush in-memory buffer to current Parquet file."""
        if not self._buffer:
            return
        
        file_path = self._current_file_path()
        
        # Convert buffer to PyArrow table
        table = pa.Table.from_pylist(self._buffer, schema=self._schema)
        
        # Append to Parquet file (create if doesn't exist)
        if file_path.exists():
            # Read existing file and append
            existing_table = pq.read_table(file_path)
            combined = pa.concat_tables([existing_table, table])
            pq.write_table(combined, file_path, compression="snappy")
        else:
            pq.write_table(table, file_path, compression="snappy")
            self._files_written.append(file_path)
        
        self._rows_in_current_file += len(self._buffer)
        self._buffer.clear()
        
        logger.debug(
            f"Flushed {len(self._buffer)} rows to {file_path.name}, "
            f"total in file: {self._rows_in_current_file}"
        )
    
    def _current_file_path(self) -> Path:
        """Get path to current Parquet file."""
        return self._temp_dir / f"qe_{self.test_id}_{self.worker_id}_{self._file_index:04d}.parquet"
    
    def _start_new_file(self) -> None:
        """Start a new Parquet file for subsequent records."""
        self._file_index += 1
        self._rows_in_current_file = 0
        logger.info(f"Starting new file: index={self._file_index}")
    
    async def finalize(self, pool: SnowflakeConnectionPool) -> dict[str, Any]:
        """
        Finalize logging: flush buffer, upload to stage, COPY INTO, cleanup.
        
        Called during PROCESSING phase after benchmark completes.
        
        Args:
            pool: Snowflake connection pool for stage operations.
            
        Returns:
            Stats dict with rows loaded, files processed, etc.
        """
        # 1. Final buffer flush
        self._flush_buffer_to_disk()
        
        if not self._files_written:
            logger.info(f"No query execution records to upload for {self.worker_id}")
            return {"rows_loaded": 0, "files_processed": 0}
        
        stage_path = f"@{self.results_prefix}.QUERY_EXECUTIONS_STAGE/{self.test_id}/{self.worker_id}"
        
        try:
            # 2. PUT files to stage
            for file_path in self._files_written:
                put_sql = f"""
                    PUT file://{file_path} {stage_path}/
                    AUTO_COMPRESS=FALSE OVERWRITE=TRUE
                """
                await pool.execute_query(put_sql)
                logger.info(f"Uploaded {file_path.name} to stage")
            
            # 3. COPY INTO QUERY_EXECUTIONS
            copy_sql = f"""
                COPY INTO {self.results_prefix}.QUERY_EXECUTIONS
                FROM {stage_path}/
                FILE_FORMAT = (TYPE = PARQUET)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
                ON_ERROR = CONTINUE
            """
            result = await pool.execute_query(copy_sql)
            
            # Parse COPY result for loaded row count
            rows_loaded = self._parse_copy_result(result)
            
            logger.info(
                f"COPY INTO completed: {rows_loaded} rows from "
                f"{len(self._files_written)} files"
            )
            
            # 4. Cleanup stage files
            remove_sql = f"REMOVE {stage_path}/"
            await pool.execute_query(remove_sql)
            logger.info(f"Cleaned up stage files at {stage_path}")
            
            return {
                "rows_loaded": rows_loaded,
                "files_processed": len(self._files_written),
                "total_rows_captured": self._total_rows,
            }
            
        finally:
            # 5. Cleanup local temp directory
            self._cleanup_local_files()
    
    def _parse_copy_result(self, result: Any) -> int:
        """Parse COPY INTO result to extract rows loaded."""
        try:
            if isinstance(result, list) and len(result) > 0:
                # COPY INTO returns list of dicts with file stats
                return sum(r.get("rows_loaded", 0) for r in result)
            return self._total_rows  # Fallback to tracked count
        except Exception:
            return self._total_rows
    
    def _cleanup_local_files(self) -> None:
        """Remove local temp directory and files."""
        try:
            if self._temp_dir.exists():
                shutil.rmtree(self._temp_dir)
                logger.info(f"Cleaned up local temp dir: {self._temp_dir}")
        except Exception as e:
            logger.warning(f"Failed to cleanup temp dir {self._temp_dir}: {e}")
    
    def cleanup_on_error(self) -> None:
        """
        Cleanup method for error scenarios where finalize() won't be called.
        
        Call this in exception handlers to avoid orphaned temp files.
        """
        self._cleanup_local_files()
```

### Stage Creation

**Location**: `sql/schema/results_tables.sql` (add to existing file)

```sql
-- Internal stage for query execution Parquet files
-- Used by FileBasedQueryLogger for bulk COPY INTO
CREATE STAGE IF NOT EXISTS QUERY_EXECUTIONS_STAGE
    FILE_FORMAT = (TYPE = PARQUET)
    COMMENT = 'Temporary stage for query execution Parquet files during PROCESSING phase';
```

### Worker Integration

**Location**: `scripts/run_worker.py`

Changes required:
1. Replace `QueryExecutionStreamer` with `FileBasedQueryLogger`
2. Pass logger to `TestExecutor`
3. Call `logger.finalize()` during shutdown (PROCESSING phase)

```python
# In _run_worker_control_plane(), replace:
#   query_streamer = QueryExecutionStreamer(...)
#   await query_streamer.start()
# With:
#   query_logger = FileBasedQueryLogger(
#       test_id=run_id,
#       worker_id=worker_id,
#       results_prefix=results_prefix,
#   )

# In shutdown sequence, replace:
#   await query_streamer.shutdown(...)
# With:
#   finalize_stats = await query_logger.finalize(control_pool)
#   logger.info(f"Query logging finalized: {finalize_stats}")
```

### TestExecutor Integration

**Location**: `backend/core/test_executor.py`

Changes required:
1. Accept `FileBasedQueryLogger` instead of `QueryExecutionStreamer`
2. Call `logger.append()` synchronously (no await needed)

```python
# In _record_query_execution(), replace:
#   await self._query_streamer.append(record)
# With:
#   self._query_logger.append(record)  # Sync call, no await
```

## SPCS Compatibility

For Snowflake SPCS (containerized workers):

### Volume Configuration

```yaml
# In SPCS service spec
volumes:
  - name: temp-storage
    source: local
    size: 10Gi  # Adjust based on expected test duration

volumeMounts:
  - name: temp-storage
    mountPath: /tmp
```

### Considerations

1. **Temp directory**: Use `/tmp` which maps to container-local storage
2. **Stage access**: SPCS containers have access to internal stages via service credentials
3. **Cleanup**: Local cleanup happens automatically on container termination
4. **Multi-container**: Each container writes to its own temp dir and stage subfolder

## Implementation Checklist

### Phase 1: Infrastructure

- [ ] Create `backend/core/file_query_logger.py`
- [ ] Add `QUERY_EXECUTIONS_STAGE` to `sql/schema/results_tables.sql`
- [ ] Apply schema changes to Snowflake

### Phase 2: Worker Integration

- [ ] Update `scripts/run_worker.py` to use `FileBasedQueryLogger`
- [ ] Update `backend/core/test_executor.py` to use sync `append()`
- [ ] Add `finalize()` call to worker shutdown sequence
- [ ] Add `cleanup_on_error()` to exception handlers

### Phase 3: Testing

- [ ] Unit tests for `FileBasedQueryLogger`
  - [ ] Buffer flushing
  - [ ] File splitting at threshold
  - [ ] Parquet schema correctness
- [ ] Integration tests
  - [ ] PUT + COPY INTO flow
  - [ ] Stage cleanup verification
  - [ ] Multi-worker concurrent uploads
- [ ] Performance validation
  - [ ] Verify no concurrency dips during benchmark
  - [ ] Measure PROCESSING phase duration impact

### Phase 4: Cleanup

- [ ] Remove `QueryExecutionStreamer` class
- [ ] Remove `query_execution_streamer.py`
- [ ] Update `docs/plan/query-execution-streaming.md` status to "Superseded"
- [ ] Update architecture documentation

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Disk space exhaustion | Low | Medium | Monitor temp dir size; set file count limit |
| PUT failures | Low | Medium | Retry with backoff; preserve local files on error |
| COPY INTO partial failure | Low | Low | ON_ERROR=CONTINUE; log skipped files |
| Orphaned stage files | Low | Low | Stage cleanup in finalize; periodic stage garbage collection |
| Local file permission issues | Low | Medium | Use tempfile.mkdtemp() for safe temp dirs |

## Performance Expectations

| Metric | Streaming Approach | File-Based Approach |
|--------|-------------------|---------------------|
| Benchmark concurrency dips | 30-60 (periodic) | None expected |
| Event loop blocking | Yes (lock contention) | No |
| Memory usage | ~135MB buffer | ~5MB buffer + disk |
| PROCESSING phase duration | N/A (already streaming) | +10-60s (PUT + COPY) |
| Total test duration | Slightly shorter | Slightly longer |

**Tradeoff**: We accept slightly longer total test time (PROCESSING phase) in exchange for stable benchmark concurrency.

## References

- [Snowflake COPY INTO](https://docs.snowflake.com/en/sql-reference/sql/copy-into-table)
- [Snowflake PUT Command](https://docs.snowflake.com/en/sql-reference/sql/put)
- [PyArrow Parquet](https://arrow.apache.org/docs/python/parquet.html)
- [query-execution-streaming.md](query-execution-streaming.md) - Previous streaming approach
