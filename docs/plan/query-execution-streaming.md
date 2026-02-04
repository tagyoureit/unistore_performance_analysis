# Query Execution Streaming

**Status**: Superseded by [file-based-query-logging.md](file-based-query-logging.md)  
**Created**: 2025-01-31  
**Last Updated**: 2026-02-04

> **SUPERSEDED**: This streaming approach caused event loop contention at high QPS,
> resulting in periodic concurrency dips (100 → 30-60). The file-based approach
> in [file-based-query-logging.md](file-based-query-logging.md) eliminates this
> by deferring Snowflake writes to the PROCESSING phase.

## Problem Statement

The current query execution persistence approach has several limitations:

1. **200k record cap**: Workers use `deque(maxlen=200_000)` - high-QPS tests lose data via FIFO eviction
2. **No streaming**: All records written at test shutdown, creating "thundering herd" when 100+ workers write simultaneously
3. **Memory pressure**: Long-running tests accumulate records in memory until shutdown
4. **Enrichment limitations**: Hybrid table workloads only achieve 50-70% enrichment from QUERY_HISTORY (sampled data)

### Scale Target

- Maximum: 5,000 QPS across all workers
- Example configurations: 50 workers × 100 QPS, 100 workers × 50 QPS
- Duration: Up to 1 hour (5k × 3600 = 18M records)

## Current Architecture

```
Worker executes query
    ↓
Append to in-memory deque (maxlen=50000)
    ↓
[... test runs ...]
    ↓
STOP signal received
    ↓
Bulk INSERT to QUERY_EXECUTIONS (standard table)
    ↓
Orchestrator enriches from QUERY_HISTORY
```

### Current Connection Pools

| Pool | Warehouse | Purpose |
|------|-----------|---------|
| Control pool | `SNOWFLAKE_WAREHOUSE` (.env) | Results persistence, UI reads, control tables |
| Telemetry pool | `SNOWFLAKE_WAREHOUSE` (.env) | Out-of-band warehouse sampling |
| Benchmark pool | Template `warehouse_name` | Benchmark query execution |

The system enforces `warehouse_name != SNOWFLAKE_WAREHOUSE` to isolate benchmark load from persistence.

### Key Files

| Component | Location |
|-----------|----------|
| Query execution capture | `backend/core/test_executor.py:2850-3027` |
| Record append | `backend/core/test_executor.py:3000-3027` |
| Worker persist | `scripts/run_worker.py:1092-1123` |
| Bulk insert | `backend/core/results_store.py:892-1001` |
| Table schema | `sql/schema/results_tables.sql:255-307` |
| Enrichment | `backend/core/results_store.py:1448-1498` |

## Proposed Architecture

### Overview

```
┌─────────────────────────────────────────────────────────────────┐
│ WORKER PROCESS                                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Query Execution ──► QueryExecutionStreamer                     │
│  (benchmark pool)    ├─ _buffer: deque (unbounded)              │
│                      ├─ _flush_task: asyncio.Task               │
│                      └─ _pool: control pool (SNOWFLAKE_WH)      │
│                                                                 │
│  Flush triggers:                                                │
│    • Every 30 seconds (relative timer, natural jitter)          │
│    • OR buffer >= 5000 records                                  │
│    • OR shutdown signal received                                │
│                                                                 │
│  On flush failure: back off recording → benchmark continues     │
│  → mark run partial                                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
                    QUERY_EXECUTIONS (STANDARD TABLE)
                    ├─ Batched INSERTs (2000-5000 rows)
                    ├─ ~1.7 INSERTs/sec average (50 workers)
                    └─ Control warehouse (separate from benchmark)
```

### Why Standard Table (Not Hybrid)

Initial assumption was that hybrid tables would provide better concurrent write throughput.
Analysis revealed the opposite for this use case:

| Table Type | Optimized For | Bulk INSERT Throughput |
|------------|---------------|------------------------|
| **Standard** | Bulk/batch operations | Millions/min (COPY), 100k+/min (INSERT) |
| **Hybrid** | Single-row OLTP | ~8k ops/sec ceiling, row-by-row overhead |

**Key insight**: Hybrid tables pay overhead for row-level locking and transactional
guarantees that aren't needed for append-only telemetry. Standard tables with batched
INSERTs outperform hybrid for this workload.

**Write load calculation**:
- 50 workers × 1 INSERT every 30 seconds = ~1.7 INSERTs/sec average
- Workers start at different times → natural flush stagger (no explicit jitter needed)
- Standard tables have no ops/sec ceiling—just warehouse capacity

### QueryExecutionStreamer Class

New class: `backend/core/query_execution_streamer.py`

Responsibilities:
- Buffer management (unbounded deque)
- Background flush task (30s interval or 5000 records)
- Batched INSERTs (2000-5000 rows per statement)
- Failure handling (re-queue once, then drop batch)
- Graceful shutdown with flush-latency-based timeout

```python
class QueryExecutionStreamer:
    def __init__(
        self,
        pool: SnowflakeConnectionPool,
        test_id: str,
        flush_interval_seconds: float = 30.0,
        flush_threshold_records: int = 5000,
        batch_insert_size: int = 2000,
    ): ...

    async def append(self, record: QueryExecutionRecord) -> None:
        """Add record to buffer (100% capture, no sampling)."""
        ...

    async def start(self) -> None:
        """Start background flush task."""
        ...

    async def shutdown(self, flush_p99_ms: float) -> None:
        """Flush remaining buffer. Timeout = 2x flush p99 (min 30s)."""
        ...
```

## Sampling Strategy

### Current Approach: 100% Capture (No Sampling)

With standard table batched INSERTs, the system can handle full capture at 5,000 QPS:
- 50 workers × 30s flush interval × 100 QPS = 3,000 records/batch
- ~1.7 INSERTs/sec to Snowflake (well within capacity)
- Storage: 18M records/hour is acceptable (storage is cheap)

**Decision**: Implement 100% capture. No sampling logic needed in initial implementation.

### Future Option: Sampling (Not Implemented)

If future requirements exceed standard table capacity (e.g., 10k+ QPS, multi-hour tests),
sampling can be added as a configuration option:

| Record Type | Sample Rate | Rationale |
|-------------|-------------|-----------|
| Errors (`success=False`) | 100% (never drop) | Errors are rare and critical |
| Warmup queries | 100% (never drop) | Low volume, needed for diagnostics |
| Measurement queries | Configurable % | Statistical accuracy maintained with 30k+ samples |

**When to revisit**: If storage costs become significant or write throughput is insufficient
at higher scale targets.

### Metadata Tracking (For Future Sampling)

If sampling is implemented later, store metadata for downstream correction:

```sql
-- In TEST_RESULTS (add when sampling is implemented)
SAMPLE_RATE FLOAT,              -- e.g., 0.10 for 10%
TOTAL_QUERIES_ISSUED NUMBER,    -- Actual count before sampling
SAMPLED_QUERIES_STORED NUMBER   -- Count after sampling
```

## Downstream Impact Analysis

### With 100% Capture (Current Plan)

No query adjustments needed for sampling. All existing queries work unchanged.

**Time bucketing**: Use Snowflake server time for per-second bucketization to avoid
worker clock skew. Record server timestamp at insert time.

### If Sampling Is Added Later

The following queries would need adjustment:

| Data Use | Current Location | Mitigation |
|----------|------------------|------------|
| Post-test percentiles | `test_results.py:598-682` | Statistical sampling maintains accuracy |
| Per-second overhead timeseries | `test_results.py:4081-4191` | Lower sample count per bucket (acceptable) |
| Error rate calculations | `test_results.py:1586-1599` | Never sample errors |
| Actual QPS calculation | `test_results.py:5085-5091` | Scale by `1/sample_rate` |
| Query kind breakdown | `test_results.py:5053-5062` | Stratified sampling helps |

### NOT Affected

| Data Use | Reason |
|----------|--------|
| Live dashboard p50/p95/p99 | Uses `WORKER_METRICS_SNAPSHOTS` (in-memory aggregates) |
| Parent run aggregates | Uses `WORKER_METRICS_SNAPSHOTS` |
| Real-time WebSocket updates | Uses in-memory metrics |

### Time Bucketing (DECIDED)

- Use Snowflake server time for per-second bucketization to avoid worker clock skew.
- Record server timestamp at insert time and use it for charts and time-series calculations.

### Query Adjustments Required

**QPS Calculation:**
```sql
-- Before
SELECT COUNT(*) / DATEDIFF('second', MIN(START_TIME), MAX(START_TIME)) AS qps

-- After
SELECT COUNT(*) / NULLIF(tr.SAMPLE_RATE, 0) / DATEDIFF(...) AS qps
FROM QUERY_EXECUTIONS qe
JOIN TEST_RESULTS tr ON qe.TEST_ID = tr.TEST_ID
```

**Error Rates:**
```sql
-- Errors are 100% captured, success queries sampled
SELECT 
    SUM(CASE WHEN SUCCESS = FALSE THEN 1 ELSE 0 END) AS errors,
    SUM(CASE WHEN SUCCESS = TRUE THEN 1 ELSE 0 END) / tr.SAMPLE_RATE AS estimated_success
```

### Output Calculations Impact (Summary)

- **Live dashboard charts**: Unchanged; live p50/p95/p99 and QPS use in-memory metrics.
- **Post-test p50/p95/p99 and latency**: Calculated from all measurement rows (100% capture).
- **Per-second latency charts**: Buckets use server time; full data available.
- **QPS and success counts**: Direct counts (no scaling needed).
- **Error rates**: Direct calculation from full data.
- **Warmup metrics**: Unchanged (100% retained).

## Design Decisions

### Decided

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Architecture | Worker-side streaming | No orchestrator collection infrastructure exists |
| Table type | Standard | Batched INSERTs outperform hybrid for append-only telemetry |
| Connection pool | Control pool | Already separated from benchmark warehouse |
| Flush interval | 30 seconds | Natural jitter from worker start times; ~1.7 INSERTs/sec |
| Batch trigger | 5000 records | Caps buffer to ~50s at 100 QPS/worker |
| Batch INSERT size | 2000-5000 rows | Sweet spot for standard table INSERT performance |
| Failure behavior | Re-queue once, then drop batch | One retry attempt; benchmark continues unaffected |
| Shutdown timeout | 2× flush p99 (min 30s) | Aligns drain timeout with insert latency |
| Capture rate | 100% (no sampling) | Standard tables handle 5k QPS; storage is cheap |
| Time authority | Snowflake server time | Consistent cross-worker bucketization |
| Benchmark priority | Never throttle benchmark for recording | Preserve benchmark fidelity |
| Control warehouse scaling | Auto-scale configured in backend | Avoid manual tuning |
| Enrichment | Still attempt | Accept 3-70% match rate depending on workload |

### Open
None.

### Future Considerations

| Item | Trigger | Action |
|------|---------|--------|
| Sampling | 10k+ QPS or multi-hour tests | Add configurable sample rate |
| Hybrid tables | Single-row update patterns | Reconsider if workload changes |
| Snowflake Postgres | Extreme write throughput needs | Evaluate if standard tables insufficient |

## Implementation Plan

### Phase 1: Core Infrastructure

1. **Create `QueryExecutionStreamer` class**
   - `backend/core/query_execution_streamer.py`
   - Buffer management, flush task, batched INSERTs
   - Failure handling/backoff
   - Graceful shutdown

2. **Keep `QUERY_EXECUTIONS` as standard table**
   - No schema changes needed
   - Existing table structure works as-is

### Phase 2: Worker Integration

3. **Integrate into worker**
   - `scripts/run_worker.py`
   - Instantiate streamer with control pool
   - Wire `append()` calls
   - Handle shutdown sequence

4. **Update TestExecutor**
   - `backend/core/test_executor.py`
   - Remove `maxlen=50000` cap
   - Replace deque append with `streamer.append()`
   - Remove end-of-test bulk persist

5. **Update results_store**
   - `backend/core/results_store.py`
   - Keep `insert_query_executions()` for streamer use
   - Remove worker-shutdown bulk insert logic

### Phase 3: Testing & Validation

6. **Unit tests**
   - QueryExecutionStreamer flush behavior
   - Re-queue on failure, drop on second failure
   - Graceful shutdown

7. **Integration tests**
   - High-QPS scenario (verify streaming keeps up)
   - Concurrent workers (verify no contention issues)
   - Shutdown sequence (verify all records flushed)

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Standard table INSERT throughput | Low | Medium | Batched INSERTs well within capacity; warehouse auto-scales |
| Enrichment match rate drops further | Medium | Low | Already lossy (50-70%), acceptable |

## References

- [Snowflake Hybrid Tables](https://docs.snowflake.com/en/user-guide/tables-hybrid)
- [Hybrid Table Best Practices](https://docs.snowflake.com/en/user-guide/tables-hybrid-best-practices)
- `docs/architecture-overview.md` - System architecture
- `docs/data-flow-and-lifecycle.md` - Data flow patterns
