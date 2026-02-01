# Query Execution Streaming

**Status**: Planning  
**Created**: 2025-01-31  
**Last Updated**: 2025-01-31

## Problem Statement

The current query execution persistence approach has several limitations:

1. **50k record cap**: Workers use `deque(maxlen=50000)` - high-QPS tests lose data via FIFO eviction
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
│                      ├─ _pool: control pool (SNOWFLAKE_WH)      │
│                      └─ _sample_rate: float                     │
│                                                                 │
│  Flush triggers:                                                │
│    • Every 5 seconds                                            │
│    • OR buffer >= 1000 records                                  │
│    • OR shutdown signal received                                │
│                                                                 │
│  On flush failure: back off recording → benchmark continues     │
│  → mark run partial                                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
                               │
                               ▼
                    QUERY_EXECUTIONS (HYBRID)
                    ├─ Row-level locking
                    ├─ 50 workers × concurrent INSERTs
                    └─ Control warehouse (separate from benchmark)
```

### Why Hybrid Table

From Snowflake documentation:

> "Hybrid tables provide a row-based storage engine that supports row locking for high concurrency."

> "High-concurrency random writes, including inserts, updates, and merges"

> "Metadata for applications and workflows, such as maintaining state for an ingestion workflow that requires high-concurrency updates to a single table from thousands of parallel workers"

The project already uses hybrid tables for `WORKER_HEARTBEATS` (same concurrent write pattern).

### QueryExecutionStreamer Class

New class: `backend/core/query_execution_streamer.py`

Responsibilities:
- Buffer management (unbounded deque, with back-pressure threshold)
- Background flush task (5s interval or 1000 records)
- Batched INSERTs (500 rows per statement)
- Sampling logic (100% errors, 100% warmup, constant % for measurement)
- Failure handling (back off recording; benchmark continues)
- Graceful shutdown with flush-latency-based timeout

```python
class QueryExecutionStreamer:
    def __init__(
        self,
        pool: SnowflakeConnectionPool,
        test_id: str,
        sample_rate: float = 1.0,
        flush_interval_seconds: float = 5.0,
        flush_threshold_records: int = 1000,
        back_pressure_threshold: int = 100_000,
    ): ...

    async def append(self, record: QueryExecutionRecord) -> None:
        """Add record to buffer. May sample based on sample_rate."""
        ...

    async def start(self) -> None:
        """Start background flush task."""
        ...

    async def shutdown(self, flush_p99_ms: float) -> None:
        """Flush remaining buffer. Timeout = 2x flush p99 (min 30s)."""
        ...
```

## Sampling Strategy

### Why Sample

1. **Enrichment is already lossy**: Hybrid tables achieve only 50-70% match rate with QUERY_HISTORY
2. **Statistical accuracy**: 30k samples provide excellent percentile accuracy
3. **Storage efficiency**: 18M records → 1.8M with 10% sampling

### Sampling Rules

| Record Type | Sample Rate | Rationale |
|-------------|-------------|-----------|
| Errors (`success=False`) | 100% (never drop) | Errors are rare and critical |
| Warmup queries | 100% (never drop) | Low volume, needed for diagnostics |
| Measurement queries | Configurable % (constant per run) | Statistical accuracy maintained |

### Phase Classification (DECIDED)

- Phase is captured at query submit time using the latest `RUN_CONTROL_EVENTS.SEQUENCE_ID`.
- Sampling decisions use the captured phase (warmup is always 100%).

### Sample Rate Calculation (DECIDED)

Use template target QPS to compute a constant per-run sample rate:

```python
target_total_qps = template_config.get("target_qps")
num_workers = template_config.get("num_workers")
expected_per_worker = target_total_qps / num_workers
sample_rate = min(1.0, TARGET_SAMPLES_PER_SEC / expected_per_worker)
```
- Computed once at run start and applied for the full run
- Stored on `TEST_RESULTS.SAMPLE_RATE` for downstream scaling
- Avoids per-worker or time-varying rates

### Standard Tables Note (DECIDED)

- Target full capture with no sampling on standard tables once ingest headroom is proven.
- Fixed sampling remains the initial guardrail; revisit after capacity validation.

### Metadata Tracking

Store sampling metadata for downstream correction:

```sql
-- In TEST_RESULTS
SAMPLE_RATE FLOAT,              -- e.g., 0.10 for 10%
TOTAL_QUERIES_ISSUED NUMBER,    -- Actual count before sampling
SAMPLED_QUERIES_STORED NUMBER   -- Count after sampling
```

### Ingest Backoff Policy (DECIDED)

- Benchmark execution is primary; never throttle benchmark to protect recording.
- If ingest pressure threatens benchmark or control-plane bottlenecks appear, back off recording immediately.
- When backoff triggers, mark results as partial and surface the condition in summaries/charts.

## Downstream Impact Analysis

### Affected by Sampling

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
- **Post-test p50/p95/p99 and latency**: Calculated from sampled measurement rows. No scaling is applied to percentile values; accuracy depends on sample size.
- **Per-second latency charts**: Buckets use server time; buckets with low sample counts are noisier.
- **QPS and success counts**: Scale measurement query counts by `1 / sample_rate`.
- **Error rates**: Errors are exact (100% retained). Success counts are scaled.
- **Warmup metrics**: Unchanged (100% retained).
- **Recording backoff**: If backoff triggers, post-test stats are partial and must be flagged as incomplete.

## Design Decisions

### Decided

| Decision | Choice | Rationale |
|----------|--------|-----------|
| Architecture | Worker-side streaming | No orchestrator collection infrastructure exists |
| Table type | Hybrid | Row-level locking for concurrent INSERTs |
| Connection pool | Control pool | Already separated from benchmark warehouse |
| Primary key | `EXECUTION_ID` (UUID) | Already generated and unique |
| Flush interval | 5 seconds | Balance latency vs write overhead |
| Batch trigger | 1000 records | Headroom above 5s interval; caps buffer to ~10s at 100 QPS/worker |
| Batch INSERT size | 500 rows | Proven chunk size |
| Failure behavior | Back off recording; benchmark continues | Protect benchmark from ingest bottlenecks |
| Shutdown timeout | 2× flush p99 (min 30s) | Aligns drain timeout with insert latency |
| Error sampling | Never drop | Errors are rare and critical |
| Warmup sampling | Never drop | Needed for diagnostics |
| Sample rate calculation | Template target QPS (constant per run) | Consistent across workers and stable scaling |
| Standard tables | Target full capture; fixed sampling is temporary | Remove sampling after headroom validation |
| Phase classification | At query submit time | Prevents phase boundary drift |
| Time authority | Snowflake server time | Consistent cross-worker bucketization |
| Benchmark priority | Never throttle benchmark for recording | Preserve benchmark fidelity |
| Flush interval bias | Prefer lower interval if no benchmark impact | Minimize ingest lag |
| Control warehouse scaling | Auto-scale configured in backend | Avoid manual tuning |
| Buffer back-pressure | Back off recording at 100k records | Protect benchmark under ingest pressure |
| UI indication | Show sample rate in summary + chart indicators | User needs to know data is sampled |
| Enrichment | Still attempt | Accept 3-70% match rate depending on workload |

### Open
None.

## Implementation Plan

### Phase 1: Core Infrastructure

1. **Create `QueryExecutionStreamer` class**
   - `backend/core/query_execution_streamer.py`
   - Buffer management, flush task, batched INSERTs
   - Sampling logic
   - Failure handling/backoff
   - Graceful shutdown

2. **Convert `QUERY_EXECUTIONS` to hybrid**
   - `sql/schema/results_tables.sql`
   - Add PRIMARY KEY on `EXECUTION_ID`
   - Migration script for existing data

3. **Update `TEST_RESULTS` schema**
   - Add `SAMPLE_RATE` column
   - Add `TOTAL_QUERIES_ISSUED` column
   - Add `SAMPLED_QUERIES_STORED` column

### Phase 2: Worker Integration

4. **Integrate into worker**
   - `scripts/run_worker.py`
   - Instantiate streamer with control pool
   - Wire `append()` calls
   - Handle shutdown sequence

5. **Update TestExecutor**
   - `backend/core/test_executor.py`
   - Remove `maxlen=50000` cap
   - Replace deque append with `streamer.append()`
   - Remove end-of-test bulk persist

6. **Update results_store**
   - `backend/core/results_store.py`
   - Keep `insert_query_executions()` for streamer use
   - Remove worker-shutdown bulk insert logic

### Phase 3: Downstream Updates

7. **Update downstream queries**
   - `backend/api/routes/test_results.py`
   - Scale COUNTs by sample rate
   - Adjust QPS calculations
   - Update error rate calculations

8. **Update UI**
   - Add sample rate indicator to test summary
   - Add "sampled data" indicator on charts when `sample_rate < 1.0`

### Phase 4: Testing & Validation

9. **Unit tests**
   - QueryExecutionStreamer flush behavior
   - Sampling logic (errors never dropped)
   - Back-pressure handling
   - Graceful shutdown

10. **Integration tests**
    - High-QPS scenario (verify streaming keeps up)
    - Concurrent workers (verify no contention issues)
    - Shutdown sequence (verify all records flushed)

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Hybrid table write contention | Low | Medium | Row-level locking, proven with WORKER_HEARTBEATS |
| Buffer overflow under extreme load | Low | High | Back-pressure threshold backs off recording |
| Sampling affects percentile accuracy | Low | Medium | 30k+ samples provide <3% error |
| Enrichment match rate drops further | Medium | Low | Already lossy (50-70%), acceptable |
| Downstream queries break | Medium | Medium | Comprehensive test coverage |

## References

- [Snowflake Hybrid Tables](https://docs.snowflake.com/en/user-guide/tables-hybrid)
- [Hybrid Table Best Practices](https://docs.snowflake.com/en/user-guide/tables-hybrid-best-practices)
- `docs/architecture-overview.md` - System architecture
- `docs/data-flow-and-lifecycle.md` - Data flow patterns
