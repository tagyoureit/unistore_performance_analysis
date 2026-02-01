# Implementation Decisions

Recorded decisions made during the multi-worker architecture implementation.

## 2.3 Decisions (OrchestratorService: Start)

1. **Nomenclature**: Use "worker" terminology everywhere, not "node". All CLI
   args, variables, and docs should use `worker_id`, `per_worker_connections`,
   etc.

2. **WorkerSpawner Scope**: Start with local subprocess support embedded
   directly in `OrchestratorService` (no abstraction class yet). SPCS support
   will be added in Phase 3.

3. **Poll Loop Lifecycle**: The poll loop starts automatically when
   `start_run()` is called. It runs as a background task tied to that run.

4. **Scope Boundary**:
   - **2.3 includes**: `start_run()` implementation, RUN_STATUS update, START
     event emission, local worker spawning, poll loop start (minimal).
   - **2.4 includes**: Worker-side CLI args, heartbeats, watchdog, start gate.
   - **2.5 includes**: Poll loop logic (heartbeat scan, metrics aggregation,
     phase transitions, guardrails).
   - **2.6 includes**: `stop_run()` implementation, drain timeout, force
     termination.

## Phase 2 Control-Plane Decisions (Internal Orchestrator Only)

- **Ownership**: Orchestrator owns `RUN_STATUS` and the parent `TEST_RESULTS`
  row. The controller is the API entrypoint and calls the orchestrator directly.
- **Invocation**: Direct async method calls (no internal HTTP/RPC) with a
  per-run background poll loop.
- **Event Ordering**: `RUN_CONTROL_EVENTS.SEQUENCE_ID` is monotonic per `RUN_ID`.
  Workers track the last seen sequence and process events in order.
- **Event Scoping**: `RUN_CONTROL_EVENTS.EVENT_DATA.scope` is one of `RUN`,
  `WORKER_GROUP`, or `WORKER`. `START`, `STOP`, `SET_PHASE` use `RUN` scope.
  Per-worker target updates use `WORKER` scope and include explicit targets.
- **Snapshot Cadence**: Workers write `WORKER_METRICS_SNAPSHOTS` every 1s; the
  controller aggregates on a 1s tick. Warehouse polling remains 5s.
- **Target Allocation**: For total delta changes, distribute evenly across
  workers using `WORKER_GROUP_ID` order: base = floor(delta / N), remainder
  distributed to lowest group IDs. Scale-down uses the same rule.
- **API Contract**:
  - **UI Start flow**: prepare via `POST /api/tests/from-template/{id}`, then start via
    `POST /api/tests/{test_id}/start`. All scaling modes (AUTO, BOUNDED, FIXED) use
    the same endpoints. The dashboard polls `/api/tests/{test_id}` until status is
    RUNNING or STOPPING, then connects WebSocket.
  - **Programmatic flow**: `POST /api/runs` creates a PREPARED run,
    `POST /api/runs/{run_id}/start` starts it, `POST /api/runs/{run_id}/stop`
    requests STOP.
  - Controller does not make per-worker API calls; worker commands are
    broadcast via `RUN_CONTROL_EVENTS`.

## Phase 4 Decisions (Query Execution Streaming)

See [query-execution-streaming.md](query-execution-streaming.md) for full design document.

### Architecture Decisions

1. **Worker-side streaming**: Each worker streams directly to `QUERY_EXECUTIONS` table.
   No orchestrator collection infrastructure - the architecture is "Snowflake as message bus".

2. **Separate connection pools**: Control pool (from `SNOWFLAKE_WAREHOUSE` in .env) handles
   results persistence. Benchmark pool (from template `warehouse_name`) handles workload.
   System enforces `warehouse_name != SNOWFLAKE_WAREHOUSE`.

3. **Hybrid table for QUERY_EXECUTIONS**: Row-level locking enables concurrent INSERTs
   from 50+ workers. Already proven with `WORKER_HEARTBEATS` table.

4. **QueryExecutionStreamer class**: New dedicated class (`backend/core/query_execution_streamer.py`)
   to separate concerns from TestExecutor. Handles buffering, flushing, sampling, and shutdown.

### Streaming Parameters

| Parameter | Value | Rationale |
|-----------|-------|-----------|
| Flush interval | 5 seconds | Balance latency vs write overhead |
| Flush interval bias | Prefer lower interval if no benchmark impact | Minimize ingest lag |
| Batch trigger | 1000 records | Headroom above 5s interval; caps buffer to ~10s at 100 QPS/worker |
| Batch INSERT size | 500 rows | Proven chunk size |
| Back-pressure threshold | 100,000 records | Back off recording when writes can't keep up |
| Control warehouse scaling | Auto-scale configured in backend | Avoid manual tuning |

### Failure Behavior

- **Benchmark priority**: Never throttle benchmark queries for recording.
- **Flush failure**: Back off recording immediately; benchmark continues; mark run partial.
- **Shutdown timeout**: Best-effort flush; remaining records dropped and run marked partial.
- **Buffer overflow**: Back off recording immediately when buffer exceeds 100k records.

### Sampling Strategy

1. **Never drop errors**: 100% retention for `success=False` records
2. **Never drop warmup**: 100% retention for warmup phase queries
3. **Sample measurement queries**: Constant per-run rate based on template target QPS (temporary guardrail)
4. **Phase classification**: Captured at query submit time using latest `RUN_CONTROL_EVENTS.SEQUENCE_ID`
5. **Standard tables**: Target full capture; fixed sampling remains until ingest headroom is validated

**Sample rate calculation (DECIDED)**:
- Compute sample rate once at run start from template target QPS and worker count
- Store on `TEST_RESULTS.SAMPLE_RATE` and apply for the full run

### Downstream Impact

- **QPS calculations**: Scale by `1/sample_rate`
- **Error rates**: Errors are 100% captured; scale success count by sample rate
- **UI**: Show sample rate in test summary + indicator on charts when `sample_rate < 1.0`
- **Post-test p50/p95/p99 and latency**: Calculated from sampled measurement rows (no scaling on percentiles)
- **Live dashboard charts**: Unchanged; real-time p50/p95/p99 and QPS use in-memory metrics
- **Time buckets**: Use Snowflake server time for per-second charts to avoid worker clock skew
- **Recording backoff**: Post-test stats are partial and must be flagged as incomplete

### Enrichment

- **Still attempt enrichment**: Even with streaming, enrich from QUERY_HISTORY
- **Accept low match rates**: Hybrid tables achieve 50-70% enrichment; some benchmarks may be 3-5%
- **No change to enrichment timing**: Still runs once at end with retry logic
