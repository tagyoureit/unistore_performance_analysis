# Multi-Worker Specifications (Implementation Details)

This document provides concrete schemas, SQL, and specifications for Phase 2
implementation. It fills gaps identified during plan review.

## Terminology

- **WORKER_ID**: Unique identifier for a worker process (string, e.g., `worker-0`)
- **WORKER_GROUP_ID**: Zero-based index for deterministic sharding (integer)
- **TARGET_CONNECTIONS**: Number of concurrent queries a worker should maintain
- **SEQUENCE_ID**: Monotonic counter for event ordering within a run
- **Warmup**: Run-level phase to prime Snowflake compute (not per-worker)

## Design Decisions

- **Worker START timeout**: 120 seconds (balance between startup delays and
  zombie detection).
- **Graceful drain timeout**: 120 seconds (allow in-flight queries to complete).
- **Poll loop**: Per-run background task (isolated lifecycle, easier cleanup).
- **Snapshot cadence**: 1 second for `WORKER_METRICS_SNAPSHOTS` and heartbeats.
- **Control event polling**: 1 second cadence for workers.
- **Event ordering**: `SEQUENCE_ID` is monotonic per `RUN_ID`, generated via
  `RUN_STATUS.NEXT_SEQUENCE_ID` atomic increment.
- **Schema versioning**: None (complete migration, no legacy support).
- **OrchestratorService location**: `backend/core/orchestrator.py`
  (parallel to existing `test_registry.py`).
- **UI Start flow**: prepare via `POST /api/tests/from-template/{id}`, then start via
  `POST /api/tests/{test_id}/start`. All scaling modes (AUTO, BOUNDED, FIXED) use
  the same endpoints. The dashboard polls `/api/tests/{test_id}` until
  RUNNING/STOPPING, then connects WebSocket.
- **Warmup model**: Single run-level warmup; workers joining after warmup
  inherit MEASUREMENT phase.
- **API query parallelization**: `/api/tests/{id}` uses `asyncio.gather` to run
  6-8 Snowflake queries concurrently, reducing response time from 7+ seconds
  to 0.6-1.1 seconds (warm) or 2-4 seconds (cold warehouse).
- **Phase normalization**: The backend uses `MEASUREMENT` for the main test phase,
  but the frontend displays this as "Running". The frontend's `normalizePhase()`
  function handles this mapping. The API also derives `phase=PROCESSING` when
  `status=COMPLETED` and `enrichment_status=PENDING` to ensure the UI shows the
  Processing phase during post-test enrichment.

## Results Schema Overview

- DDL sources (rerunnable):
  - `sql/schema/results_tables.sql` (core results tables + views)
  - `sql/schema/templates_table.sql` (template store)
  - `sql/schema/template_value_pools_table.sql` (high-concurrency pools)
  - `sql/schema/test_logs_table.sql` (test log events)
  - `sql/schema/control_tables.sql` (control-plane hybrid tables)
- Schema is applied via `backend/setup_schema.py`.
- The app does **not** execute DDL at runtime.

### Core Tables (Results Schema)

- `TEST_RESULTS`: summary, status, timing, rollups, `FIND_MAX_RESULT`, `QUERY_TAG`.
- `METRICS_SNAPSHOTS`: time-series snapshots (single-worker/legacy path).
- `WORKER_METRICS_SNAPSHOTS`: time-series snapshots per worker (autoscale runs).
- `QUERY_EXECUTIONS`: per-query execution records (enriched post-run).
- `TEST_LOGS`: per-test log events.
- `WAREHOUSE_POLL_SNAPSHOTS`: controller warehouse poller samples (~5s cadence).
- `FIND_MAX_STEP_HISTORY`: step history for FIND_MAX runs.

### Views (Results Schema)

- `V_LATEST_TEST_RESULTS`
- `V_METRICS_BY_MINUTE`
- `V_WAREHOUSE_METRICS`
- `V_CLUSTER_BREAKDOWN`
- `V_WAREHOUSE_TIMESERIES`

## Templates and Workloads

- Templates are stored in `UNISTORE_BENCHMARK.TEST_RESULTS.TEST_TEMPLATES`.
- All UI runs are template-based; there is no ad-hoc run UI.
- Workload presets (READ_ONLY / WRITE_ONLY / READ_HEAVY / WRITE_HEAVY / MIXED) are
  UI convenience defaults only. The backend normalizes presets to `CUSTOM` on
  save (see `backend/api/routes/templates.py`).
- YAML templates in `config/test_scenarios/` are reference-only for the UI.
  `backend/core/template_loader.py` can load them programmatically.
- R180 reference scenario: `config/test_scenarios/r180_poc.yaml` (not used by UI).
- `TEMPLATE_VALUE_POOLS` stores large sampled pools to keep template configs small.
- Load pattern classes exist in `backend/core/load_patterns.py` but are not wired
  to the executor.

## Post-Run Enrichment Notes

- Enrichment merges `INFORMATION_SCHEMA.QUERY_HISTORY` into `QUERY_EXECUTIONS`
  using the per-test query tag prefix (`unistore_benchmark:test_id=<id>%`).
- QUERY_HISTORY table functions return a maximum of 10,000 rows per call;
  enrichment paginates by `END_TIME` until it passes the test window.
- `update_test_overhead_percentiles()` computes app overhead percentiles and
  stores them on `TEST_RESULTS`.
- Hybrid workloads can yield very low QUERY_HISTORY enrichment; prefer app-side
  metrics and derived overheads when SF timing data is sparse.
- For manual inspection, `QUERY_HISTORY_BY_WAREHOUSE` can provide same-day
  visibility when ACCOUNT_USAGE lags. For hybrid workloads, consider
  `AGGREGATE_QUERY_HISTORY`.

---

## 1. WebSocket Payload Schema

The WebSocket payload is the single source of truth for live dashboard data
after a run has started. Before Start, the dashboard is template-only and uses
HTTP polling after the prepare call returns `test_id`.
All runs (single-worker and multi-worker) use the same payload structure.

### WebSocket Event Semantics

The controller emits a single event type on `/ws/test/{run_id}`:

```json
{
  "event": "RUN_UPDATE",
  "data": { ...payload... }
}
```

Rules:

- One `RUN_UPDATE` is emitted every 1 second.
- Payload is a full snapshot (not a diff). The client replaces prior state.
- WebSocket is read-only: no control commands are sent over this channel.
- Control-plane commands are written by the orchestrator to `RUN_CONTROL_EVENTS`.

### Live Metrics Ingestion (Cache)

Workers POST live metrics snapshots to the controller every 1 second:

- **Endpoint**: `POST /api/runs/{run_id}/metrics/live`
- **Purpose**: feed the in-memory cache used by the WebSocket stream
- **Behavior**: best-effort, cache TTL defaults to 5s; WebSocket falls back to Snowflake

Request body (shape):
```json
{
  "test_id": "worker-test-id",
  "worker_id": "worker-0",
  "worker_group_id": 0,
  "worker_group_count": 2,
  "phase": "WARMUP",
  "status": "RUNNING",
  "target_connections": 25,
  "metrics": { "...": "Metrics payload (worker snapshot)" }
}
```

### Live Metrics Payload

```json
{
  "test_id": "uuid",
  "status": "RUNNING",
  "phase": "MEASUREMENT",
  "timestamp": "2026-01-23T10:15:30.123Z",
  "run": {
    "run_id": "uuid",
    "status": "RUNNING",
    "phase": "MEASUREMENT",
    "worker_count": 2,
    "elapsed_seconds": 125.5,
    "timing": {
      "elapsed_display_seconds": 125.5
    },
    "aggregate_metrics": {
      "total_ops": 125000,
      "qps": 1250.5,
      "p50_latency_ms": 12.5,
      "p95_latency_ms": 48.3,
      "p99_latency_ms": 98.7,
      "avg_latency_ms": 18.3,
      "error_rate": 0.00004,
      "total_errors": 5,
      "active_connections": 50,
      "target_connections": 50,
      "read_count": 100000,
      "write_count": 25000
    }
  },
  "timing": {
    "elapsed_display_seconds": 125.5
  },
  "latency_aggregation_method": "slowest_worker_approximation",
  "workers": [
    {
      "worker_id": "worker-0",
      "worker_group_id": 0,
      "status": "RUNNING",
      "phase": "MEASUREMENT",
      "health": "HEALTHY",
      "last_heartbeat": "2026-01-23T10:15:28.000Z",
      "last_heartbeat_ago_s": 2,
      "metrics": {
        "qps": 625.2,
        "p95_latency_ms": 42.1,
        "error_count": 2,
        "active_connections": 25,
        "target_connections": 25
      }
    },
    {
      "worker_id": "worker-1",
      "worker_group_id": 1,
      "status": "RUNNING",
      "phase": "MEASUREMENT",
      "health": "HEALTHY",
      "last_heartbeat": "2026-01-23T10:15:29.000Z",
      "last_heartbeat_ago_s": 1,
      "metrics": {
        "qps": 625.3,
        "p95_latency_ms": 48.3,
        "error_count": 3,
        "active_connections": 25,
        "target_connections": 25
      }
    }
  ],
  "ops": {
    "total": 125000,
    "current_per_sec": 1250.5
  },
  "operations": {
    "reads": 100000,
    "writes": 25000
  },
  "latency": {
    "p50": 12.5,
    "p95": 45.2,
    "p99": 98.7,
    "avg": 18.3
  },
  "errors": {
    "count": 5,
    "rate": 0.00004
  },
  "connections": {
    "active": 50,
    "target": 50
  },
  "warehouse": {
    "name": "BENCHMARK_WH",
    "started_clusters": 2,
    "running": 45,
    "queued": 0
  },
  "find_max": {
    "current_step": 3,
    "target_workers": 35,
    "baseline_p95_ms": 42.5,
    "current_p95_ms": 48.2,
    "p95_vs_baseline_pct": 13.4,
    "qps_vs_prior_pct": 2.1,
    "queue_detected": false,
    "status": "STEPPING"
  },
  "custom_metrics": {
    "app_ops_breakdown": {},
    "sf_bench": {},
    "resources": {}
  }
}
```

Notes:

- The canonical aggregate is `run.aggregate_metrics`. Top-level `ops`, `latency`,
  `errors`, and `connections` mirror the aggregate for existing chart code.
- `latency_aggregation_method` is `slowest_worker_approximation` for multi-worker
  parent runs (P95/P99 = max across workers, P50 = avg); single-worker runs use
  `null`.
- `timing.elapsed_display_seconds` is authoritative for elapsed time in live
  payloads (computed in Snowflake; completed runs use stored duration when
  available).
- **Duration calculation**: For completed runs, `DURATION_SECONDS` in TEST_RESULTS
  is computed from `END_TIME - START_TIME` rather than worker metrics snapshots,
  which can contain stale values during post-test processing (enrichment). The
  frontend also guards against dramatic elapsed time decreases to prevent UI
  flicker when stale data arrives.
- **HTTP Polling Termination**: When a test reaches a terminal state (COMPLETED,
  STOPPED, FAILED, CANCELLED), the frontend stops HTTP polling for test info and
  metrics to avoid unnecessary API calls. This is handled in both `loadTestInfo()`
  (data-loading.js) and the WebSocket message handler (dashboard.js).
- **Timer Behavior**: The frontend uses two separate timers:
  1. **Total Time**: Starts at 0 when Start button is clicked (optimistic update),
     increments continuously through all phases until test completes.
  2. **Phase Timer**: Shows phase-specific elapsed time (e.g., "4s / 10s" for warmup).
     Resets to 0 when entering each timed phase (WARMUP, RUNNING). Tracked via
     `_warmupStartElapsed` and `_runningStartElapsed` recorded at phase transitions.
- `warehouse` is sourced from the controller poller (`WAREHOUSE_POLL_SNAPSHOTS`).
- `find_max` is the controller-owned state from `RUN_STATUS.FIND_MAX_STATE`.

### Worker Health States

| Health | Condition | UI Display |
|--------|-----------|------------|
| `HEALTHY` | Heartbeat within 30s | Green indicator |
| `STALE` | Heartbeat 30-60s old | Yellow indicator + warning |
| `DEAD` | No heartbeat for 60s+ | Red indicator + excluded from aggregates |

### Status Values

| Status | Meaning |
|--------|---------|
| `PREPARED` | Run created, waiting to start |
| `RUNNING` | Workers active, load generating |
| `CANCELLING` | STOP issued, draining in-flight queries |
| `COMPLETED` | All workers finished successfully |
| `FAILED` | Error during execution |
| `CANCELLED` | User-initiated stop completed |

Note: In the UI flow, `PREPARED` is visible after prepare and before Start.
The programmatic `/api/runs` flow also uses `PREPARED` explicitly.

### Phase Values

| Phase | Meaning |
|-------|---------|
| `""` (empty) | Test is PREPARED but not yet started (awaiting user click) |
| `PREPARING` | Test started; setting up connections, spawning workers |
| `WARMUP` | Initial load, metrics not counted |
| `MEASUREMENT` | Main test period, metrics counted |
| `COOLDOWN` | Draining before finalization |
| `PROCESSING` | Workers draining (STOPPING) or post-run enrichment in progress |

**Note**: When a test is first created (STATUS=PREPARED), PHASE is empty string (column is NOT NULL).
The PREPARING phase is only set after the user clicks Start and the orchestrator begins spawning workers.

**PROCESSING Phase**: The PROCESSING phase is shown during two periods:
1. **Worker draining** (status=STOPPING): After measurement completes, workers drain in-flight queries
2. **Enrichment** (status=COMPLETED, enrichment_status=PENDING): Post-run query enrichment from QUERY_HISTORY

The WebSocket tracks `last_sent_phase` and guarantees PROCESSING is shown at least once before
transitioning to COMPLETED, even if enrichment completes faster than the 1-second polling interval.

---

## 2. Control Event Schemas

Events written to `RUN_CONTROL_EVENTS` by the orchestrator.

### STOP Event

```json
{
  "event_type": "STOP",
  "event_data": {
    "scope": "RUN",
    "reason": "user_requested",
    "initiated_by": "api",
    "drain_timeout_seconds": 120
  }
}
```

### START Event

```json
{
  "event_type": "START",
  "event_data": {
    "scope": "RUN",
    "expected_workers": 5
  }
}
```

### SET_PHASE Event

```json
{
  "event_type": "SET_PHASE",
  "event_data": {
    "scope": "RUN",
    "phase": "MEASUREMENT",
    "effective_at": "2026-01-23T10:15:30.000Z"
  }
}
```

### SET_WORKER_TARGET Event (Per-Worker Targeting)

```json
{
  "event_type": "SET_WORKER_TARGET",
  "event_data": {
    "scope": "WORKER",
    "worker_id": "worker-3",
    "worker_group_id": 3,
    "target_connections": 42,
    "target_qps": 250.0,
    "step_id": "uuid",
    "step_number": 3,
    "effective_at": "2026-01-23T10:15:30.000Z",
    "ramp_seconds": 5,
    "reason": "step_advance"
  }
}
```

### Event Targeting and Ordering

- `scope` determines which workers apply the event: `RUN` (all), `WORKER_GROUP`,
  or `WORKER`.
- For per-worker targets (QPS and FIND_MAX), the orchestrator emits one
  `SET_WORKER_TARGET` event per worker with explicit `target_connections`
  (and optional `target_qps`).
- `SEQUENCE_ID` is monotonic per `RUN_ID`. Workers track the last seen sequence
  and process events in order.

### Control Plane Processing Rules

- Workers ignore events with `SEQUENCE_ID <= last_seen_sequence`.
- `RUN`-scoped events apply to all workers.
- `WORKER_GROUP` events apply only to matching `worker_group_id`.
- `WORKER` events apply only to matching `worker_id`.
- `SET_WORKER_TARGET` uses absolute targets; workers do not compute deltas.

---

## 3. SCENARIO_CONFIG Schema

Stored in `RUN_STATUS.SCENARIO_CONFIG` as VARIANT.

```json
{
  "template_id": "uuid",
  "template_name": "High Concurrency Test",
  
  "target": {
    "table_name": "BENCHMARK_TABLE",
    "table_type": "HYBRID",
    "warehouse": "BENCHMARK_WH"
  },
  
  "workload": {
    "load_mode": "CONCURRENCY",
    "concurrent_connections": 100,
    "duration_seconds": 300,
    "warmup_seconds": 60,
    "read_percent": 80,
    "write_percent": 20
  },
  
  "scaling": {
    "mode": "AUTO",
    "worker_count": 5,
    "per_worker_capacity": 25,
    "worker_group_count": 5,
    "min_workers": 1,
    "max_workers": null,
    "min_connections": 1,
    "max_connections": null
  },
  
  "find_max": {
    "enabled": false,
    "start_concurrency": 5,
    "concurrency_increment": 10,
    "step_duration_seconds": 30,
    "qps_stability_pct": 5,
    "latency_stability_pct": 20,
    "max_error_rate_pct": 1
  },
  
  "guardrails": {
    "max_cpu_percent": 85,
    "max_memory_percent": 90
  }
}
```

### Scaling Block Fields

| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `mode` | string | No | `"AUTO"` | Scaling mode: `AUTO`, `BOUNDED`, or `FIXED` |
| `worker_count` | int | No | computed | Current/target worker count |
| `per_worker_capacity` | int | No | auto-detect | Max connections per worker |
| `worker_group_count` | int | No | = worker_count | Total worker slots |
| `min_workers` | int \| null | No | 1 | Minimum workers (BOUNDED/FIXED) |
| `max_workers` | int \| null | No | null | Max workers (BOUNDED) |
| `min_connections` | int \| null | No | 1 | Per-worker floor (all modes) |
| `max_connections` | int \| null | No | null | Per-worker ceiling (BOUNDED) |

**Representing "Unbounded":**

- Use `null` in JSON to mean "no user-defined limit".
- Accept `-1` as equivalent to `null` for consistency with `concurrent_connections=-1`.
- Omitted fields use the defaults above.

**Mode Behaviors:**

- **AUTO**: Current behavior. `worker_count` and per-worker targets computed
  from `concurrent_connections`, `target_qps`, or FIND_MAX config.
- **BOUNDED**: Auto-scale within `[min_workers, max_workers]` and
  `[min_connections, max_connections]`. QPS mode completes with
  `BOUNDS_LIMIT_REACHED` if ceiling is hit.
- **FIXED**: Exact allocation. Uses `min_workers` and `min_connections` as the
  fixed values. No auto-scaling. `max_*` fields ignored.

---

## 4. Aggregation SQL

### Parent Rollup Query (Live)

Used by the orchestrator to compute aggregate metrics for the live payload and
to persist minimal rollups to `RUN_STATUS` (for example, `TOTAL_OPS`,
`ERROR_COUNT`, `CURRENT_QPS`, and worker counts).

```sql
WITH latest_per_worker AS (
    SELECT
        wms.*,
        ROW_NUMBER() OVER (
            PARTITION BY WORKER_ID
            ORDER BY TIMESTAMP DESC
        ) AS rn,
        TIMESTAMPDIFF('second', TIMESTAMP, CURRENT_TIMESTAMP()) AS stale_seconds
    FROM WORKER_METRICS_SNAPSHOTS wms
    WHERE RUN_ID = :run_id
      AND PHASE = 'MEASUREMENT'  -- Only aggregate MEASUREMENT phase
),
healthy_workers AS (
    SELECT * FROM latest_per_worker
    WHERE rn = 1 AND stale_seconds <= 60
)
SELECT
    -- Timing (max across workers)
    MAX(ELAPSED_SECONDS) AS elapsed_seconds,
    
    -- Counts (sum across workers)
    SUM(TOTAL_QUERIES) AS total_ops,
    SUM(QPS) AS aggregate_qps,
    SUM(READ_COUNT) AS total_reads,
    SUM(WRITE_COUNT) AS total_writes,
    SUM(ERROR_COUNT) AS total_errors,
    SUM(ACTIVE_CONNECTIONS) AS total_active_connections,
    SUM(TARGET_CONNECTIONS) AS total_target_connections,
    
    -- Latency (slowest-worker for P95/P99, avg for P50)
    AVG(P50_LATENCY_MS) AS p50_latency_ms,
    MAX(P95_LATENCY_MS) AS p95_latency_ms,
    MAX(P99_LATENCY_MS) AS p99_latency_ms,
    AVG(AVG_LATENCY_MS) AS avg_latency_ms,
    
    -- Worker counts
    COUNT(*) AS healthy_worker_count,
    (SELECT COUNT(DISTINCT WORKER_ID) FROM latest_per_worker WHERE rn = 1) AS total_worker_count
    
FROM healthy_workers;
```

### Per-Worker Status Query

Used to populate the `workers` array in the WebSocket payload.

```sql
WITH latest_per_worker AS (
    SELECT
        wms.*,
        hb.STATUS AS worker_status,
        hb.LAST_HEARTBEAT,
        ROW_NUMBER() OVER (
            PARTITION BY wms.WORKER_ID
            ORDER BY wms.TIMESTAMP DESC
        ) AS rn
    FROM WORKER_METRICS_SNAPSHOTS wms
    LEFT JOIN WORKER_HEARTBEATS hb 
        ON wms.RUN_ID = hb.RUN_ID 
        AND wms.WORKER_ID = hb.WORKER_ID
    WHERE wms.RUN_ID = :run_id
)
SELECT
    WORKER_ID,
    WORKER_GROUP_ID,
    worker_status AS STATUS,
    PHASE,
    LAST_HEARTBEAT,
    TIMESTAMPDIFF('second', LAST_HEARTBEAT, CURRENT_TIMESTAMP()) AS stale_seconds,
    QPS,
    P95_LATENCY_MS,
    ERROR_COUNT,
    ACTIVE_CONNECTIONS,
    TARGET_CONNECTIONS
FROM latest_per_worker
WHERE rn = 1
ORDER BY WORKER_GROUP_ID;
```

### Final Parent Rollup Query (Post-Run)

True percentiles from all query executions.

```sql
SELECT
    COUNT(*) AS total_operations,
    SUM(
        CASE
            WHEN QUERY_KIND IN ('POINT_LOOKUP', 'RANGE_SCAN') THEN 1
            ELSE 0
        END
    ) AS read_operations,
    SUM(
        CASE
            WHEN QUERY_KIND IN ('INSERT', 'UPDATE', 'DELETE') THEN 1
            ELSE 0
        END
    ) AS write_operations,
    SUM(CASE WHEN SUCCESS = FALSE THEN 1 ELSE 0 END) AS failed_operations,
    
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY APP_ELAPSED_MS) AS p50_latency_ms,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY APP_ELAPSED_MS) AS p95_latency_ms,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY APP_ELAPSED_MS) AS p99_latency_ms,
    AVG(APP_ELAPSED_MS) AS avg_latency_ms,
    MIN(APP_ELAPSED_MS) AS min_latency_ms,
    MAX(APP_ELAPSED_MS) AS max_latency_ms
    
FROM QUERY_EXECUTIONS qe
JOIN TEST_RESULTS tr ON qe.TEST_ID = tr.TEST_ID
WHERE tr.RUN_ID = :run_id
  AND qe.WARMUP = FALSE;
```

---

## 5. Schema DDL (Ensure Present in sql/schema/)

### results_tables.sql (verify presence)

```sql
-- =============================================================================
-- WAREHOUSE_POLL_SNAPSHOTS: Controller warehouse poller persistence
-- =============================================================================
CREATE OR ALTER TABLE WAREHOUSE_POLL_SNAPSHOTS (
    snapshot_id VARCHAR(36) NOT NULL,
    run_id VARCHAR(36) NOT NULL,
    
    -- Timing
    timestamp TIMESTAMP_NTZ NOT NULL,
    elapsed_seconds FLOAT,
    
    -- Warehouse Identity
    warehouse_name VARCHAR(500) NOT NULL,
    
    -- MCW Metrics (from SHOW WAREHOUSES)
    started_clusters INTEGER,
    running INTEGER,
    queued INTEGER,
    
    -- Scaling State
    min_cluster_count INTEGER,
    max_cluster_count INTEGER,
    scaling_policy VARCHAR(50),
    
    -- Raw SHOW WAREHOUSES row (for debugging)
    raw_result VARIANT,
    
    -- Audit
    created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- FIND_MAX_STEP_HISTORY: Step-by-step history for Find Max runs
-- =============================================================================
CREATE OR ALTER TABLE FIND_MAX_STEP_HISTORY (
    STEP_ID VARCHAR(36) NOT NULL,
    RUN_ID VARCHAR(36) NOT NULL,
    STEP_NUMBER INTEGER NOT NULL,
    
    -- Step Configuration
    TOTAL_TARGET_CONNECTIONS INTEGER NOT NULL,  -- Sum of all worker targets
    STEP_START_TIME TIMESTAMP_NTZ NOT NULL,
    STEP_END_TIME TIMESTAMP_NTZ,
    STEP_DURATION_SECONDS FLOAT,
    
    -- Aggregate Metrics (slowest-worker for P95/P99)
    TOTAL_QUERIES INTEGER,
    QPS FLOAT,
    P50_LATENCY_MS FLOAT,
    P95_LATENCY_MS FLOAT,              -- MAX across workers (conservative)
    P99_LATENCY_MS FLOAT,              -- MAX across workers (conservative)
    ERROR_COUNT INTEGER,
    ERROR_RATE FLOAT,
    
    -- Stability Evaluation
    QPS_VS_PRIOR_PCT FLOAT,            -- % change vs prior step
    P95_VS_BASELINE_PCT FLOAT,         -- % change vs baseline (step 1)
    QUEUE_DETECTED BOOLEAN DEFAULT FALSE,
    
    -- Outcome
    OUTCOME VARCHAR(50),               -- STABLE, DEGRADED, ERROR_THRESHOLD, QUEUE_DETECTED
    STOP_REASON TEXT,                  -- Populated if this step triggered stop
    
    -- Audit
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- =============================================================================
-- TEST_LOGS: Centralized worker logs
-- =============================================================================
CREATE TABLE IF NOT EXISTS TEST_LOGS (
    LOG_ID VARCHAR(36) NOT NULL,
    RUN_ID VARCHAR(36) NOT NULL,
    TEST_ID VARCHAR(36),              -- Nullable (orchestrator logs may omit)
    WORKER_ID VARCHAR(100),           -- Nullable
    
    LEVEL VARCHAR(20) NOT NULL,       -- INFO, WARNING, ERROR, CRITICAL
    MESSAGE TEXT NOT NULL,
    DETAILS VARIANT,                  -- Optional structured context
    
    TIMESTAMP TIMESTAMP_NTZ NOT NULL,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
```

### control_tables.sql (verify presence)

```sql
-- Add phase column to WORKER_METRICS_SNAPSHOTS (worker snapshots)
ALTER TABLE WORKER_METRICS_SNAPSHOTS ADD COLUMN IF NOT EXISTS PHASE VARCHAR(50);

-- Add find_max_state column to RUN_STATUS
ALTER TABLE RUN_STATUS ADD COLUMN IF NOT EXISTS FIND_MAX_STATE VARIANT;

-- Add worker_targets column to RUN_STATUS (fallback for missed events)
ALTER TABLE RUN_STATUS ADD COLUMN IF NOT EXISTS WORKER_TARGETS VARIANT;

-- Add next_sequence_id column to RUN_STATUS (atomic event ordering)
ALTER TABLE RUN_STATUS
    ADD COLUMN IF NOT EXISTS NEXT_SEQUENCE_ID INTEGER DEFAULT 1;
```

---

## 6. Poll Loop Lifecycle

The orchestrator poll loop runs as a **per-run background task** managed by
FastAPI's background task system.

### Lifecycle

```text
create_run() → Inserts RUN_STATUS, returns run_id
start_run() → Spawns workers, starts poll loop as background task
[poll loop runs every 1s]
stop_run() → STOP event + CANCELLING → drain wait → CANCELLED + END_TIME
```

### Implementation Pattern

```python
class OrchestratorService:
    def __init__(self):
        self._poll_tasks: dict[str, asyncio.Task] = {}
    
    async def start_run(self, run_id: str) -> None:
        # ... spawn workers ...
        
        # Start poll loop as background task
        task = asyncio.create_task(self._poll_loop(run_id))
        self._poll_tasks[run_id] = task
    
    async def stop_run(self, run_id: str) -> None:
        # Write STOP event (drain_timeout_seconds=120)
        await self._write_stop_event(run_id)
        await self._set_status(run_id, "CANCELLING")
        
        # Wait for graceful drain; local SIGTERM fallback after 10s
        await self._wait_for_workers(run_id, timeout=120, local_sigterm_after=10)
        
        # Finalize status
        await self._set_status(run_id, "CANCELLED")
        
        # Cancel poll loop
        if run_id in self._poll_tasks:
            self._poll_tasks[run_id].cancel()
            del self._poll_tasks[run_id]
    
    async def _poll_loop(self, run_id: str) -> None:
        while True:
            await asyncio.sleep(1.0)
            await self._check_heartbeats(run_id)
            await self._update_aggregates(run_id)
            await self._check_phase_transitions(run_id)
```

---

## 7. Worker Startup Sequence

### Worker Flow (run_worker.py)

```text
1. Parse args (run-id, worker-id, worker-group-id, worker-group-count)
2. Connect to Snowflake
3. Upsert WORKER_HEARTBEATS with STATUS='STARTING'
4. Load SCENARIO_CONFIG from RUN_STATUS
5. Compute initial TARGET_CONNECTIONS from config
6. Check RUN_STATUS.STATUS:
   - If RUNNING: start immediately with current RUN_STATUS.PHASE
   - If PREPARED: poll RUN_CONTROL_EVENTS for START (timeout: 120s)
7. Create child TEST_RESULTS row
8. Determine initial phase from RUN_STATUS.PHASE:
   - If WARMUP: participate in warmup
   - If MEASUREMENT: skip warmup (Snowflake already primed)
9. Begin workload
10. During run:
    - Write heartbeats every 1s
    - Write metrics snapshots every 1s
    - Poll for STOP, SET_PHASE, SET_WORKER_TARGET every 1s (ordered by SEQUENCE_ID)
    - Reconcile state from RUN_STATUS every 5s (fallback for missed events)
11. On STOP: Drain in-flight queries (max 120s), exit
12. On completion: Finalize child TEST_RESULTS, update heartbeat to COMPLETED, exit
```

### Worker Self-Termination Conditions

| Condition | Action |
|-----------|--------|
| STOP event received | Drain in-flight queries, exit |
| No START within 120s | Exit with error |
| No Snowflake connection for 60s | Exit (assume partition) |
| Parent RUN_STATUS is terminal | Exit |

---

## 8. Acceptance Test Criteria

### Local Multi-Worker Acceptance Test

Create `scripts/acceptance_test_multinode.py`:

```python
"""
Acceptance test for local multi-worker orchestration.

Tests:
1. Create a 2-worker run
2. Verify both workers start and register heartbeats
3. Verify aggregated metrics appear in WebSocket
4. Issue STOP and verify propagation within 10s
5. Verify final RUN_STATUS is CANCELLED
"""
```

### Test Checklist

- [ ] **Run Creation**: `RUN_STATUS` row created with `STATUS=PREPARED`
  (programmatic `/api/runs` flow; UI prepare shows PREPARED before Start)
- [ ] **Worker Registration**: Both workers appear in `WORKER_HEARTBEATS`
- [ ] **START Propagation**: Workers transition to `RUNNING` within 2s of START
- [ ] **Metrics Flow**: `WORKER_METRICS_SNAPSHOTS` populated by both workers
- [ ] **Aggregation**: WebSocket shows combined QPS from both workers
- [ ] **Phase Transition**: `WARMUP` → `MEASUREMENT` at correct time
- [ ] **STOP Propagation**: Workers exit within 10s of STOP event
- [ ] **Final Status**: `RUN_STATUS.STATUS` = `CANCELLED`
- [ ] **Cleanup**: No orphan processes after test

### Automated Checks

```bash
# Run acceptance test
uv run python scripts/acceptance_test_multinode.py

# Expected output:
# [PASS] Run created: RUN_STATUS.STATUS = PREPARED
# [PASS] Workers registered: 2/2 in WORKER_HEARTBEATS
# [PASS] START propagated: workers RUNNING in 3.2s
# [PASS] Metrics flowing: WORKER_METRICS_SNAPSHOTS has 2 workers
# [PASS] Aggregation correct: combined QPS = 1250 (sum of 625 + 625)
# [PASS] STOP propagated: workers exited in 4.8s
# [PASS] Final status: RUN_STATUS.STATUS = CANCELLED
# 
# All 7 checks passed.
```

---

## 9. Documentation Consistency

- Use a single unified payload structure across all docs.
- Remove legacy payload compatibility language.

---

## Reiterated Constraints

- No DDL is executed at runtime.
- Schema changes are rerunnable DDL in `sql/schema/`.
- Templates remain stored in `UNISTORE_BENCHMARK.TEST_RESULTS.TEST_TEMPLATES`
  with `CONFIG` as the authoritative payload.
- Snowflake is the authoritative results store.
- No schema versioning - complete migration, single payload structure.
