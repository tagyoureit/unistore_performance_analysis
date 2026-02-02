# UI Architecture (Current)

This document describes the UI contract for test runs. All scaling modes
(AUTO, BOUNDED, FIXED) use the same orchestrator-based architecture.

## Rendering Model

- Server-rendered HTML via Jinja2.
- Alpine.js provides client-side state.
- Chart.js renders charts.
- HTMX handles partial navigation.

## Frontend Modules

- `backend/static/js/dashboard/` (live + history dashboards)
- `backend/static/js/history.js` (history list and compare)
- `backend/static/js/templates_manager.js` (templates list actions)
- `backend/static/css/app.css` (styling)

## Terminology

- **WORKER_ID**: Unique identifier for a worker process (string, e.g., `worker-0`)
- **TARGET_CONNECTIONS**: Number of concurrent queries a worker should maintain
- **Warmup**: Run-level phase to prime Snowflake compute (not per-worker)
- **Parent Run**: The orchestrator entry (test_id == run_id) that users
  interact with
- **Worker Run**: Individual worker entries (internal implementation detail)

## Architecture Note

All scaling modes use the unified orchestrator path:
- Creates `RUN_STATUS` entry for state management
- Supports real-time elapsed tracking
- FIXED mode simply means no auto-scaling (exact worker/connection counts)

The UI is "dumb" regarding topology. It consumes a unified data shape regardless
of worker count (n=1 or n=many).

## Dashboard Start Flow (UI)

1. Templates page prepares a run via `POST /api/tests/from-template/{id}`.
2. Browser navigates to `/dashboard/{test_id}`.
3. Dashboard shows template info in "Ready" state; no timers and no WebSocket.
4. User clicks "Start" → `POST /api/tests/{test_id}/start`.
5. Dashboard polls `/api/tests/{test_id}` until phase is RUNNING (measurement) or
   status is STOPPING.
6. WebSocket connects only after phase RUNNING or status STOPPING for live updates
   (pre-start and warmup use HTTP polling).

### Data Source

- **URL**: `/ws/test/{run_id}`
- **Source**: Controller aggregates data from Snowflake authoritative state.
- **Control Path**: WebSocket is read-only. Start/stop and target changes go
  through controller APIs to the orchestrator, which writes `RUN_CONTROL_EVENTS`.
- **Connection gating**: WebSocket connects only after Start and only when phase
  is RUNNING (measurement) or status is STOPPING. Pre-start and warmup use HTTP
  polling.
- **Frequency**:
  - **Status/Health**: 1 second updates (fast feedback for "is it alive?").
  - **Aggregate Metrics**: 1 second updates (driven by 1s snapshots; rollups
    are lightweight and use the latest snapshot).
  - **Warehouse/MCW**: 5 seconds updates from a dedicated poller.

### WebSocket Event Semantics

The controller emits a single event type on `/ws/test/{run_id}`:

```json
{
  "event": "RUN_UPDATE",
  "data": { ...payload... }
}
```

Rules:

- One `RUN_UPDATE` per second.
- Full snapshot payload; client replaces prior state (no diffs).
- WebSocket is UI-only; control commands are not sent over this channel.

## Display Contract

The WebSocket payload is a JSON object with these top-level keys:

```json
{
  "run": {
    "run_id": "uuid",
    "status": "RUNNING",
    "phase": "MEASUREMENT",
    "worker_count": 5,
    "elapsed_seconds": 120,
    "timing": {
      "elapsed_display_seconds": 120.0
    },
    "aggregate_metrics": {
      "qps": 5000,
      "p95_latency_ms": 45.2,
      "error_rate": 0.001
    }
  },
  "timing": {
    "elapsed_display_seconds": 120.0
  },
  "latency_aggregation_method": "slowest_worker_approximation",
  "workers": [
    {
      "worker_id": "worker-1",
      "health": "HEALTHY",
      "phase": "MEASUREMENT",
      "qps": 1000,
      "last_heartbeat_ago_s": 2
    },
    {
      "worker_id": "worker-2",
      "health": "DEAD",
      "phase": "WARMUP",
      "qps": 0,
      "last_heartbeat_ago_s": 65
    }
  ]
}
```

Worker freshness uses `health` (`HEALTHY`, `STALE`, `DEAD`). If we need to surface
worker lifecycle separately, use `status` for `STARTING`, `RUNNING`, `COMPLETED`.

**Note**: `TARGET_CONNECTIONS` is the orchestrator-assigned target for concurrent
queries. `ACTIVE_CONNECTIONS` is the actual count of currently executing queries.

**Latency aggregation**: When `latency_aggregation_method` is
`slowest_worker_approximation`, the controller reports P95/P99 as the max across
workers (conservative) and P50 as the average across workers. Single-worker runs
set `latency_aggregation_method` to `null` (not applicable—no aggregation needed).
See [The `latency_aggregation_method` Field](data-flow-and-lifecycle.md#the-latency_aggregation_method-field)
for details on sentinel values and UI handling.

## Payload Compatibility

- The controller emits a single unified payload structure.
- No legacy payload variants are supported.

## Chart.js Lifecycle (Live Dashboards)

- Do not store Chart.js instances on Alpine reactive state (circular refs).
- Keep instances on `canvas.__chart`, and use `Chart.getChart(canvas)` when available.
- Always destroy previous instances before re-creating (`safeDestroy` in
  `backend/static/js/dashboard.js` and `safeDestroyChart` in
  `backend/static/js/compare_detail.js`).
- `initCharts()` in `backend/static/js/dashboard.js` is idempotent; it skips
  re-creation when a chart already exists.
- The warehouse/MCW chart is initialized in `loadWarehouseTimeseries()` and
  should not be destroyed by later `initCharts()` calls.
- Live charts disable animation (`animation.duration = 0`) to keep per-second
  updates smooth.

## Alpine.js Data Access (Debugging)

For debugging, Alpine component data can be accessed via the data stack:

```javascript
const alpineData = element._x_dataStack[0];
```

## Component Responsibility

### Header / Status Bar
- Shows parent run status (`RUNNING`, `COMPLETED`).
- Shows phase (`WARMUP`, `MEASUREMENT`).
- Shows global elapsed time (from parent start time).

### Test Progress (Phase Bubbles)

The dashboard displays a pipeline of phase bubbles showing test progression:

```text
✓ Preparing → ✓ Warmup → ● Running → ○ Processing → ○ Completed
```

#### Phase Display States

| State | Visual | CSS Class |
|-------|--------|-----------|
| Completed | Green checkmark (✓) | `phase-badge--completed` |
| Current | Highlighted + spinner | `phase-badge--active phase-{name}` |
| Pending | Muted/gray | `phase-badge--pending` |

#### Backend to Frontend Phase Mapping

The backend uses different phase names than the frontend displays. The frontend
normalizes phases via `normalizePhase()`:

| Backend Phase | Frontend Display | Notes |
|---------------|------------------|-------|
| `PREPARING` | Preparing | Initial setup |
| `WARMUP` | Warmup | Pre-measurement warm-up |
| `MEASUREMENT` | Running | **Key mapping** - backend sends MEASUREMENT |
| `PROCESSING` | Processing | Post-test enrichment |
| `COMPLETED` | Completed | Terminal state |

#### Phase Derivation Logic

The API derives the phase from multiple sources for worker-based runs:

1. **Elapsed Time Calculation** (primary, when status is `RUNNING` or `STOPPING`):
   - `elapsed < warmup_seconds` → `WARMUP`
   - `elapsed < warmup_seconds + duration_seconds` → `RUNNING` (displayed as "Running")
   - `elapsed >= warmup_seconds + duration_seconds` → `PROCESSING`

2. **Run Status Table**: When elapsed-based derivation doesn't apply, uses
   `RUN_STATUS.PHASE` (may return `MEASUREMENT` which frontend maps to "Running")

3. **In-Memory Registry**: For tests still tracked in memory

4. **Derived from Status + Enrichment**: When `status=COMPLETED` and
   `enrichment_status=PENDING`, phase is set to `PROCESSING`

The elapsed-based derivation ensures that `PROCESSING` displays correctly when
the test enters the post-measurement phase (even while status is still `STOPPING`
as workers finish up).

#### Visibility Control

The Test Progress section only appears after the test has started:

```javascript
// phase.js
hasTestStarted() {
  const status = (this.status || "").toString().toUpperCase();
  const notStartedStatuses = ["PENDING", "PREPARED", ""];
  return !notStartedStatuses.includes(status);
}
```

This prevents showing phase bubbles and the timer before the user clicks "Start".

#### Total Time Counter

The total elapsed timer is a **single, continuous counter** that starts the
moment the user clicks "Start" and runs through **PREPARING → WARMUP → RUNNING →
PROCESSING → COMPLETED**. The timer remains visible through processing and only
stops once the test is fully completed:

- Timer stops automatically at COMPLETED
- Final elapsed time is preserved (not reset to configured duration)
- If API provides `timing.elapsed_display_seconds`, that value is authoritative
- During PROCESSING, keep the final elapsed value visible (do not reset)

#### Phase Timers (Warmup + Running)

In addition to total time, the UI exposes **phase timers**:

- **Warmup**: timer runs for the configured warmup window
- **Running/Measurement**: timer runs for the configured duration when the
  expected running time is known (fixed workers / fixed QPS modes)

#### Elapsed Time Source of Truth (Critical)

Elapsed time MUST be calculated in **Snowflake SQL** using `TIMESTAMPDIFF`, NOT in
Python using datetime arithmetic. This is because Snowflake returns naive datetimes
in the **session timezone** (often Pacific time), while Python's `datetime.now(UTC)`
returns UTC. Calculating elapsed in Python causes an 8-hour discrepancy (~28800s).

**Correct (all backends must use this pattern):**

```sql
SELECT TIMESTAMPDIFF(SECOND, START_TIME, CURRENT_TIMESTAMP()) AS ELAPSED_SECONDS
FROM RUN_STATUS WHERE RUN_ID = ?
```

**WRONG (causes 8-hour elapsed time bug):**

```python
# DO NOT DO THIS - timezone mismatch!
start_time = run_status.get("start_time")
now = datetime.now(UTC)
elapsed_seconds = (now - start_time).total_seconds()  # BUG: ~28800s off
```

**Data sources for elapsed time (in priority order):**

1. `timing.elapsed_display_seconds` from API (authoritative for `/api/tests/{id}`)
2. `timing.elapsed_display_seconds` from WebSocket payload (authoritative in live)
3. `elapsed_seconds` from WebSocket payload (legacy, must match TIMESTAMPDIFF)
4. `run.elapsed_seconds` from `run` snapshot in WebSocket
5. Frontend timer (only for active tests, synced with server values)

See `project-plan.md` section **2.19** for the fix implementation.

### Aggregate Metrics Cards
- QPS (sum of all workers).
- Latency (P50 avg across workers, P95/P99 slowest-worker).
- Error Rate (global average).

### Worker Grid (New)
- Visual grid of N boxes.
- Color-coded by health (Green=Healthy, Yellow=Stale, Red=Dead).
- Tooltip shows per-worker QPS and errors.
- Clicking a worker expands inline per-worker details (KPIs and tables/charts) on
  the parent dashboard.

### MCW Active Clusters (Real-Time)
- Sourced from the controller's warehouse poller (dedicated Snowflake pool).
- Sampled every ~5 seconds via `SHOW WAREHOUSES`.
- Persist poller samples to `WAREHOUSE_POLL_SNAPSHOTS` (append-only).
- Use the poller result directly (max/last), never sum across workers.
- Workers do not query MCW/warehouse state.

**Live Chart Update Flow**:

1. Orchestrator's `_poll_warehouse()` runs every 5s during an active run.
2. Results are inserted into `WAREHOUSE_POLL_SNAPSHOTS` with `started_clusters`,
   `running`, and `queued` values.
3. WebSocket handler queries latest warehouse poll snapshot and includes
   `started_clusters` in the `RUN_UPDATE` payload.
4. Frontend `dashboard.js` updates the MCW live chart when `started_clusters > 0`:

```javascript
if (allowCharts && this.metrics.started_clusters > 0) {
  mcwLiveChart.data.labels.push(timestamp);
  mcwLiveChart.data.datasets[0].data.push(this.metrics.started_clusters);
  mcwLiveChart.update();
}
```

The chart auto-scrolls (removes oldest data points when > 60 samples) to maintain
a ~5-minute rolling window.

### MCW Clusters + Queue (History)
- History chart uses `/api/tests/{test_id}/warehouse-timeseries`.
- Active clusters prefer `V_WAREHOUSE_TIMESERIES.ACTIVE_CLUSTERS`.
- When query history lags, fall back to poller snapshots.
- Queue metrics come from `V_WAREHOUSE_TIMESERIES` (QUERY_EXECUTIONS).

### Find Max (Step-Load) Live Panel
- Controller-owned step controller drives all worker changes.
- UI renders a single aggregated step state for the run.
- Workers report metrics only; they never decide step transitions.
- Surface: current step, target workers, baseline vs current P95/P99, error
  rate, and stop reason.

### Worker Health Timeline (Minimal)
- A compact timeline showing counts of HEALTHY/STALE/DEAD workers over time.
- Default collapsed; expand only on demand or when non-healthy counts appear.

## Inline Worker Details (Phase 2)

Clicking a worker in the grid expands per-worker KPIs and tables/charts inline on
the parent page. No child dashboard routes are created in Phase 2.

TODO: Consider a child drilldown page after the multi-worker UI stabilizes.

## Latency vs. Freshness

- **Status Updates**: Pushed every 1s. Users need immediate feedback on state
  changes (e.g., stopping).
- **Metrics Updates**: Pushed every 1s using the latest snapshots. Heavy
  persistence remains adaptive and does not run every tick.

## Dashboard HTTP Polling

The live dashboard uses HTTP polling as a fallback/supplement to WebSocket updates
for multi-worker runs. Polling is **phase-gated** to avoid unnecessary API load
during inactive phases.

Before the run starts, the dashboard is template-only ("Ready") and does not
open WebSocket. After the prepare call returns `test_id`, polling begins and
continues until the phase reaches RUNNING (measurement) or status STOPPING, at
which point WebSocket connects for live updates.

### Polling Intervals

| Poller | Interval | Endpoint | Purpose |
|--------|----------|----------|---------|
| Metrics | 1s | `/api/tests/{id}/metrics` | Per-second metric snapshots |
| Logs | 1s | `/api/tests/{id}/logs` | Live log stream |
| Test Info | 3s | `/api/tests/{id}` | Status/phase/timing |
| Warehouse Details | 3s | `/api/tests/{id}/warehouse-details` | MCW config |

#### Warehouse Details Response

The `/api/tests/{id}/warehouse-details` endpoint returns current warehouse
configuration via `SHOW WAREHOUSES`:

```json
{
  "test_id": "...",
  "warehouse": {
    "name": "WH_NAME",
    "state": "STARTED",
    "type": "STANDARD",
    "size": "MEDIUM",
    "min_cluster_count": 1,
    "max_cluster_count": 4,
    "started_clusters": 2,
    "running": 5,
    "queued": 0,
    "auto_suspend": 300,
    "auto_resume": true,
    "scaling_policy": "STANDARD",
    "enable_query_acceleration": false,
    "query_acceleration_max_scale_factor": 0,
    "resource_constraint": null,
    "captured_at": "2026-01-27T00:30:00Z"
  }
}
```

Key MCW fields for dashboard display:
- `started_clusters`: Number of clusters currently started
- `running`: Number of queries currently executing
- `queued`: Number of queries waiting for resources (triggers Find Max stop)
- `min_cluster_count` / `max_cluster_count`: MCW scaling bounds

### Phase Gating

Polling is **phase-gated** to reduce unnecessary API traffic:

| Phase | Polling Active | Rationale |
|-------|----------------|-----------|
| PREPARING | Test Info only | Detect RUNNING/STOPPING after prepare call |
| WARMUP | Yes | Test is running, real-time feedback needed |
| MEASUREMENT | Yes | Test is running, real-time feedback needed |
| COOLDOWN | No | Test is draining, updates not critical |
| PROCESSING | No | Post-processing, no new metrics |
| Terminal states | No | Test complete, historical view only |

This prevents:
- Excessive metrics/log polling before a test starts
- 404 errors from `/warehouse-details` before warehouse data exists
- Unnecessary load during post-processing

### Implementation

Phase gating is implemented in `backend/static/js/dashboard/data-loading.js`:

```javascript
const activeMetricPhases = ["WARMUP", "MEASUREMENT"];
const shouldUseWebSocket =
  phaseUpper === "RUNNING" || statusUpper === "STOPPING";
const shouldPollMetrics =
  !shouldUseWebSocket && activeMetricPhases.includes(phaseUpper);
const shouldPollTestInfo = !shouldUseWebSocket;
```

All runs now use the unified orchestrator model, so polling is enabled for any
run in an active phase (no `isMultiWorker` distinction).

When a test transitions out of an active phase, all polling loops are stopped
automatically via `stopMultiNodeMetricsPolling()`, `stopMultiNodeLogPolling()`,
and `stopMultiNodeTestInfoPolling()`.

### API Performance Optimizations

The `/api/tests/{id}` endpoint aggregates data from multiple Snowflake tables to
provide comprehensive test information. To minimize latency, queries are
**parallelized using `asyncio.gather`** instead of running sequentially.

#### Parallel Query Execution

The following queries run concurrently after the initial TEST_RESULTS lookup:

| Query | Condition | Purpose |
|-------|-----------|---------|
| Worker find_max_results | Parent runs | Per-worker find_max state |
| Error rates | Terminal states | Per-kind error rates from QUERY_EXECUTIONS |
| SF execution latency | Always | Snowflake execution time percentiles |
| App latency summary | Parent runs | End-to-end latency from child workers |
| Warehouse metrics | Always | Queue times and cluster breakdown |
| Cluster breakdown | Always | Per-cluster query distribution |
| Postgres stats | Postgres tables | Connection pool statistics |

**Performance Impact**: Sequential execution of 6-8 Snowflake queries accumulated
~500ms+ per query due to warehouse resume and compilation time, resulting in
7+ second response times. Parallel execution reduces this to 0.6-1.1 seconds
for warm warehouses (limited by the slowest individual query), with ~2-4 seconds
on cold warehouse resume.

| Scenario | Before (Sequential) | After (Parallel) |
|----------|---------------------|------------------|
| Cold warehouse | 7+ seconds | 2-4 seconds |
| Warm warehouse | 4-5 seconds | 0.6-1.1 seconds |

#### Two-Phase Parallel Execution

The `get_test()` endpoint uses a two-phase parallelization strategy:

#### Phase 1: Initial Parallel Fetch

The main TEST_RESULTS query, RUN_STATUS, and enrichment status are fetched
concurrently. Previously these were sequential, adding ~1.5-3s of round-trip time.

```python
# backend/api/routes/test_results.py - Phase 1
initial_results = await asyncio.gather(
    pool.execute_query(query, params=[test_id]),      # TEST_RESULTS
    _fetch_run_status(pool, str(test_id)),            # RUN_STATUS
    _aggregate_parent_enrichment_status(...),         # Enrichment
    return_exceptions=True,
)
```

Additionally, `update_parent_run_aggregate()` is now fire-and-forget via
`asyncio.create_task()` to avoid blocking the API response.

#### Phase 2: Supplementary Parallel Fetch

```python
# backend/api/routes/test_results.py - Phase 2
parallel_tasks = [
    ("sf_latency", _fetch_sf_execution_latency_summary_for_run(...)),
    ("warehouse_metrics", _fetch_warehouse_metrics(...)),
    ("cluster_breakdown", _fetch_cluster_breakdown(...)),
    # ... additional tasks
]

# Execute all queries concurrently
results = await asyncio.gather(
    *[coro for _, coro in parallel_tasks],
    return_exceptions=True
)
```

Failures are handled gracefully with `return_exceptions=True` - each failed query
populates default values without blocking other results.

## History and Comparison

### Routes

| Route | Purpose |
|-------|---------|
| `/history` | Browse, search, and filter completed test results |
| `/history/compare?ids=<id1>,<id2>` | Deep comparison view for exactly 2 tests |
| `/comparison` | **Deprecated** - redirects to `/history` |
| `/dashboard/history/{test_id}` | Read-only history view for a completed test |
| `/dashboard/history/{test_id}/data` | Query execution drilldown |

### Comparison API

The comparison view uses these API endpoints:

| Endpoint | Purpose |
|----------|---------|
| `GET /api/tests/{test_id}` | Test summary, config, and final metrics |
| `GET /api/tests/{test_id}/metrics` | Time-series snapshots for charts |

### Multi-Worker Aggregation in Comparison

When comparing runs, the `/api/tests/{test_id}/metrics` endpoint automatically
handles multi-worker (parent) runs:

1. **Detection**: If `test_id == run_id`, the test is a parent run.
2. **Data Source**: Fetches from `WORKER_METRICS_SNAPSHOTS` instead of
   `METRICS_SNAPSHOTS`.
3. **Aggregation Method**:
   - **QPS**: SUM across all workers (total throughput)
   - **P50 Latency**: AVG across workers
   - **P95/P99 Latency**: MAX across workers (worst-case approximation)
   - **Active Connections**: SUM across workers
4. **Phase Filter**: Only `MEASUREMENT` phase data is included.
5. **Bucketing**: Worker snapshots are aggregated into whole-second buckets.

This ensures comparisons between single-worker and multi-worker runs use
consistent aggregation semantics.

### Current Comparison Features

The deep comparison view (`/history/compare`) provides:

- **Side-by-side summary cards**: Template name, table type, warehouse,
  concurrency, duration, final QPS, and P95 latency for each test.
- **Overlaid time-series charts**:
  - Throughput (QPS) over elapsed time
  - Latency (P50/P95/P99) over elapsed time
  - Concurrency (Snowflake running queries or Postgres in-flight)
- **Visual distinction**: Primary test in color, secondary test in gray/dashed.
- **CSV export**: Download comparison data for offline analysis.

### Comparison Limitations (Current)

- Limited to exactly 2 tests.
- No detailed breakdown by query type (point lookup, range scan, etc.).
- No error rate comparison chart.
- No Find Max step-by-step comparison.
- No worker-level breakdown for multi-worker runs.

---

## Enhanced Comparison (Proposed)

### Overview

Extend comparison capabilities beyond the current overlay-chart view to support
deeper analysis and more than 2 runs.

### Proposed Routes

| Route | Purpose |
|-------|---------|
| `/history/compare?ids=<id1>,<id2>` | Quick comparison (existing) |
| `/history/compare/detail?ids=<id1>,<id2>[...]` | In-depth comparison (new) |

### Chart Time Alignment Strategy

When comparing runs of different lengths, charts must handle alignment carefully
to enable meaningful visual comparison.

#### X-Axis: Elapsed Seconds

- Use elapsed seconds (0-based), not wall-clock timestamps.
- All runs start at X=0 regardless of actual start time.
- Chart X-axis range = `max(duration_seconds)` across all compared runs.

#### Handling Different Run Lengths

```text
Run A (300s): ████████████████████████████████████████░░░░░░░░░░
Run B (500s): ████████████████████████████████████████████████████████████████
Run C (180s): ████████████████████████░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░
              ^                       ^                        ^
              0s                    180s                     300s              500s
```text

- Shorter runs end where their data ends (no extrapolation).
- Longer runs continue with only their series visible past shorter runs.
- Visual end markers show where each run completed.

#### Visual Indicators

| Indicator | Purpose |
|-----------|---------|
| Vertical marker | Subtle line at each run's end time |
| Series fade | Dim/fade series after run completes |
| Duration in legend | Show run duration next to series label |
| Phase bands | Optional vertical bands for warmup/measurement boundaries |

#### Latency Chart Considerations

- **Y-Axis Scale**: Consistent scale across all series (auto-range to max value).
- **Outlier Handling**: Log-scale toggle when P99 varies significantly (e.g., 10x).
- **Missing Data**: Show discontinuity for gaps (paused runs) rather than
  interpolating.

### Managing Chart Complexity

With multi-run comparison, charts can become cluttered quickly. For example,
comparing 5 runs on a latency chart with P50/P95/P99 produces 15 series. Several
strategies can mitigate this:

#### Strategy 1: Small Multiples (Recommended for Detail View)

Show each run in its own small chart, vertically stacked with aligned X-axes:

```text
┌─────────────────────────────────────┐
│ Run A: Latency (P50/P95/P99)        │
│ ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  │
├─────────────────────────────────────┤
│ Run B: Latency (P50/P95/P99)        │
│ ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  │
├─────────────────────────────────────┤
│ Run C: Latency (P50/P95/P99)        │
│ ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  │
└─────────────────────────────────────┘
         0s        100s       200s       300s
```text

**Pros**: Each run is fully readable; easy vertical comparison at any time point.
**Cons**: Takes more vertical space; harder to see exact overlay differences.

#### Strategy 2: Single-Metric Focus (Recommended for Quick Compare)

Default to showing only one metric (e.g., P95) across all runs, with a dropdown
to switch metrics:

```text
┌─────────────────────────────────────────────────┐
│  Metric: [P95 ▼]                                │
│                                                 │
│  ─── Run A    ─── Run B    ─── Run C           │
│  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~│
└─────────────────────────────────────────────────┘
```

**Pros**: Clean overlay comparison; easy to see which run is faster.
**Cons**: Loses P50/P99 context; requires switching to see other percentiles.

#### Strategy 3: Primary + Ghost (Hybrid Approach)

Select one "primary" run shown in full detail (all percentiles), with other runs
shown as muted/ghost P95-only lines for context:

```text
┌─────────────────────────────────────────────────┐
│  Primary: [Run A ▼]                             │
│                                                 │
│  ━━━ Run A P50   ━━━ Run A P95   ━━━ Run A P99 │
│  --- Run B P95 (ghost)                          │
│  --- Run C P95 (ghost)                          │
│  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~│
└─────────────────────────────────────────────────┘
```

**Pros**: Full detail for focus run; context from others without clutter.
**Cons**: Asymmetric; must switch primary to see other runs in detail.

#### Strategy 4: Toggleable Series with Smart Defaults

Interactive legend where clicking toggles series visibility. Default state shows
only P95 for all runs; user can expand to show P50/P99:

| Default State | User Action | Result |
|---------------|-------------|--------|
| P95 only (all runs) | Click "Show P50" | Add P50 for all runs |
| P95 only (all runs) | Click Run A legend | Toggle all Run A series |
| All percentiles | Click "Simplify" | Return to P95-only |

#### Strategy 5: Color Encoding (For Overlays)

Use color hue for runs and line style for percentiles:

| Run | P50 | P95 | P99 |
|-----|-----|-----|-----|
| Run A (Blue) | Dotted | Solid | Dashed |
| Run B (Orange) | Dotted | Solid | Dashed |
| Run C (Green) | Dotted | Solid | Dashed |

This keeps the same line style semantic across runs (P95 always solid).

#### Recommended Implementation

| View | Strategy | Rationale |
|------|----------|-----------|
| Quick Compare | Strategy 2 (single-metric) | Fast visual compare |
| Detail Compare | Strategy 1 (small multiples) | Full analysis |
| Hover behavior | Strategy 3 (highlight run) | Focused context |

**Interactive Controls**:

```text
┌────────────────────────────────────────────────────────┐
│ View: [Overlay ▼] [Small Multiples]                    │
│                                                        │
│ Metric: [P95 ▼]   □ Show P50   □ Show P99             │
│                                                        │
│ Runs:  ☑ Run A   ☑ Run B   ☑ Run C   □ Run D         │
└────────────────────────────────────────────────────────┘
```

#### Implementation Notes

```javascript
// Pseudocode for chart data preparation
function prepareComparisonData(runs) {
  const maxDuration = Math.max(...runs.map(r => r.duration_seconds));

  return runs.map((run, idx) => ({
    label: `${run.test_name} (${run.duration_seconds}s)`,
    color: PALETTE[idx],
    data: run.metrics.map(m => ({
      x: m.elapsed_seconds,  // 0-based
      y: m.value
    })),
    endMarker: run.duration_seconds,
    fadeAfter: run.duration_seconds
  }));
}
```

### Enhanced Comparison Features

#### 1. Multi-Run Support (2-5 runs)

- Allow comparison of up to 5 runs simultaneously.
- Color-coded series with consistent palette across all charts.
- Legend with run identifiers and key config differences.

#### 2. Configuration Diff Table

Side-by-side table highlighting configuration differences:

| Setting | Run A | Run B | Run C |
|---------|-------|-------|-------|
| Table Type | HYBRID | HYBRID | STANDARD |
| Warehouse Size | MEDIUM | LARGE | MEDIUM |
| Concurrency | 50 | 50 | 100 |
| Duration | 300s | 300s | 300s |
| Workload | 80/20 R/W | 80/20 R/W | 50/50 R/W |

Highlight cells where values differ.

#### 3. Detailed Metrics Comparison

Tabular comparison of final metrics:

| Metric | Run A | Run B | Run C | Best |
|--------|-------|-------|-------|------|
| QPS | 5,000 | 7,200 | 4,800 | Run B |
| P50 Latency | 12ms | 8ms | 15ms | Run B |
| P95 Latency | 45ms | 32ms | 52ms | Run B |
| P99 Latency | 89ms | 61ms | 98ms | Run B |
| Error Rate | 0.1% | 0.05% | 0.2% | Run B |
| Total Ops | 1.5M | 2.1M | 1.4M | Run B |

#### 4. Per-Query-Type Breakdown

Compare latencies by operation type:

| Query Type | Run A P95 | Run B P95 | Delta |
|------------|-----------|-----------|-------|
| Point Lookup | 25ms | 18ms | -28% |
| Range Scan | 120ms | 95ms | -21% |
| Insert | 35ms | 28ms | -20% |
| Update | 42ms | 33ms | -21% |

#### 5. Error Analysis Comparison

Compare error patterns across runs:

| Error Category | Run A | Run B | Run C |
|----------------|-------|-------|-------|
| Lock Timeout | 45 | 12 | 89 |
| Connection Error | 0 | 0 | 3 |
| Query Timeout | 2 | 0 | 5 |

#### 6. Find Max Comparison (for step-load runs)

Compare Find Max progression:

| Concurrency | Run A QPS | Run A P95 | Run B QPS | Run B P95 |
|-------------|-----------|-----------|-----------|-----------|
| 10 | 1,000 | 15ms | 1,200 | 12ms |
| 20 | 1,950 | 18ms | 2,350 | 14ms |
| 40 | 3.8k | 25ms | 4.6k | 19ms |
| 80 | 7.2k | 45ms | 8.8k | 32ms |
| 160 | 12k | 89ms | 15.2k | 61ms |

#### 7. Resource Utilization Comparison

Compare warehouse and client-side resource metrics:

**Warehouse Metrics (per-run table)**:

| Metric | Run A | Run B | Run C |
|--------|-------|-------|-------|
| Clusters Used | 3 | 4 | 2 |
| Queued (Overload) | 1.2s | 0.8s | 2.5s |
| Queued (Provisioning) | 0.3s | 0.1s | 0.5s |
| Queries w/ Overload Queue | 45 | 12 | 89 |
| Read Cache Hit % | 92% | 95% | 88% |

**Time-series overlays**:

- MCW active clusters over time (from `V_WAREHOUSE_TIMESERIES`)
- Queue depth (avg queued ms per query per second)
- App overhead (derived from `APP_ELAPSED_MS - SF_TOTAL_ELAPSED_MS`)

**Client Resources** (averaged across workers for multi-worker runs):

| Resource | Run A | Run B | Run C |
|----------|-------|-------|-------|
| Host CPU % | 45% | 52% | 38% |
| Process CPU % | 42% | 48% | 35% |
| Host Memory MB | 2,100 | 2,400 | 1,800 |
| Process Memory MB | 1,200 | 1,350 | 1,050 |

#### 8. Per-Cluster Breakdown Comparison

Compare MCW cluster distribution across runs:

| Cluster | Run A Queries | Run A P95 | Run B Queries | Run B P95 |
|---------|---------------|-----------|---------------|-----------|
| 1 | 15,000 | 28ms | 18,000 | 22ms |
| 2 | 14,800 | 30ms | 17,500 | 24ms |
| 3 | 15,200 | 27ms | 18,200 | 21ms |
| 4 | - | - | 17,800 | 23ms |

Includes query mix breakdown (Point Lookup, Range Scan, Insert, Update).

#### 9. Worker Distribution (Multi-Worker Runs)

For multi-worker parent runs, show worker-level metrics:

| Worker | Run A QPS | Run A P95 | Run B QPS | Run B P95 |
|--------|-----------|-----------|-----------|-----------|
| worker-0 | 1,000 | 45ms | 1,450 | 32ms |
| worker-1 | 1,020 | 43ms | 1,430 | 31ms |
| worker-2 | 980 | 47ms | 1,420 | 33ms |
| **Total** | **3,000** | **47ms** | **4,300** | **33ms** |

### API Enhancements Required

| Endpoint | Change |
|----------|--------|
| `GET /api/tests/{test_id}` | Add `workers` summary |
| `GET /api/tests/{test_id}/metrics` | Add `by_query_type` breakdown |
| `GET /api/tests/{test_id}/errors` | New error categorization |
| `GET /api/tests/{test_id}/warehouse-timeseries` | Reuse for comparison |
| `GET /api/tests/{test_id}/overhead-timeseries` | Reuse for comparison |
| `GET /api/compare?ids=...` | Return diff payload |

### Data Sources

The enhanced comparison leverages existing data already collected by the history
dashboard:

| Feature | Data Source |
|---------|-------------|
| Throughput/Latency charts | `METRICS_SNAPSHOTS` or `WORKER_METRICS_SNAPSHOTS` |
| Query type breakdown | `METRICS_SNAPSHOTS.BY_KIND_*` columns |
| Warehouse metrics | `V_WAREHOUSE_TIMESERIES`, post-processing enrichment |
| Cluster breakdown | `cluster_breakdown` from enrichment |
| Step history (Find Max) | `RUN_STATUS.STEP_HISTORY` JSON column |
| Worker metrics | `WORKER_METRICS_SNAPSHOTS` |
| Resource usage | `METRICS_SNAPSHOTS.{CPU,MEMORY}_*` columns |
| App overhead | Computed from `APP_ELAPSED_MS - SF_TOTAL_ELAPSED_MS` |

### Implementation Priority

1. **Phase 1**: Multi-run support (2-5), config diff table, metrics table
2. **Phase 2**: Per-query-type breakdown, error analysis
3. **Phase 3**: Find Max comparison, per-cluster breakdown, worker distribution
4. **Phase 4**: Resource utilization charts, app overhead comparison

---

## Toast Notifications

The UI uses toast notifications to alert users of important events. Toasts are
implemented in `backend/static/js/toast.js` and exposed via `window.toast`.

### Toast API

```javascript
window.toast.success("Operation completed");
window.toast.error("Something went wrong");
window.toast.warning("Check configuration");
window.toast.info("Processing started");
```

### Test Cancellation/Failure Notifications

When a test is cancelled or fails, the dashboard displays an error-level toast
with the cancellation reason. This ensures users are immediately notified of
abnormal test termination.

**Sources of cancellation reasons:**
- `"User requested cancellation"` - Manual stop via UI
- `"Guardrail triggered: {detail}"` - CPU/memory guardrail exceeded
- `"Worker failure: N worker(s) stopped responding"` - Dead worker detection

**Data flow:**
1. Orchestrator writes `CANCELLATION_REASON` to `RUN_STATUS` Hybrid Table
2. Backend includes `cancellation_reason` in:
   - WebSocket `RUN_UPDATE` payload (real-time via `websocket.js`)
   - HTTP GET `/api/tests/{test_id}` response (polling via `data-loading.js`)
3. Frontend displays toast via `phase.js.setStatusIfAllowed()` when status
   transitions to `FAILED` or `CANCELLED`

**Deduplication:** A `_shownCancellationToast` flag prevents showing the same
toast multiple times (e.g., when both WebSocket and HTTP polling report the
same status change).

---

## Reiterated Constraints

- No DDL is executed at runtime.
- Schema changes are rerunnable DDL in `sql/schema/`.
- Templates remain stored in `UNISTORE_BENCHMARK.TEST_RESULTS.TEST_TEMPLATES`
  with `CONFIG` as the authoritative payload for runs.
