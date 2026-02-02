# Metrics Streaming and Phase Transitions - Debug Guide

This document explains the end-to-end flow of metrics from workers to the UI,
with focus on phase transitions and common debugging scenarios.

## Timeline: Test Execution Phases

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│ PHASE      │ PREPARING     │ WARMUP        │ MEASUREMENT   │ PROCESSING        │
├─────────────────────────────────────────────────────────────────────────────────┤
│ Duration   │ ~10-30s       │ warmup_seconds│ duration_s    │ ~5-30s            │
│ STATUS     │ STARTING      │ RUNNING       │ RUNNING       │ STOPPING/COMPLETED│
│ Workers    │ Initializing  │ Executing     │ Executing     │ Draining          │
│ QPS        │ 0             │ Ramping up    │ Steady state  │ Declining         │
│ UI Shows   │ (excluded)    │ Live metrics  │ Live metrics  │ Final metrics     │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Why QPS Shows 0 During PREPARING

**Root cause**: Workers don't execute queries until orchestrator sets `STATUS='RUNNING'`.

### Detailed Flow

1. **Orchestrator creates run** (`orchestrator.py:530-538`)
   ```python
   UPDATE RUN_STATUS SET STATUS = 'STARTING', PHASE = 'PREPARING'
   ```

2. **Workers spawn and initialize** (`run_worker.py:580`)
   - Worker reads `phase = 'PREPARING'` from RUN_STATUS
   - Sets `current_phase = "PREPARING"`
   - Starts `_collect_metrics()` task (metrics callback active)
   - **But**: Workers enter `_wait_for_start()` and block until `STATUS='RUNNING'`

3. **Metrics written with PHASE='PREPARING'** (`run_worker.py:676-683`)
   - Callback fires every 1s, writes to `WORKER_METRICS_SNAPSHOTS`
   - `phase='PREPARING'`, `QPS=0`, `total_operations=0`

4. **WebSocket excludes PREPARING** (`main.py:909-913`)
   ```python
   include_for_metrics = (
       not phase_value or phase_value in ("WARMUP", "MEASUREMENT")
   ) and status_upper != "DEAD"
   ```
   - PREPARING snapshots are **excluded** from aggregation
   - UI receives `QPS=0` because no qualifying snapshots exist

5. **Orchestrator transitions to RUNNING/WARMUP** (`orchestrator.py:1350-1389`)
   - When all workers report `STATUS='READY'` via heartbeats
   - Sets `STATUS='RUNNING', PHASE='WARMUP'`

6. **Workers start executing** (`run_worker.py:761-879`)
   - `_wait_for_start()` returns True
   - Workers spawn query execution tasks
   - Metrics snapshots now have `phase='WARMUP'` and `QPS > 0`
   - WebSocket includes these → UI shows live QPS

## Metrics Flow Diagram

```
┌──────────────────┐     ┌───────────────────────────────┐     ┌─────────────────┐
│     Worker       │     │   WORKER_METRICS_SNAPSHOTS    │     │   WebSocket     │
│  (run_worker.py) │     │      (Snowflake Table)        │     │   (main.py)     │
└────────┬─────────┘     └───────────────┬───────────────┘     └────────┬────────┘
         │                               │                              │
         │ _metrics_callback()           │                              │
         │ every 1s                      │                              │
         │                               │                              │
         ├──────────────────────────────►│                              │
         │ INSERT snapshot               │                              │
         │ (phase, qps, latency, etc.)   │                              │
         │                               │                              │
         │                               │  _aggregate_multi_worker_    │
         │                               │  metrics() every 1s          │
         │                               │◄─────────────────────────────┤
         │                               │  SELECT latest per worker    │
         │                               │  WHERE phase IN              │
         │                               │  ('WARMUP','MEASUREMENT')    │
         │                               │                              │
         │                               ├─────────────────────────────►│
         │                               │  Aggregated metrics          │
         │                               │                              │
         │                               │                              │
         │                               │                    send_json │
         │                               │                    to UI     │
         │                               │                              ▼
         │                               │                      ┌───────────────┐
         │                               │                      │   Dashboard   │
         │                               │                      │   (browser)   │
         │                               │                      └───────────────┘
```

**Live cache path (low-latency):**
- Workers also POST 1s snapshots to `/api/runs/{run_id}/metrics/live`.
- The WebSocket prefers the in-memory cache and falls back to Snowflake if empty.

## Key Code Locations

| Component | File | Line(s) | Description |
|-----------|------|---------|-------------|
| Phase initialization | `orchestrator.py` | 527-528 | Sets initial phase based on warmup_seconds |
| Worker phase tracking | `run_worker.py` | 580, 670-683 | current_phase variable and callback |
| Worker wait for start | `run_worker.py` | 693-753 | Blocks until STATUS='RUNNING' |
| Worker query execution | `run_worker.py` | 822-832, 878-879 | _spawn_one(), _scale_to() |
| Phase transition (WARMUP→MEASUREMENT) | `orchestrator.py` | 1480-1513 | Based on elapsed time |
| WebSocket aggregation | `main.py` | 750-927 | _aggregate_multi_worker_metrics() |
| Phase filter (critical) | `main.py` | 909-913 | Only WARMUP/MEASUREMENT included |
| WebSocket streaming | `main.py` | 1175-1307 | _stream_run_metrics() |

## Common Debug Scenarios

### Scenario 1: QPS = 0 During Entire Warmup

**Symptoms**: Dashboard shows QPS=0 for ~30s (PREPARING + WARMUP)

**Check**:
```sql
SELECT PHASE, COUNT(*), AVG(QPS), MIN(TIMESTAMP), MAX(TIMESTAMP)
FROM WORKER_METRICS_SNAPSHOTS
WHERE RUN_ID = '<run_id>'
GROUP BY PHASE
ORDER BY MIN(TIMESTAMP);
```

**Expected**: 
- PREPARING rows with QPS=0
- WARMUP rows with QPS>0 (if workers started executing)

**If WARMUP rows also have QPS=0**: Workers may not be reaching the query execution loop.
Check worker logs for errors after `_wait_for_start()` returns.

### Scenario 2: Metrics Appear Late (45s+ Delay)

**Symptoms**: Dashboard is blank, then suddenly shows metrics

**Root cause**: This was a known bug where only MEASUREMENT metrics were included.
Fixed by adding WARMUP to the filter at `main.py:912`.

**Verify fix is active**:
```python
# main.py:909-913 should include WARMUP:
include_for_metrics = (
    not phase_value or phase_value in ("WARMUP", "MEASUREMENT")
) and status_upper != "DEAD"
```

### Scenario 3: Phase Never Transitions to MEASUREMENT

**Symptoms**: Dashboard shows "WARMUP" forever

**Check orchestrator poll loop** (`orchestrator.py:1480-1485`):
- Requires `status == "RUNNING"`
- Requires `phase == "WARMUP"`
- Requires `elapsed_seconds >= warmup_seconds`

**Debug query**:
```sql
SELECT STATUS, PHASE, 
       TIMESTAMPDIFF('second', START_TIME, CURRENT_TIMESTAMP()) as elapsed,
       (SELECT warmup_seconds FROM ... ) as warmup_threshold
FROM RUN_STATUS WHERE RUN_ID = '<run_id>';
```

### Scenario 4: Workers Not Writing Snapshots

**Symptoms**: WORKER_METRICS_SNAPSHOTS has no/few rows

**Check**:
1. Worker logs for errors in `_metrics_callback`
2. Snowflake permissions for INSERT
3. Worker health status in WORKER_HEARTBEATS

```sql
SELECT WORKER_ID, STATUS, LAST_HEARTBEAT, ERROR_COUNT
FROM WORKER_HEARTBEATS
WHERE RUN_ID = '<run_id>';
```

## Phase State Machine

```
                              ┌─────────────────────────────────┐
                              │         Orchestrator            │
                              │   (backend/core/orchestrator.py)│
                              └─────────────┬───────────────────┘
                                            │
    ┌───────────────────────────────────────┼───────────────────────────────────────┐
    │                                       │                                       │
    ▼                                       ▼                                       ▼
┌───────────┐  workers ready  ┌───────────┐  elapsed>=warmup  ┌─────────────┐  elapsed>=total
│ PREPARING │ ───────────────►│  WARMUP   │ ─────────────────►│ MEASUREMENT │ ─────────────►
│           │                 │           │                   │             │
│ STATUS:   │                 │ STATUS:   │                   │ STATUS:     │
│ STARTING  │                 │ RUNNING   │                   │ RUNNING     │
└───────────┘                 └───────────┘                   └─────────────┘
     │                              │                               │
     │                              │                               │
     │ Worker actions:              │ Worker actions:               │ Worker actions:
     │ - Initialize pools           │ - Execute queries             │ - Execute queries
     │ - Connect to SF              │ - Write snapshots             │ - Write snapshots
     │ - Poll for START             │   (phase=WARMUP)              │   (phase=MEASUREMENT)
     │ - Write heartbeats           │ - Write heartbeats            │ - Metrics reset
     │   (status=READY)             │   (status=RUNNING)            │
     │                              │                               │
     │ UI sees:                     │ UI sees:                      │ UI sees:
     │ - Status: Preparing          │ - Live QPS (ramping)          │ - Live QPS (steady)
     │ - QPS: 0                     │ - Latency metrics             │ - Latency metrics
     │                              │ - Phase: "Warming up"         │ - Phase: "Running"
     └──────────────────────────────┴───────────────────────────────┘

                                            │
                              ┌─────────────┴───────────────────┐
                              │                                 │
                              ▼                                 ▼
                        ┌───────────┐                    ┌─────────────┐
                        │ STOPPING  │                    │ PROCESSING  │
                        │           │                    │             │
                        │ Workers   │                    │ Enrichment  │
                        │ draining  │ ──────────────────►│ running     │
                        └───────────┘                    └─────────────┘
```

## Environment Variables Affecting Timing

| Variable | Default | Effect |
|----------|---------|--------|
| `warmup_seconds` | Template config | Duration of WARMUP phase |
| `duration_seconds` | Template config | Duration of MEASUREMENT phase |
| Poll interval | 1s (hardcoded) | How often orchestrator checks phase transitions |
| Heartbeat interval | 1s (hardcoded) | How often workers report status |

## Related Documentation

- `docs/data-flow-and-lifecycle.md` - Authoritative lifecycle spec
- `docs/ui-architecture.md` - WebSocket and UI integration
- `docs/orchestrator-spec.md` - Orchestrator design details
- `docs/worker-implementation.md` - Worker internals
