# FIND_MAX Transition Lag Postmortem

This document captures a hard-to-debug issue where the FIND_MAX dashboard
countdown reached `0s` but the next phase/concurrency did not appear
immediately in the UI.

## Executive Summary

- **User-visible symptom**: "Next change in" reached `0s` and stayed there for
  several seconds before the next step showed up.
- **Observed delay window**: initially ~9-12s, then ~6-9s, then ~4-5s.
- **Final state**: near-instant updates after combined backend + frontend fixes.
- **Why it was hard**: this was not a single bug. Delay came from multiple
  timing paths across orchestrator loop cadence, DB round-trips, worker ramp
  behavior, and frontend countdown assumptions.

## Symptoms and Log Signatures

Common signatures seen during debugging:

- Orchestrator completion/next-step logs were late relative to UI countdown:
  - `FIND_MAX step N complete: ...`
  - `FIND_MAX step N (next): persisting early_state ...`
- WebSocket showed DB and LIVE state mismatch (or missing LIVE):
  - `[WS] find_max sources: DB(... ) LIVE(step=None,target=None,end_ms=None)`
- Poll loop showed multi-second iterations:
  - `FIND_MAX poll loop slow iteration ...: 2-4.5s`
- Worker target updates occurred during visible UI lag window:
  - `[WorkerPool] Scale up: running=X < target=Y ...`

## Root Causes (Multi-factor)

### 1) Step boundary timing used stale timestamps

Some step decisions were using a timestamp captured earlier in the loop while
other state fields used fresh time, causing the countdown to hit 0 before
completion logic ran.

### 2) Orchestrator loop did heavy awaited I/O in hot path

The FIND_MAX poll loop included multiple DB operations that could block step
transitions (state persistence, rollups, aggregate queries, history inserts).

### 3) WebSocket/controller state source lag

UI often depended on DB-reflected state rather than immediate controller state,
especially when worker LIVE controller payloads were absent.

### 4) State ordering/race regression in live controller cache

A newer transition snapshot could be overwritten by an older same-step running
snapshot, briefly regressing countdown state.

### 5) FIND_MAX target application perceived as transition delay

Worker target changes were visibly tied to scale-up activity. Even when
countdown state arrived quickly, ramp/update timing could make transition feel
late.

### 6) Frontend countdown assumptions

The UI countdown depended on fields that were not always present in early state
and used browser `Date.now()` directly, which can drift from server time.

## What We Changed

### Backend (control-plane responsiveness)

- In `backend/core/orchestrator.py`:
  - Re-evaluated step elapsed time immediately before completion checks.
  - Moved/limited heavy operations from the critical path.
  - Throttled active rollups/state writes that were not step-critical.
  - Published FIND_MAX early state immediately on step start/advance.
  - Added `next_planned_concurrency` to `STEP_STARTING` state.
  - Made FIND_MAX history inserts asynchronous (non-blocking for handoff).

- In `backend/core/orchestrator.py` + `backend/core/live_metrics_cache.py`:
  - Preferred live in-memory metrics for measurement path before DB fallback.

### Backend (state transport)

- Added `backend/core/live_controller_state.py`:
  - In-memory cache for FIND_MAX and QPS controller state.
  - Monotonic replacement guard to prevent stale state regressions.

- In `backend/websocket/streaming.py`:
  - Prefer in-memory controller state (`MEM`) over DB state for payload.
  - Kept DB state as fallback/durability path.
  - Added source logging (`DB/MEM/LIVE`) for validation during debugging.

### Worker target behavior

- In `backend/core/orchestrator.py` and `scripts/run_worker.py`:
  - For `FIND_MAX_CONCURRENCY`, apply `SET_WORKER_TARGET` with `ramp_seconds=0`
    so transitions are not stretched by target ramping.
  - Left non-FIND_MAX behavior unchanged.

### Frontend (countdown correctness and clarity)

- In `backend/static/js/dashboard/find-max.js`:
  - Added fallback computation of "next workers" when
    `next_planned_concurrency` is missing.
  - Switched countdown clock from raw browser time to server-offset-adjusted
    time (`_serverNowMs()`).
  - Reduced noisy countdown debug logs unless debug mode is enabled.

- In `backend/static/js/dashboard/state.js` and
  `backend/static/js/dashboard.js`:
  - Added `_serverClockOffsetMs` and continuously smoothed offset from WS
    payload timestamps.

- In `backend/templates/components/latency_sections.html`:
  - Updated labels for clarity:
    - `Workers` -> `Threads`
    - `Active:` -> `Active Workers:`

## Debugging Sequence (Condensed)

1. **Timestamp consistency fixes**: removed obvious stale/fresh mismatches.
2. **Poll-loop hot path reductions**: throttled/async'd expensive tasks.
3. **WS cadence tuning**: reduced cache TTLs for RUN_STATUS in active states.
4. **In-memory controller path**: MEM state to WS, DB as fallback.
5. **State ordering protection**: blocked stale overwrite regressions.
6. **Asynchronous history writes**: decoupled history insert from step handoff.
7. **FIND_MAX immediate target apply**: removed ramp-induced transition drag.
8. **Frontend countdown fixes**: next-target fallback + server-clock offset.

## Verification Checklist

Use these log checks when validating future regressions:

- Step handoff occurs quickly:
  - `FIND_MAX step N complete ...`
  - `FIND_MAX step N (next): persisting early_state ...`
  - delta should be small.
- WS source alignment:
  - `[WS] find_max sources: DB(...) MEM(...) LIVE(...)`
  - MEM should advance immediately at step boundaries.
- No stale MEM regressions:
  - `MEM(step=...)` should not move backward for the same run.
- Slow-iteration monitoring:
  - `FIND_MAX poll loop slow iteration ...` should be rare and low.
- UI behavior:
  - "Next change in" should not sit at 0 for multiple seconds before next step.

## Lessons Learned

- Treat timing bugs as **distributed systems bugs** (controller, DB, worker,
  WS, browser clocks), not single-file bugs.
- Keep step-transition paths minimal and avoid synchronous DB dependencies.
- For operator-facing countdowns, prefer monotonic/live controller state over
  delayed persisted state.
- Add durable debugging signals (`DB/MEM/LIVE` source logs) during incident
  work; remove or gate noisy logs after stabilization.
