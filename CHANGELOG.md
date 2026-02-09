# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- feat(connections): simplified connection model - removed database_name, schema_name,
  pool_size, max_overflow, pool_timeout fields from connections (these belong in
  template configuration).
- feat(connections): connection selection per template - templates can now specify a
  `connection_id` to use stored credentials for benchmark execution.
- feat(ui): connection dropdown on Configure page with type-aware filtering (shows
  Snowflake connections for Snowflake table types, Postgres for Postgres types).
- feat(worker): benchmark workers can now use stored connection credentials via
  `connection_manager.get_connection_for_pool()` instead of requiring `.env` vars.
- feat(security): credential encryption warning toast shown only when clicking
  "Add Connection" in Settings (not globally).
- Dashboard log panel now supports source badges, level filtering, and a
  verbose logger toggle.
- feat(orchestrator): persist run creation in `RUN_STATUS` and parent
  `TEST_RESULTS` with scenario config snapshots.
- feat(api): add `/api/runs` endpoints for orchestrator run creation/start/stop.
- UI-driven autoscale option to launch multi-node runs with a total concurrency target.
- Autoscale controller for scale-out only with host CPU/memory guardrails.
- History list grouping for multi-node runs with parent/child expand toggle.
- `WORKER_METRICS_SNAPSHOTS` table for per-node metrics in multi-node runs.
- Headless worker CLI (`scripts/run_worker.py`) to run template-based workers.
- feat(worker): control-plane worker mode with RUN_STATUS config, control-event
  polling, and `WORKER_HEARTBEATS`/`WORKER_METRICS_SNAPSHOTS` persistence.
- feat(orchestrator): poll loop with heartbeat staleness, guardrail STOP,
  measurement rollups, and duration-based STOP scheduling.
- feat(orchestrator): stop_run STOP events with drain timeout, local SIGTERM
  fallback, and CANCELLED status finalization.
- feat(orchestrator): persist warehouse poller samples and parent rollups during
  control-plane polling.
- feat(orchestrator): controller-owned Find Max steps with per-worker
  `SET_WORKER_TARGET` events, `RUN_STATUS.FIND_MAX_STATE`, and step history
  persistence.
- test(orchestrator): unit coverage for poll loop, stop semantics, and parent
  rollups.
- Worker CLI overrides for concurrency/QPS parameters.
- Per-node metrics snapshots persisted during multi-node runs.
- Aggregated parent-run metrics stored in `TEST_RESULTS` for multi-node runs.
- API + history UI for per-node metrics drilldown in multi-node runs.
- Host + cgroup-aware resource metrics (CPU/memory) in live + history dashboards,
  including per-process context for capacity planning.
- Post-processing heartbeat logs every 30s in server logs and the browser console
  on the history dashboard while phase=PROCESSING.
- History dashboard status now uses colored badges and includes a Re-run button
  next to Open Live Dashboard.
- Separate `ENRICHMENT_STATUS` column to track post-processing (Snowflake QUERY_HISTORY
  enrichment) independently from test execution status. Values: PENDING, COMPLETED,
  FAILED, SKIPPED.
- History filters now support a custom date range (start/end).
- `POST /api/tests/{test_id}/retry-enrichment` endpoint to retry failed enrichment
  without re-running the test.
- `GET /api/tests/{test_id}/enrichment-status` endpoint to check enrichment status.
- History dashboard shows enrichment failure alert with "Retry Enrichment" button
  when post-processing failed but test completed successfully.
- AI Analysis button now enabled for all terminal test statuses (COMPLETED, FAILED,
  STOPPED, CANCELLED, ERROR) instead of only COMPLETED.
- QPS load mode to dynamically scale worker tasks to hit a target ops/sec, with
  warmup ramping to reduce lag at the start of the measurement window.
- feat(orchestrator): add fail-fast checks for results store connectivity,
  required control tables, and template presence on run create/start.
- feat(ui): show scaling mode summary and bounds status on live + history dashboards.
- FIND_MAX_CONCURRENCY live dashboard card: baseline vs current p95 (with % delta
  and max threshold), current → next worker target (with countdown), and a
  plain-text conclusion reason on completion.
- FIND_MAX_CONCURRENCY live dashboard card now shows P99 and error thresholds with
  near/over-limit highlighting.
- History dashboard step history table now includes P99, P99 delta, and per-step
  SLO status (when targets are configured).
- Configure page + dashboard SLO table now support optional per-query-type P99
  latency targets (in addition to existing P95 + error-rate targets).
- Live dashboard worker grid with per-worker health and on-demand snapshot
  drilldowns.
- Live dashboard counter for in-flight queries (concurrent DB operations).
- Live, per-test Snowflake query state telemetry (RUNNING/QUEUED/BLOCKED) derived
  from `INFORMATION_SCHEMA.QUERY_HISTORY_BY_WAREHOUSE` using a per-test `QUERY_TAG`.
- Live dashboard chart for concurrent queries (Snowflake bench running + client in-flight).
- History dashboard chart for worker concurrency (target workers vs actual in-flight).
- Live + history chart for Snowflake RUNNING queries (server) with benchmark-tagged
  totals and breakdown toggles (read/write or point/range/insert/update).
- History page compare panel: search tests by name and toggle-select up to 2 tests
  for side-by-side high-level metrics.
- Deep Compare page for two tests (`/history/compare?ids=<id1>,<id2>`) with
  overlaid throughput/latency/concurrency charts (primary in color, secondary in
  gray).
- feat(testing): short smoke test variant (45s) with temporary templates that
  auto-clean after run.
- docs(task): make the short smoke test the default and rename the original to
  smoke:long.
- feat(testing): on-demand API smoke runner for the 4 table-type variations,
  with AI analysis summaries and metrics validation.
- feat(testing): self-contained smoke setup (Snowflake smoke tables + templates)
  with optional Postgres seeding, plus setup/cleanup tasks.

### Fixed

- fix(worker): drain background metrics tasks before closing pools to avoid
  executor shutdown errors during worker teardown.
- fix(logging): enable colored level prefixes in console output.
- Remove unused imports and locals flagged by Ruff.
- Normalize console log formatting to match Uvicorn's level-prefix style.
- fix(ws): stream all live log updates over WebSocket (no server limit) and
  drop live HTTP log fetches while keeping enrichment/warehouse updates streaming.
- perf(api): parallelize initial queries in `GET /api/tests/{test_id}` endpoint.
  TEST_RESULTS, RUN_STATUS, and enrichment status now fetch concurrently in
  Phase 1 (previously sequential, adding ~1.5-3s latency). Also made
  `update_parent_run_aggregate()` fire-and-forget to avoid blocking the API
  response.
- fix(ws): derive WebSocket elapsed timing in Snowflake (TIMESTAMPDIFF +
  duration_seconds) to avoid timezone drift and post-completion growth.
- fix(ui): gate live WebSocket to measurement/STOPPING and prefer
  `timing.elapsed_display_seconds` for timer sync.
- fix(orchestrator): reset parent START_TIME on run start to avoid stale elapsed
  baselines.
- fix(orchestrator): stop duration now uses warmup start time so PREPARING
  does not shorten the measurement window.
- fix(api): treat zero duration_seconds as missing and cast CURRENT_TIMESTAMP
  to TIMESTAMP_NTZ for elapsed calculations.
- Live multi-node dashboard now streams parent metrics with correct payload shape,
  so phases and real-time charts update during autoscale runs.
- fix(ui): show PROCESSING for parent runs while post-run enrichment is pending.
- fix(api): keep final elapsed time from start/end timestamps instead of the
  configured duration on completion.
- fix(ui): keep Total Time stable during PROCESSING instead of resetting to 0.
- fix(ui): prevent phase regression from PROCESSING back to RUNNING on late payloads.
- fix(orchestrator): prevent find-max step tracking from failing in the poll loop.
- test(orchestrator): align poll loop tests with server-side elapsed timing.
- Parent multi-node timing now interprets TIMESTAMP_NTZ start times as UTC so
  warmup progresses to RUNNING instead of resetting at 0s.
- Parent multi-node dashboards now derive phase progression from per-node snapshots
  so PREPARING/WARMUP display correctly.
- Multi-node live dashboard run logs now support per-node selection via a dropdown
  instead of showing an empty aggregate log view.
- fix(ui): add an "All" option and ensure worker IDs populate the unified
  run log source dropdown spanning parent sources and worker logs.
- fix(ui): keep live run log polling active through PREPARING and PROCESSING
  so new worker sources appear while the WebSocket is active.
- Autoscale runs now prepare without autostart, returning to the live dashboard
  and starting only when the user clicks Start.
- Autoscale worker launch now uses async subprocesses to avoid blocking the
  FastAPI event loop under load.
- Multi-node parent aggregation no longer counts the parent row as a child,
  allowing parent status and metrics to finalize correctly.
- Multi-node parent runs now refresh aggregation when a completed parent is
  viewed or when the orchestrator finishes, preventing stale RUNNING status.
- History metrics endpoint now aggregates parent time-series data from
  `WORKER_METRICS_SNAPSHOTS` when `METRICS_SNAPSHOTS` is empty.
- Multi-worker live aggregation now uses slowest-worker P95/P99 and averages P50,
  with warehouse queueing fallback sourced from `WAREHOUSE_POLL_SNAPSHOTS`.
- Test status incorrectly set to CANCELLED when post-processing (enrichment) was
  interrupted. Now saves COMPLETED status immediately after test execution succeeds,
  before starting long-running enrichment. Enrichment failures no longer affect
  test status.
- fix(executor): custom workload now falls back to ROW pools when KEY/RANGE pools
  are missing for parameter binding.
- FIND_MAX_CONCURRENCY step history now persists backoff and midpoint probe steps
  so history matches captured worker concurrency.
- Added sampled per-operation SQL error logging (first 10 per error category) that
  surfaces the underlying exception + query + params preview in server logs and
  the live dashboard log stream (without flooding logs under load).
- Postgres schema introspection now preserves `CHAR(n)` / `VARCHAR(n)` lengths so
  custom workloads don’t generate values that overflow narrow character columns
  (e.g. `CHAR(1)` order status fields).
- When AI value pools are present (profiling skipped), insert ID sequences are now
  seeded from the table’s current MAX(key) + 1 (per table) instead of a fixed
  1e9-based offset, preventing repeat-run primary key collisions while keeping the
  x+N insert pattern across Postgres/Snowflake/hybrid.
- Removed a duplicate `target_qps` field definition in `TestScenario` that caused
  inconsistent typing/validation (int vs float) for QPS-mode templates.
- Configure page now shows toast error messages when catalog API calls fail
  (e.g., Postgres connection refused), instead of failing silently.

### Changed

- Persist `WORKER_ID` on `TEST_LOGS` entries and include it in log streaming.
- refactor(terminology): rename "node" to "worker" across API, UI, and docs to
  align with project nomenclature decision. Changes include:
  - API endpoint `/api/tests/{test_id}/node-metrics` → `/worker-metrics`
  - API response fields: `nodes` → `workers`, `node_id` → `worker_id`
  - API response fields: `node_find_max_results` → `worker_find_max_results`,
    `node_index` → `worker_index`, `total_nodes` → `total_workers`,
    `active_nodes` → `active_workers`
  - UI labels "Node Metrics", "Node Logs" → "Worker Metrics", "Worker Logs"
  - UI labels "All N Nodes" → "All N Workers", "Node X" → "Worker X"
  - JavaScript state variables and functions renamed (`nodeMetrics*` → `workerMetrics*`)
  - `docs/next-*.md` headers updated from "Multi-Node" to "Multi-Worker"
  - Internal autoscale code: `target_node_count` → `target_worker_count`,
    `started_nodes` → `started_workers`, `per_node_*` → `per_worker_*`
  - Autoscale state fields: `node_count` → `worker_count`,
    `target_qps_per_node` → `target_qps_per_worker`
  - CLI argument `--node-id` renamed to `--worker-id`
- refactor(api): parent run status/phase/timing now sourced from `RUN_STATUS`.
- chore(schema): remove legacy per-node snapshot DDL; consolidate on
  `WORKER_METRICS_SNAPSHOTS`.
- docs(plan): mark OrchestratorService create/prepare tasks complete.
- Autoscale total target now derives from the load mode's max worker settings
  instead of a separate autoscale target field.
- config: rename `min_concurrency` to `scaling.min_connections` for QPS per-worker
  floors, with updated UI bindings and CLI flag (`--min-connections`).
- docs(runbooks): document manual multi-worker acceptance flow and current
  template store for fail-fast checks.
- QPS auto-scale controller now ramps more gradually (lower gains, 5s control
  interval, tighter step caps, and a short under-target streak before scale-up)
  to reduce overshoot.
- feat(orchestrator): persist bounds completion reason when bounded QPS hits the
  max ceiling and throughput remains under target.
- History search now matches test name, table/database/schema, and warehouse fields.
- History page headers are compact and filters are collapsible with icons in results.
- FIND_MAX_CONCURRENCY no longer stops/backoffs on a step-over-step P95 spike when
  the absolute P95 is still below the run’s baseline (reduces false early
  termination).
- Configure page table dropdown now filters objects by table type:
  - STANDARD: shows tables and views
  - HYBRID: shows tables only (no views)
  - INTERACTIVE: shows tables only (no views)
  - Postgres: shows tables and views
- FIND_MAX_CONCURRENCY now treats enabled per-query-type SLO targets
  (P95/P99/error%) as hard constraints and records the failing query kind/metric
  in the stop reason.
- Dashboard SLO table highlights near-threshold values in orange and over-target
  values in red.
- Template list hides duration for FIND_MAX_CONCURRENCY templates.
- Postgres dashboards now show a Postgres database label and connection stats
  instead of Snowflake warehouse sizing.
- WebSocket streaming now emits `RUN_UPDATE` with `run` + `workers` payloads,
  mirroring aggregate metrics for existing charts and surfacing controller
  warehouse poller snapshots at the top level.
- AI analysis now renders Postgres stats (and uses N/A for missing queue
  metrics) instead of failing on null warehouse metrics.
- Compare Results page is now folded into History; `/comparison` redirects to `/history`.
- "Use Snowflake result cache" option now only shown for STANDARD table type
  (not applicable to hybrid/interactive tables which are optimized for OLTP).
- Suppressed per-operation SQL failure logs during tests (locks/timeouts/etc)
  and added throttled (10s) aggregated SQL error categories alongside the
  existing websocket metrics stream log.

### Removed

- Removed unused `sql/schema/migrations` folder and files.
- chore(cli): remove deprecated local multi-worker script (`scripts/run_multi_worker.py`).
- refactor(api): drop legacy compatibility branches in results/worker-metrics payloads.
- refactor(ui): remove legacy template card view and edit fallback paths.

### Performance

- perf(ws): serve live metrics from the in-memory cache fed by worker POSTs to
  restore 1s dashboard updates and reduce Snowflake polling.
- perf(ws): tier non-critical polling (logs, enrichment, warehouse) and document
  cadences to preserve 1s live dashboard updates.
- Skip table profiling when AI value pools exist. Previously, `profile_snowflake_table()`
  ran expensive DESCRIBE + MIN/MAX queries even when pre-computed value pools were
  already available from template preparation.
- Use `INFORMATION_SCHEMA.TABLES` row estimate instead of `SELECT COUNT(*)` for
  hybrid and standard table stats. COUNT(*) on large hybrid tables (e.g., 150M rows)
  could take 1-2 minutes; INFORMATION_SCHEMA lookup is instant.
- Hybrid-safe CUSTOM workload defaults:
  - Prefer key-range `RANGE_SCAN` (ID_BETWEEN) over time-cutoff scans for HYBRID
    tables.
  - Avoid `ORDER BY` + `SELECT *` in generated point/range queries (use a narrow
    projection).
  - RANGE value pools now generate "recent" cutoffs near `time_max` to avoid
    scanning most of the table.

- Fixed AI Adjust SQL timeout on hybrid tables by:
  - detecting hybrid table type reliably (column-name based)
  - skipping expensive `SAMPLE` queries and MIN/MAX bound scans during SQL adjustment
    (AI Adjust SQL only needs key/time column names)
- Fixed CUSTOM workload parameter binding when AI value pools exist by seeding a
  minimal per-table profile (key/time column names) even when skipping profiling,
  and falling back to the ROW pool when KEY/RANGE pools are missing.
- History dashboard now includes an **Error Summary** table aggregating
  Snowflake errors from `INFORMATION_SCHEMA.QUERY_HISTORY` (filtered by
  `QUERY_TAG`) plus app-captured local failures from `QUERY_EXECUTIONS`
  (`LOCAL_*`).
- Live Snowflake bench queued counts no longer incorrectly stay at 0 due to
  filtering out queued queries with `START_TIME IS NULL` in
  `QUERY_HISTORY_BY_WAREHOUSE` sampling.
- Live "Concurrent Queries" chart now plots warehouse-level queued counts
  (from `SHOW WAREHOUSES`) so the queued series matches Snowsight warehouse
  monitoring.
- Pre-create benchmark Snowflake pool sessions during PREPARING (with progress
  logging) to avoid RUNNING stalls at high concurrency.
- Prevented QPS-mode warmup from blocking the start of measurement when worker
  operations are slow/stuck (e.g. Snowflake connector timeouts/retries), by
  introducing a metrics epoch so late warmup completions cannot pollute the
  measurement window.
- Prevented per-test Snowflake pool teardown from hanging for minutes and
  spamming connector timeout traces by closing sessions in parallel with bounded
  per-connection close timeouts and a single summary log.
- Increased Snowflake connector timeout defaults (default/results pool: 15s → 60s,
  benchmark pool: 60s → 300s) to reduce noisy timeouts and avoid client-side
  failures under warehouse-side queueing/slowdowns.
- Hardened template scenario parsing for QPS settings to avoid type/validation
  edge cases.
- Prevented QPS scaling from overspawning beyond the Snowflake pool size, which
  could cause "Connection pool exhausted" errors.
- QPS mode now auto-scales workers to a single, consistent target: app-side
  throughput (ops/sec). Snowflake server-side `RUNNING`/queued metrics are
  captured separately for observability.
- Added Snowflake warehouse running/queued counters to the live dashboard stream
  to compare server-side running queries vs. client in-flight.
- Query history enrichment now supports per-test `QUERY_TAG` values (prefix match),
  preserving server-side SQL execution timing fields on `QUERY_EXECUTIONS`.
- History dashboard MCW chart now loads per-second warehouse timeseries without
  depending on `METRICS_SNAPSHOTS`, and surfaces backend errors instead of showing
  a misleading "no data" message.
- History dashboard now retries per-second MCW timeseries fetches while
  post-processing completes, preventing premature "no data" when
  `QUERY_HISTORY` is delayed.
- Query history enrichment now waits longer (up to 240s) before finalizing
  post-processing to account for `QUERY_HISTORY` latency under load.
- History concurrency chart now prefers per-test queued query counts (from
  `QUERY_HISTORY`) over warehouse queued cluster counts to align with MCW queue
  timing metrics.
- FIND_MAX_CONCURRENCY progress timing no longer shows a fixed end time; the
  running phase now displays elapsed-only timing.
