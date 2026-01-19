# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

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
- FIND_MAX_CONCURRENCY live dashboard card: baseline vs current p95 (with % delta
  and max threshold), current → next worker target (with countdown), and a
  plain-text conclusion reason on completion.
- FIND_MAX_CONCURRENCY live dashboard card now shows P99 and error thresholds with
  near/over-limit highlighting.
- History dashboard step history table now includes P99, P99 delta, and per-step
  SLO status (when targets are configured).
- Configure page + dashboard SLO table now support optional per-query-type P99
  latency targets (in addition to existing P95 + error-rate targets).
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

- QPS auto-scale controller now ramps more gradually (lower gains, 5s control
  interval, tighter step caps, and a short under-target streak before scale-up)
  to reduce overshoot.
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

### Performance

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
