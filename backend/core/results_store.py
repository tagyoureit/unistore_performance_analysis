"""
Snowflake Results Store

Persists test runs, test results, and time-series metrics snapshots into
UNISTORE_BENCHMARK.TEST_RESULTS.* tables.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import UTC, datetime, timedelta
from typing import Any, NamedTuple, Optional
from uuid import uuid4

from backend.config import settings
from backend.connectors import snowflake_pool
from backend.models import Metrics, TestResult, TestScenario

logger = logging.getLogger(__name__)


def _results_prefix() -> str:
    # Keep identifiers fully-qualified so we don't rely on session USE DATABASE/SCHEMA.
    return f"{settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}"


async def fetch_warehouse_config_snapshot(
    warehouse_name: str,
) -> Optional[dict[str, Any]]:
    """
    Fetch current warehouse configuration via SHOW WAREHOUSES.

    Captures MCW settings, scaling policy, query acceleration, etc. at a point in time.
    Returns None if warehouse not found or on error.
    """
    if not warehouse_name:
        return None

    try:
        pool = snowflake_pool.get_default_pool()
        query = f"SHOW WAREHOUSES LIKE '{warehouse_name}'"
        results = await pool.execute_query(query)

        if not results:
            return None

        row = results[0]
        # SHOW WAREHOUSES column indices (0-based):
        # 0=name, 1=state, 2=type, 3=size, 4=min_cluster_count, 5=max_cluster_count
        # 6=started_clusters, 7=running, 8=queued, 9=is_default, 10=is_current
        # 11=is_interactive, 12=auto_suspend, 13=auto_resume
        # 23=enable_query_acceleration, 24=query_acceleration_max_scale_factor
        # 31=scaling_policy, 33=resource_constraint (Gen1/Gen2)
        return {
            "name": row[0],
            "state": row[1],
            "type": row[2],
            "size": row[3],
            "min_cluster_count": row[4] if row[4] else 1,
            "max_cluster_count": row[5] if row[5] else 1,
            "started_clusters": row[6] if row[6] else 0,
            "running": row[7] if row[7] else 0,
            "queued": row[8] if row[8] else 0,
            "is_default": row[9] == "Y" if row[9] else False,
            "is_current": row[10] == "Y" if row[10] else False,
            "auto_suspend": row[12],
            "auto_resume": row[13] == "true" if row[13] else False,
            "scaling_policy": row[31] if len(row) > 31 and row[31] else "STANDARD",
            "enable_query_acceleration": row[23] == "true"
            if len(row) > 23 and row[23]
            else False,
            "query_acceleration_max_scale_factor": row[24]
            if len(row) > 24 and row[24]
            else 0,
            "resource_constraint": row[33] if len(row) > 33 else None,
            "captured_at": datetime.now(UTC).isoformat(),
        }
    except Exception:
        # Non-fatal: return None if we can't fetch warehouse details
        return None


async def insert_test_start(
    *,
    test_id: str,
    test_name: str,
    scenario: TestScenario,
    table_name: str,
    table_type: str,
    warehouse: Optional[str],
    warehouse_size: Optional[str],
    template_id: str,
    template_name: str,
    template_config: dict[str, Any],
    warehouse_config_snapshot: Optional[dict[str, Any]] = None,
    query_tag: Optional[str] = None,
) -> None:
    pool = snowflake_pool.get_default_pool()
    now = datetime.now(UTC).isoformat()

    payload = {
        "template_id": template_id,
        "template_name": template_name,
        "template_config": template_config,
        "scenario": scenario.model_dump(mode="json"),
    }

    query = f"""
    INSERT INTO {_results_prefix()}.TEST_RESULTS (
        TEST_ID,
        RUN_ID,
        TEST_NAME,
        SCENARIO_NAME,
        TABLE_NAME,
        TABLE_TYPE,
        WAREHOUSE,
        WAREHOUSE_SIZE,
        STATUS,
        START_TIME,
        CONCURRENT_CONNECTIONS,
        TEST_CONFIG,
        WAREHOUSE_CONFIG_SNAPSHOT,
        QUERY_TAG
    )
    SELECT
        ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, PARSE_JSON(?), PARSE_JSON(?), ?
    """

    params = [
        test_id,
        test_name,
        scenario.name,
        table_name,
        table_type,
        warehouse,
        warehouse_size,
        "RUNNING",
        now,
        scenario.concurrent_connections,
        json.dumps(payload),
        json.dumps(warehouse_config_snapshot) if warehouse_config_snapshot else None,
        query_tag,
    ]

    await pool.execute_query(query, params=params)


async def insert_metrics_snapshot(*, test_id: str, metrics: Metrics) -> None:
    pool = snowflake_pool.get_default_pool()
    snapshot_id = str(uuid4())

    snapshot_query = f"""
    INSERT INTO {_results_prefix()}.METRICS_SNAPSHOTS (
        SNAPSHOT_ID,
        TEST_ID,
        TIMESTAMP,
        ELAPSED_SECONDS,
        TOTAL_QUERIES,
        QPS,
        P50_LATENCY_MS,
        P95_LATENCY_MS,
        P99_LATENCY_MS,
        AVG_LATENCY_MS,
        READ_COUNT,
        WRITE_COUNT,
        ERROR_COUNT,
        BYTES_PER_SECOND,
        ROWS_PER_SECOND,
        ACTIVE_CONNECTIONS,
        TARGET_WORKERS,
        CUSTOM_METRICS
    )
    SELECT
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, PARSE_JSON(?)
    """

    params = [
        snapshot_id,
        test_id,
        metrics.timestamp.isoformat(),
        metrics.elapsed_seconds,
        metrics.total_operations,
        metrics.current_qps,
        metrics.overall_latency.p50,
        metrics.overall_latency.p95,
        metrics.overall_latency.p99,
        metrics.overall_latency.avg,
        metrics.read_metrics.count,
        metrics.write_metrics.count,
        metrics.failed_operations,
        metrics.bytes_per_second,
        metrics.rows_per_second,
        metrics.active_connections,
        metrics.target_workers,
        json.dumps(metrics.custom_metrics or {}),
    ]

    await pool.execute_query(snapshot_query, params=params)


async def insert_query_executions(
    *,
    test_id: str,
    rows: list[dict[str, Any]],
    chunk_size: int = 500,
) -> None:
    """
    Bulk insert per-operation rows into TEST_RESULTS.QUERY_EXECUTIONS.

    Notes:
    - This is best-effort and should not fail the test if it can't persist.
    - `rows` can include warmup operations (WARMUP flag).
    """
    if not rows:
        return

    pool = snowflake_pool.get_default_pool()

    cols = [
        "EXECUTION_ID",
        "TEST_ID",
        "QUERY_ID",
        "QUERY_TEXT",
        "START_TIME",
        "END_TIME",
        "DURATION_MS",
        "ROWS_AFFECTED",
        "BYTES_SCANNED",
        "WAREHOUSE",
        "SUCCESS",
        "ERROR",
        "CONNECTION_ID",
        "CUSTOM_METADATA",
        "QUERY_KIND",
        "WORKER_ID",
        "WARMUP",
        "APP_ELAPSED_MS",
        # DML row counters (derived deterministically from QUERY_KIND + ROWS_AFFECTED).
        # These are NOT reliably available via INFORMATION_SCHEMA.QUERY_HISTORY.
        "SF_ROWS_INSERTED",
        "SF_ROWS_UPDATED",
        "SF_ROWS_DELETED",
    ]

    # NOTE: Snowflake does not accept PARSE_JSON(?) inside a VALUES clause for this
    # connector/paramstyle combination. Use INSERT ... SELECT ... FROM VALUES, and
    # apply TRY_PARSE_JSON() in the SELECT projection.
    #
    # This keeps the bulk insert best-effort and avoids failing runs due to
    # CUSTOM_METADATA (VARIANT) binding.
    custom_idx_1based = cols.index("CUSTOM_METADATA") + 1
    select_exprs: list[str] = []
    for i, col in enumerate(cols, start=1):
        if i == custom_idx_1based:
            select_exprs.append(f"TRY_PARSE_JSON(COLUMN{i}) AS {col}")
        else:
            select_exprs.append(f"COLUMN{i} AS {col}")

    insert_prefix = f"""
    INSERT INTO {_results_prefix()}.QUERY_EXECUTIONS (
        {", ".join(cols)}
    )
    SELECT
        {", ".join(select_exprs)}
    FROM VALUES
    """

    def _row_params(r: dict[str, Any]) -> list[Any]:
        query_kind = (r.get("query_kind") or "").strip().upper()
        rows_affected = r.get("rows_affected")
        sf_rows_inserted = rows_affected if query_kind == "INSERT" else None
        sf_rows_updated = rows_affected if query_kind == "UPDATE" else None
        # We don't execute deletes today; keep null unless we add DELETE operations.
        sf_rows_deleted = rows_affected if query_kind == "DELETE" else None

        return [
            r.get("execution_id"),
            test_id,
            r.get("query_id"),
            r.get("query_text"),
            r.get("start_time"),
            r.get("end_time"),
            r.get("duration_ms"),
            rows_affected,
            r.get("bytes_scanned"),
            r.get("warehouse"),
            bool(r.get("success")),
            r.get("error"),
            r.get("connection_id"),
            json.dumps(r.get("custom_metadata") or {}),
            r.get("query_kind"),
            r.get("worker_id"),
            bool(r.get("warmup")),
            r.get("app_elapsed_ms"),
            sf_rows_inserted,
            sf_rows_updated,
            sf_rows_deleted,
        ]

    i = 0
    while i < len(rows):
        batch = rows[i : i + chunk_size]
        row_tpl = "(" + ", ".join(["?"] * len(cols)) + ")"
        values_sql = ",\n".join([row_tpl] * len(batch))
        query = insert_prefix + values_sql
        params: list[Any] = []
        for r in batch:
            params.extend(_row_params(r))
        await pool.execute_query(query, params=params)
        i += chunk_size


async def insert_test_logs(
    *, rows: list[dict[str, Any]], chunk_size: int = 500
) -> None:
    """
    Bulk insert log rows into TEST_RESULTS.TEST_LOGS.

    Expected keys per row:
    - log_id, test_id, seq, timestamp, level, logger, message, exception
    """
    if not rows:
        return

    pool = snowflake_pool.get_default_pool()
    cols = [
        "LOG_ID",
        "TEST_ID",
        "SEQ",
        "TIMESTAMP",
        "LEVEL",
        "LOGGER",
        "MESSAGE",
        "EXCEPTION",
    ]
    insert_prefix = (
        f"INSERT INTO {_results_prefix()}.TEST_LOGS ({', '.join(cols)}) VALUES\n"
    )

    def _row_params(r: dict[str, Any]) -> list[Any]:
        return [
            r.get("log_id"),
            r.get("test_id"),
            r.get("seq"),
            r.get("timestamp"),
            r.get("level"),
            r.get("logger"),
            r.get("message"),
            r.get("exception"),
        ]

    i = 0
    while i < len(rows):
        batch = rows[i : i + chunk_size]
        row_tpl = "(" + ", ".join(["?"] * len(cols)) + ")"
        values_sql = ",\n".join([row_tpl] * len(batch))
        query = insert_prefix + values_sql
        params: list[Any] = []
        for r in batch:
            params.extend(_row_params(r))
        await pool.execute_query(query, params=params)
        i += chunk_size


async def update_test_result_final(
    *, test_id: str, result: TestResult, find_max_result: dict | None = None
) -> None:
    pool = snowflake_pool.get_default_pool()

    end_time = result.end_time.isoformat() if result.end_time else None

    query = f"""
    UPDATE {_results_prefix()}.TEST_RESULTS
    SET
        STATUS = ?,
        END_TIME = ?,
        DURATION_SECONDS = ?,
        TOTAL_OPERATIONS = ?,
        READ_OPERATIONS = ?,
        WRITE_OPERATIONS = ?,
        FAILED_OPERATIONS = ?,
        QPS = ?,
        READS_PER_SECOND = ?,
        WRITES_PER_SECOND = ?,
        AVG_LATENCY_MS = ?,
        P50_LATENCY_MS = ?,
        P90_LATENCY_MS = ?,
        P95_LATENCY_MS = ?,
        P99_LATENCY_MS = ?,
        MIN_LATENCY_MS = ?,
        MAX_LATENCY_MS = ?,
        ERROR_COUNT = ?,
        ERROR_RATE = ?,
        FAILURE_REASON = ?,
        ROWS_READ = ?,
        ROWS_WRITTEN = ?,
        READ_P50_LATENCY_MS = ?,
        READ_P95_LATENCY_MS = ?,
        READ_P99_LATENCY_MS = ?,
        READ_MIN_LATENCY_MS = ?,
        READ_MAX_LATENCY_MS = ?,
        WRITE_P50_LATENCY_MS = ?,
        WRITE_P95_LATENCY_MS = ?,
        WRITE_P99_LATENCY_MS = ?,
        WRITE_MIN_LATENCY_MS = ?,
        WRITE_MAX_LATENCY_MS = ?,
        POINT_LOOKUP_P50_LATENCY_MS = ?,
        POINT_LOOKUP_P95_LATENCY_MS = ?,
        POINT_LOOKUP_P99_LATENCY_MS = ?,
        POINT_LOOKUP_MIN_LATENCY_MS = ?,
        POINT_LOOKUP_MAX_LATENCY_MS = ?,
        RANGE_SCAN_P50_LATENCY_MS = ?,
        RANGE_SCAN_P95_LATENCY_MS = ?,
        RANGE_SCAN_P99_LATENCY_MS = ?,
        RANGE_SCAN_MIN_LATENCY_MS = ?,
        RANGE_SCAN_MAX_LATENCY_MS = ?,
        INSERT_P50_LATENCY_MS = ?,
        INSERT_P95_LATENCY_MS = ?,
        INSERT_P99_LATENCY_MS = ?,
        INSERT_MIN_LATENCY_MS = ?,
        INSERT_MAX_LATENCY_MS = ?,
        UPDATE_P50_LATENCY_MS = ?,
        UPDATE_P95_LATENCY_MS = ?,
        UPDATE_P99_LATENCY_MS = ?,
        UPDATE_MIN_LATENCY_MS = ?,
        UPDATE_MAX_LATENCY_MS = ?,
        APP_OVERHEAD_P50_MS = ?,
        APP_OVERHEAD_P95_MS = ?,
        APP_OVERHEAD_P99_MS = ?,
        FIND_MAX_RESULT = PARSE_JSON(?),
        UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE TEST_ID = ?
    """

    error_rate = 0.0
    if result.total_operations > 0:
        error_rate = result.failed_operations / result.total_operations

    params = [
        str(result.status).upper(),
        end_time,
        result.duration_seconds,
        result.total_operations,
        result.read_operations,
        result.write_operations,
        result.failed_operations,
        result.qps,
        result.reads_per_second,
        result.writes_per_second,
        result.avg_latency_ms,
        result.p50_latency_ms,
        result.p90_latency_ms,
        result.p95_latency_ms,
        result.p99_latency_ms,
        result.min_latency_ms,
        result.max_latency_ms,
        result.error_count,
        error_rate,
        result.failure_reason,
        result.rows_read,
        result.rows_written,
        result.read_p50_latency_ms,
        result.read_p95_latency_ms,
        result.read_p99_latency_ms,
        result.read_min_latency_ms,
        result.read_max_latency_ms,
        result.write_p50_latency_ms,
        result.write_p95_latency_ms,
        result.write_p99_latency_ms,
        result.write_min_latency_ms,
        result.write_max_latency_ms,
        result.point_lookup_p50_latency_ms,
        result.point_lookup_p95_latency_ms,
        result.point_lookup_p99_latency_ms,
        result.point_lookup_min_latency_ms,
        result.point_lookup_max_latency_ms,
        result.range_scan_p50_latency_ms,
        result.range_scan_p95_latency_ms,
        result.range_scan_p99_latency_ms,
        result.range_scan_min_latency_ms,
        result.range_scan_max_latency_ms,
        result.insert_p50_latency_ms,
        result.insert_p95_latency_ms,
        result.insert_p99_latency_ms,
        result.insert_min_latency_ms,
        result.insert_max_latency_ms,
        result.update_p50_latency_ms,
        result.update_p95_latency_ms,
        result.update_p99_latency_ms,
        result.update_min_latency_ms,
        result.update_max_latency_ms,
        result.app_overhead_p50_ms,
        result.app_overhead_p95_ms,
        result.app_overhead_p99_ms,
        json.dumps(find_max_result) if find_max_result else None,
        test_id,
    ]

    await pool.execute_query(query, params=params)


async def _get_test_time_range(
    pool: Any, prefix: str, test_id: str
) -> tuple[datetime, datetime] | None:
    """Get start/end timestamps for a test from QUERY_EXECUTIONS or TEST_RESULTS."""
    qe_rows = await pool.execute_query(
        f"""
        SELECT MIN(START_TIME), MAX(END_TIME)
        FROM {prefix}.QUERY_EXECUTIONS
        WHERE TEST_ID = ?
        """,
        params=[test_id],
    )

    start_ntz = qe_rows[0][0] if qe_rows and qe_rows[0] else None
    end_ntz = qe_rows[0][1] if qe_rows and qe_rows[0] else None

    if start_ntz is None or end_ntz is None:
        tr_rows = await pool.execute_query(
            f"""
            SELECT START_TIME, COALESCE(END_TIME, CURRENT_TIMESTAMP())
            FROM {prefix}.TEST_RESULTS
            WHERE TEST_ID = ?
            """,
            params=[test_id],
        )
        if not tr_rows:
            return None
        start_ntz, end_ntz = tr_rows[0][0], tr_rows[0][1]

    if isinstance(start_ntz, str):
        start_dt = datetime.fromisoformat(start_ntz)
    else:
        start_dt = start_ntz
    if isinstance(end_ntz, str):
        end_dt = datetime.fromisoformat(end_ntz)
    else:
        end_dt = end_ntz

    if start_dt.tzinfo is None:
        start_dt = start_dt.replace(tzinfo=UTC)
    if end_dt.tzinfo is None:
        end_dt = end_dt.replace(tzinfo=UTC)

    return start_dt, end_dt


async def enrich_query_executions_from_query_history(*, test_id: str) -> int:
    """
    Enrich QUERY_EXECUTIONS rows for a test using INFORMATION_SCHEMA.QUERY_HISTORY.

    Paginates through results since QUERY_HISTORY has a 10k row limit per call.
    Returns the total number of rows merged across all pages.
    """
    pool = snowflake_pool.get_default_pool()
    prefix = _results_prefix()

    time_range = await _get_test_time_range(pool, prefix, test_id)
    if time_range is None:
        return 0
    start_dt, end_dt = time_range

    start_buf = (start_dt - timedelta(minutes=5)).isoformat()
    current_end = (end_dt + timedelta(minutes=5)).isoformat()
    total_merged = 0
    max_pages = 20

    for page in range(max_pages):
        result = await pool.execute_query(
            """
            SELECT MIN(END_TIME) as oldest_end, COUNT(*) as cnt
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                END_TIME_RANGE_START=>TO_TIMESTAMP_LTZ(?),
                END_TIME_RANGE_END=>TO_TIMESTAMP_LTZ(?),
                RESULT_LIMIT=>10000
            ))
            WHERE QUERY_TAG LIKE 'unistore_benchmark%'
            """,
            params=[start_buf, current_end],
        )

        if not result or result[0][1] == 0:
            break

        row_count = result[0][1]
        oldest_end_time = result[0][0]

        merge_query = f"""
        MERGE INTO {prefix}.QUERY_EXECUTIONS tgt
        USING (
            SELECT
                QUERY_ID,
                TOTAL_ELAPSED_TIME::FLOAT AS SF_TOTAL_ELAPSED_MS,
                EXECUTION_TIME::FLOAT AS SF_EXECUTION_MS,
                COMPILATION_TIME::FLOAT AS SF_COMPILATION_MS,
                QUEUED_OVERLOAD_TIME::FLOAT AS SF_QUEUED_OVERLOAD_MS,
                QUEUED_PROVISIONING_TIME::FLOAT AS SF_QUEUED_PROVISIONING_MS,
                TRANSACTION_BLOCKED_TIME::FLOAT AS SF_TX_BLOCKED_MS,
                BYTES_SCANNED::BIGINT AS SF_BYTES_SCANNED,
                ROWS_PRODUCED::BIGINT AS SF_ROWS_PRODUCED,
                CLUSTER_NUMBER::INTEGER AS SF_CLUSTER_NUMBER
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                END_TIME_RANGE_START=>TO_TIMESTAMP_LTZ(?),
                END_TIME_RANGE_END=>TO_TIMESTAMP_LTZ(?),
                RESULT_LIMIT=>10000
            ))
            WHERE QUERY_TAG LIKE 'unistore_benchmark%'
        ) src
        ON tgt.QUERY_ID = src.QUERY_ID
        AND tgt.TEST_ID = ?
        WHEN MATCHED THEN UPDATE SET
            SF_TOTAL_ELAPSED_MS = src.SF_TOTAL_ELAPSED_MS,
            SF_EXECUTION_MS = src.SF_EXECUTION_MS,
            SF_COMPILATION_MS = src.SF_COMPILATION_MS,
            SF_QUEUED_OVERLOAD_MS = src.SF_QUEUED_OVERLOAD_MS,
            SF_QUEUED_PROVISIONING_MS = src.SF_QUEUED_PROVISIONING_MS,
            SF_TX_BLOCKED_MS = src.SF_TX_BLOCKED_MS,
            SF_BYTES_SCANNED = src.SF_BYTES_SCANNED,
            SF_ROWS_PRODUCED = src.SF_ROWS_PRODUCED,
            SF_CLUSTER_NUMBER = src.SF_CLUSTER_NUMBER,
            APP_OVERHEAD_MS = IFF(
                tgt.APP_ELAPSED_MS IS NULL OR src.SF_TOTAL_ELAPSED_MS IS NULL,
                NULL,
                tgt.APP_ELAPSED_MS - src.SF_TOTAL_ELAPSED_MS
            );
        """
        await pool.execute_query(merge_query, params=[start_buf, current_end, test_id])
        total_merged += row_count

        if row_count < 10000:
            break

        if oldest_end_time:
            current_end = (oldest_end_time - timedelta(microseconds=1)).isoformat()
        else:
            break

        logger.debug(
            "Enrichment page %d: %d rows, moving end to %s",
            page + 1,
            row_count,
            current_end,
        )

    return total_merged


class EnrichmentStats(NamedTuple):
    total_queries: int
    enriched_queries: int
    enrichment_ratio: float


async def get_enrichment_stats(*, test_id: str) -> EnrichmentStats:
    pool = snowflake_pool.get_default_pool()
    prefix = _results_prefix()
    rows = await pool.execute_query(
        f"""
        SELECT
            COUNT(*) AS total,
            COUNT(SF_CLUSTER_NUMBER) AS enriched
        FROM {prefix}.QUERY_EXECUTIONS
        WHERE TEST_ID = ?
        """,
        params=[test_id],
    )
    total = rows[0][0] if rows else 0
    enriched = rows[0][1] if rows else 0
    ratio = enriched / total if total > 0 else 0.0
    return EnrichmentStats(total, enriched, ratio)


async def enrich_query_executions_with_retry(
    *,
    test_id: str,
    target_ratio: float = 0.90,
    max_wait_seconds: int = 120,
    poll_interval_seconds: int = 10,
) -> EnrichmentStats:
    """
    Enrich QUERY_EXECUTIONS with retries until target_ratio of queries are enriched.

    QUERY_HISTORY has ~45+ second latency before queries appear. This function polls
    periodically and re-runs the paginated enrichment merge until we achieve the
    target ratio or timeout.
    """
    start = datetime.now(UTC)
    deadline = start + timedelta(seconds=max_wait_seconds)
    best_stats = EnrichmentStats(0, 0, 0.0)

    while datetime.now(UTC) < deadline:
        merged = await enrich_query_executions_from_query_history(test_id=test_id)
        stats = await get_enrichment_stats(test_id=test_id)
        best_stats = stats

        if stats.total_queries == 0:
            logger.debug("No queries to enrich for test %s", test_id)
            break

        logger.debug(
            "Enrichment stats for %s: %d/%d (%.1f%%) - merged %d rows this pass",
            test_id,
            stats.enriched_queries,
            stats.total_queries,
            stats.enrichment_ratio * 100,
            merged,
        )

        if stats.enrichment_ratio >= target_ratio:
            logger.info(
                "Enrichment complete for %s: %.1f%% (%d/%d queries)",
                test_id,
                stats.enrichment_ratio * 100,
                stats.enriched_queries,
                stats.total_queries,
            )
            break

        await asyncio.sleep(poll_interval_seconds)

    if best_stats.enrichment_ratio < target_ratio and best_stats.total_queries > 0:
        logger.warning(
            "Enrichment timeout for %s: only %.1f%% (%d/%d queries) after %ds",
            test_id,
            best_stats.enrichment_ratio * 100,
            best_stats.enriched_queries,
            best_stats.total_queries,
            max_wait_seconds,
        )

    return best_stats


async def update_test_overhead_percentiles(*, test_id: str) -> None:
    """
    Compute overhead percentiles from QUERY_EXECUTIONS.APP_OVERHEAD_MS and store
    them on TEST_RESULTS (one-row summary table).
    """
    pool = snowflake_pool.get_default_pool()
    prefix = _results_prefix()

    # NOTE: Snowflake does not support CTE + UPDATE in the way we need here.
    # Use scalar subqueries instead (still set-based, easy to reason about).
    query = f"""
    UPDATE {prefix}.TEST_RESULTS
    SET
        APP_OVERHEAD_P50_MS = (
            SELECT PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY APP_OVERHEAD_MS)
            FROM {prefix}.QUERY_EXECUTIONS
            WHERE TEST_ID = ?
              AND APP_OVERHEAD_MS IS NOT NULL
        ),
        APP_OVERHEAD_P95_MS = (
            SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY APP_OVERHEAD_MS)
            FROM {prefix}.QUERY_EXECUTIONS
            WHERE TEST_ID = ?
              AND APP_OVERHEAD_MS IS NOT NULL
        ),
        APP_OVERHEAD_P99_MS = (
            SELECT PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY APP_OVERHEAD_MS)
            FROM {prefix}.QUERY_EXECUTIONS
            WHERE TEST_ID = ?
              AND APP_OVERHEAD_MS IS NOT NULL
        ),
        UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE TEST_ID = ?;
    """

    await pool.execute_query(query, params=[test_id, test_id, test_id, test_id])


async def update_enrichment_status(
    *,
    test_id: str,
    status: str,
    error: str | None = None,
) -> None:
    """
    Update the enrichment status for a test.

    Args:
        test_id: The test ID
        status: One of PENDING, COMPLETED, FAILED, SKIPPED
        error: Optional error message if status is FAILED
    """
    pool = snowflake_pool.get_default_pool()
    prefix = _results_prefix()

    query = f"""
    UPDATE {prefix}.TEST_RESULTS
    SET
        ENRICHMENT_STATUS = ?,
        ENRICHMENT_ERROR = ?,
        UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE TEST_ID = ?
    """

    await pool.execute_query(query, params=[status, error, test_id])


async def get_enrichment_status(*, test_id: str) -> dict[str, Any] | None:
    """
    Get the enrichment status for a test.

    Returns dict with enrichment_status, enrichment_error, or None if test not found.
    """
    pool = snowflake_pool.get_default_pool()
    prefix = _results_prefix()

    rows = await pool.execute_query(
        f"""
        SELECT STATUS, ENRICHMENT_STATUS, ENRICHMENT_ERROR
        FROM {prefix}.TEST_RESULTS
        WHERE TEST_ID = ?
        """,
        params=[test_id],
    )

    if not rows:
        return None

    return {
        "test_status": rows[0][0],
        "enrichment_status": rows[0][1],
        "enrichment_error": rows[0][2],
    }
