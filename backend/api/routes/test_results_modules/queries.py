"""
Database query functions for test results.

Contains functions for fetching run status, metrics, and enrichment data.
"""

from __future__ import annotations

import logging
from typing import Any

from backend.api.routes.test_results.utils import get_prefix, to_float_or_none
from backend.connectors import postgres_pool

logger = logging.getLogger(__name__)


async def fetch_run_status(pool: Any, run_id: str) -> dict[str, Any] | None:
    """Fetch run status from RUN_STATUS table."""
    rows = await pool.execute_query(
        f"""
        SELECT RUN_ID, STATUS, PHASE, START_TIME, END_TIME, FIND_MAX_STATE, CANCELLATION_REASON,
               TIMESTAMPDIFF(SECOND, START_TIME, CURRENT_TIMESTAMP()) AS ELAPSED_SECONDS
        FROM {get_prefix()}.RUN_STATUS
        WHERE RUN_ID = ?
        """,
        params=[run_id],
    )
    if not rows:
        return None
    (
        run_id_val,
        status,
        phase,
        start_time,
        end_time,
        find_max_state,
        cancellation_reason,
        elapsed_secs,
    ) = rows[0]
    return {
        "run_id": str(run_id_val or ""),
        "status": str(status or "").upper() or None,
        "phase": str(phase or "").upper() or None,
        "start_time": start_time,
        "end_time": end_time,
        "find_max_state": find_max_state,
        "cancellation_reason": str(cancellation_reason) if cancellation_reason else None,
        "elapsed_seconds": float(elapsed_secs) if elapsed_secs is not None else None,
    }


async def aggregate_parent_enrichment_status(
    *, pool: Any, run_id: str
) -> tuple[str | None, str | None]:
    """Aggregate ENRICHMENT_STATUS, checking parent first (authoritative), then workers."""
    prefix = get_prefix()

    # Check parent row first - it's the authoritative source
    parent_rows = await pool.execute_query(
        f"""
        SELECT ENRICHMENT_STATUS, ENRICHMENT_ERROR
        FROM {prefix}.TEST_RESULTS
        WHERE TEST_ID = ?
        """,
        params=[run_id],
    )
    if parent_rows and parent_rows[0][0]:
        parent_status = str(parent_rows[0][0]).strip().upper()
        parent_error = parent_rows[0][1]
        if parent_status in ("COMPLETED", "FAILED", "SKIPPED"):
            return parent_status, str(parent_error) if parent_error else None

    # Fallback: aggregate worker rows
    worker_rows = await pool.execute_query(
        f"""
        SELECT ENRICHMENT_STATUS, ENRICHMENT_ERROR
        FROM {prefix}.TEST_RESULTS
        WHERE RUN_ID = ?
          AND TEST_ID <> ?
        """,
        params=[run_id, run_id],
    )
    statuses: list[str] = []
    errors: list[str] = []
    for status_value, error in worker_rows or []:
        status_val = str(status_value or "").strip().upper()
        if status_val:
            statuses.append(status_val)
        if error:
            errors.append(str(error))
    if not statuses:
        if parent_rows and parent_rows[0][0]:
            parent_status = str(parent_rows[0][0]).strip().upper()
            parent_error = parent_rows[0][1]
            return parent_status, str(parent_error) if parent_error else None
        return None, None
    if "PENDING" in statuses:
        return "PENDING", None
    if "FAILED" in statuses:
        error_out = next((err for err in errors if err), None)
        return "FAILED", error_out
    if "COMPLETED" in statuses:
        return "COMPLETED", None
    if "SKIPPED" in statuses:
        return "SKIPPED", None
    return statuses[0], None


async def aggregate_parent_enrichment_stats(
    *, pool: Any, run_id: str
) -> tuple[int, int, float]:
    """Get enrichment statistics for a parent run."""
    prefix = get_prefix()
    rows = await pool.execute_query(
        f"""
        SELECT
            COUNT(*) AS total,
            COUNT(SF_CLUSTER_NUMBER) AS enriched
        FROM {prefix}.QUERY_EXECUTIONS qe
        JOIN {prefix}.TEST_RESULTS tr
          ON qe.TEST_ID = tr.TEST_ID
        WHERE tr.RUN_ID = ?
          AND tr.TEST_ID <> ?
        """,
        params=[run_id, run_id],
    )
    total = int(rows[0][0] or 0) if rows else 0
    enriched = int(rows[0][1] or 0) if rows else 0
    ratio = enriched / total if total > 0 else 0.0
    return total, enriched, ratio


async def fetch_warehouse_metrics(*, pool: Any, test_id: str) -> dict[str, Any]:
    """Fetch test-level warehouse queueing + MCW metrics."""
    prefix = get_prefix()

    run_id_rows = await pool.execute_query(
        f"SELECT RUN_ID FROM {prefix}.TEST_RESULTS WHERE TEST_ID = ?",
        params=[test_id],
    )
    run_id = run_id_rows[0][0] if run_id_rows and run_id_rows[0] else None
    is_parent = bool(run_id) and str(run_id) == str(test_id)

    if is_parent:
        query = f"""
        SELECT
            MAX(CLUSTERS_USED) AS CLUSTERS_USED,
            SUM(TOTAL_QUEUED_OVERLOAD_MS) AS TOTAL_QUEUED_OVERLOAD_MS,
            SUM(TOTAL_QUEUED_PROVISIONING_MS) AS TOTAL_QUEUED_PROVISIONING_MS,
            SUM(QUERIES_WITH_OVERLOAD_QUEUE) AS QUERIES_WITH_OVERLOAD_QUEUE,
            AVG(READ_CACHE_HIT_PCT) AS READ_CACHE_HIT_PCT
        FROM {prefix}.V_WAREHOUSE_METRICS vm
        WHERE vm.TEST_ID IN (
            SELECT TEST_ID
            FROM {prefix}.TEST_RESULTS
            WHERE RUN_ID = ?
        )
        """
        rows = await pool.execute_query(query, params=[test_id])
    else:
        query = f"""
        SELECT
            CLUSTERS_USED,
            TOTAL_QUEUED_OVERLOAD_MS,
            TOTAL_QUEUED_PROVISIONING_MS,
            QUERIES_WITH_OVERLOAD_QUEUE,
            READ_CACHE_HIT_PCT
        FROM {prefix}.V_WAREHOUSE_METRICS
        WHERE TEST_ID = ?
        """
        rows = await pool.execute_query(query, params=[test_id])

    if not rows or rows[0][0] is None:
        return {"warehouse_metrics_available": False}

    (
        clusters_used,
        total_overload_ms,
        total_provisioning_ms,
        queries_with_overload_queue,
        read_cache_hit_pct,
    ) = rows[0]

    return {
        "warehouse_metrics_available": True,
        "warehouse_metrics": {
            "clusters_used": int(clusters_used or 0),
            "total_queued_overload_ms": to_float_or_none(total_overload_ms),
            "total_queued_provisioning_ms": to_float_or_none(total_provisioning_ms),
            "queries_with_overload_queue": int(queries_with_overload_queue or 0),
            "read_cache_hit_pct": to_float_or_none(read_cache_hit_pct),
        },
    }


async def fetch_postgres_stats(
    *,
    table_type: str,
    database: str | None,
) -> dict[str, Any]:
    """Fetch Postgres connection pool stats."""
    if not database:
        return {"postgres_stats_available": False}

    pg_pool = postgres_pool.get_pool_for_database(database, pool_type="default")
    stats = await pg_pool.get_pool_stats()

    max_connections = None
    active_connections = None
    try:
        max_connections = await pg_pool.fetch_val(
            "SELECT setting::int FROM pg_settings WHERE name = 'max_connections'"
        )
    except Exception:
        max_connections = None
    try:
        active_connections = await pg_pool.fetch_val(
            "SELECT COUNT(*) FROM pg_stat_activity WHERE datname = current_database()"
        )
    except Exception:
        active_connections = None

    return {
        "postgres_stats_available": True,
        "postgres_stats": {
            "pool": stats,
            "max_connections": int(max_connections) if max_connections is not None else None,
            "active_connections": int(active_connections) if active_connections is not None else None,
        },
    }


async def fetch_pg_enrichment(*, pool: Any, test_id: str) -> dict[str, Any]:
    """Fetch pg_stat_statements enrichment data for a Postgres test."""
    prefix = get_prefix()

    query = f"""
    SELECT
        PG_TOTAL_CALLS,
        PG_TOTAL_EXEC_TIME_MS,
        PG_MEAN_EXEC_TIME_MS,
        PG_CACHE_HIT_RATIO,
        PG_SHARED_BLKS_HIT,
        PG_SHARED_BLKS_READ,
        PG_ROWS_RETURNED,
        PG_QUERY_PATTERN_COUNT,
        PG_SHARED_BLK_READ_TIME_MS,
        PG_SHARED_BLK_WRITE_TIME_MS,
        PG_WAL_RECORDS,
        PG_WAL_BYTES,
        PG_TEMP_BLKS_READ,
        PG_TEMP_BLKS_WRITTEN,
        PG_STATS_BY_KIND,
        PG_STAT_STATEMENTS_AVAILABLE,
        PG_TRACK_IO_TIMING,
        PG_VERSION
    FROM {prefix}.TEST_RESULTS
    WHERE TEST_ID = ?
    """

    rows = await pool.execute_query(query, params=[test_id])

    if not rows:
        return {"pg_enrichment_available": False}

    row = rows[0]
    (
        total_calls,
        total_exec_time_ms,
        mean_exec_time_ms,
        cache_hit_ratio,
        shared_blks_hit,
        shared_blks_read,
        rows_returned,
        query_pattern_count,
        blk_read_time_ms,
        blk_write_time_ms,
        wal_records,
        wal_bytes,
        temp_blks_read,
        temp_blks_written,
        stats_by_kind,
        pg_stat_available,
        track_io_timing,
        pg_version,
    ) = row

    if pg_stat_available is None:
        return {"pg_enrichment_available": False}

    if not pg_stat_available:
        return {
            "pg_enrichment_available": False,
            "pg_capabilities": {
                "pg_stat_statements_available": False,
                "track_io_timing": bool(track_io_timing),
                "pg_version": pg_version,
            },
        }

    # Parse stats_by_kind JSON if present
    by_kind = None
    if stats_by_kind:
        try:
            import json
            by_kind = json.loads(stats_by_kind) if isinstance(stats_by_kind, str) else stats_by_kind
        except Exception:
            by_kind = None

    return {
        "pg_enrichment_available": True,
        "pg_enrichment": {
            "total_calls": int(total_calls or 0),
            "total_exec_time_ms": to_float_or_none(total_exec_time_ms),
            "mean_exec_time_ms": to_float_or_none(mean_exec_time_ms),
            "cache_hit_ratio": to_float_or_none(cache_hit_ratio),
            "cache_hit_pct": (
                round(float(cache_hit_ratio) * 100, 2)
                if cache_hit_ratio is not None
                else None
            ),
            "shared_blks_hit": int(shared_blks_hit or 0),
            "shared_blks_read": int(shared_blks_read or 0),
            "rows_returned": int(rows_returned or 0),
            "query_pattern_count": int(query_pattern_count or 0),
            "blk_read_time_ms": to_float_or_none(blk_read_time_ms),
            "blk_write_time_ms": to_float_or_none(blk_write_time_ms),
            "wal_records": int(wal_records or 0) if wal_records else None,
            "wal_bytes": int(wal_bytes or 0) if wal_bytes else None,
            "temp_blks_read": int(temp_blks_read or 0) if temp_blks_read else None,
            "temp_blks_written": int(temp_blks_written or 0) if temp_blks_written else None,
            "by_kind": by_kind,
        },
        "pg_capabilities": {
            "pg_stat_statements_available": True,
            "track_io_timing": bool(track_io_timing),
            "pg_version": pg_version,
        },
    }


async def fetch_cluster_breakdown(*, pool: Any, test_id: str) -> dict[str, Any]:
    """Fetch per-cluster breakdown for MCW tests."""
    prefix = get_prefix()

    run_id_rows = await pool.execute_query(
        f"SELECT RUN_ID FROM {prefix}.TEST_RESULTS WHERE TEST_ID = ?",
        params=[test_id],
    )
    run_id = run_id_rows[0][0] if run_id_rows and run_id_rows[0] else None
    is_parent = bool(run_id) and str(run_id) == str(test_id)

    if is_parent:
        query = f"""
        SELECT
            cb.CLUSTER_NUMBER,
            SUM(cb.QUERY_COUNT) AS QUERY_COUNT,
            AVG(cb.P50_EXEC_MS) AS P50_EXEC_MS,
            AVG(cb.P95_EXEC_MS) AS P95_EXEC_MS,
            MAX(cb.MAX_EXEC_MS) AS MAX_EXEC_MS,
            AVG(cb.AVG_QUEUED_OVERLOAD_MS) AS AVG_QUEUED_OVERLOAD_MS,
            AVG(cb.AVG_QUEUED_PROVISIONING_MS) AS AVG_QUEUED_PROVISIONING_MS,
            SUM(cb.POINT_LOOKUPS) AS POINT_LOOKUPS,
            SUM(cb.RANGE_SCANS) AS RANGE_SCANS,
            SUM(cb.INSERTS) AS INSERTS,
            SUM(cb.UPDATES) AS UPDATES
        FROM {prefix}.V_CLUSTER_BREAKDOWN cb
        WHERE cb.TEST_ID IN (
            SELECT TEST_ID
            FROM {prefix}.TEST_RESULTS
            WHERE RUN_ID = ?
              AND TEST_ID <> ?
        )
        GROUP BY cb.CLUSTER_NUMBER
        ORDER BY cb.CLUSTER_NUMBER
        """
        rows = await pool.execute_query(query, params=[test_id, test_id])
    else:
        query = f"""
        SELECT
            CLUSTER_NUMBER,
            QUERY_COUNT,
            P50_EXEC_MS,
            P95_EXEC_MS,
            MAX_EXEC_MS,
            AVG_QUEUED_OVERLOAD_MS,
            AVG_QUEUED_PROVISIONING_MS,
            POINT_LOOKUPS,
            RANGE_SCANS,
            INSERTS,
            UPDATES
        FROM {prefix}.V_CLUSTER_BREAKDOWN
        WHERE TEST_ID = ?
        ORDER BY CLUSTER_NUMBER
        """
        rows = await pool.execute_query(query, params=[test_id])

    if not rows:
        return {"cluster_breakdown_available": False}

    clusters = []
    for row in rows:
        (
            cluster_number,
            query_count,
            p50_exec_ms,
            p95_exec_ms,
            max_exec_ms,
            avg_queued_overload_ms,
            avg_queued_provisioning_ms,
            point_lookups,
            range_scans,
            inserts,
            updates,
        ) = row
        clusters.append({
            "cluster_number": int(cluster_number or 0),
            "query_count": int(query_count or 0),
            "p50_exec_ms": to_float_or_none(p50_exec_ms),
            "p95_exec_ms": to_float_or_none(p95_exec_ms),
            "max_exec_ms": to_float_or_none(max_exec_ms),
            "avg_queued_overload_ms": to_float_or_none(avg_queued_overload_ms),
            "avg_queued_provisioning_ms": to_float_or_none(avg_queued_provisioning_ms),
            "point_lookups": int(point_lookups or 0),
            "range_scans": int(range_scans or 0),
            "inserts": int(inserts or 0),
            "updates": int(updates or 0),
        })

    return {
        "cluster_breakdown_available": True,
        "cluster_breakdown": clusters,
    }
