"""
API routes for persisted test results and running tests.

UI endpoints:
- History: GET /api/tests
- Comparison search: GET /api/tests/search?q=...
- Query executions (drilldown): GET /api/tests/{test_id}/query-executions
- Re-run: POST /api/tests/{test_id}/rerun
- Delete: DELETE /api/tests/{test_id}
- Run template: POST /api/tests/from-template/{template_id}
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel

from backend.config import settings
from backend.connectors import postgres_pool, snowflake_pool
from backend.core.test_registry import registry
from backend.api.error_handling import http_exception

router = APIRouter()
logger = logging.getLogger(__name__)

_TXN_RE = re.compile(r"\btransaction\s+\d+\b", re.IGNORECASE)
_SF_QUERY_ID_PREFIX_RE = re.compile(
    r"(\(\s*\d{5}\s*\)\s*:)\s*[0-9a-zA-Z-]{12,}\s*:",
    re.IGNORECASE,
)
_SF_ERROR_PREFIX_RE = re.compile(r"^\s*(\d+)\s*\(\s*(\d{5})\s*\)", re.IGNORECASE)
_ABORTED_BECAUSE_RE = re.compile(
    r"\bwas\s+aborted\s+because\b\s*(.*?)(?:\.|$)", re.IGNORECASE
)
_SQL_COMPILATION_RE = re.compile(
    r"\bsql\s+compilation\s+error\b\s*:\s*(.*?)(?:\.|$)", re.IGNORECASE
)


def _error_reason(msg: str) -> str:
    """
    Extract a short, human-readable reason for UI summaries.

    Keep this low-cardinality and derived from the normalized message.
    """
    s = str(msg or "").strip()
    if not s:
        return ""

    m = _ABORTED_BECAUSE_RE.search(s)
    if m:
        return str(m.group(1) or "").strip()

    m = _SQL_COMPILATION_RE.search(s)
    if m:
        detail = str(m.group(1) or "").strip()
        return f"SQL compilation error: {detail}" if detail else "SQL compilation error"

    return ""


def _normalize_error_message(msg: Any) -> str:
    """
    Normalize error messages to reduce high-cardinality IDs in grouping.

    Example: lock errors often embed statement IDs and transaction numbers that would
    otherwise explode group counts.
    """
    s = str(msg or "").strip()
    if not s:
        return ""

    # Normalize the Snowflake query-id prefix that often appears between colons:
    # "000625 (57014): 01c1abde-...: Statement ..."
    s = _SF_QUERY_ID_PREFIX_RE.sub(r"\1 <query_id>:", s)

    # Normalize common Snowflake lock error noise while preserving table names.
    s = re.sub(
        r"Statement\s+'[^']+'", "Statement '<statement_id>'", s, flags=re.IGNORECASE
    )
    s = re.sub(
        r"Your statement\s+'[^']+'",
        "Your statement '<statement_id>'",
        s,
        flags=re.IGNORECASE,
    )
    s = _TXN_RE.sub("transaction <txn>", s)

    # Collapse whitespace/newlines for UI table readability.
    s = " ".join(s.split())
    return s


def _prefix() -> str:
    return f"{settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}"


def _to_float_or_none(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


async def _fetch_warehouse_metrics(*, pool: Any, test_id: str) -> dict[str, Any]:
    """
    Fetch test-level warehouse queueing + MCW metrics.

    Backward compatible: callers should catch exceptions (e.g. missing view/columns).
    """
    prefix = _prefix()
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
    if not rows:
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
            "total_queued_overload_ms": _to_float_or_none(total_overload_ms),
            "total_queued_provisioning_ms": _to_float_or_none(total_provisioning_ms),
            "queries_with_overload_queue": int(queries_with_overload_queue or 0),
            "read_cache_hit_pct": _to_float_or_none(read_cache_hit_pct),
        },
    }


async def _fetch_postgres_stats(
    *,
    table_type: str,
    database: str | None,
) -> dict[str, Any]:
    if not database:
        return {"postgres_stats_available": False}

    pool_type = (
        "snowflake_postgres"
        if str(table_type).upper() == "SNOWFLAKE_POSTGRES"
        else "default"
    )
    pg_pool = postgres_pool.get_pool_for_database(database, pool_type=pool_type)
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
            "max_connections": (
                int(max_connections) if max_connections is not None else None
            ),
            "active_connections": (
                int(active_connections) if active_connections is not None else None
            ),
        },
    }


async def _fetch_cluster_breakdown(*, pool: Any, test_id: str) -> dict[str, Any]:
    """
    Fetch per-cluster breakdown for MCW tests.

    Backward compatible: callers should catch exceptions (e.g. missing view/columns).
    """
    prefix = _prefix()
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
    ORDER BY CLUSTER_NUMBER ASC
    """
    rows = await pool.execute_query(query, params=[test_id])
    if not rows:
        return {"cluster_breakdown_available": False, "cluster_breakdown": []}

    out: list[dict[str, Any]] = []
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
        out.append(
            {
                "cluster_number": int(cluster_number),
                "query_count": int(query_count or 0),
                "p50_exec_ms": _to_float_or_none(p50_exec_ms),
                "p95_exec_ms": _to_float_or_none(p95_exec_ms),
                "max_exec_ms": _to_float_or_none(max_exec_ms),
                "avg_queued_overload_ms": _to_float_or_none(avg_queued_overload_ms),
                "avg_queued_provisioning_ms": _to_float_or_none(
                    avg_queued_provisioning_ms
                ),
                "point_lookups": int(point_lookups or 0),
                "range_scans": int(range_scans or 0),
                "inserts": int(inserts or 0),
                "updates": int(updates or 0),
            }
        )

    return {"cluster_breakdown_available": True, "cluster_breakdown": out}


async def _fetch_sf_execution_latency_summary(
    *, pool: Any, test_id: str
) -> dict[str, Any]:
    """
    Compute server-side SQL execution percentiles from QUERY_EXECUTIONS.

    This uses SF_EXECUTION_MS (INFORMATION_SCHEMA.QUERY_HISTORY.EXECUTION_TIME)
    which excludes client/network overhead.

    NOTE: This will raise if the underlying columns don't exist (e.g. older schema).
    Callers should catch and treat as "not available".
    """
    prefix = _prefix()

    # NOTE: Snowflake does not support FILTER(...) for ordered-set aggregates like
    # PERCENTILE_CONT the way we'd like. Use NULLing expressions (IFF) instead.
    # PERCENTILE_CONT ignores NULLs.

    # Reads are POINT_LOOKUP + RANGE_SCAN; writes are INSERT + UPDATE.
    query = f"""
    SELECT
        COUNT(*) AS SF_LATENCY_SAMPLE_COUNT,

        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY SF_EXECUTION_MS) AS SF_P50_LATENCY_MS,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY SF_EXECUTION_MS) AS SF_P95_LATENCY_MS,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY SF_EXECUTION_MS) AS SF_P99_LATENCY_MS,
        MIN(SF_EXECUTION_MS) AS SF_MIN_LATENCY_MS,
        MAX(SF_EXECUTION_MS) AS SF_MAX_LATENCY_MS,

        PERCENTILE_CONT(0.50) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND IN ('POINT_LOOKUP', 'RANGE_SCAN'), SF_EXECUTION_MS, NULL)
        ) AS SF_READ_P50_LATENCY_MS,
        PERCENTILE_CONT(0.95) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND IN ('POINT_LOOKUP', 'RANGE_SCAN'), SF_EXECUTION_MS, NULL)
        ) AS SF_READ_P95_LATENCY_MS,
        PERCENTILE_CONT(0.99) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND IN ('POINT_LOOKUP', 'RANGE_SCAN'), SF_EXECUTION_MS, NULL)
        ) AS SF_READ_P99_LATENCY_MS,
        MIN(IFF(QUERY_KIND IN ('POINT_LOOKUP', 'RANGE_SCAN'), SF_EXECUTION_MS, NULL))
            AS SF_READ_MIN_LATENCY_MS,
        MAX(IFF(QUERY_KIND IN ('POINT_LOOKUP', 'RANGE_SCAN'), SF_EXECUTION_MS, NULL))
            AS SF_READ_MAX_LATENCY_MS,

        PERCENTILE_CONT(0.50) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND IN ('INSERT', 'UPDATE'), SF_EXECUTION_MS, NULL)
        ) AS SF_WRITE_P50_LATENCY_MS,
        PERCENTILE_CONT(0.95) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND IN ('INSERT', 'UPDATE'), SF_EXECUTION_MS, NULL)
        ) AS SF_WRITE_P95_LATENCY_MS,
        PERCENTILE_CONT(0.99) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND IN ('INSERT', 'UPDATE'), SF_EXECUTION_MS, NULL)
        ) AS SF_WRITE_P99_LATENCY_MS,
        MIN(IFF(QUERY_KIND IN ('INSERT', 'UPDATE'), SF_EXECUTION_MS, NULL))
            AS SF_WRITE_MIN_LATENCY_MS,
        MAX(IFF(QUERY_KIND IN ('INSERT', 'UPDATE'), SF_EXECUTION_MS, NULL))
            AS SF_WRITE_MAX_LATENCY_MS,

        PERCENTILE_CONT(0.50) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'POINT_LOOKUP', SF_EXECUTION_MS, NULL)
        ) AS SF_POINT_LOOKUP_P50_LATENCY_MS,
        PERCENTILE_CONT(0.95) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'POINT_LOOKUP', SF_EXECUTION_MS, NULL)
        ) AS SF_POINT_LOOKUP_P95_LATENCY_MS,
        PERCENTILE_CONT(0.99) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'POINT_LOOKUP', SF_EXECUTION_MS, NULL)
        ) AS SF_POINT_LOOKUP_P99_LATENCY_MS,
        MIN(IFF(QUERY_KIND = 'POINT_LOOKUP', SF_EXECUTION_MS, NULL))
            AS SF_POINT_LOOKUP_MIN_LATENCY_MS,
        MAX(IFF(QUERY_KIND = 'POINT_LOOKUP', SF_EXECUTION_MS, NULL))
            AS SF_POINT_LOOKUP_MAX_LATENCY_MS,

        PERCENTILE_CONT(0.50) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'RANGE_SCAN', SF_EXECUTION_MS, NULL)
        ) AS SF_RANGE_SCAN_P50_LATENCY_MS,
        PERCENTILE_CONT(0.95) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'RANGE_SCAN', SF_EXECUTION_MS, NULL)
        ) AS SF_RANGE_SCAN_P95_LATENCY_MS,
        PERCENTILE_CONT(0.99) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'RANGE_SCAN', SF_EXECUTION_MS, NULL)
        ) AS SF_RANGE_SCAN_P99_LATENCY_MS,
        MIN(IFF(QUERY_KIND = 'RANGE_SCAN', SF_EXECUTION_MS, NULL))
            AS SF_RANGE_SCAN_MIN_LATENCY_MS,
        MAX(IFF(QUERY_KIND = 'RANGE_SCAN', SF_EXECUTION_MS, NULL))
            AS SF_RANGE_SCAN_MAX_LATENCY_MS,

        PERCENTILE_CONT(0.50) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'INSERT', SF_EXECUTION_MS, NULL)
        ) AS SF_INSERT_P50_LATENCY_MS,
        PERCENTILE_CONT(0.95) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'INSERT', SF_EXECUTION_MS, NULL)
        ) AS SF_INSERT_P95_LATENCY_MS,
        PERCENTILE_CONT(0.99) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'INSERT', SF_EXECUTION_MS, NULL)
        ) AS SF_INSERT_P99_LATENCY_MS,
        MIN(IFF(QUERY_KIND = 'INSERT', SF_EXECUTION_MS, NULL)) AS SF_INSERT_MIN_LATENCY_MS,
        MAX(IFF(QUERY_KIND = 'INSERT', SF_EXECUTION_MS, NULL)) AS SF_INSERT_MAX_LATENCY_MS,

        PERCENTILE_CONT(0.50) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'UPDATE', SF_EXECUTION_MS, NULL)
        ) AS SF_UPDATE_P50_LATENCY_MS,
        PERCENTILE_CONT(0.95) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'UPDATE', SF_EXECUTION_MS, NULL)
        ) AS SF_UPDATE_P95_LATENCY_MS,
        PERCENTILE_CONT(0.99) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'UPDATE', SF_EXECUTION_MS, NULL)
        ) AS SF_UPDATE_P99_LATENCY_MS,
        MIN(IFF(QUERY_KIND = 'UPDATE', SF_EXECUTION_MS, NULL)) AS SF_UPDATE_MIN_LATENCY_MS,
        MAX(IFF(QUERY_KIND = 'UPDATE', SF_EXECUTION_MS, NULL)) AS SF_UPDATE_MAX_LATENCY_MS
    FROM {prefix}.QUERY_EXECUTIONS
    WHERE TEST_ID = ?
      AND COALESCE(WARMUP, FALSE) = FALSE
      AND SUCCESS = TRUE
      AND SF_EXECUTION_MS IS NOT NULL
    """
    rows = await pool.execute_query(query, params=[test_id])
    if not rows:
        return {
            "sf_latency_available": False,
            "sf_latency_sample_count": 0,
        }

    r = rows[0]
    sample_count = int(r[0] or 0)
    payload = {
        "sf_latency_available": sample_count > 0,
        "sf_latency_sample_count": sample_count,
        "sf_p50_latency_ms": _to_float_or_none(r[1]),
        "sf_p95_latency_ms": _to_float_or_none(r[2]),
        "sf_p99_latency_ms": _to_float_or_none(r[3]),
        "sf_min_latency_ms": _to_float_or_none(r[4]),
        "sf_max_latency_ms": _to_float_or_none(r[5]),
        "sf_read_p50_latency_ms": _to_float_or_none(r[6]),
        "sf_read_p95_latency_ms": _to_float_or_none(r[7]),
        "sf_read_p99_latency_ms": _to_float_or_none(r[8]),
        "sf_read_min_latency_ms": _to_float_or_none(r[9]),
        "sf_read_max_latency_ms": _to_float_or_none(r[10]),
        "sf_write_p50_latency_ms": _to_float_or_none(r[11]),
        "sf_write_p95_latency_ms": _to_float_or_none(r[12]),
        "sf_write_p99_latency_ms": _to_float_or_none(r[13]),
        "sf_write_min_latency_ms": _to_float_or_none(r[14]),
        "sf_write_max_latency_ms": _to_float_or_none(r[15]),
        "sf_point_lookup_p50_latency_ms": _to_float_or_none(r[16]),
        "sf_point_lookup_p95_latency_ms": _to_float_or_none(r[17]),
        "sf_point_lookup_p99_latency_ms": _to_float_or_none(r[18]),
        "sf_point_lookup_min_latency_ms": _to_float_or_none(r[19]),
        "sf_point_lookup_max_latency_ms": _to_float_or_none(r[20]),
        "sf_range_scan_p50_latency_ms": _to_float_or_none(r[21]),
        "sf_range_scan_p95_latency_ms": _to_float_or_none(r[22]),
        "sf_range_scan_p99_latency_ms": _to_float_or_none(r[23]),
        "sf_range_scan_min_latency_ms": _to_float_or_none(r[24]),
        "sf_range_scan_max_latency_ms": _to_float_or_none(r[25]),
        "sf_insert_p50_latency_ms": _to_float_or_none(r[26]),
        "sf_insert_p95_latency_ms": _to_float_or_none(r[27]),
        "sf_insert_p99_latency_ms": _to_float_or_none(r[28]),
        "sf_insert_min_latency_ms": _to_float_or_none(r[29]),
        "sf_insert_max_latency_ms": _to_float_or_none(r[30]),
        "sf_update_p50_latency_ms": _to_float_or_none(r[31]),
        "sf_update_p95_latency_ms": _to_float_or_none(r[32]),
        "sf_update_p99_latency_ms": _to_float_or_none(r[33]),
        "sf_update_min_latency_ms": _to_float_or_none(r[34]),
        "sf_update_max_latency_ms": _to_float_or_none(r[35]),
    }
    return payload


class RunTemplateResponse(BaseModel):
    test_id: str
    dashboard_url: str


@router.post(
    "/from-template/{template_id}",
    response_model=RunTemplateResponse,
    status_code=status.HTTP_201_CREATED,
)
async def run_from_template(template_id: str) -> RunTemplateResponse:
    try:
        # Prepare only: do not start executing until explicitly started from the dashboard.
        running = await registry.start_from_template(template_id, auto_start=False)
        return RunTemplateResponse(
            test_id=running.test_id, dashboard_url=f"/dashboard/{running.test_id}"
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Template not found")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise http_exception("start test", e)


@router.post("/{test_id}/start", status_code=status.HTTP_202_ACCEPTED)
async def start_prepared_test(test_id: str) -> dict[str, Any]:
    try:
        running = await registry.start_prepared(test_id)
        return {"test_id": running.test_id, "status": running.status}
    except KeyError:
        raise HTTPException(status_code=404, detail="Test not found")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise http_exception("start prepared test", e)


@router.post("/{test_id}/stop", status_code=status.HTTP_202_ACCEPTED)
async def stop_test(test_id: str) -> dict[str, Any]:
    try:
        running = await registry.stop(test_id)
        return {"test_id": running.test_id, "status": running.status}
    except KeyError:
        raise HTTPException(status_code=404, detail="Test not found")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise http_exception("stop test", e)


@router.get("")
async def list_tests(
    page: int = 1,
    page_size: int = 20,
    table_type: str = "",
    warehouse_size: str = "",
    status_filter: str = Query("", alias="status"),
    date_range: str = "all",
    start_date: str = "",
    end_date: str = "",
) -> dict[str, Any]:
    try:
        pool = snowflake_pool.get_default_pool()

        where_clauses: list[str] = []
        params: list[Any] = []

        if table_type:
            where_clauses.append("TABLE_TYPE = ?")
            params.append(table_type.upper())
        if warehouse_size:
            where_clauses.append("WAREHOUSE_SIZE = ?")
            params.append(warehouse_size.upper())
        if status_filter:
            where_clauses.append("STATUS = ?")
            params.append(status_filter.upper())

        if date_range in {"today", "week", "month"}:
            days = {"today": 1, "week": 7, "month": 30}[date_range]
            where_clauses.append("START_TIME >= DATEADD(day, ?, CURRENT_TIMESTAMP())")
            params.append(-days)
        elif date_range == "custom":

            def _is_date(value: str) -> bool:
                return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", value or ""))

            if _is_date(start_date):
                where_clauses.append("START_TIME >= TO_DATE(?)")
                params.append(start_date)
            if _is_date(end_date):
                where_clauses.append("START_TIME < DATEADD(day, 1, TO_DATE(?))")
                params.append(end_date)

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        offset = max(page - 1, 0) * page_size
        query = f"""
        SELECT
            TEST_ID,
            TEST_NAME,
            TABLE_TYPE,
            WAREHOUSE_SIZE,
            START_TIME,
            QPS,
            P95_LATENCY_MS,
            P99_LATENCY_MS,
            ERROR_RATE,
            STATUS,
            CONCURRENT_CONNECTIONS,
            DURATION_SECONDS,
            FAILURE_REASON
        FROM {_prefix()}.TEST_RESULTS
        {where_sql}
        ORDER BY START_TIME DESC
        LIMIT ? OFFSET ?
        """
        rows = await pool.execute_query(query, params=[*params, page_size, offset])

        count_query = f"SELECT COUNT(*) FROM {_prefix()}.TEST_RESULTS {where_sql}"
        count_rows = await pool.execute_query(count_query, params=params)
        total = int(count_rows[0][0]) if count_rows else 0
        total_pages = max((total + page_size - 1) // page_size, 1)

        results = []
        for row in rows:
            (
                test_id,
                test_name,
                table_type_db,
                wh_size,
                created_at,
                ops,
                p95,
                p99,
                err_rate,
                status_db,
                concurrency,
                duration,
                failure_reason,
            ) = row

            # For running tests, get the current phase from the in-memory registry
            phase = None
            if status_db and str(status_db).upper() == "RUNNING":
                try:
                    running = await registry.get(test_id)
                    if running is not None and isinstance(running.last_payload, dict):
                        phase = running.last_payload.get("phase")
                except Exception:
                    pass

            results.append(
                {
                    "test_id": test_id,
                    "test_name": test_name,
                    "table_type": table_type_db,
                    "warehouse_size": wh_size,
                    "created_at": (created_at.isoformat() + "Z")
                    if hasattr(created_at, "isoformat")
                    else str(created_at),
                    "ops_per_sec": float(ops or 0),
                    "p95_latency": float(p95 or 0),
                    "p99_latency": float(p99 or 0),
                    "error_rate": float(err_rate or 0) * 100.0,
                    "status": status_db,
                    "phase": phase,
                    "concurrent_connections": int(concurrency or 0),
                    "duration": float(duration or 0),
                    "failure_reason": failure_reason,
                }
            )

        return {"results": results, "total_pages": total_pages}

    except Exception as e:
        raise http_exception("list tests", e)


@router.get("/search")
async def search_tests(q: str) -> dict[str, Any]:
    try:
        pool = snowflake_pool.get_default_pool()
        q_text = str(q or "").strip()
        if not q_text:
            return {"results": []}
        like = f"%{q_text.lower()}%"
        query = f"""
        SELECT
            TEST_ID,
            TEST_NAME,
            TABLE_TYPE,
            WAREHOUSE_SIZE,
            START_TIME,
            QPS,
            P50_LATENCY_MS,
            P95_LATENCY_MS,
            P99_LATENCY_MS,
            ERROR_RATE
        FROM {_prefix()}.TEST_RESULTS
        WHERE (
            LOWER(TEST_NAME) LIKE ?
            OR LOWER(SCENARIO_NAME) LIKE ?
            OR LOWER(TABLE_NAME) LIKE ?
            OR LOWER(WAREHOUSE) LIKE ?
            OR LOWER(WAREHOUSE_SIZE) LIKE ?
            OR LOWER(TABLE_TYPE) LIKE ?
            OR LOWER(NOTES) LIKE ?
            OR LOWER(TO_VARCHAR(TEST_CONFIG:"database")) LIKE ?
            OR LOWER(TO_VARCHAR(TEST_CONFIG:"schema")) LIKE ?
            OR LOWER(TO_VARCHAR(TEST_CONFIG:"table_name")) LIKE ?
        )
        ORDER BY START_TIME DESC
        LIMIT 25
        """
        rows = await pool.execute_query(query, params=[like] * 10)
        results = []
        for row in rows:
            (
                test_id,
                test_name,
                table_type_db,
                wh_size,
                created_at,
                ops,
                p50,
                p95,
                p99,
                err_rate,
            ) = row
            results.append(
                {
                    "test_id": test_id,
                    "test_name": test_name,
                    "table_type": table_type_db,
                    "warehouse_size": wh_size,
                    "created_at": (created_at.isoformat() + "Z")
                    if hasattr(created_at, "isoformat")
                    else str(created_at),
                    "ops_per_sec": float(ops or 0),
                    "p50_latency": float(p50 or 0),
                    "p95_latency": float(p95 or 0),
                    "p99_latency": float(p99 or 0),
                    "error_rate": float(err_rate or 0) * 100.0,
                    "duration": 0,
                }
            )
        return {"results": results}
    except Exception as e:
        raise http_exception("search tests", e)


@router.get("/{test_id}")
async def get_test(test_id: str) -> dict[str, Any]:
    try:
        pool = snowflake_pool.get_default_pool()

        # Workload mix helper fields (templates normalize to workload_type=CUSTOM).
        def _coerce_pct(v: Any) -> int:
            try:
                return int(float(v))
            except Exception:
                return 0

        def _pct_from_dict(d: Any, key: str) -> int:
            if not isinstance(d, dict):
                return 0
            return _coerce_pct(d.get(key) or 0)

        def _num_from_dict(d: Any, key: str) -> float:
            if not isinstance(d, dict):
                return -1.0
            try:
                v = d.get(key)
                if v is None:
                    return -1.0
                return float(v)
            except Exception:
                return -1.0

        def _pct_from_custom_queries(queries: Any) -> dict[str, int]:
            out = {"POINT_LOOKUP": 0, "RANGE_SCAN": 0, "INSERT": 0, "UPDATE": 0}
            if not queries:
                return out

            items = queries if isinstance(queries, list) else [queries]
            for q in items:
                if not isinstance(q, dict):
                    continue
                kind = str(q.get("query_kind") or "").upper()
                if kind in out:
                    out[kind] = _coerce_pct(q.get("weight_pct") or 0)
            return out

        query = f"""
        SELECT
            TEST_ID,
            TEST_NAME,
            SCENARIO_NAME,
            TABLE_NAME,
            TABLE_TYPE,
            WAREHOUSE,
            WAREHOUSE_SIZE,
            STATUS,
            START_TIME,
            END_TIME,
            DURATION_SECONDS,
            CONCURRENT_CONNECTIONS,
            TEST_CONFIG,
            TOTAL_OPERATIONS,
            READ_OPERATIONS,
            WRITE_OPERATIONS,
            FAILED_OPERATIONS,
            QPS,
            READS_PER_SECOND,
            WRITES_PER_SECOND,
            ROWS_READ,
            ROWS_WRITTEN,
            AVG_LATENCY_MS,
            P50_LATENCY_MS,
            P90_LATENCY_MS,
            P95_LATENCY_MS,
            P99_LATENCY_MS,
            MIN_LATENCY_MS,
            MAX_LATENCY_MS,
            READ_P50_LATENCY_MS,
            READ_P95_LATENCY_MS,
            READ_P99_LATENCY_MS,
            READ_MIN_LATENCY_MS,
            READ_MAX_LATENCY_MS,
            WRITE_P50_LATENCY_MS,
            WRITE_P95_LATENCY_MS,
            WRITE_P99_LATENCY_MS,
            WRITE_MIN_LATENCY_MS,
            WRITE_MAX_LATENCY_MS,
            POINT_LOOKUP_P50_LATENCY_MS,
            POINT_LOOKUP_P95_LATENCY_MS,
            POINT_LOOKUP_P99_LATENCY_MS,
            POINT_LOOKUP_MIN_LATENCY_MS,
            POINT_LOOKUP_MAX_LATENCY_MS,
            RANGE_SCAN_P50_LATENCY_MS,
            RANGE_SCAN_P95_LATENCY_MS,
            RANGE_SCAN_P99_LATENCY_MS,
            RANGE_SCAN_MIN_LATENCY_MS,
            RANGE_SCAN_MAX_LATENCY_MS,
            INSERT_P50_LATENCY_MS,
            INSERT_P95_LATENCY_MS,
            INSERT_P99_LATENCY_MS,
            INSERT_MIN_LATENCY_MS,
            INSERT_MAX_LATENCY_MS,
            UPDATE_P50_LATENCY_MS,
            UPDATE_P95_LATENCY_MS,
            UPDATE_P99_LATENCY_MS,
            UPDATE_MIN_LATENCY_MS,
            UPDATE_MAX_LATENCY_MS,
            QUERY_TAG,
            FIND_MAX_RESULT,
            FAILURE_REASON,
            ENRICHMENT_STATUS,
            ENRICHMENT_ERROR
        FROM {_prefix()}.TEST_RESULTS
        WHERE TEST_ID = ?
        """
        rows = await pool.execute_query(query, params=[test_id])
        if not rows:
            # Fallback to in-memory registry for freshly prepared tests that
            # haven't been persisted to results tables yet.
            running = await registry.get(test_id)
            if running is not None:
                cfg = running.template_config or {}
                pcts = _pct_from_custom_queries(running.scenario.custom_queries)
                phase = None
                if isinstance(running.last_payload, dict):
                    phase = running.last_payload.get("phase")
                load_mode = (
                    str(
                        getattr(running.scenario, "load_mode", "CONCURRENCY")
                        or "CONCURRENCY"
                    )
                    .strip()
                    .upper()
                )
                if load_mode not in {"CONCURRENCY", "QPS", "FIND_MAX_CONCURRENCY"}:
                    load_mode = "CONCURRENCY"
                table_type_u = (
                    str(
                        getattr(running.scenario.table_configs[0], "table_type", "")
                        or ""
                    )
                    .strip()
                    .upper()
                )
                is_postgres = table_type_u in {"POSTGRES", "SNOWFLAKE_POSTGRES"}
                table_full = (
                    f"{cfg.get('database')}.{cfg.get('schema')}.{cfg.get('table_name')}"
                    if cfg.get("database")
                    and cfg.get("schema")
                    and cfg.get("table_name")
                    else running.scenario.table_configs[0].name
                )
                payload = {
                    "test_id": test_id,
                    "query_tag": getattr(
                        running.executor, "_benchmark_query_tag", None
                    ),
                    "test_name": running.template_name,
                    "template_id": running.template_id,
                    "template_name": running.template_name,
                    "scenario_name": running.scenario.name,
                    "table_type": str(
                        running.scenario.table_configs[0].table_type
                    ).upper(),
                    "table_name": running.scenario.table_configs[0].name,
                    "table_full_name": table_full,
                    "warehouse": cfg.get("warehouse_name"),
                    "warehouse_size": cfg.get("warehouse_size"),
                    "status": running.status,
                    "phase": phase,
                    "start_time": running.created_at.isoformat(),
                    "end_time": None,
                    "duration_seconds": None
                    if load_mode == "FIND_MAX_CONCURRENCY"
                    else float(
                        (running.scenario.duration_seconds or 0)
                        + (running.scenario.warmup_seconds or 0)
                    ),
                    "concurrent_connections": int(
                        running.scenario.concurrent_connections or 0
                    ),
                    "load_mode": load_mode,
                    "target_qps": (
                        float(getattr(running.scenario, "target_qps", 0.0) or 0.0)
                        if load_mode == "QPS"
                        else None
                    ),
                    "min_concurrency": int(
                        getattr(running.scenario, "min_concurrency", 1) or 1
                    )
                    if load_mode == "QPS"
                    else None,
                    "qps_target_mode": "QPS" if load_mode == "QPS" else None,
                    "workload_type": str(running.scenario.workload_type),
                    "custom_point_lookup_pct": _pct_from_dict(
                        cfg, "custom_point_lookup_pct"
                    )
                    or pcts["POINT_LOOKUP"],
                    "custom_range_scan_pct": _pct_from_dict(
                        cfg, "custom_range_scan_pct"
                    )
                    or pcts["RANGE_SCAN"],
                    "custom_insert_pct": _pct_from_dict(cfg, "custom_insert_pct")
                    or pcts["INSERT"],
                    "custom_update_pct": _pct_from_dict(cfg, "custom_update_pct")
                    or pcts["UPDATE"],
                    # Targets (SLOs) from template config (if present).
                    "target_point_lookup_p95_latency_ms": float(
                        cfg.get("target_point_lookup_p95_latency_ms", -1)
                    ),
                    "target_range_scan_p95_latency_ms": float(
                        cfg.get("target_range_scan_p95_latency_ms", -1)
                    ),
                    "target_insert_p95_latency_ms": float(
                        cfg.get("target_insert_p95_latency_ms", -1)
                    ),
                    "target_update_p95_latency_ms": float(
                        cfg.get("target_update_p95_latency_ms", -1)
                    ),
                    "target_point_lookup_p99_latency_ms": float(
                        cfg.get("target_point_lookup_p99_latency_ms", -1)
                    ),
                    "target_range_scan_p99_latency_ms": float(
                        cfg.get("target_range_scan_p99_latency_ms", -1)
                    ),
                    "target_insert_p99_latency_ms": float(
                        cfg.get("target_insert_p99_latency_ms", -1)
                    ),
                    "target_update_p99_latency_ms": float(
                        cfg.get("target_update_p99_latency_ms", -1)
                    ),
                    "target_point_lookup_error_rate_pct": float(
                        cfg.get("target_point_lookup_error_rate_pct", -1)
                    ),
                    "target_range_scan_error_rate_pct": float(
                        cfg.get("target_range_scan_error_rate_pct", -1)
                    ),
                    "target_insert_error_rate_pct": float(
                        cfg.get("target_insert_error_rate_pct", -1)
                    ),
                    "target_update_error_rate_pct": float(
                        cfg.get("target_update_error_rate_pct", -1)
                    ),
                }
                if is_postgres:
                    try:
                        payload.update(
                            await _fetch_postgres_stats(
                                table_type=table_type_u,
                                database=cfg.get("database"),
                            )
                        )
                    except Exception as e:
                        logger.debug("Postgres stats unavailable: %s", e)
                return payload
            raise HTTPException(status_code=404, detail="Test not found")

        (
            _,
            test_name,
            scenario_name,
            table_name,
            table_type,
            warehouse,
            warehouse_size,
            status_db,
            start_time,
            end_time,
            duration_seconds,
            concurrency,
            test_config,
            total_operations,
            read_operations,
            write_operations,
            failed_operations,
            qps,
            reads_per_second,
            writes_per_second,
            rows_read,
            rows_written,
            avg_latency_ms,
            p50_latency_ms,
            p90_latency_ms,
            p95_latency_ms,
            p99_latency_ms,
            min_latency_ms,
            max_latency_ms,
            read_p50_latency_ms,
            read_p95_latency_ms,
            read_p99_latency_ms,
            read_min_latency_ms,
            read_max_latency_ms,
            write_p50_latency_ms,
            write_p95_latency_ms,
            write_p99_latency_ms,
            write_min_latency_ms,
            write_max_latency_ms,
            point_lookup_p50_latency_ms,
            point_lookup_p95_latency_ms,
            point_lookup_p99_latency_ms,
            point_lookup_min_latency_ms,
            point_lookup_max_latency_ms,
            range_scan_p50_latency_ms,
            range_scan_p95_latency_ms,
            range_scan_p99_latency_ms,
            range_scan_min_latency_ms,
            range_scan_max_latency_ms,
            insert_p50_latency_ms,
            insert_p95_latency_ms,
            insert_p99_latency_ms,
            insert_min_latency_ms,
            insert_max_latency_ms,
            update_p50_latency_ms,
            update_p95_latency_ms,
            update_p99_latency_ms,
            update_min_latency_ms,
            update_max_latency_ms,
            query_tag,
            find_max_result_raw,
            failure_reason,
            enrichment_status,
            enrichment_error,
        ) = rows[0]

        cfg = test_config
        if isinstance(cfg, str):
            cfg = json.loads(cfg)

        find_max_result = None
        if find_max_result_raw:
            if isinstance(find_max_result_raw, str):
                find_max_result = json.loads(find_max_result_raw)
            else:
                find_max_result = find_max_result_raw

        template_name = cfg.get("template_name") if isinstance(cfg, dict) else None
        template_id = cfg.get("template_id") if isinstance(cfg, dict) else None
        template_cfg = cfg.get("template_config") if isinstance(cfg, dict) else None
        workload_type = None
        if isinstance(template_cfg, dict):
            workload_type = template_cfg.get("workload_type")
        load_mode = "CONCURRENCY"
        if isinstance(template_cfg, dict):
            load_mode = (
                str(template_cfg.get("load_mode") or "CONCURRENCY").strip().upper()
                or "CONCURRENCY"
            )
        if load_mode not in {"CONCURRENCY", "QPS", "FIND_MAX_CONCURRENCY"}:
            load_mode = "CONCURRENCY"
        target_qps = None
        if load_mode == "QPS" and isinstance(template_cfg, dict):
            try:
                v = template_cfg.get("target_qps")
                target_qps = float(v) if v is not None else None
            except Exception:
                target_qps = None
        min_concurrency = None
        if load_mode == "QPS" and isinstance(template_cfg, dict):
            try:
                min_concurrency = int(float(template_cfg.get("min_concurrency") or 1))
            except Exception:
                min_concurrency = 1
        table_type_u = str(table_type or "").strip().upper()
        is_postgres = table_type_u in {"POSTGRES", "SNOWFLAKE_POSTGRES"}

        payload: dict[str, Any] = {
            "test_id": test_id,
            "query_tag": query_tag,
            "test_name": test_name,
            "template_id": template_id,
            "template_name": template_name,
            "scenario_name": scenario_name,
            "table_type": table_type,
            "table_name": table_name,
            "table_full_name": (
                f"{template_cfg.get('database')}.{template_cfg.get('schema')}.{template_cfg.get('table_name')}"
                if isinstance(template_cfg, dict)
                else table_name
            ),
            "warehouse": warehouse,
            "warehouse_size": warehouse_size,
            "status": status_db,
            "start_time": start_time.isoformat()
            if hasattr(start_time, "isoformat")
            else str(start_time),
            "end_time": end_time.isoformat()
            if end_time and hasattr(end_time, "isoformat")
            else None,
            "duration_seconds": float(duration_seconds or 0),
            "concurrent_connections": int(concurrency or 0),
            "load_mode": load_mode,
            "target_qps": target_qps if load_mode == "QPS" else None,
            "min_concurrency": min_concurrency if load_mode == "QPS" else None,
            "qps_target_mode": "QPS" if load_mode == "QPS" else None,
            "workload_type": workload_type,
            "custom_point_lookup_pct": _pct_from_dict(
                template_cfg, "custom_point_lookup_pct"
            ),
            "custom_range_scan_pct": _pct_from_dict(
                template_cfg, "custom_range_scan_pct"
            ),
            "custom_insert_pct": _pct_from_dict(template_cfg, "custom_insert_pct"),
            "custom_update_pct": _pct_from_dict(template_cfg, "custom_update_pct"),
            # Targets (SLOs) from template config.
            "target_point_lookup_p95_latency_ms": _num_from_dict(
                template_cfg, "target_point_lookup_p95_latency_ms"
            ),
            "target_range_scan_p95_latency_ms": _num_from_dict(
                template_cfg, "target_range_scan_p95_latency_ms"
            ),
            "target_insert_p95_latency_ms": _num_from_dict(
                template_cfg, "target_insert_p95_latency_ms"
            ),
            "target_update_p95_latency_ms": _num_from_dict(
                template_cfg, "target_update_p95_latency_ms"
            ),
            "target_point_lookup_p99_latency_ms": _num_from_dict(
                template_cfg, "target_point_lookup_p99_latency_ms"
            ),
            "target_range_scan_p99_latency_ms": _num_from_dict(
                template_cfg, "target_range_scan_p99_latency_ms"
            ),
            "target_insert_p99_latency_ms": _num_from_dict(
                template_cfg, "target_insert_p99_latency_ms"
            ),
            "target_update_p99_latency_ms": _num_from_dict(
                template_cfg, "target_update_p99_latency_ms"
            ),
            "target_point_lookup_error_rate_pct": _num_from_dict(
                template_cfg, "target_point_lookup_error_rate_pct"
            ),
            "target_range_scan_error_rate_pct": _num_from_dict(
                template_cfg, "target_range_scan_error_rate_pct"
            ),
            "target_insert_error_rate_pct": _num_from_dict(
                template_cfg, "target_insert_error_rate_pct"
            ),
            "target_update_error_rate_pct": _num_from_dict(
                template_cfg, "target_update_error_rate_pct"
            ),
            "total_operations": int(total_operations or 0),
            "read_operations": int(read_operations or 0),
            "write_operations": int(write_operations or 0),
            "failed_operations": int(failed_operations or 0),
            "qps": float(qps or 0),
            "reads_per_second": float(reads_per_second or 0),
            "writes_per_second": float(writes_per_second or 0),
            "rows_read": int(rows_read or 0),
            "rows_written": int(rows_written or 0),
            "avg_latency_ms": float(avg_latency_ms or 0),
            "p50_latency_ms": float(p50_latency_ms or 0),
            "p90_latency_ms": float(p90_latency_ms or 0),
            "p95_latency_ms": float(p95_latency_ms or 0),
            "p99_latency_ms": float(p99_latency_ms or 0),
            "min_latency_ms": float(min_latency_ms or 0),
            "max_latency_ms": float(max_latency_ms or 0),
            "read_p50_latency_ms": float(read_p50_latency_ms or 0),
            "read_p95_latency_ms": float(read_p95_latency_ms or 0),
            "read_p99_latency_ms": float(read_p99_latency_ms or 0),
            "read_min_latency_ms": float(read_min_latency_ms or 0),
            "read_max_latency_ms": float(read_max_latency_ms or 0),
            "write_p50_latency_ms": float(write_p50_latency_ms or 0),
            "write_p95_latency_ms": float(write_p95_latency_ms or 0),
            "write_p99_latency_ms": float(write_p99_latency_ms or 0),
            "write_min_latency_ms": float(write_min_latency_ms or 0),
            "write_max_latency_ms": float(write_max_latency_ms or 0),
            "point_lookup_p50_latency_ms": float(point_lookup_p50_latency_ms or 0),
            "point_lookup_p95_latency_ms": float(point_lookup_p95_latency_ms or 0),
            "point_lookup_p99_latency_ms": float(point_lookup_p99_latency_ms or 0),
            "point_lookup_min_latency_ms": float(point_lookup_min_latency_ms or 0),
            "point_lookup_max_latency_ms": float(point_lookup_max_latency_ms or 0),
            "range_scan_p50_latency_ms": float(range_scan_p50_latency_ms or 0),
            "range_scan_p95_latency_ms": float(range_scan_p95_latency_ms or 0),
            "range_scan_p99_latency_ms": float(range_scan_p99_latency_ms or 0),
            "range_scan_min_latency_ms": float(range_scan_min_latency_ms or 0),
            "range_scan_max_latency_ms": float(range_scan_max_latency_ms or 0),
            "insert_p50_latency_ms": float(insert_p50_latency_ms or 0),
            "insert_p95_latency_ms": float(insert_p95_latency_ms or 0),
            "insert_p99_latency_ms": float(insert_p99_latency_ms or 0),
            "insert_min_latency_ms": float(insert_min_latency_ms or 0),
            "insert_max_latency_ms": float(insert_max_latency_ms or 0),
            "update_p50_latency_ms": float(update_p50_latency_ms or 0),
            "update_p95_latency_ms": float(update_p95_latency_ms or 0),
            "update_p99_latency_ms": float(update_p99_latency_ms or 0),
            "update_min_latency_ms": float(update_min_latency_ms or 0),
            "update_max_latency_ms": float(update_max_latency_ms or 0),
            "find_max_result": find_max_result,
            "failure_reason": failure_reason,
            # Enrichment status (post-processing)
            "enrichment_status": enrichment_status,
            "enrichment_error": enrichment_error,
            "can_retry_enrichment": (
                str(status_db or "").upper() == "COMPLETED"
                and str(enrichment_status or "").upper() == "FAILED"
            ),
        }

        # Per-query-type error rates and counts (best-effort; derived from QUERY_EXECUTIONS).
        payload.update(
            {
                "point_lookup_error_rate_pct": None,
                "range_scan_error_rate_pct": None,
                "insert_error_rate_pct": None,
                "update_error_rate_pct": None,
                "point_lookup_count": 0,
                "range_scan_count": 0,
                "insert_count": 0,
                "update_count": 0,
            }
        )
        try:
            status_upper = str(status_db or "").upper()
            if status_upper in {"COMPLETED", "FAILED", "STOPPED", "CANCELLED"}:
                err_rows = await pool.execute_query(
                    f"""
                    SELECT QUERY_KIND, COUNT(*) AS N, SUM(IFF(SUCCESS, 0, 1)) AS ERR
                    FROM {_prefix()}.QUERY_EXECUTIONS
                    WHERE TEST_ID = ?
                      AND COALESCE(WARMUP, FALSE) = FALSE
                    GROUP BY QUERY_KIND
                    """,
                    params=[test_id],
                )
                err_key_map = {
                    "POINT_LOOKUP": "point_lookup_error_rate_pct",
                    "RANGE_SCAN": "range_scan_error_rate_pct",
                    "INSERT": "insert_error_rate_pct",
                    "UPDATE": "update_error_rate_pct",
                }
                count_key_map = {
                    "POINT_LOOKUP": "point_lookup_count",
                    "RANGE_SCAN": "range_scan_count",
                    "INSERT": "insert_count",
                    "UPDATE": "update_count",
                }
                for kind, n, err in err_rows:
                    k = str(kind or "").upper()
                    err_key = err_key_map.get(k)
                    count_key = count_key_map.get(k)
                    if not err_key:
                        continue
                    denom = float(n or 0)
                    numer = float(err or 0)
                    payload[err_key] = (numer / denom * 100.0) if denom > 0 else 0.0
                    if count_key:
                        payload[count_key] = int(n or 0)
        except Exception as e:
            logger.debug(
                "Failed to compute per-kind error rates for %s: %s", test_id, e
            )

        # Best-effort SQL-execution latency summaries (may be missing for running tests,
        # cancelled runs, or older schemas without SF_* columns).
        try:
            payload.update(
                await _fetch_sf_execution_latency_summary(pool=pool, test_id=test_id)
            )
        except Exception as e:
            logger.debug(
                "Failed to compute SF execution latency summary for test %s: %s",
                test_id,
                e,
            )
            payload.update(
                {"sf_latency_available": False, "sf_latency_sample_count": 0}
            )

        # Best-effort: warehouse queueing + MCW breakdown (requires query-history enrichment).
        try:
            payload.update(await _fetch_warehouse_metrics(pool=pool, test_id=test_id))
        except Exception as e:
            logger.debug(
                "Failed to fetch warehouse metrics for test %s: %s", test_id, e
            )
            payload.update({"warehouse_metrics_available": False})

        try:
            payload.update(await _fetch_cluster_breakdown(pool=pool, test_id=test_id))
        except Exception as e:
            logger.debug(
                "Failed to fetch cluster breakdown for test %s: %s", test_id, e
            )
            payload.update(
                {"cluster_breakdown_available": False, "cluster_breakdown": []}
            )

        if is_postgres:
            try:
                payload.update(
                    await _fetch_postgres_stats(
                        table_type=table_type,
                        database=template_cfg.get("database")
                        if isinstance(template_cfg, dict)
                        else None,
                    )
                )
            except Exception as e:
                logger.debug("Postgres stats unavailable for %s: %s", test_id, e)

        # If this test is still tracked in-memory, expose the latest execution phase
        # so the history dashboard can stay consistent with the live dashboard
        # (e.g., show PROCESSING until post-processing completes).
        try:
            running = await registry.get(test_id)
            if running is not None and isinstance(running.last_payload, dict):
                phase = running.last_payload.get("phase")
                if phase:
                    payload["phase"] = phase
            # Also prefer in-memory status for in-progress cancellation. The DB row
            # can remain RUNNING until the runner finalizes, but the UI needs to show
            # CANCELLING immediately after the user clicks Stop.
            if running is not None:
                status_live = str(getattr(running, "status", "") or "").upper()
                if status_live:
                    payload["status"] = status_live
        except Exception:
            # Best-effort only; never fail the API response due to registry issues.
            pass

        return payload
    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("get test", e)


@router.get("/{test_id}/query-executions")
async def list_query_executions(
    test_id: str,
    kinds: str = "",
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    sort: str = "sf_execution_ms",
    direction: str = "desc",
) -> dict[str, Any]:
    """
    List persisted per-operation query executions for a test.

    Defaults match the percentile calculations:
    - Excludes warmup operations
    - Includes only successful operations

    Query params:
    - kinds: comma-separated QUERY_KIND list (e.g. POINT_LOOKUP,RANGE_SCAN)
    - page, page_size: pagination
    - sort: one of [sf_execution_ms, app_elapsed_ms, start_time]
    - direction: asc|desc
    """
    try:
        pool = snowflake_pool.get_default_pool()
        prefix = _prefix()

        where_clauses: list[str] = [
            "TEST_ID = ?",
            "COALESCE(WARMUP, FALSE) = FALSE",
            "SUCCESS = TRUE",
        ]
        params: list[Any] = [test_id]

        kind_list = [
            k.strip().upper()
            for k in (kinds or "").split(",")
            if k is not None and k.strip()
        ]
        if kind_list:
            where_clauses.append(f"QUERY_KIND IN ({', '.join(['?'] * len(kind_list))})")
            params.extend(kind_list)

        sort_map = {
            "sf_execution_ms": "SF_EXECUTION_MS",
            "app_elapsed_ms": "APP_ELAPSED_MS",
            "start_time": "START_TIME",
        }
        sort_key = (sort or "").strip().lower()
        sort_col = sort_map.get(sort_key)
        if not sort_col:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid sort '{sort}'. Must be one of {sorted(sort_map.keys())}.",
            )

        dir_key = (direction or "").strip().lower()
        if dir_key not in {"asc", "desc"}:
            raise HTTPException(
                status_code=400, detail="Invalid direction. Must be 'asc' or 'desc'."
            )
        dir_sql = dir_key.upper()

        where_sql = "WHERE " + " AND ".join(where_clauses)
        offset = max(page - 1, 0) * page_size

        async def _run_query(*, extended: bool) -> list[Any]:
            cols_sql = """
                EXECUTION_ID,
                QUERY_ID,
                QUERY_KIND,
                START_TIME,
                END_TIME,
                DURATION_MS,
                APP_ELAPSED_MS,
                SF_EXECUTION_MS,
                ROWS_AFFECTED,
                WAREHOUSE
            """
            if extended:
                cols_sql += """,
                SF_CLUSTER_NUMBER,
                SF_QUEUED_OVERLOAD_MS,
                SF_QUEUED_PROVISIONING_MS,
                SF_PCT_SCANNED_FROM_CACHE
                """

            query = f"""
            SELECT
                {cols_sql}
            FROM {prefix}.QUERY_EXECUTIONS
            {where_sql}
            ORDER BY {sort_col} {dir_sql} NULLS LAST, START_TIME DESC
            LIMIT ? OFFSET ?
            """
            return await pool.execute_query(query, params=[*params, page_size, offset])

        try:
            rows = await _run_query(extended=True)
            extended = True
        except Exception as e:
            # Backward-compat: older schemas won't have SF_* columns.
            msg = str(e).lower()
            if (
                "invalid identifier" in msg
                or "unknown column" in msg
                or "does not exist" in msg
            ):
                rows = await _run_query(extended=False)
                extended = False
            else:
                raise

        count_query = f"SELECT COUNT(*) FROM {prefix}.QUERY_EXECUTIONS {where_sql}"
        count_rows = await pool.execute_query(count_query, params=params)
        total = int(count_rows[0][0]) if count_rows else 0
        total_pages = max((total + page_size - 1) // page_size, 1)

        results: list[dict[str, Any]] = []
        for row in rows:
            if extended:
                (
                    execution_id,
                    query_id,
                    query_kind,
                    start_time,
                    end_time,
                    duration_ms,
                    app_elapsed_ms,
                    sf_execution_ms,
                    rows_affected,
                    warehouse,
                    sf_cluster_number,
                    sf_queued_overload_ms,
                    sf_queued_provisioning_ms,
                    sf_pct_scanned_from_cache,
                ) = row
            else:
                (
                    execution_id,
                    query_id,
                    query_kind,
                    start_time,
                    end_time,
                    duration_ms,
                    app_elapsed_ms,
                    sf_execution_ms,
                    rows_affected,
                    warehouse,
                ) = row
                sf_cluster_number = None
                sf_queued_overload_ms = None
                sf_queued_provisioning_ms = None
                sf_pct_scanned_from_cache = None

            results.append(
                {
                    "execution_id": execution_id,
                    "query_id": query_id,
                    "query_kind": query_kind,
                    "start_time": start_time.isoformat()
                    if hasattr(start_time, "isoformat")
                    else str(start_time),
                    "end_time": end_time.isoformat()
                    if hasattr(end_time, "isoformat")
                    else str(end_time),
                    "duration_ms": _to_float_or_none(duration_ms),
                    "app_elapsed_ms": _to_float_or_none(app_elapsed_ms),
                    "sf_execution_ms": _to_float_or_none(sf_execution_ms),
                    "rows_affected": int(rows_affected)
                    if rows_affected is not None
                    else None,
                    "warehouse": warehouse,
                    "sf_cluster_number": int(sf_cluster_number)
                    if sf_cluster_number is not None
                    else None,
                    "sf_queued_overload_ms": _to_float_or_none(sf_queued_overload_ms),
                    "sf_queued_provisioning_ms": _to_float_or_none(
                        sf_queued_provisioning_ms
                    ),
                    "sf_pct_scanned_from_cache": _to_float_or_none(
                        sf_pct_scanned_from_cache
                    ),
                }
            )

        return {"results": results, "total_pages": total_pages}
    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("list query executions", e)


class ErrorSummaryRow(BaseModel):
    source: str
    summary: str
    message: str
    count: int


class ErrorSummaryResponse(BaseModel):
    test_id: str
    query_tag: str | None = None
    available: bool = True
    rows: list[ErrorSummaryRow] = []


@router.get("/{test_id}/error-summary", response_model=ErrorSummaryResponse)
async def get_error_summary(test_id: str) -> ErrorSummaryResponse:
    """
    Aggregate error signatures for a test:
    - Snowflake: INFORMATION_SCHEMA.QUERY_HISTORY filtered by TEST_RESULTS.QUERY_TAG
    - App/local: QUERY_EXECUTIONS rows where SUCCESS=FALSE and QUERY_ID starts with 'LOCAL_'
    """
    try:
        pool = snowflake_pool.get_default_pool()
        prefix = _prefix()

        # Fetch query_tag + time bounds for a tight query_history scan window.
        query = f"""
        SELECT QUERY_TAG, START_TIME, END_TIME
        FROM {prefix}.TEST_RESULTS
        WHERE TEST_ID = ?
        """
        rows = await pool.execute_query(query, params=[test_id])
        if not rows:
            # Best-effort: allow this endpoint for running tests (registry) too.
            running = await registry.get(test_id)
            qtag = (
                getattr(
                    getattr(running, "executor", None), "_benchmark_query_tag", None
                )
                if running
                else None
            )
            return ErrorSummaryResponse(
                test_id=test_id, query_tag=qtag, available=False, rows=[]
            )

        (query_tag, start_time, end_time) = rows[0]
        qtag = str(query_tag or "").strip() or None
        if not qtag:
            return ErrorSummaryResponse(
                test_id=test_id, query_tag=None, available=False, rows=[]
            )

        # Build a bounded time window around the test.
        start_iso = None
        end_iso = None
        try:
            start_dt = start_time if hasattr(start_time, "isoformat") else None
            end_dt = end_time if hasattr(end_time, "isoformat") else None
            if start_dt is not None and end_dt is not None and end_dt < start_dt:
                # Defensive: timestamps can drift due to NTZ/local conversions. Ensure we
                # always query a valid window.
                start_dt, end_dt = end_dt, start_dt
            if start_dt is not None:
                start_iso = start_dt.isoformat()
            if end_dt is not None:
                end_iso = end_dt.isoformat()
        except Exception:
            start_iso = None
            end_iso = None

        # Default fallback window (last 12h) if timestamps are unavailable.
        time_window_sql = ""
        time_params: list[Any] = []
        if start_iso and end_iso:
            time_window_sql = """
              END_TIME_RANGE_START => DATEADD('hour', -1, TO_TIMESTAMP_NTZ(?)::TIMESTAMP_LTZ),
              END_TIME_RANGE_END => DATEADD('hour', 1, TO_TIMESTAMP_NTZ(?)::TIMESTAMP_LTZ),
            """
            time_params = [start_iso, end_iso]
        elif start_iso:
            time_window_sql = """
              END_TIME_RANGE_START => DATEADD('hour', -1, TO_TIMESTAMP_NTZ(?)::TIMESTAMP_LTZ),
              END_TIME_RANGE_END => DATEADD('hour', 6, TO_TIMESTAMP_NTZ(?)::TIMESTAMP_LTZ),
            """
            time_params = [start_iso, start_iso]
        else:
            time_window_sql = """
              END_TIME_RANGE_START => DATEADD('hour', -12, CURRENT_TIMESTAMP()),
              END_TIME_RANGE_END => CURRENT_TIMESTAMP(),
            """

        # 1) Snowflake-side errors (QUERY_HISTORY)
        sf_query = f"""
        SELECT
          QUERY_TYPE,
          ERROR_CODE,
          ERROR_MESSAGE,
          COUNT(*) AS N
        FROM TABLE(
          INFORMATION_SCHEMA.QUERY_HISTORY(
            {time_window_sql}
            RESULT_LIMIT => 10000
          )
        )
        WHERE QUERY_TAG = ?
          AND COALESCE(ERROR_CODE, 0) <> 0
        GROUP BY 1, 2, 3
        ORDER BY N DESC
        LIMIT 1000
        """
        sf_rows = await pool.execute_query(sf_query, params=[*time_params, qtag])
        if not sf_rows:
            # Defensive fallback: if timestamp bounds are inconsistent (e.g. NTZ drift) or
            # the bounded window misses rows, fall back to a simple recent scan.
            sf_query_fallback = """
            SELECT
              QUERY_TYPE,
              ERROR_CODE,
              ERROR_MESSAGE,
              COUNT(*) AS N
            FROM TABLE(
              INFORMATION_SCHEMA.QUERY_HISTORY(
                END_TIME_RANGE_START => DATEADD('hour', -12, CURRENT_TIMESTAMP()),
                END_TIME_RANGE_END => CURRENT_TIMESTAMP(),
                RESULT_LIMIT => 10000
              )
            )
            WHERE QUERY_TAG = ?
              AND (COALESCE(ERROR_CODE, 0) <> 0 OR NULLIF(TRIM(ERROR_MESSAGE), '') IS NOT NULL)
            GROUP BY 1, 2, 3
            ORDER BY N DESC
            LIMIT 1000
            """
            sf_rows = await pool.execute_query(sf_query_fallback, params=[qtag])

        sf_available = bool(sf_rows)

        # 2) App/local errors (QUERY_EXECUTIONS, LOCAL_ only to avoid double-counting Snowflake errors)
        app_query = f"""
        SELECT
          COALESCE(WARMUP, FALSE) AS WARMUP,
          COALESCE(QUERY_KIND, '(NULL)') AS QUERY_KIND,
          ERROR,
          COUNT(*) AS N
        FROM {prefix}.QUERY_EXECUTIONS
        WHERE TEST_ID = ?
          AND SUCCESS = FALSE
          AND QUERY_ID LIKE 'LOCAL\\_%'
        GROUP BY 1, 2, 3
        ORDER BY N DESC
        LIMIT 1000
        """
        app_rows = await pool.execute_query(app_query, params=[test_id])

        # Normalize + aggregate in Python to collapse statement IDs/txns.
        agg: dict[tuple[str, str, str], int] = {}

        for qt, code, msg, n in sf_rows:
            qt_s = str(qt or "").strip().upper() or "(NULL)"
            code_i = int(code or 0)
            msg_norm = _normalize_error_message(msg)[:500]
            reason = _error_reason(msg_norm)
            summary = f"SNOWFLAKE {qt_s} ERROR_CODE={code_i}" + (
                f" - {reason}" if reason else ""
            )
            key = ("SNOWFLAKE", summary, msg_norm)
            agg[key] = int(agg.get(key, 0)) + int(n or 0)

        for warmup, kind, msg, n in app_rows:
            kind_s = str(kind or "").strip().upper() or "(NULL)"
            phase = "WARMUP" if bool(warmup) else "RUN"
            msg_s = str(msg or "")
            msg_norm = _normalize_error_message(msg_s)[:500]

            m = _SF_ERROR_PREFIX_RE.search(msg_s)
            is_sf = bool(m)
            if is_sf and sf_available:
                # If QUERY_HISTORY is available, avoid double-counting Snowflake errors.
                continue

            if is_sf and m:
                code_i = int(m.group(1))
                sqlstate = m.group(2)
                source = "SNOWFLAKE"
                reason = _error_reason(msg_norm)
                summary = (
                    f"SNOWFLAKE {kind_s} ERROR_CODE={code_i} SQLSTATE={sqlstate} ({phase})"
                    + (f" - {reason}" if reason else "")
                )
            else:
                source = "APP"
                reason = _error_reason(msg_norm)
                summary = f"APP {kind_s} ({phase})" + (f" - {reason}" if reason else "")

            key = (source, summary, msg_norm)
            agg[key] = int(agg.get(key, 0)) + int(n or 0)

        out_rows: list[ErrorSummaryRow] = []
        for (source, summary, msg_norm), n in agg.items():
            out_rows.append(
                ErrorSummaryRow(
                    source=source,
                    summary=summary,
                    message=msg_norm or "(no message)",
                    count=int(n),
                )
            )
        out_rows.sort(key=lambda r: int(r.count), reverse=True)

        return ErrorSummaryResponse(
            test_id=test_id,
            query_tag=qtag,
            available=True,
            rows=out_rows[:200],
        )
    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("get error summary", e)


@router.get("/{test_id}/logs")
async def get_test_logs(
    test_id: str,
    limit: int = Query(500, ge=1, le=2000),
    offset: int = Query(0, ge=0),
) -> dict[str, Any]:
    """
    Fetch persisted per-test logs (and in-memory logs for running tests).
    """
    try:
        # Prefer in-memory logs for running/prepared tests so refreshes don't lose context.
        running = await registry.get(test_id)
        if running is not None and running.log_buffer:
            logs = list(running.log_buffer)
            logs.sort(key=lambda r: int(r.get("seq") or 0))
            return {"test_id": test_id, "logs": logs[offset : offset + limit]}

        pool = snowflake_pool.get_default_pool()
        query = f"""
        SELECT
            LOG_ID,
            TEST_ID,
            SEQ,
            TIMESTAMP,
            LEVEL,
            LOGGER,
            MESSAGE,
            EXCEPTION
        FROM {_prefix()}.TEST_LOGS
        WHERE TEST_ID = ?
        ORDER BY SEQ ASC
        LIMIT ? OFFSET ?
        """
        rows = await pool.execute_query(query, params=[test_id, limit, offset])

        logs: list[dict[str, Any]] = []
        for row in rows:
            (
                log_id,
                test_id_db,
                seq,
                ts,
                level,
                logger_name,
                message,
                exc,
            ) = row
            logs.append(
                {
                    "kind": "log",
                    "log_id": log_id,
                    "test_id": test_id_db,
                    "seq": int(seq or 0),
                    "timestamp": ts.isoformat()
                    if hasattr(ts, "isoformat")
                    else str(ts),
                    "level": level,
                    "logger": logger_name,
                    "message": message,
                    "exception": exc,
                }
            )

        return {"test_id": test_id, "logs": logs}
    except Exception as e:
        # If logs table isn't present yet, or any query fails, degrade gracefully
        # for the dashboard rather than hard-erroring.
        msg = str(e).lower()
        if "does not exist" in msg or "unknown table" in msg:
            return {"test_id": test_id, "logs": []}
        raise http_exception("get test logs", e)


@router.get("/{test_id}/metrics")
async def get_test_metrics(test_id: str) -> dict[str, Any]:
    """
    Fetch historical time-series metrics snapshots for a completed test.
    This is used to populate charts in the dashboard for historical tests.
    """
    try:
        pool = snowflake_pool.get_default_pool()
        query = f"""
        SELECT
            TIMESTAMP,
            ELAPSED_SECONDS,
            QPS,
            P50_LATENCY_MS,
            P95_LATENCY_MS,
            P99_LATENCY_MS,
            ACTIVE_CONNECTIONS,
            TARGET_WORKERS,
            CUSTOM_METRICS
        FROM {_prefix()}.METRICS_SNAPSHOTS
        WHERE TEST_ID = ?
        ORDER BY TIMESTAMP ASC
        """
        rows = await pool.execute_query(query, params=[test_id])

        snapshots = []
        for row in rows:
            (
                timestamp,
                elapsed,
                ops_per_sec,
                p50,
                p95,
                p99,
                active_connections,
                target_workers,
                custom_metrics,
            ) = row

            # Optional: attach Snowflake server-side concurrency series (captured in
            # METRICS_SNAPSHOTS.CUSTOM_METRICS by the executor).
            #
            # Older runs may have null/empty custom metrics; degrade gracefully.
            cm: Any = custom_metrics
            if isinstance(cm, str):
                try:
                    cm = json.loads(cm)
                except Exception:
                    cm = {}
            if cm is None:
                cm = {}
            sf_bench: dict[str, Any] = {}
            app_ops: dict[str, Any] = {}
            warehouse: dict[str, Any] = {}
            if isinstance(cm, dict):
                maybe_sf = cm.get("sf_bench")
                if isinstance(maybe_sf, dict):
                    sf_bench = maybe_sf
                maybe_app = cm.get("app_ops_breakdown")
                if isinstance(maybe_app, dict):
                    app_ops = maybe_app
                maybe_wh = cm.get("warehouse")
                if isinstance(maybe_wh, dict):
                    warehouse = maybe_wh

            def _to_int(v: Any) -> int:
                try:
                    return int(v or 0)
                except Exception:
                    return 0

            snapshots.append(
                {
                    "timestamp": timestamp.isoformat()
                    if hasattr(timestamp, "isoformat")
                    else str(timestamp),
                    "elapsed_seconds": float(elapsed or 0),
                    "ops_per_sec": float(ops_per_sec or 0),
                    "p50_latency": float(p50 or 0),
                    "p95_latency": float(p95 or 0),
                    "p99_latency": float(p99 or 0),
                    # Client-side in-flight operations (connection pool checkouts).
                    # Useful for Postgres and as a general concurrency signal.
                    "active_connections": _to_int(active_connections),
                    # Target workers (desired concurrency from controller).
                    "target_workers": _to_int(target_workers),
                    # Snowflake server-side RUNNING concurrency (best-effort).
                    "sf_running": _to_int(sf_bench.get("running")),
                    "sf_running_read": _to_int(sf_bench.get("running_read")),
                    "sf_running_write": _to_int(sf_bench.get("running_write")),
                    "sf_running_point_lookup": _to_int(
                        sf_bench.get("running_point_lookup")
                    ),
                    "sf_running_range_scan": _to_int(
                        sf_bench.get("running_range_scan")
                    ),
                    "sf_running_insert": _to_int(sf_bench.get("running_insert")),
                    "sf_running_update": _to_int(sf_bench.get("running_update")),
                    "sf_running_tagged": _to_int(sf_bench.get("running_tagged")),
                    "sf_running_other": _to_int(sf_bench.get("running_other")),
                    # Warehouse-level queued counts (aligned with Snowsight warehouse monitoring).
                    "sf_queued": _to_int(warehouse.get("queued")),
                    # Per-test queued query count from QUERY_HISTORY (best-effort).
                    "sf_queued_bench": _to_int(sf_bench.get("queued")),
                    # Per-test blocked query count from QUERY_HISTORY (lock contention).
                    "sf_blocked": _to_int(sf_bench.get("blocked")),
                    # App-side QPS breakdown (from internal counters).
                    "app_point_lookup_ops_sec": float(
                        app_ops.get("point_lookup_ops_sec") or 0
                    ),
                    "app_range_scan_ops_sec": float(
                        app_ops.get("range_scan_ops_sec") or 0
                    ),
                    "app_insert_ops_sec": float(app_ops.get("insert_ops_sec") or 0),
                    "app_update_ops_sec": float(app_ops.get("update_ops_sec") or 0),
                    "app_read_ops_sec": float(app_ops.get("read_ops_sec") or 0),
                    "app_write_ops_sec": float(app_ops.get("write_ops_sec") or 0),
                }
            )

        return {"test_id": test_id, "snapshots": snapshots, "count": len(snapshots)}
    except Exception as e:
        raise http_exception("get test metrics", e)


@router.get("/{test_id}/warehouse-timeseries")
async def get_warehouse_timeseries(test_id: str) -> dict[str, Any]:
    """
    Fetch per-second warehouse timeseries for a completed test.

    This is derived from QUERY_EXECUTIONS (post-processed) and expanded to a
    per-second timeline based on TEST_RESULTS.START_TIME + TEST_RESULTS.DURATION_SECONDS.

    Falls back to real-time METRICS_SNAPSHOTS cluster data when QUERY_HISTORY
    enrichment is incomplete (which happens due to ~45s latency in QUERY_HISTORY).
    """
    try:
        pool = snowflake_pool.get_default_pool()
        prefix = _prefix()

        query = f"""
        WITH tr AS (
            SELECT
                TEST_ID,
                DATE_TRUNC('second', START_TIME) AS START_SECOND,
                COALESCE(DURATION_SECONDS, DATEDIFF('second', START_TIME, END_TIME)) AS DURATION_SECONDS
            FROM {prefix}.TEST_RESULTS
            WHERE TEST_ID = ?
        ),
        seconds AS (
            SELECT
                DATEADD('second', g.SEQ, tr.START_SECOND) AS SECOND,
                g.SEQ AS ELAPSED_SECONDS
            FROM tr
            JOIN (
                SELECT SEQ4() AS SEQ
                FROM TABLE(GENERATOR(ROWCOUNT => 86400))
            ) g
              ON g.SEQ <= COALESCE(tr.DURATION_SECONDS, 0)
        ),
        realtime_clusters AS (
            SELECT
                DATE_TRUNC('second', TIMESTAMP) AS SECOND,
                MAX(COALESCE(CUSTOM_METRICS:warehouse:started_clusters::INTEGER, 0)) AS STARTED_CLUSTERS
            FROM {prefix}.METRICS_SNAPSHOTS
            WHERE TEST_ID = ?
            GROUP BY DATE_TRUNC('second', TIMESTAMP)
        )
        SELECT
            s.SECOND AS TS,
            s.ELAPSED_SECONDS,
            COALESCE(wt.ACTIVE_CLUSTERS, 0) AS ACTIVE_CLUSTERS,
            COALESCE(rt.STARTED_CLUSTERS, 0) AS REALTIME_CLUSTERS,
            COALESCE(wt.QUERIES_STARTED, 0) AS QUERIES_STARTED,
            COALESCE(wt.TOTAL_QUEUE_OVERLOAD_MS, 0) AS TOTAL_QUEUE_OVERLOAD_MS,
            COALESCE(wt.TOTAL_QUEUE_PROVISIONING_MS, 0) AS TOTAL_QUEUE_PROVISIONING_MS,
            COALESCE(
                COALESCE(wt.TOTAL_QUEUE_OVERLOAD_MS, 0) / NULLIF(wt.QUERIES_STARTED, 0),
                0
            ) AS AVG_QUEUE_OVERLOAD_MS,
            COALESCE(
                COALESCE(wt.TOTAL_QUEUE_PROVISIONING_MS, 0) / NULLIF(wt.QUERIES_STARTED, 0),
                0
            ) AS AVG_QUEUE_PROVISIONING_MS
        FROM seconds s
        LEFT JOIN {prefix}.V_WAREHOUSE_TIMESERIES wt
          ON wt.TEST_ID = ?
         AND wt.SECOND = s.SECOND
        LEFT JOIN realtime_clusters rt
          ON rt.SECOND = s.SECOND
        ORDER BY s.SECOND ASC
        """
        rows = await pool.execute_query(query, params=[test_id, test_id, test_id])

        points: list[dict[str, Any]] = []
        has_data = False
        for row in rows:
            (
                ts,
                elapsed,
                active_clusters,
                realtime_clusters,
                queries_started,
                total_overload_ms,
                total_provisioning_ms,
                avg_overload_ms,
                avg_provisioning_ms,
            ) = row

            active_clusters_i = int(active_clusters or 0)
            realtime_clusters_i = int(realtime_clusters or 0)
            best_clusters = max(active_clusters_i, realtime_clusters_i)
            queries_started_i = int(queries_started or 0)
            total_overload_f = _to_float_or_none(total_overload_ms) or 0.0
            total_provisioning_f = _to_float_or_none(total_provisioning_ms) or 0.0

            if (
                best_clusters > 0
                or queries_started_i > 0
                or total_overload_f > 0
                or total_provisioning_f > 0
            ):
                has_data = True

            points.append(
                {
                    "timestamp": ts.isoformat()
                    if hasattr(ts, "isoformat")
                    else str(ts),
                    "elapsed_seconds": float(elapsed or 0),
                    "active_clusters": best_clusters,
                    "queries_started": queries_started_i,
                    "total_queue_overload_ms": total_overload_f,
                    "total_queue_provisioning_ms": total_provisioning_f,
                    "avg_queue_overload_ms": _to_float_or_none(avg_overload_ms) or 0.0,
                    "avg_queue_provisioning_ms": _to_float_or_none(avg_provisioning_ms)
                    or 0.0,
                }
            )

        available = has_data
        return {
            "test_id": test_id,
            "available": available,
            "points": points if available else [],
        }
    except Exception as e:
        logger.debug("Failed to load warehouse timeseries for %s: %s", test_id, e)
        return {"test_id": test_id, "available": False, "points": [], "error": str(e)}


@router.delete("/{test_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_test(test_id: str) -> None:
    try:
        pool = snowflake_pool.get_default_pool()
        await pool.execute_query(
            f"DELETE FROM {_prefix()}.METRICS_SNAPSHOTS WHERE TEST_ID = ?",
            params=[test_id],
        )
        await pool.execute_query(
            f"DELETE FROM {_prefix()}.QUERY_EXECUTIONS WHERE TEST_ID = ?",
            params=[test_id],
        )
        await pool.execute_query(
            f"DELETE FROM {_prefix()}.TEST_RESULTS WHERE TEST_ID = ?",
            params=[test_id],
        )
        return None
    except Exception as e:
        raise http_exception("delete test", e)


@router.post("/{test_id}/rerun")
async def rerun_test(test_id: str) -> dict[str, Any]:
    try:
        pool = snowflake_pool.get_default_pool()
        rows = await pool.execute_query(
            f"SELECT TEST_CONFIG FROM {_prefix()}.TEST_RESULTS WHERE TEST_ID = ?",
            params=[test_id],
        )
        if not rows:
            raise HTTPException(status_code=404, detail="Test not found")

        test_config = rows[0][0]
        if isinstance(test_config, str):
            test_config = json.loads(test_config)

        template_id = test_config.get("template_id") or test_config.get(
            "template", {}
        ).get("template_id")
        if not template_id:
            raise HTTPException(
                status_code=400, detail="Cannot rerun: missing template_id"
            )

        running = await registry.start_from_template(str(template_id), auto_start=False)
        return {"new_test_id": running.test_id}
    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("rerun test", e)


@router.post("/{test_id}/retry-enrichment")
async def retry_enrichment(test_id: str) -> dict[str, Any]:
    """
    Retry post-processing enrichment for a completed test.

    This allows users to retry enrichment if it failed or was cancelled,
    without having to re-run the entire test.

    Only allowed for tests with status=COMPLETED and enrichment_status=FAILED.
    """
    from backend.core.results_store import (
        enrich_query_executions_with_retry,
        update_test_overhead_percentiles,
        update_enrichment_status,
    )

    try:
        pool = snowflake_pool.get_default_pool()
        prefix = _prefix()

        # Check test status and enrichment status
        rows = await pool.execute_query(
            f"""
            SELECT STATUS, ENRICHMENT_STATUS, TEST_CONFIG
            FROM {prefix}.TEST_RESULTS
            WHERE TEST_ID = ?
            """,
            params=[test_id],
        )
        if not rows:
            raise HTTPException(status_code=404, detail="Test not found")

        test_status = rows[0][0]
        enrichment_status = rows[0][1]
        test_config = rows[0][2]

        # Only allow retry for COMPLETED tests
        if str(test_status).upper() != "COMPLETED":
            raise HTTPException(
                status_code=400,
                detail=f"Cannot retry enrichment: test status is {test_status}, must be COMPLETED",
            )

        # Only allow retry if enrichment failed (not if skipped or pending)
        if enrichment_status and str(enrichment_status).upper() not in (
            "FAILED",
            "PENDING",
        ):
            raise HTTPException(
                status_code=400,
                detail=f"Cannot retry enrichment: current status is {enrichment_status}",
            )

        # Parse test config to check if collect_query_history was enabled
        if isinstance(test_config, str):
            test_config = json.loads(test_config)

        scenario_config = test_config.get("scenario") or test_config.get(
            "template_config", {}
        )
        collect_query_history = bool(
            scenario_config.get("collect_query_history", False)
        )

        if not collect_query_history:
            raise HTTPException(
                status_code=400,
                detail="Cannot retry enrichment: collect_query_history was not enabled for this test",
            )

        # Update status to PENDING
        await update_enrichment_status(test_id=test_id, status="PENDING", error=None)

        # Run enrichment
        try:
            stats = await enrich_query_executions_with_retry(
                test_id=test_id,
                target_ratio=0.90,
                max_wait_seconds=240,
                poll_interval_seconds=10,
            )
            await update_test_overhead_percentiles(test_id=test_id)
            await update_enrichment_status(
                test_id=test_id, status="COMPLETED", error=None
            )

            return {
                "test_id": test_id,
                "enrichment_status": "COMPLETED",
                "stats": {
                    "total_queries": stats.total_queries,
                    "enriched_queries": stats.enriched_queries,
                    "enrichment_ratio": round(stats.enrichment_ratio * 100, 1),
                },
            }
        except Exception as e:
            await update_enrichment_status(
                test_id=test_id, status="FAILED", error=str(e)
            )
            raise HTTPException(
                status_code=500,
                detail=f"Enrichment failed: {e}",
            )

    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("retry enrichment", e)


@router.get("/{test_id}/enrichment-status")
async def get_test_enrichment_status(test_id: str) -> dict[str, Any]:
    """Get the enrichment status for a test."""
    from backend.core.results_store import get_enrichment_status

    try:
        status_info = await get_enrichment_status(test_id=test_id)
        if status_info is None:
            raise HTTPException(status_code=404, detail="Test not found")

        return {
            "test_id": test_id,
            "test_status": status_info["test_status"],
            "enrichment_status": status_info["enrichment_status"],
            "enrichment_error": status_info["enrichment_error"],
            "can_retry": (
                str(status_info["test_status"]).upper() == "COMPLETED"
                and str(status_info.get("enrichment_status") or "").upper() == "FAILED"
            ),
        }
    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("get enrichment status", e)


class AiAnalysisRequest(BaseModel):
    context: str | None = None


class AiChatRequest(BaseModel):
    message: str
    history: list[dict[str, str]] = []


async def _fetch_relevant_logs(
    pool: Any,
    test_id: str,
    load_mode: str,
    limit: int = 50,
) -> str:
    """
    Fetch and filter logs relevant to AI analysis based on load mode.

    Returns formatted log text for inclusion in prompts.
    """
    prefix = _prefix()

    # Fetch logs ordered by sequence
    log_rows = await pool.execute_query(
        f"""
        SELECT LEVEL, MESSAGE
        FROM {prefix}.TEST_LOGS
        WHERE TEST_ID = ?
        ORDER BY SEQ ASC
        LIMIT 500
        """,
        params=[test_id],
    )

    if not log_rows:
        return ""

    # Define relevance patterns based on mode
    always_include_levels = {"WARNING", "ERROR"}

    # Mode-specific message patterns (case-insensitive substrings)
    mode_patterns: dict[str, list[str]] = {
        "FIND_MAX_CONCURRENCY": [
            "find_max:",
            "degradation",
            "backoff",
            "stable",
            "step ",
            "stopping",
            "baseline",
            "unstable",
        ],
        "QPS": [
            "auto-scale controller",
            "qps controller",
            "target_qps",
            "target_sf_running",
            "scaling",
            "workers=",
            "→",
        ],
        "CONCURRENCY": [
            "warmup",
            "starting measurement",
            "stopping",
            "worker",
            "pool",
            "connection",
        ],
    }

    # Common patterns for all modes
    common_patterns = [
        "error",
        "failed",
        "exception",
        "timeout",
        "retry",
        "queue",
        "blocked",
    ]

    patterns = mode_patterns.get(load_mode, mode_patterns["CONCURRENCY"])
    all_patterns = patterns + common_patterns

    # Filter logs
    relevant_logs: list[str] = []
    for row in log_rows:
        level = str(row[0] or "").upper()
        message = str(row[1] or "")

        # Always include warnings and errors
        if level in always_include_levels:
            relevant_logs.append(f"[{level}] {message}")
            continue

        # Check mode-specific patterns
        msg_lower = message.lower()
        for pattern in all_patterns:
            if pattern.lower() in msg_lower:
                relevant_logs.append(f"[{level}] {message}")
                break

    # Limit and format
    if not relevant_logs:
        return ""

    # Take last N logs (most recent are often most relevant for diagnosis)
    if len(relevant_logs) > limit:
        relevant_logs = relevant_logs[-limit:]

    return "\n".join(relevant_logs)


def _build_concurrency_prompt(
    *,
    test_name: str,
    test_status: str,
    table_type: str,
    warehouse: str,
    warehouse_size: str,
    concurrency: int,
    duration: int,
    total_ops: int,
    read_ops: int,
    write_ops: int,
    failed_ops: int,
    ops_per_sec: float,
    p50: float,
    p95: float,
    p99: float,
    breakdown_text: str,
    qps_info: str,
    wh_text: str,
    context: str | None,
    execution_logs: str,
) -> str:
    """Build prompt for CONCURRENCY mode (fixed worker count)."""
    error_pct = (failed_ops / total_ops * 100) if total_ops > 0 else 0

    logs_section = ""
    if execution_logs:
        logs_section = f"""
EXECUTION LOGS (filtered for relevance):
{execution_logs}
"""

    return f"""You are analyzing a Snowflake CONCURRENCY mode benchmark test.

**Mode: CONCURRENCY (Fixed Workers / Closed Model)**
This test ran a fixed number of concurrent workers for a set duration to measure steady-state performance under constant load.

TEST SUMMARY:
- Test Name: {test_name}
- Status: {test_status}
- Table Type: {table_type}
- Warehouse: {warehouse} ({warehouse_size})
- Fixed Workers: {concurrency} workers
- Duration: {duration}s
- Total Operations: {total_ops} (Reads: {read_ops}, Writes: {write_ops})
- Failed Operations: {failed_ops} ({error_pct:.2f}%)
- Throughput: {ops_per_sec:.1f} ops/sec
- Latency: p50={p50:.1f}ms, p95={p95:.1f}ms, p99={p99:.1f}ms

{breakdown_text}

{qps_info}
{wh_text}

{f"Additional Context: {context}" if context else ""}
{logs_section}
Provide analysis structured as:

1. **Performance Summary**: How did the system perform under {concurrency} concurrent workers?
   - Is throughput ({ops_per_sec:.1f} ops/sec) reasonable for this configuration?
   - Are latencies acceptable? (p95={p95:.1f}ms, p99={p99:.1f}ms)
   - Is error rate ({error_pct:.2f}%) acceptable?

2. **Key Findings**: What stands out in the metrics?
   - Any concerning latency outliers (compare p50 vs p95 vs p99)?
   - Read vs write performance differences?
   - Queue wait times indicating saturation?

3. **Bottleneck Analysis**: What's limiting performance?
   - Compute bound (high CPU, low queue times)?
   - Queue bound (high queue_overload_ms)?
   - Data bound (high bytes_scanned)?

4. **Recommendations**: Specific suggestions:
   - Should concurrency be increased or decreased?
   - Would a larger warehouse help?
   - Any query optimization opportunities?

5. **Overall Grade**: A/B/C/D/F with brief justification.

Keep analysis concise and actionable. Use bullet points with specific numbers."""


def _build_qps_prompt(
    *,
    test_name: str,
    test_status: str,
    table_type: str,
    warehouse: str,
    warehouse_size: str,
    target_qps: int,
    min_concurrency: int,
    max_concurrency: int,
    duration: int,
    total_ops: int,
    read_ops: int,
    write_ops: int,
    failed_ops: int,
    ops_per_sec: float,
    p50: float,
    p95: float,
    p99: float,
    breakdown_text: str,
    qps_info: str,
    wh_text: str,
    context: str | None,
    execution_logs: str,
) -> str:
    """Build prompt for QPS mode (auto-scaling to target)."""
    error_pct = (failed_ops / total_ops * 100) if total_ops > 0 else 0
    target_achieved_pct = (ops_per_sec / target_qps * 100) if target_qps > 0 else 0

    logs_section = ""
    if execution_logs:
        logs_section = f"""
AUTO-SCALER LOGS (showing controller decisions):
{execution_logs}
"""

    return f"""You are analyzing a Snowflake QPS mode benchmark test.

**Mode: QPS (Auto-Scale to Target Throughput)**
This test dynamically scaled workers between {min_concurrency}-{max_concurrency} to achieve a target throughput of {target_qps} ops/sec. The controller adjusts concurrency based on achieved vs target QPS.

TEST SUMMARY:
- Test Name: {test_name}
- Status: {test_status}
- Table Type: {table_type}
- Warehouse: {warehouse} ({warehouse_size})
- Target QPS: {target_qps} ops/sec
- Achieved QPS: {ops_per_sec:.1f} ops/sec ({target_achieved_pct:.1f}% of target)
- Worker Range: {min_concurrency}-{max_concurrency}
- Duration: {duration}s
- Total Operations: {total_ops} (Reads: {read_ops}, Writes: {write_ops})
- Failed Operations: {failed_ops} ({error_pct:.2f}%)
- Latency: p50={p50:.1f}ms, p95={p95:.1f}ms, p99={p99:.1f}ms

{breakdown_text}

{qps_info}
{wh_text}

{f"Additional Context: {context}" if context else ""}
{logs_section}
Provide analysis structured as:

1. **Target Achievement**: Did the test hit the target?
   - Target: {target_qps} ops/sec, Achieved: {ops_per_sec:.1f} ops/sec
   - If target not achieved, why? (hit max workers? high latency? errors?)
   - Was the target realistic for this configuration?

2. **Auto-Scaler Performance**: How well did the controller perform?
   - Did it effectively scale to meet demand?
   - Were there oscillations or instability? (Check the logs for scaling decisions)
   - Queue times indicating the controller couldn't keep up?

3. **Latency Under Load**: How did latency change as load increased?
   - p50={p50:.1f}ms, p95={p95:.1f}ms, p99={p99:.1f}ms
   - Any latency degradation signs?
   - Acceptable latency vs throughput tradeoff?

4. **Bottleneck Analysis**: What limited target achievement?
   - Max workers reached?
   - Warehouse saturation (queue times)?
   - High error rate?

5. **Recommendations**:
   - Adjust target QPS (higher or lower)?
   - Increase max_concurrency?
   - Larger warehouse needed?
   - Query optimizations?

6. **Overall Grade**: A/B/C/D/F with brief justification.

Keep analysis concise and actionable. Use bullet points with specific numbers."""


def _build_find_max_prompt(
    *,
    test_name: str,
    test_status: str,
    table_type: str,
    warehouse: str,
    warehouse_size: str,
    start_concurrency: int,
    increment: int,
    step_duration: int,
    max_concurrency: int,
    total_ops: int,
    read_ops: int,
    write_ops: int,
    failed_ops: int,
    ops_per_sec: float,
    p50: float,
    p95: float,
    p99: float,
    breakdown_text: str,
    qps_info: str,
    wh_text: str,
    find_max_result: dict | None,
    context: str | None,
    latency_stability_pct: float,
    max_error_rate_pct: float,
    qps_stability_pct: float,
    execution_logs: str,
) -> str:
    """Build prompt for FIND_MAX_CONCURRENCY mode (step-load test)."""
    error_pct = (failed_ops / total_ops * 100) if total_ops > 0 else 0

    logs_section = ""
    if execution_logs:
        logs_section = f"""
EXECUTION LOGS (step transitions, degradation detection, backoff decisions):
{execution_logs}
"""

    # Extract step history and results
    step_history_text = ""
    best_concurrency = "Unknown"
    best_qps = "Unknown"
    baseline_p95 = "Unknown"
    final_reason = "Unknown"

    if find_max_result and isinstance(find_max_result, dict):
        best_concurrency = find_max_result.get("final_best_concurrency", "Unknown")
        best_qps = find_max_result.get("final_best_qps", "Unknown")
        baseline_p95 = find_max_result.get("baseline_p95_latency_ms", "Unknown")
        final_reason = find_max_result.get("final_reason", "Unknown")

        step_history = find_max_result.get("step_history", [])
        if step_history:
            step_history_text = "\nSTEP-BY-STEP PROGRESSION:\n"
            step_history_text += (
                "| Step | Workers | QPS | p95 (ms) | Error % | Stable | Reason |\n"
            )
            step_history_text += (
                "|------|---------|-----|----------|---------|--------|--------|\n"
            )
            for step in step_history:
                step_num = step.get("step", "?")
                workers = step.get("concurrency", "?")
                qps = step.get("qps", 0)
                step_p95 = step.get("p95_latency_ms", 0)
                err_pct = step.get("error_rate_pct", 0)
                stable = "Yes" if step.get("stable", False) else "No"
                reason = step.get("stop_reason", "-") or "-"
                step_history_text += (
                    f"| {step_num} | {workers} | {qps:.1f} | {step_p95:.1f} "
                    f"| {err_pct:.2f}% | {stable} | {reason} |\n"
                )

    return f"""You are analyzing a Snowflake FIND_MAX_CONCURRENCY benchmark test.

**Mode: FIND_MAX_CONCURRENCY (Step-Load Test)**
This test incrementally increased concurrent workers to find the maximum sustainable concurrency before performance degraded. It started at {start_concurrency} workers, increased by +{increment} every {step_duration}s, up to max {max_concurrency}.

**How it works:**
1. Runs {step_duration}s at each concurrency level
2. Measures QPS, latency, and error rate for each step
3. Stops when: latency degrades >{latency_stability_pct}% from baseline, error rate >{max_error_rate_pct}%, or max workers reached
4. Reports the highest "stable" concurrency level (QPS stable within {qps_stability_pct}%)

TEST SUMMARY:
- Test Name: {test_name}
- Status: {test_status}
- Table Type: {table_type}
- Warehouse: {warehouse} ({warehouse_size})
- Step Configuration: Start={start_concurrency}, +{increment}/step, {step_duration}s/step, Max={max_concurrency}
- **Best Sustainable Concurrency: {best_concurrency} workers**
- **Best Stable QPS: {best_qps}**
- Baseline p95 Latency: {baseline_p95}ms
- Stop Reason: {final_reason}
- Total Operations: {total_ops} (Reads: {read_ops}, Writes: {write_ops})
- Failed Operations: {failed_ops} ({error_pct:.2f}%)
- Overall Throughput: {ops_per_sec:.1f} ops/sec
- Overall Latency: p50={p50:.1f}ms, p95={p95:.1f}ms, p99={p99:.1f}ms

{step_history_text}

{breakdown_text}

{qps_info}
{wh_text}

{f"Additional Context: {context}" if context else ""}
{logs_section}
Provide analysis structured as:

1. **Maximum Concurrency Found**: What was the result?
   - Best sustainable concurrency: {best_concurrency} workers
   - QPS at best concurrency: {best_qps}
   - Why did the test stop? ({final_reason})
   - Review the execution logs for detailed step-by-step decisions

2. **Scaling Curve Analysis**: How did performance scale?
   - Was scaling linear (2x workers = 2x QPS)?
   - Where did diminishing returns begin?
   - At what point did latency start degrading?

3. **Degradation Point**: What caused the system to stop scaling?
   - Latency spike (compare baseline vs final p95)?
   - Error rate increase?
   - Queue saturation?
   - Check logs for specific degradation messages

4. **Bottleneck Identification**: What resource was exhausted?
   - Warehouse compute capacity?
   - Connection/query queue limits?
   - Data access contention?

5. **Recommendations**:
   - Optimal operating point (usually 70-80% of max)?
   - Would larger warehouse increase max concurrency?
   - Any configuration changes to improve scalability?

6. **Overall Grade**: A/B/C/D/F based on:
   - How well did max concurrency match expectations for {warehouse_size} warehouse?
   - Was degradation graceful or sudden?
   - Is the recommended operating point practical?

Keep analysis concise and actionable. Use bullet points with specific numbers."""


@router.post("/{test_id}/ai-analysis")
async def ai_analysis(
    test_id: str, req: AiAnalysisRequest | None = None
) -> dict[str, Any]:
    """Generate AI analysis of a benchmark test with mode-specific prompts."""
    try:
        pool = snowflake_pool.get_default_pool()
        prefix = _prefix()

        # Fetch test data including FIND_MAX_RESULT
        test_row = await pool.execute_query(
            f"""
            SELECT
                TEST_NAME, TABLE_TYPE, WAREHOUSE, WAREHOUSE_SIZE,
                CONCURRENT_CONNECTIONS, DURATION_SECONDS, STATUS,
                QPS, TOTAL_OPERATIONS, FAILED_OPERATIONS,
                P50_LATENCY_MS, P95_LATENCY_MS, P99_LATENCY_MS,
                READ_OPERATIONS, WRITE_OPERATIONS,
                TEST_CONFIG, FIND_MAX_RESULT
            FROM {prefix}.TEST_RESULTS
            WHERE TEST_ID = ?
            """,
            params=[test_id],
        )
        if not test_row:
            raise HTTPException(status_code=404, detail="Test not found")

        row = test_row[0]
        test_name = row[0]
        table_type = row[1]
        warehouse = row[2]
        warehouse_size = row[3]
        concurrency = row[4]
        duration = row[5]
        test_status = row[6]
        ops_per_sec = float(row[7] or 0)
        total_ops = int(row[8] or 0)
        failed_ops = int(row[9] or 0)
        p50 = float(row[10] or 0)
        p95 = float(row[11] or 0)
        p99 = float(row[12] or 0)
        read_ops = int(row[13] or 0)
        write_ops = int(row[14] or 0)
        test_config = row[15]
        find_max_result_raw = row[16]

        if isinstance(test_config, str):
            test_config = json.loads(test_config)

        # Parse find_max_result
        find_max_result = None
        if find_max_result_raw:
            if isinstance(find_max_result_raw, str):
                find_max_result = json.loads(find_max_result_raw)
            else:
                find_max_result = find_max_result_raw

        # Extract load mode and config
        template_cfg = (
            test_config.get("template_config")
            if isinstance(test_config, dict)
            else None
        )
        table_type_u = str(table_type or "").strip().upper()
        is_postgres = table_type_u in {"POSTGRES", "SNOWFLAKE_POSTGRES"}
        load_mode = "CONCURRENCY"
        target_qps = None
        min_concurrency = 1
        start_concurrency = 5
        concurrency_increment = 10
        step_duration_seconds = 30
        latency_stability_pct = 20.0
        max_error_rate_pct = 1.0
        qps_stability_pct = 5.0

        if isinstance(test_config, dict):
            load_mode = str(test_config.get("load_mode", "CONCURRENCY")).upper()
            target_qps = test_config.get("target_qps")
            min_concurrency = int(test_config.get("min_concurrency", 1) or 1)
            start_concurrency = int(test_config.get("start_concurrency", 5) or 5)
            concurrency_increment = int(
                test_config.get("concurrency_increment", 10) or 10
            )
            step_duration_seconds = int(
                test_config.get("step_duration_seconds", 30) or 30
            )
            latency_stability_pct = float(
                test_config.get("latency_stability_pct", 20.0) or 20.0
            )
            max_error_rate_pct = float(
                test_config.get("max_error_rate_pct", 1.0) or 1.0
            )
            qps_stability_pct = float(test_config.get("qps_stability_pct", 5.0) or 5.0)

        # Fetch query breakdown
        query_breakdown = await pool.execute_query(
            f"""
            SELECT
                QUERY_KIND,
                COUNT(*) AS query_count,
                AVG(DURATION_MS) AS avg_latency,
                PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY DURATION_MS) AS p50,
                PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY DURATION_MS) AS p95,
                PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY DURATION_MS) AS p99,
                AVG(SF_BYTES_SCANNED) AS avg_bytes_scanned,
                SUM(SF_QUEUED_OVERLOAD_MS) AS total_queue_ms
            FROM {prefix}.QUERY_EXECUTIONS
            WHERE TEST_ID = ? AND WARMUP = FALSE
            GROUP BY QUERY_KIND
            ORDER BY query_count DESC
            """,
            params=[test_id],
        )

        breakdown_text = "QUERY TYPE BREAKDOWN:\n"
        for qrow in query_breakdown:
            qkind = qrow[0]
            qcount = int(qrow[1] or 0)
            qavg = float(qrow[2] or 0)
            qp50 = float(qrow[3] or 0)
            qp95 = float(qrow[4] or 0)
            qp99 = float(qrow[5] or 0)
            qbytes = float(qrow[6] or 0)
            qtotal_queue = float(qrow[7] or 0)
            breakdown_text += (
                f"- {qkind}: {qcount} queries, "
                f"avg={qavg:.1f}ms, p50={qp50:.1f}ms, p95={qp95:.1f}ms, p99={qp99:.1f}ms, "
                f"avg_bytes_scanned={qbytes / 1e9:.2f}GB, total_queue_wait={qtotal_queue:.0f}ms\n"
            )

        # Fetch actual QPS info
        timeseries = await pool.execute_query(
            f"""
            SELECT
                MIN(START_TIME) AS first_query,
                MAX(START_TIME) AS last_query,
                COUNT(*) / NULLIF(DATEDIFF('second', MIN(START_TIME), MAX(START_TIME)), 0) AS actual_qps,
                SUM(CASE WHEN SUCCESS = FALSE THEN 1 ELSE 0 END) AS errors
            FROM {prefix}.QUERY_EXECUTIONS
            WHERE TEST_ID = ? AND WARMUP = FALSE
            """,
            params=[test_id],
        )

        qps_info = ""
        if timeseries and timeseries[0]:
            actual_qps = float(timeseries[0][2] or 0)
            errors_count = int(timeseries[0][3] or 0)
            qps_info = f"Actual average QPS: {actual_qps:.1f}, Errors during test: {errors_count}"

        def _format_stat(value: Any, fmt: str) -> str:
            if value is None:
                return "N/A"
            try:
                return format(float(value), fmt)
            except Exception:
                return "N/A"

        # Fetch warehouse/postgres metrics
        wh_text = ""
        if is_postgres:
            pg_text = ""
            try:
                db_name = None
                if isinstance(template_cfg, dict):
                    db_name = template_cfg.get("database")
                elif isinstance(test_config, dict):
                    db_name = test_config.get("database")
                pg_metrics = await _fetch_postgres_stats(
                    table_type=table_type_u,
                    database=db_name,
                )
            except Exception:
                pg_metrics = {"postgres_stats_available": False}

            if not isinstance(pg_metrics, dict):
                pg_metrics = {}

            if pg_metrics.get("postgres_stats_available"):
                ps = pg_metrics.get("postgres_stats", {})
                if not isinstance(ps, dict):
                    ps = {}
                pool_stats = ps.get("pool", {}) or {}
                if not isinstance(pool_stats, dict):
                    pool_stats = {}
                pg_text = (
                    "POSTGRES METRICS:\n"
                    f"- Pool size: {pool_stats.get('size', 'N/A')}\n"
                    f"- Pool in-use: {pool_stats.get('in_use', 'N/A')}\n"
                    f"- Pool free: {pool_stats.get('free', 'N/A')}\n"
                    f"- Pool max: {pool_stats.get('max_size', 'N/A')}\n"
                    f"- Active connections: {ps.get('active_connections', 'N/A')}\n"
                    f"- Max connections: {ps.get('max_connections', 'N/A')}"
                )
            wh_text = pg_text
        else:
            wh_metrics = await _fetch_warehouse_metrics(pool=pool, test_id=test_id)
            if wh_metrics.get("warehouse_metrics_available"):
                wm = wh_metrics.get("warehouse_metrics", {})
                wh_text = (
                    "WAREHOUSE METRICS:\n"
                    f"- Clusters used: {wm.get('clusters_used', 0)}\n"
                    f"- Total queue overload: {_format_stat(wm.get('total_queued_overload_ms'), '.0f')}ms\n"
                    f"- Total queue provisioning: {_format_stat(wm.get('total_queued_provisioning_ms'), '.0f')}ms\n"
                    f"- Queries with queue wait: {wm.get('queries_with_overload_queue', 0)}\n"
                    f"- Cache hit rate: {_format_stat(wm.get('read_cache_hit_pct'), '.1f')}%"
                )

        context = req.context if req else None

        # Fetch relevant execution logs for AI analysis
        execution_logs = await _fetch_relevant_logs(
            pool=pool,
            test_id=test_id,
            load_mode=load_mode,
            limit=50,
        )

        # Build mode-specific prompt
        if load_mode == "FIND_MAX_CONCURRENCY":
            prompt = _build_find_max_prompt(
                test_name=test_name,
                test_status=test_status,
                table_type=table_type,
                warehouse=warehouse,
                warehouse_size=warehouse_size,
                start_concurrency=start_concurrency,
                increment=concurrency_increment,
                step_duration=step_duration_seconds,
                max_concurrency=concurrency,
                total_ops=total_ops,
                read_ops=read_ops,
                write_ops=write_ops,
                failed_ops=failed_ops,
                ops_per_sec=ops_per_sec,
                p50=p50,
                p95=p95,
                p99=p99,
                breakdown_text=breakdown_text,
                qps_info=qps_info,
                wh_text=wh_text,
                find_max_result=find_max_result,
                context=context,
                latency_stability_pct=latency_stability_pct,
                max_error_rate_pct=max_error_rate_pct,
                qps_stability_pct=qps_stability_pct,
                execution_logs=execution_logs,
            )
        elif load_mode == "QPS":
            prompt = _build_qps_prompt(
                test_name=test_name,
                test_status=test_status,
                table_type=table_type,
                warehouse=warehouse,
                warehouse_size=warehouse_size,
                target_qps=target_qps or 100,
                min_concurrency=min_concurrency,
                max_concurrency=concurrency,
                duration=duration,
                total_ops=total_ops,
                read_ops=read_ops,
                write_ops=write_ops,
                failed_ops=failed_ops,
                ops_per_sec=ops_per_sec,
                p50=p50,
                p95=p95,
                p99=p99,
                breakdown_text=breakdown_text,
                qps_info=qps_info,
                wh_text=wh_text,
                context=context,
                execution_logs=execution_logs,
            )
        else:
            # Default to CONCURRENCY mode
            prompt = _build_concurrency_prompt(
                test_name=test_name,
                test_status=test_status,
                table_type=table_type,
                warehouse=warehouse,
                warehouse_size=warehouse_size,
                concurrency=concurrency,
                duration=duration,
                total_ops=total_ops,
                read_ops=read_ops,
                write_ops=write_ops,
                failed_ops=failed_ops,
                ops_per_sec=ops_per_sec,
                p50=p50,
                p95=p95,
                p99=p99,
                breakdown_text=breakdown_text,
                qps_info=qps_info,
                wh_text=wh_text,
                context=context,
                execution_logs=execution_logs,
            )

        try:
            ai_resp = await pool.execute_query(
                "SELECT AI_COMPLETE(model => ?, prompt => ?, model_parameters => PARSE_JSON(?)) AS RESP",
                params=[
                    "claude-4-sonnet",
                    prompt,
                    json.dumps({"temperature": 0.3, "max_tokens": 2500}),
                ],
            )
            analysis = (
                ai_resp[0][0] if ai_resp and ai_resp[0] else "Analysis unavailable"
            )
        except Exception as ai_err:
            logger.warning("AI analysis failed: %s", ai_err)
            analysis = f"AI analysis unavailable: {ai_err}"

        return {
            "test_id": test_id,
            "analysis": analysis,
            "load_mode": load_mode,
            "summary": {
                "test_name": test_name,
                "status": test_status,
                "ops_per_sec": ops_per_sec,
                "target_qps": target_qps,
                "total_ops": total_ops,
                "failed_ops": failed_ops,
                "p95_latency": p95,
                "best_concurrency": (
                    find_max_result.get("final_best_concurrency")
                    if find_max_result
                    else None
                ),
            },
        }
    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("ai analysis", e)


@router.post("/{test_id}/ai-chat")
async def ai_chat(test_id: str, req: AiChatRequest) -> dict[str, Any]:
    """Chat with AI about a benchmark test."""
    try:
        pool = snowflake_pool.get_default_pool()
        prefix = _prefix()

        test_row = await pool.execute_query(
            f"""
            SELECT
                TEST_NAME, TABLE_TYPE, WAREHOUSE, WAREHOUSE_SIZE,
                CONCURRENT_CONNECTIONS, DURATION_SECONDS, STATUS,
                QPS, TOTAL_OPERATIONS, FAILED_OPERATIONS,
                P50_LATENCY_MS, P95_LATENCY_MS, P99_LATENCY_MS,
                READ_OPERATIONS, WRITE_OPERATIONS,
                TEST_CONFIG
            FROM {prefix}.TEST_RESULTS
            WHERE TEST_ID = ?
            """,
            params=[test_id],
        )
        if not test_row:
            raise HTTPException(status_code=404, detail="Test not found")

        row = test_row[0]
        test_config = row[15]
        if isinstance(test_config, str):
            test_config = json.loads(test_config)

        context = f"""You are an expert Snowflake performance analyst. You are helping analyze a benchmark test.

TEST CONTEXT:
- Test ID: {test_id}
- Test Name: {row[0]}
- Status: {row[6]}
- Table Type: {row[1]}
- Warehouse: {row[2]} ({row[3]})
- Concurrency: {row[4]} workers
- Duration: {row[5]}s
- Total Operations: {row[8]} (Reads: {row[13]}, Writes: {row[14]})
- Failed Operations: {row[9]}
- Operations/Second: {float(row[7] or 0):.1f}
- Latency: p50={float(row[10] or 0):.1f}ms, p95={float(row[11] or 0):.1f}ms, p99={float(row[12] or 0):.1f}ms
- Load Mode: {test_config.get("load_mode", "CONCURRENCY") if isinstance(test_config, dict) else "CONCURRENCY"}
- Target QPS: {test_config.get("target_qps", "N/A") if isinstance(test_config, dict) else "N/A"}

Answer the user's question based on this test data. Be specific and use numbers when relevant.
If asked about data you don't have access to, say so and suggest what data would help.
"""

        history_text = ""
        for h in req.history[-10:]:
            role = h.get("role", "user")
            content = h.get("content", "")
            history_text += f"\n{role.upper()}: {content}"

        full_prompt = f"{context}\n\nCONVERSATION HISTORY:{history_text}\n\nUSER: {req.message}\n\nASSISTANT:"

        try:
            ai_resp = await pool.execute_query(
                "SELECT AI_COMPLETE(model => ?, prompt => ?, model_parameters => PARSE_JSON(?)) AS RESP",
                params=[
                    "claude-4-sonnet",
                    full_prompt,
                    json.dumps({"temperature": 0.5, "max_tokens": 1500}),
                ],
            )
            response = (
                ai_resp[0][0]
                if ai_resp and ai_resp[0]
                else "I couldn't process that request."
            )
        except Exception as ai_err:
            logger.warning("AI chat failed: %s", ai_err)
            response = f"AI chat unavailable: {ai_err}"

        return {
            "test_id": test_id,
            "response": response,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("ai chat", e)
