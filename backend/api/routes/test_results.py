"""
API routes for persisted test results and running tests.

UI endpoints:
- History: GET /api/tests
- Comparison search: GET /api/tests/search?q=...
- Query executions (drilldown): GET /api/tests/{test_id}/query-executions
- Compare context: GET /api/tests/{test_id}/compare-context
- Re-run: POST /api/tests/{test_id}/rerun
- Delete: DELETE /api/tests/{test_id}
- Run template: POST /api/tests/from-template/{template_id}
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from dataclasses import dataclass
from typing import Any, cast

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel

from backend.config import settings
from backend.connectors import postgres_pool, snowflake_pool
from backend.core.orchestrator import orchestrator
from backend.core.results_store import update_parent_run_aggregate
from backend.core.test_registry import registry
from backend.core.cost_calculator import calculate_estimated_cost, calculate_cost_efficiency
from backend.api.error_handling import http_exception
from backend.api.routes.test_results_modules.comparison import build_compare_context
from backend.api.routes.test_results_modules.comparison_prompts import (
    generate_comparison_prompt,
    generate_deep_compare_prompt,
)
from backend.api.routes.test_results_modules.comparison_scoring import (
    calculate_similarity_score,
    classify_change,
)

router = APIRouter()
logger = logging.getLogger(__name__)


def _build_cost_fields(
    duration_seconds: float,
    warehouse_size: str | None,
    total_operations: int = 0,
    qps: float = 0.0,
    table_type: str | None = None,
    postgres_instance_size: str | None = None,
) -> dict[str, Any]:
    """
    Build cost-related fields for API responses.

    Args:
        duration_seconds: Test duration in seconds
        warehouse_size: Warehouse size string (e.g., "XSMALL", "MEDIUM")
        total_operations: Total operations executed (for efficiency metrics)
        qps: Queries per second (for efficiency metrics)
        table_type: Table type (e.g., "HYBRID", "POSTGRES")
                   Postgres uses instance-based pricing, not warehouse credits
        postgres_instance_size: For Postgres, explicit instance size override.
                               If not provided, looks up from configured Postgres host.

    Returns:
        Dictionary with cost fields to merge into response
    """
    from backend.core.cost_calculator import get_postgres_instance_size_by_host
    
    # For Postgres, look up the actual instance size from the configured host
    effective_postgres_size = postgres_instance_size
    if not effective_postgres_size and table_type:
        table_type_upper = table_type.upper().strip()
        if table_type_upper == "POSTGRES":
            # Try to get actual instance size from configured Postgres host
            actual_size = get_postgres_instance_size_by_host(settings.POSTGRES_HOST)
            if actual_size:
                effective_postgres_size = actual_size
            else:
                # Fall back to default
                effective_postgres_size = settings.POSTGRES_INSTANCE_SIZE
    
    cost_info = calculate_estimated_cost(
        duration_seconds=duration_seconds,
        warehouse_size=warehouse_size,
        dollars_per_credit=settings.COST_DOLLARS_PER_CREDIT,
        table_type=table_type,
        postgres_instance_size=effective_postgres_size,
    )

    result: dict[str, Any] = {
        "credits_used": cost_info["credits_used"],
        "estimated_cost_usd": cost_info["estimated_cost_usd"],
        "cost_per_hour": cost_info["cost_per_hour"],
        "credits_per_hour": cost_info.get("credits_per_hour", 0.0),
        "cost_calculation_method": cost_info["calculation_method"],
        "postgres_instance_size": effective_postgres_size if table_type and table_type.upper().strip() == "POSTGRES" else None,
    }

    # Add efficiency metrics if we have operation data AND have a cost to work with
    if (total_operations > 0 or qps > 0) and cost_info["estimated_cost_usd"] > 0:
        efficiency = calculate_cost_efficiency(
            total_cost=cost_info["estimated_cost_usd"],
            total_operations=total_operations,
            qps=qps,
            duration_seconds=duration_seconds,
        )
        result["cost_per_operation"] = efficiency["cost_per_operation"]
        result["cost_per_1000_ops"] = efficiency["cost_per_1000_ops"]
        result["cost_per_1k_ops"] = efficiency["cost_per_1000_ops"]  # Alias for frontend
        result["cost_per_1000_qps"] = efficiency["cost_per_1000_qps"]

    return result

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
LATENCY_AGGREGATION_METHOD = "slowest_worker_approximation"


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


async def _call_sp(pool: Any, sp_name: str, *args: Any) -> dict[str, Any]:
    """
    Call a stored procedure and return parsed JSON result.
    
    Args:
        pool: Snowflake connection pool
        sp_name: Name of the stored procedure (without schema prefix)
        *args: Arguments to pass to the stored procedure
        
    Returns:
        Parsed JSON result from the stored procedure
    """
    prefix = _prefix()
    # Build parameter placeholders
    placeholders = ", ".join(["?" for _ in args]) if args else ""
    query = f"CALL {prefix}.{sp_name}({placeholders})"
    
    rows = await pool.execute_query(query, params=list(args) if args else [])
    
    if not rows or not rows[0] or not rows[0][0]:
        return {}
    
    result = rows[0][0]
    # Handle both string JSON and already-parsed dict
    if isinstance(result, str):
        return json.loads(result)
    return dict(result) if result else {}


async def _fetch_run_status(pool: Any, run_id: str) -> dict[str, Any] | None:
    rows = await pool.execute_query(
        f"""
        SELECT RUN_ID, STATUS, PHASE, START_TIME, END_TIME, FIND_MAX_STATE, CANCELLATION_REASON,
               TIMESTAMPDIFF(SECOND, START_TIME, CURRENT_TIMESTAMP()) AS ELAPSED_SECONDS
        FROM {_prefix()}.RUN_STATUS
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
        "cancellation_reason": str(cancellation_reason)
        if cancellation_reason
        else None,
        "elapsed_seconds": float(elapsed_secs) if elapsed_secs is not None else None,
    }


async def _aggregate_parent_enrichment_status(
    *, pool: Any, run_id: str
) -> tuple[str | None, str | None]:
    """Aggregate ENRICHMENT_STATUS, checking parent first (authoritative), then workers.

    This mirrors the logic in _fetch_parent_enrichment_status() (main.py) to ensure
    HTTP and WebSocket endpoints return consistent enrichment status.
    """
    prefix = _prefix()

    # Check parent row first - it's the authoritative source for enrichment status.
    # The orchestrator sets enrichment status on the parent row in _mark_run_completed()
    # and updates it when enrichment completes/fails.
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
        # If parent has a terminal enrichment status, use it immediately
        if parent_status in ("COMPLETED", "FAILED", "SKIPPED"):
            return parent_status, str(parent_error) if parent_error else None

    # Fallback: aggregate worker rows (for multi-worker runs where parent may still
    # be PENDING but we want to reflect any worker-level failures)
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
        # No workers yet - return parent status (likely PENDING)
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


async def _aggregate_parent_enrichment_stats(
    *, pool: Any, run_id: str
) -> tuple[int, int, float]:
    prefix = _prefix()
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


def _compute_aggregated_find_max(worker_results: list[dict]) -> dict:
    """
    Compute true aggregate metrics across all workers' find_max_result.

    For each concurrency level (step), aggregates:
    - Total QPS (sum across workers)
    - Max P95/P99 latencies (worst case)
    - Number of active workers at each step
    """
    if not worker_results:
        return {}

    steps_by_concurrency: dict[int, dict[int, dict]] = {}
    all_baselines_p95 = []
    all_baselines_p99 = []

    for worker in worker_results:
        fmr = worker.get("find_max_result", {})
        if not fmr:
            continue

        worker_idx = worker.get("worker_index", 0)
        if fmr.get("baseline_p95_latency_ms"):
            all_baselines_p95.append(fmr["baseline_p95_latency_ms"])
        if fmr.get("baseline_p99_latency_ms"):
            all_baselines_p99.append(fmr["baseline_p99_latency_ms"])

        step_history = fmr.get("step_history", [])
        for step in step_history:
            cc = step.get("concurrency")
            if cc is not None:
                if cc not in steps_by_concurrency:
                    steps_by_concurrency[cc] = {}
                if worker_idx not in steps_by_concurrency[cc]:
                    steps_by_concurrency[cc][worker_idx] = {
                        "worker_index": worker_idx,
                        **step,
                    }

    aggregated_steps = []
    total_workers = len(worker_results)

    for cc in sorted(steps_by_concurrency.keys()):
        worker_steps = list(steps_by_concurrency[cc].values())
        active_workers = len(worker_steps)

        total_qps = sum(s.get("qps", 0) or 0 for s in worker_steps)
        max_p95 = max((s.get("p95_latency_ms") or 0 for s in worker_steps), default=0)
        max_p99 = max((s.get("p99_latency_ms") or 0 for s in worker_steps), default=0)
        avg_p95 = (
            sum(s.get("p95_latency_ms") or 0 for s in worker_steps) / active_workers
            if active_workers > 0
            else 0
        )
        avg_p99 = (
            sum(s.get("p99_latency_ms") or 0 for s in worker_steps) / active_workers
            if active_workers > 0
            else 0
        )

        any_degraded = any(s.get("degraded") for s in worker_steps)
        reasons = [
            s.get("degrade_reason") for s in worker_steps if s.get("degrade_reason")
        ]

        aggregated_steps.append(
            {
                "concurrency": cc,
                "total_concurrency": cc * active_workers,
                "qps": round(total_qps, 2),
                "p95_latency_ms": round(max_p95, 2),
                "p99_latency_ms": round(max_p99, 2),
                "avg_p95_latency_ms": round(avg_p95, 2),
                "avg_p99_latency_ms": round(avg_p99, 2),
                "active_workers": active_workers,
                "total_workers": total_workers,
                "degraded": any_degraded,
                "degrade_reasons": reasons if reasons else None,
            }
        )

    best_step = None
    for step in aggregated_steps:
        if step["active_workers"] == total_workers and not step["degraded"]:
            if best_step is None or step["qps"] > best_step["qps"]:
                best_step = step

    if best_step is None and aggregated_steps:
        non_degraded = [s for s in aggregated_steps if not s["degraded"]]
        if non_degraded:
            best_step = max(non_degraded, key=lambda s: s["qps"])
        else:
            best_step = aggregated_steps[0]

    return {
        "step_history": aggregated_steps,
        "baseline_p95_latency_ms": max(all_baselines_p95)
        if all_baselines_p95
        else None,
        "baseline_p99_latency_ms": max(all_baselines_p99)
        if all_baselines_p99
        else None,
        "final_best_concurrency": best_step["concurrency"] if best_step else None,
        "final_best_qps": best_step["qps"] if best_step else None,
        "total_workers": total_workers,
        "is_aggregate": True,
    }


def _to_float_or_none(v: Any) -> float | None:
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def _compute_latency_spread(p50: float | None, p95: float | None) -> dict[str, Any]:
    """
    Compute latency spread ratio (P95/P50) and warning flag.

    The spread ratio indicates latency variance - a high ratio means tail latencies
    are much worse than typical (median) latencies. This is common with hybrid tables
    under load, while interactive tables tend to have more consistent latencies.

    Args:
        p50: P50 (median) latency in milliseconds
        p95: P95 latency in milliseconds

    Returns:
        Dictionary with:
        - latency_spread_ratio: P95/P50 ratio (None if not computable)
        - latency_spread_warning: True if ratio > 5x (high variance)
    """
    if not p50 or not p95 or p50 <= 0:
        return {
            "latency_spread_ratio": None,
            "latency_spread_warning": False,
        }

    ratio = p95 / p50
    return {
        "latency_spread_ratio": round(ratio, 1),
        "latency_spread_warning": ratio > 5.0,
    }


async def _fetch_warehouse_metrics(*, pool: Any, test_id: str) -> dict[str, Any]:
    """
    Fetch test-level warehouse queueing + MCW metrics.

    For parent runs (multi-worker), aggregates from all TEST_IDs in the run
    (including the parent, since QUERY_EXECUTIONS stores data under run_id).
    """
    prefix = _prefix()

    run_id_rows = await pool.execute_query(
        f"SELECT RUN_ID FROM {prefix}.TEST_RESULTS WHERE TEST_ID = ?",
        params=[test_id],
    )
    run_id = run_id_rows[0][0] if run_id_rows and run_id_rows[0] else None
    is_parent = bool(run_id) and str(run_id) == str(test_id)

    if is_parent:
        # NOTE: QUERY_EXECUTIONS data is stored under run_id (parent TEST_ID),
        # so we must include the parent in the query. V_WAREHOUSE_METRICS joins
        # QUERY_EXECUTIONS, so the data lives under the parent TEST_ID.
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
            "max_connections": (
                int(max_connections) if max_connections is not None else None
            ),
            "active_connections": (
                int(active_connections) if active_connections is not None else None
            ),
        },
    }


async def _fetch_pg_enrichment(*, pool: Any, test_id: str) -> dict[str, Any]:
    """
    Fetch pg_stat_statements enrichment data for a Postgres test.
    
    Returns server-side execution metrics captured via pg_stat_statements
    during the measurement phase of the test.
    """
    prefix = _prefix()
    
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
    
    # If pg_stat_statements availability was never checked (old test), return no capabilities
    if pg_stat_available is None:
        return {"pg_enrichment_available": False}
    
    # If pg_stat_statements was checked but wasn't available
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
            "total_exec_time_ms": _to_float_or_none(total_exec_time_ms),
            "mean_exec_time_ms": _to_float_or_none(mean_exec_time_ms),
            "cache_hit_ratio": _to_float_or_none(cache_hit_ratio),
            "cache_hit_pct": (
                round(float(cache_hit_ratio) * 100, 2) 
                if cache_hit_ratio is not None 
                else None
            ),
            "shared_blks_hit": int(shared_blks_hit or 0),
            "shared_blks_read": int(shared_blks_read or 0),
            "rows_returned": int(rows_returned or 0),
            "query_pattern_count": int(query_pattern_count or 0),
            "blk_read_time_ms": _to_float_or_none(blk_read_time_ms),
            "blk_write_time_ms": _to_float_or_none(blk_write_time_ms),
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


async def _fetch_find_max_step_history(*, pool: Any, test_id: str) -> dict[str, Any]:
    """
    Fetch FIND_MAX step history showing concurrency progression and degradation points.
    
    Returns step-by-step data showing:
    - Each concurrency level tested
    - QPS achieved at each level
    - Latency metrics (P50, P95, P99)
    - Outcome (STABLE/DEGRADED) and stop reason
    """
    prefix = _prefix()
    
    query = f"""
    SELECT 
        STEP_NUMBER, TARGET_WORKERS, 
        QPS, P50_LATENCY_MS, P95_LATENCY_MS, P99_LATENCY_MS,
        ERROR_RATE, OUTCOME, STOP_REASON
    FROM {prefix}.CONTROLLER_STEP_HISTORY
    WHERE RUN_ID = ?
    ORDER BY STEP_NUMBER
    """
    
    rows = await pool.execute_query(query, params=[test_id])
    
    if not rows:
        return {"find_max_history_available": False}
    
    steps = []
    best_stable_concurrency = None
    best_stable_qps = 0
    degradation_points = []
    
    for row in rows:
        step_num, target_workers, qps, p50, p95, p99, error_rate, outcome, stop_reason = row
        
        step_data = {
            "step": int(step_num or 0),
            "concurrency": int(target_workers or 0),
            "qps": float(qps or 0),
            "p50_ms": float(p50 or 0),
            "p95_ms": float(p95 or 0),
            "p99_ms": float(p99 or 0),
            "error_rate": float(error_rate or 0),
            "outcome": str(outcome or "UNKNOWN"),
            "stop_reason": stop_reason,
        }
        steps.append(step_data)
        
        # Track best stable concurrency
        if outcome == "STABLE" and qps and float(qps) > best_stable_qps:
            best_stable_qps = float(qps)
            best_stable_concurrency = int(target_workers or 0)
        
        # Track degradation points
        if outcome == "DEGRADED":
            degradation_points.append({
                "concurrency": int(target_workers or 0),
                "reason": stop_reason,
            })
    
    return {
        "find_max_history_available": True,
        "steps": steps,
        "best_stable_concurrency": best_stable_concurrency,
        "best_stable_qps": best_stable_qps,
        "degradation_points": degradation_points,
    }


async def _fetch_cluster_breakdown(*, pool: Any, test_id: str) -> dict[str, Any]:
    """
    Fetch per-cluster breakdown for MCW tests.

    For parent runs (multi-worker), aggregates from all child runs.
    """
    prefix = _prefix()

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
        ORDER BY cb.CLUSTER_NUMBER ASC
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

    When enrichment ratio is low (< 50%), also computes ESTIMATED SF execution
    times by subtracting the median network overhead from APP_ELAPSED_MS.

    NOTE: This will raise if the underlying columns don't exist (e.g. older schema).
    Callers should catch and treat as "not available".
    """
    prefix = _prefix()

    enrichment_query = f"""
    SELECT
        COUNT(*) AS total_queries,
        COUNT(SF_EXECUTION_MS) AS enriched_queries,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY APP_ELAPSED_MS - SF_TOTAL_ELAPSED_MS) AS p50_overhead_ms
    FROM {prefix}.QUERY_EXECUTIONS
    WHERE TEST_ID = ?
      AND COALESCE(WARMUP, FALSE) = FALSE
      AND SUCCESS = TRUE
    """
    enrichment_rows = await pool.execute_query(enrichment_query, params=[test_id])
    total_queries = int(enrichment_rows[0][0] or 0) if enrichment_rows else 0
    enriched_queries = int(enrichment_rows[0][1] or 0) if enrichment_rows else 0
    p50_overhead_ms = (
        _to_float_or_none(enrichment_rows[0][2]) if enrichment_rows else None
    )
    enrichment_ratio = enriched_queries / total_queries if total_queries > 0 else 0.0

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
            "sf_enrichment_total_queries": total_queries,
            "sf_enrichment_enriched_queries": enriched_queries,
            "sf_enrichment_ratio_pct": round(enrichment_ratio * 100, 1),
            "sf_enrichment_low_warning": enrichment_ratio < 0.5 and total_queries > 100,
        }

    r = rows[0]
    sample_count = int(r[0] or 0)
    low_enrichment = enrichment_ratio < 0.5 and total_queries > 100

    payload = {
        "sf_latency_available": sample_count > 0,
        "sf_latency_sample_count": sample_count,
        "sf_enrichment_total_queries": total_queries,
        "sf_enrichment_enriched_queries": enriched_queries,
        "sf_enrichment_ratio_pct": round(enrichment_ratio * 100, 1),
        "sf_enrichment_low_warning": low_enrichment,
        "sf_enrichment_p50_overhead_ms": round(p50_overhead_ms, 2)
        if p50_overhead_ms is not None
        else None,
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

    if low_enrichment and p50_overhead_ms is not None and p50_overhead_ms > 0:
        est_query = f"""
        SELECT
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY GREATEST(0, APP_ELAPSED_MS - ?)) AS EST_P50_MS,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY GREATEST(0, APP_ELAPSED_MS - ?)) AS EST_P95_MS,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY GREATEST(0, APP_ELAPSED_MS - ?)) AS EST_P99_MS,
            PERCENTILE_CONT(0.50) WITHIN GROUP (
                ORDER BY IFF(QUERY_KIND = 'POINT_LOOKUP', GREATEST(0, APP_ELAPSED_MS - ?), NULL)
            ) AS EST_POINT_LOOKUP_P50_MS,
            PERCENTILE_CONT(0.50) WITHIN GROUP (
                ORDER BY IFF(QUERY_KIND = 'RANGE_SCAN', GREATEST(0, APP_ELAPSED_MS - ?), NULL)
            ) AS EST_RANGE_SCAN_P50_MS
        FROM {prefix}.QUERY_EXECUTIONS
        WHERE TEST_ID = ?
          AND COALESCE(WARMUP, FALSE) = FALSE
          AND SUCCESS = TRUE
        """
        overhead = p50_overhead_ms
        est_rows = await pool.execute_query(
            est_query,
            params=[overhead, overhead, overhead, overhead, overhead, test_id],
        )
        if est_rows and est_rows[0]:
            er = est_rows[0]
            payload["sf_estimated_available"] = True
            payload["sf_estimated_p50_latency_ms"] = _to_float_or_none(er[0])
            payload["sf_estimated_p95_latency_ms"] = _to_float_or_none(er[1])
            payload["sf_estimated_p99_latency_ms"] = _to_float_or_none(er[2])
            payload["sf_estimated_point_lookup_p50_latency_ms"] = _to_float_or_none(
                er[3]
            )
            payload["sf_estimated_range_scan_p50_latency_ms"] = _to_float_or_none(er[4])

    return payload


async def _fetch_app_latency_summary_for_run(
    *,
    pool: Any,
    parent_run_id: str,
    parent_test_id: str,
) -> dict[str, Any]:
    """
    Compute end-to-end (app) latency percentiles across all child tests for a parent run.
    """
    prefix = _prefix()
    query = f"""
    SELECT
        COUNT(*) AS SAMPLE_COUNT,

        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY APP_ELAPSED_MS) AS P50_LATENCY_MS,
        PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY APP_ELAPSED_MS) AS P95_LATENCY_MS,
        PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY APP_ELAPSED_MS) AS P99_LATENCY_MS,
        MIN(APP_ELAPSED_MS) AS MIN_LATENCY_MS,
        MAX(APP_ELAPSED_MS) AS MAX_LATENCY_MS,

        PERCENTILE_CONT(0.50) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND IN ('POINT_LOOKUP', 'RANGE_SCAN'), APP_ELAPSED_MS, NULL)
        ) AS READ_P50_LATENCY_MS,
        PERCENTILE_CONT(0.95) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND IN ('POINT_LOOKUP', 'RANGE_SCAN'), APP_ELAPSED_MS, NULL)
        ) AS READ_P95_LATENCY_MS,
        PERCENTILE_CONT(0.99) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND IN ('POINT_LOOKUP', 'RANGE_SCAN'), APP_ELAPSED_MS, NULL)
        ) AS READ_P99_LATENCY_MS,
        MIN(IFF(QUERY_KIND IN ('POINT_LOOKUP', 'RANGE_SCAN'), APP_ELAPSED_MS, NULL))
            AS READ_MIN_LATENCY_MS,
        MAX(IFF(QUERY_KIND IN ('POINT_LOOKUP', 'RANGE_SCAN'), APP_ELAPSED_MS, NULL))
            AS READ_MAX_LATENCY_MS,

        PERCENTILE_CONT(0.50) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND IN ('INSERT', 'UPDATE'), APP_ELAPSED_MS, NULL)
        ) AS WRITE_P50_LATENCY_MS,
        PERCENTILE_CONT(0.95) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND IN ('INSERT', 'UPDATE'), APP_ELAPSED_MS, NULL)
        ) AS WRITE_P95_LATENCY_MS,
        PERCENTILE_CONT(0.99) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND IN ('INSERT', 'UPDATE'), APP_ELAPSED_MS, NULL)
        ) AS WRITE_P99_LATENCY_MS,
        MIN(IFF(QUERY_KIND IN ('INSERT', 'UPDATE'), APP_ELAPSED_MS, NULL))
            AS WRITE_MIN_LATENCY_MS,
        MAX(IFF(QUERY_KIND IN ('INSERT', 'UPDATE'), APP_ELAPSED_MS, NULL))
            AS WRITE_MAX_LATENCY_MS,

        PERCENTILE_CONT(0.50) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'POINT_LOOKUP', APP_ELAPSED_MS, NULL)
        ) AS POINT_LOOKUP_P50_LATENCY_MS,
        PERCENTILE_CONT(0.95) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'POINT_LOOKUP', APP_ELAPSED_MS, NULL)
        ) AS POINT_LOOKUP_P95_LATENCY_MS,
        PERCENTILE_CONT(0.99) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'POINT_LOOKUP', APP_ELAPSED_MS, NULL)
        ) AS POINT_LOOKUP_P99_LATENCY_MS,
        MIN(IFF(QUERY_KIND = 'POINT_LOOKUP', APP_ELAPSED_MS, NULL))
            AS POINT_LOOKUP_MIN_LATENCY_MS,
        MAX(IFF(QUERY_KIND = 'POINT_LOOKUP', APP_ELAPSED_MS, NULL))
            AS POINT_LOOKUP_MAX_LATENCY_MS,

        PERCENTILE_CONT(0.50) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'RANGE_SCAN', APP_ELAPSED_MS, NULL)
        ) AS RANGE_SCAN_P50_LATENCY_MS,
        PERCENTILE_CONT(0.95) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'RANGE_SCAN', APP_ELAPSED_MS, NULL)
        ) AS RANGE_SCAN_P95_LATENCY_MS,
        PERCENTILE_CONT(0.99) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'RANGE_SCAN', APP_ELAPSED_MS, NULL)
        ) AS RANGE_SCAN_P99_LATENCY_MS,
        MIN(IFF(QUERY_KIND = 'RANGE_SCAN', APP_ELAPSED_MS, NULL))
            AS RANGE_SCAN_MIN_LATENCY_MS,
        MAX(IFF(QUERY_KIND = 'RANGE_SCAN', APP_ELAPSED_MS, NULL))
            AS RANGE_SCAN_MAX_LATENCY_MS,

        PERCENTILE_CONT(0.50) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'INSERT', APP_ELAPSED_MS, NULL)
        ) AS INSERT_P50_LATENCY_MS,
        PERCENTILE_CONT(0.95) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'INSERT', APP_ELAPSED_MS, NULL)
        ) AS INSERT_P95_LATENCY_MS,
        PERCENTILE_CONT(0.99) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'INSERT', APP_ELAPSED_MS, NULL)
        ) AS INSERT_P99_LATENCY_MS,
        MIN(IFF(QUERY_KIND = 'INSERT', APP_ELAPSED_MS, NULL)) AS INSERT_MIN_LATENCY_MS,
        MAX(IFF(QUERY_KIND = 'INSERT', APP_ELAPSED_MS, NULL)) AS INSERT_MAX_LATENCY_MS,

        PERCENTILE_CONT(0.50) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'UPDATE', APP_ELAPSED_MS, NULL)
        ) AS UPDATE_P50_LATENCY_MS,
        PERCENTILE_CONT(0.95) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'UPDATE', APP_ELAPSED_MS, NULL)
        ) AS UPDATE_P95_LATENCY_MS,
        PERCENTILE_CONT(0.99) WITHIN GROUP (
            ORDER BY IFF(QUERY_KIND = 'UPDATE', APP_ELAPSED_MS, NULL)
        ) AS UPDATE_P99_LATENCY_MS,
        MIN(IFF(QUERY_KIND = 'UPDATE', APP_ELAPSED_MS, NULL)) AS UPDATE_MIN_LATENCY_MS,
        MAX(IFF(QUERY_KIND = 'UPDATE', APP_ELAPSED_MS, NULL)) AS UPDATE_MAX_LATENCY_MS
    FROM {prefix}.QUERY_EXECUTIONS qe
    WHERE qe.TEST_ID IN (
        SELECT TEST_ID FROM {prefix}.TEST_RESULTS WHERE RUN_ID = ?
    )
      AND COALESCE(qe.WARMUP, FALSE) = FALSE
      AND qe.SUCCESS = TRUE
      AND qe.APP_ELAPSED_MS IS NOT NULL
    """
    rows = await pool.execute_query(query, params=[parent_run_id])
    if not rows:
        return {}

    r = rows[0]
    sample_count = int(r[0] or 0)
    if sample_count <= 0:
        return {}
    return {
        "p50_latency_ms": _to_float_or_none(r[1]),
        "p95_latency_ms": _to_float_or_none(r[2]),
        "p99_latency_ms": _to_float_or_none(r[3]),
        "min_latency_ms": _to_float_or_none(r[4]),
        "max_latency_ms": _to_float_or_none(r[5]),
        "read_p50_latency_ms": _to_float_or_none(r[6]),
        "read_p95_latency_ms": _to_float_or_none(r[7]),
        "read_p99_latency_ms": _to_float_or_none(r[8]),
        "read_min_latency_ms": _to_float_or_none(r[9]),
        "read_max_latency_ms": _to_float_or_none(r[10]),
        "write_p50_latency_ms": _to_float_or_none(r[11]),
        "write_p95_latency_ms": _to_float_or_none(r[12]),
        "write_p99_latency_ms": _to_float_or_none(r[13]),
        "write_min_latency_ms": _to_float_or_none(r[14]),
        "write_max_latency_ms": _to_float_or_none(r[15]),
        "point_lookup_p50_latency_ms": _to_float_or_none(r[16]),
        "point_lookup_p95_latency_ms": _to_float_or_none(r[17]),
        "point_lookup_p99_latency_ms": _to_float_or_none(r[18]),
        "point_lookup_min_latency_ms": _to_float_or_none(r[19]),
        "point_lookup_max_latency_ms": _to_float_or_none(r[20]),
        "range_scan_p50_latency_ms": _to_float_or_none(r[21]),
        "range_scan_p95_latency_ms": _to_float_or_none(r[22]),
        "range_scan_p99_latency_ms": _to_float_or_none(r[23]),
        "range_scan_min_latency_ms": _to_float_or_none(r[24]),
        "range_scan_max_latency_ms": _to_float_or_none(r[25]),
        "insert_p50_latency_ms": _to_float_or_none(r[26]),
        "insert_p95_latency_ms": _to_float_or_none(r[27]),
        "insert_p99_latency_ms": _to_float_or_none(r[28]),
        "insert_min_latency_ms": _to_float_or_none(r[29]),
        "insert_max_latency_ms": _to_float_or_none(r[30]),
        "update_p50_latency_ms": _to_float_or_none(r[31]),
        "update_p95_latency_ms": _to_float_or_none(r[32]),
        "update_p99_latency_ms": _to_float_or_none(r[33]),
        "update_min_latency_ms": _to_float_or_none(r[34]),
        "update_max_latency_ms": _to_float_or_none(r[35]),
    }


async def _fetch_sf_execution_latency_summary_for_run(
    *,
    pool: Any,
    parent_run_id: str,
    parent_test_id: str,
) -> dict[str, Any]:
    """
    Compute SQL execution percentiles across all child tests for a parent run.
    Also includes enrichment ratio stats and estimates when enrichment is low.
    """
    prefix = _prefix()

    enrichment_query = f"""
    SELECT
        COUNT(*) AS total_queries,
        COUNT(qe.SF_EXECUTION_MS) AS enriched_queries,
        PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY qe.APP_ELAPSED_MS - qe.SF_TOTAL_ELAPSED_MS) AS p50_overhead_ms
    FROM {prefix}.QUERY_EXECUTIONS qe
    WHERE qe.TEST_ID IN (
        SELECT TEST_ID FROM {prefix}.TEST_RESULTS WHERE RUN_ID = ?
    )
      AND COALESCE(qe.WARMUP, FALSE) = FALSE
      AND qe.SUCCESS = TRUE
    """
    enrichment_rows = await pool.execute_query(enrichment_query, params=[parent_run_id])
    total_queries = int(enrichment_rows[0][0] or 0) if enrichment_rows else 0
    enriched_queries = int(enrichment_rows[0][1] or 0) if enrichment_rows else 0
    p50_overhead_ms = (
        _to_float_or_none(enrichment_rows[0][2]) if enrichment_rows else None
    )
    enrichment_ratio = enriched_queries / total_queries if total_queries > 0 else 0.0

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
    FROM {prefix}.QUERY_EXECUTIONS qe
    WHERE qe.TEST_ID IN (
        SELECT TEST_ID FROM {prefix}.TEST_RESULTS WHERE RUN_ID = ?
    )
      AND COALESCE(qe.WARMUP, FALSE) = FALSE
      AND qe.SUCCESS = TRUE
      AND qe.SF_EXECUTION_MS IS NOT NULL
    """
    rows = await pool.execute_query(query, params=[parent_run_id])
    if not rows:
        return {
            "sf_latency_available": False,
            "sf_latency_sample_count": 0,
            "sf_enrichment_total_queries": total_queries,
            "sf_enrichment_enriched_queries": enriched_queries,
            "sf_enrichment_ratio_pct": round(enrichment_ratio * 100, 1),
            "sf_enrichment_low_warning": enrichment_ratio < 0.5 and total_queries > 100,
        }

    r = rows[0]
    sample_count = int(r[0] or 0)
    low_enrichment = enrichment_ratio < 0.5 and total_queries > 100

    payload = {
        "sf_latency_available": sample_count > 0,
        "sf_latency_sample_count": sample_count,
        "sf_enrichment_total_queries": total_queries,
        "sf_enrichment_enriched_queries": enriched_queries,
        "sf_enrichment_ratio_pct": round(enrichment_ratio * 100, 1),
        "sf_enrichment_low_warning": low_enrichment,
        "sf_enrichment_p50_overhead_ms": round(p50_overhead_ms, 2)
        if p50_overhead_ms is not None
        else None,
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

    if low_enrichment and p50_overhead_ms is not None and p50_overhead_ms > 0:
        est_query = f"""
        SELECT
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY GREATEST(0, qe.APP_ELAPSED_MS - ?)) AS EST_P50_MS,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY GREATEST(0, qe.APP_ELAPSED_MS - ?)) AS EST_P95_MS,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY GREATEST(0, qe.APP_ELAPSED_MS - ?)) AS EST_P99_MS,
            PERCENTILE_CONT(0.50) WITHIN GROUP (
                ORDER BY IFF(qe.QUERY_KIND = 'POINT_LOOKUP', GREATEST(0, qe.APP_ELAPSED_MS - ?), NULL)
            ) AS EST_POINT_LOOKUP_P50_MS,
            PERCENTILE_CONT(0.50) WITHIN GROUP (
                ORDER BY IFF(qe.QUERY_KIND = 'RANGE_SCAN', GREATEST(0, qe.APP_ELAPSED_MS - ?), NULL)
            ) AS EST_RANGE_SCAN_P50_MS
        FROM {prefix}.QUERY_EXECUTIONS qe
        WHERE qe.TEST_ID IN (
            SELECT TEST_ID FROM {prefix}.TEST_RESULTS WHERE RUN_ID = ?
        )
          AND COALESCE(qe.WARMUP, FALSE) = FALSE
          AND qe.SUCCESS = TRUE
        """
        overhead = p50_overhead_ms
        est_rows = await pool.execute_query(
            est_query,
            params=[
                overhead,
                overhead,
                overhead,
                overhead,
                overhead,
                parent_run_id,
            ],
        )
        if est_rows and est_rows[0]:
            er = est_rows[0]
            payload["sf_estimated_available"] = True
            payload["sf_estimated_p50_latency_ms"] = _to_float_or_none(er[0])
            payload["sf_estimated_p95_latency_ms"] = _to_float_or_none(er[1])
            payload["sf_estimated_p99_latency_ms"] = _to_float_or_none(er[2])
            payload["sf_estimated_point_lookup_p50_latency_ms"] = _to_float_or_none(
                er[3]
            )
            payload["sf_estimated_range_scan_p50_latency_ms"] = _to_float_or_none(er[4])

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
    """Create a new run from template via OrchestratorService.

    This endpoint delegates to the orchestrator which properly creates both
    RUN_STATUS and TEST_RESULTS entries. The run is created in PREPARED state.

    All scaling modes (AUTO, BOUNDED, FIXED) use the same orchestrator path.
    FIXED mode simply means no auto-scaling - the template runs with exactly
    the specified workers/connections.
    """
    try:
        template = await registry._load_template(template_id)
        template_config = dict(template.get("config") or {})
        template_name = str(template.get("template_name") or "")

        # Create scenario from template config
        scenario = registry._scenario_from_template_config(
            template_name, template_config
        )

        # Use OrchestratorService to create the run (creates RUN_STATUS + TEST_RESULTS)
        run_id = await orchestrator.create_run(
            template_id=str(template.get("template_id") or template_id),
            template_config=template_config,
            scenario=scenario,
        )
        return RunTemplateResponse(
            test_id=run_id,
            dashboard_url=f"/dashboard/{run_id}",
        )
    except HTTPException:
        raise
    except KeyError:
        raise HTTPException(status_code=404, detail="Template not found")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise http_exception("create run from template", e)


@router.post(
    "/from-template/{template_id}/autoscale",
    response_model=RunTemplateResponse,
    status_code=status.HTTP_201_CREATED,
)
async def run_from_template_autoscale(template_id: str) -> RunTemplateResponse:
    """Create a new autoscale run from template via OrchestratorService.

    Legacy endpoint retained for UI compatibility. FIXED scaling mode is rejected.
    """
    try:
        template = await registry._load_template(template_id)
        template_config = dict(template.get("config") or {})
        scaling_cfg = dict(template_config.get("scaling") or {})
        scaling_mode = str(scaling_cfg.get("mode") or "").strip().upper()
        if scaling_mode == "FIXED":
            raise HTTPException(
                status_code=400,
                detail="FIXED scaling mode is not allowed for autoscale endpoint",
            )

        template_name = str(template.get("template_name") or "")
        scenario = registry._scenario_from_template_config(
            template_name, template_config
        )
        run_id = await orchestrator.create_run(
            template_id=str(template.get("template_id") or template_id),
            template_config=template_config,
            scenario=scenario,
        )
        return RunTemplateResponse(
            test_id=run_id,
            dashboard_url=f"/dashboard/{run_id}",
        )
    except HTTPException:
        raise
    except KeyError:
        raise HTTPException(status_code=404, detail="Template not found")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise http_exception("create autoscale run from template", e)


@router.post("/{test_id}/start-autoscale", status_code=status.HTTP_202_ACCEPTED)
async def start_autoscale_test(test_id: str) -> dict[str, Any]:
    """Start a prepared run via OrchestratorService.

    This endpoint delegates to the orchestrator which properly updates RUN_STATUS,
    emits START events, and spawns workers.
    """
    try:
        # Start the run via orchestrator (handles RUN_STATUS, workers, etc.)
        await orchestrator.start_run(run_id=test_id)

        # Get the updated status
        status_row = await orchestrator.get_run_status(test_id)
        status_val = (
            str(status_row.get("status") or "").upper()
            if status_row is not None
            else "RUNNING"
        )
        return {"test_id": test_id, "status": status_val}
    except ValueError:
        raise HTTPException(status_code=404, detail="Run not found")
    except Exception as e:
        raise http_exception("start run", e)


@router.post("/{test_id}/start", status_code=status.HTTP_202_ACCEPTED)
async def start_prepared_test(test_id: str) -> dict[str, Any]:
    """Start a prepared run via OrchestratorService.

    This endpoint delegates to the orchestrator which properly updates RUN_STATUS,
    emits START events, and spawns workers.
    """
    try:
        pool = snowflake_pool.get_default_pool()
        run_status = await _fetch_run_status(pool, test_id)
        if not run_status:
            raise HTTPException(status_code=404, detail="Run not found")

        await orchestrator.start_run(run_id=str(test_id))
        updated = await _fetch_run_status(pool, test_id)
        status_val = str((updated or run_status).get("status") or "RUNNING").upper()
        return {"test_id": test_id, "status": status_val}
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise http_exception("start prepared test", e)


@router.post("/{test_id}/stop", status_code=status.HTTP_202_ACCEPTED)
async def stop_test(test_id: str) -> dict[str, Any]:
    """Stop a running test via OrchestratorService.

    This endpoint delegates to the orchestrator which properly updates RUN_STATUS
    and signals workers to stop.
    """
    try:
        pool = snowflake_pool.get_default_pool()
        run_status = await _fetch_run_status(pool, test_id)
        if not run_status:
            raise HTTPException(status_code=404, detail="Run not found")

        await orchestrator.stop_run(run_id=str(test_id))
        updated = await _fetch_run_status(pool, test_id)
        status_val = str((updated or run_status).get("status") or "CANCELLING").upper()
        return {"test_id": test_id, "status": status_val}
    except HTTPException:
        raise
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
    load_mode: str = "",
    scaling_mode: str = "",
    search_query: str = "",
) -> dict[str, Any]:
    try:
        pool = snowflake_pool.get_default_pool()

        where_clauses: list[str] = []
        params: list[Any] = []

        if table_type:
            where_clauses.append("tr.TABLE_TYPE = ?")
            params.append(table_type.upper())
        if warehouse_size:
            where_clauses.append("tr.WAREHOUSE_SIZE = ?")
            params.append(warehouse_size.upper())
        if status_filter:
            where_clauses.append("tr.STATUS = ?")
            params.append(status_filter.upper())
        if load_mode:
            where_clauses.append("COALESCE(tr.TEST_CONFIG:template_config:load_mode::STRING, 'CONCURRENCY') = ?")
            params.append(load_mode.upper())
        if scaling_mode:
            where_clauses.append("COALESCE(tr.TEST_CONFIG:template_config:scaling:mode::STRING, 'FIXED') = ?")
            params.append(scaling_mode.upper())
        if search_query and search_query.strip():
            like = f"%{search_query.strip().lower()}%"
            where_clauses.append("""(
                LOWER(tr.TEST_NAME) LIKE ?
                OR LOWER(tr.SCENARIO_NAME) LIKE ?
                OR LOWER(tr.TABLE_NAME) LIKE ?
                OR LOWER(tr.WAREHOUSE) LIKE ?
                OR LOWER(tr.WAREHOUSE_SIZE) LIKE ?
                OR LOWER(tr.TABLE_TYPE) LIKE ?
            )""")
            params.extend([like] * 6)

        if date_range in {"today", "week", "month"}:
            days = {"today": 1, "week": 7, "month": 30}[date_range]
            where_clauses.append("tr.START_TIME >= DATEADD(day, ?, CURRENT_TIMESTAMP())")
            params.append(-days)
        elif date_range == "custom":

            def _is_date(value: str) -> bool:
                return bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", value or ""))

            if _is_date(start_date):
                where_clauses.append("tr.START_TIME >= TO_DATE(?)")
                params.append(start_date)
            if _is_date(end_date):
                where_clauses.append("tr.START_TIME < DATEADD(day, 1, TO_DATE(?))")
                params.append(end_date)

        where_clauses.append(
            "(tr.RUN_ID IS NULL OR tr.RUN_ID = tr.TEST_ID)"
        )

        where_sql = ""
        if where_clauses:
            where_sql = "WHERE " + " AND ".join(where_clauses)

        offset = max(page - 1, 0) * page_size
        query = f"""
        SELECT
            tr.TEST_ID,
            tr.RUN_ID,
            tr.TEST_NAME,
            tr.TABLE_TYPE,
            tr.WAREHOUSE_SIZE,
            tr.START_TIME,
            tr.QPS,
            tr.P95_LATENCY_MS,
            tr.P99_LATENCY_MS,
            tr.ERROR_RATE,
            tr.STATUS,
            tr.CONCURRENT_CONNECTIONS,
            tr.DURATION_SECONDS,
            tr.FAILURE_REASON,
            tr.ENRICHMENT_STATUS,
            tr.TEST_CONFIG:template_config:postgres_instance_size::STRING AS POSTGRES_INSTANCE_SIZE,
            tr.TEST_CONFIG:template_config:load_mode::STRING AS LOAD_MODE,
            tr.TEST_CONFIG:template_config:target_qps::NUMBER AS TARGET_QPS,
            tr.TEST_CONFIG:template_config:start_concurrency::NUMBER AS START_CONCURRENCY,
            tr.TEST_CONFIG:template_config:concurrency_increment::NUMBER AS CONCURRENCY_INCREMENT,
            tr.TEST_CONFIG:template_config:scaling AS SCALING_CONFIG,
            tr.TEST_CONFIG:template_config:custom_point_lookup_pct::NUMBER AS CUSTOM_POINT_LOOKUP_PCT,
            tr.TEST_CONFIG:template_config:custom_range_scan_pct::NUMBER AS CUSTOM_RANGE_SCAN_PCT,
            tr.TEST_CONFIG:template_config:custom_insert_pct::NUMBER AS CUSTOM_INSERT_PCT,
            tr.TEST_CONFIG:template_config:custom_update_pct::NUMBER AS CUSTOM_UPDATE_PCT,
            rs.STATUS AS RUN_STATUS,
            rs.PHASE AS RUN_PHASE
        FROM {_prefix()}.TEST_RESULTS tr
        LEFT JOIN {_prefix()}.RUN_STATUS rs
          ON rs.RUN_ID = tr.TEST_ID
        {where_sql}
        ORDER BY tr.START_TIME DESC
        LIMIT ? OFFSET ?
        """
        rows = await pool.execute_query(query, params=[*params, page_size, offset])

        count_query = f"SELECT COUNT(*) FROM {_prefix()}.TEST_RESULTS tr {where_sql}"
        count_rows = await pool.execute_query(count_query, params=params)
        total = int(count_rows[0][0]) if count_rows else 0
        total_pages = max((total + page_size - 1) // page_size, 1)

        results = []
        for row in rows:
            (
                test_id,
                run_id,
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
                enrichment_status,
                postgres_instance_size,
                load_mode,
                target_qps,
                start_concurrency,
                concurrency_increment,
                scaling_config_raw,
                custom_point_lookup_pct,
                custom_range_scan_pct,
                custom_insert_pct,
                custom_update_pct,
                run_status_db,
                run_phase_db,
            ) = row

            # Parse scaling config JSON if present
            scaling_config = None
            if scaling_config_raw:
                try:
                    if isinstance(scaling_config_raw, str):
                        scaling_config = json.loads(scaling_config_raw)
                    else:
                        scaling_config = scaling_config_raw
                except Exception:
                    pass

            is_parent_run = bool(run_id) and str(run_id) == str(test_id)
            status_db_u = str(status_db or "").upper()
            run_status_u = str(run_status_db or "").upper()
            live_statuses = {
                "PREPARED",
                "READY",
                "PENDING",
                "STARTING",
                "RUNNING",
                "STOPPING",
                "CANCELLING",
                "PROCESSING",
            }
            effective_status = status_db_u or status_db
            phase = None
            if is_parent_run and run_status_u and (
                run_status_u in live_statuses or status_db_u in live_statuses
            ):
                # Reconcile stale/active parent states from RUN_STATUS.
                effective_status = run_status_u
                phase = str(run_phase_db or "").upper() or None

            # For in-memory tests, use the latest phase from the registry.
            try:
                running = await registry.get(test_id)
                if running is not None and isinstance(running.last_payload, dict):
                    phase = running.last_payload.get("phase")
            except Exception:
                pass
            if (
                not phase
                and str(effective_status or "").upper() == "COMPLETED"
                and str(enrichment_status or "").upper() == "PENDING"
            ):
                phase = "PROCESSING"
            status_out = effective_status
            if (
                str(effective_status or "").upper() == "COMPLETED"
                and str(phase or "").upper() == "PROCESSING"
            ):
                status_out = "PROCESSING"

            results.append(
                {
                    "test_id": test_id,
                    "run_id": run_id,
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
                    "status": status_out,
                    "phase": phase,
                    "enrichment_status": enrichment_status,
                    "concurrent_connections": int(concurrency or 0),
                    "duration": float(duration or 0),
                    "failure_reason": failure_reason,
                    # Load mode fields
                    "load_mode": load_mode or "CONCURRENCY",
                    "target_qps": int(target_qps or 0) if target_qps else None,
                    "start_concurrency": int(start_concurrency or 0) if start_concurrency else None,
                    "concurrency_increment": int(concurrency_increment or 0) if concurrency_increment else None,
                    # Scaling config
                    "scaling": scaling_config,
                    # Workload mix
                    "custom_point_lookup_pct": int(custom_point_lookup_pct or 0) if custom_point_lookup_pct is not None else None,
                    "custom_range_scan_pct": int(custom_range_scan_pct or 0) if custom_range_scan_pct is not None else None,
                    "custom_insert_pct": int(custom_insert_pct or 0) if custom_insert_pct is not None else None,
                    "custom_update_pct": int(custom_update_pct or 0) if custom_update_pct is not None else None,
                    **_build_cost_fields(
                        float(duration or 0),
                        wh_size,
                        total_operations=int(float(ops or 0) * float(duration or 0)),
                        qps=float(ops or 0),
                        table_type=table_type_db,
                        postgres_instance_size=postgres_instance_size,
                    ),
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
            ERROR_RATE,
            DURATION_SECONDS,
            TEST_CONFIG:template_config:postgres_instance_size::STRING AS POSTGRES_INSTANCE_SIZE
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
                duration,
                postgres_instance_size,
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
                    "duration": float(duration or 0),
                    **_build_cost_fields(
                        float(duration or 0),
                        wh_size,
                        total_operations=int(float(ops or 0) * float(duration or 0)),
                        qps=float(ops or 0),
                        table_type=table_type_db,
                        postgres_instance_size=postgres_instance_size,
                    ),
                }
            )
        return {"results": results}
    except Exception as e:
        raise http_exception("search tests", e)


# ---------------------------------------------------------------------------
# Helper functions for parallel query execution in get_test()
# ---------------------------------------------------------------------------


async def _fetch_worker_find_max_results(
    *, pool: Any, run_id: str, test_id: str
) -> list[dict[str, Any]]:
    """Fetch per-worker find_max_result for a parent run."""
    prefix = _prefix()
    rows = await pool.execute_query(
        f"""
        SELECT TEST_ID, FIND_MAX_RESULT
        FROM {prefix}.TEST_RESULTS
        WHERE RUN_ID = ?
          AND TEST_ID <> ?
          AND FIND_MAX_RESULT IS NOT NULL
        ORDER BY START_TIME ASC
        """,
        params=[run_id, test_id],
    )
    results = []
    for idx, (child_test_id, child_fmr_raw) in enumerate(rows):
        if child_fmr_raw:
            child_fmr = (
                json.loads(child_fmr_raw)
                if isinstance(child_fmr_raw, str)
                else child_fmr_raw
            )
            results.append(
                {
                    "worker_index": idx + 1,
                    "test_id": child_test_id,
                    "find_max_result": child_fmr,
                }
            )
    return results


async def _fetch_qps_controller_step_history(
    *, pool: Any, run_id: str
) -> list[dict[str, Any]]:
    """Fetch QPS controller step history for a run from CONTROLLER_STEP_HISTORY."""
    prefix = _prefix()
    rows = await pool.execute_query(
        f"""
        SELECT
            STEP_NUMBER,
            STEP_TYPE,
            FROM_THREADS,
            TO_THREADS,
            DIRECTION,
            STEP_START_TIME,
            QPS,
            TARGET_QPS,
            QPS_ERROR_PCT,
            OUTCOME
        FROM {prefix}.CONTROLLER_STEP_HISTORY
        WHERE RUN_ID = ?
          AND STEP_TYPE = 'QPS_SCALING'
        ORDER BY STEP_NUMBER ASC
        """,
        params=[run_id],
    )
    results = []
    for row in rows:
        (
            step_number,
            step_type,
            from_threads,
            to_threads,
            direction,
            step_start_time,
            qps,
            target_qps,
            qps_error_pct,
            outcome,
        ) = row
        results.append(
            {
                "step": step_number,
                "from_threads": int(from_threads) if from_threads is not None else None,
                "to_threads": int(to_threads) if to_threads is not None else None,
                "direction": str(direction).lower() if direction else None,
                "timestamp": step_start_time.isoformat() if hasattr(step_start_time, "isoformat") else str(step_start_time),
                "qps": float(qps) if qps is not None else None,
                "target_qps": float(target_qps) if target_qps is not None else None,
                "qps_error_pct": float(qps_error_pct) if qps_error_pct is not None else None,
                "reason": str(outcome).lower() if outcome else None,
            }
        )
    return results


async def _fetch_error_rates(
    *, pool: Any, run_id: str, test_id: str, is_parent_run: bool
) -> dict[str, Any]:
    """Fetch per-query-kind error rates and counts from QUERY_EXECUTIONS."""
    prefix = _prefix()
    result: dict[str, Any] = {
        "point_lookup_error_rate_pct": None,
        "range_scan_error_rate_pct": None,
        "insert_error_rate_pct": None,
        "update_error_rate_pct": None,
        "point_lookup_count": 0,
        "range_scan_count": 0,
        "insert_count": 0,
        "update_count": 0,
    }

    if is_parent_run:
        # Query executions may be stored under:
        # 1. Worker TEST_IDs (old behavior): tr.TEST_ID <> parent TEST_ID
        # 2. Parent TEST_ID (streaming mode): qe.TEST_ID = run_id directly
        # We union both to handle either case.
        err_rows = await pool.execute_query(
            f"""
            SELECT QUERY_KIND, SUM(N) AS N, SUM(ERR) AS ERR
            FROM (
                -- Worker-linked executions (old behavior)
                SELECT
                    qe.QUERY_KIND,
                    COUNT(*) AS N,
                    SUM(IFF(qe.SUCCESS, 0, 1)) AS ERR
                FROM {prefix}.QUERY_EXECUTIONS qe
                JOIN {prefix}.TEST_RESULTS tr
                  ON tr.TEST_ID = qe.TEST_ID
                WHERE tr.RUN_ID = ?
                  AND tr.TEST_ID <> ?
                  AND COALESCE(qe.WARMUP, FALSE) = FALSE
                GROUP BY qe.QUERY_KIND

                UNION ALL

                -- Parent-linked executions (streaming mode)
                SELECT
                    QUERY_KIND,
                    COUNT(*) AS N,
                    SUM(IFF(SUCCESS, 0, 1)) AS ERR
                FROM {prefix}.QUERY_EXECUTIONS
                WHERE TEST_ID = ?
                  AND COALESCE(WARMUP, FALSE) = FALSE
                GROUP BY QUERY_KIND
            )
            GROUP BY QUERY_KIND
            """,
            params=[run_id, test_id, run_id],
        )
    else:
        err_rows = await pool.execute_query(
            f"""
            SELECT QUERY_KIND, COUNT(*) AS N, SUM(IFF(SUCCESS, 0, 1)) AS ERR
            FROM {prefix}.QUERY_EXECUTIONS
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
        result[err_key] = (numer / denom * 100.0) if denom > 0 else 0.0
        if count_key:
            result[count_key] = int(n or 0)

    return result


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

        def _coerce_optional_int(value: Any) -> int | None:
            if value is None:
                return None
            if isinstance(value, str) and not value.strip():
                return None
            try:
                out = int(float(value))
            except Exception:
                return None
            if out == -1:
                return None
            return out

        query = f"""
        SELECT
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
            END_TIME,
            DURATION_SECONDS,
            CONCURRENT_CONNECTIONS,
            TEST_CONFIG,
            CUSTOM_METRICS,
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

        # ---------------------------------------------------------------------------
        # Phase 1: Initial parallel fetch - TEST_RESULTS + RUN_STATUS + enrichment
        # These queries are independent and can run concurrently to reduce latency.
        # Previously these were sequential, adding ~1.5-3s of round-trip time.
        # ---------------------------------------------------------------------------
        initial_results = await asyncio.gather(
            pool.execute_query(query, params=[test_id]),
            _fetch_run_status(pool, str(test_id)),
            _aggregate_parent_enrichment_status(pool=pool, run_id=str(test_id)),
            return_exceptions=True,
        )

        rows = (
            initial_results[0] if not isinstance(initial_results[0], Exception) else []
        )
        prefetched_run_status = (
            initial_results[1]
            if not isinstance(initial_results[1], Exception)
            else None
        )
        prefetched_enrichment = (
            initial_results[2]
            if not isinstance(initial_results[2], Exception)
            else (None, None)
        )

        if rows:
            row = rows[0]
            status_db = row[8]
            end_time = row[10]
            run_id = row[1]
            run_status_value = (
                str((prefetched_run_status or {}).get("status") or "").upper()
                if isinstance(prefetched_run_status, dict)
                else ""
            )
            if (
                run_id
                and str(run_id) == str(test_id)
                and str(status_db or "").upper() == "RUNNING"
                and (
                    end_time is not None
                    or run_status_value in {"COMPLETED", "FAILED", "CANCELLED"}
                )
            ):
                # Fire-and-forget repair for stale parent rows in TEST_RESULTS.
                asyncio.create_task(
                    update_parent_run_aggregate(parent_run_id=str(test_id))
                )
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
                scaling_cfg = cfg.get("scaling") if isinstance(cfg, dict) else None
                scaling_payload = None
                if isinstance(scaling_cfg, dict):
                    scaling_mode = (
                        str(scaling_cfg.get("mode") or "AUTO").strip().upper() or "AUTO"
                    )
                    if scaling_mode not in {"AUTO", "BOUNDED", "FIXED"}:
                        scaling_mode = "AUTO"
                    min_workers = (
                        _coerce_optional_int(scaling_cfg.get("min_workers")) or 1
                    )
                    max_workers = _coerce_optional_int(scaling_cfg.get("max_workers"))
                    min_connections = (
                        _coerce_optional_int(scaling_cfg.get("min_connections")) or 1
                    )
                    max_connections = _coerce_optional_int(
                        scaling_cfg.get("max_connections")
                    )
                    scaling_payload = {
                        "mode": scaling_mode,
                        "min_workers": int(min_workers),
                        "max_workers": int(max_workers)
                        if max_workers is not None
                        else None,
                        "min_connections": int(min_connections),
                        "max_connections": int(max_connections)
                        if max_connections is not None
                        else None,
                    }
                table_type_u = (
                    str(
                        getattr(running.scenario.table_configs[0], "table_type", "")
                        or ""
                    )
                    .strip()
                    .upper()
                )
                is_postgres = table_type_u == "POSTGRES"
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
                    "duration": None
                    if load_mode == "FIND_MAX_CONCURRENCY"
                    else float(
                        (running.scenario.duration_seconds or 0)
                        + (running.scenario.warmup_seconds or 0)
                    ),
                    "created_at": running.created_at.isoformat(),
                    "concurrent_connections": int(running.scenario.total_threads or 0),
                    "ops_per_sec": 0.0,
                    "p50_latency": 0.0,
                    "p95_latency": 0.0,
                    "p99_latency": 0.0,
                    "error_rate": 0.0,
                    "latency_aggregation_method": None,
                    "load_mode": load_mode,
                    "start_concurrency": (
                        _coerce_optional_int(getattr(running.scenario, "start_concurrency", None))
                        or _coerce_optional_int(cfg.get("start_concurrency"))
                    ) if load_mode == "FIND_MAX_CONCURRENCY" else None,
                    "concurrency_increment": (
                        _coerce_optional_int(getattr(running.scenario, "concurrency_increment", None))
                        or _coerce_optional_int(cfg.get("concurrency_increment"))
                    ) if load_mode == "FIND_MAX_CONCURRENCY" else None,
                    "scaling": scaling_payload,
                    "target_qps": (
                        float(getattr(running.scenario, "target_qps", 0.0) or 0.0)
                        if load_mode == "QPS"
                        else None
                    ),
                    "min_connections": int(
                        getattr(running.scenario, "min_threads_per_worker", 1) or 1
                    )
                    if load_mode == "QPS"
                    else None,
                    "qps_target_mode": "QPS" if load_mode == "QPS" else None,
                    "workload_type": str(running.scenario.workload_type),
                    # Derive autoscale_enabled from scaling.mode for backward compatibility
                    "autoscale_enabled": str(
                        (cfg.get("scaling") or {}).get("mode") or "AUTO"
                    ).upper()
                    != "FIXED",
                    "autoscale_max_cpu_percent": _num_from_dict(
                        cfg.get("guardrails") or cfg, "max_cpu_percent"
                    )
                    or _num_from_dict(cfg, "autoscale_max_cpu_percent"),
                    "autoscale_max_memory_percent": _num_from_dict(
                        cfg.get("guardrails") or cfg, "max_memory_percent"
                    )
                    or _num_from_dict(cfg, "autoscale_max_memory_percent"),
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
                # Add timing info for in-memory tests
                warmup_secs = int(running.scenario.warmup_seconds or 0)
                run_secs = int(running.scenario.duration_seconds or 0)
                total_expected = warmup_secs + run_secs
                from datetime import datetime, timezone

                now = datetime.now(timezone.utc)
                created_at = running.created_at
                if created_at and not hasattr(created_at, "tzinfo"):
                    created_at = created_at.replace(tzinfo=timezone.utc)
                elif created_at and created_at.tzinfo is None:
                    created_at = created_at.replace(tzinfo=timezone.utc)
                elapsed_secs = (now - created_at).total_seconds() if created_at else 0.0
                elapsed_secs = max(0.0, elapsed_secs)
                payload["timing"] = {
                    "warmup_seconds": warmup_secs,
                    "run_seconds": run_secs,
                    "total_expected_seconds": total_expected,
                    "elapsed_display_seconds": round(elapsed_secs, 1),
                }
                # Add cost estimation for running tests (based on elapsed time)
                payload.update(
                    _build_cost_fields(
                        duration_seconds=elapsed_secs,
                        warehouse_size=cfg.get("warehouse_size"),
                        table_type=table_type_u,
                        postgres_instance_size=cfg.get("postgres_instance_size"),
                    )
                )
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
                # Compute latency spread ratio (P95/P50) - will be null for running tests
                payload.update(
                    _compute_latency_spread(
                        p50=payload.get("p50_latency_ms") or payload.get("p50_latency"),
                        p95=payload.get("p95_latency_ms") or payload.get("p95_latency"),
                    )
                )
                return payload
            raise HTTPException(status_code=404, detail="Test not found")

        (
            _,
            run_id,
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
            custom_metrics,
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

        is_parent_run = bool(run_id) and str(run_id) == str(test_id)
        latency_aggregation_method = (
            LATENCY_AGGREGATION_METHOD if is_parent_run else None
        )

        # Use prefetched run_status and enrichment from Phase 1 parallel fetch
        run_status = prefetched_run_status if is_parent_run else None
        status_live = str(status_db or "").upper()
        phase_live: str | None = None
        cancellation_reason_live: str | None = None
        start_time_live = start_time
        end_time_live = end_time
        if is_parent_run and run_status:
            run_status_value = str(run_status.get("status") or "").upper()
            live_statuses = {
                "PREPARED",
                "READY",
                "PENDING",
                "STARTING",
                "RUNNING",
                "STOPPING",
                "CANCELLING",
                "PROCESSING",
            }
            if run_status_value and (
                run_status_value in live_statuses or status_live in live_statuses
            ):
                status_live = run_status_value
                phase_live = run_status.get("phase") or phase_live
                cancellation_reason_live = run_status.get("cancellation_reason")
                start_time_live = run_status.get("start_time") or start_time_live
                end_time_live = run_status.get("end_time") or end_time_live

        enrichment_status_live = enrichment_status
        enrichment_error_live = enrichment_error
        if is_parent_run and run_id:
            agg_status, agg_error = prefetched_enrichment
            if agg_status:
                enrichment_status_live = agg_status
                enrichment_error_live = agg_error

        cfg = test_config
        if isinstance(cfg, str):
            cfg = json.loads(cfg)

        custom_metrics_raw: Any = custom_metrics
        if isinstance(custom_metrics_raw, str):
            try:
                custom_metrics_raw = json.loads(custom_metrics_raw)
            except Exception:
                custom_metrics_raw = None
        bounds_state = None
        if isinstance(custom_metrics_raw, dict):
            bounds_state = custom_metrics_raw.get("bounds_state")
            if isinstance(bounds_state, str):
                try:
                    bounds_state = json.loads(bounds_state)
                except Exception:
                    bounds_state = None

        find_max_result = None
        if find_max_result_raw:
            if isinstance(find_max_result_raw, str):
                find_max_result = json.loads(find_max_result_raw)
            else:
                find_max_result = find_max_result_raw
        if is_parent_run and run_status and run_status.get("find_max_state"):
            find_max_state = run_status.get("find_max_state")
            if isinstance(find_max_state, str):
                find_max_state = json.loads(find_max_state)
            if isinstance(find_max_state, dict):
                find_max_result = find_max_state

        template_name = cfg.get("template_name") if isinstance(cfg, dict) else None
        template_id = cfg.get("template_id") if isinstance(cfg, dict) else None
        template_cfg = cfg.get("template_config") if isinstance(cfg, dict) else None
        scenario_cfg = cfg.get("scenario") if isinstance(cfg, dict) else None
        workload_type = None
        if isinstance(template_cfg, dict):
            workload_type = template_cfg.get("workload_type")
        
        # Extract load_mode - check template_config first, then scenario as fallback
        load_mode = "CONCURRENCY"
        if isinstance(template_cfg, dict) and template_cfg.get("load_mode"):
            load_mode = str(template_cfg.get("load_mode")).strip().upper()
        elif isinstance(scenario_cfg, dict) and scenario_cfg.get("load_mode"):
            load_mode = str(scenario_cfg.get("load_mode")).strip().upper()
        
        if load_mode not in {"CONCURRENCY", "QPS", "FIND_MAX_CONCURRENCY"}:
            load_mode = "CONCURRENCY"
        
        # Extract FIND_MAX_CONCURRENCY specific fields from template_config or scenario
        start_concurrency = None
        concurrency_increment = None
        if load_mode == "FIND_MAX_CONCURRENCY":
            if isinstance(template_cfg, dict):
                start_concurrency = _coerce_optional_int(template_cfg.get("start_concurrency"))
                concurrency_increment = _coerce_optional_int(template_cfg.get("concurrency_increment"))
            if start_concurrency is None and isinstance(scenario_cfg, dict):
                start_concurrency = _coerce_optional_int(scenario_cfg.get("start_concurrency"))
            if concurrency_increment is None and isinstance(scenario_cfg, dict):
                concurrency_increment = _coerce_optional_int(scenario_cfg.get("concurrency_increment"))
        
        target_qps = None
        if load_mode == "QPS" and isinstance(template_cfg, dict):
            try:
                v = template_cfg.get("target_qps")
                target_qps = float(v) if v is not None else None
            except Exception:
                target_qps = None
        min_connections = None
        scaling_cfg = (
            template_cfg.get("scaling") if isinstance(template_cfg, dict) else None
        )
        scaling_payload = None
        if isinstance(scaling_cfg, dict):
            scaling_mode = (
                str(scaling_cfg.get("mode") or "AUTO").strip().upper() or "AUTO"
            )
            if scaling_mode not in {"AUTO", "BOUNDED", "FIXED"}:
                scaling_mode = "AUTO"
            min_workers = _coerce_optional_int(scaling_cfg.get("min_workers")) or 1
            max_workers = _coerce_optional_int(scaling_cfg.get("max_workers"))
            min_connections_val = (
                _coerce_optional_int(scaling_cfg.get("min_connections")) or 1
            )
            max_connections = _coerce_optional_int(scaling_cfg.get("max_connections"))
            scaling_payload = {
                "mode": scaling_mode,
                "min_workers": int(min_workers),
                "max_workers": int(max_workers) if max_workers is not None else None,
                "min_connections": int(min_connections_val),
                "max_connections": int(max_connections)
                if max_connections is not None
                else None,
            }
            if load_mode == "QPS":
                min_connections = int(min_connections_val)
        if load_mode == "QPS" and min_connections is None:
            min_connections = 1
        table_type_u = str(table_type or "").strip().upper()
        is_postgres = table_type_u == "POSTGRES"
        error_rate_pct = 0.0
        if total_operations:
            error_rate_pct = (
                float(failed_operations or 0) / float(total_operations or 0)
            ) * 100.0

        payload: dict[str, Any] = {
            "test_id": test_id,
            "run_id": run_id,
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
            "status": status_live,
            "start_time": start_time_live.isoformat()
            if hasattr(start_time_live, "isoformat")
            else str(start_time_live),
            "created_at": start_time_live.isoformat()
            if hasattr(start_time_live, "isoformat")
            else str(start_time_live),
            "end_time": end_time_live.isoformat()
            if end_time_live and hasattr(end_time_live, "isoformat")
            else None,
            # Use configured duration from template, not actual elapsed time
            "duration_seconds": int(
                float(
                    (cfg.get("scenario", {}) if isinstance(cfg, dict) else {}).get("duration_seconds")
                    or (cfg.get("duration") if isinstance(cfg, dict) else 0)
                    or 0
                )
            ),
            "duration": int(
                float(
                    (cfg.get("scenario", {}) if isinstance(cfg, dict) else {}).get("duration_seconds")
                    or (cfg.get("duration") if isinstance(cfg, dict) else 0)
                    or 0
                )
            ),
            "elapsed_seconds": float(duration_seconds or 0),  # Actual elapsed time
            "warmup_seconds": int(
                float(
                    (cfg.get("scenario", {}) if isinstance(cfg, dict) else {}).get("warmup_seconds")
                    or (cfg.get("warmup") if isinstance(cfg, dict) else 0)
                    or 0
                )
            ),
            "concurrent_connections": int(concurrency or 0),
            "load_mode": load_mode,
            "start_concurrency": start_concurrency,
            "concurrency_increment": concurrency_increment,
            "target_qps": target_qps if load_mode == "QPS" else None,
            "min_connections": min_connections if load_mode == "QPS" else None,
            "scaling": scaling_payload,
            "bounds_state": bounds_state,
            "qps_target_mode": "QPS" if load_mode == "QPS" else None,
            "workload_type": workload_type,
            # Derive autoscale_enabled from scaling.mode for backward compatibility
            "autoscale_enabled": str(
                (template_cfg.get("scaling") or {}).get("mode") or "AUTO"
            ).upper()
            != "FIXED"
            if isinstance(template_cfg, dict)
            else True,
            "autoscale_max_cpu_percent": (
                _num_from_dict(template_cfg.get("guardrails") or {}, "max_cpu_percent")
                or _num_from_dict(template_cfg, "autoscale_max_cpu_percent")
            )
            if isinstance(template_cfg, dict)
            else None,
            "autoscale_max_memory_percent": (
                _num_from_dict(
                    template_cfg.get("guardrails") or {}, "max_memory_percent"
                )
                or _num_from_dict(template_cfg, "autoscale_max_memory_percent")
            )
            if isinstance(template_cfg, dict)
            else None,
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
            "ops_per_sec": float(qps or 0),
            "reads_per_second": float(reads_per_second or 0),
            "writes_per_second": float(writes_per_second or 0),
            "rows_read": int(rows_read or 0),
            "rows_written": int(rows_written or 0),
            "avg_latency_ms": float(avg_latency_ms or 0),
            "p50_latency_ms": float(p50_latency_ms or 0),
            "p90_latency_ms": float(p90_latency_ms or 0),
            "p95_latency_ms": float(p95_latency_ms or 0),
            "p99_latency_ms": float(p99_latency_ms or 0),
            "p50_latency": float(p50_latency_ms or 0),
            "p95_latency": float(p95_latency_ms or 0),
            "p99_latency": float(p99_latency_ms or 0),
            "latency_aggregation_method": latency_aggregation_method,
            "error_rate": error_rate_pct,
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
            "enrichment_status": enrichment_status_live,
            "enrichment_error": enrichment_error_live,
            "can_retry_enrichment": (
                not is_parent_run
                and str(status_db or "").upper() == "COMPLETED"
                and str(enrichment_status_live or "").upper() == "FAILED"
            ),
            # Cost estimation fields
            **_build_cost_fields(
                duration_seconds=float(duration_seconds or 0),
                warehouse_size=warehouse_size,
                total_operations=int(total_operations or 0),
                qps=float(qps or 0),
                table_type=table_type,
                postgres_instance_size=cfg.get("template_config", {}).get("postgres_instance_size") if isinstance(cfg, dict) else None,
            ),
        }

        if phase_live:
            payload["phase"] = phase_live
        if cancellation_reason_live:
            payload["cancellation_reason"] = cancellation_reason_live

        # ---------------------------------------------------------------------------
        # Parallel query execution for performance optimization
        # These queries are independent and can run concurrently via asyncio.gather
        # ---------------------------------------------------------------------------
        status_upper = str(status_live or "").upper()
        is_terminal = status_upper in {"COMPLETED", "FAILED", "STOPPED", "CANCELLED"}

        # Build list of coroutines to run in parallel
        parallel_tasks: list[tuple[str, Any]] = []

        # 1. Worker find_max_results (parent runs only)
        if is_parent_run:
            parallel_tasks.append(
                (
                    "worker_fmr",
                    _fetch_worker_find_max_results(
                        pool=pool, run_id=str(run_id), test_id=test_id
                    ),
                )
            )

        # 2. Error rates (terminal states only)
        if is_terminal:
            parallel_tasks.append(
                (
                    "error_rates",
                    _fetch_error_rates(
                        pool=pool,
                        run_id=str(run_id) if run_id else test_id,
                        test_id=test_id,
                        is_parent_run=is_parent_run,
                    ),
                )
            )

        # 3. SF execution latency summary
        if is_parent_run:
            parallel_tasks.append(
                (
                    "sf_latency",
                    _fetch_sf_execution_latency_summary_for_run(
                        pool=pool,
                        parent_run_id=str(run_id),
                        parent_test_id=str(test_id),
                    ),
                )
            )
        else:
            parallel_tasks.append(
                (
                    "sf_latency",
                    _fetch_sf_execution_latency_summary(pool=pool, test_id=test_id),
                )
            )

        # 4. App latency summary (parent runs only)
        if is_parent_run:
            parallel_tasks.append(
                (
                    "app_latency",
                    _fetch_app_latency_summary_for_run(
                        pool=pool,
                        parent_run_id=str(run_id),
                        parent_test_id=str(test_id),
                    ),
                )
            )

        # 5. Warehouse metrics
        parallel_tasks.append(
            (
                "warehouse_metrics",
                _fetch_warehouse_metrics(pool=pool, test_id=test_id),
            )
        )

        # 6. Cluster breakdown
        parallel_tasks.append(
            (
                "cluster_breakdown",
                _fetch_cluster_breakdown(pool=pool, test_id=test_id),
            )
        )

        # 7. QPS controller step history (for QPS mode parent runs)
        if is_parent_run and load_mode == "QPS":
            parallel_tasks.append(
                (
                    "qps_step_history",
                    _fetch_qps_controller_step_history(pool=pool, run_id=str(run_id)),
                )
            )

        # 8. Postgres stats (postgres tables only)
        if is_postgres:
            parallel_tasks.append(
                (
                    "postgres_stats",
                    _fetch_postgres_stats(
                        table_type=table_type,
                        database=template_cfg.get("database")
                        if isinstance(template_cfg, dict)
                        else None,
                    ),
                )
            )
            # 8. pg_stat_statements enrichment (postgres tables only)
            parallel_tasks.append(
                (
                    "pg_enrichment",
                    _fetch_pg_enrichment(pool=pool, test_id=test_id),
                )
            )

        # Execute all queries in parallel
        task_names = [name for name, _ in parallel_tasks]
        task_coros = [coro for _, coro in parallel_tasks]

        # Use return_exceptions=True to handle individual failures gracefully
        results = await asyncio.gather(*task_coros, return_exceptions=True)

        # Process results
        for task_name, result in zip(task_names, results):
            if isinstance(result, Exception):
                logger.debug(
                    "Parallel query '%s' failed for test %s: %s",
                    task_name,
                    test_id,
                    result,
                )
                # Apply default values for failed queries
                if task_name == "worker_fmr":
                    payload["worker_find_max_results"] = []
                elif task_name == "error_rates":
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
                elif task_name == "sf_latency":
                    payload.update(
                        {"sf_latency_available": False, "sf_latency_sample_count": 0}
                    )
                elif task_name == "warehouse_metrics":
                    payload.update({"warehouse_metrics_available": False})
                elif task_name == "cluster_breakdown":
                    payload.update(
                        {"cluster_breakdown_available": False, "cluster_breakdown": []}
                    )
                elif task_name == "qps_step_history":
                    # QPS step history failure - set empty qps_controller_state
                    payload["qps_controller_state"] = {"step_history": []}
                # postgres_stats and app_latency failures are silent (no defaults needed)
            else:
                # Apply successful results
                if task_name == "worker_fmr":
                    payload["worker_find_max_results"] = result
                    if len(result) > 1:
                        payload["aggregated_find_max_result"] = (
                            _compute_aggregated_find_max(result)
                        )
                elif task_name == "error_rates":
                    # Initialize defaults first, then update with results
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
                    payload.update(result)
                elif task_name in (
                    "sf_latency",
                    "app_latency",
                    "warehouse_metrics",
                    "cluster_breakdown",
                    "postgres_stats",
                    "pg_enrichment",
                ):
                    payload.update(result)
                elif task_name == "qps_step_history":
                    # Build qps_controller_state from step history
                    # This provides historical QPS controller data for history view
                    payload["qps_controller_state"] = {
                        "mode": "QPS",
                        "target_qps": target_qps,
                        "step_history": result,
                    }

        # Set defaults for error_rates if not fetched (non-terminal states)
        if not is_terminal:
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

        if not is_parent_run:
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

        # Add timing info for all tests (needed for phase progress display)
        # All tests use the worker-based architecture (even with min_workers=1)
        try:
            scenario_cfg = cfg.get("scenario", {}) if isinstance(cfg, dict) else {}
            if not isinstance(scenario_cfg, dict):
                scenario_cfg = {}
            # Try scenario config first, then top-level config
            warmup_secs = int(
                float(scenario_cfg.get("warmup_seconds") or cfg.get("warmup") or 0)
            )
            run_secs = int(
                float(scenario_cfg.get("duration_seconds") or cfg.get("duration") or 0)
            )
            load_mode_upper = str(load_mode or "").strip().upper()
            if load_mode_upper == "FIND_MAX_CONCURRENCY":
                run_secs = 0
                total_expected_secs = 0
            else:
                total_expected_secs = warmup_secs + run_secs

            # Calculate elapsed time based on test state
            elapsed_secs = 0.0
            status_upper = str(status_live or "").upper()

            # For completed tests, prefer end_time - start_time (full run, includes warmup)
            if status_upper in {"COMPLETED", "FAILED", "CANCELLED", "STOPPED"}:
                if start_time_live and end_time_live:
                    try:
                        st = start_time_live
                        et = end_time_live
                        if hasattr(st, "timestamp") and hasattr(et, "timestamp"):
                            calc = et.timestamp() - st.timestamp()
                            if calc > 0:
                                elapsed_secs = calc
                    except Exception:
                        pass
                if (
                    elapsed_secs <= 0
                    and duration_seconds
                    and float(duration_seconds or 0) > 0
                ):
                    # Fallback to stored duration if timestamps unavailable
                    elapsed_secs = float(duration_seconds)
            elif (
                is_parent_run
                and run_status
                and run_status.get("elapsed_seconds") is not None
            ):
                # For running/stopping tests: use run_status elapsed (calculated server-side)
                elapsed_raw = run_status.get("elapsed_seconds")
                if isinstance(elapsed_raw, (int, float)):
                    elapsed_secs = max(0.0, float(elapsed_raw))
            elif duration_seconds and float(duration_seconds or 0) > 0:
                # Fallback to stored duration
                elapsed_secs = float(duration_seconds)

            # Only set timing if we don't already have it (in-memory tests set it earlier)
            if not payload.get("timing"):
                payload["timing"] = {
                    "warmup_seconds": warmup_secs,
                    "run_seconds": run_secs,
                    "total_expected_seconds": total_expected_secs,
                    "elapsed_display_seconds": round(elapsed_secs, 1),
                }
        except Exception as e:
            logger.debug("Failed to derive timing for test %s: %s", test_id, e)

        # For worker-based tests, derive phase from elapsed time when RUNNING or STOPPING
        # All tests now use the worker architecture (even with min_workers=1)
        status_for_timing = str(status_live or "").upper()
        if is_parent_run and status_for_timing in ("RUNNING", "STOPPING"):
            try:
                # Extract warmup and run seconds from the scenario config
                scenario_cfg = cfg.get("scenario", {}) if isinstance(cfg, dict) else {}
                if not isinstance(scenario_cfg, dict):
                    scenario_cfg = {}
                warmup_secs = int(float(scenario_cfg.get("warmup_seconds") or 0))
                run_secs = int(float(scenario_cfg.get("duration_seconds") or 0))
                load_mode_upper = str(load_mode or "").strip().upper()
                if load_mode_upper == "FIND_MAX_CONCURRENCY":
                    run_secs = 0
                    total_expected_secs = 0
                else:
                    total_expected_secs = warmup_secs + run_secs

                # Use elapsed_seconds from run_status (calculated via TIMESTAMPDIFF in SQL)
                # IMPORTANT: Do NOT calculate elapsed time in Python - Snowflake returns
                # naive datetimes in session timezone (Pacific), while Python uses UTC,
                # causing an 8-hour discrepancy (~28800s displayed instead of actual time)
                elapsed_secs = 0.0
                if run_status and run_status.get("elapsed_seconds") is not None:
                    elapsed_raw = run_status.get("elapsed_seconds")
                    if isinstance(elapsed_raw, (int, float)):
                        elapsed_secs = max(0.0, float(elapsed_raw))

                # Derive phase from elapsed time
                derived_phase = "PREPARING"
                if elapsed_secs >= 0:
                    derived_phase = "WARMUP"
                if warmup_secs > 0 and elapsed_secs >= warmup_secs:
                    derived_phase = "RUNNING"
                if (
                    load_mode_upper != "FIND_MAX_CONCURRENCY"
                    and total_expected_secs > 0
                    and elapsed_secs >= total_expected_secs
                ):
                    derived_phase = "PROCESSING"

                # Apply derived phase:
                # - Always override if derived_phase is PROCESSING (post-run period)
                # - Otherwise only set if no phase from run_status
                if derived_phase == "PROCESSING" or not payload.get("phase"):
                    payload["phase"] = derived_phase

                # Update timing with accurate elapsed
                payload["timing"] = {
                    "warmup_seconds": warmup_secs,
                    "run_seconds": run_secs,
                    "total_expected_seconds": total_expected_secs,
                    "elapsed_display_seconds": round(elapsed_secs, 1),
                }
            except Exception as e:
                logger.debug(
                    "Failed to derive timing for worker-based test %s: %s", test_id, e
                )

        # For completed runs with pending enrichment, show PROCESSING phase
        # This applies to both single-worker and multi-worker tests
        final_status = str(payload.get("status") or "").upper()
        final_enrichment = str(enrichment_status_live or "").upper()
        if final_status == "COMPLETED" and final_enrichment == "PENDING":
            payload["phase"] = "PROCESSING"

        # Compute latency spread ratio (P95/P50) to highlight variance
        # End-to-end (app-side) spread
        e2e_spread = _compute_latency_spread(
            p50=payload.get("p50_latency_ms"),
            p95=payload.get("p95_latency_ms"),
        )
        payload.update(e2e_spread)

        # SF execution spread (for enriched queries from QUERY_HISTORY)
        sf_spread = _compute_latency_spread(
            p50=payload.get("sf_p50_latency_ms"),
            p95=payload.get("sf_p95_latency_ms"),
        )
        payload["sf_latency_spread_ratio"] = sf_spread.get("latency_spread_ratio")
        payload["sf_latency_spread_warning"] = sf_spread.get("latency_spread_warning")

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
            WAREHOUSE,
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
        rows = await pool.execute_query(query, params=[*params, page_size, offset])

        count_query = f"SELECT COUNT(*) FROM {prefix}.QUERY_EXECUTIONS {where_sql}"
        count_rows = await pool.execute_query(count_query, params=params)
        total = int(count_rows[0][0]) if count_rows else 0
        total_pages = max((total + page_size - 1) // page_size, 1)

        results: list[dict[str, Any]] = []
        for row in rows:
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
    level: str = "ERROR"  # ERROR or WARNING
    query_type: str = "OTHER"  # INSERT, SELECT, UPDATE, DELETE, TIMEOUT, CONNECTION, OTHER
    earliest_occurrence: str | None = None  # ISO timestamp of first occurrence


class ErrorDetailEntry(BaseModel):
    execution_id: str
    test_id: str
    worker_id: str | None = None
    worker_label: str | None = None
    timestamp: str
    query_kind: str | None = None
    warmup: bool = False
    error: str


class ErrorDetailsResponse(BaseModel):
    test_id: str
    message_filter: str
    entries: list[ErrorDetailEntry] = []
    total_count: int = 0


class ErrorSummaryHierarchy(BaseModel):
    """Hierarchical aggregation for drill-down UI."""

    by_level: dict[str, int] = {}  # {"ERROR": 100, "WARNING": 50}
    by_query_type: dict[str, dict[str, int]] = {}  # {"ERROR": {"INSERT": 50, "SELECT": 50}}


class ErrorSummaryResponse(BaseModel):
    test_id: str
    query_tag: str | None = None
    available: bool = True
    rows: list[ErrorSummaryRow] = []
    hierarchy: ErrorSummaryHierarchy | None = None


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
        qtag = None
        start_time = None
        end_time = None
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
            # Continue anyway - we can still query QUERY_EXECUTIONS
        else:
            (query_tag, start_time, end_time) = rows[0]
            qtag = str(query_tag or "").strip() or None

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

        # 1) Snowflake-side errors (QUERY_HISTORY) - only if we have a query_tag
        sf_rows: list[Any] = []
        if qtag:
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

        # 2) App/local errors from QUERY_EXECUTIONS
        # If QUERY_HISTORY is available (sf_available), only query LOCAL_ to avoid double-counting.
        # If QUERY_HISTORY is unavailable (no query_tag), query ALL failed executions.
        local_filter = "AND QUERY_ID LIKE 'LOCAL\\_%'" if sf_available else ""

        # Check if this is a parent run (multi-worker) - QUERY_EXECUTIONS data is stored
        # under worker TEST_IDs, not the parent TEST_ID.
        run_id_rows = await pool.execute_query(
            f"SELECT RUN_ID FROM {prefix}.TEST_RESULTS WHERE TEST_ID = ?",
            params=[test_id],
        )
        run_id = run_id_rows[0][0] if run_id_rows and run_id_rows[0] else None
        is_parent_run = bool(run_id) and str(run_id) == str(test_id)

        if is_parent_run:
            # For parent runs, query all worker TEST_IDs
            app_query = f"""
            SELECT
              COALESCE(qe.WARMUP, FALSE) AS WARMUP,
              COALESCE(qe.QUERY_KIND, '(NULL)') AS QUERY_KIND,
              qe.ERROR,
              COUNT(*) AS N,
              MIN(qe.START_TIME) AS EARLIEST
            FROM {prefix}.QUERY_EXECUTIONS qe
            JOIN {prefix}.TEST_RESULTS tr ON tr.TEST_ID = qe.TEST_ID
            WHERE tr.RUN_ID = ?
              AND qe.SUCCESS = FALSE
              {local_filter}
            GROUP BY 1, 2, 3
            ORDER BY N DESC
            LIMIT 1000
            """
            app_rows = await pool.execute_query(app_query, params=[test_id])
        else:
            app_query = f"""
            SELECT
              COALESCE(WARMUP, FALSE) AS WARMUP,
              COALESCE(QUERY_KIND, '(NULL)') AS QUERY_KIND,
              ERROR,
              COUNT(*) AS N,
              MIN(START_TIME) AS EARLIEST
            FROM {prefix}.QUERY_EXECUTIONS
            WHERE TEST_ID = ?
              AND SUCCESS = FALSE
              {local_filter}
            GROUP BY 1, 2, 3
            ORDER BY N DESC
            LIMIT 1000
            """
            app_rows = await pool.execute_query(app_query, params=[test_id])

        # Normalize + aggregate in Python to collapse statement IDs/txns.
        # Key: (source, summary, msg_norm, level, query_type)
        # Value: (count, earliest_timestamp)
        agg: dict[tuple[str, str, str, str, str], tuple[int, Any]] = {}

        def classify_error(msg: str) -> tuple[str, str]:
            """Classify error message into (level, query_type)."""
            msg_lower = msg.lower() if msg else ""
            # Determine query type from message or context
            query_type = "OTHER"
            if "insert" in msg_lower:
                query_type = "INSERT"
            elif "select" in msg_lower:
                query_type = "SELECT"
            elif "update" in msg_lower:
                query_type = "UPDATE"
            elif "delete" in msg_lower:
                query_type = "DELETE"
            elif "timeout" in msg_lower or "timed out" in msg_lower:
                query_type = "TIMEOUT"
            elif "connection" in msg_lower or "connect" in msg_lower:
                query_type = "CONNECTION"

            # Determine severity level
            # Warnings: constraint violations, duplicate keys (often expected in benchmarks)
            # Errors: timeouts, connection failures, syntax errors
            level = "ERROR"
            if any(
                x in msg_lower
                for x in [
                    "duplicate key",
                    "unique constraint",
                    "unique violation",
                    "already exists",
                    "constraint violation",
                ]
            ):
                level = "WARNING"
            elif any(
                x in msg_lower
                for x in [
                    "timeout",
                    "connection refused",
                    "connection reset",
                    "connection failed",
                    "network error",
                    "syntax error",
                    "permission denied",
                    "access denied",
                ]
            ):
                level = "ERROR"

            return level, query_type

        for qt, code, msg, n in sf_rows:
            qt_s = str(qt or "").strip().upper() or "(NULL)"
            code_i = int(code or 0)
            msg_norm = _normalize_error_message(msg)[:500]
            reason = _error_reason(msg_norm)
            summary = f"SNOWFLAKE {qt_s} ERROR_CODE={code_i}" + (
                f" - {reason}" if reason else ""
            )
            level, _ = classify_error(msg_norm)
            # Use Snowflake's query type directly
            query_type = qt_s if qt_s in ("INSERT", "SELECT", "UPDATE", "DELETE") else "OTHER"
            key = ("SNOWFLAKE", summary, msg_norm, level, query_type)
            prev_count, prev_earliest = agg.get(key, (0, None))
            agg[key] = (int(prev_count) + int(n or 0), prev_earliest)  # SF rows don't have timestamps

        for warmup, kind, msg, n, earliest in app_rows:
            kind_s = str(kind or "").strip().upper() or "(NULL)"
            phase = "WARMUP" if bool(warmup) else "RUN"
            msg_s = str(msg or "")
            msg_norm = _normalize_error_message(msg_s)[:500]

            m = _SF_ERROR_PREFIX_RE.search(msg_s)
            is_sf = bool(m)
            if is_sf and sf_available:
                # If QUERY_HISTORY is available, avoid double-counting Snowflake errors.
                continue

            level, _ = classify_error(msg_norm)
            # Map query kind to query type
            query_type = kind_s if kind_s in ("INSERT", "SELECT", "UPDATE", "DELETE", "POINT_LOOKUP", "RANGE_SCAN") else "OTHER"
            if query_type in ("POINT_LOOKUP", "RANGE_SCAN"):
                query_type = "SELECT"

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

            key = (source, summary, msg_norm, level, query_type)
            prev_count, prev_earliest = agg.get(key, (0, None))
            # Track earliest timestamp
            if earliest:
                if prev_earliest is None or earliest < prev_earliest:
                    new_earliest = earliest
                else:
                    new_earliest = prev_earliest
            else:
                new_earliest = prev_earliest
            agg[key] = (int(prev_count) + int(n or 0), new_earliest)

        # Build output rows with level and query_type
        out_rows: list[ErrorSummaryRow] = []
        for (source, summary, msg_norm, level, query_type), (n, earliest) in agg.items():
            earliest_str = None
            if earliest:
                earliest_str = earliest.isoformat() if hasattr(earliest, "isoformat") else str(earliest)
            out_rows.append(
                ErrorSummaryRow(
                    source=source,
                    summary=summary,
                    message=msg_norm or "(no message)",
                    count=int(n),
                    level=level,
                    query_type=query_type,
                    earliest_occurrence=earliest_str,
                )
            )
        out_rows.sort(key=lambda r: int(r.count), reverse=True)

        # Build hierarchy for drill-down UI
        by_level: dict[str, int] = {}
        by_query_type: dict[str, dict[str, int]] = {}
        for row in out_rows:
            by_level[row.level] = by_level.get(row.level, 0) + row.count
            if row.level not in by_query_type:
                by_query_type[row.level] = {}
            by_query_type[row.level][row.query_type] = (
                by_query_type[row.level].get(row.query_type, 0) + row.count
            )

        hierarchy = ErrorSummaryHierarchy(
            by_level=by_level,
            by_query_type=by_query_type,
        )

        return ErrorSummaryResponse(
            test_id=test_id,
            query_tag=qtag,
            available=True,
            rows=out_rows[:200],
            hierarchy=hierarchy,
        )
    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("get error summary", e)


class ErrorDetailRow(BaseModel):
    execution_id: str
    test_id: str
    worker_id: int | None = None
    worker_label: str | None = None
    query_kind: str | None = None
    warmup: bool = False
    error: str
    timestamp: str
    log_id: str | None = None


class ErrorDetailResponse(BaseModel):
    test_id: str
    message_filter: str
    rows: list[ErrorDetailRow] = []
    total_count: int = 0


@router.get("/{test_id}/error-details", response_model=ErrorDetailResponse)
async def get_error_details(
    test_id: str,
    message: str = Query(..., description="Error message to filter by (exact or partial match)"),
    level: str = Query(None, description="ERROR or WARNING"),
    query_type: str = Query(None, description="INSERT, SELECT, UPDATE, DELETE, etc."),
    warmup: bool | None = Query(None, description="Filter by warmup phase"),
    limit: int = Query(50, ge=1, le=200),
) -> ErrorDetailResponse:
    """
    Get individual error occurrences for a specific error message.
    Used for Level 4 drill-down from aggregated error summary.
    """
    try:
        pool = snowflake_pool.get_default_pool()
        prefix = _prefix()

        run_id_rows = await pool.execute_query(
            f"SELECT RUN_ID FROM {prefix}.TEST_RESULTS WHERE TEST_ID = ?",
            params=[test_id],
        )
        run_id = run_id_rows[0][0] if run_id_rows and run_id_rows[0] else None
        is_parent_run = bool(run_id) and str(run_id) == str(test_id)

        warmup_filter = ""
        if warmup is not None:
            warmup_filter = f"AND qe.WARMUP = {str(warmup).upper()}"

        if is_parent_run:
            query = f"""
            SELECT
                qe.EXECUTION_ID,
                qe.TEST_ID,
                qe.WORKER_ID,
                qe.QUERY_KIND,
                qe.WARMUP,
                qe.ERROR,
                qe.START_TIME
            FROM {prefix}.QUERY_EXECUTIONS qe
            JOIN {prefix}.TEST_RESULTS tr ON tr.TEST_ID = qe.TEST_ID
            WHERE tr.RUN_ID = ?
              AND qe.SUCCESS = FALSE
              AND qe.ERROR LIKE ?
              {warmup_filter}
            ORDER BY qe.START_TIME DESC
            LIMIT ?
            """
            rows = await pool.execute_query(
                query,
                params=[test_id, f"%{message}%", limit],
            )
        else:
            query = f"""
            SELECT
                qe.EXECUTION_ID,
                qe.TEST_ID,
                qe.WORKER_ID,
                qe.QUERY_KIND,
                qe.WARMUP,
                qe.ERROR,
                qe.START_TIME
            FROM {prefix}.QUERY_EXECUTIONS qe
            WHERE qe.TEST_ID = ?
              AND qe.SUCCESS = FALSE
              AND qe.ERROR LIKE ?
              {warmup_filter}
            ORDER BY qe.START_TIME DESC
            LIMIT ?
            """
            rows = await pool.execute_query(
                query,
                params=[test_id, f"%{message}%", limit],
            )

        worker_labels: dict[str, str] = {}
        if is_parent_run:
            label_rows = await pool.execute_query(
                f"""
                SELECT DISTINCT TEST_ID, WORKER_ID
                FROM {prefix}.TEST_LOGS
                WHERE TEST_ID IN (
                    SELECT TEST_ID FROM {prefix}.TEST_RESULTS
                    WHERE RUN_ID = ? AND TEST_ID != RUN_ID
                )
                AND WORKER_ID IS NOT NULL
                AND WORKER_ID != 'ORCHESTRATOR'
                """,
                params=[test_id],
            )
            for tid, worker_id in label_rows:
                worker_labels[str(tid)] = str(worker_id)

        out_rows: list[ErrorDetailRow] = []
        for row in rows:
            exec_id, tid, worker_id, query_kind, warmup_val, error, start_time = row
            timestamp_str = (
                start_time.isoformat() if hasattr(start_time, "isoformat") else str(start_time)
            )
            worker_label = worker_labels.get(str(tid), f"worker-{worker_id}" if worker_id else None)

            out_rows.append(
                ErrorDetailRow(
                    execution_id=str(exec_id),
                    test_id=str(tid),
                    worker_id=int(worker_id) if worker_id is not None else None,
                    worker_label=worker_label,
                    query_kind=str(query_kind) if query_kind else None,
                    warmup=bool(warmup_val),
                    error=str(error) if error else "",
                    timestamp=timestamp_str,
                )
            )

        return ErrorDetailResponse(
            test_id=test_id,
            message_filter=message,
            rows=out_rows,
            total_count=len(out_rows),
        )
    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("get error details", e)


@router.get("/{test_id}/logs")
async def get_test_logs(
    test_id: str,
    limit: int = Query(500, ge=1, le=2000),
    offset: int = Query(0, ge=0),
    child_test_id: str | None = Query(None),
    target_id: str | None = Query(None),
) -> dict[str, Any]:
    """
    Fetch persisted per-test logs (and in-memory logs for running tests).
    """
    try:
        pool = snowflake_pool.get_default_pool()
        prefix = _prefix()
        run_rows = await pool.execute_query(
            f"SELECT RUN_ID FROM {prefix}.TEST_RESULTS WHERE TEST_ID = ?",
            params=[test_id],
        )
        run_id = run_rows[0][0] if run_rows else None
        is_parent = bool(run_id) and str(run_id) == str(test_id)

        targets: list[dict[str, Any]] = []
        selected_test_id = test_id
        selected_target_id = str(target_id) if target_id else None
        if is_parent:
            worker_targets: dict[str, dict[str, Any]] = {}
            child_rows = await pool.execute_query(
                f"""
                SELECT TEST_ID, TEST_CONFIG
                FROM {prefix}.TEST_RESULTS
                WHERE RUN_ID = ?
                  AND TEST_ID <> ?
                ORDER BY TEST_ID ASC
                """,
                params=[test_id, test_id],
            )
            metrics_rows = await pool.execute_query(
                f"""
                SELECT DISTINCT
                    TEST_ID,
                    WORKER_ID,
                    WORKER_GROUP_ID,
                    WORKER_GROUP_COUNT
                FROM {prefix}.WORKER_METRICS_SNAPSHOTS
                WHERE RUN_ID = ?
                ORDER BY TEST_ID ASC
                """,
                params=[test_id],
            )

            def _parse_test_config(raw: Any) -> dict[str, Any]:
                cfg: Any = raw
                if isinstance(cfg, str):
                    try:
                        cfg = json.loads(cfg)
                    except Exception:
                        cfg = {}
                return cfg if isinstance(cfg, dict) else {}

            def _extract_worker_context(cfg: dict[str, Any]) -> dict[str, Any]:
                template_cfg = cfg.get("template_config", {})
                if not isinstance(template_cfg, dict):
                    template_cfg = {}
                scenario_cfg = cfg.get("scenario", {})
                if not isinstance(scenario_cfg, dict):
                    scenario_cfg = {}
                return {
                    "worker_id": template_cfg.get("worker_id"),
                    "worker_group_id": int(scenario_cfg.get("worker_group_id") or 0),
                    "worker_group_count": int(
                        scenario_cfg.get("worker_group_count") or 1
                    ),
                }

            def _default_worker_label(
                worker_id: str | None, group_id: int | None, test_id_val: Any
            ) -> str:
                if worker_id:
                    return worker_id
                if group_id is not None:
                    return f"worker-{group_id}"
                return str(test_id_val)

            def _upsert_worker_target(
                *,
                test_id_val: Any,
                worker_id_val: str | None,
                group_id_val: int | None,
                group_count_val: int | None,
            ) -> None:
                test_key = str(test_id_val)
                existing = worker_targets.get(test_key, {})
                worker_id_norm = str(worker_id_val) if worker_id_val else None
                group_id_norm = int(group_id_val) if group_id_val is not None else None
                group_count_norm = (
                    int(group_count_val) if group_count_val is not None else None
                )
                worker_id_final = worker_id_norm or existing.get("worker_id")
                group_id_final = (
                    group_id_norm
                    if group_id_norm is not None
                    else existing.get("worker_group_id")
                )
                group_count_final = (
                    group_count_norm
                    if group_count_norm is not None
                    else existing.get("worker_group_count")
                )
                label = _default_worker_label(
                    worker_id_final, group_id_final, test_id_val
                )
                if (
                    group_count_final
                    and group_id_final is not None
                    and group_count_final > 1
                ):
                    label = f"{label} (group {group_id_final + 1}/{group_count_final})"
                worker_targets[test_key] = {
                    "target_id": f"worker:{test_key}",
                    "test_id": test_key,
                    "worker_id": worker_id_final,
                    "worker_group_id": int(group_id_final or 0),
                    "worker_group_count": int(group_count_final or 1),
                    "label": label,
                    "kind": "worker",
                }

            for child_id, child_cfg in child_rows:
                ctx = _extract_worker_context(_parse_test_config(child_cfg))
                _upsert_worker_target(
                    test_id_val=child_id,
                    worker_id_val=ctx.get("worker_id"),
                    group_id_val=ctx.get("worker_group_id"),
                    group_count_val=ctx.get("worker_group_count"),
                )

            for (
                metrics_test_id,
                metrics_worker_id,
                metrics_group_id,
                metrics_group_count,
            ) in metrics_rows:
                _upsert_worker_target(
                    test_id_val=metrics_test_id,
                    worker_id_val=str(metrics_worker_id) if metrics_worker_id else None,
                    group_id_val=metrics_group_id,
                    group_count_val=metrics_group_count,
                )

            targets.append(
                {
                    "target_id": "all",
                    "test_id": str(test_id),
                    "worker_id": None,
                    "worker_group_id": 0,
                    "worker_group_count": 1,
                    "label": "All",
                    "kind": "all",
                }
            )
            for source in ["ORCHESTRATOR", "CONTROLLER", "UNKNOWN"]:
                targets.append(
                    {
                        "target_id": f"parent:{source}",
                        "test_id": str(test_id),
                        "worker_id": source,
                        "worker_group_id": 0,
                        "worker_group_count": 1,
                        "label": source,
                        "kind": "parent",
                    }
                )

            worker_items = list(worker_targets.values())
            worker_items.sort(
                key=lambda item: (
                    int(item.get("worker_group_id") or 0),
                    str(item.get("worker_id") or ""),
                    str(item.get("test_id") or ""),
                )
            )
            targets.extend(worker_items)

            selected_target = None
            if selected_target_id:
                selected_target = next(
                    (t for t in targets if t.get("target_id") == selected_target_id),
                    None,
                )

            if selected_target:
                if selected_target.get("kind") == "worker":
                    selected_test_id = selected_target.get("test_id") or test_id
                else:
                    selected_test_id = test_id
            elif child_test_id and any(
                t.get("test_id") == child_test_id for t in worker_targets.values()
            ):
                selected_test_id = child_test_id
            else:
                selected_test_id = test_id

        # Prefer in-memory logs for running/prepared tests so refreshes don't lose context.
        selected_kind = None
        if selected_target_id:
            selected_kind = next(
                (
                    t.get("kind")
                    for t in targets
                    if t.get("target_id") == selected_target_id
                ),
                None,
            )
        if selected_kind != "all":
            running = await registry.get(selected_test_id)
            if running is not None and running.log_buffer:
                logs = list(running.log_buffer)
                logs.sort(key=lambda r: int(r.get("seq") or 0))
                return {
                    "test_id": test_id,
                    "selected_test_id": selected_test_id,
                    "targets": targets,
                    "workers": targets,
                    "logs": logs[offset : offset + limit],
                }
        if is_parent and selected_kind == "all":
            test_ids = [str(test_id)]
            for item in targets:
                if item.get("kind") == "worker":
                    tid = str(item.get("test_id") or "")
                    if tid and tid not in test_ids:
                        test_ids.append(tid)
            if not test_ids:
                return {
                    "test_id": test_id,
                    "selected_test_id": selected_test_id,
                    "targets": targets,
                    "workers": targets,
                    "logs": [],
                }
            placeholders = ", ".join(["?"] * len(test_ids))
            query = f"""
            SELECT
                LOG_ID,
                TEST_ID,
                WORKER_ID,
                SEQ,
                TIMESTAMP,
                LEVEL,
                LOGGER,
                MESSAGE,
                EXCEPTION
            FROM {prefix}.TEST_LOGS
            WHERE TEST_ID IN ({placeholders})
            ORDER BY TIMESTAMP ASC, SEQ ASC
            LIMIT ? OFFSET ?
            """
            rows = await pool.execute_query(query, params=[*test_ids, limit, offset])
            logs: list[dict[str, Any]] = []
            for row in rows:
                (
                    log_id,
                    test_id_db,
                    worker_id,
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
                        "worker_id": str(worker_id) if worker_id else None,
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
            return {
                "test_id": test_id,
                "selected_test_id": selected_test_id,
                "targets": targets,
                "workers": targets,
                "logs": logs,
            }
        query = f"""
        SELECT
            LOG_ID,
            TEST_ID,
            WORKER_ID,
            SEQ,
            TIMESTAMP,
            LEVEL,
            LOGGER,
            MESSAGE,
            EXCEPTION
        FROM {prefix}.TEST_LOGS
        WHERE TEST_ID = ?
        ORDER BY SEQ ASC
        LIMIT ? OFFSET ?
        """
        rows = await pool.execute_query(query, params=[selected_test_id, limit, offset])

        logs: list[dict[str, Any]] = []
        for row in rows:
            (
                log_id,
                test_id_db,
                worker_id,
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
                    "worker_id": str(worker_id) if worker_id else None,
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

        return {
            "test_id": test_id,
            "selected_test_id": selected_test_id,
            "targets": targets,
            "workers": targets,
            "logs": logs,
        }
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
        latency_aggregation_method = None
        rows: list[tuple[Any, ...]] = []

        # All runs store metrics in WORKER_METRICS_SNAPSHOTS with PHASE column.
        # Query using run_id (which equals test_id for parent runs).
        run_rows = await pool.execute_query(
            f"SELECT RUN_ID FROM {_prefix()}.TEST_RESULTS WHERE TEST_ID = ?",
            params=[test_id],
        )
        run_id = run_rows[0][0] if run_rows else test_id

        worker_query = f"""
        SELECT
            TIMESTAMP,
            ELAPSED_SECONDS,
            QPS,
            P50_LATENCY_MS,
            P95_LATENCY_MS,
            P99_LATENCY_MS,
            ACTIVE_CONNECTIONS,
            TARGET_CONNECTIONS,
            CUSTOM_METRICS,
            PHASE,
            WORKER_ID,
            WORKER_GROUP_COUNT
        FROM {_prefix()}.WORKER_METRICS_SNAPSHOTS
        WHERE RUN_ID = ?
        ORDER BY TIMESTAMP ASC
        """
        worker_rows = await pool.execute_query(worker_query, params=[str(run_id)])
        global_warmup_end_elapsed: float | None = None
        smoothing_applied = False
        if worker_rows:
            latency_aggregation_method = LATENCY_AGGREGATION_METHOD

            # Use the earliest timestamp from the data as the reference point.
            # This avoids timezone issues between RUN_STATUS and WORKER_METRICS_SNAPSHOTS.
            run_start_time: Any = worker_rows[0][0] if worker_rows else None

            @dataclass
            class _Bucket:
                timestamp: Any
                elapsed_seconds: float
                ops_per_sec: float
                p50_latency_ms: list[float]
                p95_latency_ms: list[float]
                p99_latency_ms: list[float]
                active_connections: int
                target_workers: int
                custom_metrics: list[Any]
                has_warmup: bool
                has_measurement: bool
                worker_ids_seen: set[str]
                expected_worker_count: int

            buckets: dict[float, _Bucket] = {}
            first_warmup_timestamp: Any = None
            first_measurement_timestamp: Any = None
            for (
                timestamp,
                elapsed,
                ops_per_sec,
                p50,
                p95,
                p99,
                active_connections,
                target_connections,
                custom_metrics,
                phase,
                worker_id,
                worker_group_count,
            ) in worker_rows:
                phase_value = str(phase or "").strip().upper()
                is_warmup = phase_value == "WARMUP"
                is_measurement = phase_value in ("MEASUREMENT", "RUNNING")
                if phase_value and phase_value not in ("WARMUP", "MEASUREMENT", "RUNNING"):
                    continue
                if is_warmup and first_warmup_timestamp is None and timestamp:
                    first_warmup_timestamp = timestamp
                if is_measurement and first_measurement_timestamp is None and timestamp:
                    first_measurement_timestamp = timestamp
                try:
                    # Compute run-relative elapsed time using absolute timestamp.
                    # This ensures workers spawned at different times align correctly.
                    if run_start_time and timestamp:
                        # Both should be datetime objects from Snowflake
                        delta = timestamp - run_start_time
                        elapsed_from_start = delta.total_seconds() if hasattr(delta, 'total_seconds') else float(elapsed or 0)
                    else:
                        # Fallback to per-worker elapsed (may be misaligned for multi-worker)
                        elapsed_from_start = float(elapsed or 0)
                    # Aggregate to whole-second buckets for history charts.
                    bucket = round(elapsed_from_start, 0)
                except Exception:
                    bucket = 0.0
                    elapsed_from_start = float(elapsed or 0)
                agg = buckets.get(bucket)
                worker_id_str = str(worker_id or "unknown")
                expected_count = int(worker_group_count or 1)
                if not agg:
                    agg = _Bucket(
                        timestamp=timestamp,
                        elapsed_seconds=elapsed_from_start,
                        ops_per_sec=0.0,
                        p50_latency_ms=[],
                        p95_latency_ms=[],
                        p99_latency_ms=[],
                        active_connections=0,
                        target_workers=0,
                        custom_metrics=[],
                        has_warmup=is_warmup,
                        has_measurement=is_measurement,
                        worker_ids_seen=set(),
                        expected_worker_count=expected_count,
                    )
                    buckets[bucket] = agg
                agg.worker_ids_seen.add(worker_id_str)
                agg.expected_worker_count = max(agg.expected_worker_count, expected_count)
                if is_warmup:
                    agg.has_warmup = True
                if is_measurement:
                    agg.has_measurement = True
                if timestamp and agg.timestamp and timestamp < agg.timestamp:
                    agg.timestamp = timestamp
                agg.elapsed_seconds = max(elapsed_from_start, agg.elapsed_seconds)
                agg.ops_per_sec += float(ops_per_sec or 0)
                agg.p50_latency_ms.append(float(p50 or 0))
                agg.p95_latency_ms.append(float(p95 or 0))
                agg.p99_latency_ms.append(float(p99 or 0))
                agg.active_connections += int(active_connections or 0)
                agg.target_workers += int(target_connections or 0)
                if custom_metrics:
                    agg.custom_metrics.append(custom_metrics)

            def _avg(values: list[float]) -> float:
                if not values:
                    return 0.0
                return float(sum(values) / len(values))

            def _max(values: list[float]) -> float:
                if not values:
                    return 0.0
                return float(max(values))

            def _sum_dicts(
                dicts: list[dict[str, Any]],
            ) -> dict[str, float]:
                out: dict[str, float] = {}
                for d in dicts:
                    for key, value in d.items():
                        try:
                            out[key] = out.get(key, 0.0) + float(value or 0)
                        except Exception:
                            continue
                return out

            def _avg_dicts(
                dicts: list[dict[str, Any]],
            ) -> dict[str, float]:
                if not dicts:
                    return {}
                summed = _sum_dicts(dicts)
                return {key: value / len(dicts) for key, value in summed.items()}

            def _normalize_metrics(raw: Any) -> dict[str, Any]:
                if isinstance(raw, str):
                    try:
                        raw = json.loads(raw)
                    except Exception:
                        return {}
                return raw if isinstance(raw, dict) else {}

            global_warmup_end_elapsed = None
            if first_warmup_timestamp and first_measurement_timestamp:
                try:
                    delta = first_measurement_timestamp - first_warmup_timestamp
                    global_warmup_end_elapsed = delta.total_seconds()
                except Exception:
                    pass

            smoothing_applied = False
            aggregated_rows: list[tuple[Any, ...]] = []
            sorted_buckets = sorted(buckets.values(), key=lambda b: b.elapsed_seconds)
            max_expected_workers = max((b.expected_worker_count for b in sorted_buckets), default=1)
            if max_expected_workers > 1:
                smoothing_applied = True
                window_size = 5
                raw_ops_list = [b.ops_per_sec for b in sorted_buckets]
                smoothed_ops_list: list[float] = []
                for i in range(len(raw_ops_list)):
                    start_idx = max(0, i - window_size // 2)
                    end_idx = min(len(raw_ops_list), i + window_size // 2 + 1)
                    window = raw_ops_list[start_idx:end_idx]
                    smoothed_ops_list.append(sum(window) / len(window))
            else:
                smoothed_ops_list = [b.ops_per_sec for b in sorted_buckets]
            for idx, agg in enumerate(sorted_buckets):
                normalized_ops = smoothed_ops_list[idx]
                custom_list = [_normalize_metrics(cm) for cm in agg.custom_metrics]
                app_ops_list = [
                    cm.get("app_ops_breakdown", {})
                    for cm in custom_list
                    if isinstance(cm.get("app_ops_breakdown"), dict)
                ]
                sf_bench_list = [
                    cm.get("sf_bench", {})
                    for cm in custom_list
                    if isinstance(cm.get("sf_bench"), dict)
                ]
                warehouse_list = [
                    cm.get("warehouse", {})
                    for cm in custom_list
                    if isinstance(cm.get("warehouse"), dict)
                ]
                resources_list = [
                    cm.get("resources", {})
                    for cm in custom_list
                    if isinstance(cm.get("resources"), dict)
                ]
                qps_list = [
                    cm.get("qps", {})
                    for cm in custom_list
                    if isinstance(cm.get("qps"), dict)
                ]
                custom_agg = {
                    "app_ops_breakdown": _sum_dicts(app_ops_list),
                    "sf_bench": _sum_dicts(sf_bench_list),
                    "warehouse": _sum_dicts(warehouse_list),
                    "resources": _avg_dicts(resources_list),
                    "qps": _avg_dicts(qps_list),
                }
                if first_measurement_timestamp is not None and agg.timestamp:
                    try:
                        is_warmup_bucket = agg.timestamp < first_measurement_timestamp
                    except Exception:
                        is_warmup_bucket = agg.has_warmup and not agg.has_measurement
                else:
                    is_warmup_bucket = agg.has_warmup and not agg.has_measurement
                aggregated_rows.append(
                    (
                        agg.timestamp,
                        agg.elapsed_seconds,
                        normalized_ops,
                        _avg(agg.p50_latency_ms),
                        _max(agg.p95_latency_ms),
                        _max(agg.p99_latency_ms),
                        agg.active_connections,
                        agg.target_workers,
                        custom_agg,
                        is_warmup_bucket,
                    )
                )
            rows = sorted(aggregated_rows, key=lambda item: item[1] or 0)

        snapshots = []
        warmup_end_elapsed_seconds: float | None = global_warmup_end_elapsed if worker_rows else None
        for row in rows:
            is_warmup = False
            if len(row) == 10:
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
                    is_warmup,
                ) = row
            else:
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
            resources: dict[str, Any] = {}
            qps_data: dict[str, Any] = {}
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
                maybe_res = cm.get("resources")
                if isinstance(maybe_res, dict):
                    resources = maybe_res
                maybe_qps = cm.get("qps")
                if isinstance(maybe_qps, dict):
                    qps_data = maybe_qps

            # Use smoothed or windowed QPS when raw QPS is an outlier.
            # This handles the case where a burst of errors at test end
            # causes an unrealistically high instantaneous QPS.
            # NOTE: This logic is DISABLED for multi-worker runs because the smoothed/windowed
            # values in qps_data are per-worker averages, not totals. Using them would incorrectly
            # replace the aggregated total QPS with a single worker's value.
            # TODO: Fix qps_data to contain properly aggregated smoothed values for multi-worker runs.
            # if qps_data:
            #     raw_qps = float(ops_per_sec or 0)
            #     smoothed_qps = float(qps_data.get("smoothed") or 0)
            #     windowed_qps = float(qps_data.get("windowed") or 0)
            #     # If raw QPS is more than 3x the smoothed value, use smoothed
            #     if smoothed_qps > 0 and raw_qps > smoothed_qps * 3:
            #         ops_per_sec = smoothed_qps
            #     # Fallback to windowed if smoothed is 0 but raw is suspiciously high
            #     elif windowed_qps > 0 and smoothed_qps == 0 and raw_qps > windowed_qps * 3:
            #         ops_per_sec = windowed_qps

            def _to_int(v: Any) -> int:
                try:
                    return int(v or 0)
                except Exception:
                    return 0

            def _to_float(v: Any) -> float:
                try:
                    return float(v or 0)
                except Exception:
                    return 0.0

            def _compute_breakdown_rate(
                ops_per_sec: float, app_ops: dict[str, Any], count_key: str
            ) -> float:
                """Compute instantaneous breakdown rate from total ops_per_sec.

                The workers report cumulative average rates (count/elapsed), not
                instantaneous rates. To get instantaneous breakdown rates, we
                distribute ops_per_sec according to the proportion of counts.
                """
                try:
                    total_count = float(app_ops.get("total_count") or 0)
                    type_count = float(app_ops.get(count_key) or 0)
                    if total_count <= 0:
                        return 0.0
                    return ops_per_sec * (type_count / total_count)
                except Exception:
                    return 0.0

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
                    # App-side QPS breakdown: compute instantaneous rates by distributing
                    # ops_per_sec according to the proportion of operation counts.
                    # The raw *_ops_sec from workers are cumulative averages (count/elapsed),
                    # not instantaneous rates, so we recalculate here.
                    "app_point_lookup_ops_sec": _compute_breakdown_rate(
                        float(ops_per_sec or 0), app_ops, "point_lookup_count"
                    ),
                    "app_range_scan_ops_sec": _compute_breakdown_rate(
                        float(ops_per_sec or 0), app_ops, "range_scan_count"
                    ),
                    "app_insert_ops_sec": _compute_breakdown_rate(
                        float(ops_per_sec or 0), app_ops, "insert_count"
                    ),
                    "app_update_ops_sec": _compute_breakdown_rate(
                        float(ops_per_sec or 0), app_ops, "update_count"
                    ),
                    "app_read_ops_sec": _compute_breakdown_rate(
                        float(ops_per_sec or 0), app_ops, "read_count"
                    ),
                    "app_write_ops_sec": _compute_breakdown_rate(
                        float(ops_per_sec or 0), app_ops, "write_count"
                    ),
                    "resources_cpu_percent": _to_float(resources.get("cpu_percent")),
                    "resources_memory_mb": _to_float(resources.get("memory_mb")),
                    "resources_process_cpu_percent": _to_float(
                        resources.get("process_cpu_percent")
                    ),
                    "resources_process_memory_mb": _to_float(
                        resources.get("process_memory_mb")
                    ),
                    "resources_host_cpu_percent": _to_float(
                        resources.get("host_cpu_percent")
                    ),
                    "resources_host_cpu_cores": _to_float(
                        resources.get("host_cpu_cores")
                    ),
                    "resources_host_memory_mb": _to_float(
                        resources.get("host_memory_mb")
                    ),
                    "resources_host_memory_total_mb": _to_float(
                        resources.get("host_memory_total_mb")
                    ),
                    "resources_host_memory_available_mb": _to_float(
                        resources.get("host_memory_available_mb")
                    ),
                    "resources_host_memory_percent": _to_float(
                        resources.get("host_memory_percent")
                    ),
                    "resources_cgroup_cpu_percent": _to_float(
                        resources.get("cgroup_cpu_percent")
                    ),
                    "resources_cgroup_cpu_quota_cores": _to_float(
                        resources.get("cgroup_cpu_quota_cores")
                    ),
                    "resources_cgroup_memory_mb": _to_float(
                        resources.get("cgroup_memory_mb")
                    ),
                    "resources_cgroup_memory_limit_mb": _to_float(
                        resources.get("cgroup_memory_limit_mb")
                    ),
                    "resources_cgroup_memory_percent": _to_float(
                        resources.get("cgroup_memory_percent")
                    ),
                    "warmup": bool(is_warmup),
                }
            )

        return {
            "test_id": test_id,
            "snapshots": snapshots,
            "count": len(snapshots),
            "latency_aggregation_method": latency_aggregation_method,
            "warmup_end_elapsed_seconds": warmup_end_elapsed_seconds,
            "smoothing_applied": smoothing_applied,
        }
    except Exception as e:
        raise http_exception("get test metrics", e)


@router.get("/{test_id}/worker-metrics")
async def get_worker_metrics(test_id: str) -> dict[str, Any]:
    """
    Fetch per-worker time-series metrics snapshots for multi-worker runs.
    """
    try:
        pool = snowflake_pool.get_default_pool()
        prefix = _prefix()

        run_rows = await pool.execute_query(
            f"SELECT RUN_ID FROM {prefix}.TEST_RESULTS WHERE TEST_ID = ?",
            params=[test_id],
        )
        run_id = run_rows[0][0] if run_rows else None
        parent_run_id = run_id or test_id

        query = f"""
        SELECT
            WORKER_ID,
            WORKER_GROUP_ID,
            WORKER_GROUP_COUNT,
            TIMESTAMP,
            ELAPSED_SECONDS,
            QPS,
            P50_LATENCY_MS,
            P95_LATENCY_MS,
            P99_LATENCY_MS,
            ACTIVE_CONNECTIONS,
            TARGET_CONNECTIONS,
            CUSTOM_METRICS,
            PHASE
        FROM {prefix}.WORKER_METRICS_SNAPSHOTS
        WHERE RUN_ID = ?
        ORDER BY WORKER_GROUP_ID ASC, WORKER_ID ASC, TIMESTAMP ASC
        """
        rows = await pool.execute_query(query, params=[parent_run_id])
        if not rows:
            return {
                "test_id": test_id,
                "parent_run_id": parent_run_id,
                "available": False,
                "workers": [],
            }

        def _to_int(v: Any) -> int:
            try:
                return int(v or 0)
            except Exception:
                return 0

        def _to_float(v: Any) -> float:
            try:
                return float(v or 0)
            except Exception:
                return 0.0

        workers: dict[str, dict[str, Any]] = {}
        
        # Use the first timestamp as reference for computing run-relative elapsed_seconds.
        # This ensures all workers' data is aligned to the same timeline.
        run_start_time: Any = rows[0][3] if rows else None
        
        for row in rows:
            (
                worker_id_from_row,
                worker_group_id,
                worker_group_count,
                timestamp,
                elapsed_seconds,
                qps,
                p50,
                p95,
                p99,
                active_connections,
                target_connections,
                custom_metrics,
                _phase,
            ) = row
            worker_id_val = worker_id_from_row

            cm: Any = custom_metrics
            if isinstance(cm, str):
                try:
                    cm = json.loads(cm)
                except Exception:
                    cm = {}
            if cm is None:
                cm = {}
            resources: dict[str, Any] = {}
            if isinstance(cm, dict):
                maybe_res = cm.get("resources")
                if isinstance(maybe_res, dict):
                    resources = maybe_res

            key = f"{worker_id_val or 'worker'}:{int(worker_group_id or 0)}"
            worker = workers.get(key)
            if worker is None:
                worker = {
                    "key": key,
                    "worker_id": worker_id_val,
                    "worker_group_id": int(worker_group_id or 0),
                    "worker_group_count": int(worker_group_count or 0),
                    "snapshots": [],
                }
                workers[key] = worker

            snapshots = worker.get("snapshots")
            if not isinstance(snapshots, list):
                snapshots = []
                worker["snapshots"] = snapshots
            snapshots_list = cast(list[dict[str, Any]], snapshots)
            
            # Compute run-relative elapsed time using absolute timestamp.
            if run_start_time and timestamp:
                delta = timestamp - run_start_time
                run_relative_elapsed = delta.total_seconds() if hasattr(delta, 'total_seconds') else float(elapsed_seconds or 0)
            else:
                run_relative_elapsed = float(elapsed_seconds or 0)
            
            snapshots_list.append(
                {
                    "timestamp": timestamp.isoformat()
                    if hasattr(timestamp, "isoformat")
                    else str(timestamp),
                    "elapsed_seconds": run_relative_elapsed,
                    "qps": float(qps or 0),
                    "p50_latency": float(p50 or 0),
                    "p95_latency": float(p95 or 0),
                    "p99_latency": float(p99 or 0),
                    "active_connections": _to_int(active_connections),
                    "target_workers": _to_int(target_connections),
                    "resources_cpu_percent": _to_float(resources.get("cpu_percent")),
                    "resources_memory_mb": _to_float(resources.get("memory_mb")),
                    "resources_host_cpu_percent": _to_float(
                        resources.get("host_cpu_percent")
                    ),
                    "resources_host_memory_mb": _to_float(
                        resources.get("host_memory_mb")
                    ),
                    "resources_cgroup_cpu_percent": _to_float(
                        resources.get("cgroup_cpu_percent")
                    ),
                    "resources_cgroup_memory_mb": _to_float(
                        resources.get("cgroup_memory_mb")
                    ),
                }
            )

        first_data_timestamp: str | None = None
        if rows and rows[0] and rows[0][3]:
            ts = rows[0][3]
            first_data_timestamp = ts.isoformat() if hasattr(ts, 'isoformat') else str(ts)

        return {
            "test_id": test_id,
            "parent_run_id": parent_run_id,
            "available": True,
            "run_start_at": first_data_timestamp,
            "workers": list(workers.values()),
        }
    except Exception as e:
        raise http_exception("get worker metrics", e)


@router.get("/{test_id}/warehouse-details")
async def get_warehouse_details(test_id: str) -> dict[str, Any]:
    """
    Fetch current warehouse configuration for a test.

    Returns MCW settings, scaling policy, query acceleration config, and current
    cluster state (started, running, queued) via SHOW WAREHOUSES.

    Used by the dashboard for real-time MCW status display during active tests.
    """
    from backend.core.results_store import fetch_warehouse_config_snapshot

    try:
        pool = snowflake_pool.get_default_pool()
        prefix = _prefix()

        # Get the warehouse name from the test record
        rows = await pool.execute_query(
            f"""
            SELECT WAREHOUSE
            FROM {prefix}.TEST_RESULTS
            WHERE TEST_ID = ?
            """,
            params=[test_id],
        )

        if not rows or not rows[0] or not rows[0][0]:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"message": "Test not found or no warehouse configured"},
            )

        warehouse_name = str(rows[0][0]).strip()
        if not warehouse_name:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"message": "No warehouse configured for this test"},
            )

        # Fetch current warehouse config via SHOW WAREHOUSES
        config = await fetch_warehouse_config_snapshot(warehouse_name)

        if not config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"message": f"Warehouse '{warehouse_name}' not found"},
            )

        return {
            "test_id": test_id,
            "warehouse": config,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("get warehouse details", e)


@router.get("/{test_id}/warehouse-timeseries")
async def get_warehouse_timeseries(test_id: str) -> dict[str, Any]:
    """
    Fetch per-second warehouse timeseries for a completed test.

    This is derived from QUERY_EXECUTIONS (post-processed) and expanded to a
    per-second timeline based on TEST_RESULTS.START_TIME + TEST_RESULTS.DURATION_SECONDS.

    For parent runs (multi-worker), aggregates data from all child runs.

    Falls back to WAREHOUSE_POLL_SNAPSHOTS when QUERY_HISTORY enrichment is
    incomplete (which happens due to ~45s latency in QUERY_HISTORY).
    """
    try:
        pool = snowflake_pool.get_default_pool()
        prefix = _prefix()

        run_id_rows = await pool.execute_query(
            f"SELECT RUN_ID FROM {prefix}.TEST_RESULTS WHERE TEST_ID = ?",
            params=[test_id],
        )
        run_id = run_id_rows[0][0] if run_id_rows and run_id_rows[0] else None
        is_parent = bool(run_id) and str(run_id) == str(test_id)

        if is_parent:
            # NOTE: QUERY_EXECUTIONS data is stored under run_id (parent TEST_ID),
            # so we must include the parent in the query. V_WAREHOUSE_TIMESERIES
            # joins QUERY_EXECUTIONS, so the data lives under the parent TEST_ID.
            query = f"""
            WITH query_bounds AS (
                -- Get first query time (warmup or measurement) and last measurement query time
                -- Also get measurement start time for warmup boundary
                SELECT
                    MIN(DATE_TRUNC('second', qe.START_TIME)) AS FIRST_QUERY_SECOND,
                    MAX(DATE_TRUNC('second', qe.START_TIME)) AS LAST_QUERY_SECOND,
                    MIN(CASE WHEN COALESCE(qe.WARMUP, FALSE) = FALSE THEN DATE_TRUNC('second', qe.START_TIME) END) AS FIRST_MEASUREMENT_SECOND
                FROM {prefix}.QUERY_EXECUTIONS qe
                JOIN {prefix}.TEST_RESULTS t ON t.TEST_ID = qe.TEST_ID
                WHERE t.RUN_ID = ?
            ),
            seconds AS (
                SELECT
                    DATEADD('second', g.SEQ, qb.FIRST_QUERY_SECOND) AS SECOND,
                    g.SEQ AS ELAPSED_SECONDS,
                    CASE WHEN DATEADD('second', g.SEQ, qb.FIRST_QUERY_SECOND) < qb.FIRST_MEASUREMENT_SECOND THEN TRUE ELSE FALSE END AS IS_WARMUP,
                    DATEDIFF('second', qb.FIRST_QUERY_SECOND, qb.FIRST_MEASUREMENT_SECOND) AS WARMUP_END_ELAPSED
                FROM query_bounds qb
                JOIN (
                    SELECT SEQ4() AS SEQ
                    FROM TABLE(GENERATOR(ROWCOUNT => 86400))
                ) g
                  ON g.SEQ <= DATEDIFF('second', qb.FIRST_QUERY_SECOND, qb.LAST_QUERY_SECOND)
            ),
            run_test_ids AS (
                SELECT TEST_ID
                FROM {prefix}.TEST_RESULTS
                WHERE RUN_ID = ?
            ),
            wh_data AS (
                SELECT
                    wt.SECOND,
                    MAX(wt.ACTIVE_CLUSTERS) AS ACTIVE_CLUSTERS,
                    SUM(wt.QUERIES_STARTED) AS QUERIES_STARTED,
                    SUM(wt.TOTAL_QUEUE_OVERLOAD_MS) AS TOTAL_QUEUE_OVERLOAD_MS,
                    SUM(wt.TOTAL_QUEUE_PROVISIONING_MS) AS TOTAL_QUEUE_PROVISIONING_MS
                FROM {prefix}.V_WAREHOUSE_TIMESERIES wt
                JOIN run_test_ids rti ON rti.TEST_ID = wt.TEST_ID
                GROUP BY wt.SECOND
            ),
            poller_clusters AS (
                SELECT
                    DATE_TRUNC('second', TIMESTAMP) AS SECOND,
                    MAX(COALESCE(STARTED_CLUSTERS, 0)) AS STARTED_CLUSTERS
                FROM {prefix}.WAREHOUSE_POLL_SNAPSHOTS
                WHERE RUN_ID = ?
                GROUP BY DATE_TRUNC('second', TIMESTAMP)
            )
            SELECT
                s.SECOND AS TS,
                s.ELAPSED_SECONDS,
                COALESCE(wh.ACTIVE_CLUSTERS, 0) AS ACTIVE_CLUSTERS,
                -- Forward-fill: use last known poller value for gaps between ~5s polls
                COALESCE(
                    pc.STARTED_CLUSTERS,
                    LAST_VALUE(pc.STARTED_CLUSTERS IGNORE NULLS) OVER (ORDER BY s.SECOND ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                    0
                ) AS REALTIME_CLUSTERS,
                COALESCE(wh.QUERIES_STARTED, 0) AS QUERIES_STARTED,
                COALESCE(wh.TOTAL_QUEUE_OVERLOAD_MS, 0) AS TOTAL_QUEUE_OVERLOAD_MS,
                COALESCE(wh.TOTAL_QUEUE_PROVISIONING_MS, 0) AS TOTAL_QUEUE_PROVISIONING_MS,
                COALESCE(
                    COALESCE(wh.TOTAL_QUEUE_OVERLOAD_MS, 0) / NULLIF(wh.QUERIES_STARTED, 0),
                    0
                ) AS AVG_QUEUE_OVERLOAD_MS,
                COALESCE(
                    COALESCE(wh.TOTAL_QUEUE_PROVISIONING_MS, 0) / NULLIF(wh.QUERIES_STARTED, 0),
                    0
                ) AS AVG_QUEUE_PROVISIONING_MS,
                s.IS_WARMUP,
                s.WARMUP_END_ELAPSED
            FROM seconds s
            LEFT JOIN wh_data wh ON wh.SECOND = s.SECOND
            LEFT JOIN poller_clusters pc ON pc.SECOND = s.SECOND
            ORDER BY s.SECOND ASC
            """
            rows = await pool.execute_query(
                query,
                params=[test_id, test_id, test_id],
            )
        else:
            query = f"""
            WITH query_bounds AS (
                -- Get first query time (warmup or measurement) and last query time
                -- Also get measurement start time for warmup boundary
                SELECT
                    MIN(DATE_TRUNC('second', START_TIME)) AS FIRST_QUERY_SECOND,
                    MAX(DATE_TRUNC('second', START_TIME)) AS LAST_QUERY_SECOND,
                    MIN(CASE WHEN COALESCE(WARMUP, FALSE) = FALSE THEN DATE_TRUNC('second', START_TIME) END) AS FIRST_MEASUREMENT_SECOND
                FROM {prefix}.QUERY_EXECUTIONS
                WHERE TEST_ID = ?
            ),
            seconds AS (
                SELECT
                    DATEADD('second', g.SEQ, qb.FIRST_QUERY_SECOND) AS SECOND,
                    g.SEQ AS ELAPSED_SECONDS,
                    CASE WHEN DATEADD('second', g.SEQ, qb.FIRST_QUERY_SECOND) < qb.FIRST_MEASUREMENT_SECOND THEN TRUE ELSE FALSE END AS IS_WARMUP,
                    DATEDIFF('second', qb.FIRST_QUERY_SECOND, qb.FIRST_MEASUREMENT_SECOND) AS WARMUP_END_ELAPSED
                FROM query_bounds qb
                JOIN (
                    SELECT SEQ4() AS SEQ
                    FROM TABLE(GENERATOR(ROWCOUNT => 86400))
                ) g
                  ON g.SEQ <= DATEDIFF('second', qb.FIRST_QUERY_SECOND, qb.LAST_QUERY_SECOND)
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
                -- Forward-fill: use last known poller value for gaps between ~5s polls
                COALESCE(
                    rt.STARTED_CLUSTERS,
                    LAST_VALUE(rt.STARTED_CLUSTERS IGNORE NULLS) OVER (ORDER BY s.SECOND ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING),
                    0
                ) AS REALTIME_CLUSTERS,
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
                ) AS AVG_QUEUE_PROVISIONING_MS,
                s.IS_WARMUP,
                s.WARMUP_END_ELAPSED
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
        warmup_end_elapsed: int | None = None
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
                is_warmup,
                warmup_end,
            ) = row

            if warmup_end_elapsed is None and warmup_end is not None:
                warmup_end_elapsed = int(warmup_end)

            active_clusters_i = int(active_clusters or 0)
            realtime_clusters_i = int(realtime_clusters or 0)
            best_clusters = (
                realtime_clusters_i if realtime_clusters_i > 0 else active_clusters_i
            )
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
                    "warmup": bool(is_warmup),
                }
            )

        available = has_data
        return {
            "test_id": test_id,
            "available": available,
            "points": points if available else [],
            "warmup_end_elapsed_seconds": warmup_end_elapsed,
        }
    except Exception as e:
        logger.debug("Failed to load warehouse timeseries for %s: %s", test_id, e)
        return {"test_id": test_id, "available": False, "points": [], "error": str(e)}


@router.get("/{test_id}/overhead-timeseries")
async def get_overhead_timeseries(test_id: str) -> dict[str, Any]:
    """
    Fetch per-second app overhead timeseries for a test.

    This calculates the overhead (APP_ELAPSED_MS - SF_TOTAL_ELAPSED_MS) per second
    using enriched queries from QUERY_EXECUTIONS. For seconds without enriched data,
    we use interpolation from neighboring seconds.

    This data is useful for:
    1. Understanding network/client overhead variations over time
    2. Estimating SF execution time for non-enriched queries
    """
    try:
        pool = snowflake_pool.get_default_pool()
        prefix = _prefix()

        run_id_rows = await pool.execute_query(
            f"SELECT RUN_ID FROM {prefix}.TEST_RESULTS WHERE TEST_ID = ?",
            params=[test_id],
        )
        run_id = run_id_rows[0][0] if run_id_rows and run_id_rows[0] else None
        is_parent = bool(run_id) and str(run_id) == str(test_id)

        if is_parent:
            query = f"""
            WITH query_bounds AS (
                -- Get first query time (warmup or measurement) and last query time
                -- Also get measurement start time for warmup boundary
                SELECT
                    MIN(DATE_TRUNC('second', qe.START_TIME)) AS FIRST_QUERY_SECOND,
                    MAX(DATE_TRUNC('second', qe.START_TIME)) AS LAST_QUERY_SECOND,
                    MIN(CASE WHEN COALESCE(qe.WARMUP, FALSE) = FALSE THEN DATE_TRUNC('second', qe.START_TIME) END) AS FIRST_MEASUREMENT_SECOND
                FROM {prefix}.QUERY_EXECUTIONS qe
                JOIN {prefix}.TEST_RESULTS t ON t.TEST_ID = qe.TEST_ID
                WHERE t.RUN_ID = ?
                  AND t.TEST_ID <> ?
            ),
            seconds AS (
                SELECT
                    DATEADD('second', g.SEQ, qb.FIRST_QUERY_SECOND) AS SECOND,
                    g.SEQ AS ELAPSED_SECONDS,
                    CASE WHEN DATEADD('second', g.SEQ, qb.FIRST_QUERY_SECOND) < qb.FIRST_MEASUREMENT_SECOND THEN TRUE ELSE FALSE END AS IS_WARMUP,
                    DATEDIFF('second', qb.FIRST_QUERY_SECOND, qb.FIRST_MEASUREMENT_SECOND) AS WARMUP_END_ELAPSED
                FROM query_bounds qb
                JOIN (
                    SELECT SEQ4() AS SEQ
                    FROM TABLE(GENERATOR(ROWCOUNT => 86400))
                ) g
                  ON g.SEQ <= DATEDIFF('second', qb.FIRST_QUERY_SECOND, qb.LAST_QUERY_SECOND)
            ),
            per_second AS (
                SELECT
                    DATE_TRUNC('second', qe.START_TIME) AS SECOND,
                    COUNT(*) AS TOTAL_QUERIES,
                    SUM(IFF(qe.SF_TOTAL_ELAPSED_MS IS NOT NULL, 1, 0)) AS ENRICHED_QUERIES,
                    AVG(IFF(qe.SF_TOTAL_ELAPSED_MS IS NOT NULL, qe.APP_ELAPSED_MS - qe.SF_TOTAL_ELAPSED_MS, NULL)) AS AVG_OVERHEAD_MS,
                    PERCENTILE_CONT(0.50) WITHIN GROUP (
                        ORDER BY IFF(qe.SF_TOTAL_ELAPSED_MS IS NOT NULL, qe.APP_ELAPSED_MS - qe.SF_TOTAL_ELAPSED_MS, NULL)
                    ) AS P50_OVERHEAD_MS,
                    AVG(qe.APP_ELAPSED_MS) AS AVG_APP_MS,
                    AVG(qe.SF_TOTAL_ELAPSED_MS) AS AVG_SF_TOTAL_MS,
                    AVG(qe.SF_EXECUTION_MS) AS AVG_SF_EXEC_MS,
                    MAX(CASE WHEN COALESCE(qe.WARMUP, FALSE) THEN 1 ELSE 0 END) AS HAS_WARMUP_QUERIES
                FROM {prefix}.QUERY_EXECUTIONS qe
                JOIN {prefix}.TEST_RESULTS tr ON tr.TEST_ID = qe.TEST_ID
                WHERE tr.RUN_ID = ?
                  AND tr.TEST_ID <> ?
                  AND qe.SUCCESS = TRUE
                GROUP BY DATE_TRUNC('second', qe.START_TIME)
            ),
            with_lag AS (
                SELECT
                    s.SECOND,
                    s.ELAPSED_SECONDS,
                    s.IS_WARMUP,
                    s.WARMUP_END_ELAPSED,
                    COALESCE(ps.TOTAL_QUERIES, 0) AS TOTAL_QUERIES,
                    COALESCE(ps.ENRICHED_QUERIES, 0) AS ENRICHED_QUERIES,
                    ps.AVG_OVERHEAD_MS,
                    ps.P50_OVERHEAD_MS,
                    ps.AVG_APP_MS,
                    ps.AVG_SF_TOTAL_MS,
                    ps.AVG_SF_EXEC_MS,
                    LAG(ps.P50_OVERHEAD_MS) IGNORE NULLS OVER (ORDER BY s.SECOND) AS PREV_P50_OVERHEAD,
                    LEAD(ps.P50_OVERHEAD_MS) IGNORE NULLS OVER (ORDER BY s.SECOND) AS NEXT_P50_OVERHEAD
                FROM seconds s
                LEFT JOIN per_second ps ON ps.SECOND = s.SECOND
            )
            SELECT
                SECOND,
                ELAPSED_SECONDS,
                TOTAL_QUERIES,
                ENRICHED_QUERIES,
                AVG_OVERHEAD_MS,
                COALESCE(P50_OVERHEAD_MS, (PREV_P50_OVERHEAD + NEXT_P50_OVERHEAD) / 2, PREV_P50_OVERHEAD, NEXT_P50_OVERHEAD) AS P50_OVERHEAD_MS,
                P50_OVERHEAD_MS IS NULL AND (PREV_P50_OVERHEAD IS NOT NULL OR NEXT_P50_OVERHEAD IS NOT NULL) AS INTERPOLATED,
                AVG_APP_MS,
                AVG_SF_TOTAL_MS,
                AVG_SF_EXEC_MS,
                IS_WARMUP,
                WARMUP_END_ELAPSED
            FROM with_lag
            ORDER BY SECOND ASC
            """
            params = [test_id, test_id, test_id, test_id]
        else:
            query = f"""
            WITH query_bounds AS (
                -- Get first query time (warmup or measurement) and last query time
                -- Also get measurement start time for warmup boundary
                SELECT
                    MIN(DATE_TRUNC('second', START_TIME)) AS FIRST_QUERY_SECOND,
                    MAX(DATE_TRUNC('second', START_TIME)) AS LAST_QUERY_SECOND,
                    MIN(CASE WHEN COALESCE(WARMUP, FALSE) = FALSE THEN DATE_TRUNC('second', START_TIME) END) AS FIRST_MEASUREMENT_SECOND
                FROM {prefix}.QUERY_EXECUTIONS
                WHERE TEST_ID = ?
            ),
            seconds AS (
                SELECT
                    DATEADD('second', g.SEQ, qb.FIRST_QUERY_SECOND) AS SECOND,
                    g.SEQ AS ELAPSED_SECONDS,
                    CASE WHEN DATEADD('second', g.SEQ, qb.FIRST_QUERY_SECOND) < qb.FIRST_MEASUREMENT_SECOND THEN TRUE ELSE FALSE END AS IS_WARMUP,
                    DATEDIFF('second', qb.FIRST_QUERY_SECOND, qb.FIRST_MEASUREMENT_SECOND) AS WARMUP_END_ELAPSED
                FROM query_bounds qb
                JOIN (
                    SELECT SEQ4() AS SEQ
                    FROM TABLE(GENERATOR(ROWCOUNT => 86400))
                ) g
                  ON g.SEQ <= DATEDIFF('second', qb.FIRST_QUERY_SECOND, qb.LAST_QUERY_SECOND)
            ),
            per_second AS (
                SELECT
                    DATE_TRUNC('second', qe.START_TIME) AS SECOND,
                    COUNT(*) AS TOTAL_QUERIES,
                    SUM(IFF(qe.SF_TOTAL_ELAPSED_MS IS NOT NULL, 1, 0)) AS ENRICHED_QUERIES,
                    AVG(IFF(qe.SF_TOTAL_ELAPSED_MS IS NOT NULL, qe.APP_ELAPSED_MS - qe.SF_TOTAL_ELAPSED_MS, NULL)) AS AVG_OVERHEAD_MS,
                    PERCENTILE_CONT(0.50) WITHIN GROUP (
                        ORDER BY IFF(qe.SF_TOTAL_ELAPSED_MS IS NOT NULL, qe.APP_ELAPSED_MS - qe.SF_TOTAL_ELAPSED_MS, NULL)
                    ) AS P50_OVERHEAD_MS,
                    AVG(qe.APP_ELAPSED_MS) AS AVG_APP_MS,
                    AVG(qe.SF_TOTAL_ELAPSED_MS) AS AVG_SF_TOTAL_MS,
                    AVG(qe.SF_EXECUTION_MS) AS AVG_SF_EXEC_MS
                FROM {prefix}.QUERY_EXECUTIONS qe
                WHERE qe.TEST_ID = ?
                  AND qe.SUCCESS = TRUE
                GROUP BY DATE_TRUNC('second', qe.START_TIME)
            ),
            with_lag AS (
                SELECT
                    s.SECOND,
                    s.ELAPSED_SECONDS,
                    s.IS_WARMUP,
                    s.WARMUP_END_ELAPSED,
                    COALESCE(ps.TOTAL_QUERIES, 0) AS TOTAL_QUERIES,
                    COALESCE(ps.ENRICHED_QUERIES, 0) AS ENRICHED_QUERIES,
                    ps.AVG_OVERHEAD_MS,
                    ps.P50_OVERHEAD_MS,
                    ps.AVG_APP_MS,
                    ps.AVG_SF_TOTAL_MS,
                    ps.AVG_SF_EXEC_MS,
                    LAG(ps.P50_OVERHEAD_MS) IGNORE NULLS OVER (ORDER BY s.SECOND) AS PREV_P50_OVERHEAD,
                    LEAD(ps.P50_OVERHEAD_MS) IGNORE NULLS OVER (ORDER BY s.SECOND) AS NEXT_P50_OVERHEAD
                FROM seconds s
                LEFT JOIN per_second ps ON ps.SECOND = s.SECOND
            )
            SELECT
                SECOND,
                ELAPSED_SECONDS,
                TOTAL_QUERIES,
                ENRICHED_QUERIES,
                AVG_OVERHEAD_MS,
                COALESCE(P50_OVERHEAD_MS, (PREV_P50_OVERHEAD + NEXT_P50_OVERHEAD) / 2, PREV_P50_OVERHEAD, NEXT_P50_OVERHEAD) AS P50_OVERHEAD_MS,
                P50_OVERHEAD_MS IS NULL AND (PREV_P50_OVERHEAD IS NOT NULL OR NEXT_P50_OVERHEAD IS NOT NULL) AS INTERPOLATED,
                AVG_APP_MS,
                AVG_SF_TOTAL_MS,
                AVG_SF_EXEC_MS,
                IS_WARMUP,
                WARMUP_END_ELAPSED
            FROM with_lag
            ORDER BY SECOND ASC
            """
            params = [test_id, test_id]

        rows = await pool.execute_query(query, params=params)

        points: list[dict[str, Any]] = []
        has_data = False
        total_enriched = 0
        total_queries = 0
        warmup_end_elapsed: int | None = None

        for row in rows:
            (
                ts,
                elapsed,
                total_q,
                enriched_q,
                avg_overhead,
                p50_overhead,
                interpolated,
                avg_app,
                avg_sf_total,
                avg_sf_exec,
                is_warmup,
                warmup_end,
            ) = row

            if warmup_end_elapsed is None and warmup_end is not None:
                warmup_end_elapsed = int(warmup_end)

            total_q_i = int(total_q or 0)
            enriched_q_i = int(enriched_q or 0)
            total_queries += total_q_i
            total_enriched += enriched_q_i

            if enriched_q_i > 0 or interpolated:
                has_data = True

            points.append(
                {
                    "timestamp": ts.isoformat()
                    if hasattr(ts, "isoformat")
                    else str(ts),
                    "elapsed_seconds": float(elapsed or 0),
                    "total_queries": total_q_i,
                    "enriched_queries": enriched_q_i,
                    "avg_overhead_ms": _to_float_or_none(avg_overhead),
                    "p50_overhead_ms": _to_float_or_none(p50_overhead),
                    "interpolated": bool(interpolated),
                    "avg_app_ms": _to_float_or_none(avg_app),
                    "avg_sf_total_ms": _to_float_or_none(avg_sf_total),
                    "avg_sf_exec_ms": _to_float_or_none(avg_sf_exec),
                    "warmup": bool(is_warmup),
                }
            )

        enrichment_ratio_pct = (
            round(100.0 * total_enriched / total_queries, 1)
            if total_queries > 0
            else 0.0
        )

        return {
            "test_id": test_id,
            "available": has_data,
            "total_queries": total_queries,
            "total_enriched": total_enriched,
            "enrichment_ratio_pct": enrichment_ratio_pct,
            "points": points if has_data else [],
            "warmup_end_elapsed_seconds": warmup_end_elapsed,
        }
    except Exception as e:
        logger.debug("Failed to load overhead timeseries for %s: %s", test_id, e)
        return {
            "test_id": test_id,
            "available": False,
            "points": [],
            "error": str(e),
        }


@router.delete("/{test_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_test(test_id: str) -> None:
    """Delete a test and all related data.

    If this is a parent test (run_id == test_id), cascade deletes all child
    worker tests and their associated data.
    """
    try:
        pool = snowflake_pool.get_default_pool()
        prefix = _prefix()

        # Check if this is a parent test (run_id == test_id)
        rows = await pool.execute_query(
            f"SELECT RUN_ID FROM {prefix}.TEST_RESULTS WHERE TEST_ID = ?",
            params=[test_id],
        )
        if not rows:
            # Test not found, nothing to delete
            return None

        run_id = rows[0][0]
        is_parent = run_id is not None and str(run_id) == str(test_id)

        if is_parent:
            # Cascade delete: remove all data for this run_id (parent + children)

            # Delete from tables that use RUN_ID
            await pool.execute_query(
                f"DELETE FROM {prefix}.WORKER_METRICS_SNAPSHOTS WHERE RUN_ID = ?",
                params=[test_id],
            )
            await pool.execute_query(
                f"DELETE FROM {prefix}.WAREHOUSE_POLL_SNAPSHOTS WHERE RUN_ID = ?",
                params=[test_id],
            )
            await pool.execute_query(
                f"DELETE FROM {prefix}.FIND_MAX_STEP_HISTORY WHERE RUN_ID = ?",
                params=[test_id],
            )

            # Get all test_ids in this run (parent + children)
            child_rows = await pool.execute_query(
                f"SELECT TEST_ID FROM {prefix}.TEST_RESULTS WHERE RUN_ID = ?",
                params=[test_id],
            )
            all_test_ids = [str(r[0]) for r in child_rows] if child_rows else [test_id]

            # Delete from tables keyed by TEST_ID for all tests in the run
            for tid in all_test_ids:
                await pool.execute_query(
                    f"DELETE FROM {prefix}.METRICS_SNAPSHOTS WHERE TEST_ID = ?",
                    params=[tid],
                )
                await pool.execute_query(
                    f"DELETE FROM {prefix}.QUERY_EXECUTIONS WHERE TEST_ID = ?",
                    params=[tid],
                )

            # Delete all TEST_RESULTS for this run (parent + children)
            await pool.execute_query(
                f"DELETE FROM {prefix}.TEST_RESULTS WHERE RUN_ID = ?",
                params=[test_id],
            )

            # Delete from Hybrid control tables (children first due to FK constraints)
            await pool.execute_query(
                f"DELETE FROM {prefix}.WORKER_HEARTBEATS WHERE RUN_ID = ?",
                params=[test_id],
            )
            await pool.execute_query(
                f"DELETE FROM {prefix}.RUN_CONTROL_EVENTS WHERE RUN_ID = ?",
                params=[test_id],
            )
            await pool.execute_query(
                f"DELETE FROM {prefix}.RUN_STATUS WHERE RUN_ID = ?",
                params=[test_id],
            )
        else:
            # Single test delete (child or standalone test)
            await pool.execute_query(
                f"DELETE FROM {prefix}.METRICS_SNAPSHOTS WHERE TEST_ID = ?",
                params=[test_id],
            )
            await pool.execute_query(
                f"DELETE FROM {prefix}.QUERY_EXECUTIONS WHERE TEST_ID = ?",
                params=[test_id],
            )
            await pool.execute_query(
                f"DELETE FROM {prefix}.TEST_RESULTS WHERE TEST_ID = ?",
                params=[test_id],
            )

        return None
    except Exception as e:
        raise http_exception("delete test", e)


@router.post("/{test_id}/rerun")
async def rerun_test(test_id: str) -> dict[str, Any]:
    """Re-run a test with the same configuration via OrchestratorService.

    This endpoint creates a new run from the original test's template.
    """
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

        template_id = test_config.get("template_id")
        if not template_id:
            raise HTTPException(
                status_code=400, detail="Cannot rerun: missing template_id"
            )

        # Load template and create run via orchestrator
        template = await registry._load_template(str(template_id))
        template_config = dict(template.get("config") or {})
        template_name = str(template.get("template_name") or "")

        scenario = registry._scenario_from_template_config(
            template_name, template_config
        )

        run_id = await orchestrator.create_run(
            template_id=str(template.get("template_id") or template_id),
            template_config=template_config,
            scenario=scenario,
        )
        return {"new_test_id": run_id}
    except HTTPException:
        raise
    except KeyError:
        raise HTTPException(status_code=404, detail="Template not found")
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

        scenario_config = test_config.get("scenario")
        if not isinstance(scenario_config, dict):
            scenario_config = {}
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
    """Get the enrichment status for a test, including progress stats."""
    from backend.core.results_store import get_enrichment_status

    try:
        pool = snowflake_pool.get_default_pool()
        prefix = _prefix()
        rows = await pool.execute_query(
            f"""
            SELECT RUN_ID, STATUS, ENRICHMENT_STATUS, ENRICHMENT_ERROR
            FROM {prefix}.TEST_RESULTS
            WHERE TEST_ID = ?
            """,
            params=[test_id],
        )
        if not rows:
            raise HTTPException(status_code=404, detail="Test not found")

        run_id, status_db, enrichment_status_db, enrichment_error_db = rows[0]
        test_status = str(status_db or "").upper()
        is_parent_run = bool(run_id) and str(run_id) == str(test_id)

        if is_parent_run and run_id:
            agg_status, agg_error = await _aggregate_parent_enrichment_status(
                pool=pool, run_id=str(run_id)
            )
            (
                total_queries,
                enriched_queries,
                enrichment_ratio,
            ) = await _aggregate_parent_enrichment_stats(pool=pool, run_id=str(run_id))
            enrichment_status = agg_status or str(enrichment_status_db or "").upper()
            enrichment_error = agg_error or enrichment_error_db
        else:
            status_info = await get_enrichment_status(test_id=test_id)
            if status_info is None:
                raise HTTPException(status_code=404, detail="Test not found")
            enrichment_status = str(status_info.get("enrichment_status") or "").upper()
            enrichment_error = status_info.get("enrichment_error")
            total_queries = status_info.get("total_queries", 0)
            enriched_queries = status_info.get("enriched_queries", 0)
            enrichment_ratio = status_info.get("enrichment_ratio", 0.0)

        is_complete = enrichment_status in ("COMPLETED", "SKIPPED") or (
            enrichment_ratio >= 0.90 and total_queries > 0
        )

        return {
            "test_id": test_id,
            "test_status": test_status,
            "enrichment_status": enrichment_status or None,
            "enrichment_error": enrichment_error,
            "total_queries": int(total_queries or 0),
            "enriched_queries": int(enriched_queries or 0),
            "enrichment_ratio_pct": round(enrichment_ratio * 100, 1),
            "is_complete": is_complete,
            "can_retry": (test_status == "COMPLETED" and enrichment_status == "FAILED"),
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


class DeepCompareAiRequest(BaseModel):
    """Request model for deep compare AI analysis."""
    primary_id: str
    secondary_id: str
    force_regenerate: bool = False


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


def _build_latency_variance_text(
    *,
    e2e_p50: float | None,
    e2e_p95: float | None,
    e2e_spread_ratio: float | None,
    sf_p50: float | None,
    sf_p95: float | None,
    sf_spread_ratio: float | None,
) -> str:
    """
    Build latency variance analysis text for AI prompts.

    The spread ratio (P95/P50) indicates latency variance:
    - <3x: Low variance, consistent performance
    - 3-5x: Moderate variance, some tail latency
    - >5x: High variance, significant tail latency issues

    Comparing end-to-end vs SF execution spread helps identify WHERE variance occurs:
    - High E2E, Low SF: Network/client-side issues
    - Low E2E, High SF: Snowflake execution variance (contention, cache, scaling)
    - High both: Problems at multiple layers
    """
    lines = ["LATENCY VARIANCE ANALYSIS:"]

    # End-to-end spread
    if e2e_spread_ratio is not None:
        severity = (
            "LOW (consistent)"
            if e2e_spread_ratio < 3
            else "MODERATE" if e2e_spread_ratio < 5 else "HIGH (variable tail latency)"
        )
        lines.append(
            f"- End-to-end spread: {e2e_spread_ratio:.1f}x (P95={e2e_p95:.1f}ms / P50={e2e_p50:.1f}ms) - {severity}"
        )
    else:
        lines.append("- End-to-end spread: N/A")

    # SF execution spread
    if sf_spread_ratio is not None:
        severity = (
            "LOW (consistent)"
            if sf_spread_ratio < 3
            else "MODERATE" if sf_spread_ratio < 5 else "HIGH (variable tail latency)"
        )
        lines.append(
            f"- SF execution spread: {sf_spread_ratio:.1f}x (P95={sf_p95:.1f}ms / P50={sf_p50:.1f}ms) - {severity}"
        )
    else:
        lines.append("- SF execution spread: N/A (enrichment data unavailable)")

    # Diagnostic interpretation
    if e2e_spread_ratio is not None and sf_spread_ratio is not None:
        if e2e_spread_ratio > 5 and sf_spread_ratio < 3:
            lines.append(
                "- DIAGNOSIS: High variance in app/network layer, Snowflake execution is consistent. "
                "Check network latency, client processing, or connection pool issues."
            )
        elif e2e_spread_ratio < 3 and sf_spread_ratio > 5:
            lines.append(
                "- DIAGNOSIS: High variance in Snowflake execution, app layer is consistent. "
                "Check warehouse contention, multi-cluster scaling, cold cache hits, or query compilation."
            )
        elif e2e_spread_ratio > 5 and sf_spread_ratio > 5:
            lines.append(
                "- DIAGNOSIS: High variance at both layers. "
                "Multiple bottlenecks - address Snowflake execution variance first, then app/network."
            )
        elif e2e_spread_ratio < 3 and sf_spread_ratio < 3:
            lines.append("- DIAGNOSIS: Consistent latency at all layers. Good performance profile.")

    return "\n".join(lines)


def _build_postgres_latency_analysis_text(
    *,
    e2e_p50: float | None,
    e2e_p95: float | None,
    pg_mean_exec_time_ms: float | None,
) -> str:
    """
    Build PostgreSQL-specific latency analysis comparing end-to-end vs server execution time.
    
    For PostgreSQL, we use pg_stat_statements mean execution time rather than per-query metrics.
    This provides insight into the network overhead vs server processing time.
    """
    lines = ["POSTGRES LATENCY BREAKDOWN:"]
    
    if e2e_p50 is not None and pg_mean_exec_time_ms is not None and pg_mean_exec_time_ms > 0:
        network_overhead_ms = e2e_p50 - pg_mean_exec_time_ms
        network_pct = (network_overhead_ms / e2e_p50 * 100) if e2e_p50 > 0 else 0
        server_pct = 100 - network_pct
        
        lines.append(f"- End-to-end P50 latency: {e2e_p50:.2f}ms (what your application sees)")
        lines.append(f"- Server execution time: {pg_mean_exec_time_ms:.3f}ms (PostgreSQL processing)")
        lines.append(f"- Network/protocol overhead: {network_overhead_ms:.2f}ms ({network_pct:.1f}% of total)")
        lines.append("")
        
        if network_pct > 95:
            lines.append(
                "- DIAGNOSIS: Network-dominated latency (>95%). PostgreSQL is extremely fast, "
                "but network round-trip dominates. To improve: reduce geographic distance, "
                "use connection pooling, batch operations, or consider read replicas closer to the app."
            )
        elif network_pct > 80:
            lines.append(
                "- DIAGNOSIS: Network-heavy latency (80-95%). Server execution is efficient, "
                "but network overhead is significant. Consider connection pooling, batching, "
                "or moving compute closer to the database."
            )
        elif network_pct > 50:
            lines.append(
                "- DIAGNOSIS: Balanced latency profile. Both network and server contribute meaningfully. "
                "Optimization opportunities exist at both layers."
            )
        else:
            lines.append(
                "- DIAGNOSIS: Server-dominated latency. PostgreSQL execution time is the primary factor. "
                "Consider query optimization, indexing improvements, or instance sizing."
            )
    elif e2e_p50 is not None:
        lines.append(f"- End-to-end P50 latency: {e2e_p50:.2f}ms")
        lines.append("- Server execution time: N/A (pg_stat_statements data unavailable)")
    else:
        lines.append("- Latency data: N/A")
    
    return "\n".join(lines)


def _build_postgres_server_metrics_guidance() -> str:
    """
    Return guidance text for interpreting PostgreSQL server metrics.
    
    This explains what the pg_stat_statements metrics mean and how to interpret them
    for OLTP workload analysis.
    """
    return """
POSTGRESQL METRICS INTERPRETATION GUIDE:

**Cache Hit Ratio** (shared_blks_hit / (shared_blks_hit + shared_blks_read)):
- >99%: Excellent - working set fits in shared_buffers, minimal disk I/O
- 95-99%: Good - most data cached, occasional disk reads for less frequent data
- 90-95%: Acceptable - consider increasing shared_buffers or optimizing queries
- <90%: Concerning - significant disk I/O, may indicate under-provisioned memory

IMPORTANT: High cache hit ratio does NOT mean "same query repeated" - it means the INDEX 
PAGES and DATA PAGES are cached in memory. Different queries with different parameter 
values (e.g., different primary keys) all benefit from cached B-tree index pages.

**Mean Server Execution Time**: 
- PostgreSQL's actual query processing time (excludes network)
- <1ms: Excellent for OLTP point lookups
- 1-10ms: Good for indexed queries
- 10-100ms: May indicate table scans or complex joins
- >100ms: Likely full table scans or analytical queries

**Network Overhead** (App latency - Server time):
- This is typically the LARGEST component of end-to-end latency for OLTP
- Includes: TCP round-trip, TLS handshake (if not pooled), protocol parsing, result serialization
- Optimize via: Connection pooling, geographic proximity, batching operations

**WAL (Write-Ahead Log) Generated**:
- Indicates write activity volume
- High WAL with few writes = large row updates or indexes being modified
- Monitor for replication lag implications

**Block Read Time**:
- Time spent on actual disk I/O (only when track_io_timing=on)
- High block read time with low cache hit = memory pressure
- High block read time with high cache hit = occasional cold data access
"""


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
    latency_variance_text: str,
    # New parameters for scaling and workload config
    scaling_mode: str = "AUTO",
    min_workers: int = 1,
    max_workers: int | None = None,
    min_threads_per_worker: int | None = None,
    max_threads_per_worker: int | None = None,
    cpu_limit_pct: float | None = None,
    mem_limit_pct: float | None = None,
    point_lookup_pct: int = 0,
    range_scan_pct: int = 0,
    insert_pct: int = 0,
    update_pct: int = 0,
    slo_p95_target: float | None = None,
    slo_p99_target: float | None = None,
    slo_max_error_pct: float | None = None,
    think_time_ms: int = 0,
    warmup_seconds: int = 0,
    use_cached_result: bool = True,
    # Multi-cluster and cost parameters
    min_clusters: int = 1,
    max_clusters: int = 1,
    scaling_policy: str = "STANDARD",
    warehouse_credits_used: float | None = None,
) -> str:
    """Build prompt for CONCURRENCY mode (fixed worker count)."""
    error_pct = (failed_ops / total_ops * 100) if total_ops > 0 else 0
    
    # Determine if this is a Postgres test
    table_type_u = str(table_type or "").upper().strip()
    is_postgres = table_type_u == "POSTGRES"

    logs_section = ""
    if execution_logs:
        logs_section = f"""
EXECUTION LOGS (filtered for relevance):
{execution_logs}
"""

    # Build scaling mode description
    scaling_mode_desc = {
        "FIXED": "FIXED (static worker count, no auto-scaling)",
        "AUTO": "AUTO (workers scale automatically, unbounded)",
        "BOUNDED": "BOUNDED (workers scale within min/max limits)",
    }.get(scaling_mode, scaling_mode)

    # Build scaling configuration text
    scaling_config_lines = [f"- Scaling Mode: {scaling_mode_desc}"]
    if scaling_mode == "BOUNDED":
        scaling_config_lines.append(f"- Worker Bounds: {min_workers}-{max_workers or 'unlimited'} workers")
        if min_threads_per_worker is not None or max_threads_per_worker is not None:
            scaling_config_lines.append(
                f"- Threads per Worker: {min_threads_per_worker or 1}-{max_threads_per_worker or 'unlimited'}"
            )
    elif scaling_mode == "AUTO":
        scaling_config_lines.append(f"- Min Workers: {min_workers}")
        if max_workers:
            scaling_config_lines.append(f"- Max Workers: {max_workers}")
    
    # Add multi-cluster config for Snowflake
    if not is_postgres and max_clusters > 1:
        scaling_config_lines.append(f"- Multi-Cluster: {min_clusters}-{max_clusters} clusters ({scaling_policy})")
    
    # Add guardrails if configured
    if cpu_limit_pct is not None or mem_limit_pct is not None:
        guardrail_parts = []
        if cpu_limit_pct is not None:
            guardrail_parts.append(f"CPU<{cpu_limit_pct}%")
        if mem_limit_pct is not None:
            guardrail_parts.append(f"Memory<{mem_limit_pct}%")
        scaling_config_lines.append(f"- Resource Guardrails: {', '.join(guardrail_parts)}")
    
    scaling_config_text = "\n".join(scaling_config_lines)

    # Build cost analysis text (Snowflake only)
    cost_text = ""
    if not is_postgres and warehouse_credits_used is not None and warehouse_credits_used > 0:
        cost_per_op = (warehouse_credits_used / total_ops * 1000) if total_ops > 0 else 0
        cost_text = f"""
COST ANALYSIS:
- Warehouse Credits Used: {warehouse_credits_used:.4f}
- Cost per 1000 Operations: {cost_per_op:.6f} credits
- Total Operations: {total_ops:,}"""

    # Build workload mix text
    mix_parts = []
    if point_lookup_pct > 0:
        mix_parts.append(f"Point Lookup: {point_lookup_pct}%")
    if range_scan_pct > 0:
        mix_parts.append(f"Range Scan: {range_scan_pct}%")
    if insert_pct > 0:
        mix_parts.append(f"Insert: {insert_pct}%")
    if update_pct > 0:
        mix_parts.append(f"Update: {update_pct}%")
    workload_mix_text = ", ".join(mix_parts) if mix_parts else "Not specified"

    # Build SLO targets text
    slo_lines = []
    if slo_p95_target is not None and slo_p95_target > 0:
        slo_lines.append(f"P95 ≤ {slo_p95_target}ms")
    if slo_p99_target is not None and slo_p99_target > 0:
        slo_lines.append(f"P99 ≤ {slo_p99_target}ms")
    if slo_max_error_pct is not None and slo_max_error_pct >= 0:
        slo_lines.append(f"Error Rate ≤ {slo_max_error_pct}%")
    slo_text = ", ".join(slo_lines) if slo_lines else "None configured"

    # SLO evaluation
    slo_evaluation = ""
    if slo_lines:
        p95_pass = slo_p95_target is None or slo_p95_target <= 0 or p95 <= slo_p95_target
        p99_pass = slo_p99_target is None or slo_p99_target <= 0 or p99 <= slo_p99_target
        error_pass = slo_max_error_pct is None or error_pct <= slo_max_error_pct
        all_pass = p95_pass and p99_pass and error_pass
        slo_evaluation = f"\n- SLO Status: {'✅ PASSED' if all_pass else '❌ FAILED'}"
        if not p95_pass:
            slo_evaluation += f" (P95: {p95:.1f}ms > {slo_p95_target}ms target)"
        if not p99_pass:
            slo_evaluation += f" (P99: {p99:.1f}ms > {slo_p99_target}ms target)"
        if not error_pass:
            slo_evaluation += f" (Error: {error_pct:.2f}% > {slo_max_error_pct}% target)"
    
    # Build platform-specific guidance
    if is_postgres:
        platform_intro = "You are analyzing a **PostgreSQL** CONCURRENCY mode benchmark test."
        latency_analysis_guidance = """3. **Latency Breakdown Analysis**: Interpret the server vs network latency split.
   - What percentage of latency is network overhead vs PostgreSQL server execution?
   - Is the database the bottleneck, or is network round-trip dominating?
   - For high network overhead: consider connection pooling, geographic proximity, batching
   - For high server time: look at cache hit ratio, query optimization, indexing

4. **PostgreSQL Server Metrics Analysis**: Interpret the pg_stat_statements data.
   - Cache hit ratio: Is the working set fitting in shared_buffers? (>99% is excellent)
   - Mean server time: Is PostgreSQL processing efficiently? (<1ms excellent for OLTP)
   - WAL generated: Is write activity reasonable for the workload mix?
   - Block read time: Is there excessive disk I/O?"""
        bottleneck_guidance = """5. **Bottleneck Analysis**: What's limiting performance?
   - Network bound (high network overhead, low server time)?
   - Database bound (high server time, cache misses)?
   - Connection pool bound (waiting for connections)?
   - Instance size bound (consider larger Postgres instance)?"""
        recommendations_guidance = """6. **Recommendations**: Specific suggestions:
   - Should concurrency be increased or decreased?
   - Would a larger Postgres instance help?
   - Are there query optimization or indexing opportunities?
   - Would connection pooling or batching improve throughput?
   - Is the test client too far from the database (network latency)?"""
    else:
        platform_intro = "You are analyzing a Snowflake CONCURRENCY mode benchmark test."
        latency_analysis_guidance = """3. **Latency Variance Analysis**: Interpret the spread ratios (P95/P50).
   - Is variance coming from Snowflake execution or app/network layer?
   - If high SF spread: warehouse contention, cold cache, multi-cluster scaling?
   - If high E2E but low SF: network latency, client processing, connection pooling?"""
        bottleneck_guidance = """4. **Bottleneck Analysis**: What's limiting performance?
   - Compute bound (high CPU, low queue times)?
   - Queue bound (high queue_overload_ms)?
   - Data bound (high bytes_scanned)?"""
        recommendations_guidance = """5. **Recommendations**: Specific suggestions:
   - Should concurrency be increased or decreased?
   - Would a larger warehouse help?
   - Any query optimization opportunities?"""

    return f"""{platform_intro}

**Mode: CONCURRENCY (Fixed Workers / Closed Model)**
This test ran a fixed number of concurrent workers for a set duration to measure steady-state performance under constant load.

TEST SUMMARY:
- Test Name: {test_name}
- Status: {test_status}
- Table Type: {table_type}
- {"Instance" if is_postgres else "Warehouse"}: {warehouse} ({warehouse_size})
- Fixed Workers: {concurrency} workers
- Duration: {duration}s (Warmup: {warmup_seconds}s)
- Think Time: {think_time_ms}ms between operations
- Result Cache: {"Enabled" if use_cached_result else "Disabled"}

SCALING CONFIGURATION:
{scaling_config_text}

WORKLOAD MIX: {workload_mix_text}

SLO TARGETS: {slo_text}{slo_evaluation}

RESULTS:
- Total Operations: {total_ops} (Reads: {read_ops}, Writes: {write_ops})
- Failed Operations: {failed_ops} ({error_pct:.2f}%)
- Throughput: {ops_per_sec:.1f} ops/sec
- Latency: p50={p50:.1f}ms, p95={p95:.1f}ms, p99={p99:.1f}ms

{latency_variance_text}

{breakdown_text}

{qps_info}
{wh_text}
{cost_text}
{f"Additional Context: {context}" if context else ""}
{logs_section}
Provide analysis structured as:

1. **Performance Summary**: How did the system perform under {concurrency} concurrent workers?
   - Is throughput ({ops_per_sec:.1f} ops/sec) reasonable for this configuration?
   - Are latencies acceptable? (p95={p95:.1f}ms, p99={p99:.1f}ms)
   - Is error rate ({error_pct:.2f}%) acceptable?
   - Did the test meet SLO targets (if configured)?

2. **Key Findings**: What stands out in the metrics?
   - Any concerning latency outliers (compare p50 vs p95 vs p99)?
   - Read vs write performance differences?
   {"- Cache hit ratio and server execution time?" if is_postgres else "- Queue wait times indicating saturation?"}

{latency_analysis_guidance}

{bottleneck_guidance}

{recommendations_guidance}

{"7" if is_postgres else "6"}. **Overall Grade**: A/B/C/D/F based on:
   - **A**: Excellent - met all SLOs, low error rate (<0.1%), stable latencies (p99/p50 < 3x), good throughput
   - **B**: Good - met most SLOs, acceptable error rate (<1%), moderate latency variance
   - **C**: Fair - missed some SLOs, noticeable issues but functional
   - **D**: Poor - significant SLO misses, high errors (>5%), or severe latency spikes
   - **F**: Failed - test failed, extremely high errors, or unusable performance

Keep analysis concise and actionable. Use bullet points with specific numbers."""


def _build_qps_prompt(
    *,
    test_name: str,
    test_status: str,
    table_type: str,
    warehouse: str,
    warehouse_size: str,
    target_qps: int,
    min_connections: int,
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
    latency_variance_text: str,
    # New parameters for scaling and workload config
    scaling_mode: str = "AUTO",
    min_workers: int = 1,
    max_workers: int | None = None,
    min_threads_per_worker: int | None = None,
    max_threads_per_worker: int | None = None,
    cpu_limit_pct: float | None = None,
    mem_limit_pct: float | None = None,
    point_lookup_pct: int = 0,
    range_scan_pct: int = 0,
    insert_pct: int = 0,
    update_pct: int = 0,
    slo_p95_target: float | None = None,
    slo_p99_target: float | None = None,
    slo_max_error_pct: float | None = None,
    think_time_ms: int = 0,
    warmup_seconds: int = 0,
    use_cached_result: bool = True,
    start_threads: int | None = None,
    max_threads_per_interval: int | None = None,
    # Multi-cluster and cost parameters
    min_clusters: int = 1,
    max_clusters: int = 1,
    scaling_policy: str = "STANDARD",
    warehouse_credits_used: float | None = None,
) -> str:
    """Build prompt for QPS mode (auto-scaling to target)."""
    error_pct = (failed_ops / total_ops * 100) if total_ops > 0 else 0
    target_achieved_pct = (ops_per_sec / target_qps * 100) if target_qps > 0 else 0
    
    # Determine if this is a Postgres test
    table_type_u = str(table_type or "").upper().strip()
    is_postgres = table_type_u == "POSTGRES"

    logs_section = ""
    if execution_logs:
        logs_section = f"""
AUTO-SCALER LOGS (showing controller decisions):
{execution_logs}
"""

    # Build scaling mode description
    scaling_mode_desc = {
        "FIXED": "FIXED (static thread count, no auto-scaling)",
        "AUTO": "AUTO (threads scale automatically, unbounded)",
        "BOUNDED": "BOUNDED (threads scale within min/max limits)",
    }.get(scaling_mode, scaling_mode)

    # Build scaling configuration text
    scaling_config_lines = [f"- Scaling Mode: {scaling_mode_desc}"]
    if scaling_mode == "BOUNDED":
        scaling_config_lines.append(f"- Worker Bounds: {min_workers}-{max_workers or 'unlimited'} workers")
        if min_threads_per_worker is not None or max_threads_per_worker is not None:
            scaling_config_lines.append(
                f"- Threads per Worker: {min_threads_per_worker or 1}-{max_threads_per_worker or 'unlimited'}"
            )
    elif scaling_mode == "AUTO":
        scaling_config_lines.append(f"- Min Workers: {min_workers}")
        if max_workers:
            scaling_config_lines.append(f"- Max Workers: {max_workers}")
    
    if start_threads is not None:
        scaling_config_lines.append(f"- Starting Threads: {start_threads}")
    if max_threads_per_interval is not None:
        scaling_config_lines.append(f"- Max Threads/Interval: {max_threads_per_interval}")
    
    # Add multi-cluster config for Snowflake
    if not is_postgres and max_clusters > 1:
        scaling_config_lines.append(f"- Multi-Cluster: {min_clusters}-{max_clusters} clusters ({scaling_policy})")
    
    # Add guardrails if configured
    if cpu_limit_pct is not None or mem_limit_pct is not None:
        guardrail_parts = []
        if cpu_limit_pct is not None:
            guardrail_parts.append(f"CPU<{cpu_limit_pct}%")
        if mem_limit_pct is not None:
            guardrail_parts.append(f"Memory<{mem_limit_pct}%")
        scaling_config_lines.append(f"- Resource Guardrails: {', '.join(guardrail_parts)}")
    
    scaling_config_text = "\n".join(scaling_config_lines)

    # Build cost analysis text (Snowflake only)
    cost_text = ""
    if not is_postgres and warehouse_credits_used is not None and warehouse_credits_used > 0:
        cost_per_op = (warehouse_credits_used / total_ops * 1000) if total_ops > 0 else 0
        cost_text = f"""
COST ANALYSIS:
- Warehouse Credits Used: {warehouse_credits_used:.4f}
- Cost per 1000 Operations: {cost_per_op:.6f} credits
- Total Operations: {total_ops:,}"""

    # Build workload mix text
    mix_parts = []
    if point_lookup_pct > 0:
        mix_parts.append(f"Point Lookup: {point_lookup_pct}%")
    if range_scan_pct > 0:
        mix_parts.append(f"Range Scan: {range_scan_pct}%")
    if insert_pct > 0:
        mix_parts.append(f"Insert: {insert_pct}%")
    if update_pct > 0:
        mix_parts.append(f"Update: {update_pct}%")
    workload_mix_text = ", ".join(mix_parts) if mix_parts else "Not specified"

    # Build SLO targets text
    slo_lines = []
    if slo_p95_target is not None and slo_p95_target > 0:
        slo_lines.append(f"P95 ≤ {slo_p95_target}ms")
    if slo_p99_target is not None and slo_p99_target > 0:
        slo_lines.append(f"P99 ≤ {slo_p99_target}ms")
    if slo_max_error_pct is not None and slo_max_error_pct >= 0:
        slo_lines.append(f"Error Rate ≤ {slo_max_error_pct}%")
    slo_text = ", ".join(slo_lines) if slo_lines else "None configured"

    # SLO evaluation
    slo_evaluation = ""
    if slo_lines:
        p95_pass = slo_p95_target is None or slo_p95_target <= 0 or p95 <= slo_p95_target
        p99_pass = slo_p99_target is None or slo_p99_target <= 0 or p99 <= slo_p99_target
        error_pass = slo_max_error_pct is None or error_pct <= slo_max_error_pct
        all_pass = p95_pass and p99_pass and error_pass
        slo_evaluation = f"\n- SLO Status: {'✅ PASSED' if all_pass else '❌ FAILED'}"
        if not p95_pass:
            slo_evaluation += f" (P95: {p95:.1f}ms > {slo_p95_target}ms target)"
        if not p99_pass:
            slo_evaluation += f" (P99: {p99:.1f}ms > {slo_p99_target}ms target)"
        if not error_pass:
            slo_evaluation += f" (Error: {error_pct:.2f}% > {slo_max_error_pct}% target)"
    
    # Platform-specific intro and guidance
    if is_postgres:
        platform_intro = "You are analyzing a **PostgreSQL** QPS mode benchmark test."
        latency_guidance = """3. **Latency Breakdown Analysis**: Interpret the server vs network latency split.
   - What percentage of latency is network overhead vs PostgreSQL server execution?
   - Is the database the bottleneck, or is network round-trip dominating?
   - Cache hit ratio: Is the working set fitting in shared_buffers?
   - Mean server time: Is PostgreSQL processing efficiently?"""
        bottleneck_guidance = """4. **Bottleneck Analysis**: What limited target achievement?
   - Max workers reached?
   - Network latency limiting throughput?
   - PostgreSQL server capacity (check server execution time)?
   - Connection pool exhaustion?
   - Resource guardrails triggered (CPU/Memory limits)?
   - High error rate?"""
        recommendations_guidance = """5. **Recommendations**:
   - Adjust target QPS (higher or lower)?
   - Increase max_concurrency or max_workers?
   - Larger Postgres instance needed?
   - Improve network latency (geographic proximity)?
   - Query optimizations or better indexing?
   - Adjust resource guardrails if they're limiting scaling?"""
    else:
        platform_intro = "You are analyzing a Snowflake QPS mode benchmark test."
        latency_guidance = """3. **Latency Variance Analysis**: Interpret the spread ratios (P95/P50).
   - Is variance coming from Snowflake execution or app/network layer?
   - If high SF spread: warehouse contention, cold cache, multi-cluster scaling?
   - If high E2E but low SF: network latency, client processing, connection pooling?"""
        bottleneck_guidance = """4. **Bottleneck Analysis**: What limited target achievement?
   - Max workers reached?
   - Warehouse saturation (queue times)?
   - Resource guardrails triggered (CPU/Memory limits)?
   - High error rate?"""
        recommendations_guidance = """5. **Recommendations**:
   - Adjust target QPS (higher or lower)?
   - Increase max_concurrency or max_workers?
   - Larger warehouse needed?
   - Query optimizations?
   - Adjust resource guardrails if they're limiting scaling?"""

    return f"""{platform_intro}

**Mode: QPS (Auto-Scale to Target Throughput)**
This test dynamically scaled threads to achieve a target throughput of {target_qps} ops/sec. The controller adjusts concurrency based on achieved vs target QPS, evaluating every ~10 seconds.

TEST SUMMARY:
- Test Name: {test_name}
- Status: {test_status}
- Table Type: {table_type}
- {"Instance" if is_postgres else "Warehouse"}: {warehouse} ({warehouse_size})
- Target QPS: {target_qps} ops/sec
- Achieved QPS: {ops_per_sec:.1f} ops/sec ({target_achieved_pct:.1f}% of target)
- Duration: {duration}s (Warmup: {warmup_seconds}s)
- Think Time: {think_time_ms}ms between operations
- Result Cache: {"Enabled" if use_cached_result else "Disabled"}

SCALING CONFIGURATION:
{scaling_config_text}
- Connection Range: {min_connections}-{max_concurrency} (total threads)

WORKLOAD MIX: {workload_mix_text}

SLO TARGETS: {slo_text}{slo_evaluation}

RESULTS:
- Total Operations: {total_ops} (Reads: {read_ops}, Writes: {write_ops})
- Failed Operations: {failed_ops} ({error_pct:.2f}%)
- Latency: p50={p50:.1f}ms, p95={p95:.1f}ms, p99={p99:.1f}ms

{latency_variance_text}

{breakdown_text}

{qps_info}
{wh_text}
{cost_text}
{f"Additional Context: {context}" if context else ""}
{logs_section}
Provide analysis structured as:

1. **Target Achievement**: Did the test hit the target?
   - Target: {target_qps} ops/sec, Achieved: {ops_per_sec:.1f} ops/sec ({target_achieved_pct:.1f}%)
   - If target not achieved, why? (hit max workers? high latency? errors? guardrails?)
   - Was the target realistic for this configuration?
   - Did the test meet SLO targets (if configured)?

2. **Auto-Scaler Performance**: How well did the controller perform?
   - Did it effectively scale to meet demand?
   - Were there oscillations or instability? (Check the logs for scaling decisions)
   - Did scaling mode ({scaling_mode}) allow sufficient flexibility?
   {"- Connection pool utilization and PostgreSQL connection limits?" if is_postgres else "- Queue times indicating the controller couldn't keep up?"}

{latency_guidance}

{bottleneck_guidance}

{recommendations_guidance}

6. **Overall Grade**: A/B/C/D/F based on:
   - **A**: Excellent - achieved >95% of target QPS, met all SLOs, stable scaling
   - **B**: Good - achieved >80% of target QPS, met most SLOs, minor oscillations
   - **C**: Fair - achieved >60% of target QPS, some SLO misses, scaling issues
   - **D**: Poor - achieved <60% of target QPS, significant SLO misses
   - **F**: Failed - test failed, unable to scale, or unusable performance

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
    latency_variance_text: str,
    # New parameters for scaling and workload config
    scaling_mode: str = "AUTO",
    min_workers: int = 1,
    max_workers: int | None = None,
    min_threads_per_worker: int | None = None,
    max_threads_per_worker: int | None = None,
    cpu_limit_pct: float | None = None,
    mem_limit_pct: float | None = None,
    point_lookup_pct: int = 0,
    range_scan_pct: int = 0,
    insert_pct: int = 0,
    update_pct: int = 0,
    slo_p95_target: float | None = None,
    slo_p99_target: float | None = None,
    slo_max_error_pct: float | None = None,
    think_time_ms: int = 0,
    warmup_seconds: int = 0,
    use_cached_result: bool = True,
    # Multi-cluster and cost parameters
    min_clusters: int = 1,
    max_clusters: int = 1,
    scaling_policy: str = "STANDARD",
    warehouse_credits_used: float | None = None,
    # Step history for FIND_MAX
    step_history_text: str = "",
) -> str:
    """Build prompt for FIND_MAX_CONCURRENCY mode (step-load test)."""
    error_pct = (failed_ops / total_ops * 100) if total_ops > 0 else 0
    
    # Determine if this is a Postgres test
    table_type_u = str(table_type or "").upper().strip()
    is_postgres = table_type_u == "POSTGRES"

    logs_section = ""
    if execution_logs:
        logs_section = f"""
EXECUTION LOGS (step transitions, degradation detection, backoff decisions):
{execution_logs}
"""

    # Build scaling mode description
    scaling_mode_desc = {
        "FIXED": "FIXED (static worker count per step)",
        "AUTO": "AUTO (workers scale automatically within step)",
        "BOUNDED": "BOUNDED (workers scale within min/max limits per step)",
    }.get(scaling_mode, scaling_mode)

    # Build scaling configuration text
    scaling_config_lines = [f"- Scaling Mode: {scaling_mode_desc}"]
    if scaling_mode == "BOUNDED":
        scaling_config_lines.append(f"- Worker Bounds: {min_workers}-{max_workers or 'unlimited'} workers")
        if min_threads_per_worker is not None or max_threads_per_worker is not None:
            scaling_config_lines.append(
                f"- Threads per Worker: {min_threads_per_worker or 1}-{max_threads_per_worker or 'unlimited'}"
            )
    elif scaling_mode == "AUTO":
        scaling_config_lines.append(f"- Min Workers: {min_workers}")
        if max_workers:
            scaling_config_lines.append(f"- Max Workers: {max_workers}")
    
    # Add multi-cluster config for Snowflake
    if not is_postgres and max_clusters > 1:
        scaling_config_lines.append(f"- Multi-Cluster: {min_clusters}-{max_clusters} clusters ({scaling_policy})")
    
    # Add guardrails if configured
    if cpu_limit_pct is not None or mem_limit_pct is not None:
        guardrail_parts = []
        if cpu_limit_pct is not None:
            guardrail_parts.append(f"CPU<{cpu_limit_pct}%")
        if mem_limit_pct is not None:
            guardrail_parts.append(f"Memory<{mem_limit_pct}%")
        scaling_config_lines.append(f"- Resource Guardrails: {', '.join(guardrail_parts)}")
    
    scaling_config_text = "\n".join(scaling_config_lines)

    # Build cost analysis text (Snowflake only)
    cost_text = ""
    if not is_postgres and warehouse_credits_used is not None and warehouse_credits_used > 0:
        cost_per_op = (warehouse_credits_used / total_ops * 1000) if total_ops > 0 else 0
        cost_text = f"""
COST ANALYSIS:
- Warehouse Credits Used: {warehouse_credits_used:.4f}
- Cost per 1000 Operations: {cost_per_op:.6f} credits
- Total Operations: {total_ops:,}"""

    # Build workload mix text
    mix_parts = []
    if point_lookup_pct > 0:
        mix_parts.append(f"Point Lookup: {point_lookup_pct}%")
    if range_scan_pct > 0:
        mix_parts.append(f"Range Scan: {range_scan_pct}%")
    if insert_pct > 0:
        mix_parts.append(f"Insert: {insert_pct}%")
    if update_pct > 0:
        mix_parts.append(f"Update: {update_pct}%")
    workload_mix_text = ", ".join(mix_parts) if mix_parts else "Not specified"

    # Build SLO targets text
    slo_lines = []
    if slo_p95_target is not None and slo_p95_target > 0:
        slo_lines.append(f"P95 ≤ {slo_p95_target}ms")
    if slo_p99_target is not None and slo_p99_target > 0:
        slo_lines.append(f"P99 ≤ {slo_p99_target}ms")
    if slo_max_error_pct is not None and slo_max_error_pct >= 0:
        slo_lines.append(f"Error Rate ≤ {slo_max_error_pct}%")
    slo_text = ", ".join(slo_lines) if slo_lines else "None configured"

    # Extract step history and results from find_max_result (if available)
    # Note: step_history_text parameter may already have data from CONTROLLER_STEP_HISTORY
    db_step_history_text = ""
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
            db_step_history_text = "\nSTEP-BY-STEP PROGRESSION:\n"
            db_step_history_text += (
                "| Step | Workers | QPS | p95 (ms) | Error % | Stable | Reason |\n"
            )
            db_step_history_text += (
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
                db_step_history_text += (
                    f"| {step_num} | {workers} | {qps:.1f} | {step_p95:.1f} "
                    f"| {err_pct:.2f}% | {stable} | {reason} |\n"
                )
    
    # Use passed step_history_text if db doesn't have it
    final_step_history_text = db_step_history_text if db_step_history_text else step_history_text
    
    # Platform-specific intro and guidance
    if is_postgres:
        platform_intro = "You are analyzing a **PostgreSQL** FIND_MAX_CONCURRENCY benchmark test."
        latency_guidance = """3. **Latency Breakdown Analysis**: Interpret the server vs network latency split.
   - What percentage of latency is network overhead vs PostgreSQL server execution?
   - Did server execution time increase as concurrency increased?
   - Is the cache hit ratio maintaining at higher concurrency?
   - At what concurrency did network overhead become the bottleneck?"""
        bottleneck_guidance = """5. **Bottleneck Identification**: What resource was exhausted?
   - Network bandwidth or connection limits?
   - PostgreSQL server capacity (check server execution time)?
   - Connection pool exhaustion?
   - Shared buffer contention (cache hit ratio dropping)?
   - Lock contention at high concurrency?
   - Resource guardrails triggered (CPU/Memory limits)?"""
        recommendations_guidance = """6. **Recommendations**:
   - Optimal operating point (usually 70-80% of max)?
   - Would a larger Postgres instance increase max concurrency?
   - Would improved network latency allow higher throughput?
   - Any connection pooling or batching optimizations?
   - Adjust resource guardrails if they're limiting discovery?"""
        grade_criteria = f"""7. **Overall Grade**: A/B/C/D/F based on:
   - **A**: Excellent - found high max concurrency for {warehouse_size}, graceful degradation, clear optimal point
   - **B**: Good - reasonable max concurrency, mostly graceful degradation, useful results
   - **C**: Fair - lower than expected max, some abrupt degradation, but usable data
   - **D**: Poor - very low max concurrency, sudden failures, limited insights
   - **F**: Failed - test failed early, no useful max concurrency found"""
    else:
        platform_intro = "You are analyzing a Snowflake FIND_MAX_CONCURRENCY benchmark test."
        latency_guidance = """3. **Latency Variance Analysis**: Interpret the spread ratios (P95/P50).
   - Is variance coming from Snowflake execution or app/network layer?
   - If high SF spread: warehouse contention, cold cache, multi-cluster scaling?
   - If high E2E but low SF: network latency, client processing, connection pooling?"""
        bottleneck_guidance = """5. **Bottleneck Identification**: What resource was exhausted?
   - Warehouse compute capacity (queue saturation)?
   - Multi-cluster scaling limits reached?
   - Data access contention?
   - Resource guardrails triggered (CPU/Memory limits)?
   
   **IMPORTANT - Understanding Snowflake Scaling:**
   - "Workers/threads" in this test are CLIENT-SIDE concurrency (app threads sending queries)
   - Snowflake warehouses scale INDEPENDENTLY via compute clusters, not client threads
   - Reducing client threads does NOT free Snowflake resources - it just sends fewer queries
   - To handle more load: scale UP the warehouse (MEDIUM→LARGE) or enable multi-cluster warehouses
   - If cache hit rate is 0% and Result Cache is "Disabled", that's intentional test configuration"""
        recommendations_guidance = """6. **Recommendations**:
   - What is the sustainable operating point found by this test?
   - Would a LARGER warehouse (not fewer threads) increase capacity?
   - Would multi-cluster warehousing help with queue saturation?
   - If cache is disabled, that's intentional - don't suggest enabling it
   - Consider warehouse auto-suspend/resume settings for cost optimization"""
        grade_criteria = f"""7. **Overall Grade**: A/B/C/D/F based on:
   - **A**: Excellent - found high max concurrency for {warehouse_size}, graceful degradation, clear optimal point
   - **B**: Good - reasonable max concurrency, mostly graceful degradation, useful results
   - **C**: Fair - lower than expected max, some abrupt degradation, but usable data
   - **D**: Poor - very low max concurrency, sudden failures, limited insights
   - **F**: Failed - test failed early, no useful max concurrency found"""

    return f"""{platform_intro}

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
- {"Instance" if is_postgres else "Warehouse"}: {warehouse} ({warehouse_size})
- Step Configuration: Start={start_concurrency}, +{increment}/step, {step_duration}s/step, Max={max_concurrency}
- Warmup: {warmup_seconds}s
- Think Time: {think_time_ms}ms between operations
- Result Cache: {"Enabled" if use_cached_result else "Disabled"}

SCALING CONFIGURATION:
{scaling_config_text}

WORKLOAD MIX: {workload_mix_text}

SLO TARGETS: {slo_text}

FIND MAX RESULTS:
- **Best Sustainable Concurrency: {best_concurrency} workers**
- **Best Stable QPS: {best_qps}**
- Baseline p95 Latency: {baseline_p95}ms
- Stop Reason: {final_reason}
- Total Operations: {total_ops} (Reads: {read_ops}, Writes: {write_ops})
- Failed Operations: {failed_ops} ({error_pct:.2f}%)
- Overall Throughput: {ops_per_sec:.1f} ops/sec
- Overall Latency: p50={p50:.1f}ms, p95={p95:.1f}ms, p99={p99:.1f}ms

{latency_variance_text}

{final_step_history_text}

{breakdown_text}

{qps_info}
{wh_text}
{cost_text}
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
   - Did scaling mode ({scaling_mode}) affect the discovery process?

{latency_guidance}

4. **Degradation Point**: What caused the system to stop scaling?
   - Latency spike (compare baseline vs final p95)?
   - Error rate increase?
   {"- Cache hit ratio dropping or server time increasing?" if is_postgres else "- Queue saturation?"}
   - Resource guardrails triggered?
   - Check logs for specific degradation messages

{bottleneck_guidance}

{recommendations_guidance}

{grade_criteria}

Keep analysis concise and actionable. Use bullet points with specific numbers."""


# =============================================================================
# COMPARE CONTEXT ENDPOINT (Section 11.1 of ai-comparison-plan.md)
# =============================================================================


@router.get("/{test_id}/compare-context")
async def get_compare_context(
    test_id: str,
    baseline_count: int = Query(5, ge=1, le=20, description="Number of baseline runs to consider"),
    comparable_limit: int = Query(5, ge=1, le=10, description="Max comparable candidates to return"),
    min_similarity: float = Query(0.55, ge=0.0, le=1.0, description="Minimum similarity score"),
    include_excluded: bool = Query(False, description="Include excluded candidates with reasons"),
) -> dict[str, Any]:
    """
    Get comparison context for a test, including baseline statistics and similar runs.

    Returns data needed for AI analysis comparisons:
    - Rolling median from recent baselines
    - Comparison vs previous run
    - Comparison vs median
    - Trend analysis
    - List of comparable runs with similarity scores

    Only works for parent/rollup tests (not child workers).
    """
    try:
        pool = snowflake_pool.get_default_pool()

        result = await build_compare_context(
            pool=pool,
            test_id=test_id,
            baseline_count=baseline_count,
            comparable_limit=comparable_limit,
            min_similarity=min_similarity,
            include_excluded=include_excluded,
        )

        if result.get("error"):
            raise HTTPException(status_code=404, detail=result["error"])

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error building compare context for test {test_id}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to build comparison context: {str(e)}",
        )


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
                TEST_CONFIG, FIND_MAX_RESULT, WAREHOUSE_CREDITS_USED
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
        warehouse_credits_used = float(row[17] or 0) if row[17] is not None else None

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
        is_postgres = table_type_u == "POSTGRES"
        load_mode = "CONCURRENCY"
        target_qps = None
        min_connections = 1
        start_concurrency = 5
        concurrency_increment = 10
        step_duration_seconds = 30
        latency_stability_pct = 20.0
        max_error_rate_pct = 1.0
        qps_stability_pct = 5.0

        # Scaling configuration defaults
        scaling_mode = "AUTO"
        min_workers = 1
        max_workers: int | None = None
        min_threads_per_worker: int | None = None
        max_threads_per_worker: int | None = None
        cpu_limit_pct: float | None = None
        mem_limit_pct: float | None = None

        # Multi-cluster config defaults (Snowflake only)
        min_clusters = 1
        max_clusters = 1
        scaling_policy = "STANDARD"

        # Postgres-specific config defaults
        use_pgbouncer = False
        postgres_instance_size = ""

        # Workload mix defaults
        point_lookup_pct = 0
        range_scan_pct = 0
        insert_pct = 0
        update_pct = 0

        # SLO targets defaults
        slo_p95_target: float | None = None
        slo_p99_target: float | None = None
        slo_max_error_pct: float | None = None

        # Other config defaults
        think_time_ms = 0
        warmup_seconds = 0
        use_cached_result = True

        # Extract scenario config (test_config contains both scenario and template_config)
        scenario_cfg = (
            test_config.get("scenario")
            if isinstance(test_config, dict)
            else None
        )
        if not isinstance(scenario_cfg, dict):
            scenario_cfg = {}

        if isinstance(test_config, dict):
            # Load mode is in scenario or template_config, not at top level
            load_mode = (
                str(scenario_cfg.get("load_mode", "")).upper()
                or str(template_cfg.get("load_mode", "") if template_cfg else "").upper()
                or "CONCURRENCY"
            )
            target_qps = scenario_cfg.get("target_qps") or test_config.get("target_qps")
            
            # Extract scaling config - try test_config first, then template_config
            scaling_cfg = test_config.get("scaling")
            if not isinstance(scaling_cfg, dict) and isinstance(template_cfg, dict):
                scaling_cfg = template_cfg.get("scaling")
            if not isinstance(scaling_cfg, dict):
                scaling_cfg = {}

            # Extract scaling mode and bounds
            scaling_mode = str(scaling_cfg.get("mode", "AUTO")).strip().upper() or "AUTO"
            min_connections = int(scaling_cfg.get("min_connections", 1) or 1)
            min_workers = int(scaling_cfg.get("min_workers", 1) or 1)
            max_workers = scaling_cfg.get("max_workers")
            if max_workers is not None:
                max_workers = int(max_workers)
            min_threads_per_worker = scaling_cfg.get("min_threads_per_worker")
            if min_threads_per_worker is not None:
                min_threads_per_worker = int(min_threads_per_worker)
            max_threads_per_worker = scaling_cfg.get("max_threads_per_worker")
            if max_threads_per_worker is not None:
                max_threads_per_worker = int(max_threads_per_worker)

            # Extract resource guardrails - try test_config first, then template_config
            guardrails = test_config.get("guardrails")
            if not isinstance(guardrails, dict) and isinstance(template_cfg, dict):
                guardrails = template_cfg.get("guardrails")
            if isinstance(guardrails, dict):
                # Support both naming conventions
                cpu_limit_pct = guardrails.get("max_cpu_percent") or guardrails.get("cpu_limit_pct")
                mem_limit_pct = guardrails.get("max_memory_percent") or guardrails.get("mem_limit_pct")

            # Extract workload mix from template_config
            if isinstance(template_cfg, dict):
                # Try new format first (mix object), then fall back to custom_*_pct fields
                mix = template_cfg.get("mix")
                if isinstance(mix, dict):
                    point_lookup_pct = int(mix.get("point_lookup", 0) or 0)
                    range_scan_pct = int(mix.get("range_scan", 0) or 0)
                    insert_pct = int(mix.get("insert", 0) or 0)
                    update_pct = int(mix.get("update", 0) or 0)
                else:
                    # Fall back to custom_*_pct fields (legacy format)
                    point_lookup_pct = int(template_cfg.get("custom_point_lookup_pct", 0) or 0)
                    range_scan_pct = int(template_cfg.get("custom_range_scan_pct", 0) or 0)
                    insert_pct = int(template_cfg.get("custom_insert_pct", 0) or 0)
                    update_pct = int(template_cfg.get("custom_update_pct", 0) or 0)

                # Extract SLO targets - try new format first, then per-query-type targets
                targets = template_cfg.get("targets")
                if isinstance(targets, dict):
                    slo_p95_target = targets.get("p95_latency_ms")
                    slo_p99_target = targets.get("p99_latency_ms")
                    slo_max_error_pct = targets.get("max_error_pct")
                else:
                    # Fall back to per-query-type targets (use point lookup as primary)
                    p95_pl = template_cfg.get("target_point_lookup_p95_latency_ms")
                    p99_pl = template_cfg.get("target_point_lookup_p99_latency_ms")
                    err_pl = template_cfg.get("target_point_lookup_error_rate_pct")
                    # Only use if they're positive (configured)
                    if p95_pl is not None and float(p95_pl) > 0:
                        slo_p95_target = float(p95_pl)
                    if p99_pl is not None and float(p99_pl) > 0:
                        slo_p99_target = float(p99_pl)
                    if err_pl is not None and float(err_pl) >= 0:
                        slo_max_error_pct = float(err_pl)

                # Other settings
                use_cached_result = bool(template_cfg.get("use_cached_result", True))

                # Extract multi-cluster config (Snowflake only)
                min_clusters = int(template_cfg.get("min_clusters", 1) or 1)
                max_clusters = int(template_cfg.get("max_clusters", 1) or 1)
                scaling_policy = str(template_cfg.get("scaling_policy", "STANDARD") or "STANDARD").upper()

                # Extract Postgres-specific config
                use_pgbouncer = bool(template_cfg.get("use_pgbouncer", False))
                postgres_instance_size = str(template_cfg.get("postgres_instance_size", "") or "")

            # Extract timing config - try scenario first, then test_config
            think_time_ms = int(test_config.get("think_time_ms", 0) or 0)
            warmup_seconds = int(test_config.get("warmup_seconds", 0) or 0)

            # FIND_MAX specific config
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
        pg_server_metrics_text = ""
        pg_mean_exec_time_ms = None
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
                
                # Build Postgres configuration text
                pg_config_lines = ["POSTGRES CONFIGURATION:"]
                if postgres_instance_size:
                    pg_config_lines.append(f"- Instance Size: {postgres_instance_size}")
                pg_config_lines.append(f"- Connection Pooling: {'PgBouncer (external)' if use_pgbouncer else 'Direct connections'}")
                pg_config_lines.append(f"- Max Concurrent Threads: {concurrency}")
                
                # Note: The orchestrator pool is small (1-2) for management queries only
                # Worker threads connect separately via PgBouncer or direct connections
                pg_config_lines.append(f"- Active Connections (current): {ps.get('active_connections', 'N/A')}")
                pg_config_lines.append(f"- Max Connections (server limit): {ps.get('max_connections', 'N/A')}")
                
                pg_text = "\n".join(pg_config_lines)
            
            # Fetch pg_stat_statements enrichment data (server-side metrics)
            pg_enrichment = await _fetch_pg_enrichment(pool=pool, test_id=test_id)
            if pg_enrichment.get("pg_enrichment_available"):
                pge = pg_enrichment.get("pg_enrichment", {})
                pg_mean_exec_time_ms = pge.get("mean_exec_time_ms")
                
                # Format WAL bytes nicely
                wal_bytes = pge.get("wal_bytes")
                if wal_bytes and wal_bytes > 0:
                    if wal_bytes > 1024 * 1024:
                        wal_str = f"{wal_bytes / (1024 * 1024):.2f} MB"
                    elif wal_bytes > 1024:
                        wal_str = f"{wal_bytes / 1024:.2f} KB"
                    else:
                        wal_str = f"{wal_bytes} bytes"
                else:
                    wal_str = "N/A"
                
                # Build breakdown by query kind if available
                by_kind = pge.get("by_kind", {})
                by_kind_text = ""
                if by_kind:
                    by_kind_text = "\n\nSERVER METRICS BY QUERY TYPE:\n"
                    for kind, stats in by_kind.items():
                        if kind == "SYSTEM":
                            continue  # Skip system queries in analysis
                        calls = stats.get("calls", 0)
                        mean_ms = stats.get("mean_exec_time_ms", 0)
                        rows = stats.get("rows_returned", 0)
                        cache_hit = stats.get("cache_hit_ratio", 0)
                        by_kind_text += (
                            f"- {kind}: {calls:,} calls, "
                            f"mean={mean_ms:.3f}ms, "
                            f"rows={rows:,}, "
                            f"cache_hit={cache_hit * 100:.1f}%\n"
                        )
                
                pg_server_metrics_text = (
                    "\nPOSTGRES SERVER METRICS (from pg_stat_statements):\n"
                    f"- Total Server Calls: {pge.get('total_calls', 0):,}\n"
                    f"- Mean Server Execution Time: {_format_stat(pge.get('mean_exec_time_ms'), '.3f')}ms\n"
                    f"- Total Server Execution Time: {_format_stat(pge.get('total_exec_time_ms'), '.2f')}ms\n"
                    f"- Cache Hit Ratio: {_format_stat(pge.get('cache_hit_pct'), '.2f')}%\n"
                    f"- Shared Blocks Hit: {pge.get('shared_blks_hit', 0):,}\n"
                    f"- Shared Blocks Read (disk): {pge.get('shared_blks_read', 0):,}\n"
                    f"- Block Read Time (I/O): {_format_stat(pge.get('blk_read_time_ms'), '.2f')}ms\n"
                    f"- Rows Returned: {pge.get('rows_returned', 0):,}\n"
                    f"- WAL Generated: {wal_str}\n"
                    f"- Query Patterns Captured: {pge.get('query_pattern_count', 0)}"
                    f"{by_kind_text}"
                )
            
            wh_text = pg_text + pg_server_metrics_text
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

        # Fetch SF execution latency for spread analysis (only for Snowflake)
        sf_latency = await _fetch_sf_execution_latency_summary(pool=pool, test_id=test_id)
        sf_p50 = sf_latency.get("sf_p50_latency_ms")
        sf_p95 = sf_latency.get("sf_p95_latency_ms")

        # Compute latency spread ratios (P95/P50) for both views
        e2e_spread = _compute_latency_spread(p50=p50, p95=p95)
        sf_spread = _compute_latency_spread(p50=sf_p50, p95=sf_p95)

        # Build latency variance text for prompt (different for Postgres vs Snowflake)
        if is_postgres and pg_mean_exec_time_ms is not None:
            # Use Postgres-specific latency analysis with server metrics
            latency_variance_text = _build_postgres_latency_analysis_text(
                e2e_p50=p50,
                e2e_p95=p95,
                pg_mean_exec_time_ms=pg_mean_exec_time_ms,
            )
            # Add the Postgres metrics interpretation guide
            latency_variance_text += "\n\n" + _build_postgres_server_metrics_guidance()
        else:
            # Standard Snowflake latency variance analysis
            latency_variance_text = _build_latency_variance_text(
                e2e_p50=p50,
                e2e_p95=p95,
                e2e_spread_ratio=e2e_spread.get("latency_spread_ratio"),
                sf_p50=sf_p50,
                sf_p95=sf_p95,
                sf_spread_ratio=sf_spread.get("latency_spread_ratio"),
            )

        context = req.context if req else None

        # Fetch relevant execution logs for AI analysis
        execution_logs = await _fetch_relevant_logs(
            pool=pool,
            test_id=test_id,
            load_mode=load_mode,
            limit=50,
        )

        # Fetch FIND_MAX step history if applicable
        find_max_history = None
        find_max_history_text = ""
        if load_mode == "FIND_MAX_CONCURRENCY":
            find_max_history = await _fetch_find_max_step_history(pool=pool, test_id=test_id)
            if find_max_history.get("find_max_history_available"):
                # Build step history text for prompt
                steps = find_max_history.get("steps", [])
                best_concurrency = find_max_history.get("best_stable_concurrency")
                best_qps = find_max_history.get("best_stable_qps", 0)
                degradation_points = find_max_history.get("degradation_points", [])
                
                find_max_history_text = "STEP-BY-STEP CONCURRENCY PROGRESSION:\n"
                for step in steps:
                    outcome_marker = "✓" if step["outcome"] == "STABLE" else "✗"
                    find_max_history_text += (
                        f"- Step {step['step']}: {step['concurrency']} threads → "
                        f"{step['qps']:.1f} QPS, P95={step['p95_ms']:.1f}ms "
                        f"[{outcome_marker} {step['outcome']}]"
                    )
                    if step["stop_reason"]:
                        find_max_history_text += f" ({step['stop_reason']})"
                    find_max_history_text += "\n"
                
                find_max_history_text += f"\nBEST STABLE CONCURRENCY: {best_concurrency} threads @ {best_qps:.1f} QPS"
                
                if degradation_points:
                    find_max_history_text += f"\nDEGRADATION DETECTED AT: {', '.join(str(d['concurrency']) for d in degradation_points)} threads"

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
                latency_variance_text=latency_variance_text,
                # Scaling and workload configuration
                scaling_mode=scaling_mode,
                min_workers=min_workers,
                max_workers=max_workers,
                min_threads_per_worker=min_threads_per_worker,
                max_threads_per_worker=max_threads_per_worker,
                cpu_limit_pct=cpu_limit_pct,
                mem_limit_pct=mem_limit_pct,
                point_lookup_pct=point_lookup_pct,
                range_scan_pct=range_scan_pct,
                insert_pct=insert_pct,
                update_pct=update_pct,
                slo_p95_target=slo_p95_target,
                slo_p99_target=slo_p99_target,
                slo_max_error_pct=slo_max_error_pct,
                think_time_ms=think_time_ms,
                warmup_seconds=warmup_seconds,
                use_cached_result=use_cached_result,
                # Multi-cluster and cost parameters
                min_clusters=min_clusters,
                max_clusters=max_clusters,
                scaling_policy=scaling_policy,
                warehouse_credits_used=warehouse_credits_used,
                # Step history for FIND_MAX
                step_history_text=find_max_history_text,
            )
        elif load_mode == "QPS":
            prompt = _build_qps_prompt(
                test_name=test_name,
                test_status=test_status,
                table_type=table_type,
                warehouse=warehouse,
                warehouse_size=warehouse_size,
                target_qps=target_qps or 100,
                min_connections=min_connections,
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
                latency_variance_text=latency_variance_text,
                # Scaling and workload configuration
                scaling_mode=scaling_mode,
                min_workers=min_workers,
                max_workers=max_workers,
                min_threads_per_worker=min_threads_per_worker,
                max_threads_per_worker=max_threads_per_worker,
                cpu_limit_pct=cpu_limit_pct,
                mem_limit_pct=mem_limit_pct,
                point_lookup_pct=point_lookup_pct,
                range_scan_pct=range_scan_pct,
                insert_pct=insert_pct,
                update_pct=update_pct,
                slo_p95_target=slo_p95_target,
                slo_p99_target=slo_p99_target,
                slo_max_error_pct=slo_max_error_pct,
                think_time_ms=think_time_ms,
                warmup_seconds=warmup_seconds,
                use_cached_result=use_cached_result,
                # Multi-cluster and cost parameters
                min_clusters=min_clusters,
                max_clusters=max_clusters,
                scaling_policy=scaling_policy,
                warehouse_credits_used=warehouse_credits_used,
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
                latency_variance_text=latency_variance_text,
                # Scaling and workload configuration
                scaling_mode=scaling_mode,
                min_workers=min_workers,
                max_workers=max_workers,
                min_threads_per_worker=min_threads_per_worker,
                max_threads_per_worker=max_threads_per_worker,
                cpu_limit_pct=cpu_limit_pct,
                mem_limit_pct=mem_limit_pct,
                point_lookup_pct=point_lookup_pct,
                range_scan_pct=range_scan_pct,
                insert_pct=insert_pct,
                update_pct=update_pct,
                slo_p95_target=slo_p95_target,
                slo_p99_target=slo_p99_target,
                slo_max_error_pct=slo_max_error_pct,
                think_time_ms=think_time_ms,
                warmup_seconds=warmup_seconds,
                use_cached_result=use_cached_result,
            )

        # =================================================================
        # PHASE 3: Fetch comparison context and append to prompt
        # =================================================================
        compare_context = None
        comparison_summary = None
        try:
            compare_context = await build_compare_context(
                pool=pool,
                test_id=test_id,
                baseline_count=5,
                comparable_limit=3,
                min_similarity=0.55,
                include_excluded=False,
            )
            
            # Only append comparison section if we have baseline data
            if compare_context and not compare_context.get("error"):
                comparison_prompt = generate_comparison_prompt(compare_context, load_mode)
                if comparison_prompt:
                    prompt = prompt + "\n\n" + comparison_prompt
                
                # Build comparison summary for response
                baseline = compare_context.get("baseline", {})
                vs_median = compare_context.get("vs_median", {})
                vs_previous = compare_context.get("vs_previous", {})
                trend = compare_context.get("trend", {})
                
                comparison_summary = {
                    "baseline_available": baseline.get("available", False),
                    "baseline_count": baseline.get("candidate_count", 0),
                    "verdict": vs_median.get("verdict") if vs_median else None,
                    "verdict_reasons": vs_median.get("verdict_reasons", []) if vs_median else [],
                    "qps_delta_pct": vs_median.get("qps_delta_pct") if vs_median else None,
                    "p95_delta_pct": vs_median.get("p95_delta_pct") if vs_median else None,
                    "trend_direction": trend.get("direction") if trend else None,
                    "vs_previous_test_id": vs_previous.get("test_id") if vs_previous else None,
                    "vs_previous_similarity": vs_previous.get("similarity_score") if vs_previous else None,
                }
        except Exception as compare_err:
            # Don't fail the whole analysis if comparison fails
            logger.warning("Comparison context failed for test %s: %s", test_id, compare_err)
            comparison_summary = {"error": str(compare_err)}

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
            "comparison_summary": comparison_summary,
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


@router.post("/compare/ai-analysis")
async def deep_compare_ai_analysis(req: DeepCompareAiRequest) -> dict[str, Any]:
    """
    Generate AI-powered comparison analysis for two benchmark tests.

    This endpoint compares two specific tests side-by-side, highlighting
    configuration differences and performance deltas, then generates
    AI analysis explaining which test performed better and why.
    """
    try:
        pool = snowflake_pool.get_default_pool()
        prefix = _prefix()

        primary_id = req.primary_id
        secondary_id = req.secondary_id

        # Fetch both tests' data
        test_query = f"""
        SELECT
            TEST_ID, TEST_NAME, TEMPLATE_NAME, TABLE_TYPE,
            WAREHOUSE, WAREHOUSE_SIZE, CONCURRENT_CONNECTIONS,
            DURATION_SECONDS, STATUS, QPS, TOTAL_OPERATIONS,
            FAILED_OPERATIONS, P50_LATENCY_MS, P95_LATENCY_MS,
            P99_LATENCY_MS, READ_OPERATIONS, WRITE_OPERATIONS,
            TEST_CONFIG
        FROM {prefix}.TEST_RESULTS
        WHERE TEST_ID = ?
        """

        test_a_row = await pool.execute_query(test_query, params=[primary_id])
        test_b_row = await pool.execute_query(test_query, params=[secondary_id])

        if not test_a_row:
            raise HTTPException(status_code=404, detail=f"Primary test {primary_id} not found")
        if not test_b_row:
            raise HTTPException(status_code=404, detail=f"Secondary test {secondary_id} not found")

        def _parse_test_row(row: tuple) -> dict[str, Any]:
            """Parse a test row into a dict."""
            test_config = row[17]
            if isinstance(test_config, str):
                test_config = json.loads(test_config)

            template_cfg = (
                test_config.get("template_config")
                if isinstance(test_config, dict)
                else {}
            ) or {}

            load_mode = str(template_cfg.get("load_mode", "CONCURRENCY")).upper()

            return {
                "test_id": row[0],
                "test_name": row[1],
                "template_name": row[2],
                "table_type": row[3],
                "warehouse": row[4],
                "warehouse_size": row[5],
                "concurrency": row[6],
                "duration_seconds": float(row[7] or 0),
                "status": row[8],
                "qps": float(row[9] or 0),
                "total_operations": int(row[10] or 0),
                "failed_operations": int(row[11] or 0),
                "p50_latency_ms": float(row[12] or 0),
                "p95_latency_ms": float(row[13] or 0),
                "p99_latency_ms": float(row[14] or 0),
                "read_operations": int(row[15] or 0),
                "write_operations": int(row[16] or 0),
                "load_mode": load_mode,
                "workload_mix": template_cfg.get("workload_mix"),
                "scaling_mode": (
                    template_cfg.get("scaling", {}).get("mode")
                    if isinstance(template_cfg.get("scaling"), dict)
                    else None
                ),
            }

        test_a = _parse_test_row(test_a_row[0])
        test_b = _parse_test_row(test_b_row[0])

        # Fetch statistics for both tests (for more accurate metrics)
        async def _fetch_test_statistics(test_id: str) -> dict[str, Any] | None:
            """Fetch comprehensive statistics for a test including SF execution data."""
            try:
                stats_query = f"""
                SELECT
                    COUNT(*) AS TOTAL_QUERIES,
                    SUM(CASE WHEN SUCCESS = FALSE THEN 1 ELSE 0 END) AS ERROR_COUNT,
                    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY APP_ELAPSED_MS) AS P50_MS,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY APP_ELAPSED_MS) AS P95_MS,
                    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY APP_ELAPSED_MS) AS P99_MS,
                    -- SF Execution timing
                    AVG(SF_ELAPSED_MS) AS AVG_SF_ELAPSED_MS,
                    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY SF_ELAPSED_MS) AS P95_SF_ELAPSED_MS,
                    -- Queue time statistics
                    AVG(COALESCE(SF_QUEUED_OVERLOAD_MS, 0)) AS AVG_QUEUED_OVERLOAD_MS,
                    AVG(COALESCE(SF_QUEUED_PROVISIONING_MS, 0)) AS AVG_QUEUED_PROVISIONING_MS,
                    SUM(CASE WHEN COALESCE(SF_QUEUED_OVERLOAD_MS, 0) > 0 THEN 1 ELSE 0 END) AS QUERIES_WITH_OVERLOAD_QUEUE,
                    SUM(CASE WHEN COALESCE(SF_QUEUED_PROVISIONING_MS, 0) > 0 THEN 1 ELSE 0 END) AS QUERIES_WITH_PROVISIONING_QUEUE,
                    -- Cache statistics
                    AVG(SF_PCT_SCANNED_FROM_CACHE) AS AVG_CACHE_HIT_PCT,
                    SUM(CASE WHEN COALESCE(SF_PCT_SCANNED_FROM_CACHE, 0) >= 100 THEN 1 ELSE 0 END) AS FULL_CACHE_HIT_QUERIES,
                    -- Warmup queries count (tells us if cache warming was used)
                    (SELECT COUNT(*) FROM {prefix}.QUERY_EXECUTIONS WHERE TEST_ID = qe.TEST_ID AND WARMUP = TRUE) AS WARMUP_QUERY_COUNT
                FROM {prefix}.QUERY_EXECUTIONS qe
                WHERE TEST_ID = ?
                  AND COALESCE(WARMUP, FALSE) = FALSE
                GROUP BY TEST_ID
                """
                rows = await pool.execute_query(stats_query, params=[test_id])
                if not rows or rows[0][0] is None:
                    return None

                r = rows[0]
                total = int(r[0] or 0)
                errors = int(r[1] or 0)
                queries_with_overload = int(r[9] or 0)
                queries_with_provisioning = int(r[10] or 0)
                full_cache_hits = int(r[12] or 0)
                warmup_count = int(r[13] or 0)

                # Calculate test duration for QPS
                duration_query = f"""
                SELECT TIMESTAMPDIFF(SECOND, MIN(START_TIME), MAX(START_TIME))
                FROM {prefix}.QUERY_EXECUTIONS
                WHERE TEST_ID = ?
                  AND COALESCE(WARMUP, FALSE) = FALSE
                """
                dur_rows = await pool.execute_query(duration_query, params=[test_id])
                duration = float(dur_rows[0][0] or 0) if dur_rows and dur_rows[0] else 0

                return {
                    "total_queries": total,
                    "duration_seconds": duration,
                    "errors": {
                        "error_count": errors,
                        "error_rate_pct": (errors / total * 100) if total > 0 else 0,
                    },
                    "latency": {
                        "p50_ms": _to_float_or_none(r[2]),
                        "p95_ms": _to_float_or_none(r[3]),
                        "p99_ms": _to_float_or_none(r[4]),
                    },
                    "sf_execution": {
                        "avg_sf_elapsed_ms": _to_float_or_none(r[5]),
                        "p95_sf_elapsed_ms": _to_float_or_none(r[6]),
                        "available": r[5] is not None,
                    },
                    "queue_times": {
                        "avg_overload_ms": _to_float_or_none(r[7]),
                        "avg_provisioning_ms": _to_float_or_none(r[8]),
                        "queries_with_overload_queue": queries_with_overload,
                        "queries_with_provisioning_queue": queries_with_provisioning,
                        "pct_with_overload": (queries_with_overload / total * 100) if total > 0 else 0,
                        "pct_with_provisioning": (queries_with_provisioning / total * 100) if total > 0 else 0,
                    },
                    "cache": {
                        "avg_cache_hit_pct": _to_float_or_none(r[11]),
                        "full_cache_hit_queries": full_cache_hits,
                        "full_cache_hit_pct": (full_cache_hits / total * 100) if total > 0 else 0,
                    },
                    "warmup": {
                        "warmup_queries_used": warmup_count > 0,
                        "warmup_query_count": warmup_count,
                    },
                    "throughput": {
                        "avg_qps": (total / duration) if duration > 0 else None,
                    },
                }
            except Exception as e:
                logger.warning("Failed to fetch deep compare statistics for %s: %s", test_id, e)
                return None

        stats_a, stats_b = await asyncio.gather(
            _fetch_test_statistics(primary_id),
            _fetch_test_statistics(secondary_id),
        )

        # Generate the comparison prompt
        prompt, deltas = generate_deep_compare_prompt(test_a, test_b, stats_a, stats_b)

        # Calculate similarity score between tests
        similarity = calculate_similarity_score(test_a, test_b)

        # Identify configuration differences
        differences = []
        config_fields = [
            ("warehouse_size", "Warehouse size"),
            ("warehouse", "Warehouse"),
            ("concurrency", "Concurrency"),
            ("duration_seconds", "Duration"),
            ("load_mode", "Load mode"),
            ("table_type", "Table type"),
            ("scaling_mode", "Scaling mode"),
        ]
        for field, label in config_fields:
            val_a = test_a.get(field)
            val_b = test_b.get(field)
            if val_a != val_b and (val_a is not None or val_b is not None):
                differences.append(f"{label}: {val_a} vs {val_b}")

        # Determine verdict based on deltas
        qps_delta = deltas.get("qps_delta_pct")
        p95_delta = deltas.get("p95_delta_pct")
        error_delta = deltas.get("error_rate_delta_pct")

        verdict = "SIMILAR"
        if qps_delta is not None and p95_delta is not None:
            qps_class = classify_change("qps", qps_delta)
            p95_class = classify_change("p95_latency", p95_delta)

            # Determine overall verdict
            if qps_class == "IMPROVEMENT" and p95_class in ("IMPROVEMENT", "NEUTRAL"):
                verdict = "IMPROVED"
            elif qps_class == "REGRESSION" or p95_class == "REGRESSION":
                verdict = "REGRESSED"
            elif qps_class == "IMPROVEMENT" and p95_class == "REGRESSION":
                verdict = "MIXED"
            elif qps_class == "REGRESSION" and p95_class == "IMPROVEMENT":
                verdict = "MIXED"
            else:
                verdict = "SIMILAR"

        # Call AI for analysis
        try:
            ai_resp = await pool.execute_query(
                "SELECT AI_COMPLETE(model => ?, prompt => ?, model_parameters => PARSE_JSON(?)) AS RESP",
                params=[
                    "claude-4-sonnet",
                    prompt,
                    json.dumps({"temperature": 0.3, "max_tokens": 2000}),
                ],
            )
            analysis = (
                ai_resp[0][0] if ai_resp and ai_resp[0] else "Analysis unavailable"
            )
        except Exception as ai_err:
            logger.warning("Deep compare AI analysis failed: %s", ai_err)
            analysis = f"AI analysis unavailable: {ai_err}"

        return {
            "primary_id": primary_id,
            "secondary_id": secondary_id,
            "analysis": analysis,
            "deltas": deltas,
            "verdict": verdict,
            "similarity_score": similarity,
            "differences": differences,
            "load_mode": test_a.get("load_mode", "CONCURRENCY"),
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Error in deep compare AI analysis")
        raise http_exception("deep compare ai analysis", e)


@router.get("/{test_id}/statistics")
async def get_test_statistics(test_id: str) -> dict[str, Any]:
    """
    Get statistical summary for a test including:
    - Latency statistics (avg/min/max/stddev) overall and per query kind
    - Queue time metrics (overload + provisioning)
    - Cache hit rate statistics

    For parent runs (multi-worker), aggregates data from all child runs.
    """
    try:
        pool = snowflake_pool.get_default_pool()
        prefix = _prefix()

        # Check if this is a parent run
        run_id_rows = await pool.execute_query(
            f"SELECT RUN_ID FROM {prefix}.TEST_RESULTS WHERE TEST_ID = ?",
            params=[test_id],
        )
        run_id = run_id_rows[0][0] if run_id_rows and run_id_rows[0] else None
        is_parent = bool(run_id) and str(run_id) == str(test_id)

        # Build the WHERE clause based on parent/child status
        if is_parent:
            # For parent runs, query all test IDs in the run
            test_filter = f"""
            qe.TEST_ID IN (
                SELECT TEST_ID FROM {prefix}.TEST_RESULTS WHERE RUN_ID = ?
            )
            """
            params = [test_id]
        else:
            test_filter = "qe.TEST_ID = ?"
            params = [test_id]

        # Main statistics query
        query = f"""
        SELECT
            -- Overall latency statistics
            COUNT(*) AS TOTAL_QUERIES,
            AVG(qe.APP_ELAPSED_MS) AS AVG_LATENCY_MS,
            MIN(qe.APP_ELAPSED_MS) AS MIN_LATENCY_MS,
            MAX(qe.APP_ELAPSED_MS) AS MAX_LATENCY_MS,
            STDDEV(qe.APP_ELAPSED_MS) AS STDDEV_LATENCY_MS,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY qe.APP_ELAPSED_MS) AS P50_LATENCY_MS,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY qe.APP_ELAPSED_MS) AS P95_LATENCY_MS,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY qe.APP_ELAPSED_MS) AS P99_LATENCY_MS,

            -- Queue time statistics
            AVG(COALESCE(qe.SF_QUEUED_OVERLOAD_MS, 0)) AS AVG_QUEUED_OVERLOAD_MS,
            AVG(COALESCE(qe.SF_QUEUED_PROVISIONING_MS, 0)) AS AVG_QUEUED_PROVISIONING_MS,
            SUM(COALESCE(qe.SF_QUEUED_OVERLOAD_MS, 0)) AS TOTAL_QUEUED_OVERLOAD_MS,
            SUM(COALESCE(qe.SF_QUEUED_PROVISIONING_MS, 0)) AS TOTAL_QUEUED_PROVISIONING_MS,
            SUM(CASE WHEN COALESCE(qe.SF_QUEUED_OVERLOAD_MS, 0) > 0 THEN 1 ELSE 0 END) AS QUERIES_WITH_OVERLOAD_QUEUE,
            SUM(CASE WHEN COALESCE(qe.SF_QUEUED_PROVISIONING_MS, 0) > 0 THEN 1 ELSE 0 END) AS QUERIES_WITH_PROVISIONING_QUEUE,

            -- Cache hit statistics
            AVG(qe.SF_PCT_SCANNED_FROM_CACHE) AS AVG_CACHE_HIT_PCT,
            MIN(qe.SF_PCT_SCANNED_FROM_CACHE) AS MIN_CACHE_HIT_PCT,
            MAX(qe.SF_PCT_SCANNED_FROM_CACHE) AS MAX_CACHE_HIT_PCT,
            SUM(CASE WHEN COALESCE(qe.SF_PCT_SCANNED_FROM_CACHE, 0) >= 100 THEN 1 ELSE 0 END) AS FULL_CACHE_HIT_QUERIES,

            -- Error counts
            SUM(CASE WHEN qe.SUCCESS = FALSE THEN 1 ELSE 0 END) AS ERROR_COUNT
        FROM {prefix}.QUERY_EXECUTIONS qe
        WHERE {test_filter}
          AND COALESCE(qe.WARMUP, FALSE) = FALSE
        """
        rows = await pool.execute_query(query, params=params)

        if not rows or rows[0][0] is None or int(rows[0][0] or 0) == 0:
            return {
                "test_id": test_id,
                "available": False,
                "message": "No query execution data available",
            }

        r = rows[0]
        total_queries = int(r[0] or 0)

        # Per-query-kind statistics
        kind_query = f"""
        SELECT
            qe.QUERY_KIND,
            COUNT(*) AS QUERY_COUNT,
            AVG(qe.APP_ELAPSED_MS) AS AVG_LATENCY_MS,
            MIN(qe.APP_ELAPSED_MS) AS MIN_LATENCY_MS,
            MAX(qe.APP_ELAPSED_MS) AS MAX_LATENCY_MS,
            STDDEV(qe.APP_ELAPSED_MS) AS STDDEV_LATENCY_MS,
            PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY qe.APP_ELAPSED_MS) AS P50_LATENCY_MS,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY qe.APP_ELAPSED_MS) AS P95_LATENCY_MS,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY qe.APP_ELAPSED_MS) AS P99_LATENCY_MS,
            AVG(COALESCE(qe.SF_QUEUED_OVERLOAD_MS, 0) + COALESCE(qe.SF_QUEUED_PROVISIONING_MS, 0)) AS AVG_TOTAL_QUEUE_MS,
            AVG(qe.SF_PCT_SCANNED_FROM_CACHE) AS AVG_CACHE_HIT_PCT,
            SUM(CASE WHEN qe.SUCCESS = FALSE THEN 1 ELSE 0 END) AS ERROR_COUNT
        FROM {prefix}.QUERY_EXECUTIONS qe
        WHERE {test_filter}
          AND COALESCE(qe.WARMUP, FALSE) = FALSE
        GROUP BY qe.QUERY_KIND
        ORDER BY QUERY_COUNT DESC
        """
        kind_rows = await pool.execute_query(kind_query, params=params)

        per_kind: list[dict[str, Any]] = []
        for kr in kind_rows:
            kind_count = int(kr[1] or 0)
            per_kind.append({
                "query_kind": kr[0],
                "query_count": kind_count,
                "avg_latency_ms": _to_float_or_none(kr[2]),
                "min_latency_ms": _to_float_or_none(kr[3]),
                "max_latency_ms": _to_float_or_none(kr[4]),
                "stddev_latency_ms": _to_float_or_none(kr[5]),
                "p50_latency_ms": _to_float_or_none(kr[6]),
                "p95_latency_ms": _to_float_or_none(kr[7]),
                "p99_latency_ms": _to_float_or_none(kr[8]),
                "avg_total_queue_ms": _to_float_or_none(kr[9]),
                "avg_cache_hit_pct": _to_float_or_none(kr[10]),
                "error_count": int(kr[11] or 0),
                "error_rate_pct": (int(kr[11] or 0) / kind_count * 100) if kind_count > 0 else 0.0,
            })

        # Updated indices after adding p50, p95, p99 at positions 5, 6, 7:
        # 0: TOTAL_QUERIES, 1: AVG_LATENCY, 2: MIN_LATENCY, 3: MAX_LATENCY, 4: STDDEV
        # 5: P50, 6: P95, 7: P99
        # 8: AVG_QUEUED_OVERLOAD, 9: AVG_QUEUED_PROVISIONING
        # 10: TOTAL_QUEUED_OVERLOAD, 11: TOTAL_QUEUED_PROVISIONING
        # 12: QUERIES_WITH_OVERLOAD_QUEUE, 13: QUERIES_WITH_PROVISIONING_QUEUE
        # 14: AVG_CACHE_HIT_PCT, 15: MIN_CACHE_HIT_PCT, 16: MAX_CACHE_HIT_PCT
        # 17: FULL_CACHE_HIT_QUERIES, 18: ERROR_COUNT
        error_count = int(r[18] or 0)
        success_count = total_queries - error_count
        success_rate = (success_count / total_queries * 100) if total_queries > 0 else 0.0

        # Get test duration for throughput calculation
        duration_query = f"""
        SELECT
            TIMESTAMPDIFF(SECOND, MIN(qe.START_TIME), MAX(qe.START_TIME)) AS DURATION_SECONDS
        FROM {prefix}.QUERY_EXECUTIONS qe
        WHERE {test_filter}
          AND COALESCE(qe.WARMUP, FALSE) = FALSE
        """
        duration_rows = await pool.execute_query(duration_query, params=params)
        duration_seconds = float(duration_rows[0][0] or 0) if duration_rows and duration_rows[0] else 0
        avg_qps = (total_queries / duration_seconds) if duration_seconds > 0 else None

        return {
            "test_id": test_id,
            "available": True,
            "is_parent_run": is_parent,
            "total_queries": total_queries,
            "success_rate_pct": success_rate,
            "throughput": {
                "avg_qps": avg_qps,
                "duration_seconds": duration_seconds,
            },
            "latency": {
                "avg_ms": _to_float_or_none(r[1]),
                "min_ms": _to_float_or_none(r[2]),
                "max_ms": _to_float_or_none(r[3]),
                "stddev_ms": _to_float_or_none(r[4]),
                "p50_ms": _to_float_or_none(r[5]),
                "p95_ms": _to_float_or_none(r[6]),
                "p99_ms": _to_float_or_none(r[7]),
            },
            "queue_time": {
                "avg_overload_ms": _to_float_or_none(r[8]),
                "avg_provisioning_ms": _to_float_or_none(r[9]),
                "total_overload_ms": _to_float_or_none(r[10]),
                "total_provisioning_ms": _to_float_or_none(r[11]),
                "queries_with_overload_queue": int(r[12] or 0),
                "queries_with_provisioning_queue": int(r[13] or 0),
                "pct_queries_queued": (int(r[12] or 0) / total_queries * 100) if total_queries > 0 else 0.0,
            },
            "cache": {
                "avg_hit_pct": _to_float_or_none(r[14]),
                "min_hit_pct": _to_float_or_none(r[15]),
                "max_hit_pct": _to_float_or_none(r[16]),
                "full_cache_hit_queries": int(r[17] or 0),
                "full_cache_hit_pct": (int(r[17] or 0) / total_queries * 100) if total_queries > 0 else 0.0,
            },
            "errors": {
                "error_count": error_count,
                "error_rate_pct": (error_count / total_queries * 100) if total_queries > 0 else 0.0,
            },
            "per_query_kind": per_kind,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("get test statistics", e)


@router.get("/{test_id}/error-timeline")
async def get_error_timeline(test_id: str) -> dict[str, Any]:
    """
    Get error counts bucketed by time for visualizing error trends.

    Returns error counts per 5-second bucket along with total query counts,
    allowing calculation of error rates over time.

    For parent runs (multi-worker), aggregates data from all child runs.
    
    Uses stored procedure GET_ERROR_TIMELINE for consistency.
    """
    try:
        pool = snowflake_pool.get_default_pool()
        return await _call_sp(pool, "GET_ERROR_TIMELINE", test_id)
    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("get error timeline", e)


@router.get("/{test_id}/latency-breakdown")
async def get_latency_breakdown(test_id: str) -> dict[str, Any]:
    """
    Get detailed latency breakdown for a test including:
    - Read vs Write operations summary (count, ops/s, P50/P95/P99/min/max)
    - Per-query-type latency breakdown (Point Lookup, Range Scan, Insert, Update)
    For parent runs (multi-worker), aggregates data from all child runs.
    
    Uses stored procedure GET_LATENCY_BREAKDOWN for consistency.
    """
    try:
        pool = snowflake_pool.get_default_pool()
        return await _call_sp(pool, "GET_LATENCY_BREAKDOWN", test_id)
    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("get latency breakdown", e)
