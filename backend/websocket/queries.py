"""
Database queries for WebSocket metrics streaming.
"""

import logging
from typing import Any

from backend.config import settings
from backend.connectors import snowflake_pool

logger = logging.getLogger(__name__)


async def get_parent_test_status(test_id: str) -> str | None:
    """Fetch the status of a test from the database."""
    try:
        pool = snowflake_pool.get_default_pool()
        rows = await pool.execute_query(
            f"""
            SELECT STATUS
            FROM {settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}.TEST_RESULTS
            WHERE TEST_ID = ?
            LIMIT 1
            """,
            params=[test_id],
        )
        if rows:
            return str(rows[0][0] or "").upper()
        return None
    except Exception:
        return None


async def fetch_run_status(run_id: str) -> dict[str, Any] | None:
    """Fetch run status with timing and worker information."""
    try:
        pool = snowflake_pool.get_default_pool()
        rows = await pool.execute_query(
            f"""
            SELECT
                rs.STATUS,
                rs.PHASE,
                rs.START_TIME,
                rs.END_TIME,
                rs.WARMUP_END_TIME,
                rs.TOTAL_WORKERS_EXPECTED,
                rs.WORKERS_REGISTERED,
                rs.WORKERS_ACTIVE,
                rs.WORKERS_COMPLETED,
                rs.FIND_MAX_STATE,
                rs.QPS_CONTROLLER_STATE,
                rs.CANCELLATION_REASON,
                CASE
                    WHEN rs.STATUS IN ('COMPLETED', 'FAILED', 'CANCELLED', 'STOPPED') THEN
                        COALESCE(
                            NULLIF(tr.DURATION_SECONDS, 0),
                            TIMESTAMPDIFF(SECOND, rs.START_TIME, rs.END_TIME)
                        )
                    ELSE
                        TIMESTAMPDIFF(
                            SECOND,
                            rs.START_TIME,
                            CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
                        )
                END AS ELAPSED_SECONDS,
                tr.FAILURE_REASON
            FROM {settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}.RUN_STATUS rs
            LEFT JOIN {settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}.TEST_RESULTS tr
                ON tr.TEST_ID = rs.RUN_ID
            WHERE rs.RUN_ID = ?
            LIMIT 1
            """,
            params=[run_id],
        )
        if not rows:
            return None
        (
            status,
            phase,
            start_time,
            end_time,
            warmup_end_time,
            total_workers_expected,
            workers_registered,
            workers_active,
            workers_completed,
            find_max_state,
            qps_controller_state,
            cancellation_reason,
            elapsed_seconds,
            failure_reason,
        ) = rows[0]
        return {
            "status": str(status or "").upper(),
            "phase": str(phase or "").upper(),
            "start_time": start_time,
            "end_time": end_time,
            "warmup_end_time": warmup_end_time,
            "total_workers_expected": total_workers_expected,
            "workers_registered": workers_registered,
            "workers_active": workers_active,
            "workers_completed": workers_completed,
            "find_max_state": find_max_state,
            "qps_controller_state": qps_controller_state,
            "cancellation_reason": str(cancellation_reason)
            if cancellation_reason
            else None,
            "elapsed_seconds": float(elapsed_seconds)
            if elapsed_seconds is not None
            else None,
            "failure_reason": str(failure_reason)
            if failure_reason
            else None,
        }
    except Exception:
        return None


async def fetch_run_test_ids(run_id: str) -> list[str]:
    """Fetch all test IDs associated with a run."""
    try:
        pool = snowflake_pool.get_default_pool()
        rows = await pool.execute_query(
            f"""
            SELECT TEST_ID
            FROM {settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}.TEST_RESULTS
            WHERE RUN_ID = ?
            ORDER BY TEST_ID ASC
            """,
            params=[run_id],
        )
        test_ids = [str(row[0]) for row in rows if row and row[0]]
        if str(run_id) not in test_ids:
            test_ids.insert(0, str(run_id))
        seen: set[str] = set()
        ordered: list[str] = []
        for test_id in test_ids:
            if test_id in seen:
                continue
            seen.add(test_id)
            ordered.append(test_id)
        return ordered
    except Exception:
        return [str(run_id)]


async def fetch_warehouse_context(test_id: str) -> tuple[str | None, str | None]:
    """Fetch warehouse name and table type for a test."""
    try:
        pool = snowflake_pool.get_default_pool()
        rows = await pool.execute_query(
            f"""
            SELECT WAREHOUSE, TABLE_TYPE
            FROM {settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}.TEST_RESULTS
            WHERE TEST_ID = ?
            LIMIT 1
            """,
            params=[test_id],
        )
        if not rows:
            return None, None
        warehouse_raw = rows[0][0] if rows[0] else None
        table_type_raw = rows[0][1] if rows[0] else None
        warehouse = str(warehouse_raw).strip() if warehouse_raw else None
        table_type = str(table_type_raw).strip().lower() if table_type_raw else None
        return warehouse or None, table_type or None
    except Exception:
        return None, None


async def fetch_parent_enrichment_status(run_id: str) -> str | None:
    """Fetch enrichment status for a test run.

    Enrichment is done centrally by the orchestrator and updates ONLY the parent
    row (where TEST_ID = RUN_ID). So we check the parent row first - it's the
    authoritative source.
    """
    try:
        pool = snowflake_pool.get_default_pool()
        # Always check the parent row first - enrichment status is updated here
        parent_rows = await pool.execute_query(
            f"""
            SELECT ENRICHMENT_STATUS
            FROM {settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}.TEST_RESULTS
            WHERE TEST_ID = ?
            """,
            params=[run_id],
        )
        if parent_rows and parent_rows[0] and parent_rows[0][0]:
            parent_status = str(parent_rows[0][0]).strip().upper()
            # If parent has a definitive status, return it
            if parent_status in ("COMPLETED", "FAILED", "SKIPPED"):
                return parent_status
            # If parent is PENDING, also check child rows (for multi-worker tests)
            # in case they have a more specific status
            if parent_status == "PENDING":
                child_rows = await pool.execute_query(
                    f"""
                    SELECT ENRICHMENT_STATUS
                    FROM {settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}.TEST_RESULTS
                    WHERE RUN_ID = ?
                      AND TEST_ID <> ?
                    """,
                    params=[run_id, run_id],
                )
                child_statuses = [
                    str(row[0] or "").strip().upper()
                    for row in child_rows or []
                    if row and row[0]
                ]
                # If any child has a terminal status, use the worst case
                if "FAILED" in child_statuses:
                    return "FAILED"
                # Otherwise return parent's PENDING status
                return "PENDING"
            return parent_status
        return None
    except Exception:
        return None


async def fetch_enrichment_progress(run_id: str) -> dict[str, Any] | None:
    """Fetch enrichment progress details for a test run."""
    try:
        # Import here to avoid circular imports
        from backend.api.routes import test_results
        from backend.core import results_store

        pool = snowflake_pool.get_default_pool()
        prefix = f"{settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}"
        rows = await pool.execute_query(
            f"""
            SELECT RUN_ID, STATUS, ENRICHMENT_STATUS, ENRICHMENT_ERROR
            FROM {prefix}.TEST_RESULTS
            WHERE TEST_ID = ?
            """,
            params=[run_id],
        )
        if not rows:
            return None
        run_id_val, status_db, enrichment_status_db, enrichment_error_db = rows[0]
        test_status = str(status_db or "").upper()
        is_parent_run = bool(run_id_val) and str(run_id_val) == str(run_id)

        if is_parent_run:
            (
                agg_status,
                agg_error,
            ) = await test_results._aggregate_parent_enrichment_status(
                pool=pool, run_id=str(run_id)
            )
            (
                total_queries,
                enriched_queries,
                enrichment_ratio,
            ) = await test_results._aggregate_parent_enrichment_stats(
                pool=pool, run_id=str(run_id)
            )
            enrichment_status = agg_status or str(enrichment_status_db or "").upper()
            enrichment_error = agg_error or enrichment_error_db
        else:
            status_info = await results_store.get_enrichment_status(test_id=run_id)
            if status_info is None:
                return None
            enrichment_status = str(status_info.get("enrichment_status") or "").upper()
            enrichment_error = status_info.get("enrichment_error")
            total_queries = status_info.get("total_queries", 0)
            enriched_queries = status_info.get("enriched_queries", 0)
            enrichment_ratio = status_info.get("enrichment_ratio", 0.0)

        is_complete = enrichment_status in ("COMPLETED", "SKIPPED") or (
            enrichment_ratio >= 0.90 and total_queries > 0
        )
        return {
            "test_id": run_id,
            "test_status": test_status,
            "enrichment_status": enrichment_status or None,
            "enrichment_error": enrichment_error,
            "total_queries": int(total_queries or 0),
            "enriched_queries": int(enriched_queries or 0),
            "enrichment_ratio_pct": round(enrichment_ratio * 100, 1),
            "is_complete": is_complete,
            "can_retry": (test_status == "COMPLETED" and enrichment_status == "FAILED"),
        }
    except Exception:
        return None


async def fetch_logs_since_seq(
    test_id: str, since_seq: int, limit: int = 100
) -> list[dict[str, Any]]:
    """Fetch logs from TEST_LOGS table for a given test since a sequence number."""
    try:
        pool = snowflake_pool.get_default_pool()
        prefix = f"{settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}"
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
          AND SEQ > ?
        ORDER BY SEQ ASC
        LIMIT ?
        """
        rows = await pool.execute_query(query, params=[test_id, since_seq, limit])
        logs = []
        for row in rows:
            log_id, tid, worker_id, seq, ts, level, logger_name, message, exception = (
                row
            )
            logs.append(
                {
                    "kind": "log",
                    "log_id": str(log_id) if log_id else None,
                    "test_id": str(tid) if tid else test_id,
                    "worker_id": str(worker_id) if worker_id else None,
                    "seq": int(seq) if seq is not None else 0,
                    "timestamp": ts.isoformat()
                    if hasattr(ts, "isoformat")
                    else str(ts)
                    if ts
                    else None,
                    "level": str(level) if level else "INFO",
                    "logger": str(logger_name) if logger_name else "",
                    "message": str(message) if message else "",
                    "exception": str(exception) if exception else None,
                }
            )
        return logs
    except Exception as e:
        logger.debug(f"Failed to fetch logs for test {test_id}: {e}")
        return []


async def fetch_logs_for_tests(
    test_ids: list[str], last_seq_by_test: dict[str, int]
) -> list[dict[str, Any]]:
    """Fetch logs for multiple tests since their last sequence numbers."""
    if not test_ids:
        return []
    try:
        pool = snowflake_pool.get_default_pool()
        prefix = f"{settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}"
        clauses: list[str] = []
        params: list[Any] = []
        for test_id in test_ids:
            since_seq = int(last_seq_by_test.get(test_id, 0))
            clauses.append("(TEST_ID = ? AND SEQ > ?)")
            params.extend([test_id, since_seq])
        if not clauses:
            return []
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
        WHERE {" OR ".join(clauses)}
        ORDER BY TIMESTAMP ASC, SEQ ASC
        """
        rows = await pool.execute_query(query, params=params)
        logs = []
        for row in rows:
            log_id, tid, worker_id, seq, ts, level, logger_name, message, exception = (
                row
            )
            test_key = str(tid) if tid else None
            if test_key:
                last_seq_by_test[test_key] = max(
                    last_seq_by_test.get(test_key, 0), int(seq or 0)
                )
            logs.append(
                {
                    "kind": "log",
                    "log_id": str(log_id) if log_id else None,
                    "test_id": str(tid) if tid else None,
                    "worker_id": str(worker_id) if worker_id else None,
                    "seq": int(seq) if seq is not None else 0,
                    "timestamp": ts.isoformat()
                    if hasattr(ts, "isoformat")
                    else str(ts)
                    if ts
                    else None,
                    "level": str(level) if level else "INFO",
                    "logger": str(logger_name) if logger_name else "",
                    "message": str(message) if message else "",
                    "exception": str(exception) if exception else None,
                }
            )
        return logs
    except Exception as e:
        logger.debug(f"Failed to fetch logs for tests {test_ids}: {e}")
        return []
