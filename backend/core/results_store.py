"""
Snowflake Results Store

Persists test runs, test results, and time-series metrics snapshots into
UNISTORE_BENCHMARK.TEST_RESULTS.* tables.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Sequence
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


async def insert_warehouse_poll_snapshot(
    *,
    run_id: str,
    warehouse_name: str,
    elapsed_seconds: float | None,
    row: Sequence[Any],
) -> None:
    pool = snowflake_pool.get_default_pool()
    snapshot_id = str(uuid4())
    timestamp = datetime.now(UTC).isoformat()

    def _to_int(value: Any, default: int = 0) -> int:
        try:
            return int(value) if value is not None else default
        except Exception:
            return default

    def _row_to_json(values: Sequence[Any]) -> str:
        try:
            return json.dumps(values, default=str)
        except Exception:
            return json.dumps([str(v) for v in values])

    min_cluster_count = _to_int(row[4] if len(row) > 4 else None, default=1)
    max_cluster_count = _to_int(row[5] if len(row) > 5 else None, default=1)
    started_clusters = _to_int(row[6] if len(row) > 6 else None)
    running = _to_int(row[7] if len(row) > 7 else None)
    queued = _to_int(row[8] if len(row) > 8 else None)
    scaling_policy = row[31] if len(row) > 31 and row[31] else "STANDARD"

    query = f"""
    INSERT INTO {_results_prefix()}.WAREHOUSE_POLL_SNAPSHOTS (
        SNAPSHOT_ID,
        RUN_ID,
        TIMESTAMP,
        ELAPSED_SECONDS,
        WAREHOUSE_NAME,
        STARTED_CLUSTERS,
        RUNNING,
        QUEUED,
        MIN_CLUSTER_COUNT,
        MAX_CLUSTER_COUNT,
        SCALING_POLICY,
        RAW_RESULT
    )
    SELECT
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, PARSE_JSON(?)
    """

    params = [
        snapshot_id,
        run_id,
        timestamp,
        float(elapsed_seconds) if elapsed_seconds is not None else None,
        str(warehouse_name),
        started_clusters,
        running,
        queued,
        min_cluster_count,
        max_cluster_count,
        str(scaling_policy),
        _row_to_json(row),
    ]

    await pool.execute_query(query, params=params)


async def fetch_latest_warehouse_poll_snapshot(*, run_id: str) -> dict[str, Any] | None:
    pool = snowflake_pool.get_default_pool()
    prefix = _results_prefix()
    rows = await pool.execute_query(
        f"""
        SELECT
            TIMESTAMP,
            ELAPSED_SECONDS,
            WAREHOUSE_NAME,
            STARTED_CLUSTERS,
            RUNNING,
            QUEUED,
            MIN_CLUSTER_COUNT,
            MAX_CLUSTER_COUNT,
            SCALING_POLICY
        FROM {prefix}.WAREHOUSE_POLL_SNAPSHOTS
        WHERE RUN_ID = ?
        ORDER BY TIMESTAMP DESC
        LIMIT 1
        """,
        params=[run_id],
    )
    if not rows:
        return None

    (
        timestamp,
        elapsed_seconds,
        warehouse,
        started_clusters,
        running,
        queued,
        min_cluster_count,
        max_cluster_count,
        scaling_policy,
    ) = rows[0]
    return {
        "warehouse": str(warehouse or ""),
        "timestamp": timestamp.isoformat()
        if hasattr(timestamp, "isoformat")
        else str(timestamp),
        "elapsed_seconds": float(elapsed_seconds or 0.0),
        "started_clusters": int(started_clusters or 0),
        "running": int(running or 0),
        "queued": int(queued or 0),
        "min_cluster_count": int(min_cluster_count or 0),
        "max_cluster_count": int(max_cluster_count or 0),
        "scaling_policy": str(scaling_policy or "STANDARD"),
    }


async def insert_test_start(
    *,
    test_id: str,
    run_id: str | None,
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
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, PARSE_JSON(?), PARSE_JSON(?), ?
    """

    params = [
        test_id,
        run_id,
        test_name,
        scenario.name,
        table_name,
        table_type,
        warehouse,
        warehouse_size,
        "RUNNING",
        now,
        scenario.total_threads,
        json.dumps(payload),
        json.dumps(warehouse_config_snapshot) if warehouse_config_snapshot else None,
        query_tag,
    ]

    await pool.execute_query(query, params=params)


async def insert_test_prepare(
    *,
    test_id: str,
    run_id: str | None,
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
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, PARSE_JSON(?), PARSE_JSON(?), ?
    """

    params = [
        test_id,
        run_id,
        test_name,
        scenario.name,
        table_name,
        table_type,
        warehouse,
        warehouse_size,
        "PREPARED",
        now,
        scenario.total_threads,
        json.dumps(payload),
        json.dumps(warehouse_config_snapshot) if warehouse_config_snapshot else None,
        query_tag,
    ]

    await pool.execute_query(query, params=params)


async def upsert_worker_heartbeat(
    *,
    run_id: str,
    worker_id: str,
    worker_group_id: int,
    status: str,
    phase: str | None,
    active_connections: int,
    target_connections: int,
    cpu_percent: float | None,
    memory_percent: float | None,
    queries_processed: int,
    error_count: int,
    last_error: str | None,
) -> None:
    pool = snowflake_pool.get_default_pool()
    now = datetime.now(UTC).isoformat()

    query = f"""
    MERGE INTO {_results_prefix()}.WORKER_HEARTBEATS AS target
    USING (
        SELECT
            ? AS RUN_ID,
            ? AS WORKER_ID,
            ? AS WORKER_GROUP_ID,
            ? AS STATUS,
            ? AS PHASE,
            ? AS LAST_HEARTBEAT,
            ? AS ACTIVE_CONNECTIONS,
            ? AS TARGET_CONNECTIONS,
            ? AS CPU_PERCENT,
            ? AS MEMORY_PERCENT,
            ? AS QUERIES_PROCESSED,
            ? AS ERROR_COUNT,
            ? AS LAST_ERROR
    ) AS src
    ON target.RUN_ID = src.RUN_ID
       AND target.WORKER_ID = src.WORKER_ID
    WHEN MATCHED THEN UPDATE SET
        WORKER_GROUP_ID = src.WORKER_GROUP_ID,
        STATUS = src.STATUS,
        PHASE = src.PHASE,
        LAST_HEARTBEAT = src.LAST_HEARTBEAT,
        HEARTBEAT_COUNT = COALESCE(target.HEARTBEAT_COUNT, 0) + 1,
        ACTIVE_CONNECTIONS = src.ACTIVE_CONNECTIONS,
        TARGET_CONNECTIONS = src.TARGET_CONNECTIONS,
        CPU_PERCENT = src.CPU_PERCENT,
        MEMORY_PERCENT = src.MEMORY_PERCENT,
        QUERIES_PROCESSED = src.QUERIES_PROCESSED,
        ERROR_COUNT = src.ERROR_COUNT,
        LAST_ERROR = src.LAST_ERROR,
        UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        RUN_ID,
        WORKER_ID,
        WORKER_GROUP_ID,
        STATUS,
        PHASE,
        LAST_HEARTBEAT,
        HEARTBEAT_COUNT,
        ACTIVE_CONNECTIONS,
        TARGET_CONNECTIONS,
        CPU_PERCENT,
        MEMORY_PERCENT,
        QUERIES_PROCESSED,
        ERROR_COUNT,
        LAST_ERROR
    )
    VALUES (
        src.RUN_ID,
        src.WORKER_ID,
        src.WORKER_GROUP_ID,
        src.STATUS,
        src.PHASE,
        src.LAST_HEARTBEAT,
        1,
        src.ACTIVE_CONNECTIONS,
        src.TARGET_CONNECTIONS,
        src.CPU_PERCENT,
        src.MEMORY_PERCENT,
        src.QUERIES_PROCESSED,
        src.ERROR_COUNT,
        src.LAST_ERROR
    )
    """

    params = [
        run_id,
        worker_id,
        int(worker_group_id),
        str(status).upper(),
        str(phase).upper() if phase else None,
        now,
        int(active_connections or 0),
        int(target_connections or 0),
        float(cpu_percent) if cpu_percent is not None else None,
        float(memory_percent) if memory_percent is not None else None,
        int(queries_processed or 0),
        int(error_count or 0),
        last_error,
    ]

    await pool.execute_query(query, params=params)


async def update_parent_run_aggregate(*, parent_run_id: str) -> None:
    pool = snowflake_pool.get_default_pool()
    prefix = _results_prefix()

    summary_rows = await pool.execute_query(
        f"""
        SELECT
            MIN(TEST_NAME),
            MIN(SCENARIO_NAME),
            MIN(TABLE_NAME),
            MIN(TABLE_TYPE),
            MIN(WAREHOUSE),
            MIN(WAREHOUSE_SIZE),
            MIN(START_TIME),
            MAX(END_TIME),
            TIMESTAMPDIFF(SECOND, MIN(START_TIME), MAX(END_TIME)) AS DURATION_SECONDS,
            SUM(COALESCE(CONCURRENT_CONNECTIONS, 0)) AS TOTAL_CONCURRENCY,
            SUM(COALESCE(READ_OPERATIONS, 0)) AS READ_OPERATIONS,
            SUM(COALESCE(WRITE_OPERATIONS, 0)) AS WRITE_OPERATIONS,
            SUM(COALESCE(FAILED_OPERATIONS, 0)) AS FAILED_OPERATIONS,
            SUM(COALESCE(TOTAL_OPERATIONS, 0)) AS TOTAL_OPERATIONS,
            SUM(
                CASE
                    WHEN UPPER(STATUS) IN ('FAILED', 'ERROR', 'CANCELLED', 'STOPPED')
                    THEN 1
                    ELSE 0
                END
            ) AS FAILED_WORKERS,
            SUM(
                CASE
                    WHEN UPPER(STATUS) IN ('RUNNING', 'PROCESSING', 'PREPARING')
                    THEN 1
                    ELSE 0
                END
            ) AS RUNNING_WORKERS,
            COUNT(*) AS WORKER_COUNT
        FROM {prefix}.TEST_RESULTS
        WHERE RUN_ID = ?
          AND TEST_ID <> ?
        """,
        params=[parent_run_id, parent_run_id],
    )
    if not summary_rows:
        return

    (
        test_name,
        scenario_name,
        table_name,
        table_type,
        warehouse,
        warehouse_size,
        start_time,
        end_time,
        duration_seconds_db,
        total_concurrency,
        read_operations,
        write_operations,
        failed_operations,
        total_operations,
        failed_workers,
        running_workers,
        worker_count,
    ) = summary_rows[0]
    if not worker_count:
        return

    status = "COMPLETED"
    if failed_workers and failed_workers > 0:
        status = "FAILED"
    elif running_workers and running_workers > 0:
        status = "RUNNING"

    metrics_rows = await pool.execute_query(
        f"""
        WITH latest AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY WORKER_ID
                    ORDER BY TIMESTAMP DESC
                ) AS RN
            FROM {prefix}.WORKER_METRICS_SNAPSHOTS
            WHERE RUN_ID = ?
        )
        SELECT
            SUM(TOTAL_QUERIES) AS TOTAL_QUERIES,
            SUM(READ_COUNT) AS READ_COUNT,
            SUM(WRITE_COUNT) AS WRITE_COUNT,
            SUM(ERROR_COUNT) AS ERROR_COUNT,
            SUM(QPS) AS QPS,
            MAX(ELAPSED_SECONDS) AS ELAPSED_SECONDS,
            AVG(IFF(TOTAL_QUERIES > 0, P50_LATENCY_MS, NULL)) AS P50_LATENCY_MS,
            MAX(IFF(TOTAL_QUERIES > 0, P95_LATENCY_MS, NULL)) AS P95_LATENCY_MS,
            MAX(IFF(TOTAL_QUERIES > 0, P99_LATENCY_MS, NULL)) AS P99_LATENCY_MS,
            CASE
                WHEN SUM(TOTAL_QUERIES) > 0
                THEN SUM(AVG_LATENCY_MS * TOTAL_QUERIES) / SUM(TOTAL_QUERIES)
                ELSE 0
            END AS AVG_LATENCY_MS
        FROM latest
        WHERE RN = 1
        """,
        params=[parent_run_id],
    )

    (
        worker_total_queries,
        worker_read_count,
        worker_write_count,
        worker_error_count,
        worker_qps,
        worker_elapsed,
        worker_p50,
        worker_p95,
        worker_p99,
        worker_avg,
    ) = metrics_rows[0] if metrics_rows else (None,) * 10

    # Use duration from TIMESTAMPDIFF calculated in Snowflake to avoid timezone issues.
    # Snowflake returns naive datetimes in session timezone (often Pacific), so
    # Python-based (end_time - start_time).total_seconds() causes ~8 hour discrepancy.
    if duration_seconds_db is not None and float(duration_seconds_db) >= 0:
        duration_seconds = float(duration_seconds_db)
    else:
        # Fallback to worker elapsed if database duration not available
        duration_seconds = float(worker_elapsed or 0.0)
    total_operations = (
        int(worker_total_queries)
        if worker_total_queries is not None
        else total_operations
    )
    read_operations = (
        int(worker_read_count) if worker_read_count is not None else read_operations
    )
    write_operations = (
        int(worker_write_count) if worker_write_count is not None else write_operations
    )
    error_count = int(worker_error_count or 0)
    qps = float(worker_qps or 0.0)
    reads_per_second = (
        float(read_operations) / duration_seconds if duration_seconds > 0 else 0.0
    )
    writes_per_second = (
        float(write_operations) / duration_seconds if duration_seconds > 0 else 0.0
    )
    error_rate = (
        float(error_count) / float(total_operations) if total_operations else 0.0
    )

    custom_metrics = {
        "multi_node": {
            "parent_run_id": parent_run_id,
            "worker_count": int(worker_count or 0),
        }
    }

    # Fetch FIND_MAX_RESULT from the first child test (all workers run same step progression)
    find_max_rows = await pool.execute_query(
        f"""
        SELECT FIND_MAX_RESULT
        FROM {prefix}.TEST_RESULTS
        WHERE RUN_ID = ?
          AND TEST_ID <> ?
          AND FIND_MAX_RESULT IS NOT NULL
        ORDER BY START_TIME ASC
        LIMIT 1
        """,
        params=[parent_run_id, parent_run_id],
    )
    find_max_result_json = (
        find_max_rows[0][0] if find_max_rows and find_max_rows[0][0] else None
    )

    # Aggregate detailed breakdown latency stats from worker TEST_RESULTS rows.
    # Use AVG for p50, MAX for p95/p99/max, MIN for min across workers.
    breakdown_rows = await pool.execute_query(
        f"""
        SELECT
            AVG(READ_P50_LATENCY_MS) AS READ_P50,
            MAX(READ_P95_LATENCY_MS) AS READ_P95,
            MAX(READ_P99_LATENCY_MS) AS READ_P99,
            MIN(READ_MIN_LATENCY_MS) AS READ_MIN,
            MAX(READ_MAX_LATENCY_MS) AS READ_MAX,
            AVG(WRITE_P50_LATENCY_MS) AS WRITE_P50,
            MAX(WRITE_P95_LATENCY_MS) AS WRITE_P95,
            MAX(WRITE_P99_LATENCY_MS) AS WRITE_P99,
            MIN(WRITE_MIN_LATENCY_MS) AS WRITE_MIN,
            MAX(WRITE_MAX_LATENCY_MS) AS WRITE_MAX,
            AVG(POINT_LOOKUP_P50_LATENCY_MS) AS POINT_LOOKUP_P50,
            MAX(POINT_LOOKUP_P95_LATENCY_MS) AS POINT_LOOKUP_P95,
            MAX(POINT_LOOKUP_P99_LATENCY_MS) AS POINT_LOOKUP_P99,
            MIN(POINT_LOOKUP_MIN_LATENCY_MS) AS POINT_LOOKUP_MIN,
            MAX(POINT_LOOKUP_MAX_LATENCY_MS) AS POINT_LOOKUP_MAX,
            AVG(RANGE_SCAN_P50_LATENCY_MS) AS RANGE_SCAN_P50,
            MAX(RANGE_SCAN_P95_LATENCY_MS) AS RANGE_SCAN_P95,
            MAX(RANGE_SCAN_P99_LATENCY_MS) AS RANGE_SCAN_P99,
            MIN(RANGE_SCAN_MIN_LATENCY_MS) AS RANGE_SCAN_MIN,
            MAX(RANGE_SCAN_MAX_LATENCY_MS) AS RANGE_SCAN_MAX,
            AVG(INSERT_P50_LATENCY_MS) AS INSERT_P50,
            MAX(INSERT_P95_LATENCY_MS) AS INSERT_P95,
            MAX(INSERT_P99_LATENCY_MS) AS INSERT_P99,
            MIN(INSERT_MIN_LATENCY_MS) AS INSERT_MIN,
            MAX(INSERT_MAX_LATENCY_MS) AS INSERT_MAX,
            AVG(UPDATE_P50_LATENCY_MS) AS UPDATE_P50,
            MAX(UPDATE_P95_LATENCY_MS) AS UPDATE_P95,
            MAX(UPDATE_P99_LATENCY_MS) AS UPDATE_P99,
            MIN(UPDATE_MIN_LATENCY_MS) AS UPDATE_MIN,
            MAX(UPDATE_MAX_LATENCY_MS) AS UPDATE_MAX,
            AVG(APP_OVERHEAD_P50_MS) AS APP_OVERHEAD_P50,
            MAX(APP_OVERHEAD_P95_MS) AS APP_OVERHEAD_P95,
            MAX(APP_OVERHEAD_P99_MS) AS APP_OVERHEAD_P99
        FROM {prefix}.TEST_RESULTS
        WHERE RUN_ID = ?
          AND TEST_ID <> ?
        """,
        params=[parent_run_id, parent_run_id],
    )
    breakdown = breakdown_rows[0] if breakdown_rows else (None,) * 33
    (
        read_p50,
        read_p95,
        read_p99,
        read_min,
        read_max,
        write_p50,
        write_p95,
        write_p99,
        write_min,
        write_max,
        point_lookup_p50,
        point_lookup_p95,
        point_lookup_p99,
        point_lookup_min,
        point_lookup_max,
        range_scan_p50,
        range_scan_p95,
        range_scan_p99,
        range_scan_min,
        range_scan_max,
        insert_p50,
        insert_p95,
        insert_p99,
        insert_min,
        insert_max,
        update_p50,
        update_p95,
        update_p99,
        update_min,
        update_max,
        app_overhead_p50,
        app_overhead_p95,
        app_overhead_p99,
    ) = breakdown

    merge_query = f"""
    MERGE INTO {prefix}.TEST_RESULTS AS t
    USING (SELECT ? AS TEST_ID) AS s
    ON t.TEST_ID = s.TEST_ID
    WHEN MATCHED THEN UPDATE SET
        RUN_ID = ?,
        TEST_NAME = ?,
        SCENARIO_NAME = ?,
        TABLE_NAME = ?,
        TABLE_TYPE = ?,
        WAREHOUSE = ?,
        WAREHOUSE_SIZE = ?,
        STATUS = ?,
        START_TIME = ?,
        END_TIME = ?,
        DURATION_SECONDS = ?,
        CONCURRENT_CONNECTIONS = ?,
        TOTAL_OPERATIONS = ?,
        READ_OPERATIONS = ?,
        WRITE_OPERATIONS = ?,
        FAILED_OPERATIONS = ?,
        QPS = ?,
        READS_PER_SECOND = ?,
        WRITES_PER_SECOND = ?,
        AVG_LATENCY_MS = ?,
        P50_LATENCY_MS = ?,
        P95_LATENCY_MS = ?,
        P99_LATENCY_MS = ?,
        ERROR_COUNT = ?,
        ERROR_RATE = ?,
        CUSTOM_METRICS = PARSE_JSON(?),
        FIND_MAX_RESULT = PARSE_JSON(?),
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
        UPDATED_AT = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
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
        TOTAL_OPERATIONS,
        READ_OPERATIONS,
        WRITE_OPERATIONS,
        FAILED_OPERATIONS,
        QPS,
        READS_PER_SECOND,
        WRITES_PER_SECOND,
        AVG_LATENCY_MS,
        P50_LATENCY_MS,
        P95_LATENCY_MS,
        P99_LATENCY_MS,
        ERROR_COUNT,
        ERROR_RATE,
        CUSTOM_METRICS,
        FIND_MAX_RESULT,
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
        APP_OVERHEAD_P50_MS,
        APP_OVERHEAD_P95_MS,
        APP_OVERHEAD_P99_MS
    )
    VALUES (
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
        PARSE_JSON(?), PARSE_JSON(?),
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
    )
    """

    def _float_or_none(v: float | None) -> float | None:
        return float(v) if v is not None else None

    breakdown_params = [
        _float_or_none(read_p50),
        _float_or_none(read_p95),
        _float_or_none(read_p99),
        _float_or_none(read_min),
        _float_or_none(read_max),
        _float_or_none(write_p50),
        _float_or_none(write_p95),
        _float_or_none(write_p99),
        _float_or_none(write_min),
        _float_or_none(write_max),
        _float_or_none(point_lookup_p50),
        _float_or_none(point_lookup_p95),
        _float_or_none(point_lookup_p99),
        _float_or_none(point_lookup_min),
        _float_or_none(point_lookup_max),
        _float_or_none(range_scan_p50),
        _float_or_none(range_scan_p95),
        _float_or_none(range_scan_p99),
        _float_or_none(range_scan_min),
        _float_or_none(range_scan_max),
        _float_or_none(insert_p50),
        _float_or_none(insert_p95),
        _float_or_none(insert_p99),
        _float_or_none(insert_min),
        _float_or_none(insert_max),
        _float_or_none(update_p50),
        _float_or_none(update_p95),
        _float_or_none(update_p99),
        _float_or_none(update_min),
        _float_or_none(update_max),
        _float_or_none(app_overhead_p50),
        _float_or_none(app_overhead_p95),
        _float_or_none(app_overhead_p99),
    ]

    params = [
        parent_run_id,
        parent_run_id,
        test_name,
        scenario_name,
        table_name,
        table_type,
        warehouse,
        warehouse_size,
        status,
        start_time,
        end_time,
        duration_seconds,
        int(total_concurrency or 0),
        int(total_operations or 0),
        int(read_operations or 0),
        int(write_operations or 0),
        int(failed_operations or 0),
        float(qps or 0.0),
        float(reads_per_second or 0.0),
        float(writes_per_second or 0.0),
        float(worker_avg or 0.0),
        float(worker_p50 or 0.0),
        float(worker_p95 or 0.0),
        float(worker_p99 or 0.0),
        int(error_count or 0),
        float(error_rate or 0.0),
        json.dumps(custom_metrics),
        find_max_result_json,
        *breakdown_params,
        parent_run_id,
        parent_run_id,
        test_name,
        scenario_name,
        table_name,
        table_type,
        warehouse,
        warehouse_size,
        status,
        start_time,
        end_time,
        duration_seconds,
        int(total_concurrency or 0),
        int(total_operations or 0),
        int(read_operations or 0),
        int(write_operations or 0),
        int(failed_operations or 0),
        float(qps or 0.0),
        float(reads_per_second or 0.0),
        float(writes_per_second or 0.0),
        float(worker_avg or 0.0),
        float(worker_p50 or 0.0),
        float(worker_p95 or 0.0),
        float(worker_p99 or 0.0),
        int(error_count or 0),
        float(error_rate or 0.0),
        json.dumps(custom_metrics),
        find_max_result_json,
        *breakdown_params,
    ]

    await pool.execute_query(merge_query, params=params)


async def insert_worker_metrics_snapshot(
    *,
    run_id: str,
    test_id: str,
    worker_id: str,
    worker_group_id: int,
    worker_group_count: int,
    metrics: Metrics,
    phase: str | None,
    target_connections: int,
) -> None:
    pool = snowflake_pool.get_default_pool()
    snapshot_id = str(uuid4())

    snapshot_query = f"""
    INSERT INTO {_results_prefix()}.WORKER_METRICS_SNAPSHOTS (
        SNAPSHOT_ID,
        RUN_ID,
        TEST_ID,
        WORKER_ID,
        WORKER_GROUP_ID,
        WORKER_GROUP_COUNT,
        TIMESTAMP,
        ELAPSED_SECONDS,
        PHASE,
        TOTAL_QUERIES,
        READ_COUNT,
        WRITE_COUNT,
        ERROR_COUNT,
        QPS,
        P50_LATENCY_MS,
        P95_LATENCY_MS,
        P99_LATENCY_MS,
        AVG_LATENCY_MS,
        MIN_LATENCY_MS,
        MAX_LATENCY_MS,
        ACTIVE_CONNECTIONS,
        TARGET_CONNECTIONS,
        CPU_PERCENT,
        MEMORY_PERCENT,
        CUSTOM_METRICS
    )
    SELECT
        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, PARSE_JSON(?)
    """

    params = [
        snapshot_id,
        run_id,
        test_id,
        worker_id,
        int(worker_group_id),
        int(worker_group_count),
        metrics.timestamp.isoformat(),
        float(metrics.elapsed_seconds),
        str(phase).upper() if phase else None,
        int(metrics.total_operations),
        int(metrics.read_metrics.count),
        int(metrics.write_metrics.count),
        int(metrics.failed_operations),
        float(metrics.current_qps),
        float(metrics.overall_latency.p50),
        float(metrics.overall_latency.p95),
        float(metrics.overall_latency.p99),
        float(metrics.overall_latency.avg),
        float(metrics.overall_latency.min),
        float(metrics.overall_latency.max),
        int(metrics.active_connections),
        int(target_connections or 0),
        float(metrics.cpu_percent) if metrics.cpu_percent is not None else None,
        None,
        json.dumps(metrics.custom_metrics or {}),
    ]

    # Prefer percent values from custom metrics if available.
    resources = (metrics.custom_metrics or {}).get("resources")
    if isinstance(resources, dict):
        mem_pct = resources.get("cgroup_memory_percent") or resources.get(
            "host_memory_percent"
        )
        if mem_pct is not None:
            params[23] = float(mem_pct)

        cpu_pct = resources.get("cgroup_cpu_percent") or resources.get(
            "host_cpu_percent"
        )
        if cpu_pct is not None:
            params[22] = float(cpu_pct)

    await pool.execute_query(snapshot_query, params=params)


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
    - log_id, test_id, seq, timestamp, level, logger, message, exception, worker_id
    """
    if not rows:
        return

    pool = snowflake_pool.get_default_pool()
    cols = [
        "LOG_ID",
        "TEST_ID",
        "WORKER_ID",
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
            r.get("worker_id"),
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
    pool: Any, prefix: str, test_id: str | None = None, run_id: str | None = None
) -> tuple[datetime, datetime] | None:
    """Get start/end timestamps for a test from QUERY_EXECUTIONS or TEST_RESULTS."""
    if run_id:
        qe_rows = await pool.execute_query(
            f"""
            SELECT MIN(qe.START_TIME), MAX(qe.END_TIME)
            FROM {prefix}.QUERY_EXECUTIONS qe
            JOIN {prefix}.TEST_RESULTS tr ON qe.TEST_ID = tr.TEST_ID
            WHERE tr.RUN_ID = ?
            """,
            params=[run_id],
        )
    else:
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
        id_to_query = run_id or test_id
        tr_rows = await pool.execute_query(
            f"""
            SELECT MIN(START_TIME), MAX(COALESCE(END_TIME, CURRENT_TIMESTAMP()))
            FROM {prefix}.TEST_RESULTS
            WHERE RUN_ID = ?
            """,
            params=[id_to_query],
        )
        if not tr_rows or tr_rows[0][0] is None:
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


async def enrich_query_executions_from_query_history(
    *, test_id: str | None = None, run_id: str | None = None, max_pages: int = 50
) -> int:
    """
    Enrich QUERY_EXECUTIONS rows for a test using INFORMATION_SCHEMA.QUERY_HISTORY.

    Paginates through results since QUERY_HISTORY has a 10k row limit per call.
    Returns the total number of rows merged across all pages.

    Args:
        test_id: The test ID to enrich (single worker)
        run_id: The run ID to enrich (all workers in run)
        max_pages: Maximum pagination pages (default 50 = 500k queries max)
    """
    pool = snowflake_pool.get_default_pool()
    prefix = _results_prefix()

    time_range = await _get_test_time_range(
        pool, prefix, test_id=test_id, run_id=run_id
    )
    if time_range is None:
        return 0
    start_dt, end_dt = time_range

    # Get query_tag - for run_id, use parent row's tag (all workers share base tag)
    id_for_tag = run_id or test_id
    tag_rows = await pool.execute_query(
        f"""
        SELECT QUERY_TAG
        FROM {prefix}.TEST_RESULTS
        WHERE RUN_ID = ?
        LIMIT 1
        """,
        params=[id_for_tag],
    )
    query_tag = (
        str(tag_rows[0][0]).strip()
        if tag_rows and tag_rows[0] and tag_rows[0][0]
        else None
    )
    # Strip the phase suffix if present to match all phases
    if query_tag and ":phase=" in query_tag:
        query_tag = query_tag.split(":phase=")[0]
    query_tag_like = f"{query_tag}%" if query_tag else "unistore_benchmark%"

    start_buf = (start_dt - timedelta(minutes=5)).isoformat()
    current_end_dt = end_dt + timedelta(minutes=5)
    current_end = current_end_dt.isoformat()
    total_merged = 0

    for page in range(max_pages):
        result = await pool.execute_query(
            """
            SELECT MIN(END_TIME) as oldest_end, COUNT(*) as cnt
            FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                END_TIME_RANGE_START=>TO_TIMESTAMP_LTZ(?),
                END_TIME_RANGE_END=>TO_TIMESTAMP_LTZ(?),
                RESULT_LIMIT=>10000
            ))
            WHERE QUERY_TAG LIKE ?
            """,
            params=[start_buf, current_end, query_tag_like],
        )

        if not result or result[0][1] == 0:
            break

        row_count = result[0][1]
        oldest_end_time = result[0][0]

        # MERGE by QUERY_ID only - QUERY_ID is unique across all workers
        # The QUERY_TAG filter ensures we only match queries from this run
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
            WHERE QUERY_TAG LIKE ?
        ) src
        ON tgt.QUERY_ID = src.QUERY_ID
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
        await pool.execute_query(
            merge_query, params=[start_buf, current_end, query_tag_like]
        )
        total_merged += row_count

        if not oldest_end_time:
            break

        if isinstance(oldest_end_time, str):
            oldest_end_dt = datetime.fromisoformat(oldest_end_time)
        else:
            oldest_end_dt = oldest_end_time

        if oldest_end_dt.tzinfo is None:
            oldest_end_dt = oldest_end_dt.replace(tzinfo=UTC)

        # Stop if we've paged past the buffered start time.
        if oldest_end_dt <= (start_dt - timedelta(minutes=5)):
            break

        # Guard against non-progressing pagination.
        if oldest_end_dt >= current_end_dt:
            break

        current_end_dt = oldest_end_dt - timedelta(microseconds=1)
        current_end = current_end_dt.isoformat()

        logger.info(
            "Enrichment page %d/%d: %d rows merged (total: %d), window end: %s",
            page + 1,
            max_pages,
            row_count,
            total_merged,
            current_end,
        )

    return total_merged


class EnrichmentStats(NamedTuple):
    total_queries: int
    enriched_queries: int
    enrichment_ratio: float


async def get_enrichment_stats(
    *, test_id: str | None = None, run_id: str | None = None
) -> EnrichmentStats:
    """
    Get enrichment stats for a test or run.

    Args:
        test_id: Query by specific TEST_ID (single worker)
        run_id: Query by RUN_ID (all workers in a run via JOIN)

    If run_id is provided, queries all QUERY_EXECUTIONS for workers in that run.
    """
    pool = snowflake_pool.get_default_pool()
    prefix = _results_prefix()

    if run_id:
        rows = await pool.execute_query(
            f"""
            SELECT
                COUNT(*) AS total,
                COUNT(qe.SF_CLUSTER_NUMBER) AS enriched
            FROM {prefix}.QUERY_EXECUTIONS qe
            JOIN {prefix}.TEST_RESULTS tr ON qe.TEST_ID = tr.TEST_ID
            WHERE tr.RUN_ID = ?
            """,
            params=[run_id],
        )
    else:
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
    test_id: str | None = None,
    run_id: str | None = None,
    target_ratio: float = 0.90,
    max_wait_seconds: int = 120,
    poll_interval_seconds: int = 10,
) -> EnrichmentStats:
    """
    Enrich QUERY_EXECUTIONS with retries until target_ratio of queries are enriched.

    QUERY_HISTORY has ~45+ second latency before queries appear. This function polls
    periodically and re-runs the paginated enrichment merge until we achieve the
    target ratio or timeout.

    The number of pagination pages is calculated dynamically based on actual query
    count in QUERY_EXECUTIONS (10k queries per page, with 20% buffer).

    Args:
        test_id: The test ID to enrich (single worker, for backward compatibility)
        run_id: The run ID to enrich (all workers in run - preferred for multi-worker)
        target_ratio: Stop when this ratio of queries are enriched (default 0.90)
        max_wait_seconds: Maximum time to wait for enrichment (default 120)
        poll_interval_seconds: Time between enrichment attempts (default 10)
    """
    id_for_log = run_id or test_id
    initial_stats = await get_enrichment_stats(test_id=test_id, run_id=run_id)
    if initial_stats.total_queries == 0:
        logger.info("No queries to enrich for test %s", id_for_log)
        return initial_stats

    max_pages = max(10, (initial_stats.total_queries // 10_000) + 2)
    logger.info(
        "Enrichment for %s: %d queries -> max_pages=%d",
        id_for_log,
        initial_stats.total_queries,
        max_pages,
    )

    start = datetime.now(UTC)
    deadline = start + timedelta(seconds=max_wait_seconds)
    best_stats = EnrichmentStats(0, 0, 0.0)
    attempt = 0
    last_enriched = 0
    stalled_attempts = 0

    while datetime.now(UTC) < deadline:
        attempt += 1
        elapsed = int((datetime.now(UTC) - start).total_seconds())

        await enrich_query_executions_from_query_history(
            test_id=test_id, run_id=run_id, max_pages=max_pages
        )
        stats = await get_enrichment_stats(test_id=test_id, run_id=run_id)
        best_stats = stats

        progress = stats.enriched_queries - last_enriched
        last_enriched = stats.enriched_queries

        logger.info(
            "📊 Enrichment attempt %d for %s: %d/%d (%.1f%%) [+%d this pass, %ds elapsed]",
            attempt,
            id_for_log,
            stats.enriched_queries,
            stats.total_queries,
            stats.enrichment_ratio * 100,
            progress,
            elapsed,
        )

        if stats.enrichment_ratio >= target_ratio:
            logger.info(
                "✅ Enrichment complete for %s: %.1f%% (%d/%d queries) in %ds",
                id_for_log,
                stats.enrichment_ratio * 100,
                stats.enriched_queries,
                stats.total_queries,
                elapsed,
            )
            break

        if progress == 0:
            stalled_attempts += 1
            if stalled_attempts >= 3 and stats.enrichment_ratio > 0.5:
                logger.warning(
                    "⚠️ Enrichment stalled for %s at %.1f%% - hybrid workload may not emit per-query QUERY_HISTORY rows",
                    id_for_log,
                    stats.enrichment_ratio * 100,
                )
                break
        else:
            stalled_attempts = 0

        await asyncio.sleep(poll_interval_seconds)

    elapsed = int((datetime.now(UTC) - start).total_seconds())
    if best_stats.enrichment_ratio < target_ratio and best_stats.total_queries > 0:
        logger.warning(
            "⚠️ Enrichment ended for %s: %.1f%% (%d/%d queries) after %ds",
            id_for_log,
            best_stats.enrichment_ratio * 100,
            best_stats.enriched_queries,
            best_stats.total_queries,
            elapsed,
        )

    return best_stats


async def update_test_overhead_percentiles(
    *, test_id: str | None = None, run_id: str | None = None
) -> None:
    """
    Compute overhead percentiles from QUERY_EXECUTIONS.APP_OVERHEAD_MS and store
    them on TEST_RESULTS (one-row summary table).

    For run_id, updates all worker rows in the run.
    """
    pool = snowflake_pool.get_default_pool()
    prefix = _results_prefix()

    if run_id:
        # Update all TEST_RESULTS rows for this run using aggregated overhead from all workers
        query = f"""
        UPDATE {prefix}.TEST_RESULTS tr
        SET
            APP_OVERHEAD_P50_MS = (
                SELECT PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY qe.APP_OVERHEAD_MS)
                FROM {prefix}.QUERY_EXECUTIONS qe
                JOIN {prefix}.TEST_RESULTS tr2 ON qe.TEST_ID = tr2.TEST_ID
                WHERE tr2.RUN_ID = ?
                  AND qe.APP_OVERHEAD_MS IS NOT NULL
            ),
            APP_OVERHEAD_P95_MS = (
                SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY qe.APP_OVERHEAD_MS)
                FROM {prefix}.QUERY_EXECUTIONS qe
                JOIN {prefix}.TEST_RESULTS tr2 ON qe.TEST_ID = tr2.TEST_ID
                WHERE tr2.RUN_ID = ?
                  AND qe.APP_OVERHEAD_MS IS NOT NULL
            ),
            APP_OVERHEAD_P99_MS = (
                SELECT PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY qe.APP_OVERHEAD_MS)
                FROM {prefix}.QUERY_EXECUTIONS qe
                JOIN {prefix}.TEST_RESULTS tr2 ON qe.TEST_ID = tr2.TEST_ID
                WHERE tr2.RUN_ID = ?
                  AND qe.APP_OVERHEAD_MS IS NOT NULL
            ),
            UPDATED_AT = CURRENT_TIMESTAMP()
        WHERE tr.RUN_ID = ?;
        """
        await pool.execute_query(query, params=[run_id, run_id, run_id, run_id])
    else:
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
    Update the enrichment status for a test and all its worker rows.

    Args:
        test_id: The test ID (parent run ID)
        status: One of PENDING, COMPLETED, FAILED, SKIPPED
        error: Optional error message if status is FAILED
    """
    pool = snowflake_pool.get_default_pool()
    prefix = _results_prefix()

    # Update all rows with the same RUN_ID (parent + all workers)
    # This ensures the aggregated enrichment status reflects the true state.
    query = f"""
    UPDATE {prefix}.TEST_RESULTS
    SET
        ENRICHMENT_STATUS = ?,
        ENRICHMENT_ERROR = ?,
        UPDATED_AT = CURRENT_TIMESTAMP()
    WHERE RUN_ID = ?
    """

    await pool.execute_query(query, params=[status, error, test_id])


async def get_enrichment_status(*, test_id: str) -> dict[str, Any] | None:
    """
    Get the enrichment status for a test, including query stats.

    Returns dict with enrichment_status, enrichment_error, query counts, or None if test not found.
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

    stats = await get_enrichment_stats(test_id=test_id)

    return {
        "test_status": rows[0][0],
        "enrichment_status": rows[0][1],
        "enrichment_error": rows[0][2],
        "total_queries": stats.total_queries,
        "enriched_queries": stats.enriched_queries,
        "enrichment_ratio": stats.enrichment_ratio,
    }
