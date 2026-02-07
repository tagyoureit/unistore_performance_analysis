"""
Metrics aggregation for WebSocket streaming.
"""

import json
from datetime import UTC, datetime
from typing import Any

from backend.config import settings
from backend.connectors import snowflake_pool
from backend.core import results_store

from .helpers import (
    _avg_dicts,
    _coerce_datetime,
    _health_from,
    _sum_dicts,
    _to_float,
    _to_int,
)


def build_aggregate_metrics(
    *,
    ops: dict[str, Any] | None,
    latency: dict[str, Any] | None,
    errors: dict[str, Any] | None,
    connections: dict[str, Any] | None,
    operations: dict[str, Any] | None,
) -> dict[str, Any]:
    """Build aggregate metrics payload from component dictionaries."""
    ops_payload = ops or {}
    latency_payload = latency or {}
    errors_payload = errors or {}
    connections_payload = connections or {}
    operations_payload = operations or {}
    total_ops = int(ops_payload.get("total") or 0)
    return {
        "total_ops": total_ops,
        "qps": float(ops_payload.get("current_per_sec") or 0),
        "p50_latency_ms": float(latency_payload.get("p50") or 0),
        "p95_latency_ms": float(latency_payload.get("p95") or 0),
        "p99_latency_ms": float(latency_payload.get("p99") or 0),
        "avg_latency_ms": float(latency_payload.get("avg") or 0),
        "error_rate": float(errors_payload.get("rate") or 0),
        "total_errors": int(errors_payload.get("count") or 0),
        "active_connections": int(connections_payload.get("active") or 0),
        "target_connections": int(connections_payload.get("target") or 0),
        "read_count": int(operations_payload.get("reads") or 0),
        "write_count": int(operations_payload.get("writes") or 0),
    }


def build_run_snapshot(
    *,
    run_id: str,
    status: str | None,
    phase: str | None,
    elapsed_seconds: float | None,
    worker_count: int,
    aggregate_metrics: dict[str, Any],
    run_status: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a run snapshot payload."""
    run_payload: dict[str, Any] = {
        "run_id": run_id,
        "status": status or "RUNNING",
        "phase": phase or status or "RUNNING",
        "worker_count": worker_count,
        "elapsed_seconds": float(elapsed_seconds or 0),
        "aggregate_metrics": aggregate_metrics,
    }
    if run_status:
        run_payload["workers_expected"] = run_status.get("total_workers_expected")
        run_payload["workers_registered"] = run_status.get("workers_registered")
        run_payload["workers_active"] = run_status.get("workers_active")
        run_payload["workers_completed"] = run_status.get("workers_completed")
        start_time = _coerce_datetime(run_status.get("start_time"))
        end_time = _coerce_datetime(run_status.get("end_time"))
        run_payload["start_time"] = (
            start_time.isoformat() if start_time is not None else None
        )
        run_payload["end_time"] = end_time.isoformat() if end_time is not None else None
    return run_payload


async def aggregate_multi_worker_metrics(parent_run_id: str) -> dict[str, Any]:
    """Aggregate metrics from multiple workers for a run."""
    pool = snowflake_pool.get_default_pool()
    rows = await pool.execute_query(
        f"""
        WITH latest_per_worker AS (
            SELECT
                wms.*,
                ROW_NUMBER() OVER (PARTITION BY WORKER_ID ORDER BY TIMESTAMP DESC) AS rn
            FROM {settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}.WORKER_METRICS_SNAPSHOTS wms
            WHERE RUN_ID = ?
        )
        SELECT
            wms.ELAPSED_SECONDS,
            wms.TOTAL_QUERIES,
            wms.QPS,
            wms.P50_LATENCY_MS,
            wms.P95_LATENCY_MS,
            wms.P99_LATENCY_MS,
            wms.AVG_LATENCY_MS,
            wms.READ_COUNT,
            wms.WRITE_COUNT,
            wms.ERROR_COUNT,
            wms.ACTIVE_CONNECTIONS,
            wms.TARGET_CONNECTIONS,
            wms.CUSTOM_METRICS,
            wms.PHASE,
            wms.WORKER_ID,
            wms.WORKER_GROUP_ID
        FROM latest_per_worker wms
        WHERE wms.rn = 1
        """,
        params=[parent_run_id],
    )
    heartbeat_rows = await pool.execute_query(
        f"""
        SELECT
            WORKER_ID,
            WORKER_GROUP_ID,
            STATUS,
            PHASE,
            LAST_HEARTBEAT,
            ACTIVE_CONNECTIONS,
            TARGET_CONNECTIONS,
            QUERIES_PROCESSED,
            ERROR_COUNT
        FROM {settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}.WORKER_HEARTBEATS
        WHERE RUN_ID = ?
        """,
        params=[parent_run_id],
    )

    if not rows and not heartbeat_rows:
        return {}

    elapsed_seconds = 0.0
    total_ops = 0
    qps = 0.0
    p50_vals: list[float] = []
    p95_vals: list[float] = []
    p99_vals: list[float] = []
    avg_vals: list[float] = []
    read_count = 0
    write_count = 0
    error_count = 0
    active_connections = 0
    target_connections = 0

    app_ops_list: list[dict[str, Any]] = []
    sf_bench_list: list[dict[str, Any]] = []
    warehouse_list: list[dict[str, Any]] = []
    resources_list: list[dict[str, Any]] = []
    find_max_controller: dict[str, Any] | None = None
    qps_controller: dict[str, Any] | None = None
    phases: list[str] = []
    snapshot_by_worker: dict[str, dict[str, Any]] = {}

    for (
        elapsed,
        total_queries,
        qps_row,
        p50,
        p95,
        p99,
        avg_latency,
        read_row,
        write_row,
        error_row,
        active_row,
        target_row,
        custom_metrics,
        phase,
        _worker_id,
        worker_group_id,
    ) in rows:
        worker_id = str(_worker_id or "")
        snapshot_by_worker[worker_id] = {
            "worker_id": worker_id,
            "worker_group_id": int(worker_group_id or 0),
            "elapsed_seconds": float(elapsed or 0),
            "total_queries": _to_int(total_queries),
            "qps": _to_float(qps_row),
            "p50_latency_ms": _to_float(p50),
            "p95_latency_ms": _to_float(p95),
            "p99_latency_ms": _to_float(p99),
            "avg_latency_ms": _to_float(avg_latency),
            "read_count": _to_int(read_row),
            "write_count": _to_int(write_row),
            "error_count": _to_int(error_row),
            "active_connections": _to_int(active_row),
            "target_connections": _to_int(target_row),
            "phase": str(phase or ""),
        }
        phase_value = str(phase or "").strip().upper()
        if phase_value:
            phases.append(phase_value)

        heartbeat_status = None
        for hb in heartbeat_rows:
            if str(hb[0] or "") == worker_id:
                heartbeat_status = hb[2]
                break
        status_upper = str(heartbeat_status or "").upper()
        # Include WARMUP, MEASUREMENT, and RUNNING phase metrics for real-time streaming.
        # Workers report phase as "RUNNING" (not "MEASUREMENT") during benchmark execution.
        # Excluding WARMUP caused ~45s delay before metrics appeared on dashboard.
        include_for_metrics = (
            not phase_value or phase_value in ("WARMUP", "MEASUREMENT", "RUNNING")
        ) and status_upper != "DEAD"
        if include_for_metrics:
            elapsed_seconds = max(float(elapsed or 0), elapsed_seconds)
            total_ops += _to_int(total_queries)
            qps += _to_float(qps_row)
            p50_vals.append(_to_float(p50))
            p95_vals.append(_to_float(p95))
            p99_vals.append(_to_float(p99))
            avg_vals.append(_to_float(avg_latency))
            read_count += _to_int(read_row)
            write_count += _to_int(write_row)
            error_count += _to_int(error_row)
            active_connections += _to_int(active_row)
            target_connections += _to_int(target_row)

        cm: Any = custom_metrics
        if isinstance(cm, str):
            try:
                cm = json.loads(cm)
            except Exception:
                cm = {}
        if not isinstance(cm, dict):
            cm = {}

        if not phase_value:
            phase_val = cm.get("phase")
            if isinstance(phase_val, str) and phase_val.strip():
                phases.append(phase_val.strip().upper())

        app_ops = cm.get("app_ops_breakdown")
        if isinstance(app_ops, dict):
            app_ops_list.append(app_ops)
        sf_bench = cm.get("sf_bench")
        if isinstance(sf_bench, dict):
            sf_bench_list.append(sf_bench)
        warehouse = cm.get("warehouse")
        if isinstance(warehouse, dict):
            warehouse_list.append(warehouse)
        resources = cm.get("resources")
        if isinstance(resources, dict):
            resources_list.append(resources)
        if find_max_controller is None and isinstance(
            cm.get("find_max_controller"), dict
        ):
            find_max_controller = cm.get("find_max_controller")
        if qps_controller is None and isinstance(cm.get("qps_controller"), dict):
            qps_controller = cm.get("qps_controller")

    error_rate = (error_count / total_ops) if total_ops > 0 else 0.0
    p50_latency = sum(p50_vals) / len(p50_vals) if p50_vals else 0.0
    p95_latency = max(p95_vals) if p95_vals else 0.0
    p99_latency = max(p99_vals) if p99_vals else 0.0
    avg_latency = sum(avg_vals) / len(avg_vals) if avg_vals else 0.0

    custom_metrics_out = {
        "app_ops_breakdown": _sum_dicts(app_ops_list),
        "sf_bench": _sum_dicts(sf_bench_list),
        "resources": _avg_dicts(resources_list),
    }
    # Warehouse MCW data comes ONLY from orchestrator's WAREHOUSE_POLL_SNAPSHOTS
    poller_snapshot = await results_store.fetch_latest_warehouse_poll_snapshot(
        run_id=parent_run_id
    )
    if poller_snapshot is not None:
        custom_metrics_out["warehouse"] = poller_snapshot
    # If no snapshot available yet, warehouse key is simply not set (don't sum worker data)
    if find_max_controller is not None:
        custom_metrics_out["find_max_controller"] = find_max_controller
    if qps_controller is not None:
        custom_metrics_out["qps_controller"] = qps_controller

    phase_order = {
        "PREPARING": 0,
        "WARMUP": 1,
        "RUNNING": 2,
        "PROCESSING": 3,
        "COMPLETED": 4,
    }
    phase_priority = {
        "FAILED": -1,
        "CANCELLED": -1,
        "STOPPED": -1,
    }
    resolved_phase = None
    for phase in phases:
        if phase in phase_priority:
            resolved_phase = phase
            break
    if resolved_phase is None and phases:
        resolved_phase = min(phases, key=lambda p: phase_order.get(p, 99))

    heartbeat_by_worker: dict[str, dict[str, Any]] = {}
    now = datetime.now(UTC)
    for (
        worker_id,
        worker_group_id,
        status_value,
        phase_value,
        last_heartbeat,
        active_connections_hb,
        target_connections_hb,
        queries_processed,
        error_count_value,
    ) in heartbeat_rows:
        worker_id_str = str(worker_id or "")
        last_dt = _coerce_datetime(last_heartbeat)
        age_seconds = (now - last_dt).total_seconds() if last_dt is not None else None
        heartbeat_by_worker[worker_id_str] = {
            "worker_id": worker_id_str,
            "worker_group_id": int(worker_group_id or 0),
            "status": str(status_value or "").upper(),
            "phase": str(phase_value or "").upper(),
            "last_heartbeat": last_dt,
            "last_heartbeat_ago_s": age_seconds,
            "active_connections": _to_int(active_connections_hb),
            "target_connections": _to_int(target_connections_hb),
            "queries_processed": _to_int(queries_processed),
            "error_count": _to_int(error_count_value),
        }

    workers_out: list[dict[str, Any]] = []
    for worker_id, hb in heartbeat_by_worker.items():
        snapshot = snapshot_by_worker.get(worker_id, {})
        last_dt = hb.get("last_heartbeat")
        workers_out.append(
            {
                "worker_id": worker_id,
                "worker_group_id": hb.get("worker_group_id", 0),
                "status": hb.get("status"),
                "phase": hb.get("phase") or snapshot.get("phase"),
                "health": _health_from(
                    hb.get("status"), hb.get("last_heartbeat_ago_s")
                ),
                "last_heartbeat": (
                    last_dt.isoformat() if isinstance(last_dt, datetime) else None
                ),
                "last_heartbeat_ago_s": hb.get("last_heartbeat_ago_s"),
                "metrics": {
                    "qps": snapshot.get("qps") or 0.0,
                    "p50_latency_ms": snapshot.get("p50_latency_ms") or 0.0,
                    "p95_latency_ms": snapshot.get("p95_latency_ms") or 0.0,
                    "p99_latency_ms": snapshot.get("p99_latency_ms") or 0.0,
                    "avg_latency_ms": snapshot.get("avg_latency_ms") or 0.0,
                    "error_count": snapshot.get("error_count")
                    if snapshot
                    else hb.get("error_count", 0),
                    "active_connections": snapshot.get("active_connections")
                    if snapshot
                    else hb.get("active_connections", 0),
                    "target_connections": snapshot.get("target_connections")
                    if snapshot
                    else hb.get("target_connections", 0),
                },
            }
        )
    for worker_id, snapshot in snapshot_by_worker.items():
        if worker_id in heartbeat_by_worker:
            continue
        workers_out.append(
            {
                "worker_id": worker_id,
                "worker_group_id": snapshot.get("worker_group_id", 0),
                "status": "UNKNOWN",
                "phase": snapshot.get("phase") or None,
                "health": "STALE",
                "last_heartbeat": None,
                "last_heartbeat_ago_s": None,
                "metrics": {
                    "qps": snapshot.get("qps") or 0.0,
                    "p50_latency_ms": snapshot.get("p50_latency_ms") or 0.0,
                    "p95_latency_ms": snapshot.get("p95_latency_ms") or 0.0,
                    "p99_latency_ms": snapshot.get("p99_latency_ms") or 0.0,
                    "avg_latency_ms": snapshot.get("avg_latency_ms") or 0.0,
                    "error_count": snapshot.get("error_count") or 0,
                    "active_connections": snapshot.get("active_connections") or 0,
                    "target_connections": snapshot.get("target_connections") or 0,
                },
            }
        )
    workers_out.sort(
        key=lambda w: (
            int(w.get("worker_group_id") or 0),
            str(w.get("worker_id") or ""),
        )
    )

    return {
        "phase": resolved_phase,
        "elapsed": float(elapsed_seconds),
        "ops": {
            "total": total_ops,
            "current_per_sec": float(qps),
        },
        "operations": {
            "reads": read_count,
            "writes": write_count,
        },
        "latency": {
            "p50": float(p50_latency),
            "p95": float(p95_latency),
            "p99": float(p99_latency),
            "avg": float(avg_latency),
        },
        "latency_aggregation_method": "slowest_worker_approximation",
        "errors": {
            "count": error_count,
            "rate": float(error_rate),
        },
        "connections": {
            "active": active_connections,
            "target": target_connections,
        },
        "custom_metrics": custom_metrics_out,
        "workers": workers_out,
    }
