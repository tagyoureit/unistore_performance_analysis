"""
Live Metrics Cache

Stores recent worker metrics snapshots in memory for low-latency WebSocket updates.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

from backend.config import settings
from backend.models.metrics import Metrics


@dataclass
class WorkerLiveMetrics:
    test_id: str
    worker_id: str
    worker_group_id: int
    worker_group_count: int
    phase: str | None
    status: str | None
    target_connections: int | None
    metrics: Metrics
    received_at: datetime


@dataclass
class RunLiveMetrics:
    updated_at: datetime
    workers: dict[str, WorkerLiveMetrics]
    test_ids: set[str]


@dataclass
class LiveRunSnapshot:
    metrics: dict[str, Any]
    workers: list[dict[str, Any]]
    test_ids: list[str]
    updated_at: datetime


class LiveMetricsCache:
    def __init__(self, *, ttl_seconds: float) -> None:
        self._ttl_seconds = max(1.0, float(ttl_seconds or 0.0))
        self._lock = asyncio.Lock()
        self._runs: dict[str, RunLiveMetrics] = {}

    async def update(
        self,
        *,
        run_id: str,
        test_id: str,
        worker_id: str,
        worker_group_id: int,
        worker_group_count: int,
        phase: str | None,
        status: str | None,
        target_connections: int | None,
        metrics: Metrics,
    ) -> None:
        now = datetime.now(UTC)
        run_id = str(run_id).strip()
        if not run_id:
            return
        snapshot = WorkerLiveMetrics(
            test_id=str(test_id).strip(),
            worker_id=str(worker_id).strip(),
            worker_group_id=int(worker_group_id),
            worker_group_count=int(worker_group_count),
            phase=_normalize_phase(phase),
            status=_normalize_status(status),
            target_connections=(
                int(target_connections) if target_connections is not None else None
            ),
            metrics=metrics,
            received_at=now,
        )
        async with self._lock:
            run = self._runs.get(run_id)
            if run is None:
                run = RunLiveMetrics(updated_at=now, workers={}, test_ids=set())
                self._runs[run_id] = run
            run.updated_at = now
            if snapshot.worker_id:
                run.workers[snapshot.worker_id] = snapshot
            if snapshot.test_id:
                run.test_ids.add(snapshot.test_id)
            run.test_ids.add(run_id)
            self._prune_locked(now)

    async def get_run_snapshot(self, *, run_id: str) -> LiveRunSnapshot | None:
        run_id = str(run_id).strip()
        if not run_id:
            return None
        now = datetime.now(UTC)
        async with self._lock:
            self._prune_locked(now)
            run = self._runs.get(run_id)
            if run is None or not run.workers:
                return None
            workers = list(run.workers.values())
            test_ids = sorted(run.test_ids)
            updated_at = run.updated_at
        metrics, worker_rows = _aggregate_workers(workers, now=now)
        if not metrics:
            return None
        return LiveRunSnapshot(
            metrics=metrics,
            workers=worker_rows,
            test_ids=test_ids,
            updated_at=updated_at,
        )

    def _prune_locked(self, now: datetime) -> None:
        cutoff_seconds = self._ttl_seconds
        stale_runs: list[str] = []
        for run_id, run in self._runs.items():
            stale_workers = [
                worker_id
                for worker_id, snapshot in run.workers.items()
                if (now - snapshot.received_at).total_seconds() > cutoff_seconds
            ]
            for worker_id in stale_workers:
                run.workers.pop(worker_id, None)
            if not run.workers:
                stale_runs.append(run_id)
        for run_id in stale_runs:
            self._runs.pop(run_id, None)


def _normalize_phase(value: str | None) -> str | None:
    if not value:
        return None
    return str(value).strip().upper() or None


def _normalize_status(value: str | None) -> str | None:
    if not value:
        return None
    return str(value).strip().upper() or None


def _health_from(status_value: str | None, age_seconds: float | None) -> str:
    status_upper = str(status_value or "").upper()
    if status_upper == "DEAD":
        return "DEAD"
    if age_seconds is None:
        return "STALE"
    if age_seconds >= 60:
        return "DEAD"
    if age_seconds >= 30:
        return "STALE"
    return "HEALTHY"


def _sum_dicts(dicts: list[dict[str, Any]]) -> dict[str, float]:
    out: dict[str, float] = {}
    for d in dicts:
        for key, value in d.items():
            try:
                out[key] = out.get(key, 0.0) + float(value or 0)
            except Exception:
                continue
    return out


def _avg_dicts(dicts: list[dict[str, Any]]) -> dict[str, float]:
    if not dicts:
        return {}
    summed = _sum_dicts(dicts)
    return {key: value / len(dicts) for key, value in summed.items()}


def _aggregate_workers(
    workers: list[WorkerLiveMetrics], *, now: datetime
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if not workers:
        return {}, []

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

    phases: list[str] = []
    app_ops_list: list[dict[str, Any]] = []
    sf_bench_list: list[dict[str, Any]] = []
    resources_list: list[dict[str, Any]] = []
    find_max_controller: dict[str, Any] | None = None
    qps_controller: dict[str, Any] | None = None
    # NOTE: Warehouse MCW data (started_clusters) is NOT aggregated from workers.
    # It's polled by the orchestrator only and fetched from WAREHOUSE_POLL_SNAPSHOTS
    # at the API layer. See main.py where RUN_UPDATE payloads are built.

    worker_rows: list[dict[str, Any]] = []

    for snapshot in workers:
        metrics = snapshot.metrics
        phase_value = _normalize_phase(snapshot.phase)
        custom_metrics: Any = metrics.custom_metrics
        if isinstance(custom_metrics, str):
            try:
                import json

                custom_metrics = json.loads(custom_metrics)
            except Exception:
                custom_metrics = {}
        if not isinstance(custom_metrics, dict):
            custom_metrics = {}

        if not phase_value:
            phase_from_custom = custom_metrics.get("phase")
            if isinstance(phase_from_custom, str):
                phase_value = _normalize_phase(phase_from_custom)
        if phase_value:
            phases.append(phase_value)

        status_upper = _normalize_status(snapshot.status)
        # Include WARMUP, MEASUREMENT, and RUNNING phase metrics.
        # Workers report phase as "RUNNING" (not "MEASUREMENT") during benchmark execution.
        include_for_metrics = (
            not phase_value or phase_value in ("WARMUP", "MEASUREMENT", "RUNNING")
        ) and status_upper != "DEAD"

        if include_for_metrics:
            elapsed_seconds = max(
                float(metrics.elapsed_seconds or 0.0), elapsed_seconds
            )
            total_ops += int(metrics.total_operations or 0)
            qps += float(metrics.current_qps or 0.0)
            p50_vals.append(float(metrics.overall_latency.p50 or 0.0))
            p95_vals.append(float(metrics.overall_latency.p95 or 0.0))
            p99_vals.append(float(metrics.overall_latency.p99 or 0.0))
            avg_vals.append(float(metrics.overall_latency.avg or 0.0))
            read_count += int(metrics.read_metrics.count or 0)
            write_count += int(metrics.write_metrics.count or 0)
            error_count += int(metrics.failed_operations or 0)
            active_connections += int(metrics.active_connections or 0)
            target_connections += int(
                snapshot.target_connections
                if snapshot.target_connections is not None
                else metrics.target_workers
            )

        app_ops = custom_metrics.get("app_ops_breakdown")
        if isinstance(app_ops, dict):
            app_ops_list.append(app_ops)
        sf_bench = custom_metrics.get("sf_bench")
        if isinstance(sf_bench, dict):
            sf_bench_list.append(sf_bench)
        # NOTE: Warehouse MCW data is NOT collected from workers - see comment above.
        resources = custom_metrics.get("resources")
        if isinstance(resources, dict):
            resources_list.append(resources)
        if find_max_controller is None and isinstance(
            custom_metrics.get("find_max_controller"), dict
        ):
            find_max_controller = custom_metrics.get("find_max_controller")
            # Debug: Log when we pick up find_max_controller from a worker
            logger.info(
                "[LiveCache] Picked find_max_controller from worker %s: step=%s target=%s end_ms=%s",
                snapshot.worker_id,
                find_max_controller.get("current_step"),
                find_max_controller.get("target_workers"),
                find_max_controller.get("step_end_at_epoch_ms"),
            )
        if qps_controller is None and isinstance(
            custom_metrics.get("qps_controller"), dict
        ):
            qps_controller = custom_metrics.get("qps_controller")

        last_dt = metrics.timestamp if isinstance(metrics.timestamp, datetime) else None
        age_seconds = (now - snapshot.received_at).total_seconds()
        worker_rows.append(
            {
                "worker_id": snapshot.worker_id,
                "worker_group_id": int(snapshot.worker_group_id or 0),
                "status": status_upper or "UNKNOWN",
                "phase": phase_value,
                "health": _health_from(status_upper, age_seconds),
                "last_heartbeat": (
                    last_dt.isoformat() if isinstance(last_dt, datetime) else None
                ),
                "last_heartbeat_ago_s": float(age_seconds),
                "metrics": {
                    "qps": float(metrics.current_qps or 0.0),
                    "p50_latency_ms": float(metrics.overall_latency.p50 or 0.0),
                    "p95_latency_ms": float(metrics.overall_latency.p95 or 0.0),
                    "p99_latency_ms": float(metrics.overall_latency.p99 or 0.0),
                    "avg_latency_ms": float(metrics.overall_latency.avg or 0.0),
                    "error_count": int(metrics.failed_operations or 0),
                    "active_connections": int(metrics.active_connections or 0),
                    "target_connections": int(
                        snapshot.target_connections
                        if snapshot.target_connections is not None
                        else metrics.target_workers
                    ),
                },
            }
        )

    error_rate = (error_count / total_ops) if total_ops > 0 else 0.0
    p50_latency = sum(p50_vals) / len(p50_vals) if p50_vals else 0.0
    p95_latency = max(p95_vals) if p95_vals else 0.0
    p99_latency = max(p99_vals) if p99_vals else 0.0
    avg_latency = sum(avg_vals) / len(avg_vals) if avg_vals else 0.0

    custom_metrics_out = {
        "app_ops_breakdown": _sum_dicts(app_ops_list),
        "sf_bench": _sum_dicts(sf_bench_list),
        "resources": _avg_dicts(resources_list),
        # NOTE: warehouse is NOT included here - it's fetched from WAREHOUSE_POLL_SNAPSHOTS
        # at the API layer (main.py) since it's orchestrator-level data, not worker-level.
    }
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

    worker_rows.sort(
        key=lambda w: (
            int(w.get("worker_group_id") or 0),
            str(w.get("worker_id") or ""),
        )
    )

    return (
        {
            "phase": resolved_phase,
            "elapsed": float(elapsed_seconds),
            "ops": {"total": total_ops, "current_per_sec": float(qps)},
            "operations": {"reads": read_count, "writes": write_count},
            "latency": {
                "p50": float(p50_latency),
                "p95": float(p95_latency),
                "p99": float(p99_latency),
                "avg": float(avg_latency),
            },
            "latency_aggregation_method": "slowest_worker_approximation",
            "errors": {"count": error_count, "rate": float(error_rate)},
            "connections": {
                "active": active_connections,
                "target": target_connections,
            },
            "custom_metrics": custom_metrics_out,
        },
        worker_rows,
    )


live_metrics_cache = LiveMetricsCache(
    ttl_seconds=float(getattr(settings, "LIVE_METRICS_CACHE_TTL_SECONDS", 5.0) or 5.0)
)
