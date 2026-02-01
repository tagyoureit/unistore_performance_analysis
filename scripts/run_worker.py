#!/usr/bin/env python3
"""Run a headless benchmark worker for multi-worker orchestration."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from backend.config import settings
from backend.connectors import snowflake_pool
from backend.core import results_store
from backend.core.test_executor import TestExecutor
from backend.core.test_log_stream import (
    CURRENT_TEST_ID,
    CURRENT_WORKER_ID,
    TestLogQueueHandler,
)
from backend.core.test_registry import registry
from backend.models import Metrics, TableType, TestScenario, TestStatus

logger = logging.getLogger(__name__)


@dataclass
class WorkerConfig:
    run_id: str
    worker_id: str
    worker_group_id: int
    worker_group_count: int


@dataclass
class TargetRamp:
    active: bool = False
    start_time: float = 0.0
    start_value: int = 0
    end_value: int = 0
    duration_seconds: float = 0.0

    def start(self, *, current: int, target: int, duration_seconds: float) -> None:
        self.active = True
        self.start_time = time.monotonic()
        self.start_value = int(current)
        self.end_value = int(target)
        self.duration_seconds = max(0.0, float(duration_seconds))

    def current_target(self) -> int | None:
        if not self.active:
            return None
        elapsed = time.monotonic() - self.start_time
        if elapsed >= self.duration_seconds:
            self.active = False
            return int(self.end_value)
        if self.duration_seconds <= 0:
            return int(self.end_value)
        progress = elapsed / self.duration_seconds
        delta = self.end_value - self.start_value
        return int(self.start_value + (delta * progress))


class ConnectionHealthTracker:
    def __init__(self, *, timeout_seconds: float = 60.0) -> None:
        self.timeout_seconds = float(timeout_seconds)
        self._last_success = time.monotonic()

    def record_success(self) -> None:
        self._last_success = time.monotonic()

    def ok(self) -> bool:
        return (time.monotonic() - self._last_success) <= self.timeout_seconds


def _parse_variant(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            return dict(json.loads(value))
        except Exception:
            return {}
    return {}


def _coerce_int(value: Any, *, default: int) -> int:
    try:
        return int(float(value))
    except Exception:
        return int(default)


def _coerce_float(value: Any, *, default: float) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _event_applies(
    *, scope: str, event_data: dict[str, Any], cfg: WorkerConfig
) -> bool:
    scope_upper = str(scope or "RUN").upper()
    if scope_upper == "RUN":
        return True
    if scope_upper == "WORKER_GROUP":
        return _coerce_int(event_data.get("worker_group_id"), default=-1) == int(
            cfg.worker_group_id
        )
    if scope_upper == "WORKER":
        return str(event_data.get("worker_id") or "") == cfg.worker_id
    return False


def _compute_initial_target(
    *,
    total_connections: int,
    worker_group_id: int,
    worker_group_count: int,
    per_worker_connections: int | None,
) -> int:
    if per_worker_connections is not None:
        return int(per_worker_connections)
    total = max(0, int(total_connections))
    count = max(1, int(worker_group_count))
    base = total // count
    remainder = total % count
    if int(worker_group_id) < remainder:
        return base + 1
    return base


def _resolve_per_worker_cap(
    *,
    load_mode: str,
    total_connections: int,
    initial_target: int,
    per_worker_connections: int | None,
) -> int:
    if per_worker_connections is not None:
        return max(1, int(per_worker_connections))
    if load_mode in {"QPS", "FIND_MAX_CONCURRENCY"}:
        return max(1, int(total_connections))
    return max(1, int(initial_target))


def _extract_resources(metrics: Metrics) -> dict[str, Any]:
    custom = metrics.custom_metrics or {}
    resources = custom.get("resources")
    return dict(resources) if isinstance(resources, dict) else {}


def _normalize_phase(phase: str | None) -> str | None:
    phase_upper = str(phase or "").upper()
    if phase_upper in {"WARMUP", "MEASUREMENT", "COOLDOWN"}:
        return phase_upper
    return None


async def _fetch_run_status(run_id: str) -> dict[str, Any] | None:
    pool = snowflake_pool.get_default_pool()
    rows = await pool.execute_query(
        f"""
        SELECT STATUS, PHASE, SCENARIO_CONFIG, WORKER_TARGETS, TEST_NAME
        FROM {settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}.RUN_STATUS
        WHERE RUN_ID = ?
        """,
        params=[run_id],
    )
    if not rows:
        return None
    status, phase, scenario_config, worker_targets, test_name = rows[0]
    return {
        "status": str(status or "").upper(),
        "phase": str(phase or "").upper(),
        "scenario_config": scenario_config,
        "worker_targets": worker_targets,
        "test_name": str(test_name or ""),
    }


async def _fetch_control_events(
    *, run_id: str, last_seen_sequence: int, limit: int = 100
) -> list[dict[str, Any]]:
    pool = snowflake_pool.get_default_pool()
    rows = await pool.execute_query(
        f"""
        SELECT EVENT_TYPE, EVENT_DATA, SEQUENCE_ID
        FROM {settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}.RUN_CONTROL_EVENTS
        WHERE RUN_ID = ?
          AND SEQUENCE_ID > ?
        ORDER BY SEQUENCE_ID ASC
        LIMIT ?
        """,
        params=[run_id, int(last_seen_sequence), int(limit)],
    )
    events: list[dict[str, Any]] = []
    for event_type, event_data, sequence_id in rows:
        payload = _parse_variant(event_data)
        events.append(
            {
                "event_type": str(event_type or "").upper(),
                "event_data": payload,
                "sequence_id": int(sequence_id or 0),
            }
        )
    return events


async def _fetch_template_meta(template_id: str) -> dict[str, Any]:
    pool = snowflake_pool.get_default_pool()
    rows = await pool.execute_query(
        f"""
        SELECT TEMPLATE_NAME, TAGS
        FROM {settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}.TEST_TEMPLATES
        WHERE TEMPLATE_ID = ?
        """,
        params=[template_id],
    )
    if not rows:
        return {"template_name": "", "tags": {}}
    name, tags = rows[0]
    return {
        "template_name": str(name or ""),
        "tags": _parse_variant(tags),
    }


async def _run_worker_control_plane(args: argparse.Namespace) -> int:
    run_id = str(args.run_id or "").strip()
    worker_id = str(args.worker_id or "").strip()
    if not run_id or not worker_id:
        logger.error("--run-id and --worker-id are required for control-plane workers")
        return 1

    worker_group_id = _coerce_int(args.worker_group_id, default=0)
    worker_group_count = _coerce_int(args.worker_group_count, default=1)
    if worker_group_count < 1:
        logger.error("--worker-group-count must be >= 1")
        return 1
    if worker_group_id < 0:
        logger.warning("worker_group_id < 0; coercing to 0")
        worker_group_id = 0
    if worker_group_id >= worker_group_count:
        logger.warning(
            "worker_group_id %s >= worker_group_count %s; coercing to %s",
            worker_group_id,
            worker_group_count,
            worker_group_count - 1,
        )
        worker_group_id = worker_group_count - 1

    cfg = WorkerConfig(
        run_id=run_id,
        worker_id=worker_id,
        worker_group_id=worker_group_id,
        worker_group_count=worker_group_count,
    )
    health = ConnectionHealthTracker(timeout_seconds=60.0)
    last_error: str | None = None

    pool = snowflake_pool.get_default_pool()
    try:
        await pool.initialize()
        health.record_success()
    except Exception as exc:
        logger.error("Failed to initialize Snowflake pool: %s", exc)
        return 2

    try:
        run_status = await _fetch_run_status(run_id)
        health.record_success()
    except Exception as exc:
        logger.error("Failed to read RUN_STATUS: %s", exc)
        return 2
    if not run_status:
        logger.error("RUN_STATUS not found for run_id=%s", run_id)
        return 1

    run_status_phase = str(run_status.get("phase") or "").upper()
    run_status_status = str(run_status.get("status") or "").upper()
    run_config = _parse_variant(run_status.get("scenario_config"))
    template_config = _parse_variant(run_config.get("template_config"))
    template_id = str(run_config.get("template_id") or "").strip()
    template_name = str(run_config.get("template_name") or "").strip()
    if not template_name:
        template_name = str(run_status.get("test_name") or "")

    if not template_id:
        logger.error("SCENARIO_CONFIG missing template_id for run_id=%s", run_id)
        return 1

    meta = await _fetch_template_meta(template_id)
    health.record_success()
    if not template_name:
        template_name = str(meta.get("template_name") or "")

    try:
        scenario = registry._scenario_from_template_config(
            template_name, template_config
        )
    except Exception as exc:
        logger.error("Failed to build scenario: %s", exc)
        last_error = str(exc)
        try:
            await results_store.upsert_worker_heartbeat(
                run_id=cfg.run_id,
                worker_id=cfg.worker_id,
                worker_group_id=cfg.worker_group_id,
                status="DEAD",
                phase=run_status_phase,
                active_connections=0,
                target_connections=0,
                cpu_percent=None,
                memory_percent=None,
                queries_processed=0,
                error_count=1,
                last_error=last_error,
            )
        except Exception:
            pass
        return 1
    scenario_data = scenario.model_dump(mode="json")
    workload_cfg = _parse_variant(run_config.get("workload"))
    scaling_cfg = _parse_variant(run_config.get("scaling"))
    find_max_cfg = _parse_variant(run_config.get("find_max"))
    per_worker_connections = scaling_cfg.get("per_worker_connections")
    if per_worker_connections is None:
        per_worker_connections = scaling_cfg.get("per_worker_capacity")
    min_connections = _coerce_int(scaling_cfg.get("min_connections"), default=1)

    load_mode = str(
        workload_cfg.get("load_mode") or scenario_data.get("load_mode") or "CONCURRENCY"
    ).upper()
    total_connections = _coerce_int(
        workload_cfg.get("concurrent_connections"),
        default=int(scenario_data.get("concurrent_connections") or 1),
    )
    initial_target = _compute_initial_target(
        total_connections=total_connections,
        worker_group_id=worker_group_id,
        worker_group_count=worker_group_count,
        per_worker_connections=(
            _coerce_int(per_worker_connections, default=0)
            if per_worker_connections is not None
            else None
        ),
    )
    if load_mode == "QPS":
        initial_target = max(1, int(min_connections))
    per_worker_cap = _resolve_per_worker_cap(
        load_mode=load_mode,
        total_connections=total_connections,
        initial_target=initial_target,
        per_worker_connections=(
            _coerce_int(per_worker_connections, default=0)
            if per_worker_connections is not None
            else None
        ),
    )

    target_qps = workload_cfg.get("target_qps", scenario_data.get("target_qps"))
    per_worker_target_qps = target_qps
    if load_mode == "QPS" and target_qps is not None:
        try:
            per_worker_target_qps = float(target_qps) / float(
                max(1, worker_group_count)
            )
        except Exception:
            per_worker_target_qps = target_qps

    scenario_data.update(
        {
            "duration_seconds": _coerce_int(
                workload_cfg.get("duration_seconds"),
                default=int(scenario_data.get("duration_seconds") or 0),
            ),
            "warmup_seconds": _coerce_int(
                workload_cfg.get("warmup_seconds"),
                default=int(scenario_data.get("warmup_seconds") or 0),
            ),
            "concurrent_connections": int(per_worker_cap),
            "load_mode": load_mode,
            "target_qps": per_worker_target_qps,
            "min_connections": int(min_connections),
            "workload_type": workload_cfg.get(
                "workload_type", scenario_data.get("workload_type")
            ),
            "think_time_ms": _coerce_int(
                workload_cfg.get("think_time_ms"),
                default=int(scenario_data.get("think_time_ms") or 0),
            ),
            "operations_per_connection": workload_cfg.get(
                "operations_per_connection",
                scenario_data.get("operations_per_connection"),
            ),
            "read_query_templates": workload_cfg.get(
                "read_query_templates", scenario_data.get("read_query_templates")
            ),
            "read_batch_size": _coerce_int(
                workload_cfg.get("read_batch_size"),
                default=int(scenario_data.get("read_batch_size") or 0),
            ),
            "point_lookup_ratio": _coerce_float(
                workload_cfg.get("point_lookup_ratio"),
                default=float(scenario_data.get("point_lookup_ratio") or 0.0),
            ),
            "write_batch_size": _coerce_int(
                workload_cfg.get("write_batch_size"),
                default=int(scenario_data.get("write_batch_size") or 0),
            ),
            "update_ratio": _coerce_float(
                workload_cfg.get("update_ratio"),
                default=float(scenario_data.get("update_ratio") or 0.0),
            ),
            "delete_ratio": _coerce_float(
                workload_cfg.get("delete_ratio"),
                default=float(scenario_data.get("delete_ratio") or 0.0),
            ),
            "custom_queries": workload_cfg.get(
                "custom_queries", scenario_data.get("custom_queries")
            ),
            "worker_group_id": int(worker_group_id),
            "worker_group_count": int(worker_group_count),
            "start_concurrency": _coerce_int(
                find_max_cfg.get("start_concurrency"),
                default=int(scenario_data.get("start_concurrency") or 1),
            ),
            "concurrency_increment": _coerce_int(
                find_max_cfg.get("concurrency_increment"),
                default=int(scenario_data.get("concurrency_increment") or 1),
            ),
            "step_duration_seconds": _coerce_int(
                find_max_cfg.get("step_duration_seconds"),
                default=int(scenario_data.get("step_duration_seconds") or 1),
            ),
            "qps_stability_pct": _coerce_float(
                find_max_cfg.get("qps_stability_pct"),
                default=float(scenario_data.get("qps_stability_pct") or 5.0),
            ),
            "latency_stability_pct": _coerce_float(
                find_max_cfg.get("latency_stability_pct"),
                default=float(scenario_data.get("latency_stability_pct") or 20.0),
            ),
            "max_error_rate_pct": _coerce_float(
                find_max_cfg.get("max_error_rate_pct"),
                default=float(scenario_data.get("max_error_rate_pct") or 1.0),
            ),
        }
    )

    scenario = TestScenario.model_validate(scenario_data)
    scenario.collect_query_history = True

    executor = TestExecutor(scenario)
    executor.test_id = uuid4()  # Worker's own unique ID; RUN_ID links to parent
    executor._template_id = template_id  # type: ignore[attr-defined]
    executor._template_config = template_config  # type: ignore[attr-defined]

    benchmark_query_tag_base = f"unistore_benchmark:test_id={executor.test_id}"
    benchmark_query_tag_warmup = f"{benchmark_query_tag_base}:phase=WARMUP"
    benchmark_query_tag_running = f"{benchmark_query_tag_base}:phase=RUNNING"
    executor._benchmark_query_tag_base = benchmark_query_tag_base  # type: ignore[attr-defined]
    executor._benchmark_query_tag = benchmark_query_tag_warmup
    executor._benchmark_query_tag_running = benchmark_query_tag_running  # type: ignore[attr-defined]

    table_type = str(template_config.get("table_type") or "STANDARD").strip().upper()
    is_postgres = table_type in {
        TableType.POSTGRES.value.upper(),
        TableType.SNOWFLAKE_POSTGRES.value.upper(),
    }
    warehouse = None
    if not is_postgres:
        try:
            warehouse = registry._warehouse_from_config(template_config)
        except Exception as exc:
            logger.error("Warehouse selection failed: %s", exc)
            last_error = str(exc)
            try:
                await results_store.upsert_worker_heartbeat(
                    run_id=cfg.run_id,
                    worker_id=cfg.worker_id,
                    worker_group_id=cfg.worker_group_id,
                    status="DEAD",
                    phase=run_status_phase,
                    active_connections=0,
                    target_connections=0,
                    cpu_percent=None,
                    memory_percent=None,
                    queries_processed=0,
                    error_count=1,
                    last_error=last_error,
                )
            except Exception:
                pass
            return 1

    raw_use_cached = template_config.get("use_cached_result")
    if raw_use_cached is None:
        use_cached_result = True
    elif isinstance(raw_use_cached, bool):
        use_cached_result = raw_use_cached
    elif isinstance(raw_use_cached, (int, float)):
        use_cached_result = bool(raw_use_cached)
    elif isinstance(raw_use_cached, str):
        use_cached_result = raw_use_cached.strip().lower() not in {
            "0",
            "false",
            "no",
            "off",
        }
    else:
        use_cached_result = True

    tags_value = meta.get("tags")
    template_tags = tags_value if isinstance(tags_value, dict) else {}
    is_smoke = str(template_tags.get("smoke", "")).lower() == "true"
    if (
        not is_postgres
        and not is_smoke
        and warehouse
        and str(warehouse).upper() == str(settings.SNOWFLAKE_WAREHOUSE).upper()
    ):
        logger.error(
            "Template execution warehouse must not match results warehouse (SNOWFLAKE_WAREHOUSE)."
        )
        return 1

    if not is_postgres:
        max_workers = int(per_worker_cap)
        max_allowed = int(settings.SNOWFLAKE_BENCHMARK_EXECUTOR_MAX_WORKERS)
        if max_allowed > 0 and max_workers > max_allowed:
            logger.warning(
                "Requested concurrency (%d) exceeds SNOWFLAKE_BENCHMARK_EXECUTOR_MAX_WORKERS (%d).",
                int(max_workers),
                int(max_allowed),
            )

        initial_pool = max(1, min(max_workers, max(1, int(initial_target or 1))))
        overflow = max(0, max_workers - initial_pool)
        logger.info(
            "[benchmark] Pool sizing: total_connections=%d, initial_target=%d, per_worker_cap=%d, "
            "max_workers=%d, initial_pool=%d, overflow=%d, load_mode=%s",
            total_connections,
            initial_target,
            per_worker_cap,
            max_workers,
            initial_pool,
            overflow,
            load_mode,
        )
        bench_executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="sf-bench"
        )
        per_test_pool = snowflake_pool.SnowflakeConnectionPool(
            account=settings.SNOWFLAKE_ACCOUNT,
            user=settings.SNOWFLAKE_USER,
            password=settings.SNOWFLAKE_PASSWORD,
            warehouse=warehouse,
            database=settings.SNOWFLAKE_DATABASE,
            schema=settings.SNOWFLAKE_SCHEMA,
            role=settings.SNOWFLAKE_ROLE,
            pool_size=initial_pool,
            max_overflow=overflow,
            timeout=settings.SNOWFLAKE_POOL_TIMEOUT,
            recycle=settings.SNOWFLAKE_POOL_RECYCLE,
            executor=bench_executor,
            owns_executor=True,
            max_parallel_creates=settings.SNOWFLAKE_POOL_MAX_PARALLEL_CREATES,
            connect_login_timeout=settings.SNOWFLAKE_BENCHMARK_CONNECT_LOGIN_TIMEOUT,
            connect_network_timeout=settings.SNOWFLAKE_BENCHMARK_CONNECT_NETWORK_TIMEOUT,
            connect_socket_timeout=settings.SNOWFLAKE_BENCHMARK_CONNECT_SOCKET_TIMEOUT,
            session_parameters={
                "USE_CACHED_RESULT": "TRUE" if use_cached_result else "FALSE",
                "QUERY_TAG": benchmark_query_tag_warmup,
            },
            pool_name="benchmark",
        )
        executor._snowflake_pool_override = per_test_pool  # type: ignore[attr-defined]

    current_phase = run_status_phase or "PREPARING"
    current_target = int(initial_target)
    last_seen_sequence = 0
    ramp = TargetRamp()

    async def _safe_heartbeat(status: str) -> None:
        nonlocal last_error
        resources = _extract_resources(executor.metrics)
        cpu_percent = resources.get("cgroup_cpu_percent") or resources.get(
            "host_cpu_percent"
        )
        memory_percent = resources.get("cgroup_memory_percent") or resources.get(
            "host_memory_percent"
        )
        if last_error and len(last_error) > 1000:
            last_error = last_error[:1000] + "...(truncated)"
        phase_value = _normalize_phase(current_phase)
        try:
            await results_store.upsert_worker_heartbeat(
                run_id=cfg.run_id,
                worker_id=cfg.worker_id,
                worker_group_id=cfg.worker_group_id,
                status=status,
                phase=phase_value,
                active_connections=executor.metrics.active_connections,
                target_connections=current_target,
                cpu_percent=cpu_percent
                if cpu_percent is not None
                else executor.metrics.cpu_percent,
                memory_percent=memory_percent,
                queries_processed=executor.metrics.total_operations,
                error_count=executor.metrics.failed_operations,
                last_error=last_error,
            )
            health.record_success()
        except Exception as exc:
            last_error = str(exc)

    await _safe_heartbeat("STARTING")

    # =========================================================================
    # CRITICAL: Initialize benchmark pool BEFORE waiting for START.
    # This ensures workers are ready to execute queries immediately when warmup
    # begins, rather than spending the first 15-20s of warmup on pool init.
    # =========================================================================
    if not is_postgres:
        try:
            pool_override = getattr(executor, "_snowflake_pool_override", None)
            if pool_override is not None:
                await _safe_heartbeat("INITIALIZING")
                logger.info("Initializing benchmark connection pool...")
                await pool_override.initialize()
                logger.info("Benchmark connection pool initialized")
                health.record_success()
        except Exception as exc:
            logger.error("Benchmark pool initialization failed: %s", exc)
            last_error = str(exc)
            await _safe_heartbeat("DEAD")
            return 2

    # Setup executor (table profiling, value pools, etc.) before START
    logger.info("Setting up executor...")
    ok = await executor.setup()
    if not ok:
        last_error = str(getattr(executor, "_setup_error", "Setup failed"))
        logger.error("Executor setup failed: %s", last_error)
        await _safe_heartbeat("DEAD")
        return 1
    logger.info("Executor setup complete - ready for START")
    await _safe_heartbeat("READY")  # Signal orchestrator that pool init is complete

    terminal_status: str | None = None

    background_tasks: set[asyncio.Task] = set()

    def _track_task(task: asyncio.Task) -> None:
        background_tasks.add(task)

        def _done(t: asyncio.Task) -> None:
            background_tasks.discard(task)
            try:
                t.exception()
            except asyncio.CancelledError:
                return
            except Exception:
                return

        task.add_done_callback(_done)

    test_id_str = str(executor.test_id)
    CURRENT_TEST_ID.set(test_id_str)
    CURRENT_WORKER_ID.set(cfg.worker_id)
    log_queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)
    log_handler = TestLogQueueHandler(test_id=test_id_str, queue=log_queue)
    logging.getLogger().addHandler(log_handler)

    async def _drain_log_queue() -> None:
        while True:
            try:
                batch: list[dict[str, Any]] = []
                try:
                    while len(batch) < 100:
                        event = log_queue.get_nowait()
                        batch.append(event)
                except asyncio.QueueEmpty:
                    pass

                if batch:
                    try:
                        await results_store.insert_test_logs(rows=batch)
                    except Exception as exc:
                        logger.warning(
                            "Failed to persist %d logs for worker %s: %s",
                            len(batch),
                            cfg.worker_id,
                            exc,
                        )

                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                remaining: list[dict[str, Any]] = []
                try:
                    while True:
                        event = log_queue.get_nowait()
                        remaining.append(event)
                except asyncio.QueueEmpty:
                    pass
                if remaining:
                    try:
                        await results_store.insert_test_logs(rows=remaining)
                    except Exception:
                        pass
                raise

    log_drain_task = asyncio.create_task(
        _drain_log_queue(), name=f"log-drain-{cfg.worker_id}"
    )
    _track_task(log_drain_task)

    async def _stop_log_streaming() -> None:
        if log_drain_task is not None:
            log_drain_task.cancel()
            try:
                await log_drain_task
            except asyncio.CancelledError:
                pass
            except Exception:
                pass
        logging.getLogger().removeHandler(log_handler)

    def _metrics_callback(metrics: Metrics) -> None:
        nonlocal current_phase
        custom = dict(metrics.custom_metrics or {})
        custom["phase"] = current_phase
        metrics.custom_metrics = custom
        _track_task(
            asyncio.create_task(
                results_store.insert_worker_metrics_snapshot(
                    run_id=cfg.run_id,
                    test_id=str(executor.test_id),
                    worker_id=cfg.worker_id,
                    worker_group_id=cfg.worker_group_id,
                    worker_group_count=cfg.worker_group_count,
                    metrics=metrics,
                    phase=_normalize_phase(current_phase),
                    target_connections=current_target,
                )
            )
        )

    executor.set_metrics_callback(_metrics_callback)
    metrics_task = asyncio.create_task(executor._collect_metrics())
    _track_task(metrics_task)

    async def _wait_for_start(timeout_seconds: int = 120) -> bool:
        nonlocal current_phase, last_seen_sequence, terminal_status
        start_mono = time.monotonic()
        last_poll = 0.0
        last_heartbeat_wait = 0.0
        while True:
            if not health.ok():
                return False
            elapsed = time.monotonic() - start_mono
            if elapsed > float(timeout_seconds):
                return False

            try:
                status_row = await _fetch_run_status(cfg.run_id)
                health.record_success()
            except Exception as exc:
                logger.warning("RUN_STATUS poll failed: %s", exc)
                status_row = None

            if status_row:
                status = str(status_row.get("status") or "").upper()
                phase = str(status_row.get("phase") or "").upper()
                logger.info("_wait_for_start: status=%s, phase=%s", status, phase)
                if phase:
                    current_phase = phase
                if status == "RUNNING":
                    logger.info("_wait_for_start: status=RUNNING, proceeding!")
                    return True
                if status in {"COMPLETED", "FAILED", "CANCELLED"}:
                    logger.warning("Run already terminal: %s", status)
                    terminal_status = status
                    return False

            now = time.monotonic()
            if now - last_poll >= 1.0:
                last_poll = now
                try:
                    events = await _fetch_control_events(
                        run_id=cfg.run_id,
                        last_seen_sequence=last_seen_sequence,
                        limit=50,
                    )
                    health.record_success()
                except Exception as exc:
                    logger.warning("RUN_CONTROL_EVENTS poll failed: %s", exc)
                    events = []

                for event in events:
                    seq = int(event.get("sequence_id") or 0)
                    if seq <= last_seen_sequence:
                        continue
                    last_seen_sequence = seq
                    event_type = event.get("event_type")
                    if event_type == "STOP":
                        terminal_status = "CANCELLED"
                        return False

            if now - last_heartbeat_wait >= 1.0:
                last_heartbeat_wait = now
                await _safe_heartbeat(
                    "READY"
                )  # Stay READY until orchestrator sets RUNNING
            await asyncio.sleep(0.2)

    if run_status_status in {"COMPLETED", "FAILED", "CANCELLED"}:
        logger.warning("Run already terminal (%s); exiting", run_status_status)
        await _safe_heartbeat("COMPLETED")
        await _stop_log_streaming()
        return 0

    logger.info("Calling _wait_for_start (timeout=120s)...")
    if not await _wait_for_start(timeout_seconds=120):
        if terminal_status in {"COMPLETED", "FAILED", "CANCELLED"}:
            logger.warning(
                "_wait_for_start returned False, terminal_status=%s", terminal_status
            )
            await _safe_heartbeat("COMPLETED")
            await _stop_log_streaming()
            return 0
        last_error = "Timeout waiting for START"
        logger.error("_wait_for_start timed out!")
        await _safe_heartbeat("DEAD")
        await _stop_log_streaming()
        return 3

    logger.info("_wait_for_start returned True - starting benchmark execution!")
    await _safe_heartbeat("RUNNING")

    table_cfg = scenario.table_configs[0] if scenario.table_configs else None
    # NOTE: warehouse_config_snapshot is already captured by the orchestrator during
    # PREPARING phase (orchestrator.py:440) and stored in TEST_RESULTS. Workers don't
    # need to fetch it again - it was redundant and added ~200-500ms latency here.

    try:
        await results_store.insert_test_start(
            test_id=str(executor.test_id),
            run_id=cfg.run_id,
            test_name=str(template_name or "worker"),
            scenario=scenario,
            table_name=table_cfg.name if table_cfg else "unknown",
            table_type=str(table_cfg.table_type).upper() if table_cfg else "unknown",
            warehouse=warehouse,
            warehouse_size=str(template_config.get("warehouse_size") or ""),
            template_id=str(template_id),
            template_name=str(template_name or "worker"),
            template_config=template_config,
            warehouse_config_snapshot=None,  # Captured by orchestrator during PREPARING
            query_tag=benchmark_query_tag_base,
        )
        health.record_success()
    except Exception as exc:
        logger.error("Failed to insert TEST_RESULTS row: %s", exc)
        last_error = str(exc)
        await _safe_heartbeat("DEAD")
        return 1

    executor.status = TestStatus.RUNNING
    executor.start_time = datetime.now(UTC)
    executor.metrics.timestamp = executor.start_time

    start_in_measurement = run_status_phase == "MEASUREMENT"

    # NOTE: Pool initialization and executor.setup() now happen BEFORE _wait_for_start()
    # to ensure workers are ready when warmup begins (see "CRITICAL" block above).

    worker_tasks: dict[int, tuple[asyncio.Task, asyncio.Event]] = {}
    next_worker_id = 0
    scale_lock = asyncio.Lock()

    def _prune_workers() -> None:
        for wid, (task, _) in list(worker_tasks.items()):
            if task.done():
                worker_tasks.pop(wid, None)

    async def _spawn_one(*, warmup: bool) -> None:
        nonlocal next_worker_id
        wid = int(next_worker_id)
        next_worker_id += 1
        stop_signal = asyncio.Event()
        task = asyncio.create_task(
            executor._controlled_worker(
                worker_id=wid, warmup=warmup, stop_signal=stop_signal
            )
        )
        worker_tasks[wid] = (task, stop_signal)

    async def _scale_to(target: int, *, warmup: bool) -> None:
        nonlocal current_target
        async with scale_lock:
            _prune_workers()
            target = max(0, int(target))
            if target > per_worker_cap:
                logger.warning(
                    "Target %d exceeds per-worker cap %d; clamping",
                    target,
                    per_worker_cap,
                )
                target = int(per_worker_cap)
            current_target = target
            executor._target_workers = int(target)
            running = len(worker_tasks)
            if running < target:
                spawn_n = target - running
                for _ in range(spawn_n):
                    await _spawn_one(warmup=warmup)
            elif running > target:
                stop_n = running - target
                stop_ids = sorted(worker_tasks.keys(), reverse=True)[:stop_n]
                for wid in stop_ids:
                    _, stop_signal = worker_tasks.get(wid, (None, None))
                    if isinstance(stop_signal, asyncio.Event):
                        stop_signal.set()

    async def _stop_all(*, timeout_seconds: float) -> None:
        async with scale_lock:
            for _, stop_signal in worker_tasks.values():
                stop_signal.set()
        tasks = [t for t, _ in worker_tasks.values()]
        if not tasks:
            return
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=timeout_seconds,
            )
        except asyncio.TimeoutError:
            logger.warning("Timed out draining workers after %.1fs", timeout_seconds)
            for t in tasks:
                t.cancel()

    logger.info(
        "Scaling to initial target=%d workers (phase=%s)", current_target, current_phase
    )
    await _scale_to(current_target, warmup=(current_phase == "WARMUP"))
    logger.info("Initial scale complete, entering main event loop")

    last_heartbeat = time.monotonic()
    last_reconcile = time.monotonic()
    last_event_poll = 0.0
    stop_requested = False
    drain_timeout = 120.0
    exit_status = TestStatus.COMPLETED

    async def _transition_to_measurement() -> None:
        await executor._transition_to_measurement_phase()
        async with executor._metrics_lock:
            executor._metrics_epoch += 1
            executor._measurement_active = True
            executor.metrics = Metrics()
            executor.metrics.timestamp = datetime.now(UTC)
            executor._measurement_start_time = datetime.now(UTC)
            executor._last_snapshot_time = None
            executor._last_snapshot_mono = None
            executor._last_snapshot_ops = 0
            executor._qps_smoothed = None
            executor._qps_windowed = None
            executor._qps_samples.clear()
            executor._qps_controller_state = {}
            executor._latencies_ms.clear()
            executor._sql_error_categories.clear()
            if hasattr(executor, "_latency_sf_execution_ms"):
                executor._latency_sf_execution_ms.clear()
            if hasattr(executor, "_latency_network_overhead_ms"):
                executor._latency_network_overhead_ms.clear()

    if start_in_measurement:
        await _transition_to_measurement()
        current_phase = "MEASUREMENT"

    try:
        while True:
            if not health.ok():
                last_error = "No successful Snowflake operation for 60s"
                exit_status = TestStatus.FAILED
                break

            now = time.monotonic()

            if ramp.active and not stop_requested:
                target = ramp.current_target()
                if target is not None and target != current_target:
                    await _scale_to(target, warmup=(current_phase == "WARMUP"))

            if now - last_event_poll >= 1.0:
                last_event_poll = now
                try:
                    events = await _fetch_control_events(
                        run_id=cfg.run_id,
                        last_seen_sequence=last_seen_sequence,
                        limit=100,
                    )
                    health.record_success()
                except Exception as exc:
                    logger.warning("RUN_CONTROL_EVENTS poll failed: %s", exc)
                    events = []

                for event in events:
                    seq = int(event.get("sequence_id") or 0)
                    if seq <= last_seen_sequence:
                        continue
                    last_seen_sequence = seq
                    event_type = event.get("event_type")
                    event_data = event.get("event_data") or {}
                    scope = event_data.get("scope", "RUN")
                    if not _event_applies(scope=scope, event_data=event_data, cfg=cfg):
                        continue

                    if event_type == "STOP":
                        stop_requested = True
                        # Natural completion (duration_elapsed) = COMPLETED
                        # User cancellation or other reasons = CANCELLED
                        reason = event_data.get("reason", "")
                        exit_status = (
                            TestStatus.COMPLETED
                            if reason == "duration_elapsed"
                            else TestStatus.CANCELLED
                        )
                        drain_timeout = _coerce_float(
                            event_data.get("drain_timeout_seconds"), default=120.0
                        )
                        logger.info(
                            "STOP received (reason=%s, drain_timeout=%.1fs)",
                            reason or "unknown",
                            drain_timeout,
                        )
                        break  # Exit event loop immediately on STOP
                    elif event_type == "SET_PHASE":
                        new_phase = str(event_data.get("phase") or "").upper()
                        if new_phase and new_phase != current_phase:
                            if new_phase == "MEASUREMENT":
                                await _transition_to_measurement()
                            current_phase = new_phase
                            logger.info("Phase set to %s", current_phase)
                    elif event_type == "SET_WORKER_TARGET" and not stop_requested:
                        new_target = _coerce_int(
                            event_data.get("target_connections"),
                            default=current_target,
                        )
                        ramp_seconds = _coerce_float(
                            event_data.get("ramp_seconds"), default=0.0
                        )
                        if new_target < 1:
                            logger.warning("Received very low target: %s", new_target)
                        if ramp_seconds > 0:
                            ramp.start(
                                current=current_target,
                                target=new_target,
                                duration_seconds=ramp_seconds,
                            )
                        else:
                            await _scale_to(
                                new_target, warmup=(current_phase == "WARMUP")
                            )

                # Exit outer loop immediately after STOP event processed
                if stop_requested:
                    break

            if now - last_reconcile >= 5.0:
                last_reconcile = now
                try:
                    status_row = await _fetch_run_status(cfg.run_id)
                    health.record_success()
                except Exception as exc:
                    logger.warning("RUN_STATUS poll failed: %s", exc)
                    status_row = None

                if status_row:
                    status = str(status_row.get("status") or "").upper()
                    phase = str(status_row.get("phase") or "").upper()
                    if phase and phase != current_phase:
                        if phase == "MEASUREMENT":
                            await _transition_to_measurement()
                        current_phase = phase
                    if status in {"COMPLETED", "FAILED", "CANCELLED"}:
                        stop_requested = True
                        exit_status = (
                            TestStatus.FAILED
                            if status == "FAILED"
                            else TestStatus.CANCELLED
                            if status == "CANCELLED"
                            else TestStatus.COMPLETED
                        )
                        break  # Exit reconciliation on terminal status

                    targets = _parse_variant(status_row.get("worker_targets"))
                    if isinstance(targets, dict) and not stop_requested:
                        target_entry = targets.get(cfg.worker_id)
                        if isinstance(target_entry, dict):
                            desired = _coerce_int(
                                target_entry.get("target_connections"),
                                default=current_target,
                            )
                            if desired != current_target:
                                await _scale_to(
                                    desired, warmup=(current_phase == "WARMUP")
                                )

            if now - last_heartbeat >= 1.0:
                last_heartbeat = now
                await _safe_heartbeat("DRAINING" if stop_requested else "RUNNING")

            if stop_requested:
                break

            await asyncio.sleep(0.05)

        # Stop metrics collection IMMEDIATELY when STOP is received.
        # Don't wait for drain - no point publishing QPS/latency stats during drain.
        executor._stop_event.set()
        if metrics_task is not None:
            metrics_task.cancel()
            try:
                await metrics_task
            except asyncio.CancelledError:
                pass
            metrics_task = None  # Prevent double-cancel in finally

        if stop_requested:
            await _safe_heartbeat("DRAINING")
        await _stop_all(timeout_seconds=drain_timeout)

    except Exception as exc:
        logger.exception("Worker crashed: %s", exc)
        last_error = str(exc)
        exit_status = TestStatus.FAILED
    finally:
        # Ensure metrics stopped even on exception path
        executor._stop_event.set()
        if metrics_task is not None:
            metrics_task.cancel()
            try:
                await metrics_task
            except asyncio.CancelledError:
                pass
        await _stop_log_streaming()

    executor.status = exit_status
    executor.end_time = datetime.now(UTC)
    result = await executor._build_result()
    find_max_result = getattr(executor, "_find_max_controller_state", None)
    try:
        await results_store.update_test_result_final(
            test_id=str(executor.test_id),
            result=result,
            find_max_result=find_max_result,
        )
    except Exception as exc:
        logger.warning("Failed to update TEST_RESULTS final row: %s", exc)

    # NOTE: Enrichment status (PENDING/SKIPPED) is set by the orchestrator in
    # _mark_run_completed(), not by workers. This avoids race conditions when
    # multiple workers complete around the same time.

    should_enrich = bool(getattr(scenario, "collect_query_history", False))
    persisted_rows: list[dict[str, Any]] = []
    try:
        records = executor.get_query_execution_records()
        if records:
            persist_all = should_enrich
            selected = records if persist_all else [r for r in records if r.warmup]
            if selected:
                persisted_rows = [
                    {
                        "execution_id": r.execution_id,
                        "query_id": r.query_id,
                        "query_text": r.query_text,
                        "start_time": r.start_time.isoformat(),
                        "end_time": r.end_time.isoformat(),
                        "duration_ms": r.duration_ms,
                        "rows_affected": r.rows_affected,
                        "bytes_scanned": r.bytes_scanned,
                        "warehouse": r.warehouse,
                        "success": r.success,
                        "error": r.error,
                        "connection_id": r.connection_id,
                        "custom_metadata": r.custom_metadata,
                        "query_kind": r.query_kind,
                        "worker_id": r.worker_id,
                        "warmup": r.warmup,
                        "app_elapsed_ms": r.app_elapsed_ms,
                    }
                    for r in selected
                ]
                await results_store.insert_query_executions(
                    test_id=str(executor.test_id), rows=persisted_rows
                )
                health.record_success()
    except Exception as exc:
        logger.warning("Failed to persist QUERY_EXECUTIONS: %s", exc)

    # NOTE: Enrichment (QUERY_HISTORY matching) is handled centrally by the
    # orchestrator as a background task after all workers complete. Worker just
    # sets ENRICHMENT_STATUS='PENDING' above and exits quickly.

    if exit_status in {TestStatus.COMPLETED, TestStatus.CANCELLED}:
        await _safe_heartbeat("COMPLETED")
    else:
        await _safe_heartbeat("DEAD")

    # Explicitly close connection pools to avoid 50+ second GC cleanup delay.
    # Without this, Python's event loop shutdown closes 100 connections serially.
    try:
        pool_override = getattr(executor, "_snowflake_pool_override", None)
        if pool_override is not None:
            await pool_override.close_all()
    except Exception as exc:
        logger.warning("Failed to close benchmark pool: %s", exc)
    try:
        await snowflake_pool.close_default_pool()
    except Exception as exc:
        logger.warning("Failed to close default pool: %s", exc)

    return 0 if exit_status in {TestStatus.COMPLETED, TestStatus.CANCELLED} else 1


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run a headless benchmark worker for multi-worker orchestration."
    )
    parser.add_argument("--run-id", help="Parent run ID (multi-worker control plane).")
    parser.add_argument(
        "--worker-id", help="Worker identifier (multi-worker control plane)."
    )
    parser.add_argument(
        "--worker-group-id",
        type=int,
        default=0,
        help="Worker group index for deterministic sharding.",
    )
    parser.add_argument(
        "--worker-group-count",
        type=int,
        default=1,
        help="Total number of worker groups.",
    )
    return parser


async def _run_worker(args: argparse.Namespace) -> int:
    if not args.run_id:
        logger.error("--run-id is required")
        return 1
    return await _run_worker_control_plane(args)


def main() -> int:
    parser = _build_parser()
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    try:
        return asyncio.run(_run_worker(args))
    except KeyboardInterrupt:
        print("[worker] interrupted", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
