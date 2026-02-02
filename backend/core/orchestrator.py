from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from backend.config import settings
from backend.connectors import snowflake_pool
from backend.core import results_store
from backend.core.results_store import (
    enrich_query_executions_with_retry,
    insert_test_logs,
    update_enrichment_status,
    update_test_overhead_percentiles,
)
from backend.core.test_log_stream import (
    CURRENT_TEST_ID,
    CURRENT_WORKER_ID,
    TestLogQueueHandler,
)
from backend.models.test_config import TableType, TestScenario

logger = logging.getLogger(__name__)


def _uv_available() -> str | None:
    """Check if uv is available and return the path, or None if not found."""
    uv_bin = shutil.which("uv")
    return uv_bin


def build_worker_targets(
    total_target: int,
    worker_group_count: int,
    *,
    per_worker_cap: int | None = None,
    min_threads_per_worker: int | None = None,
    max_threads_per_worker: int | None = None,
    load_mode: str | None = None,
    target_qps_total: float | None = None,
) -> tuple[int, dict[str, dict[str, Any]]]:
    """
    Distribute total_target threads evenly across workers.

    Algorithm (2.10 Target Allocation):
    - base = total_target // worker_group_count
    - remainder = total_target % worker_group_count
    - Workers with group_id < remainder get base + 1
    - If per_worker_cap/max_threads_per_worker is set, clamp total to max achievable
    - Never drop below min_threads_per_worker floor when provided

    Args:
        total_target: Total threads to distribute.
        worker_group_count: Number of workers.
        per_worker_cap: Optional per-worker ceiling (clamps total if exceeded).
        min_threads_per_worker: Optional per-worker floor (clamps total if below).
        max_threads_per_worker: Optional per-worker ceiling (takes precedence over per_worker_cap).
        load_mode: Optional load mode (if "QPS", includes per-worker QPS target).
        target_qps_total: Optional total QPS target (distributed evenly in QPS mode).

    Returns:
        Tuple of (effective_total, targets_dict) where targets_dict is:
        {"worker-0": {"target_threads": N, "worker_group_id": 0, ...}, ...}
    """
    target_total = max(0, int(total_target))
    worker_count = max(1, int(worker_group_count))

    effective_cap = per_worker_cap
    if max_threads_per_worker is not None:
        effective_cap = (
            min(int(max_threads_per_worker), int(per_worker_cap))
            if per_worker_cap is not None
            else int(max_threads_per_worker)
        )
    if effective_cap is not None and int(effective_cap) > 0:
        max_total = int(effective_cap) * worker_count
        if target_total > max_total:
            logger.warning(
                "Target %d exceeds per-worker cap; clamping to %d",
                target_total,
                max_total,
            )
            target_total = max_total

    if min_threads_per_worker is not None and int(min_threads_per_worker) > 0:
        min_total = int(min_threads_per_worker) * worker_count
        if target_total < min_total:
            logger.warning(
                "Target %d below min_threads_per_worker floor; clamping to %d",
                target_total,
                min_total,
            )
            target_total = min_total

    base = target_total // worker_count
    remainder = target_total % worker_count
    per_worker_qps = None
    if load_mode == "QPS" and target_qps_total is not None:
        per_worker_qps = float(target_qps_total) / float(worker_count)

    targets: dict[str, dict[str, Any]] = {}
    for idx in range(worker_count):
        target = base + (1 if idx < remainder else 0)
        entry: dict[str, Any] = {
            "target_threads": int(target),
            "worker_group_id": int(idx),
        }
        if per_worker_qps is not None:
            entry["target_qps"] = float(per_worker_qps)
        targets[f"worker-{idx}"] = entry

    return target_total, targets


@dataclass
class RunContext:
    """Context for an active run managed by the orchestrator."""

    run_id: str
    worker_group_count: int
    template_id: str
    scenario_config: dict[str, Any]
    poll_task: asyncio.Task | None = None
    worker_procs: list[asyncio.subprocess.Process] = field(default_factory=list)
    started_at: datetime | None = None
    stopping: bool = False
    log_queue: asyncio.Queue | None = None
    log_handler: logging.Handler | None = None
    log_drain_task: asyncio.Task | None = None


class OrchestratorService:
    """
    Central control plane for benchmark runs.
    Replaces the in-memory TestRegistry with a Snowflake-backed state machine.
    """

    def __init__(self) -> None:
        self._pool = snowflake_pool.get_default_pool()
        self._background_tasks: set[asyncio.Task] = set()
        self._active_runs: dict[str, RunContext] = {}

    async def _run_fail_fast_checks(self, *, template_id: str | None = None) -> None:
        prefix = f"{settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}"
        required_tables = (
            "RUN_STATUS",
            "TEST_RESULTS",
            "RUN_CONTROL_EVENTS",
            "WORKER_HEARTBEATS",
            "WORKER_METRICS_SNAPSHOTS",
        )
        try:
            await self._pool.execute_query("SELECT 1")
        except Exception as exc:
            raise ValueError("Failed to connect to Snowflake results store") from exc

        for table in required_tables:
            try:
                await self._pool.execute_query(
                    f"SELECT 1 FROM {prefix}.{table} LIMIT 1"
                )
            except Exception as exc:
                raise ValueError(f"Missing required table {prefix}.{table}") from exc

        if template_id:
            template_id = str(template_id).strip()
        if template_id:
            try:
                rows = await self._pool.execute_query(
                    f"""
                    SELECT 1
                    FROM {prefix}.TEST_TEMPLATES
                    WHERE TEMPLATE_ID = ?
                    LIMIT 1
                    """,
                    params=[template_id],
                )
            except Exception as exc:
                raise ValueError(
                    f"Template lookup failed for {template_id} in {prefix}.TEST_TEMPLATES"
                ) from exc
            if not rows:
                raise ValueError(
                    f"Template {template_id} not found in {prefix}.TEST_TEMPLATES"
                )

    async def create_run(
        self, template_id: str, template_config: dict[str, Any], scenario: TestScenario
    ) -> str:
        """
        Creates a new parent run in PREPARED state.
        Writes to RUN_STATUS table.

        Args:
            template_id: The source template ID
            template_config: The full resolved configuration
            scenario: The parsed scenario model

        Returns:
            str: The newly created RUN_ID (UUID)
        """
        import uuid

        run_id = str(uuid.uuid4())
        if not scenario.table_configs:
            raise ValueError("Scenario must include at least one table_config.")

        await self._run_fail_fast_checks(template_id=str(template_id))

        table_cfg = scenario.table_configs[0]
        table_type_raw = getattr(table_cfg.table_type, "value", table_cfg.table_type)
        table_type = str(table_type_raw).upper()
        is_postgres = table_type in {
            TableType.POSTGRES.value.upper(),
            TableType.SNOWFLAKE_POSTGRES.value.upper(),
        }
        warehouse = None
        if not is_postgres:
            warehouse = str(template_config.get("warehouse_name") or "").strip()
            if not warehouse:
                raise ValueError(
                    "warehouse_name is required (select an existing warehouse)."
                )

        scaling_cfg = template_config.get("scaling")
        if not isinstance(scaling_cfg, dict):
            scaling_cfg = {}
        scaling_mode = str(scaling_cfg.get("mode") or "AUTO").strip().upper() or "AUTO"
        if scaling_mode not in {"AUTO", "BOUNDED", "FIXED"}:
            raise ValueError("scaling.mode must be AUTO, BOUNDED, or FIXED")

        def _coerce_optional_int(value: Any) -> int | None:
            if value is None:
                return None
            if isinstance(value, str) and not value.strip():
                return None
            try:
                v = int(value)
            except Exception:
                return None
            if v == -1:
                return None
            return v

        min_workers = int(_coerce_optional_int(scaling_cfg.get("min_workers")) or 1)
        max_workers = _coerce_optional_int(scaling_cfg.get("max_workers"))
        min_threads_per_worker = int(
            _coerce_optional_int(scaling_cfg.get("min_connections")) or 1
        )
        max_threads_per_worker = _coerce_optional_int(
            scaling_cfg.get("max_connections")
        )

        if min_workers < 1:
            raise ValueError("min_workers must be >= 1")
        if min_threads_per_worker < 1:
            raise ValueError("min_threads_per_worker must be >= 1")
        if max_workers is not None and max_workers < 1:
            raise ValueError("max_workers must be >= 1 or null")
        if max_threads_per_worker is not None and max_threads_per_worker < 1:
            raise ValueError("max_threads_per_worker must be >= 1 or null")
        if max_workers is not None and min_workers > max_workers:
            raise ValueError("min_workers must be <= max_workers")
        if (
            max_threads_per_worker is not None
            and min_threads_per_worker > max_threads_per_worker
        ):
            raise ValueError("min_threads_per_worker must be <= max_threads_per_worker")
        if scaling_mode == "FIXED":
            if scaling_cfg.get("min_workers") is None:
                raise ValueError("FIXED mode requires scaling.min_workers to be set")
            if scaling_cfg.get("min_connections") is None:
                raise ValueError(
                    "FIXED mode requires scaling.min_threads_per_worker to be set"
                )

        worker_group_count = int(
            template_config.get("worker_group_count")
            or template_config.get("worker_count")
            or getattr(scenario, "worker_group_count", 1)
            or 1
        )
        if worker_group_count < 1:
            worker_group_count = 1
        if scaling_mode == "FIXED":
            worker_group_count = min_workers
        else:
            if worker_group_count < min_workers:
                worker_group_count = min_workers
            if max_workers is not None and worker_group_count > max_workers:
                worker_group_count = max_workers

        worker_count = int(template_config.get("worker_count") or worker_group_count)
        if worker_count < 1:
            worker_count = worker_group_count
        if scaling_mode == "FIXED":
            worker_count = worker_group_count

        per_worker_threads = template_config.get("per_worker_connections")
        if per_worker_threads is None:
            per_worker_threads = template_config.get("per_worker_capacity")
        if per_worker_threads is not None:
            per_worker_threads = int(per_worker_threads)

        load_mode = str(getattr(scenario, "load_mode", "CONCURRENCY") or "CONCURRENCY")
        load_mode = load_mode.strip().upper()
        total_threads = int(scenario.total_threads)
        if scaling_mode == "FIXED" and load_mode == "CONCURRENCY":
            total_threads = int(min_workers) * int(min_threads_per_worker)

        def _resolve_effective_max_threads_per_worker() -> int | None:
            per_worker_cap = None
            if load_mode == "QPS":
                per_worker_cap = int(scenario.total_threads)
                if per_worker_cap == -1:
                    per_worker_cap = None
            elif per_worker_threads is not None:
                per_worker_cap = int(per_worker_threads)
            if max_threads_per_worker is not None:
                if per_worker_cap is None:
                    return int(max_threads_per_worker)
                return int(min(per_worker_cap, max_threads_per_worker))
            return per_worker_cap

        effective_max_threads_per_worker = _resolve_effective_max_threads_per_worker()
        if max_workers is not None and effective_max_threads_per_worker is not None:
            max_total = int(max_workers) * int(effective_max_threads_per_worker)
            if load_mode in {"CONCURRENCY", "FIND_MAX_CONCURRENCY"}:
                if total_threads > max_total:
                    raise ValueError(
                        f"Target threads {total_threads} unreachable with max {max_workers} workers × {effective_max_threads_per_worker} threads"
                    )
            elif load_mode == "QPS":
                target_qps_total = scenario.target_qps
                if target_qps_total and float(target_qps_total) > float(max_total):
                    raise ValueError(
                        f"Target QPS {target_qps_total} unreachable with max {max_workers} workers × {effective_max_threads_per_worker} threads"
                    )

        scenario_config: dict[str, Any] = {
            "template_id": template_id,
            "template_name": scenario.name,
            "template_config": template_config,
            "target": {
                "table_name": str(table_cfg.name),
                "table_type": table_type,
                "warehouse": warehouse,
                "database": str(table_cfg.database or ""),
                "schema": str(table_cfg.schema_name or ""),
            },
            "workload": {
                "load_mode": load_mode,
                "concurrent_connections": int(total_threads),
                "duration_seconds": int(scenario.duration_seconds),
                "warmup_seconds": int(scenario.warmup_seconds),
                "target_qps": (
                    float(scenario.target_qps)
                    if scenario.target_qps is not None
                    else None
                ),
                "workload_type": getattr(
                    scenario.workload_type, "value", scenario.workload_type
                ),
                "think_time_ms": int(scenario.think_time_ms),
                "operations_per_connection": scenario.operations_per_connection,
                "read_query_templates": scenario.read_query_templates,
                "read_batch_size": int(scenario.read_batch_size),
                "point_lookup_ratio": float(scenario.point_lookup_ratio),
                "write_batch_size": int(scenario.write_batch_size),
                "update_ratio": float(scenario.update_ratio),
                "delete_ratio": float(scenario.delete_ratio),
                "custom_queries": scenario.custom_queries,
            },
            "scaling": {
                "mode": scaling_mode,
                "worker_count": worker_count,
                "worker_group_count": worker_group_count,
                "min_workers": int(min_workers),
                "max_workers": int(max_workers) if max_workers is not None else None,
                "min_connections": int(min_threads_per_worker),
                "max_connections": int(max_threads_per_worker)
                if max_threads_per_worker is not None
                else None,
            },
            "find_max": {
                "enabled": load_mode == "FIND_MAX_CONCURRENCY",
                "start_concurrency": int(scenario.start_concurrency),
                "concurrency_increment": int(scenario.concurrency_increment),
                "step_duration_seconds": int(scenario.step_duration_seconds),
                "qps_stability_pct": float(scenario.qps_stability_pct),
                "latency_stability_pct": float(scenario.latency_stability_pct),
                "max_error_rate_pct": float(scenario.max_error_rate_pct),
            },
            "guardrails": {
                "max_cpu_percent": template_config.get("autoscale_max_cpu_percent"),
                "max_memory_percent": template_config.get(
                    "autoscale_max_memory_percent"
                ),
            },
        }
        if per_worker_threads is not None:
            scenario_config["scaling"]["per_worker_connections"] = per_worker_threads
            scenario_config["scaling"]["per_worker_capacity"] = per_worker_threads

        prefix = f"{settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}"
        await self._pool.execute_query(
            f"""
            INSERT INTO {prefix}.RUN_STATUS (
                RUN_ID,
                TEST_ID,
                TEMPLATE_ID,
                TEST_NAME,
                SCENARIO_CONFIG,
                STATUS,
                PHASE,
                TOTAL_WORKERS_EXPECTED,
                WORKERS_REGISTERED,
                WORKERS_ACTIVE,
                WORKERS_COMPLETED
            )
            SELECT
                ?, ?, ?, ?, PARSE_JSON(?), ?, ?, ?, ?, ?, ?
            """,
            params=[
                run_id,
                run_id,
                template_id,
                scenario.name,
                json.dumps(scenario_config),
                "PREPARED",
                "",  # Empty phase until test actually starts (column is NOT NULL)
                worker_group_count,
                0,
                0,
                0,
            ],
        )

        warehouse_snapshot = None
        if warehouse:
            warehouse_snapshot = await results_store.fetch_warehouse_config_snapshot(
                warehouse_name=str(warehouse)
            )

        await results_store.insert_test_prepare(
            test_id=run_id,
            run_id=run_id,
            test_name=str(scenario.name),
            scenario=scenario,
            table_name=str(table_cfg.name),
            table_type=table_type,
            warehouse=warehouse,
            warehouse_size=str(template_config.get("warehouse_size") or ""),
            template_id=str(template_id),
            template_name=str(scenario.name),
            template_config=template_config,
            warehouse_config_snapshot=warehouse_snapshot,
            query_tag=None,
        )

        logger.info("Created run %s for template %s", run_id, template_id)

        # Increment template usage count
        try:
            now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M:%S")
            await self._pool.execute_query(
                f"""
                UPDATE {prefix}.TEST_TEMPLATES
                SET USAGE_COUNT = COALESCE(USAGE_COUNT, 0) + 1,
                    LAST_USED_AT = '{now}'
                WHERE TEMPLATE_ID = ?
                """,
                params=[template_id],
            )
            logger.debug("Incremented usage count for template %s", template_id)
        except Exception as e:
            # Log but don't fail the run creation if usage tracking fails
            logger.warning(
                "Failed to increment usage count for template %s: %s", template_id, e
            )

        return run_id

    async def start_run(self, run_id: str) -> None:
        """
        Transitions run to RUNNING and spawns workers.

        Steps:
        1. Fetch SCENARIO_CONFIG from RUN_STATUS
        2. Update RUN_STATUS to RUNNING and set START_TIME
        3. Emit START event to RUN_CONTROL_EVENTS
        4. Spawn workers as local subprocesses
        5. Start background poll loop for this run
        """
        logger.info("Starting run %s", run_id)
        prefix = f"{settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}"

        # 1. Fetch SCENARIO_CONFIG and worker count from RUN_STATUS
        rows = await self._pool.execute_query(
            f"""
            SELECT SCENARIO_CONFIG, TOTAL_WORKERS_EXPECTED, TEMPLATE_ID, STATUS
            FROM {prefix}.RUN_STATUS
            WHERE RUN_ID = ?
            """,
            params=[run_id],
        )
        if not rows:
            raise ValueError(f"Run {run_id} not found in RUN_STATUS")

        scenario_config_raw, worker_count, template_id, current_status = rows[0]
        if current_status not in ("PREPARED", "STARTING"):
            raise ValueError(
                f"Run {run_id} is in status {current_status}, cannot start"
            )

        await self._run_fail_fast_checks(template_id=str(template_id or ""))

        # Parse scenario_config
        if isinstance(scenario_config_raw, str):
            scenario_config = json.loads(scenario_config_raw)
        elif isinstance(scenario_config_raw, dict):
            scenario_config = scenario_config_raw
        else:
            scenario_config = {}

        worker_group_count = int(worker_count or 1)

        # 2. Update RUN_STATUS to STARTING and set START_TIME immediately.
        # This ensures elapsed time includes the PREPARING phase (worker spawn/init).
        # Previously START_TIME was set when workers reported READY, which excluded
        # PREPARING time and caused frontend/backend timer mismatch.
        warmup_seconds = scenario_config.get("workload", {}).get("warmup_seconds", 0)
        initial_phase = "WARMUP" if warmup_seconds > 0 else "MEASUREMENT"

        await self._pool.execute_query(
            f"""
            UPDATE {prefix}.RUN_STATUS
            SET STATUS = 'STARTING',
                PHASE = 'PREPARING',
                START_TIME = CURRENT_TIMESTAMP(),
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE RUN_ID = ?
            """,
            params=[run_id],
        )

        # Also update the parent TEST_RESULTS row with START_TIME
        await self._pool.execute_query(
            f"""
            UPDATE {prefix}.TEST_RESULTS
            SET STATUS = 'STARTING',
                START_TIME = CURRENT_TIMESTAMP(),
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE TEST_ID = ? AND RUN_ID = ?
            """,
            params=[run_id, run_id],
        )

        # 3. Emit START event to RUN_CONTROL_EVENTS (triggers workers to begin)
        # Note: START_TIME was already set above to include PREPARING phase
        await self._emit_control_event(
            run_id=run_id,
            event_type="START",
            event_data={
                "scope": "RUN",
                "phase": initial_phase,  # Target phase after workers ready
                "workers_expected": worker_group_count,
                "timestamp": datetime.now(UTC).isoformat(),
            },
        )

        # Create run context
        ctx = RunContext(
            run_id=run_id,
            worker_group_count=worker_group_count,
            template_id=str(template_id or ""),
            scenario_config=scenario_config,
            started_at=datetime.now(UTC),
        )
        self._active_runs[run_id] = ctx

        # Setup log streaming: attach handler to root logger, start drain task
        log_queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)
        log_handler = TestLogQueueHandler(test_id=run_id, queue=log_queue)
        logging.getLogger().addHandler(log_handler)
        ctx.log_queue = log_queue
        ctx.log_handler = log_handler

        # Set the contextvar so logs are captured for this test
        CURRENT_TEST_ID.set(run_id)
        CURRENT_WORKER_ID.set("ORCHESTRATOR")

        # Start background task to drain log queue and persist to Snowflake
        drain_task = asyncio.create_task(
            self._drain_log_queue(run_id, log_queue), name=f"log-drain-{run_id}"
        )
        ctx.log_drain_task = drain_task
        self._background_tasks.add(drain_task)
        drain_task.add_done_callback(self._background_tasks.discard)

        # 4. Spawn workers as local subprocesses
        await self._spawn_workers(ctx)

        # 5. Start background poll loop for this run
        poll_task = asyncio.create_task(
            self._run_poll_loop(run_id), name=f"poll-{run_id}"
        )
        ctx.poll_task = poll_task
        self._background_tasks.add(poll_task)
        poll_task.add_done_callback(self._background_tasks.discard)

        logger.info(
            "Run %s started with %d workers, phase=%s",
            run_id,
            worker_group_count,
            initial_phase,
        )

    async def _emit_control_event(
        self, *, run_id: str, event_type: str, event_data: dict[str, Any]
    ) -> int:
        """
        Emit a control event to RUN_CONTROL_EVENTS with atomic SEQUENCE_ID.

        Returns the assigned SEQUENCE_ID.
        """
        prefix = f"{settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}"
        event_id = str(uuid.uuid4())

        # Get current sequence ID before incrementing
        # Note: Snowflake doesn't support UPDATE ... RETURNING, so we use SELECT + UPDATE
        rows = await self._pool.execute_query(
            f"""
            SELECT NEXT_SEQUENCE_ID
            FROM {prefix}.RUN_STATUS
            WHERE RUN_ID = ?
            """,
            params=[run_id],
        )

        if not rows:
            raise ValueError(f"Run {run_id} not found when emitting event")

        sequence_id = int(rows[0][0] or 0)

        # Increment the sequence ID for the next event
        await self._pool.execute_query(
            f"""
            UPDATE {prefix}.RUN_STATUS
            SET NEXT_SEQUENCE_ID = NEXT_SEQUENCE_ID + 1,
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE RUN_ID = ?
            """,
            params=[run_id],
        )

        sequence_id = int(rows[0][0])

        # Insert the control event
        await self._pool.execute_query(
            f"""
            INSERT INTO {prefix}.RUN_CONTROL_EVENTS (
                EVENT_ID, RUN_ID, EVENT_TYPE, EVENT_DATA, SEQUENCE_ID
            )
            SELECT ?, ?, ?, PARSE_JSON(?), ?
            """,
            params=[
                event_id,
                run_id,
                event_type,
                json.dumps(event_data),
                sequence_id,
            ],
        )

        logger.debug(
            "Emitted %s event for run %s (seq=%d)", event_type, run_id, sequence_id
        )
        return sequence_id

    async def _spawn_workers(self, ctx: RunContext) -> None:
        """
        Spawn worker subprocesses for local execution.
        Uses uv if available, falls back to python.
        """
        uv_bin = _uv_available()
        env = dict(os.environ)
        env["PYTHONUNBUFFERED"] = "1"

        for group_id in range(ctx.worker_group_count):
            worker_id = f"worker-{group_id}"

            # Build command
            if uv_bin:
                cmd = [
                    uv_bin,
                    "run",
                    "python",
                    "scripts/run_worker.py",
                ]
            else:
                cmd = ["python", "scripts/run_worker.py"]

            cmd.extend(
                [
                    "--run-id",
                    ctx.run_id,
                    "--worker-id",
                    worker_id,
                    "--worker-group-id",
                    str(group_id),
                    "--worker-group-count",
                    str(ctx.worker_group_count),
                ]
            )

            logger.info(
                "Spawning worker %s for run %s (group %d/%d)",
                worker_id,
                ctx.run_id,
                group_id + 1,
                ctx.worker_group_count,
            )

            proc = await asyncio.create_subprocess_exec(
                *cmd,
                env=env,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            ctx.worker_procs.append(proc)

        logger.info("Spawned %d workers for run %s", ctx.worker_group_count, ctx.run_id)

    async def _drain_log_queue(self, run_id: str, queue: asyncio.Queue) -> None:
        """
        Background task that drains the log queue and persists logs to Snowflake.
        Runs until the run completes or is cancelled.
        """
        while True:
            try:
                batch: list[dict[str, Any]] = []
                try:
                    while len(batch) < 100:
                        event = queue.get_nowait()
                        batch.append(event)
                except asyncio.QueueEmpty:
                    pass

                if batch:
                    try:
                        await insert_test_logs(rows=batch)
                    except Exception as e:
                        logger.warning(
                            "Failed to persist %d logs for run %s: %s",
                            len(batch),
                            run_id,
                            e,
                        )

                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                remaining: list[dict[str, Any]] = []
                try:
                    while True:
                        event = queue.get_nowait()
                        remaining.append(event)
                except asyncio.QueueEmpty:
                    pass
                if remaining:
                    try:
                        await insert_test_logs(rows=remaining)
                    except Exception:
                        pass
                raise

    async def _run_poll_loop(self, run_id: str) -> None:
        """
        Background poll loop for a specific run.

        Responsibilities (2.5):
        - Heartbeat scan (mark workers STALE/DEAD)
        - Guardrail checks (CPU/memory)
        - Metrics aggregation (measurement-phase only)
        - Update RUN_STATUS rollups and worker counts
        - Phase transitions (WARMUP -> MEASUREMENT)
        - STOP scheduling based on duration_seconds
        - Monitor worker process health
        """
        logger.info("Poll loop started for run %s", run_id)
        ctx = self._active_runs.get(run_id)
        if not ctx:
            logger.warning("Poll loop: run %s not found in active runs", run_id)
            return

        prefix = f"{settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}"
        workload_cfg = dict((ctx.scenario_config or {}).get("workload") or {})
        guardrails_cfg = dict((ctx.scenario_config or {}).get("guardrails") or {})
        target_cfg = dict((ctx.scenario_config or {}).get("target") or {})
        warmup_seconds = int(workload_cfg.get("warmup_seconds") or 0)
        duration_seconds = int(workload_cfg.get("duration_seconds") or 0)
        initial_phase = "WARMUP" if warmup_seconds > 0 else "MEASUREMENT"
        warehouse_name = str(target_cfg.get("warehouse") or "").strip().upper()
        logger.info(
            "Poll loop config for %s: warmup_seconds=%s, duration_seconds=%s, workload_cfg=%s",
            run_id,
            warmup_seconds,
            duration_seconds,
            workload_cfg,
        )

        def _coerce_optional_int(
            value: Any, *, allow_unbounded: bool = False
        ) -> int | None:
            if value is None:
                return None
            if isinstance(value, str) and not value.strip():
                return None
            try:
                out = int(value)
            except Exception:
                return None
            if allow_unbounded and out == -1:
                return None
            return out

        def _coerce_optional_float(value: Any) -> float | None:
            try:
                return float(value)
            except Exception:
                return None

        max_cpu_percent = _coerce_optional_float(guardrails_cfg.get("max_cpu_percent"))
        max_memory_percent = _coerce_optional_float(
            guardrails_cfg.get("max_memory_percent")
        )
        bounds_patience_intervals = 3

        scaling_cfg = dict((ctx.scenario_config or {}).get("scaling") or {})
        find_max_cfg = dict((ctx.scenario_config or {}).get("find_max") or {})
        load_mode = str(workload_cfg.get("load_mode") or "").strip().upper()
        scaling_mode = str(scaling_cfg.get("mode") or "AUTO").strip().upper() or "AUTO"
        if scaling_mode not in {"AUTO", "BOUNDED", "FIXED"}:
            scaling_mode = "AUTO"
        min_workers = _coerce_optional_int(scaling_cfg.get("min_workers")) or 1
        max_workers = _coerce_optional_int(
            scaling_cfg.get("max_workers"), allow_unbounded=True
        )
        min_threads_per_worker = (
            _coerce_optional_int(scaling_cfg.get("min_connections")) or 1
        )
        max_threads_per_worker = _coerce_optional_int(
            scaling_cfg.get("max_connections"), allow_unbounded=True
        )
        bounds_patience = (
            _coerce_optional_int(scaling_cfg.get("bounds_patience_intervals"))
            or bounds_patience_intervals
        )
        find_max_enabled = (
            bool(find_max_cfg.get("enabled")) and load_mode == "FIND_MAX_CONCURRENCY"
        )
        worker_group_count = max(1, int(ctx.worker_group_count or 1))

        per_worker_threads = _coerce_optional_int(
            scaling_cfg.get("per_worker_connections")
            if scaling_cfg.get("per_worker_connections") is not None
            else scaling_cfg.get("per_worker_capacity")
        )
        per_worker_cap = per_worker_threads
        if per_worker_cap is None:
            per_worker_cap = _coerce_optional_int(
                workload_cfg.get("concurrent_connections"), allow_unbounded=True
            )
        effective_max_threads_per_worker = per_worker_cap
        if max_threads_per_worker is not None:
            effective_max_threads_per_worker = (
                min(int(max_threads_per_worker), int(per_worker_cap))
                if per_worker_cap is not None
                else int(max_threads_per_worker)
            )
        find_max_start = (
            _coerce_optional_int(find_max_cfg.get("start_concurrency")) or 1
        )
        find_max_increment = (
            _coerce_optional_int(find_max_cfg.get("concurrency_increment")) or 1
        )
        find_max_step_seconds = (
            _coerce_optional_int(find_max_cfg.get("step_duration_seconds")) or 30
        )
        find_max_qps_stability_pct = float(find_max_cfg.get("qps_stability_pct") or 5.0)
        find_max_latency_stability_pct = float(
            find_max_cfg.get("latency_stability_pct") or 20.0
        )
        find_max_max_error_pct = float(find_max_cfg.get("max_error_rate_pct") or 1.0)
        target_qps_total = _coerce_optional_float(workload_cfg.get("target_qps"))

        max_concurrency = (
            _coerce_optional_int(workload_cfg.get("concurrent_connections"))
            or find_max_start
        )
        max_concurrency = max(find_max_start, max_concurrency)
        if effective_max_threads_per_worker is not None:
            max_concurrency = min(
                max_concurrency,
                int(effective_max_threads_per_worker) * worker_group_count,
            )
        if max_workers is not None and effective_max_threads_per_worker is not None:
            max_concurrency = min(
                max_concurrency,
                int(max_workers) * int(effective_max_threads_per_worker),
            )
        bounded_max_configured = scaling_mode == "BOUNDED" and (
            max_workers is not None or max_threads_per_worker is not None
        )

        last_metrics_ts: datetime | None = None
        last_heartbeat_update: datetime | None = None
        last_parent_rollup_status: str | None = None
        last_warehouse_poll_mono: float | None = None
        warehouse_poll_task: asyncio.Task | None = None
        bounds_under_target_intervals = 0

        find_max_step_number = 0
        find_max_step_id: str | None = None
        find_max_step_target = 0
        find_max_step_started_at: datetime | None = None
        find_max_step_started_epoch_ms: int | None = None
        find_max_step_end_epoch_ms: int | None = None
        find_max_step_start_ops = 0
        find_max_step_start_errors = 0
        find_max_step_history: list[dict[str, Any]] = []
        find_max_baseline_p95: float | None = None
        find_max_baseline_p99: float | None = None
        find_max_prev_step_qps: float | None = None
        find_max_best_concurrency = 0
        find_max_best_qps = 0.0
        find_max_completed = False
        find_max_final_reason: str | None = None

        async def _poll_warehouse(elapsed: float | None) -> None:
            if not warehouse_name:
                return
            try:
                telemetry_pool = snowflake_pool.get_telemetry_pool()
                rows = await telemetry_pool.execute_query(
                    f"SHOW WAREHOUSES LIKE '{warehouse_name}'"
                )
                if rows:
                    await results_store.insert_warehouse_poll_snapshot(
                        run_id=run_id,
                        warehouse_name=warehouse_name,
                        elapsed_seconds=elapsed,
                        row=rows[0],
                    )
            except Exception as exc:
                logger.debug("Warehouse poll failed for run %s: %s", run_id, exc)

        def _build_worker_targets(
            total_target: int,
        ) -> tuple[int, dict[str, dict[str, Any]]]:
            """Wrapper that calls module-level build_worker_targets with closure vars."""
            return build_worker_targets(
                total_target=total_target,
                worker_group_count=worker_group_count,
                per_worker_cap=per_worker_threads,
                min_threads_per_worker=min_threads_per_worker,
                max_threads_per_worker=max_threads_per_worker,
                load_mode=load_mode,
                target_qps_total=target_qps_total,
            )

        async def _apply_worker_targets(
            *,
            total_target: int,
            step_id: str,
            step_number: int,
            reason: str,
        ) -> int:
            effective_total, targets = _build_worker_targets(total_target)
            effective_at = datetime.now(UTC).isoformat()

            for worker_id, target in targets.items():
                await self._pool.execute_query(
                    f"""
                    UPDATE {prefix}.RUN_STATUS
                    SET WORKER_TARGETS = OBJECT_INSERT(
                            COALESCE(WORKER_TARGETS, OBJECT_CONSTRUCT()),
                            ?,
                            PARSE_JSON(?),
                            TRUE
                        ),
                        UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE RUN_ID = ?
                    """,
                    params=[worker_id, json.dumps(target), run_id],
                )

                event_data = {
                    "scope": "WORKER",
                    "worker_id": worker_id,
                    "worker_group_id": target.get("worker_group_id"),
                    "target_connections": target.get("target_connections"),
                    "step_id": step_id,
                    "step_number": int(step_number),
                    "effective_at": effective_at,
                    "ramp_seconds": 5,
                    "reason": reason,
                }
                if target.get("target_qps") is not None:
                    event_data["target_qps"] = target.get("target_qps")
                await self._emit_control_event(
                    run_id=run_id,
                    event_type="SET_WORKER_TARGET",
                    event_data=event_data,
                )

            return effective_total

        async def _persist_find_max_state(state: dict[str, Any]) -> None:
            await self._pool.execute_query(
                f"""
                UPDATE {prefix}.RUN_STATUS
                SET FIND_MAX_STATE = PARSE_JSON(?),
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE RUN_ID = ?
                """,
                params=[json.dumps(state), run_id],
            )

        async def _insert_find_max_step_history(
            *,
            step_id: str,
            step_number: int,
            target_workers: int,
            step_start_time: datetime,
            step_end_time: datetime,
            step_duration_seconds: float,
            total_queries: int,
            qps: float,
            p50_latency_ms: float | None,
            p95_latency_ms: float | None,
            p99_latency_ms: float | None,
            error_count: int,
            error_rate: float,
            qps_vs_prior_pct: float | None,
            p95_vs_baseline_pct: float | None,
            queue_detected: bool,
            outcome: str,
            stop_reason: str | None,
        ) -> None:
            await self._pool.execute_query(
                f"""
                INSERT INTO {prefix}.FIND_MAX_STEP_HISTORY (
                    STEP_ID,
                    RUN_ID,
                    STEP_NUMBER,
                    TARGET_WORKERS,
                    STEP_START_TIME,
                    STEP_END_TIME,
                    STEP_DURATION_SECONDS,
                    TOTAL_QUERIES,
                    QPS,
                    P50_LATENCY_MS,
                    P95_LATENCY_MS,
                    P99_LATENCY_MS,
                    ERROR_COUNT,
                    ERROR_RATE,
                    QPS_VS_PRIOR_PCT,
                    P95_VS_BASELINE_PCT,
                    QUEUE_DETECTED,
                    OUTCOME,
                    STOP_REASON
                )
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                """,
                params=[
                    step_id,
                    run_id,
                    int(step_number),
                    int(target_workers),
                    step_start_time,
                    step_end_time,
                    float(step_duration_seconds),
                    int(total_queries),
                    float(qps),
                    float(p50_latency_ms) if p50_latency_ms is not None else None,
                    float(p95_latency_ms) if p95_latency_ms is not None else None,
                    float(p99_latency_ms) if p99_latency_ms is not None else None,
                    int(error_count),
                    float(error_rate),
                    float(qps_vs_prior_pct) if qps_vs_prior_pct is not None else None,
                    float(p95_vs_baseline_pct)
                    if p95_vs_baseline_pct is not None
                    else None,
                    bool(queue_detected),
                    str(outcome),
                    stop_reason,
                ],
            )

        async def _persist_find_max_result(result: dict[str, Any]) -> None:
            await self._pool.execute_query(
                f"""
                UPDATE {prefix}.TEST_RESULTS
                SET FIND_MAX_RESULT = PARSE_JSON(?),
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE TEST_ID = ? AND RUN_ID = ?
                """,
                params=[json.dumps(result), run_id, run_id],
            )

        async def _persist_bounds_state(state: dict[str, Any]) -> None:
            await self._pool.execute_query(
                f"""
                UPDATE {prefix}.TEST_RESULTS
                SET CUSTOM_METRICS = OBJECT_INSERT(
                        COALESCE(CUSTOM_METRICS, OBJECT_CONSTRUCT()),
                        'bounds_state',
                        PARSE_JSON(?),
                        TRUE
                    ),
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE TEST_ID = ? AND RUN_ID = ?
                """,
                params=[json.dumps(state), run_id, run_id],
            )

        async def _mark_run_completed() -> None:
            # First transition to PROCESSING phase for enrichment.
            # Note: END_TIME is NOT set here - it will be set when phase→COMPLETED
            # after enrichment finishes. This ensures elapsed time includes PROCESSING.
            await self._pool.execute_query(
                f"""
                UPDATE {prefix}.RUN_STATUS
                SET STATUS = 'COMPLETED',
                    PHASE = 'PROCESSING',
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE RUN_ID = ?
                  AND STATUS NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
                """,
                params=[run_id],
            )
            await self._pool.execute_query(
                f"""
                UPDATE {prefix}.TEST_RESULTS
                SET STATUS = 'COMPLETED',
                    ENRICHMENT_STATUS = 'PENDING',
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE TEST_ID = ? AND RUN_ID = ?
                """,
                params=[run_id, run_id],
            )
            # Trigger enrichment as a background task
            asyncio.create_task(_run_enrichment(run_id))

        async def _run_enrichment(test_id: str) -> None:
            """Run enrichment in background after test completion.

            Note: QUERY_HISTORY enrichment only works for Snowflake table types.
            Postgres tests skip enrichment since pg_stat_statements only provides
            aggregated stats, not per-query execution history.
            """
            # Check if this is a Postgres test (no QUERY_HISTORY available)
            scenario_config = (
                ctx.scenario_config
                if ctx is not None and isinstance(ctx.scenario_config, dict)
                else {}
            )
            table_type_str = str(scenario_config.get("table_type", "")).strip().upper()
            is_postgres_test = table_type_str in {"POSTGRES", "SNOWFLAKE_POSTGRES"}

            try:
                if is_postgres_test:
                    logger.info(
                        "Skipping QUERY_HISTORY enrichment for Postgres test %s "
                        "(pg_stat_statements provides aggregate stats only)",
                        test_id,
                    )
                    await update_enrichment_status(
                        test_id=test_id,
                        status="SKIPPED",
                        error="Postgres: no per-query history available",
                    )
                else:
                    logger.info("Starting enrichment for run %s", test_id)
                    await enrich_query_executions_with_retry(
                        run_id=test_id,
                        target_ratio=0.90,
                        max_wait_seconds=240,
                        poll_interval_seconds=10,
                    )
                    await update_test_overhead_percentiles(run_id=test_id)
                    await update_enrichment_status(
                        test_id=test_id, status="COMPLETED", error=None
                    )
                    logger.info("Enrichment completed for run %s", test_id)

                # Transition phase from PROCESSING to COMPLETED and set END_TIME.
                # END_TIME is set here (not when status→COMPLETED) so elapsed time
                # includes the full PROCESSING phase (enrichment).
                await self._pool.execute_query(
                    f"""
                    UPDATE {prefix}.RUN_STATUS
                    SET PHASE = 'COMPLETED',
                        END_TIME = COALESCE(END_TIME, CURRENT_TIMESTAMP()),
                        UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE RUN_ID = ?
                      AND PHASE = 'PROCESSING'
                    """,
                    params=[test_id],
                )
                await self._pool.execute_query(
                    f"""
                    UPDATE {prefix}.TEST_RESULTS
                    SET END_TIME = COALESCE(END_TIME, CURRENT_TIMESTAMP()),
                        UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE TEST_ID = ? AND RUN_ID = ?
                    """,
                    params=[test_id, test_id],
                )
            except Exception as e:
                logger.error("Enrichment failed for test %s: %s", test_id, e)
                try:
                    await update_enrichment_status(
                        test_id=test_id, status="FAILED", error=str(e)
                    )
                    # Still transition phase to COMPLETED even on failure, and set END_TIME
                    await self._pool.execute_query(
                        f"""
                        UPDATE {prefix}.RUN_STATUS
                        SET PHASE = 'COMPLETED',
                            END_TIME = COALESCE(END_TIME, CURRENT_TIMESTAMP()),
                            UPDATED_AT = CURRENT_TIMESTAMP()
                        WHERE RUN_ID = ?
                          AND PHASE = 'PROCESSING'
                        """,
                        params=[test_id],
                    )
                    await self._pool.execute_query(
                        f"""
                        UPDATE {prefix}.TEST_RESULTS
                        SET END_TIME = COALESCE(END_TIME, CURRENT_TIMESTAMP()),
                            UPDATED_AT = CURRENT_TIMESTAMP()
                        WHERE TEST_ID = ? AND RUN_ID = ?
                        """,
                        params=[test_id, test_id],
                    )
                except Exception:
                    pass

        async def _mark_run_cancelled() -> None:
            """Transition from CANCELLING to CANCELLED when workers have drained."""
            await self._pool.execute_query(
                f"""
                UPDATE {prefix}.RUN_STATUS
                SET STATUS = 'CANCELLED',
                    END_TIME = COALESCE(END_TIME, CURRENT_TIMESTAMP()),
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE RUN_ID = ?
                  AND STATUS = 'CANCELLING'
                """,
                params=[run_id],
            )
            await self._pool.execute_query(
                f"""
                UPDATE {prefix}.TEST_RESULTS
                SET STATUS = 'CANCELLED',
                    END_TIME = COALESCE(END_TIME, CURRENT_TIMESTAMP()),
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE TEST_ID = ? AND RUN_ID = ?
                  AND STATUS NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
                """,
                params=[run_id, run_id],
            )

        try:
            while True:
                await asyncio.sleep(1.0)

                status_rows = await self._pool.execute_query(
                    f"""
                    SELECT STATUS, PHASE,
                           TIMESTAMPDIFF(SECOND, START_TIME, CURRENT_TIMESTAMP()) AS ELAPSED_SECONDS,
                           TIMESTAMPDIFF(SECOND, WARMUP_START_TIME, CURRENT_TIMESTAMP()) AS WARMUP_ELAPSED_SECONDS
                    FROM {prefix}.RUN_STATUS
                    WHERE RUN_ID = ?
                    """,
                    params=[run_id],
                )
                if not status_rows:
                    logger.warning("Poll loop: run %s not found in RUN_STATUS", run_id)
                    return

                status_raw, phase_raw, elapsed_raw, warmup_elapsed_raw = status_rows[0]
                status = str(status_raw or "").upper()
                phase = str(phase_raw or "").upper()
                elapsed_seconds = (
                    float(elapsed_raw) if elapsed_raw is not None else None
                )
                warmup_elapsed_seconds = (
                    float(warmup_elapsed_raw)
                    if warmup_elapsed_raw is not None
                    else None
                )
                now = datetime.now(UTC)

                if warehouse_name:
                    now_mono = asyncio.get_running_loop().time()
                    if (
                        last_warehouse_poll_mono is None
                        or now_mono - last_warehouse_poll_mono >= 5.0
                    ):
                        if warehouse_poll_task is None or warehouse_poll_task.done():
                            last_warehouse_poll_mono = now_mono
                            warehouse_poll_task = asyncio.create_task(
                                _poll_warehouse(elapsed_seconds)
                            )

                await self._pool.execute_query(
                    f"""
                    UPDATE {prefix}.WORKER_HEARTBEATS
                    SET STATUS = 'DEAD',
                        UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE RUN_ID = ?
                      AND STATUS NOT IN ('DEAD', 'COMPLETED')
                      AND TIMESTAMPDIFF('second', LAST_HEARTBEAT, CURRENT_TIMESTAMP()) > 60
                    """,
                    params=[run_id],
                )

                await self._pool.execute_query(
                    f"""
                    UPDATE {prefix}.WORKER_HEARTBEATS
                    SET STATUS = 'STALE',
                        UPDATED_AT = CURRENT_TIMESTAMP()
                    WHERE RUN_ID = ?
                      AND STATUS NOT IN ('STALE', 'DEAD', 'COMPLETED')
                      AND TIMESTAMPDIFF('second', LAST_HEARTBEAT, CURRENT_TIMESTAMP()) > 30
                    """,
                    params=[run_id],
                )

                heartbeat_rows = await self._pool.execute_query(
                    f"""
                    SELECT
                        COUNT(*) AS worker_count,
                        SUM(
                            CASE
                                WHEN STATUS IN (
                                    'STARTING', 'WAITING', 'RUNNING', 'DRAINING', 'STALE', 'READY', 'INITIALIZING'
                                ) THEN 1
                                ELSE 0
                            END
                        ) AS active_count,
                        SUM(
                            CASE
                                WHEN STATUS IN ('COMPLETED', 'DEAD') THEN 1
                                ELSE 0
                            END
                        ) AS completed_count,
                        SUM(CASE WHEN STATUS = 'DEAD' THEN 1 ELSE 0 END) AS dead_count,
                        SUM(CASE WHEN STATUS = 'READY' THEN 1 ELSE 0 END) AS ready_count,
                        MAX(UPDATED_AT) AS last_update,
                        MAX(COALESCE(CPU_PERCENT, 0)) AS max_cpu_percent,
                        MAX(COALESCE(MEMORY_PERCENT, 0)) AS max_memory_percent
                    FROM {prefix}.WORKER_HEARTBEATS
                    WHERE RUN_ID = ?
                    """,
                    params=[run_id],
                )

                (
                    workers_registered,
                    workers_active,
                    workers_completed,
                    dead_workers,
                    workers_ready,
                    heartbeat_updated_at,
                    max_cpu_seen,
                    max_memory_seen,
                ) = heartbeat_rows[0] if heartbeat_rows else (None,) * 8

                workers_registered = int(workers_registered or 0)
                workers_active = int(workers_active or 0)
                workers_completed = int(workers_completed or 0)
                dead_workers = int(dead_workers or 0)
                workers_ready = int(workers_ready or 0)
                max_cpu_seen = float(max_cpu_seen) if max_cpu_seen is not None else None
                max_memory_seen = (
                    float(max_memory_seen) if max_memory_seen is not None else None
                )

                if status == "STARTING" and workers_ready >= worker_group_count:
                    logger.info(
                        "All %d workers READY for run %s - transitioning to %s",
                        workers_ready,
                        run_id,
                        initial_phase,
                    )
                    # Note: START_TIME was already set in start_run() to include PREPARING phase
                    # Set WARMUP_START_TIME now that workers are ready (for accurate warmup timing)
                    await self._pool.execute_query(
                        f"""
                        UPDATE {prefix}.RUN_STATUS
                        SET STATUS = 'RUNNING',
                            PHASE = ?,
                            WARMUP_START_TIME = CASE WHEN ? = 'WARMUP' THEN CURRENT_TIMESTAMP() ELSE NULL END,
                            UPDATED_AT = CURRENT_TIMESTAMP()
                        WHERE RUN_ID = ?
                          AND STATUS = 'STARTING'
                        """,
                        params=[initial_phase, initial_phase, run_id],
                    )
                    await self._pool.execute_query(
                        f"""
                        UPDATE {prefix}.TEST_RESULTS
                        SET STATUS = 'RUNNING',
                            UPDATED_AT = CURRENT_TIMESTAMP()
                        WHERE TEST_ID = ? AND RUN_ID = ?
                          AND STATUS = 'STARTING'
                        """,
                        params=[run_id, run_id],
                    )
                    await self._emit_control_event(
                        run_id=run_id,
                        event_type="SET_PHASE",
                        event_data={
                            "scope": "RUN",
                            "phase": initial_phase,
                            "workers_ready": workers_ready,
                            "timestamp": datetime.now(UTC).isoformat(),
                        },
                    )
                    status = "RUNNING"
                    phase = initial_phase

                guardrail_reason = None
                if (
                    max_cpu_percent is not None
                    and max_cpu_seen is not None
                    and max_cpu_seen >= max_cpu_percent
                ):
                    guardrail_reason = (
                        f"cpu_percent {max_cpu_seen:.2f} >= {max_cpu_percent:.2f}"
                    )
                if (
                    guardrail_reason is None
                    and max_memory_percent is not None
                    and max_memory_seen is not None
                    and max_memory_seen >= max_memory_percent
                ):
                    guardrail_reason = f"memory_percent {max_memory_seen:.2f} >= {max_memory_percent:.2f}"

                if guardrail_reason and status not in {
                    "FAILED",
                    "CANCELLED",
                    "COMPLETED",
                    "CANCELLING",
                    "STOPPING",
                }:
                    logger.warning(
                        "Guardrail triggered for run %s: %s", run_id, guardrail_reason
                    )
                    await self._emit_control_event(
                        run_id=run_id,
                        event_type="STOP",
                        event_data={
                            "scope": "RUN",
                            "reason": "guardrail",
                            "detail": guardrail_reason,
                            "timestamp": datetime.now(UTC).isoformat(),
                        },
                    )
                    cancellation_msg = f"Guardrail triggered: {guardrail_reason}"
                    await self._pool.execute_query(
                        f"""
                        UPDATE {prefix}.RUN_STATUS
                        SET STATUS = 'FAILED',
                            CANCELLATION_REASON = ?,
                            END_TIME = COALESCE(END_TIME, CURRENT_TIMESTAMP()),
                            UPDATED_AT = CURRENT_TIMESTAMP()
                        WHERE RUN_ID = ?
                          AND STATUS NOT IN ('FAILED', 'CANCELLED', 'COMPLETED')
                        """,
                        params=[cancellation_msg, run_id],
                    )
                    status = "FAILED"

                if dead_workers > 0 and status not in {
                    "FAILED",
                    "CANCELLED",
                    "COMPLETED",
                    "CANCELLING",
                    "STOPPING",
                }:
                    logger.warning(
                        "Run %s has %d dead worker(s); issuing STOP",
                        run_id,
                        dead_workers,
                    )
                    await self._emit_control_event(
                        run_id=run_id,
                        event_type="STOP",
                        event_data={
                            "scope": "RUN",
                            "reason": "worker_failure",
                            "dead_workers": dead_workers,
                            "timestamp": datetime.now(UTC).isoformat(),
                        },
                    )
                    cancellation_msg = (
                        f"Worker failure: {dead_workers} worker(s) stopped responding"
                    )
                    await self._pool.execute_query(
                        f"""
                        UPDATE {prefix}.RUN_STATUS
                        SET STATUS = 'FAILED',
                            CANCELLATION_REASON = ?,
                            END_TIME = COALESCE(END_TIME, CURRENT_TIMESTAMP()),
                            UPDATED_AT = CURRENT_TIMESTAMP()
                        WHERE RUN_ID = ?
                          AND STATUS NOT IN ('FAILED', 'CANCELLED', 'COMPLETED')
                        """,
                        params=[cancellation_msg, run_id],
                    )
                    status = "FAILED"

                if (
                    status == "RUNNING"
                    and phase == "WARMUP"
                    and warmup_seconds > 0
                    and warmup_elapsed_seconds is not None
                    and warmup_elapsed_seconds >= warmup_seconds
                ):
                    logger.info(
                        "Transitioning %s from WARMUP to MEASUREMENT: warmup_elapsed=%.1fs >= warmup=%ds",
                        run_id,
                        warmup_elapsed_seconds,
                        warmup_seconds,
                    )
                    await self._pool.execute_query(
                        f"""
                        UPDATE {prefix}.RUN_STATUS
                        SET PHASE = 'MEASUREMENT',
                            WARMUP_END_TIME = COALESCE(WARMUP_END_TIME, CURRENT_TIMESTAMP()),
                            UPDATED_AT = CURRENT_TIMESTAMP()
                        WHERE RUN_ID = ?
                          AND PHASE = 'WARMUP'
                        """,
                        params=[run_id],
                    )
                    await self._emit_control_event(
                        run_id=run_id,
                        event_type="SET_PHASE",
                        event_data={
                            "scope": "RUN",
                            "phase": "MEASUREMENT",
                            "timestamp": datetime.now(UTC).isoformat(),
                        },
                    )
                    phase = "MEASUREMENT"

                effective_elapsed_seconds = elapsed_seconds
                if warmup_seconds > 0 and warmup_elapsed_seconds is not None:
                    # Use warmup start as the elapsed base so PREPARING does not
                    # eat into the configured warmup + measurement budget.
                    effective_elapsed_seconds = warmup_elapsed_seconds
                if (
                    status == "RUNNING"
                    and duration_seconds > 0
                    and effective_elapsed_seconds is not None
                    and effective_elapsed_seconds >= (warmup_seconds + duration_seconds)
                ):
                    logger.info(
                        "Stopping %s due to duration elapsed: elapsed=%.1fs >= warmup+duration=%d+%d=%ds",
                        run_id,
                        effective_elapsed_seconds,
                        warmup_seconds,
                        duration_seconds,
                        warmup_seconds + duration_seconds,
                    )
                    await self._emit_control_event(
                        run_id=run_id,
                        event_type="STOP",
                        event_data={
                            "scope": "RUN",
                            "reason": "duration_elapsed",
                            "timestamp": datetime.now(UTC).isoformat(),
                        },
                    )
                    # Use STOPPING for natural completion (not CANCELLING which is for user cancellation)
                    await self._pool.execute_query(
                        f"""
                        UPDATE {prefix}.RUN_STATUS
                        SET STATUS = 'STOPPING',
                            UPDATED_AT = CURRENT_TIMESTAMP()
                        WHERE RUN_ID = ?
                          AND STATUS = 'RUNNING'
                        """,
                        params=[run_id],
                    )
                    status = "STOPPING"

                total_ops = 0
                error_count = 0
                current_qps = 0.0
                target_connections_total = 0
                latest_metrics_ts = None
                aggregate_p50 = None
                aggregate_p95 = None
                aggregate_p99 = None

                if phase == "MEASUREMENT":
                    metrics_rows = await self._pool.execute_query(
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
                              AND PHASE = 'MEASUREMENT'
                        )
                        SELECT
                            SUM(l.TOTAL_QUERIES) AS TOTAL_OPS,
                            SUM(l.ERROR_COUNT) AS ERROR_COUNT,
                            SUM(l.QPS) AS CURRENT_QPS,
                            SUM(l.TARGET_CONNECTIONS) AS TARGET_CONNECTIONS,
                            MAX(l.TIMESTAMP) AS LATEST_TS,
                            AVG(IFF(l.TOTAL_QUERIES > 0, l.P50_LATENCY_MS, NULL))
                                AS P50_LATENCY_MS,
                            MAX(IFF(l.TOTAL_QUERIES > 0, l.P95_LATENCY_MS, NULL))
                                AS P95_LATENCY_MS,
                            MAX(IFF(l.TOTAL_QUERIES > 0, l.P99_LATENCY_MS, NULL))
                                AS P99_LATENCY_MS
                        FROM latest l
                        LEFT JOIN {prefix}.WORKER_HEARTBEATS h
                          ON h.RUN_ID = l.RUN_ID
                         AND h.WORKER_ID = l.WORKER_ID
                        WHERE l.RN = 1
                          AND (h.STATUS IS NULL OR UPPER(h.STATUS) <> 'DEAD')
                        """,
                        params=[run_id],
                    )
                    if metrics_rows:
                        total_ops = int(metrics_rows[0][0] or 0)
                        error_count = int(metrics_rows[0][1] or 0)
                        current_qps = float(metrics_rows[0][2] or 0.0)
                        target_connections_total = int(metrics_rows[0][3] or 0)
                        latest_metrics_ts = metrics_rows[0][4]
                        aggregate_p50 = (
                            float(metrics_rows[0][5])
                            if metrics_rows[0][5] is not None
                            else None
                        )
                        aggregate_p95 = (
                            float(metrics_rows[0][6])
                            if metrics_rows[0][6] is not None
                            else None
                        )
                        aggregate_p99 = (
                            float(metrics_rows[0][7])
                            if metrics_rows[0][7] is not None
                            else None
                        )

                if (
                    load_mode == "QPS"
                    and scaling_mode == "BOUNDED"
                    and status == "RUNNING"
                    and phase == "MEASUREMENT"
                    and target_qps_total
                ):
                    worker_cap = (
                        int(max_workers)
                        if max_workers is not None
                        else int(worker_group_count)
                    )
                    max_total_threads = (
                        int(effective_max_threads_per_worker) * int(worker_cap)
                        if effective_max_threads_per_worker is not None
                        else None
                    )
                    under_target = float(current_qps) < float(target_qps_total) * 0.98
                    at_ceiling = (
                        max_total_threads is not None
                        and target_connections_total >= max_total_threads
                        and (max_workers is None or worker_group_count >= max_workers)
                    )
                    if under_target and at_ceiling:
                        bounds_under_target_intervals += 1
                    else:
                        bounds_under_target_intervals = 0

                    if bounds_under_target_intervals >= bounds_patience:
                        bounds_state = {
                            "mode": scaling_mode,
                            "min_workers": int(min_workers),
                            "max_workers": int(max_workers)
                            if max_workers is not None
                            else None,
                            "min_connections": int(min_threads_per_worker),
                            "max_connections": int(max_threads_per_worker)
                            if max_threads_per_worker is not None
                            else None,
                            "bounds_patience_intervals": int(bounds_patience),
                            "completion_reason": "BOUNDS_LIMIT_REACHED",
                            "current_qps": float(current_qps),
                            "target_qps": float(target_qps_total),
                            "target_connections": int(target_connections_total),
                            "ceiling_connections": int(max_total_threads)
                            if max_total_threads is not None
                            else None,
                            "observed_at": datetime.now(UTC).isoformat(),
                        }
                        await _persist_bounds_state(bounds_state)
                        await self._emit_control_event(
                            run_id=run_id,
                            event_type="STOP",
                            event_data={
                                "scope": "RUN",
                                "reason": "bounds_limit_reached",
                                "timestamp": datetime.now(UTC).isoformat(),
                            },
                        )
                        await _mark_run_completed()
                        status = "COMPLETED"
                        phase = "COMPLETED"
                else:
                    bounds_under_target_intervals = 0

                if (
                    find_max_enabled
                    and status == "RUNNING"
                    and phase == "MEASUREMENT"
                    and not find_max_completed
                ):
                    try:
                        if find_max_step_number == 0:
                            find_max_step_number = 1
                            find_max_step_id = str(uuid.uuid4())
                            find_max_step_target = find_max_start
                            find_max_step_started_at = now
                            find_max_step_started_epoch_ms = int(
                                datetime.now(UTC).timestamp() * 1000
                            )
                            find_max_step_end_epoch_ms = (
                                int(
                                    find_max_step_started_epoch_ms
                                    + (find_max_step_seconds * 1000)
                                )
                                if find_max_step_started_epoch_ms is not None
                                else None
                            )
                            find_max_step_start_ops = total_ops
                            find_max_step_start_errors = error_count

                            find_max_step_target = await _apply_worker_targets(
                                total_target=find_max_step_target,
                                step_id=str(find_max_step_id),
                                step_number=find_max_step_number,
                                reason="step_start",
                            )
                            find_max_best_concurrency = max(
                                find_max_best_concurrency, int(find_max_step_target)
                            )

                        step_elapsed = 0.0
                        if find_max_step_started_at is not None:
                            step_elapsed = max(
                                0.0, (now - find_max_step_started_at).total_seconds()
                            )
                        step_ops = max(0, total_ops - find_max_step_start_ops)
                        step_errors = max(0, error_count - find_max_step_start_errors)
                        step_error_rate = (
                            step_errors / step_ops if step_ops > 0 else 0.0
                        )
                        step_error_rate_pct = step_error_rate * 100.0
                        step_qps = step_ops / step_elapsed if step_elapsed > 0 else 0.0

                        qps_vs_prior_pct = None
                        if (
                            find_max_prev_step_qps is not None
                            and find_max_prev_step_qps > 0
                        ):
                            qps_vs_prior_pct = (
                                (step_qps - find_max_prev_step_qps)
                                / find_max_prev_step_qps
                            ) * 100.0

                        p95_vs_baseline_pct = None
                        if (
                            find_max_baseline_p95 is not None
                            and aggregate_p95 is not None
                            and find_max_baseline_p95 > 0
                        ):
                            p95_vs_baseline_pct = (
                                (aggregate_p95 - find_max_baseline_p95)
                                / find_max_baseline_p95
                            ) * 100.0

                        warehouse_snapshot = (
                            await results_store.fetch_latest_warehouse_poll_snapshot(
                                run_id=run_id
                            )
                        )
                        queue_detected = bool(
                            warehouse_snapshot
                            and (warehouse_snapshot.get("queued") or 0) > 0
                        )

                        state = {
                            "mode": "FIND_MAX_CONCURRENCY",
                            "status": "STEP_RUNNING",
                            "current_step": int(find_max_step_number),
                            "current_concurrency": int(find_max_step_target),
                            "target_workers": int(find_max_step_target),
                            "active_worker_count": int(workers_active),
                            "current_qps": float(step_qps),
                            "current_p95_latency_ms": aggregate_p95,
                            "current_p99_latency_ms": aggregate_p99,
                            "current_error_rate_pct": float(step_error_rate_pct),
                            "qps": float(step_qps),
                            "qps_vs_prior_pct": qps_vs_prior_pct,
                            "baseline_p95_ms": find_max_baseline_p95,
                            "baseline_p95_latency_ms": find_max_baseline_p95,
                            "baseline_p99_latency_ms": find_max_baseline_p99,
                            "p95_vs_baseline_pct": p95_vs_baseline_pct,
                            "error_rate": float(step_error_rate),
                            "queue_detected": bool(queue_detected),
                            "step_duration_seconds": int(find_max_step_seconds),
                            "step_started_at_epoch_ms": find_max_step_started_epoch_ms,
                            "step_end_at_epoch_ms": find_max_step_end_epoch_ms,
                            "next_planned_concurrency": int(
                                min(
                                    max_concurrency,
                                    int(find_max_step_target) + int(find_max_increment),
                                )
                            ),
                            "start_concurrency": int(find_max_start),
                            "concurrency_increment": int(find_max_increment),
                            "max_concurrency": int(max_concurrency),
                            "qps_stability_pct": float(find_max_qps_stability_pct),
                            "latency_stability_pct": float(
                                find_max_latency_stability_pct
                            ),
                            "max_error_rate_pct": float(find_max_max_error_pct),
                            "best_concurrency": int(find_max_best_concurrency),
                            "best_qps": float(find_max_best_qps),
                            "step_history": list(find_max_step_history),
                            "last_updated": datetime.now(UTC).isoformat(),
                        }

                        await _persist_find_max_state(state)

                        if step_elapsed >= float(find_max_step_seconds):
                            stable = True
                            stop_reason = None
                            outcome = "STABLE"

                            if queue_detected:
                                stable = False
                                stop_reason = "Warehouse queue detected"
                                outcome = "QUEUE_DETECTED"
                            elif step_error_rate_pct > find_max_max_error_pct:
                                stable = False
                                stop_reason = (
                                    f"Error rate {step_error_rate_pct:.2f}% "
                                    f"> max_error_rate_pct {find_max_max_error_pct:.2f}%"
                                )
                                outcome = "ERROR_THRESHOLD"
                            elif (
                                p95_vs_baseline_pct is not None
                                and p95_vs_baseline_pct > find_max_latency_stability_pct
                            ):
                                stable = False
                                stop_reason = (
                                    f"P95 latency increased {p95_vs_baseline_pct:.1f}% "
                                    f"vs baseline (limit {find_max_latency_stability_pct:.1f}%)"
                                )
                                outcome = "DEGRADED"
                            elif (
                                qps_vs_prior_pct is not None
                                and qps_vs_prior_pct < -find_max_qps_stability_pct
                            ):
                                stable = False
                                stop_reason = (
                                    f"QPS dropped {-qps_vs_prior_pct:.1f}% "
                                    f"vs prior (limit {find_max_qps_stability_pct:.1f}%)"
                                )
                                outcome = "DEGRADED"

                            if find_max_baseline_p95 is None and aggregate_p95:
                                find_max_baseline_p95 = float(aggregate_p95)
                            if find_max_baseline_p99 is None and aggregate_p99:
                                find_max_baseline_p99 = float(aggregate_p99)

                            if stable and step_qps >= find_max_best_qps:
                                find_max_best_concurrency = int(find_max_step_target)
                                find_max_best_qps = float(step_qps)

                            step_entry = {
                                "step": int(find_max_step_number),
                                "concurrency": int(find_max_step_target),
                                "qps": float(step_qps),
                                "p95_latency_ms": aggregate_p95,
                                "p99_latency_ms": aggregate_p99,
                                "error_rate_pct": float(step_error_rate_pct),
                                "stable": bool(stable),
                                "stop_reason": stop_reason,
                                "is_backoff": False,
                            }
                            find_max_step_history.append(step_entry)

                            await _insert_find_max_step_history(
                                step_id=str(find_max_step_id or uuid.uuid4()),
                                step_number=int(find_max_step_number),
                                target_workers=int(find_max_step_target),
                                step_start_time=find_max_step_started_at or now,
                                step_end_time=now,
                                step_duration_seconds=float(step_elapsed),
                                total_queries=int(step_ops),
                                qps=float(step_qps),
                                p50_latency_ms=aggregate_p50,
                                p95_latency_ms=aggregate_p95,
                                p99_latency_ms=aggregate_p99,
                                error_count=int(step_errors),
                                error_rate=float(step_error_rate),
                                qps_vs_prior_pct=qps_vs_prior_pct,
                                p95_vs_baseline_pct=p95_vs_baseline_pct,
                                queue_detected=bool(queue_detected),
                                outcome=str(outcome),
                                stop_reason=stop_reason,
                            )

                            state.update(
                                {
                                    "status": "STEP_COMPLETE",
                                    "stable": bool(stable),
                                    "stop_reason": stop_reason,
                                    "step_history": list(find_max_step_history),
                                    "baseline_p95_latency_ms": find_max_baseline_p95,
                                    "baseline_p99_latency_ms": find_max_baseline_p99,
                                    "best_concurrency": int(find_max_best_concurrency),
                                    "best_qps": float(find_max_best_qps),
                                    "last_updated": datetime.now(UTC).isoformat(),
                                }
                            )
                            await _persist_find_max_state(state)

                            if stable and find_max_step_target < max_concurrency:
                                find_max_prev_step_qps = float(step_qps)
                                find_max_step_number += 1
                                find_max_step_id = str(uuid.uuid4())
                                find_max_step_target = min(
                                    max_concurrency,
                                    int(find_max_step_target) + int(find_max_increment),
                                )
                                find_max_step_started_at = now
                                find_max_step_started_epoch_ms = int(
                                    datetime.now(UTC).timestamp() * 1000
                                )
                                find_max_step_end_epoch_ms = (
                                    int(
                                        find_max_step_started_epoch_ms
                                        + (find_max_step_seconds * 1000)
                                    )
                                    if find_max_step_started_epoch_ms is not None
                                    else None
                                )
                                find_max_step_start_ops = total_ops
                                find_max_step_start_errors = error_count

                                find_max_step_target = await _apply_worker_targets(
                                    total_target=find_max_step_target,
                                    step_id=str(find_max_step_id),
                                    step_number=find_max_step_number,
                                    reason="step_advance",
                                )
                            else:
                                find_max_completed = True
                                max_type = None
                                bounded = False
                                ceiling_reached = False
                                if stable:
                                    ceiling_reached = True
                                    if bounded_max_configured:
                                        bounded = True
                                        max_type = "BOUNDED_MAX"
                                        find_max_final_reason = "Reached bounded max"
                                    else:
                                        max_type = "TRUE_MAX"
                                        find_max_final_reason = "Reached max workers"
                                else:
                                    find_max_final_reason = (
                                        stop_reason or "Degradation detected"
                                    )

                                final_state = dict(state)
                                final_state.update(
                                    {
                                        "completed": True,
                                        "status": "COMPLETED",
                                        "bounded": bool(bounded),
                                        "ceiling_reached": bool(ceiling_reached),
                                        "max_type": max_type,
                                        "final_best_concurrency": int(
                                            find_max_best_concurrency
                                        ),
                                        "final_best_qps": float(find_max_best_qps),
                                        "final_reason": find_max_final_reason,
                                        "baseline_p95_latency_ms": find_max_baseline_p95,
                                        "baseline_p99_latency_ms": find_max_baseline_p99,
                                        "step_history": list(find_max_step_history),
                                        "last_updated": datetime.now(UTC).isoformat(),
                                    }
                                )
                                await _persist_find_max_state(final_state)
                                await _persist_find_max_result(final_state)
                                await _mark_run_completed()
                    except Exception as exc:
                        logger.debug(
                            "Find Max controller error for run %s: %s", run_id, exc
                        )

                should_rollup = False
                if latest_metrics_ts != last_metrics_ts:
                    should_rollup = True
                if heartbeat_updated_at != last_heartbeat_update:
                    should_rollup = True

                if should_rollup:
                    await self._pool.execute_query(
                        f"""
                        UPDATE {prefix}.RUN_STATUS
                        SET WORKERS_REGISTERED = ?,
                            WORKERS_ACTIVE = ?,
                            WORKERS_COMPLETED = ?,
                            TOTAL_OPS = ?,
                            ERROR_COUNT = ?,
                            CURRENT_QPS = ?,
                            UPDATED_AT = CURRENT_TIMESTAMP()
                        WHERE RUN_ID = ?
                        """,
                        params=[
                            workers_registered,
                            workers_active,
                            workers_completed,
                            total_ops,
                            error_count,
                            current_qps,
                            run_id,
                        ],
                    )
                    last_metrics_ts = latest_metrics_ts
                    last_heartbeat_update = heartbeat_updated_at

                if should_rollup or (
                    status in {"COMPLETED", "FAILED", "CANCELLED"}
                    and status != last_parent_rollup_status
                ):
                    try:
                        await results_store.update_parent_run_aggregate(
                            parent_run_id=run_id
                        )
                        last_parent_rollup_status = status
                    except Exception as exc:
                        logger.debug(
                            "Failed to update parent rollup for %s: %s",
                            run_id,
                            exc,
                        )

                # Check if all worker processes have exited
                if ctx.worker_procs:
                    all_exited = all(
                        proc.returncode is not None for proc in ctx.worker_procs
                    )
                    if all_exited:
                        for i, proc in enumerate(ctx.worker_procs):
                            stdout_data, stderr_data = await proc.communicate()
                            if stdout_data:
                                logger.info(
                                    "Worker %d stdout for %s:\n%s",
                                    i,
                                    run_id,
                                    stdout_data.decode()[-4000:],
                                )
                            if stderr_data:
                                stderr_str = stderr_data.decode()
                                if len(stderr_str) > 8000:
                                    logger.warning(
                                        "Worker %d stderr for %s (first 4000 chars):\n%s",
                                        i,
                                        run_id,
                                        stderr_str[:4000],
                                    )
                                    logger.warning(
                                        "Worker %d stderr for %s (last 4000 chars):\n%s",
                                        i,
                                        run_id,
                                        stderr_str[-4000:],
                                    )
                                else:
                                    logger.warning(
                                        "Worker %d stderr for %s:\n%s",
                                        i,
                                        run_id,
                                        stderr_str,
                                    )
                            logger.info("Worker %d exit code: %s", i, proc.returncode)
                        logger.info(
                            "All workers exited for run %s, poll loop ending", run_id
                        )
                        break

                # Check if run is stopping
                if ctx.stopping:
                    logger.info("Run %s is stopping, poll loop ending", run_id)
                    break
                if (
                    status in {"COMPLETED", "FAILED", "CANCELLED"}
                    and not ctx.worker_procs
                ):
                    break

        except asyncio.CancelledError:
            logger.info("Poll loop cancelled for run %s", run_id)
        except Exception as e:
            logger.error("Poll loop error for run %s: %s", run_id, e)
        finally:
            # Finalize the run status - transition intermediate states to terminal
            # State machine:
            #   STOPPING -> COMPLETED (natural completion via duration_elapsed)
            #   CANCELLING -> CANCELLED (user cancellation or errors)
            try:
                final_status_rows = await self._pool.execute_query(
                    f"""
                    SELECT STATUS FROM {prefix}.RUN_STATUS WHERE RUN_ID = ?
                    """,
                    params=[run_id],
                )
                final_status = (
                    str(final_status_rows[0][0] or "").upper()
                    if final_status_rows
                    else ""
                )
                if final_status == "STOPPING":
                    # Natural completion - transition to COMPLETED
                    logger.info("Finalizing run %s: STOPPING -> COMPLETED", run_id)
                    await _mark_run_completed()
                elif final_status == "CANCELLING":
                    # User cancellation or error - transition to CANCELLED
                    logger.info("Finalizing run %s: CANCELLING -> CANCELLED", run_id)
                    await _mark_run_cancelled()
                elif final_status == "RUNNING":
                    # Shouldn't normally happen, but mark as COMPLETED if poll loop
                    # ended unexpectedly while still RUNNING
                    logger.warning(
                        "Finalizing run %s: RUNNING -> COMPLETED (unexpected)", run_id
                    )
                    await _mark_run_completed()
            except Exception as finalize_err:
                logger.error(
                    "Failed to finalize run %s status: %s", run_id, finalize_err
                )

            # Clean up log streaming
            ctx = self._active_runs.get(run_id)
            if ctx:
                if ctx.log_drain_task and not ctx.log_drain_task.done():
                    ctx.log_drain_task.cancel()
                    try:
                        await ctx.log_drain_task
                    except asyncio.CancelledError:
                        pass
                if ctx.log_handler:
                    logging.getLogger().removeHandler(ctx.log_handler)
                CURRENT_TEST_ID.set(None)

            # Clean up active runs dictionary
            if run_id in self._active_runs:
                del self._active_runs[run_id]
            logger.info("Poll loop ended for run %s", run_id)

    async def stop_run(self, run_id: str) -> None:
        """
        Signals the run to stop.

        1. Insert STOP event into RUN_CONTROL_EVENTS
        2. Update RUN_STATUS to CANCELLING
        3. Wait for workers to drain (with timeout)
        4. Update RUN_STATUS to CANCELLED
        """
        logger.info("Stop requested for run %s", run_id)
        prefix = f"{settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}"

        def _normalize_status(value: Any) -> str:
            return str(value or "").upper()

        async def _wait_for_local_workers(
            procs: list[asyncio.subprocess.Process],
        ) -> None:
            await asyncio.gather(*(proc.wait() for proc in procs))

        async def _wait_for_heartbeats(timeout_seconds: float) -> None:
            deadline = asyncio.get_running_loop().time() + timeout_seconds
            while True:
                rows = await self._pool.execute_query(
                    f"""
                    SELECT COUNT(*)
                    FROM {prefix}.WORKER_HEARTBEATS
                    WHERE RUN_ID = ?
                      AND UPPER(STATUS) NOT IN ('COMPLETED', 'DEAD')
                    """,
                    params=[run_id],
                )
                remaining = int(rows[0][0] or 0) if rows else 0
                if remaining == 0:
                    return
                if asyncio.get_running_loop().time() >= deadline:
                    return
                await asyncio.sleep(1.0)

        rows = await self._pool.execute_query(
            f"""
            SELECT STATUS
            FROM {prefix}.RUN_STATUS
            WHERE RUN_ID = ?
            """,
            params=[run_id],
        )
        if not rows:
            raise ValueError(f"Run {run_id} not found in RUN_STATUS")

        status = _normalize_status(rows[0][0])
        if status in {"COMPLETED", "FAILED", "CANCELLED"}:
            logger.info("Run %s already terminal (%s); skip STOP", run_id, status)
            return

        ctx = self._active_runs.get(run_id)
        if ctx:
            ctx.stopping = True

        drain_timeout_seconds = 120.0
        local_sigterm_after_seconds = 10.0

        if status != "CANCELLING":
            await self._emit_control_event(
                run_id=run_id,
                event_type="STOP",
                event_data={
                    "scope": "RUN",
                    "reason": "user_requested",
                    "initiated_by": "api",
                    "drain_timeout_seconds": drain_timeout_seconds,
                    "timestamp": datetime.now(UTC).isoformat(),
                },
            )

        await self._pool.execute_query(
            f"""
            UPDATE {prefix}.RUN_STATUS
            SET STATUS = 'CANCELLING',
                CANCELLATION_REASON = COALESCE(CANCELLATION_REASON, 'User requested cancellation'),
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE RUN_ID = ?
              AND STATUS NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
            """,
            params=[run_id],
        )

        if ctx and ctx.worker_procs:
            running = [proc for proc in ctx.worker_procs if proc.returncode is None]
            if running:
                try:
                    await asyncio.wait_for(
                        _wait_for_local_workers(running),
                        timeout=local_sigterm_after_seconds,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "Run %s: %d worker(s) still running after %.1fs; SIGTERM",
                        run_id,
                        len(running),
                        local_sigterm_after_seconds,
                    )
                    for proc in running:
                        if proc.returncode is None:
                            proc.terminate()
                    try:
                        await asyncio.wait_for(
                            _wait_for_local_workers(running), timeout=5.0
                        )
                    except asyncio.TimeoutError:
                        logger.warning(
                            "Run %s: workers still running after SIGTERM; SIGKILL",
                            run_id,
                        )
                        for proc in running:
                            if proc.returncode is None:
                                proc.kill()
                        await asyncio.gather(
                            *(proc.wait() for proc in running),
                            return_exceptions=True,
                        )
        else:
            await _wait_for_heartbeats(drain_timeout_seconds)

        rows = await self._pool.execute_query(
            f"""
            SELECT STATUS
            FROM {prefix}.RUN_STATUS
            WHERE RUN_ID = ?
            """,
            params=[run_id],
        )
        status = _normalize_status(rows[0][0]) if rows else ""
        if status in {"COMPLETED", "FAILED", "CANCELLED"}:
            logger.info(
                "Run %s already terminal (%s); skip CANCELLED update", run_id, status
            )
            return

        await self._pool.execute_query(
            f"""
            UPDATE {prefix}.RUN_STATUS
            SET STATUS = 'CANCELLED',
                END_TIME = COALESCE(END_TIME, CURRENT_TIMESTAMP()),
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE RUN_ID = ?
              AND STATUS NOT IN ('COMPLETED', 'FAILED', 'CANCELLED')
            """,
            params=[run_id],
        )

        await self._pool.execute_query(
            f"""
            UPDATE {prefix}.TEST_RESULTS
            SET STATUS = 'CANCELLED',
                END_TIME = COALESCE(END_TIME, CURRENT_TIMESTAMP()),
                UPDATED_AT = CURRENT_TIMESTAMP()
            WHERE TEST_ID = ? AND RUN_ID = ?
            """,
            params=[run_id, run_id],
        )

        if ctx and ctx.poll_task:
            ctx.poll_task.cancel()

    async def get_run_status(self, run_id: str) -> dict[str, Any] | None:
        """
        Fetch the current status of a run from RUN_STATUS.
        """
        prefix = f"{settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}"
        rows = await self._pool.execute_query(
            f"""
            SELECT RUN_ID, STATUS, PHASE, START_TIME, END_TIME,
                   TOTAL_WORKERS_EXPECTED, WORKERS_REGISTERED, WORKERS_ACTIVE
            FROM {prefix}.RUN_STATUS
            WHERE RUN_ID = ?
            """,
            params=[run_id],
        )
        if not rows:
            return None

        row = rows[0]
        return {
            "run_id": row[0],
            "status": row[1],
            "phase": row[2],
            "start_time": row[3],
            "end_time": row[4],
            "total_workers_expected": row[5],
            "workers_registered": row[6],
            "workers_active": row[7],
        }


# Global singleton instance
orchestrator = OrchestratorService()
