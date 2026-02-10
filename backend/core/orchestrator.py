from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import shutil
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

from backend.config import settings
from backend.connectors import snowflake_pool
from backend.core import results_store
from backend.core.live_controller_state import live_controller_state
from backend.core.live_metrics_cache import live_metrics_cache
from backend.core.mode_config import ModeConfig
from backend.core.pool_refresh import refresh_key_pool_after_writes, test_had_writes
from backend.core.postgres_stats import (
    PgCapabilities,
    PgStatDelta,
    PgStatSnapshot,
    capture_pg_stat_snapshot,
    compute_snapshot_delta,
    delta_to_dict,
    get_pg_capabilities,
)
from backend.core.results_store import (
    enrich_query_executions_with_retry,
    insert_test_logs,
    update_enrichment_status,
    update_postgres_enrichment,
    update_test_overhead_percentiles,
)
from backend.core.test_log_stream import (
    CURRENT_TEST_ID,
    CURRENT_WORKER_ID,
    TestLogQueueHandler,
)
from backend.core.table_managers import create_table_manager
from backend.core.orchestrator_modules.qps_controller import (
    QPSControllerState,
    evaluate_qps_scaling,
)
from backend.models.test_config import TableConfig, TableType, TestScenario

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
    Distribute total_target threads across workers using pack strategy.

    Algorithm (Pack Strategy):
    - Fill workers to max_threads_per_worker first
    - Last worker gets the remainder
    - Example: 100 threads, 7 workers, max 15 -> 6@15 + 1@10

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

    per_worker_qps = None
    if load_mode == "QPS" and target_qps_total is not None:
        per_worker_qps = float(target_qps_total) / float(worker_count)

    targets: dict[str, dict[str, Any]] = {}

    # Pack strategy: fill workers to effective_cap, last worker gets remainder
    if effective_cap is not None and int(effective_cap) > 0:
        cap = int(effective_cap)
        remaining = target_total
        for idx in range(worker_count):
            # Fill to cap, or take what's left
            target = min(cap, remaining)
            remaining -= target
            entry: dict[str, Any] = {
                "target_connections": int(target),
                "worker_group_id": int(idx),
            }
            if per_worker_qps is not None:
                entry["target_qps"] = float(per_worker_qps)
            targets[f"worker-{idx}"] = entry
    else:
        # No cap - balance evenly (original behavior)
        base = target_total // worker_count
        remainder = target_total % worker_count
        for idx in range(worker_count):
            target = base + (1 if idx < remainder else 0)
            entry = {
                "target_connections": int(target),
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
    worker_stream_tasks: list[asyncio.Task] = field(default_factory=list)
    started_at: datetime | None = None
    stopping: bool = False
    log_queue: asyncio.Queue | None = None
    log_handler: logging.Handler | None = None
    log_drain_task: asyncio.Task | None = None
    did_resume_warehouse: bool = False
    # PostgreSQL statistics for enrichment (3-point capture)
    pg_capabilities: PgCapabilities | None = None
    pg_snapshot_before_warmup: PgStatSnapshot | None = None
    pg_snapshot_after_warmup: PgStatSnapshot | None = None
    pg_snapshot_after_measurement: PgStatSnapshot | None = None
    pg_delta_warmup: PgStatDelta | None = None  # warmup phase only
    pg_delta_measurement: PgStatDelta | None = None  # measurement phase only
    pg_delta_total: PgStatDelta | None = None  # warmup + measurement


async def _stream_worker_output(
    proc: asyncio.subprocess.Process,
    worker_id: int,
    run_id: str,
) -> None:
    """Stream worker stdout/stderr to console in real-time.

    NOTE: We print directly instead of using logger.log() to avoid duplicate
    entries in TEST_LOGS - workers already persist their own logs via
    TestLogQueueHandler.
    """
    import sys

    async def _stream_pipe(stream: asyncio.StreamReader | None) -> None:
        if stream is None:
            return
        while True:
            line = await stream.readline()
            if not line:
                break
            text = line.decode().rstrip()
            if text:
                print(f"[worker-{worker_id}] {text}", file=sys.stderr)

    tasks = []
    if proc.stdout:
        tasks.append(_stream_pipe(proc.stdout))
    if proc.stderr:
        tasks.append(_stream_pipe(proc.stderr))

    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


class OrchestratorService:
    """
    Central control plane for benchmark runs.
    Replaces the in-memory TestRegistry with a Snowflake-backed state machine.
    """

    def __init__(self) -> None:
        self._pool = snowflake_pool.get_default_pool()
        self._background_tasks: set[asyncio.Task] = set()
        self._active_runs: dict[str, RunContext] = {}

    async def _get_postgres_connection(self, scenario_config: dict[str, Any]):
        """
        Create a Postgres connection for capturing pg_stat_statements snapshots.

        Args:
            scenario_config: The scenario configuration containing target info

        Returns:
            asyncpg connection or None if connection fails
        """
        import asyncpg
        import ssl

        from backend.connectors.postgres_pool import get_postgres_connection_params
        from backend.core import connection_manager

        target_cfg = scenario_config.get("target", {})
        table_type = str(target_cfg.get("table_type", "")).strip().upper()
        database = str(target_cfg.get("database", "")).strip()
        connection_id = target_cfg.get("connection_id")

        if not database:
            logger.warning("No database configured for Postgres stats capture")
            return None

        try:
            # Stats connections should always use direct PostgreSQL (not PgBouncer),
            # since they query system views like pg_stat_statements
            
            if connection_id:
                # Use stored connection credentials
                conn_params = await connection_manager.get_connection_for_pool(connection_id)
                if not conn_params:
                    logger.warning(f"Connection {connection_id} not found for stats capture")
                    return None
                
                # Create SSL context for Snowflake Postgres (self-signed certs)
                ssl_context = ssl.create_default_context()
                ssl_context.check_hostname = False
                ssl_context.verify_mode = ssl.CERT_NONE
                
                conn = await asyncpg.connect(
                    host=conn_params["host"],
                    port=conn_params["port"],  # Direct port, not PgBouncer
                    database=database,
                    user=conn_params["user"],
                    password=conn_params["password"],
                    timeout=10.0,
                    ssl=ssl_context,
                )
            else:
                # Fallback to environment variables (backward compat)
                params = get_postgres_connection_params(use_pgbouncer=False)
                conn = await asyncpg.connect(
                    host=params["host"],
                    port=params["port"],
                    database=database,
                    user=params["user"],
                    password=params["password"],
                    timeout=10.0,
                )
            return conn
        except Exception as e:
            logger.warning(
                "Failed to create Postgres connection for stats: %s: %s",
                type(e).__name__,
                e or "(no message)",
            )
            return None

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

    async def generate_preflight_warnings(
        self, scenario_config: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """
        Generate pre-flight warnings for a test configuration.

        Checks for configurations that are likely to hit Snowflake limits,
        particularly the 20-waiter lock limit on standard tables.

        Args:
            scenario_config: The full scenario configuration dict

        Returns:
            List of warning dicts with keys: severity, title, message, recommendations
        """
        warnings: list[dict[str, Any]] = []

        # Extract relevant config values
        table_type = str(scenario_config.get("table_type", "standard")).lower()
        workload_cfg = scenario_config.get("workload", {})
        custom_queries = workload_cfg.get("custom_queries", [])
        total_threads = int(scenario_config.get("total_threads", 10))
        table_name = str(scenario_config.get("table_name", ""))

        # Calculate write percentage from CUSTOM query weights.
        # Runtime is CUSTOM-only, but we tolerate legacy key names for old rows.
        write_pct = 0.0
        if isinstance(custom_queries, list):
            for q in custom_queries:
                if not isinstance(q, dict):
                    continue
                kind = str(q.get("query_kind") or q.get("kind") or "").upper()
                raw_weight = q.get("weight_pct", q.get("weight", 0))
                try:
                    weight = float(raw_weight)
                except (TypeError, ValueError):
                    weight = 0.0
                normalized_weight = weight / 100.0 if weight > 1.0 else weight
                if kind in ("INSERT", "UPDATE", "DELETE") and normalized_weight > 0:
                    write_pct += normalized_weight
        write_pct = max(0.0, min(write_pct, 1.0))

        # Calculate expected concurrent writers
        expected_concurrent_writes = total_threads * write_pct

        # Check for lock contention risk on standard tables
        # Snowflake limit: 20 statements waiting for a table lock
        LOCK_WAITER_LIMIT = 20

        if table_type == "standard" and expected_concurrent_writes > LOCK_WAITER_LIMIT:
            warnings.append(
                {
                    "severity": "high",
                    "title": "Lock Contention Risk",
                    "message": (
                        f"Standard tables use TABLE-LEVEL LOCKING for writes. "
                        f"With {total_threads} threads and ~{write_pct * 100:.0f}% writes, "
                        f"you may have ~{expected_concurrent_writes:.0f} concurrent write attempts. "
                        f"Snowflake's lock waiter limit is {LOCK_WAITER_LIMIT} statements. "
                        f"If any write takes >1 second, you WILL hit SF_LOCK_WAITER_LIMIT errors."
                    ),
                    "recommendations": [
                        "Use a HYBRID table for concurrent write workloads (row-level locking)",
                        f"Reduce concurrency to ≤{int(LOCK_WAITER_LIMIT / write_pct) if write_pct > 0 else total_threads} threads",
                        "For read-only benchmarking, keep CUSTOM and set INSERT/UPDATE mix to 0%",
                    ],
                    "details": {
                        "table_type": table_type,
                        "table_name": table_name,
                        "total_threads": total_threads,
                        "write_percentage": round(write_pct * 100, 1),
                        "expected_concurrent_writes": round(
                            expected_concurrent_writes, 1
                        ),
                        "lock_waiter_limit": LOCK_WAITER_LIMIT,
                    },
                }
            )
        elif (
            table_type == "standard"
            and expected_concurrent_writes > LOCK_WAITER_LIMIT * 0.5
        ):
            # Warning for approaching the limit (>50% of limit)
            warnings.append(
                {
                    "severity": "medium",
                    "title": "Potential Lock Contention",
                    "message": (
                        f"With {total_threads} threads and ~{write_pct * 100:.0f}% writes on a STANDARD table, "
                        f"you may have ~{expected_concurrent_writes:.0f} concurrent write attempts. "
                        f"This approaches Snowflake's {LOCK_WAITER_LIMIT}-waiter limit. "
                        f"Slow writes could trigger SF_LOCK_WAITER_LIMIT errors."
                    ),
                    "recommendations": [
                        "Monitor for SF_LOCK_WAITER_LIMIT errors during the run",
                        "Consider using a HYBRID table for better write concurrency",
                    ],
                    "details": {
                        "table_type": table_type,
                        "table_name": table_name,
                        "total_threads": total_threads,
                        "write_percentage": round(write_pct * 100, 1),
                        "expected_concurrent_writes": round(
                            expected_concurrent_writes, 1
                        ),
                        "lock_waiter_limit": LOCK_WAITER_LIMIT,
                    },
                }
            )

        return warnings

    async def get_preflight_warnings(self, run_id: str) -> list[dict[str, Any]]:
        """
        Get pre-flight warnings for a prepared run.

        Args:
            run_id: The run ID to check

        Returns:
            List of warning dicts
        """
        prefix = f"{settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}"

        rows = await self._pool.execute_query(
            f"""
            SELECT SCENARIO_CONFIG
            FROM {prefix}.RUN_STATUS
            WHERE RUN_ID = ?
            """,
            params=[run_id],
        )
        if not rows:
            raise ValueError(f"Run {run_id} not found")

        scenario_config_raw = rows[0][0]
        if isinstance(scenario_config_raw, str):
            scenario_config = json.loads(scenario_config_raw)
        elif isinstance(scenario_config_raw, dict):
            scenario_config = scenario_config_raw
        else:
            scenario_config = {}

        return await self.generate_preflight_warnings(scenario_config)

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
        is_postgres = table_type == TableType.POSTGRES.value.upper()
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
        # Use ModeConfig for unified parsing (strict=True raises ValueError on invalid)
        _mode_cfg = ModeConfig.from_config(scaling_cfg=scaling_cfg, scenario=scenario, strict=True)
        scaling_mode = _mode_cfg.scaling_mode

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

        # Universal default for max connections per worker.
        # Must match frontend DEFAULT_MAX_CONN in display.js for UI consistency.
        DEFAULT_MAX_THREADS_PER_WORKER = 200

        if max_threads_per_worker is None:
            max_threads_per_worker = DEFAULT_MAX_THREADS_PER_WORKER

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

        load_mode = _mode_cfg.load_mode

        # Extract thread targets early for optimal worker count calculation.
        # In FIND_MAX mode, size workers from start_concurrency (not legacy total_threads).
        total_threads = int(scenario.total_threads)
        find_max_start_threads = int(scenario.start_concurrency)
        worker_sizing_target = (
            find_max_start_threads
            if load_mode == "FIND_MAX_CONCURRENCY"
            else total_threads
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
        elif scaling_mode in {"AUTO", "BOUNDED"} and max_threads_per_worker is not None:
            # For AUTO and BOUNDED modes, compute optimal worker count
            # ceil(worker_sizing_target / max_threads_per_worker) gives minimum workers needed
            optimal_workers = math.ceil(worker_sizing_target / max_threads_per_worker)
            # Clamp between min_workers and max_workers
            worker_group_count = max(optimal_workers, min_workers)
            if max_workers is not None:
                worker_group_count = min(worker_group_count, max_workers)
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

        if scaling_mode == "FIXED" and load_mode == "CONCURRENCY":
            total_threads = int(min_workers) * int(min_threads_per_worker)

        def _resolve_effective_max_threads_per_worker() -> int | None:
            per_worker_cap = None
            # For QPS mode, total_threads is just the starting concurrency, not a cap.
            # The max threads per worker should come from scaling config (max_connections).
            # Only non-QPS modes should use total_threads as a cap.
            if load_mode != "QPS" and per_worker_threads is not None:
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
                target_threads = (
                    find_max_start_threads
                    if load_mode == "FIND_MAX_CONCURRENCY"
                    else total_threads
                )
                if target_threads > max_total:
                    raise ValueError(
                        f"Target threads {target_threads} unreachable with max {max_workers} workers × {effective_max_threads_per_worker} threads"
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
                "connection_id": table_cfg.connection_id,
            },
            "workload": {
                "load_mode": load_mode,
                "concurrent_connections": int(worker_sizing_target),
                "duration_seconds": int(scenario.duration_seconds),
                "warmup_seconds": int(scenario.warmup_seconds),
                "target_qps": (
                    float(scenario.target_qps)
                    if scenario.target_qps is not None
                    else None
                ),
                "starting_threads": (
                    float(scenario.starting_threads)
                    if scenario.starting_threads is not None
                    else None
                ),
                "max_thread_increase": (
                    float(scenario.max_thread_increase)
                    if scenario.max_thread_increase is not None
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

        return run_id

    async def start_run(self, run_id: str) -> None:
        """
        Transitions run to RUNNING and spawns workers.

        Steps:
        1. Setup log streaming (context vars + handler) FIRST so all logs are captured
        2. Fetch SCENARIO_CONFIG from RUN_STATUS
        3. Update RUN_STATUS to RUNNING and set START_TIME
        4. Emit START event to RUN_CONTROL_EVENTS
        5. Spawn workers as local subprocesses
        6. Start background poll loop for this run
        """
        # Setup log streaming FIRST, before any logger calls, so all orchestrator
        # logs for this run are captured and streamed to the UI in real-time.
        log_queue: asyncio.Queue = asyncio.Queue(maxsize=10_000)
        log_handler = TestLogQueueHandler(test_id=run_id, queue=log_queue)
        log_handler.setLevel(logging.DEBUG)  # Capture all levels

        # Add handler only to root logger - module logger propagates to root by default
        # Adding to both causes duplicate log entries
        logging.getLogger().addHandler(log_handler)

        # Set context vars so TestLogQueueHandler.emit() captures logs for this test
        CURRENT_TEST_ID.set(run_id)
        CURRENT_WORKER_ID.set("ORCHESTRATOR")

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

        # Increment template usage count now that test is actually starting
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
            logger.warning(
                "Failed to increment usage count for template %s: %s", template_id, e
            )

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

        # Store log queue/handler in context (already created at top of function)
        ctx.log_queue = log_queue
        ctx.log_handler = log_handler

        # Start background task to drain log queue and persist to Snowflake
        drain_task = asyncio.create_task(
            self._drain_log_queue(run_id, log_queue), name=f"log-drain-{run_id}"
        )
        ctx.log_drain_task = drain_task
        self._background_tasks.add(drain_task)
        drain_task.add_done_callback(self._background_tasks.discard)

        # 4. For Interactive/Hybrid tables, ensure the warehouse is running before spawning workers
        target_cfg = scenario_config.get("target") or {}
        table_type = str(target_cfg.get("table_type") or "").strip().upper()
        warehouse_name = str(target_cfg.get("warehouse") or "").strip().upper()
        logger.info(
            "Pre-spawn warehouse check: table_type=%s, warehouse=%s",
            table_type,
            warehouse_name,
        )
        if table_type in ("HYBRID", "INTERACTIVE") and warehouse_name:
            logger.info(
                "Detected Interactive/Hybrid table - ensuring warehouse %s is running...",
                warehouse_name,
            )
            await self._ensure_warehouse_running(ctx, warehouse_name)
        else:
            logger.info(
                "Skipping warehouse resume: table_type=%s not in (HYBRID, INTERACTIVE) or no warehouse",
                table_type,
            )

        # 5. Pre-validate tables (once, before spawning workers)
        # This avoids N workers each doing identical schema validation queries.
        validation_results = await self._pre_validate_tables(scenario_config)
        if validation_results:
            scenario_config["validation_results"] = validation_results
            # Update RUN_STATUS with enriched scenario_config
            await self._pool.execute_query(
                f"""
                UPDATE {prefix}.RUN_STATUS
                SET SCENARIO_CONFIG = PARSE_JSON(?),
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE RUN_ID = ?
                """,
                params=[json.dumps(scenario_config), run_id],
            )
            logger.info(
                "Pre-validated %d table(s), cached in SCENARIO_CONFIG",
                len(validation_results),
            )

        # 6. Spawn workers as local subprocesses
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

    async def _ensure_warehouse_running(
        self, ctx: RunContext, warehouse_name: str
    ) -> None:
        """
        Ensure the warehouse is running for Interactive/Hybrid tables.

        Checks warehouse state and resumes if suspended.
        Tracks whether we resumed it so we can suspend on teardown.
        """
        try:
            rows = await self._pool.execute_query(
                f"SHOW WAREHOUSES LIKE '{warehouse_name}'"
            )
            state = None
            for row in rows or []:
                # SHOW WAREHOUSES columns: name(0), state(1), type(2), size(3), ...
                if len(row) >= 2 and str(row[0]).upper() == warehouse_name:
                    state = str(row[1]).upper()
                    break

            logger.info("Warehouse %s state: %s", warehouse_name, state)

            if state == "SUSPENDED":
                logger.info(
                    "Resuming suspended warehouse %s for Interactive/Hybrid table...",
                    warehouse_name,
                )
                await self._pool.execute_query(
                    f"ALTER WAREHOUSE {warehouse_name} RESUME"
                )
                ctx.did_resume_warehouse = True
                logger.info(
                    "✅ Resumed warehouse %s, waiting for it to be ready...",
                    warehouse_name,
                )
                await asyncio.sleep(3)
            elif state == "RESUMING":
                logger.info("Warehouse %s is resuming, waiting...", warehouse_name)
                ctx.did_resume_warehouse = True
                await asyncio.sleep(5)
            elif state == "STARTED":
                logger.info("Warehouse %s is already running", warehouse_name)
            else:
                logger.warning(
                    "Unknown warehouse state %s for %s", state, warehouse_name
                )
        except Exception as e:
            logger.error(
                "Failed to ensure warehouse %s is running: %s", warehouse_name, e
            )

    async def _pre_validate_tables(
        self, scenario_config: dict[str, Any]
    ) -> dict[str, dict[str, Any]]:
        """
        Pre-validate tables before spawning workers.

        This runs schema validation and stats collection ONCE in the orchestrator,
        caching results so workers can skip redundant validation queries.

        Args:
            scenario_config: The scenario configuration dict

        Returns:
            Dict mapping table names to validation results (columns, object_type, stats)
        """
        target_cfg = scenario_config.get("target") or {}
        table_name = str(target_cfg.get("table_name") or "").strip()
        if not table_name:
            logger.warning("No table_name in scenario_config, skipping pre-validation")
            return {}

        table_type_str = str(target_cfg.get("table_type") or "").strip().upper()
        database = str(target_cfg.get("database") or "").strip()
        schema = str(target_cfg.get("schema") or "").strip()
        connection_id = target_cfg.get("connection_id")

        # Map string to TableType enum
        type_map = {
            "STANDARD": TableType.STANDARD,
            "HYBRID": TableType.HYBRID,
            "INTERACTIVE": TableType.INTERACTIVE,
            "POSTGRES": TableType.POSTGRES,
        }
        table_type = type_map.get(table_type_str)
        if not table_type:
            logger.warning(
                "Unknown table_type '%s', skipping pre-validation", table_type_str
            )
            return {}

        try:
            # Build TableConfig for the target table
            # columns is required but will be populated during validate_schema()
            config = TableConfig(
                name=table_name,
                table_type=table_type,
                database=database or None,
                schema_name=schema or None,
                connection_id=connection_id,
                columns={},  # Will be populated by validate_schema()
            )

            # Create table manager and run validation
            manager = create_table_manager(config)

            # For Snowflake types, inject the orchestrator's pool
            if table_type in (
                TableType.STANDARD,
                TableType.HYBRID,
                TableType.INTERACTIVE,
            ):
                manager.pool = self._pool

            logger.info(
                "Pre-validating table: %s (type=%s)", table_name, table_type_str
            )
            ok = await manager.setup()
            if not ok:
                logger.error("Pre-validation failed for %s", table_name)
                return {}

            # Extract validation results to cache
            results = {
                table_name: {
                    "columns": dict(manager.config.columns)
                    if manager.config.columns
                    else {},
                    "object_type": manager.object_type,
                    "stats": dict(manager.stats) if manager.stats else {},
                }
            }
            logger.info(
                "✅ Pre-validation complete: %s (object_type=%s, rows=%s)",
                table_name,
                manager.object_type,
                manager.stats.get("row_count"),
            )
            return results

        except Exception as e:
            logger.error("Pre-validation error for %s: %s", table_name, e)
            return {}

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

            stream_task = asyncio.create_task(
                _stream_worker_output(proc, group_id, ctx.run_id)
            )
            ctx.worker_stream_tasks.append(stream_task)

        logger.info("Spawned %d workers for run %s", ctx.worker_group_count, ctx.run_id)

    async def _spawn_single_worker(
        self, ctx: RunContext, group_id: int, total_workers: int
    ) -> None:
        """
        Spawn a single additional worker subprocess.
        Used for dynamic scaling in BOUNDED mode when existing workers hit capacity.
        """
        uv_bin = _uv_available()
        env = dict(os.environ)
        env["PYTHONUNBUFFERED"] = "1"

        worker_id = f"worker-{group_id}"

        if uv_bin:
            cmd = [uv_bin, "run", "python", "scripts/run_worker.py"]
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
                str(total_workers),
            ]
        )

        logger.info(
            "Spawning additional worker %s for run %s (group %d/%d)",
            worker_id,
            ctx.run_id,
            group_id + 1,
            total_workers,
        )

        proc = await asyncio.create_subprocess_exec(
            *cmd,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        ctx.worker_procs.append(proc)

        stream_task = asyncio.create_task(
            _stream_worker_output(proc, group_id, ctx.run_id)
        )
        ctx.worker_stream_tasks.append(stream_task)

        ctx.worker_group_count = total_workers

    async def _drain_log_queue(self, run_id: str, queue: asyncio.Queue) -> None:
        """
        Background task that drains the log queue and persists logs to Snowflake.
        Runs until the run completes or is cancelled.
        """
        # Ensure context vars are set so any logs from this task are captured
        CURRENT_TEST_ID.set(run_id)
        CURRENT_WORKER_ID.set("ORCHESTRATOR")

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
        # Ensure context vars are set so logs from this task are captured
        CURRENT_TEST_ID.set(run_id)
        CURRENT_WORKER_ID.set("ORCHESTRATOR")

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
        # Use ModeConfig for unified parsing (strict=False uses defaults for invalid)
        _orch_mode_cfg = ModeConfig.from_config(
            scaling_cfg=scaling_cfg, workload_cfg=workload_cfg, strict=False
        )
        load_mode = _orch_mode_cfg.load_mode
        scaling_mode = _orch_mode_cfg.scaling_mode
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
        find_max_enabled = load_mode == "FIND_MAX_CONCURRENCY"
        worker_group_count = max(1, int(ctx.worker_group_count or 1))

        per_worker_threads = _coerce_optional_int(
            scaling_cfg.get("per_worker_connections")
            if scaling_cfg.get("per_worker_connections") is not None
            else scaling_cfg.get("per_worker_capacity")
        )
        per_worker_cap = per_worker_threads
        if per_worker_cap is None:
            if find_max_enabled:
                # FIND_MAX mode: Use scaling.max_connections if set, otherwise use a high default
                # to allow unrestricted scaling. DO NOT use workload.concurrent_connections
                # as that may be a low default value (10) from the model.
                per_worker_cap = _coerce_optional_int(
                    scaling_cfg.get("max_connections"), allow_unbounded=True
                ) or 200  # Default to 200 per worker for FIND_MAX exploration
            else:
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
        root_cfg = ctx.scenario_config or {}
        find_max_start = (
            _coerce_optional_int(find_max_cfg.get("start_concurrency"))
            or _coerce_optional_int(root_cfg.get("start_concurrency"))
            or 1
        )
        find_max_increment = (
            _coerce_optional_int(find_max_cfg.get("concurrency_increment"))
            or _coerce_optional_int(root_cfg.get("concurrency_increment"))
            or 1
        )
        find_max_step_seconds = (
            _coerce_optional_int(find_max_cfg.get("step_duration_seconds"))
            or _coerce_optional_int(root_cfg.get("step_duration_seconds"))
            or 30
        )
        find_max_qps_stability_pct = float(
            find_max_cfg.get("qps_stability_pct")
            or root_cfg.get("qps_stability_pct")
            or 5.0
        )
        find_max_latency_stability_pct = float(
            find_max_cfg.get("latency_stability_pct")
            or root_cfg.get("latency_stability_pct")
            or 20.0
        )
        find_max_max_error_pct = float(
            find_max_cfg.get("max_error_rate_pct")
            or root_cfg.get("max_error_rate_pct")
            or 1.0
        )
        target_qps_total = _coerce_optional_float(workload_cfg.get("target_qps"))
        # Support both new and old field names for backwards compatibility
        starting_threads = _coerce_optional_float(
            workload_cfg.get("starting_threads") or workload_cfg.get("starting_qps")
        )
        max_thread_increase = _coerce_optional_float(
            workload_cfg.get("max_thread_increase") or workload_cfg.get("max_qps_increase")
        )

        # Calculate max_concurrency ceiling for FIND_MAX mode
        # IMPORTANT: For FIND_MAX mode, we need a high ceiling to discover the true maximum.
        # The ceiling is NOT the target - it's the upper bound for exploration.
        _configured_concurrent_connections = _coerce_optional_int(
            workload_cfg.get("concurrent_connections")
        )
        if load_mode == "FIND_MAX_CONCURRENCY":
            # FIND_MAX mode: Use a high ceiling to allow discovering the true max.
            # The algorithm stops when degradation is detected, not at this ceiling.
            # CRITICAL: Ignore concurrent_connections - it's for CONSTANT load mode.
            max_concurrency = 10000
        else:
            # Other modes: Use concurrent_connections or fall back to find_max_start
            max_concurrency = _configured_concurrent_connections or find_max_start
        max_concurrency = max(find_max_start, max_concurrency)

        # Only apply thread/worker caps when explicitly configured (BOUNDED mode)
        # CRITICAL: For FIND_MAX mode, these caps should NOT limit exploration.
        # FIND_MAX needs freedom to scale up and find the true max through degradation detection.
        if scaling_mode == "BOUNDED" and load_mode != "FIND_MAX_CONCURRENCY":
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
        # For FIND_MAX mode, bounded_max_configured should always be False
        # so the algorithm continues exploring until degradation is detected
        bounded_max_configured = (
            scaling_mode == "BOUNDED"
            and load_mode != "FIND_MAX_CONCURRENCY"
            and (max_workers is not None or max_threads_per_worker is not None)
        )

        # DEBUG: Log FIND_MAX configuration
        if load_mode == "FIND_MAX_CONCURRENCY":
            logger.info(
                "FIND_MAX config for run %s: max_concurrency=%s, bounded_max_configured=%s, "
                "scaling_mode=%s, max_workers=%s, effective_max_threads_per_worker=%s, "
                "concurrent_connections=%s, find_max_start=%s, find_max_increment=%s",
                run_id,
                max_concurrency,
                bounded_max_configured,
                scaling_mode,
                max_workers,
                effective_max_threads_per_worker,
                _configured_concurrent_connections,
                find_max_start,
                find_max_increment,
            )

        last_metrics_ts: datetime | None = None
        last_heartbeat_update: datetime | None = None
        last_parent_rollup_status: str | None = None
        parent_rollup_interval_active_seconds = 10.0
        last_parent_rollup_mono: float | None = None
        parent_rollup_task: asyncio.Task | None = None
        find_max_history_tasks: set[asyncio.Task[Any]] = set()
        run_status_rollup_interval_active_seconds = 4.0
        last_run_status_rollup_mono: float | None = None
        last_warehouse_poll_mono: float | None = None
        warehouse_poll_task: asyncio.Task | None = None
        worker_health_poll_interval_seconds = 2.0
        last_worker_health_poll_mono: float | None = None
        worker_health_task: asyncio.Task[tuple[int, int, int, int, int, Any, float | None, float | None]] | None = None
        workers_registered = 0
        workers_active = 0
        workers_completed = 0
        dead_workers = 0
        workers_ready = 0
        heartbeat_updated_at = None
        max_cpu_seen = None
        max_memory_seen = None
        bounds_under_target_intervals = 0

        # QPS controller state for orchestrator-driven scaling
        qps_controller_state: QPSControllerState | None = None
        qps_controller_step_number = 0
        qps_controller_step_history: list[dict[str, Any]] = []
        last_qps_scaling_time: float | None = None
        last_qps_evaluation_time: float | None = None

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
        find_max_backoff_attempted = False
        find_max_is_backoff_step = False  # Track if CURRENT step is a backoff step
        find_max_termination_reason: str | None = None
        find_max_failed_concurrency: int | None = None  # Track where degradation occurred
        find_max_midpoint_attempted = False  # Track if midpoint has been tried
        find_max_running_state_persist_interval_seconds = 3.0
        last_find_max_running_state_persist_mono: float | None = None

        def _track_parent_rollup_task(task: asyncio.Task[Any]) -> None:
            nonlocal parent_rollup_task

            def _done(t: asyncio.Task[Any]) -> None:
                nonlocal parent_rollup_task
                parent_rollup_task = None
                try:
                    t.result()
                except Exception as exc:
                    logger.debug(
                        "Background parent rollup failed for %s: %s",
                        run_id,
                        exc,
                    )

            task.add_done_callback(_done)

        def _track_find_max_history_task(
            task: asyncio.Task[Any], *, step_number: int
        ) -> None:
            find_max_history_tasks.add(task)

            def _done(t: asyncio.Task[Any]) -> None:
                find_max_history_tasks.discard(t)
                try:
                    t.result()
                    logger.info(
                        "FIND_MAX step %d: history insert SUCCESS", step_number
                    )
                except Exception as exc:
                    logger.error(
                        "FIND_MAX step %d: history insert FAILED: %s",
                        step_number,
                        exc,
                    )

            task.add_done_callback(_done)

        async def _refresh_worker_health_snapshot() -> tuple[
            int, int, int, int, int, Any, float | None, float | None
        ]:
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
                workers_registered_raw,
                workers_active_raw,
                workers_completed_raw,
                dead_workers_raw,
                workers_ready_raw,
                heartbeat_updated_at_raw,
                max_cpu_seen_raw,
                max_memory_seen_raw,
            ) = heartbeat_rows[0] if heartbeat_rows else (None,) * 8

            return (
                int(workers_registered_raw or 0),
                int(workers_active_raw or 0),
                int(workers_completed_raw or 0),
                int(dead_workers_raw or 0),
                int(workers_ready_raw or 0),
                heartbeat_updated_at_raw,
                float(max_cpu_seen_raw) if max_cpu_seen_raw is not None else None,
                float(max_memory_seen_raw) if max_memory_seen_raw is not None else None,
            )

        def _apply_worker_health_snapshot(
            snapshot: tuple[int, int, int, int, int, Any, float | None, float | None]
        ) -> None:
            nonlocal workers_registered, workers_active, workers_completed
            nonlocal dead_workers, workers_ready, heartbeat_updated_at
            nonlocal max_cpu_seen, max_memory_seen
            (
                workers_registered,
                workers_active,
                workers_completed,
                dead_workers,
                workers_ready,
                heartbeat_updated_at,
                max_cpu_seen,
                max_memory_seen,
            ) = snapshot

        # Postgres stats capture setup
        is_postgres_test = str(target_cfg.get("table_type", "")).strip().upper() == "POSTGRES"
        pg_stats_conn = None
        pg_stats_enabled = False

        if is_postgres_test:
            try:
                pg_stats_conn = await self._get_postgres_connection(ctx.scenario_config)
                if pg_stats_conn:
                    ctx.pg_capabilities = await get_pg_capabilities(pg_stats_conn)
                    pg_stats_enabled = ctx.pg_capabilities.pg_stat_statements_available
                    if pg_stats_enabled:
                        logger.info(
                            "pg_stat_statements enabled for run %s (track_io_timing=%s)",
                            run_id,
                            ctx.pg_capabilities.track_io_timing,
                        )
                    else:
                        logger.info(
                            "pg_stat_statements not available for run %s - skipping enrichment",
                            run_id,
                        )
            except Exception as e:
                logger.warning(
                    "Failed to check Postgres capabilities for %s: %s", run_id, e
                )
                pg_stats_enabled = False

        async def _capture_postgres_measurement_snapshot(trigger: str) -> None:
            """Capture final pg_stat snapshot and compute deltas once per run."""
            if not (pg_stats_enabled and pg_stats_conn):
                return
            if ctx.pg_snapshot_after_measurement is not None:
                return
            try:
                ctx.pg_snapshot_after_measurement = await capture_pg_stat_snapshot(
                    pg_stats_conn
                )
                logger.info(
                    "Captured pg_stat_statements snapshot 3 (after_measurement) for %s via %s: %d queries",
                    run_id,
                    trigger,
                    len(ctx.pg_snapshot_after_measurement.stats),
                )
                baseline_snapshot = (
                    ctx.pg_snapshot_after_warmup
                    if ctx.pg_snapshot_after_warmup
                    else ctx.pg_snapshot_before_warmup
                )
                if baseline_snapshot:
                    ctx.pg_delta_measurement = compute_snapshot_delta(
                        baseline_snapshot,
                        ctx.pg_snapshot_after_measurement,
                    )
                if ctx.pg_snapshot_before_warmup:
                    ctx.pg_delta_total = compute_snapshot_delta(
                        ctx.pg_snapshot_before_warmup,
                        ctx.pg_snapshot_after_measurement,
                    )
                logger.info(
                    "Computed pg_stat deltas for %s: measurement=%s, total=%s",
                    run_id,
                    ctx.pg_delta_measurement is not None,
                    ctx.pg_delta_total is not None,
                )
            except Exception as e:
                logger.warning(
                    "Failed to capture pg_stat snapshot 3 for %s (%s): %s",
                    run_id,
                    trigger,
                    e,
                )

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
            # FIND_MAX step transitions should apply promptly; a long target ramp
            # makes phase handoffs look delayed in the UI.
            ramp_seconds = 0
            if str(load_mode or "").upper() != "FIND_MAX_CONCURRENCY":
                ramp_seconds = 5

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
                    "ramp_seconds": ramp_seconds,
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

        async def _ensure_workers_for_target(target: int) -> int:
            """
            Ensure enough workers exist to handle the target concurrency.
            Spawns additional workers if needed and max_workers allows.
            Returns the (possibly increased) worker count.
            """
            nonlocal worker_group_count

            effective_cap = per_worker_threads
            if max_threads_per_worker is not None:
                effective_cap = (
                    min(int(max_threads_per_worker), int(per_worker_threads))
                    if per_worker_threads is not None
                    else int(max_threads_per_worker)
                )

            if effective_cap is None or effective_cap <= 0:
                return worker_group_count

            needed_workers = math.ceil(target / effective_cap)
            needed_workers = max(needed_workers, min_workers)

            if max_workers is not None:
                needed_workers = min(needed_workers, max_workers)

            if needed_workers <= worker_group_count:
                return worker_group_count

            workers_to_spawn = needed_workers - worker_group_count
            logger.info(
                "BOUNDED scaling: target=%d requires %d workers (have %d, spawning %d more)",
                target,
                needed_workers,
                worker_group_count,
                workers_to_spawn,
            )

            for i in range(workers_to_spawn):
                new_group_id = worker_group_count + i
                await self._spawn_single_worker(ctx, new_group_id, needed_workers)

            worker_group_count = needed_workers

            # Wait for newly spawned workers to become ready (up to 30s)
            wait_deadline = asyncio.get_running_loop().time() + 30.0
            while asyncio.get_running_loop().time() < wait_deadline:
                rows = await self._pool.execute_query(
                    f"""
                    SELECT COUNT(*)
                    FROM {prefix}.WORKER_HEARTBEATS
                    WHERE RUN_ID = ?
                      AND UPPER(STATUS) IN ('READY', 'RUNNING', 'WAITING')
                    """,
                    params=[run_id],
                )
                ready_count = int(rows[0][0] or 0) if rows else 0
                if ready_count >= needed_workers:
                    logger.info(
                        "All %d workers ready after spawn (ready_count=%d)",
                        needed_workers,
                        ready_count,
                    )
                    break
                await asyncio.sleep(1.0)
            else:
                logger.warning(
                    "Timed out waiting for workers to be ready (needed=%d, ready=%d)",
                    needed_workers,
                    ready_count,
                )

            return worker_group_count

        async def _persist_find_max_state(state: dict[str, Any]) -> None:
            await live_controller_state.set_find_max_state(run_id=run_id, state=state)
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
                INSERT INTO {prefix}.CONTROLLER_STEP_HISTORY (
                    STEP_ID,
                    RUN_ID,
                    STEP_NUMBER,
                    STEP_TYPE,
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
                SELECT ?, ?, ?, 'FIND_MAX', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
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

        async def _persist_qps_controller_state(state: dict[str, Any]) -> None:
            """Persist QPS controller live state for UI display."""
            await live_controller_state.set_qps_state(run_id=run_id, state=state)
            await self._pool.execute_query(
                f"""
                UPDATE {prefix}.RUN_STATUS
                SET QPS_CONTROLLER_STATE = PARSE_JSON(?),
                    UPDATED_AT = CURRENT_TIMESTAMP()
                WHERE RUN_ID = ?
                """,
                params=[json.dumps(state), run_id],
            )

        async def _insert_controller_step_history(
            *,
            step_id: str,
            step_number: int,
            step_type: str,
            target_workers: int,
            step_start_time: datetime,
            step_end_time: datetime | None = None,
            step_duration_seconds: float | None = None,
            from_threads: int | None = None,
            to_threads: int | None = None,
            direction: str | None = None,
            total_queries: int | None = None,
            qps: float | None = None,
            target_qps: float | None = None,
            p50_latency_ms: float | None = None,
            p95_latency_ms: float | None = None,
            p99_latency_ms: float | None = None,
            error_count: int | None = None,
            error_rate: float | None = None,
            qps_vs_prior_pct: float | None = None,
            p95_vs_baseline_pct: float | None = None,
            queue_detected: bool = False,
            qps_error_pct: float | None = None,
            outcome: str | None = None,
            stop_reason: str | None = None,
        ) -> None:
            """Insert a step into the unified CONTROLLER_STEP_HISTORY table."""
            await self._pool.execute_query(
                f"""
                INSERT INTO {prefix}.CONTROLLER_STEP_HISTORY (
                    STEP_ID,
                    RUN_ID,
                    STEP_NUMBER,
                    STEP_TYPE,
                    TARGET_WORKERS,
                    FROM_THREADS,
                    TO_THREADS,
                    DIRECTION,
                    STEP_START_TIME,
                    STEP_END_TIME,
                    STEP_DURATION_SECONDS,
                    TOTAL_QUERIES,
                    QPS,
                    TARGET_QPS,
                    P50_LATENCY_MS,
                    P95_LATENCY_MS,
                    P99_LATENCY_MS,
                    ERROR_COUNT,
                    ERROR_RATE,
                    QPS_VS_PRIOR_PCT,
                    P95_VS_BASELINE_PCT,
                    QUEUE_DETECTED,
                    QPS_ERROR_PCT,
                    OUTCOME,
                    STOP_REASON
                )
                SELECT ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                """,
                params=[
                    step_id,
                    run_id,
                    int(step_number),
                    str(step_type),
                    int(target_workers),
                    int(from_threads) if from_threads is not None else None,
                    int(to_threads) if to_threads is not None else None,
                    str(direction) if direction else None,
                    step_start_time,
                    step_end_time,
                    float(step_duration_seconds) if step_duration_seconds is not None else None,
                    int(total_queries) if total_queries is not None else None,
                    float(qps) if qps is not None else None,
                    float(target_qps) if target_qps is not None else None,
                    float(p50_latency_ms) if p50_latency_ms is not None else None,
                    float(p95_latency_ms) if p95_latency_ms is not None else None,
                    float(p99_latency_ms) if p99_latency_ms is not None else None,
                    int(error_count) if error_count is not None else None,
                    float(error_rate) if error_rate is not None else None,
                    float(qps_vs_prior_pct) if qps_vs_prior_pct is not None else None,
                    float(p95_vs_baseline_pct) if p95_vs_baseline_pct is not None else None,
                    bool(queue_detected),
                    float(qps_error_pct) if qps_error_pct is not None else None,
                    str(outcome) if outcome else None,
                    stop_reason,
                ],
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
            # table_type is nested under target.table_type in scenario_config
            target_cfg = scenario_config.get("target", {})
            table_type_str = str(target_cfg.get("table_type", "")).strip().upper()
            is_postgres_test = table_type_str == "POSTGRES"

            try:
                if is_postgres_test:
                    # Postgres: Use pg_stat_statements enrichment if available
                    if ctx and ctx.pg_delta_measurement:
                        delta = ctx.pg_delta_measurement
                        totals = delta.totals
                        logger.info(
                            "Postgres enrichment for %s: %d calls, %.2fms total exec, "
                            "%.2f%% cache hit, %d query patterns",
                            test_id,
                            totals.get("calls", 0),
                            totals.get("total_exec_time", 0),
                            totals.get("cache_hit_ratio", 0) * 100,
                            totals.get("query_pattern_count", 0),
                        )
                        # Log by query kind breakdown
                        for kind, stats in delta.by_query_kind.items():
                            logger.info(
                                "  %s: %d calls, %.2fms mean exec, %.2f%% cache hit",
                                kind,
                                stats.get("calls", 0),
                                stats.get("mean_exec_time", 0),
                                stats.get("cache_hit_ratio", 0) * 100,
                            )

                        # Persist Postgres enrichment data to TEST_RESULTS
                        pg_caps_dict = None
                        if ctx.pg_capabilities:
                            pg_caps_dict = {
                                "pg_stat_statements_available": ctx.pg_capabilities.pg_stat_statements_available,
                                "track_io_timing": ctx.pg_capabilities.track_io_timing,
                                "pg_version": ctx.pg_capabilities.pg_version,
                            }
                        await update_postgres_enrichment(
                            test_id=test_id,
                            pg_delta_measurement=delta_to_dict(ctx.pg_delta_measurement)
                            if ctx.pg_delta_measurement
                            else None,
                            pg_delta_warmup=delta_to_dict(ctx.pg_delta_warmup)
                            if ctx.pg_delta_warmup
                            else None,
                            pg_delta_total=delta_to_dict(ctx.pg_delta_total)
                            if ctx.pg_delta_total
                            else None,
                            pg_capabilities=pg_caps_dict,
                        )

                        await update_enrichment_status(
                            test_id=test_id,
                            status="COMPLETED",
                            error=None,
                        )
                        logger.info(
                            "Postgres pg_stat_statements enrichment completed for %s",
                            test_id,
                        )
                    else:
                        logger.info(
                            "Skipping QUERY_HISTORY enrichment for Postgres test %s "
                            "(pg_stat_statements not available or no deltas captured)",
                            test_id,
                        )
                        await update_enrichment_status(
                            test_id=test_id,
                            status="SKIPPED",
                            error="Postgres: pg_stat_statements unavailable or no data captured",
                        )
                else:
                    logger.info("Starting enrichment for run %s (table_type=%s)", test_id, table_type_str or "unknown")
                    await enrich_query_executions_with_retry(
                        run_id=test_id,
                        target_ratio=0.90,
                        max_wait_seconds=240,
                        poll_interval_seconds=10,
                        table_type=table_type_str,
                    )
                    await update_test_overhead_percentiles(run_id=test_id)
                    await update_enrichment_status(
                        test_id=test_id, status="COMPLETED", error=None
                    )
                    logger.info("Enrichment completed for run %s", test_id)

                # Refresh value pools if test had writes (runs in parallel with enrichment completion)
                template_id = ctx.template_id if ctx else None
                if template_id and test_had_writes(scenario_config):
                    logger.info(
                        "Test %s had writes - refreshing KEY pool for template %s",
                        test_id,
                        template_id,
                    )
                    refresh_result = await refresh_key_pool_after_writes(
                        template_id=template_id,
                        scenario_config=scenario_config,
                        pool=self._pool,
                    )
                    if refresh_result.get("refreshed"):
                        logger.info(
                            "KEY pool refreshed: %d keys sampled for template %s",
                            refresh_result.get("keys_sampled", 0),
                            template_id,
                        )
                    elif refresh_result.get("error"):
                        logger.warning(
                            "KEY pool refresh skipped: %s",
                            refresh_result.get("error"),
                        )

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
                loop_cycle_started_mono = asyncio.get_running_loop().time()

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

                now_mono = asyncio.get_running_loop().time()
                if warehouse_name:
                    if (
                        last_warehouse_poll_mono is None
                        or now_mono - last_warehouse_poll_mono >= 5.0
                    ):
                        if warehouse_poll_task is None or warehouse_poll_task.done():
                            last_warehouse_poll_mono = now_mono
                            warehouse_poll_task = asyncio.create_task(
                                _poll_warehouse(elapsed_seconds)
                            )

                # Consume completed worker health refresh first (if any).
                if worker_health_task is not None and worker_health_task.done():
                    try:
                        _apply_worker_health_snapshot(worker_health_task.result())
                    except Exception as exc:
                        logger.debug(
                            "Worker health refresh failed for %s: %s", run_id, exc
                        )
                    finally:
                        worker_health_task = None

                # Worker heartbeat maintenance is relatively expensive in Snowflake.
                # During STARTING, run inline for prompt READY->RUNNING transition.
                # During active phases, refresh asynchronously to avoid blocking the
                # FIND_MAX control loop.
                should_refresh_worker_health = (
                    last_worker_health_poll_mono is None
                    or now_mono - last_worker_health_poll_mono
                    >= worker_health_poll_interval_seconds
                    or status == "STARTING"
                )
                if should_refresh_worker_health:
                    last_worker_health_poll_mono = now_mono
                    if status == "STARTING":
                        _apply_worker_health_snapshot(
                            await _refresh_worker_health_snapshot()
                        )
                    elif worker_health_task is None:
                        worker_health_task = asyncio.create_task(
                            _refresh_worker_health_snapshot()
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

                    # Capture first pg_stat_statements snapshot (before warmup/measurement)
                    # NOTE: We capture ALL queries (no filter) because PostgreSQL's pg_stat_statements
                    # strips comments during query normalization, so UB_KIND markers won't be present.
                    # Query classification happens in extract_query_kind() based on SQL patterns.
                    if (
                        pg_stats_enabled
                        and pg_stats_conn
                        and ctx.pg_snapshot_before_warmup is None
                    ):
                        try:
                            ctx.pg_snapshot_before_warmup = (
                                await capture_pg_stat_snapshot(pg_stats_conn)
                            )
                            logger.info(
                                "Captured pg_stat_statements snapshot 1 (before_warmup) for %s: %d queries",
                                run_id,
                                len(ctx.pg_snapshot_before_warmup.stats),
                            )
                        except Exception as e:
                            logger.warning(
                                "Failed to capture pg_stat snapshot 1 for %s: %s",
                                run_id,
                                e,
                            )

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

                    # Capture second pg_stat_statements snapshot (after warmup, before measurement)
                    if (
                        pg_stats_enabled
                        and pg_stats_conn
                        and ctx.pg_snapshot_after_warmup is None
                    ):
                        try:
                            ctx.pg_snapshot_after_warmup = (
                                await capture_pg_stat_snapshot(pg_stats_conn)
                            )
                            logger.info(
                                "Captured pg_stat_statements snapshot 2 (after_warmup) for %s: %d queries",
                                run_id,
                                len(ctx.pg_snapshot_after_warmup.stats),
                            )
                            # Compute warmup phase delta
                            if ctx.pg_snapshot_before_warmup:
                                ctx.pg_delta_warmup = compute_snapshot_delta(
                                    ctx.pg_snapshot_before_warmup,
                                    ctx.pg_snapshot_after_warmup,
                                )
                        except Exception as e:
                            logger.warning(
                                "Failed to capture pg_stat snapshot 2 for %s: %s",
                                run_id,
                                e,
                            )

                effective_elapsed_seconds = elapsed_seconds
                if warmup_seconds > 0 and warmup_elapsed_seconds is not None:
                    # Use warmup start as the elapsed base so PREPARING does not
                    # eat into the configured warmup + measurement budget.
                    effective_elapsed_seconds = warmup_elapsed_seconds
                # FIND_MAX mode: Bypass duration limit - let algorithm run until it finds max.
                # Duration limit should not interrupt FIND_MAX; it stops via degradation/ceiling.
                duration_check_enabled = not (
                    find_max_enabled and not find_max_completed
                )
                if (
                    duration_check_enabled
                    and status == "RUNNING"
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

                    await _capture_postgres_measurement_snapshot("duration_elapsed")

                total_ops = 0
                error_count = 0
                current_qps = 0.0
                target_connections_total = 0
                latest_metrics_ts = None
                aggregate_p50 = None
                aggregate_p95 = None
                aggregate_p99 = None

                if phase == "MEASUREMENT":
                    metrics_loaded_from_live = False
                    live_snapshot = await live_metrics_cache.get_run_snapshot(
                        run_id=run_id
                    )
                    if live_snapshot and isinstance(live_snapshot.metrics, dict):
                        live_metrics = live_snapshot.metrics
                        ops_obj = (
                            live_metrics.get("ops")
                            if isinstance(live_metrics.get("ops"), dict)
                            else {}
                        )
                        errors_obj = (
                            live_metrics.get("errors")
                            if isinstance(live_metrics.get("errors"), dict)
                            else {}
                        )
                        connections_obj = (
                            live_metrics.get("connections")
                            if isinstance(live_metrics.get("connections"), dict)
                            else {}
                        )
                        latency_obj = (
                            live_metrics.get("latency")
                            if isinstance(live_metrics.get("latency"), dict)
                            else {}
                        )
                        total_ops = int(ops_obj.get("total") or 0)
                        error_count = int(errors_obj.get("count") or 0)
                        current_qps = float(ops_obj.get("current_per_sec") or 0.0)
                        target_connections_total = int(
                            connections_obj.get("target") or 0
                        )
                        latest_metrics_ts = live_snapshot.updated_at
                        aggregate_p50 = (
                            float(latency_obj.get("p50"))
                            if latency_obj.get("p50") is not None
                            else None
                        )
                        aggregate_p95 = (
                            float(latency_obj.get("p95"))
                            if latency_obj.get("p95") is not None
                            else None
                        )
                        aggregate_p99 = (
                            float(latency_obj.get("p99"))
                            if latency_obj.get("p99") is not None
                            else None
                        )
                        metrics_loaded_from_live = True

                    if not metrics_loaded_from_live:
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

                # QPS-based thread scaling for FIXED and BOUNDED modes
                # This runs in the MEASUREMENT phase when load_mode is QPS
                if (
                    load_mode == "QPS"
                    and status == "RUNNING"
                    and phase == "MEASUREMENT"
                    and target_qps_total
                    and current_qps is not None
                    and not find_max_enabled  # Don't interfere with FIND_MAX
                ):
                    loop_time = asyncio.get_running_loop().time()
                    
                    # Initialize QPS controller state on first iteration
                    if qps_controller_state is None:
                        # Determine thread bounds based on scaling mode
                        if scaling_mode == "FIXED":
                            # FIXED mode: can only scale threads within each worker
                            max_total = (
                                int(effective_max_threads_per_worker) * int(worker_group_count)
                                if effective_max_threads_per_worker is not None
                                else int(worker_group_count) * 100
                            )
                            min_total = int(min_threads_per_worker) * int(worker_group_count)
                        else:
                            # BOUNDED/AUTO: can scale workers too
                            # If max_workers is unlimited, cap at 10k total threads divided by threads/worker
                            MAX_TOTAL_THREADS_CAP = 10000
                            if max_workers is not None:
                                worker_cap = int(max_workers)
                            elif effective_max_threads_per_worker:
                                # e.g., 10000 / 200 = 50 workers max
                                worker_cap = max(1, MAX_TOTAL_THREADS_CAP // int(effective_max_threads_per_worker))
                            else:
                                worker_cap = MAX_TOTAL_THREADS_CAP // 100  # 100 workers if no thread limit
                            max_total = (
                                int(effective_max_threads_per_worker) * worker_cap
                                if effective_max_threads_per_worker is not None
                                else min(MAX_TOTAL_THREADS_CAP, worker_cap * 100)
                            )
                            min_total = int(min_threads_per_worker) * int(min_workers)
                        
                        qps_controller_state = QPSControllerState(
                            target_qps=float(target_qps_total),
                            starting_threads=float(starting_threads) if starting_threads else 0.0,
                            max_thread_increase=float(max_thread_increase) if max_thread_increase else 15.0,
                            min_threads=min_total,
                            max_threads=max_total,
                        )
                        logger.info(
                            "QPS controller initialized: target=%.1f qps, starting_threads=%.1f, max_thread_increase=%.1f, threads=[%d, %d]",
                            target_qps_total,
                            starting_threads or 0.0,
                            max_thread_increase or 15.0,
                            min_total,
                            max_total,
                        )
                    
                    # Rate limit scaling decisions (minimum 10 seconds between adjustments)
                    # This allows the backend to absorb changes and stabilize before re-evaluation
                    min_interval = 10.0
                    can_evaluate = (
                        last_qps_evaluation_time is None
                        or (loop_time - last_qps_evaluation_time) >= min_interval
                    )
                    
                    if can_evaluate:
                        last_qps_evaluation_time = loop_time
                        old_target = target_connections_total
                        
                        decision = evaluate_qps_scaling(
                            state=qps_controller_state,
                            current_qps=float(current_qps),
                            current_threads=int(target_connections_total),
                        )
                        
                        # Calculate QPS error percentage for UI
                        # Negative = below target (need more QPS), Positive = above target
                        qps_error_pct = (
                            ((current_qps - target_qps_total) / target_qps_total * 100)
                            if target_qps_total > 0 else 0.0
                        )
                        
                        if decision.should_scale and decision.new_target > 0:
                            new_target = decision.new_target
                            direction = "up" if new_target > target_connections_total else "down"
                            
                            # For BOUNDED mode, ensure we have enough workers
                            if scaling_mode == "BOUNDED":
                                await _ensure_workers_for_target(new_target)
                            
                            qps_controller_step_number += 1
                            step_id = str(uuid.uuid4())
                            step_time = datetime.now(UTC)
                            
                            logger.info(
                                "QPS scaling: %s from %d to %d threads (qps=%.1f, target=%.1f, reason=%s)",
                                direction,
                                target_connections_total,
                                new_target,
                                current_qps,
                                target_qps_total,
                                decision.reason,
                            )
                            
                            effective_new = await _apply_worker_targets(
                                total_target=new_target,
                                step_id=step_id,
                                step_number=qps_controller_step_number,
                                reason=f"qps_scaling_{direction}",
                            )
                            
                            # Record step in history
                            step_entry = {
                                "step": qps_controller_step_number,
                                "from_threads": old_target,
                                "to_threads": effective_new,
                                "direction": direction,
                                "current_qps": current_qps,
                                "target_qps": target_qps_total,
                                "qps_error_pct": qps_error_pct,
                                "reason": decision.reason,
                                "timestamp": step_time.isoformat(),
                            }
                            qps_controller_step_history.append(step_entry)
                            
                            # Persist step to database
                            await _insert_controller_step_history(
                                step_id=step_id,
                                step_number=qps_controller_step_number,
                                step_type="QPS_SCALING",
                                target_workers=effective_new,
                                step_start_time=step_time,
                                from_threads=old_target,
                                to_threads=effective_new,
                                direction=direction,
                                qps=current_qps,
                                target_qps=target_qps_total,
                                qps_error_pct=qps_error_pct,
                                outcome=decision.reason,
                            )
                            
                            target_connections_total = effective_new
                            last_qps_scaling_time = loop_time
                        
                        # Persist live QPS controller state for UI (every evaluation)
                        # Use time.time() for epoch timestamps (loop_time is monotonic, not epoch)
                        now_epoch_ms = int(time.time() * 1000)
                        next_eval_epoch_ms = now_epoch_ms + int(min_interval * 1000)
                        qps_live_state = {
                            "mode": "QPS",
                            "status": "RUNNING",
                            "target_qps": target_qps_total,
                            "current_qps": current_qps,
                            "current_threads": target_connections_total,
                            "min_threads": qps_controller_state.min_threads,
                            "max_threads": qps_controller_state.max_threads,
                            "last_evaluation_epoch_ms": now_epoch_ms,
                            "next_evaluation_epoch_ms": next_eval_epoch_ms,
                            "evaluation_interval_seconds": min_interval,
                            "last_scaling_step": qps_controller_step_number,
                            "qps_error_pct": qps_error_pct,
                            "step_history": qps_controller_step_history[-10:],  # Last 10 steps
                        }
                        await _persist_qps_controller_state(qps_live_state)

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

                            effective_min = min_threads_per_worker * worker_group_count
                            if find_max_step_target < effective_min:
                                logger.info(
                                    "FIND_MAX start_concurrency=%d < workers(%d) × min_connections(%d) = %d; "
                                    "effective start will be %d",
                                    find_max_start,
                                    worker_group_count,
                                    min_threads_per_worker,
                                    effective_min,
                                    effective_min,
                                )

                            # Use fresh timestamp for step start (not stale 'now' from loop start)
                            find_max_step_started_at = datetime.now(UTC)
                            find_max_step_started_epoch_ms = int(
                                find_max_step_started_at.timestamp() * 1000
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

                            # Persist countdown immediately so frontend shows timer during worker spawn
                            early_state = {
                                "mode": "FIND_MAX_CONCURRENCY",
                                "status": "STEP_STARTING",
                                "current_step": int(find_max_step_number),
                                "current_concurrency": int(find_max_step_target),
                                "target_workers": int(find_max_step_target),
                                "active_worker_count": int(workers_active),
                                "next_planned_concurrency": int(
                                    min(
                                        max_concurrency,
                                        int(find_max_step_target) + int(find_max_increment),
                                    )
                                ),
                                "step_duration_seconds": int(find_max_step_seconds),
                                "step_started_at_epoch_ms": find_max_step_started_epoch_ms,
                                "step_end_at_epoch_ms": find_max_step_end_epoch_ms,
                                "start_concurrency": int(find_max_start),
                                "concurrency_increment": int(find_max_increment),
                                "max_concurrency": int(max_concurrency),
                                "best_concurrency": int(find_max_best_concurrency),
                                "best_qps": float(find_max_best_qps),
                                "last_updated": datetime.now(UTC).isoformat(),
                            }
                            logger.info(
                                "FIND_MAX step %d: persisting early_state with step_end_at_epoch_ms=%s",
                                find_max_step_number,
                                find_max_step_end_epoch_ms,
                            )
                            await _persist_find_max_state(early_state)
                            last_find_max_running_state_persist_mono = now_mono

                            await _ensure_workers_for_target(find_max_step_target)

                            find_max_step_target = await _apply_worker_targets(
                                total_target=find_max_step_target,
                                step_id=str(find_max_step_id),
                                step_number=find_max_step_number,
                                reason="step_start",
                            )
                            # NOTE: Do NOT update best_concurrency here at step start.
                            # It should only be updated AFTER the step completes as stable
                            # (handled in the step completion logic below).

                        # Use a fresh wall-clock timestamp here instead of the loop-level
                        # `now` captured earlier in the iteration. The loop body can take
                        # several seconds (DB writes, worker target updates), and using a
                        # stale timestamp causes step completion to lag behind the UI timer.
                        step_eval_now = datetime.now(UTC)
                        step_elapsed = 0.0
                        if find_max_step_started_at is not None:
                            step_elapsed = max(
                                0.0,
                                (step_eval_now - find_max_step_started_at).total_seconds(),
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

                        # Re-evaluate elapsed time before any additional awaited I/O.
                        # This keeps step boundaries tight even when per-iteration DB
                        # operations are slow.
                        step_completion_now = datetime.now(UTC)
                        step_elapsed_for_completion = step_elapsed
                        if find_max_step_started_at is not None:
                            step_elapsed_for_completion = max(
                                0.0,
                                (
                                    step_completion_now - find_max_step_started_at
                                ).total_seconds(),
                            )
                        step_reached_end = (
                            step_elapsed_for_completion >= float(find_max_step_seconds)
                        )
                        if step_reached_end:
                            # Use completion-time values for history + transition timestamps.
                            step_elapsed = step_elapsed_for_completion
                            step_eval_now = step_completion_now

                        # Queue detection is only relevant for runs that have a warehouse.
                        # Postgres runs have no warehouse and should avoid this extra query.
                        warehouse_snapshot = None
                        queue_detected = False
                        if warehouse_name:
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

                        if not step_reached_end:
                            should_persist_running_state = (
                                last_find_max_running_state_persist_mono is None
                                or now_mono - last_find_max_running_state_persist_mono
                                >= find_max_running_state_persist_interval_seconds
                            )
                            if should_persist_running_state:
                                await _persist_find_max_state(state)
                                last_find_max_running_state_persist_mono = now_mono

                            # DEBUG: Log step progress periodically
                            if find_max_step_number >= 1 and int(step_elapsed) % 10 == 0 and step_elapsed > 0:
                                logger.info(
                                    "FIND_MAX step %d progress: elapsed=%.1fs, required=%.1fs, remaining=%.1fs",
                                    find_max_step_number, step_elapsed, float(find_max_step_seconds),
                                    max(0, float(find_max_step_seconds) - step_elapsed)
                                )

                        if step_reached_end:
                            # Immediately persist "transitioning" state so frontend countdown
                            # doesn't show 0 for several seconds during history insert
                            transitioning_state = {
                                "mode": "FIND_MAX_CONCURRENCY",
                                "status": "TRANSITIONING",
                                "current_step": int(find_max_step_number),
                                "current_concurrency": int(find_max_step_target),
                                "target_workers": int(find_max_step_target),
                                "step_duration_seconds": int(find_max_step_seconds),
                                # Set the end time to "now + 1.5s" as a placeholder
                                # This will be replaced by the real next step's end time shortly
                                "step_end_at_epoch_ms": int(
                                    step_eval_now.timestamp() * 1000
                                )
                                + 1500,
                                "start_concurrency": int(find_max_start),
                                "concurrency_increment": int(find_max_increment),
                                "max_concurrency": int(max_concurrency),
                                "best_concurrency": int(find_max_best_concurrency),
                                "best_qps": float(find_max_best_qps),
                                "last_updated": datetime.now(UTC).isoformat(),
                            }
                            await _persist_find_max_state(transitioning_state)

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
                                and not find_max_is_backoff_step  # Skip latency check for backoff steps
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

                            # Reset backoff flag after step evaluation
                            if find_max_is_backoff_step:
                                logger.info(
                                    "FIND_MAX backoff step %d: P95 baseline check skipped "
                                    "(p95_vs_baseline_pct=%.1f%%, stable=%s)",
                                    find_max_step_number,
                                    p95_vs_baseline_pct or 0.0,
                                    stable,
                                )
                                find_max_is_backoff_step = False

                            if find_max_baseline_p95 is None and aggregate_p95:
                                find_max_baseline_p95 = float(aggregate_p95)
                            if find_max_baseline_p99 is None and aggregate_p99:
                                find_max_baseline_p99 = float(aggregate_p99)

                            if stable:
                                # Update best_concurrency if this stable step has higher concurrency
                                # (FIND_MAX seeks max sustainable concurrency, not max QPS)
                                if int(find_max_step_target) > find_max_best_concurrency:
                                    find_max_best_concurrency = int(find_max_step_target)
                                # Always track best QPS among stable steps
                                if step_qps >= find_max_best_qps:
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

                            logger.info(
                                "FIND_MAX step %d complete: inserting history (outcome=%s, stable=%s)",
                                find_max_step_number, outcome, stable
                            )

                            history_task = asyncio.create_task(
                                _insert_find_max_step_history(
                                    step_id=str(find_max_step_id or uuid.uuid4()),
                                    step_number=int(find_max_step_number),
                                    target_workers=int(find_max_step_target),
                                    step_start_time=find_max_step_started_at
                                    or step_eval_now,
                                    step_end_time=step_eval_now,
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
                            )
                            _track_find_max_history_task(
                                history_task, step_number=int(find_max_step_number)
                            )

                            # Helper to setup next step
                            async def _setup_next_step(
                                next_target: int, reason: str, is_backoff: bool = False
                            ) -> int:
                                nonlocal find_max_step_number, find_max_step_id
                                nonlocal find_max_step_target, find_max_step_started_at
                                nonlocal find_max_step_started_epoch_ms, find_max_step_end_epoch_ms
                                nonlocal find_max_step_start_ops, find_max_step_start_errors
                                nonlocal find_max_prev_step_qps, find_max_is_backoff_step
                                nonlocal last_find_max_running_state_persist_mono

                                find_max_is_backoff_step = is_backoff
                                find_max_prev_step_qps = float(step_qps)
                                find_max_step_number += 1
                                find_max_step_id = str(uuid.uuid4())
                                find_max_step_target = next_target
                                # Use single timestamp for consistency
                                find_max_step_started_at = datetime.now(UTC)
                                find_max_step_started_epoch_ms = int(
                                    find_max_step_started_at.timestamp() * 1000
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

                                # Persist countdown immediately so frontend shows timer during worker spawn
                                early_state = {
                                    "mode": "FIND_MAX_CONCURRENCY",
                                    "status": "STEP_STARTING",
                                    "current_step": int(find_max_step_number),
                                    "current_concurrency": int(find_max_step_target),
                                    "target_workers": int(find_max_step_target),
                                    "active_worker_count": int(workers_active),
                                    "next_planned_concurrency": int(
                                        min(
                                            max_concurrency,
                                            int(find_max_step_target)
                                            + int(find_max_increment),
                                        )
                                    ),
                                    "step_duration_seconds": int(find_max_step_seconds),
                                    "step_started_at_epoch_ms": find_max_step_started_epoch_ms,
                                    "step_end_at_epoch_ms": find_max_step_end_epoch_ms,
                                    "start_concurrency": int(find_max_start),
                                    "concurrency_increment": int(find_max_increment),
                                    "max_concurrency": int(max_concurrency),
                                    "best_concurrency": int(find_max_best_concurrency),
                                    "best_qps": float(find_max_best_qps),
                                    "is_backoff": bool(is_backoff),
                                    "last_updated": datetime.now(UTC).isoformat(),
                                }
                                logger.info(
                                    "FIND_MAX step %d (next): persisting early_state with step_end_at_epoch_ms=%s",
                                    find_max_step_number,
                                    find_max_step_end_epoch_ms,
                                )
                                await _persist_find_max_state(early_state)
                                last_find_max_running_state_persist_mono = (
                                    asyncio.get_running_loop().time()
                                )

                                await _ensure_workers_for_target(find_max_step_target)

                                return await _apply_worker_targets(
                                    total_target=find_max_step_target,
                                    step_id=str(find_max_step_id),
                                    step_number=find_max_step_number,
                                    reason=reason,
                                )

                            should_continue = False
                            should_stop = False

                            # DEBUG: Log step completion decision inputs
                            logger.info(
                                "FIND_MAX step %d decision: stable=%s, find_max_step_target=%s, "
                                "max_concurrency=%s, find_max_best_concurrency=%s, "
                                "find_max_backoff_attempted=%s, stop_reason=%s",
                                find_max_step_number,
                                stable,
                                find_max_step_target,
                                max_concurrency,
                                find_max_best_concurrency,
                                find_max_backoff_attempted,
                                stop_reason,
                            )

                            if stable:
                                # Step was stable - continue scaling or stop at ceiling
                                if find_max_step_target < max_concurrency:
                                    # Continue to next step
                                    should_continue = True
                                    next_target = min(
                                        max_concurrency,
                                        int(find_max_step_target) + int(find_max_increment),
                                    )
                                    find_max_step_target = await _setup_next_step(
                                        next_target, "step_advance"
                                    )
                                else:
                                    # Reached ceiling while stable
                                    should_stop = True
                                    if bounded_max_configured:
                                        find_max_final_reason = "Reached bounded max"
                                    else:
                                        find_max_final_reason = "Reached max workers"
                            else:
                                # Step was NOT stable - try backoff/retry before stopping
                                if find_max_termination_reason is None and stop_reason:
                                    find_max_termination_reason = str(stop_reason).strip() or None

                                if (
                                    not find_max_backoff_attempted
                                    and find_max_best_concurrency > 0
                                    and find_max_best_concurrency < find_max_step_target
                                ):
                                    # Try backoff: go back to best_concurrency and verify stability
                                    find_max_backoff_attempted = True
                                    find_max_failed_concurrency = int(find_max_step_target)
                                    logger.info(
                                        "FIND_MAX: Degradation detected at %d workers. "
                                        "Backing off to verify %d is stable...",
                                        find_max_step_target,
                                        find_max_best_concurrency,
                                    )

                                    # Setup backoff step
                                    find_max_step_target = await _setup_next_step(
                                        find_max_best_concurrency, "backoff", is_backoff=True
                                    )

                                    # Continue the loop to run backoff step
                                    should_continue = True
                                else:
                                    # Backoff already attempted or no better concurrency to try
                                    should_stop = True
                                    find_max_final_reason = (
                                        find_max_termination_reason
                                        or stop_reason
                                        or "Degradation detected"
                                    )

                            # After backoff step is stable, try midpoint
                            if (
                                stable
                                and find_max_backoff_attempted
                                and not find_max_midpoint_attempted
                                and find_max_failed_concurrency is not None
                                and find_max_step_target == find_max_best_concurrency
                            ):
                                # Backoff was stable - try midpoint between best and failed
                                midpoint = find_max_best_concurrency + (
                                    find_max_failed_concurrency - find_max_best_concurrency
                                ) // 2
                                if (
                                    midpoint > find_max_best_concurrency
                                    and midpoint < find_max_failed_concurrency
                                ):
                                    find_max_midpoint_attempted = True
                                    logger.info(
                                        "FIND_MAX: Backoff confirmed - %d workers is stable @ %.1f QPS. "
                                        "Trying midpoint %d workers...",
                                        find_max_best_concurrency,
                                        step_qps,
                                        midpoint,
                                    )

                                    # Setup midpoint step
                                    find_max_step_target = await _setup_next_step(
                                        midpoint, "midpoint"
                                    )
                                    should_continue = True
                                    should_stop = False
                                else:
                                    # No room for midpoint, stop here
                                    logger.info(
                                        "FIND_MAX: Backoff confirmed - %d workers is stable @ %.1f QPS. "
                                        "No room for midpoint, stopping.",
                                        find_max_best_concurrency,
                                        step_qps,
                                    )
                                    should_stop = True
                                    find_max_final_reason = (
                                        find_max_termination_reason or "Degradation detected"
                                    )

                            # After midpoint step, stop regardless of outcome
                            if find_max_midpoint_attempted and stable and not should_stop:
                                # Midpoint was stable - update best if higher concurrency
                                if int(find_max_step_target) > find_max_best_concurrency:
                                    find_max_best_concurrency = int(find_max_step_target)
                                    logger.info(
                                        "FIND_MAX: Midpoint %d is stable! New best concurrency.",
                                        find_max_step_target,
                                    )
                                if step_qps > find_max_best_qps:
                                    find_max_best_qps = float(step_qps)
                                should_stop = True
                                find_max_final_reason = (
                                    find_max_termination_reason or "Degradation detected"
                                )

                            if should_stop:
                                await _capture_postgres_measurement_snapshot(
                                    "find_max_stop"
                                )
                                find_max_completed = True
                                max_type = None
                                bounded = False
                                ceiling_reached = False
                                if stable:
                                    ceiling_reached = True
                                    if bounded_max_configured:
                                        bounded = True
                                        max_type = "BOUNDED_MAX"
                                    else:
                                        max_type = "TRUE_MAX"

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
                        logger.error(
                            "Find Max controller error for run %s: %s", run_id, exc, exc_info=True
                        )

                should_rollup = False
                if latest_metrics_ts != last_metrics_ts:
                    should_rollup = True
                if heartbeat_updated_at != last_heartbeat_update:
                    should_rollup = True

                should_write_run_status_rollup = False
                if should_rollup:
                    if status in {"COMPLETED", "FAILED", "CANCELLED"}:
                        should_write_run_status_rollup = True
                    elif (
                        last_run_status_rollup_mono is None
                        or now_mono - last_run_status_rollup_mono
                        >= run_status_rollup_interval_active_seconds
                    ):
                        should_write_run_status_rollup = True

                if should_write_run_status_rollup:
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
                    last_run_status_rollup_mono = now_mono

                is_terminal_status = status in {"COMPLETED", "FAILED", "CANCELLED"}
                should_update_parent_rollup = False
                if should_rollup and not is_terminal_status:
                    if (
                        last_parent_rollup_mono is None
                        or now_mono - last_parent_rollup_mono
                        >= parent_rollup_interval_active_seconds
                    ):
                        should_update_parent_rollup = True
                if is_terminal_status and status != last_parent_rollup_status:
                    should_update_parent_rollup = True

                if should_update_parent_rollup:
                    if is_terminal_status:
                        # For terminal states, await so final aggregates are durable.
                        try:
                            await results_store.update_parent_run_aggregate(
                                parent_run_id=run_id
                            )
                            last_parent_rollup_status = status
                            last_parent_rollup_mono = now_mono
                        except Exception as exc:
                            logger.debug(
                                "Failed to update parent rollup for %s: %s",
                                run_id,
                                exc,
                            )
                    else:
                        # During active runs, do this asynchronously to avoid blocking
                        # the control loop and delaying FIND_MAX step transitions.
                        if parent_rollup_task is None:
                            parent_rollup_task = asyncio.create_task(
                                results_store.update_parent_run_aggregate(
                                    parent_run_id=run_id
                                )
                            )
                            _track_parent_rollup_task(parent_rollup_task)
                            last_parent_rollup_mono = now_mono

                # Check if all worker processes have exited
                if ctx.worker_procs:
                    all_exited = all(
                        proc.returncode is not None for proc in ctx.worker_procs
                    )
                    if all_exited:
                        # Wait for stream tasks to finish draining any remaining output
                        if ctx.worker_stream_tasks:
                            await asyncio.gather(
                                *ctx.worker_stream_tasks, return_exceptions=True
                            )
                        for i, proc in enumerate(ctx.worker_procs):
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

                loop_cycle_elapsed = (
                    asyncio.get_running_loop().time() - loop_cycle_started_mono
                )
                if (
                    find_max_enabled
                    and status == "RUNNING"
                    and phase == "MEASUREMENT"
                    and loop_cycle_elapsed > 2.0
                ):
                    logger.info(
                        "FIND_MAX poll loop slow iteration for %s: %.2fs (should_rollup=%s, parent_rollup_task=%s)",
                        run_id,
                        loop_cycle_elapsed,
                        should_rollup,
                        "active" if parent_rollup_task is not None else "idle",
                    )

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

            # Ensure background parent rollup task does not outlive the run.
            if parent_rollup_task and not parent_rollup_task.done():
                parent_rollup_task.cancel()
                try:
                    await parent_rollup_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass

            # Force one final durable parent rollup after status finalization.
            # This reconciles stale parent TEST_RESULTS rows when async rollups raced.
            try:
                await results_store.update_parent_run_aggregate(parent_run_id=run_id)
            except Exception as rollup_err:
                logger.debug(
                    "Final parent rollup failed for %s: %s", run_id, rollup_err
                )

            if worker_health_task and not worker_health_task.done():
                worker_health_task.cancel()
                try:
                    await worker_health_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass

            if find_max_history_tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(
                            *list(find_max_history_tasks), return_exceptions=True
                        ),
                        timeout=15.0,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "Timed out waiting for FIND_MAX history tasks for run %s",
                        run_id,
                    )

            # If we resumed an Interactive/Hybrid warehouse, suspend it to save costs
            ctx = self._active_runs.get(run_id)
            if ctx and ctx.did_resume_warehouse:
                target_cfg = ctx.scenario_config.get("target") or {}
                warehouse_name = str(target_cfg.get("warehouse") or "").strip().upper()
                if warehouse_name:
                    try:
                        logger.info(
                            "Suspending warehouse %s (we resumed it on startup)...",
                            warehouse_name,
                        )
                        await self._pool.execute_query(
                            f"ALTER WAREHOUSE {warehouse_name} SUSPEND"
                        )
                        logger.info("✅ Suspended warehouse %s", warehouse_name)
                    except Exception as suspend_err:
                        logger.warning(
                            "Failed to suspend warehouse %s: %s",
                            warehouse_name,
                            suspend_err,
                        )

            # Clean up Postgres stats connection
            if pg_stats_conn:
                try:
                    await pg_stats_conn.close()
                    logger.debug("Closed Postgres stats connection for run %s", run_id)
                except Exception as pg_close_err:
                    logger.warning(
                        "Failed to close Postgres stats connection for %s: %s",
                        run_id,
                        pg_close_err,
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
            await live_controller_state.clear_run(run_id=run_id)
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
