"""
Test Executor

Orchestrates performance test execution with concurrent workload generation.
"""

import asyncio
import importlib
import json
import logging
import math
import os
import re
from collections import deque
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from itertools import count
from typing import Any, Callable, List, Optional, cast
from uuid import uuid4
import time
from backend.models import (
    TestScenario,
    TestResult,
    TestStatus,
    WorkloadType,
    Metrics,
)
from backend.config import settings
from backend.connectors import snowflake_pool
from backend.core.table_managers import create_table_manager, TableManager
from backend.core.table_profiler import (
    TableProfile,
    profile_snowflake_table,
    profile_postgres_table,
)

logger = logging.getLogger(__name__)

_SF_SQLSTATE_RE = re.compile(r"\(\s*(\d{5})\s*\)")


@dataclass
class _TableRuntimeState:
    profile: Optional[TableProfile] = None
    next_insert_id: Optional[int] = None
    insert_id_seq: Optional[Iterator[int]] = None


@dataclass
class _QueryExecutionRecord:
    execution_id: str
    test_id: str
    query_id: str
    query_text: str
    start_time: datetime
    end_time: datetime
    duration_ms: float
    success: bool
    error: Optional[str]
    warehouse: Optional[str]
    rows_affected: Optional[int]
    bytes_scanned: Optional[int]
    connection_id: Optional[int]
    custom_metadata: dict[str, Any]
    query_kind: str
    worker_id: int
    warmup: bool
    app_elapsed_ms: float
    sf_execution_ms: Optional[float] = None
    queue_wait_ms: Optional[float] = None


class TestExecutor:
    """
    Orchestrates performance test execution.

    Manages:
    - Test lifecycle (setup, execute, teardown)
    - Concurrent worker pool
    - Workload generation
    - Real-time metrics collection
    """

    def __init__(self, scenario: TestScenario):
        """
        Initialize test executor.

        Args:
            scenario: Test scenario configuration
        """
        self.scenario = scenario
        self.test_id = uuid4()

        # State
        self.status = TestStatus.PENDING
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self._measurement_start_time: Optional[datetime] = None

        # Table managers
        self.table_managers: List[TableManager] = []
        self._table_state: dict[str, _TableRuntimeState] = {}

        # Workers
        self.workers: List[asyncio.Task] = []
        self._stop_event = asyncio.Event()

        # Metrics
        self.metrics = Metrics()
        self._metrics_lock = asyncio.Lock()
        self._psutil = None
        self._process = None
        self._host_cpu_cores: int | None = None
        self._cgroup_prev_usage: float | None = None
        self._cgroup_prev_time_mono: float | None = None
        try:
            psutil_mod = importlib.import_module("psutil")
            self._psutil = psutil_mod
            self._process = psutil_mod.Process()
            # Prime cpu_percent so subsequent calls return a delta.
            self._process.cpu_percent(interval=None)
            # Prime host cpu_percent so subsequent calls return a delta.
            self._psutil.cpu_percent(interval=None)
            try:
                cores = self._psutil.cpu_count(logical=True)
                self._host_cpu_cores = int(cores) if cores else None
            except Exception:
                self._host_cpu_cores = None
        except Exception:
            self._psutil = None
            self._process = None
        # Metrics epoch (generation) for phase transitions.
        #
        # We use this to prevent late completions from a previous phase (e.g. warmup)
        # from polluting the measurement window after metrics reset.
        self._metrics_epoch: int = 0
        # True once the measurement window has begun (used by QPS workers to treat
        # post-warmup operations as measurement even if the worker was spawned during warmup).
        self._measurement_active: bool = False
        self._latencies_ms: deque[float] = deque(maxlen=10000)
        self._last_snapshot_time: Optional[datetime] = None
        self._last_snapshot_mono: float | None = None
        self._last_snapshot_ops: int = 0
        self._qps_smoothed: float | None = None
        # Rolling QPS estimate over a fixed window (for stable controller input).
        self._qps_windowed: float | None = None
        self._qps_window_seconds: float = 10.0
        self._qps_samples: deque[tuple[float, int]] = deque(maxlen=300)

        # Benchmark warehouse telemetry (best-effort, sampled out-of-band).
        self._benchmark_warehouse_name: str | None = None
        # Per-test query tag for benchmark sessions (populated by TestRegistry when using
        # template runs / per-test pools).
        self._benchmark_query_tag: str | None = None
        self._warehouse_status: dict[str, Any] = {}
        self._last_warehouse_status_mono: float | None = None
        self._warehouse_status_task: asyncio.Task | None = None
        # Best-effort server-side query state sampling (running/queued) for this test.
        self._warehouse_query_status: dict[str, Any] = {}
        self._last_warehouse_query_status_mono: float | None = None
        self._warehouse_query_status_task: asyncio.Task | None = None

        # QPS controller state (for websocket visibility/debugging).
        self._qps_controller_state: dict[str, Any] = {}

        # FIND_MAX_CONCURRENCY controller state (for websocket visibility/debugging).
        self._find_max_controller_state: dict[str, Any] = {}

        # Target worker count (desired concurrency) for all load modes.
        # Updated by controllers; exposed in metrics for charting.
        self._target_workers: int = 0

        # Per-step, per-query-kind metrics for FIND_MAX_CONCURRENCY evaluation.
        # These are reset at the start of each FMC step measurement window.
        self._find_max_step_collecting: bool = False
        self._find_max_step_lat_by_kind_ms: dict[str, deque[float]] = {
            "POINT_LOOKUP": deque(maxlen=10000),
            "RANGE_SCAN": deque(maxlen=10000),
            "INSERT": deque(maxlen=10000),
            "UPDATE": deque(maxlen=10000),
        }
        self._find_max_step_ops_by_kind: dict[str, int] = {
            "POINT_LOOKUP": 0,
            "RANGE_SCAN": 0,
            "INSERT": 0,
            "UPDATE": 0,
        }
        self._find_max_step_errors_by_kind: dict[str, int] = {
            "POINT_LOOKUP": 0,
            "RANGE_SCAN": 0,
            "INSERT": 0,
            "UPDATE": 0,
        }

        # Query type counters (for debugging)
        self._point_lookup_count: int = 0
        self._range_scan_count: int = 0
        self._insert_count: int = 0
        self._update_count: int = 0

        # CUSTOM workload execution state (authoritative template mix).
        # - schedule: smooth weighted round-robin sequence of query kinds
        # - per-worker index: ensures stable offsets across workers without randomness
        self._custom_schedule: list[str] = []
        self._custom_sql_by_kind: dict[str, str] = {}
        self._custom_weights: dict[str, int] = {}
        self._custom_pos_by_worker: dict[int, int] = {}

        # Per-operation capture for QUERY_EXECUTIONS (optionally persisted).
        self._query_execution_records: deque[_QueryExecutionRecord] = deque(
            maxlen=200_000
        )

        # Latency samples by kind (non-warmup only, for summary columns).
        self._lat_by_kind_ms: dict[str, list[float]] = {
            "POINT_LOOKUP": [],
            "RANGE_SCAN": [],
            "INSERT": [],
            "UPDATE": [],
        }
        self._lat_read_ms: list[float] = []
        self._lat_write_ms: list[float] = []

        # Latency breakdown tracking (rolling windows for live metrics).
        # - sf_execution: Snowflake-reported execution time
        # - network_overhead: app_elapsed - sf_execution (includes network RTT + driver overhead)
        self._latency_sf_execution_ms: deque[float] = deque(maxlen=10000)
        self._latency_network_overhead_ms: deque[float] = deque(maxlen=10000)

        # Aggregated SQL error categories (to avoid per-operation logging spam).
        # Reset on metrics epoch transitions (warmup -> measurement).
        self._sql_error_categories: dict[str, int] = {}
        # Sampled per-operation SQL error logs (to surface real exceptions without spam).
        # Tracks emitted detailed logs per category for the current run (NOT reset on warmup->measurement).
        self._sql_error_sample_counts: dict[str, int] = {}

        # Results
        self.test_result: Optional[TestResult] = None

        # Setup/validation error message (for UI display when test fails during setup)
        self._setup_error: Optional[str] = None

        # Callbacks
        self.metrics_callback: Optional[Callable[[Metrics], None]] = None

        logger.info(f"TestExecutor initialized: {scenario.name}")
        if self.scenario.workload_type == WorkloadType.CUSTOM:
            self._init_custom_workload()

    @staticmethod
    def _classify_sql_error(exc: Exception) -> str:
        """
        Return a stable, low-cardinality category for an execution-time SQL error.

        These errors are expected under load (locks/timeouts/etc), so we aggregate
        rather than logging each failure.
        """
        msg = str(exc or "")
        msg_l = msg.lower()

        # Common Snowflake lock contention signature (from connector error strings).
        if "number of waiters for this lock exceeds" in msg_l:
            return "SF_LOCK_WAITER_LIMIT"

        # Prefer explicit sqlstate if available on the exception.
        sqlstate = getattr(exc, "sqlstate", None)
        if sqlstate:
            return f"SF_SQLSTATE_{sqlstate}"

        # Fallback: parse "(57014)" style sqlstate from the message.
        m = _SF_SQLSTATE_RE.search(msg)
        if m:
            return f"SF_SQLSTATE_{m.group(1)}"

        return type(exc).__name__

    @staticmethod
    def _is_postgres_pool(pool) -> bool:
        """Check if a pool is a PostgreSQL connection pool."""
        try:
            from backend.connectors.postgres_pool import PostgresConnectionPool

            return isinstance(pool, PostgresConnectionPool)
        except ImportError:
            return False

    @staticmethod
    def _quote_column(col: str, pool) -> str:
        """
        Quote a column identifier appropriately for the target database.

        - Snowflake: Always quote with double quotes (case-sensitive, uppercase)
        - PostgreSQL: Use unquoted identifiers (PostgreSQL folds to lowercase)
        """
        if TestExecutor._is_postgres_pool(pool):
            return col
        return f'"{col}"'

    @staticmethod
    def _annotate_query_for_sf_kind(query: str, query_kind: str) -> str:
        """
        Prefix a statement with a short SQL comment encoding the benchmark query kind.

        This is used only for Snowflake server-side concurrency sampling (QUERY_HISTORY),
        so we can break RUNNING counts down by kind without relying on fragile SQL parsing.
        """
        q = str(query or "")
        kind = str(query_kind or "").strip().upper()
        if not kind:
            return q
        marker = f"/*UB_KIND={kind}*/ "
        # Avoid double-tagging if the caller already annotated (or custom SQL includes it).
        if q.lstrip().startswith("/*UB_KIND="):
            return q
        return marker + q

    def get_sql_error_category_snapshot(self) -> dict[str, Any]:
        """
        Snapshot aggregated SQL error categories for logging/telemetry.

        NOTE: This is intentionally sync (called from non-async metrics callbacks).
        """
        return {
            "epoch": int(self._metrics_epoch),
            "categories": dict(self._sql_error_categories),
        }

    @staticmethod
    def _truncate_str_for_log(value: Any, *, max_chars: int = 800) -> str:
        text = str(value if value is not None else "")
        if len(text) > max_chars:
            return text[:max_chars] + "…[truncated]"
        return text

    @staticmethod
    def _preview_query_for_log(query: str, *, max_chars: int = 2000) -> str:
        q = re.sub(r"\s+", " ", str(query or "")).strip()
        if len(q) > max_chars:
            return q[:max_chars] + "…[truncated]"
        return q

    @staticmethod
    def _preview_param_value_for_log(value: Any, *, max_chars: int = 200) -> str:
        try:
            if isinstance(value, (bytes, bytearray, memoryview)):
                return f"<bytes len={len(value)}>"
        except Exception:
            # Best-effort; fall back to repr.
            pass

        try:
            text = repr(value)
        except Exception:
            text = f"<unreprable {type(value).__name__}>"

        if len(text) > max_chars:
            return text[:max_chars] + "…[truncated]"
        return text

    @staticmethod
    def _preview_params_for_log(
        params: Optional[list[Any]],
        *,
        max_items: int = 10,
        max_value_chars: int = 200,
    ) -> dict[str, Any]:
        if not params:
            return {"count": 0, "items": []}
        items = [
            TestExecutor._preview_param_value_for_log(v, max_chars=max_value_chars)
            for v in params[:max_items]
        ]
        out: dict[str, Any] = {"count": len(params), "items": items}
        if len(params) > max_items:
            out["truncated"] = True
        return out

    @staticmethod
    def _sql_error_meta_for_log(
        exc: Exception, *, max_chars: int = 500
    ) -> dict[str, Any]:
        """
        Extract common SQL error fields across connectors (asyncpg, Snowflake).
        """
        out: dict[str, Any] = {}
        for key in (
            "sqlstate",
            "constraint_name",
            "schema_name",
            "table_name",
            "column_name",
            "detail",
            "hint",
        ):
            try:
                raw = getattr(exc, key, None)
            except Exception:
                raw = None
            if raw is None:
                continue
            val = str(raw)
            if not val:
                continue
            if len(val) > max_chars:
                val = val[:max_chars] + "…[truncated]"
            out[key] = val
        return out

    @staticmethod
    def _build_smooth_weighted_schedule(weights: dict[str, int]) -> list[str]:
        """
        Build a smooth weighted round-robin schedule.

        This yields a stable interleaving that converges to the exact target weights
        over one full cycle (e.g., 100 slots for percentage weights).
        """
        total = int(sum(weights.values()))
        if total <= 0:
            return []
        current: dict[str, int] = {k: 0 for k in weights}
        schedule: list[str] = []
        for _ in range(total):
            for k, w in weights.items():
                current[k] += int(w)
            # Use __getitem__ (not .get) so the key function is guaranteed to return int.
            k_max = max(current, key=current.__getitem__)
            schedule.append(k_max)
            current[k_max] -= total
        return schedule

    def _init_custom_workload(self) -> None:
        """Parse scenario.custom_queries into an execution schedule and SQL map."""
        raw = self.scenario.custom_queries or []
        weights: dict[str, int] = {}
        sql_by_kind: dict[str, str] = {}

        allowed = {"POINT_LOOKUP", "RANGE_SCAN", "INSERT", "UPDATE"}
        for entry in raw:
            if not isinstance(entry, dict):
                raise ValueError("custom_queries entries must be JSON objects")
            kind_raw = entry.get("query_kind") or entry.get("kind") or ""
            kind = str(kind_raw).strip().upper()
            if kind not in allowed:
                raise ValueError(f"Unsupported custom query_kind: {kind_raw!r}")

            pct_raw = entry.get("weight_pct", entry.get("pct", entry.get("weight", 0)))
            try:
                pct = int(pct_raw)
            except Exception as e:
                raise ValueError(f"Invalid weight_pct for {kind}: {pct_raw!r}") from e
            if pct <= 0:
                continue

            sql_raw = entry.get("sql", entry.get("query", entry.get("query_text", "")))
            sql = str(sql_raw or "").strip()
            if not sql:
                raise ValueError(f"Missing SQL for custom query kind {kind}")

            weights[kind] = pct
            sql_by_kind[kind] = sql

        total = sum(weights.values())
        if total != 100:
            raise ValueError(
                f"Custom workload weights must sum to 100 (currently {total})."
            )

        self._custom_weights = dict(weights)
        self._custom_sql_by_kind = dict(sql_by_kind)
        self._custom_schedule = self._build_smooth_weighted_schedule(weights)
        self._custom_pos_by_worker.clear()

    def _custom_next_kind(self, worker_id: int) -> str:
        if not self._custom_schedule:
            self._init_custom_workload()
        if not self._custom_schedule:
            raise ValueError("CUSTOM workload has no scheduled queries")
        n = len(self._custom_schedule)
        pos = self._custom_pos_by_worker.get(worker_id, worker_id % n)
        kind = self._custom_schedule[pos]
        self._custom_pos_by_worker[worker_id] = (pos + 1) % n
        return kind

    async def setup(self) -> bool:
        """
        Setup test environment.

        - Create table managers
        - Setup tables
        - Initialize connection pools

        Returns:
            bool: True if setup successful
        """
        try:
            logger.info(f"Setting up test: {self.scenario.name}")

            # Create table managers
            for table_config in self.scenario.table_configs:
                manager = create_table_manager(table_config)
                self.table_managers.append(manager)

            # Optional: per-test Snowflake pool override (used for template-selected warehouses).
            pool_override = getattr(self, "_snowflake_pool_override", None)
            if pool_override is not None:
                for manager in self.table_managers:
                    if hasattr(manager, "pool") and hasattr(
                        manager.pool, "execute_query"
                    ):
                        cast(Any, manager).pool = pool_override

            # Cache benchmark warehouse name (for best-effort sampling of Snowflake running/queued).
            self._benchmark_warehouse_name = None
            for manager in self.table_managers:
                pool = getattr(manager, "pool", None)
                wh = str(getattr(pool, "warehouse", "")).strip()
                if wh:
                    self._benchmark_warehouse_name = wh
                    break

            # Setup tables in parallel
            setup_tasks = [manager.setup() for manager in self.table_managers]
            results = await asyncio.gather(*setup_tasks, return_exceptions=True)

            # Check for failures
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    self._setup_error = f"Failed to setup table {self.table_managers[i].table_name}: {result}"
                    logger.error(self._setup_error)
                    return False
                elif not result:
                    self._setup_error = (
                        f"Failed to setup table {self.table_managers[i].table_name}"
                    )
                    logger.error(self._setup_error)
                    return False

            logger.info(
                f"✅ Test setup complete: {len(self.table_managers)} tables ready"
            )

            # Guardrail: Views are read-only. If a view is selected, prevent workloads that
            # include writes (INSERT/UPDATE) to avoid confusing runtime failures.
            has_view = any(
                str(getattr(m, "object_type", "")).strip().upper() == "VIEW"
                for m in self.table_managers
            )
            if has_view:
                workload = self.scenario.workload_type
                writes_in_workload = workload in (
                    WorkloadType.WRITE_ONLY,
                    WorkloadType.READ_HEAVY,
                    WorkloadType.WRITE_HEAVY,
                    WorkloadType.MIXED,
                )
                writes_in_custom = False
                if workload == WorkloadType.CUSTOM:
                    for q in self.scenario.custom_queries or []:
                        kind = str(q.get("query_kind") or "").strip().upper()
                        if kind in {"INSERT", "UPDATE"}:
                            writes_in_custom = True
                            break

                if writes_in_workload or writes_in_custom:
                    self._setup_error = (
                        f"Selected object is a VIEW, but workload {getattr(workload, 'value', workload)} includes writes. "
                        "Choose a TABLE or set workload_type=READ_ONLY."
                    )
                    logger.error(self._setup_error)
                    return False

            # Snowflake-first: lightweight profiling for adaptive reads/range scans
            await self._profile_tables()
            return True

        except Exception as e:
            self._setup_error = f"Error during test setup: {e}"
            logger.error(self._setup_error)
            return False

    async def _profile_tables(self) -> None:
        """
        Profile tables so query generation can adapt to unknown schemas.

        Snowflake-first: if the table manager's pool supports `execute_query`,
        we treat it as Snowflake and run DESCRIBE + MIN/MAX aggregates.

        Optimization: If pre-computed AI value pools exist (from template preparation),
        skip expensive profiling queries since we don't need to discover ID/time ranges.
        """
        # Load persisted value pools FIRST so we can skip profiling when pools exist.
        await self._load_value_pools()

        # If value pools are loaded, we can skip profiling (pools contain pre-computed values).
        has_value_pools = bool(getattr(self, "_value_pools", None))
        if has_value_pools:
            logger.info(
                "⏩ Skipping table profiling - using pre-computed AI value pools"
            )
            # When skipping profiling, we still need a minimal per-table profile so:
            # - CUSTOM workloads can bind parameters (needs id_column/time_column names)
            # - Standard read/write workloads can decide which shapes are possible
            ai_cfg = getattr(self, "_ai_workload", None)
            key_col = None
            time_col = None
            if isinstance(ai_cfg, dict):
                key_col = ai_cfg.get("key_column")
                time_col = ai_cfg.get("time_column")

            # Fallback: infer column names from pools (best-effort).
            pools = getattr(self, "_value_pools", {}) or {}
            if not key_col:
                key_cols = [c for c in (pools.get("KEY", {}) or {}).keys() if c]
                if len(key_cols) == 1:
                    key_col = key_cols[0]
            if not time_col:
                range_cols = [c for c in (pools.get("RANGE", {}) or {}).keys() if c]
                if len(range_cols) == 1:
                    time_col = range_cols[0]

            def _norm_col(c: Any) -> str | None:
                s = str(c or "").strip()
                return s.upper() if s else None

            key_col_u = _norm_col(key_col)
            time_col_u = _norm_col(time_col)

            # Determine max ID from KEY pool for insert_id_seq starting point.
            max_key_id = 0
            key_pool = pools.get("KEY", {}) or {}
            if key_col_u and key_col_u in key_pool:
                key_values = key_pool.get(key_col_u, []) or []
                for v in key_values:
                    try:
                        numeric = int(v) if isinstance(v, (int, float, str)) else 0
                        if numeric > max_key_id:
                            max_key_id = numeric
                    except (ValueError, TypeError):
                        pass

            # Still initialize table state for insert ID tracking if needed.
            for manager in self.table_managers:
                full_name = manager.get_full_table_name()
                self._table_state.setdefault(full_name, _TableRuntimeState())
                state = self._table_state[full_name]
                if state.profile is None:
                    state.profile = TableProfile(
                        full_table_name=full_name,
                        id_column=key_col_u,
                        time_column=time_col_u,
                    )
                if (
                    state.insert_id_seq is None
                    and key_col_u
                    and any(
                        t
                        in str(
                            manager.config.columns.get(key_col_u)
                            or manager.config.columns.get(key_col_u.lower())
                            or ""
                        ).upper()
                        for t in ("NUMBER", "INT", "DECIMAL", "SERIAL")
                    )
                ):
                    # Prefer the real MAX(id) from the table to avoid collisions across runs.
                    max_db_id: int | None = None
                    pool = getattr(manager, "pool", None)
                    if pool is not None:
                        try:
                            id_col_expr = self._quote_column(key_col_u, pool)
                            max_query = f"SELECT MAX({id_col_expr}) FROM {full_name}"
                            if hasattr(pool, "fetch_val"):
                                val = await pool.fetch_val(max_query)
                            elif hasattr(pool, "execute_query"):
                                rows = await pool.execute_query(max_query)
                                val = rows[0][0] if rows and rows[0] else None
                            else:
                                val = None
                            if val is not None:
                                max_db_id = int(val)
                        except Exception as e:
                            logger.debug(
                                "Failed to fetch MAX(%s) for %s: %s",
                                key_col_u,
                                full_name,
                                e,
                            )
                            max_db_id = None

                    base = max_db_id if max_db_id is not None else max_key_id
                    insert_start = int(base) + 1 if base and int(base) > 0 else 1
                    state.insert_id_seq = count(insert_start)
                    logger.info(
                        "INSERT ID sequence starts at %d for %s (max_db_id=%s, max_key_pool=%s)",
                        int(insert_start),
                        full_name,
                        str(max_db_id) if max_db_id is not None else "None",
                        str(max_key_id),
                    )
            return

        for manager in self.table_managers:
            full_name = manager.get_full_table_name()
            self._table_state.setdefault(full_name, _TableRuntimeState())

            pool = getattr(manager, "pool", None)
            if pool is None:
                continue

            try:
                from backend.connectors.postgres_pool import PostgresConnectionPool

                is_postgres = isinstance(pool, PostgresConnectionPool)
            except ImportError:
                is_postgres = False

            try:
                if is_postgres:
                    profile = await profile_postgres_table(pool, full_name)
                elif hasattr(pool, "execute_query_with_info"):
                    profile = await profile_snowflake_table(pool, full_name)
                else:
                    continue

                state = self._table_state[full_name]
                state.profile = profile
                if profile.id_max is not None:
                    state.next_insert_id = profile.id_max + 1
                    state.insert_id_seq = count(int(profile.id_max) + 1)
                elif state.insert_id_seq is None:
                    state.insert_id_seq = count(1)

                # Warn if profiling succeeded but critical fields are missing
                if profile.id_column and (
                    profile.id_min is None or profile.id_max is None
                ):
                    logger.warning(
                        f"Table {full_name} has ID column '{profile.id_column}' but min/max are not set. "
                        f"Point lookup queries will fall back to range scans."
                    )
            except Exception as e:
                logger.warning(
                    "Table profiling failed for %s: %s - Point lookups will not be available",
                    full_name,
                    e,
                )

    async def _load_value_pools(self) -> None:
        """
        Load template-associated value pools (if any) into memory.

        Pools are persisted during template preparation and must never be generated
        at runtime (no AI calls in the hot path).
        """
        tpl_cfg = getattr(self, "_template_config", None)
        tpl_id = getattr(self, "_template_id", None)
        if not isinstance(tpl_cfg, dict) or not tpl_id:
            self._value_pools = {}
            return

        ai_cfg = tpl_cfg.get("ai_workload")
        if not isinstance(ai_cfg, dict):
            self._value_pools = {}
            return

        pool_id = ai_cfg.get("pool_id")
        if not pool_id:
            self._value_pools = {}
            return

        try:
            from backend.connectors import snowflake_pool

            pool = snowflake_pool.get_default_pool()
            rows = await pool.execute_query(
                f"""
                SELECT POOL_KIND, COLUMN_NAME, VALUE
                FROM {settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}.TEMPLATE_VALUE_POOLS
                WHERE TEMPLATE_ID = ?
                  AND POOL_ID = ?
                ORDER BY POOL_KIND, COLUMN_NAME, SEQ
                """,
                params=[str(tpl_id), str(pool_id)],
            )

            def _normalize_variant(v: Any) -> Any:
                # Snowflake VARIANT values may come back as:
                # - Python native (int/float/dict/list/datetime/date)
                # - JSON-ish strings (e.g. '"1996-06-03"' including quotes)
                # Normalize to something safe to bind as a query parameter.
                if isinstance(v, str):
                    s = v.strip()
                    if not s:
                        return v
                    try_json = (
                        s[0] in '{["'
                        or s in {"null", "true", "false"}
                        or s[0].isdigit()
                        or s[0] == "-"
                    )
                    if try_json:
                        try:
                            parsed = json.loads(s)
                            # Check if parsed is an ISO date string (YYYY-MM-DD)
                            # and convert to datetime.date for asyncpg compatibility.
                            if isinstance(parsed, str) and len(parsed) == 10:
                                try:
                                    return datetime.strptime(parsed, "%Y-%m-%d").date()
                                except ValueError:
                                    pass
                            return parsed
                        except Exception:
                            return v
                if isinstance(v, dict):
                    out: dict[str, Any] = {}
                    for k2, v2 in v.items():
                        out[str(k2).strip().upper()] = _normalize_variant(v2)
                    return out
                if isinstance(v, list):
                    return [_normalize_variant(x) for x in v]
                return v

            pools: dict[str, dict[str | None, list[Any]]] = {}
            for kind, col_name, value in rows:
                k = str(kind or "").upper()
                col = str(col_name).upper() if col_name is not None else None
                pools.setdefault(k, {}).setdefault(col, []).append(
                    _normalize_variant(value)
                )

            self._value_pools = pools
            self._ai_workload = ai_cfg
        except Exception as e:
            logger.debug("Failed to load TEMPLATE_VALUE_POOLS: %s", e)
            self._value_pools = {}

    def _select_list_sql(self) -> str:
        """
        Projection list for SELECT queries.

        If the template includes ai_workload.projection_columns, we use it to
        avoid SELECT * on wide customer tables.
        """
        tpl_cfg = getattr(self, "_template_config", None)
        if not isinstance(tpl_cfg, dict):
            return "*"
        ai_cfg = tpl_cfg.get("ai_workload")
        if not isinstance(ai_cfg, dict):
            return "*"
        cols = ai_cfg.get("projection_columns")
        if not isinstance(cols, list):
            return "*"

        out: list[str] = []
        for c in cols:
            s = str(c or "").strip().upper()
            if not s:
                continue
            out.append(f'"{s}"')
        # De-dupe and cap to keep SQL reasonable.
        seen: set[str] = set()
        deduped: list[str] = []
        for c in out:
            if c in seen:
                continue
            seen.add(c)
            deduped.append(c)
        if not deduped:
            return "*"
        return ", ".join(deduped[:50])

    def _pool_values(self, kind: str, column: Optional[str] = None) -> list[Any]:
        pools = getattr(self, "_value_pools", {}) or {}
        kind_u = (kind or "").upper()
        col_u = column.upper() if column else None
        return list(pools.get(kind_u, {}).get(col_u, []))

    def _next_from_pool(
        self, worker_id: int, kind: str, column: Optional[str] = None
    ) -> Any:
        values = self._pool_values(kind, column)
        if not values:
            return None
        # Each worker has its own cyclic walk.
        #
        # IMPORTANT: use a stride based on requested concurrency so that workers
        # traverse largely disjoint subsets early in a run. The previous
        # (n + worker_id) scheme caused heavy overlap across workers and
        # produced massive Snowflake result-cache hit rates (artificially low
        # P95 latencies).
        #
        # For multi-process/multi-worker runs, we shard the pool space across
        # worker groups to reduce cross-worker overlap. This assumes each group
        # runs the same local concurrency.
        if not hasattr(self, "_worker_pool_seq"):
            self._worker_pool_seq = {}
        key = (int(worker_id), (kind or "").upper(), (column or "").upper())
        n = int(getattr(self, "_worker_pool_seq", {}).get(key, 0))
        getattr(self, "_worker_pool_seq")[key] = n + 1
        local_concurrency = int(getattr(self.scenario, "total_threads", 1) or 1)
        local_concurrency = max(1, local_concurrency)
        group_id = int(getattr(self.scenario, "worker_group_id", 0) or 0)
        group_count = int(getattr(self.scenario, "worker_group_count", 1) or 1)
        group_count = max(1, group_count)
        if group_id < 0:
            group_id = 0
        if group_id >= group_count:
            group_id = group_count - 1
        global_stride = local_concurrency * group_count
        global_worker_id = (group_id * local_concurrency) + int(worker_id)
        idx = (n * global_stride + global_worker_id) % len(values)
        return values[idx]

    async def execute(self) -> TestResult:
        """
        Execute performance test.

        - Spawn concurrent workers
        - Generate workloads
        - Collect metrics
        - Return results

        Returns:
            TestResult: Test execution results
        """
        metrics_task: Optional[asyncio.Task] = None
        qps_controller_task: Optional[asyncio.Task] = None
        try:
            load_mode = (
                str(getattr(self.scenario, "load_mode", "CONCURRENCY") or "CONCURRENCY")
                .strip()
                .upper()
            )
            if load_mode not in {"CONCURRENCY", "QPS", "FIND_MAX_CONCURRENCY"}:
                load_mode = "CONCURRENCY"

            controller_logger = logging.LoggerAdapter(
                logger, {"worker_id": "CONTROLLER"}
            )

            logger.info("🚀 Executing test: %s", self.scenario.name)
            if load_mode == "FIND_MAX_CONCURRENCY":
                start_cc = int(getattr(self.scenario, "start_concurrency", 5) or 5)
                increment = int(
                    getattr(self.scenario, "concurrency_increment", 10) or 10
                )
                step_dur = int(
                    getattr(self.scenario, "step_duration_seconds", 30) or 30
                )
                max_cc = int(self.scenario.total_threads or 100)
                logger.info(
                    "📋 Workload: %s, Mode: FIND_MAX_CONCURRENCY, Start: %d, Increment: %d, Step: %ds, Max: %d",
                    self.scenario.workload_type,
                    start_cc,
                    increment,
                    step_dur,
                    max_cc,
                )
            elif load_mode == "QPS":
                # In "QPS" mode we dynamically scale workers to target throughput (ops/sec).
                # Snowflake RUNNING/queued are sampled for observability/safety, but are not the target.
                logger.info(
                    "📋 Workload: %s, Mode: QPS (auto-scale), Target QPS: %s, Warmup: %ss, Run: %ss, Min threads: %s, Max threads: %s",
                    self.scenario.workload_type,
                    getattr(self.scenario, "target_qps", None),
                    self.scenario.warmup_seconds,
                    self.scenario.duration_seconds,
                    getattr(self.scenario, "min_threads_per_worker", None),
                    self.scenario.total_threads,
                )
            else:
                logger.info(
                    "📋 Workload: %s, Mode: CONCURRENCY, Duration: %ss, Workers: %s",
                    self.scenario.workload_type,
                    self.scenario.duration_seconds,
                    self.scenario.total_threads,
                )

            self.status = TestStatus.RUNNING
            self.start_time = datetime.now(UTC)
            self.metrics.timestamp = self.start_time
            # Reset measurement state for this run.
            self._metrics_epoch = 0
            self._measurement_active = False
            self._sql_error_categories.clear()
            self._sql_error_sample_counts.clear()

            # Start metrics collector immediately (including warmup) so the UI
            # receives steady updates and the websocket doesn't go idle.
            metrics_task = asyncio.create_task(self._collect_metrics())

            # QPS mode: dynamic workers.
            if load_mode == "QPS":
                try:
                    target_raw = float(getattr(self.scenario, "target_qps", 0.0) or 0.0)
                except Exception:
                    target_raw = 0.0
                if not math.isfinite(target_raw) or target_raw <= 0:
                    raise ValueError("load_mode=QPS requires target_qps > 0")

                # Default to QPS targeting - more accurate than SF_RUNNING
                # because sub-second queries may complete faster than the 1s sampling interval.
                use_sf_running_target = False
                target_sf_running = int(max(1, round(float(target_raw))))
                target_qps = float(target_raw)

                tpl_cfg = getattr(self, "_template_config", None)
                autoscale_parent_run_id: str | None = None
                autoscale_enabled = False
                if isinstance(tpl_cfg, dict):
                    # Derive autoscale_enabled from scaling.mode (AUTO/BOUNDED = enabled)
                    scaling_cfg = tpl_cfg.get("scaling") or {}
                    scaling_mode = str(scaling_cfg.get("mode") or "AUTO").upper()
                    autoscale_enabled = scaling_mode != "FIXED"
                    parent_run_id = tpl_cfg.get("parent_run_id")
                    if parent_run_id:
                        autoscale_parent_run_id = str(parent_run_id)

                min_workers = int(
                    getattr(self.scenario, "min_threads_per_worker", 1) or 1
                )
                max_workers = int(getattr(self.scenario, "total_threads", 1) or 1)
                min_workers = max(1, min_workers)
                max_workers = max(1, max_workers)
                if min_workers > max_workers:
                    min_workers = max_workers

                qps_workers: dict[int, tuple[asyncio.Task, asyncio.Event]] = {}
                next_worker_id = 0

                def _sync_worker_list() -> None:
                    self.workers = [t for t, _ in qps_workers.values() if not t.done()]

                def _prune_done() -> None:
                    for wid, (t, _) in list(qps_workers.items()):
                        if t.done():
                            qps_workers.pop(wid, None)

                def _running_worker_ids() -> list[int]:
                    out: list[int] = []
                    for wid, (t, stop_signal) in qps_workers.items():
                        if t.done():
                            continue
                        if stop_signal.is_set():
                            continue
                        out.append(int(wid))
                    return out

                def _live_worker_ids() -> list[int]:
                    out: list[int] = []
                    for wid, (t, _) in qps_workers.items():
                        if t.done():
                            continue
                        out.append(int(wid))
                    return out

                def _active_worker_count() -> int:
                    return len(_running_worker_ids())

                def _live_worker_count() -> int:
                    return len(_live_worker_ids())

                async def _fetch_autoscale_target_qps() -> tuple[
                    float | None, int | None
                ]:
                    if not autoscale_enabled or not autoscale_parent_run_id:
                        return (None, None)
                    pool = snowflake_pool.get_default_pool()
                    rows = await pool.execute_query(
                        f"""
                        SELECT CUSTOM_METRICS
                        FROM {settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}.TEST_RESULTS
                        WHERE TEST_ID = ?
                        """,
                        params=[autoscale_parent_run_id],
                    )
                    if not rows or not rows[0]:
                        return (None, None)
                    cm = rows[0][0]
                    if isinstance(cm, str):
                        try:
                            cm = json.loads(cm)
                        except Exception:
                            cm = {}
                    if not isinstance(cm, dict):
                        return (None, None)
                    autoscale_state = cm.get("autoscale_state")
                    if not isinstance(autoscale_state, dict):
                        return (None, None)
                    try:
                        worker_count = int(autoscale_state.get("worker_count") or 0)
                    except Exception:
                        worker_count = 0
                    try:
                        target_total = float(
                            autoscale_state.get("target_qps_total") or 0.0
                        )
                    except Exception:
                        target_total = 0.0
                    if (
                        worker_count <= 0
                        or not math.isfinite(target_total)
                        or target_total <= 0
                    ):
                        return (None, None)
                    per_worker_target = target_total / float(worker_count)
                    return (float(per_worker_target), int(worker_count))

                async def _spawn_one(*, warmup: bool) -> None:
                    nonlocal next_worker_id
                    wid = int(next_worker_id)
                    next_worker_id += 1
                    stop_signal = asyncio.Event()
                    task = asyncio.create_task(
                        self._controlled_worker(
                            worker_id=wid, warmup=warmup, stop_signal=stop_signal
                        )
                    )
                    qps_workers[wid] = (task, stop_signal)
                    _sync_worker_list()

                async def _scale_to(*, target: int, warmup: bool) -> None:
                    _prune_done()
                    target = int(target)
                    target = max(min_workers, min(max_workers, target))
                    self._target_workers = target  # Track target for metrics

                    running_ids = sorted(_running_worker_ids())
                    running = len(running_ids)
                    live = len(_live_worker_ids())

                    # Scale up:
                    # - Never exceed max_workers/pool_size by counting ALL live tasks,
                    #   including stop-signaled ones that are still finishing an in-flight query.
                    if running < target:
                        spawn_n = min((target - running), max(0, target - live))
                        for _ in range(spawn_n):
                            await _spawn_one(warmup=warmup)

                    # Scale down:
                    # - Only mark currently running workers to stop; workers already
                    #   stop-signaled still occupy capacity until they fully exit.
                    elif running > target:
                        stop_n = running - target
                        stop_ids = list(reversed(running_ids))[:stop_n]
                        for wid in stop_ids:
                            _, stop_signal = qps_workers.get(wid, (None, None))
                            if isinstance(stop_signal, asyncio.Event):
                                stop_signal.set()
                        _sync_worker_list()

                async def _stop_all_workers(*, timeout_seconds: float) -> None:
                    _prune_done()
                    for _, stop_signal in qps_workers.values():
                        stop_signal.set()
                    _sync_worker_list()
                    tasks = [t for t, _ in qps_workers.values()]
                    if not tasks:
                        qps_workers.clear()
                        return
                    # Never cancel workers here: cancelling tasks while they're awaiting
                    # `run_in_executor(...)` can leave the underlying thread still running
                    # a query while the connection is returned to the pool.
                    #
                    # Use timeout to avoid hanging indefinitely if workers are blocked
                    # on uninterruptible I/O operations.
                    try:
                        await asyncio.wait_for(
                            asyncio.gather(*tasks, return_exceptions=True),
                            timeout=timeout_seconds,
                        )
                    except asyncio.TimeoutError:
                        logger.warning(
                            "Timed out waiting for QPS workers to stop after %.1fs",
                            timeout_seconds,
                        )
                    qps_workers.clear()
                    _sync_worker_list()

                def _desired_workers_from_qps(
                    *,
                    current_active: int,
                    current_qps: float,
                    target_qps_value: float,
                    avg_latency_ms: float = 0.0,
                ) -> int:
                    """
                    Balanced proportional controller - responsive but smooth.

                    - Moderate gains to reach target without excessive overshoot
                    - Rate limiting (max 10% change per tick) prevents oscillation
                    - Uses theoretical workers from latency as guidance
                    """
                    if current_active <= 0:
                        return min_workers

                    if (
                        not current_qps
                        or not math.isfinite(current_qps)
                        or current_qps <= 0
                    ):
                        if (
                            avg_latency_ms
                            and math.isfinite(avg_latency_ms)
                            and avg_latency_ms > 0
                        ):
                            ops_per_worker = 1000.0 / avg_latency_ms
                            theoretical = int(
                                math.ceil(target_qps_value / ops_per_worker)
                            )
                            return max(min_workers, min(max_workers, theoretical))
                        return current_active

                    error = target_qps_value - current_qps
                    error_ratio = error / target_qps_value
                    abs_error_ratio = abs(error_ratio)

                    # Moderate gains - enough to converge but not overshoot
                    if abs_error_ratio > 0.30:
                        gain = 0.35  # Aggressive when far off
                    elif abs_error_ratio > 0.15:
                        gain = 0.25
                    elif abs_error_ratio > 0.05:
                        gain = 0.15
                    else:
                        gain = 0.10  # Still responsive near target

                    adjustment_ratio = error_ratio * gain
                    desired_float = float(current_active) * (1.0 + adjustment_ratio)

                    # Use latency-based estimate as guidance
                    if (
                        avg_latency_ms
                        and math.isfinite(avg_latency_ms)
                        and avg_latency_ms > 0
                    ):
                        ops_per_worker = 1000.0 / avg_latency_ms
                        theoretical = target_qps / ops_per_worker
                        if error_ratio > 0.10:  # Under target
                            desired_float = max(desired_float, theoretical * 0.9)

                    desired = int(round(desired_float))

                    # Rate limit: max 10% change per tick to smooth oscillations
                    max_change = max(2, int(current_active * 0.10))
                    if abs(desired - current_active) > max_change:
                        if desired > current_active:
                            desired = current_active + max_change
                        else:
                            desired = current_active - max_change

                    return max(min_workers, min(max_workers, desired))

                def _desired_workers_from_sf_running(
                    *, current_workers: int, sf_running: int
                ) -> int:
                    # Primary estimator: error-based control on Snowflake RUNNING.
                    #
                    # desired ≈ workers + (target_running - observed_running)
                    # Clamp to [min_workers, max_workers].
                    if current_workers <= 0:
                        return min_workers
                    desired = (
                        int(current_workers) + int(target_sf_running) - int(sf_running)
                    )
                    if desired < min_workers:
                        desired = min_workers
                    if desired > max_workers:
                        desired = max_workers
                    return desired

                async def _qps_controller(*, warmup: bool) -> None:
                    metrics_interval = float(
                        getattr(self.scenario, "metrics_interval_seconds", 1.0) or 1.0
                    )
                    if not math.isfinite(metrics_interval) or metrics_interval <= 0:
                        metrics_interval = 1.0

                    # Control interval: 5s balances responsiveness with stability
                    control_interval = max(5.0, float(metrics_interval))

                    # Moderate scale steps with rate limiting in the controller
                    max_step = max(1, min(10, max_workers))
                    max_step_down = max(1, min(6, max_workers))
                    last_logged_target: int | None = None
                    above_target_streak = 0
                    under_target_streak = 0
                    autoscale_last_poll = 0.0
                    autoscale_target_qps: float | None = None
                    autoscale_worker_count: int | None = None
                    autoscale_poll_interval = max(5.0, float(control_interval))

                    while not self._stop_event.is_set():
                        await asyncio.sleep(control_interval)
                        _prune_done()

                        current_running = _active_worker_count()
                        current_live = _live_worker_count()
                        if current_running <= 0:
                            await _scale_to(target=min_workers, warmup=warmup)
                            current_running = _active_worker_count()
                            current_live = _live_worker_count()

                        current_target_qps = float(target_qps)
                        if autoscale_enabled and autoscale_parent_run_id:
                            now_mono = float(asyncio.get_running_loop().time())
                            if now_mono - autoscale_last_poll >= float(
                                autoscale_poll_interval
                            ):
                                (
                                    per_worker_target,
                                    worker_count,
                                ) = await _fetch_autoscale_target_qps()
                                if per_worker_target is not None:
                                    autoscale_target_qps = float(per_worker_target)
                                    autoscale_worker_count = (
                                        int(worker_count)
                                        if worker_count is not None
                                        else None
                                    )
                                autoscale_last_poll = float(now_mono)
                            if autoscale_target_qps is not None:
                                current_target_qps = float(autoscale_target_qps)

                        if use_sf_running_target:
                            now_ctl_mono = float(asyncio.get_running_loop().time())
                            bench = (
                                dict(self._warehouse_query_status or {})
                                if self._warehouse_query_status
                                else {}
                            )
                            sf_running = int(bench.get("running") or 0)
                            sf_queued = int(bench.get("queued") or 0)
                            sf_blocked = int(bench.get("blocked") or 0)
                            sf_resuming = int(bench.get("resuming_warehouse") or 0)
                            sample_mono = bench.get("sample_mono")
                            sample_age_s = None
                            try:
                                if sample_mono is not None:
                                    sample_age_s = float(now_ctl_mono) - float(
                                        sample_mono
                                    )
                            except Exception:
                                sample_age_s = None

                            # If Snowflake telemetry is missing/stale, avoid runaway scaling.
                            stale = sample_age_s is None or (
                                math.isfinite(sample_age_s) and sample_age_s > 15.0
                            )

                            # Deadband/hysteresis: ignore small deviations so we don't thrash.
                            # Reduced from 5% to 2% for tighter control.
                            deadband_running = max(
                                1, int(round(float(target_sf_running) * 0.02))
                            )
                            under_target = sf_running < (
                                int(target_sf_running) - int(deadband_running)
                            )
                            over_target = sf_running > (
                                int(target_sf_running) + int(deadband_running)
                            )
                            pressure = (sf_queued + sf_blocked + sf_resuming) > 0

                            if stale:
                                above_target_streak = 0
                                self._qps_controller_state = {
                                    "phase": "WARMUP" if warmup else "RUNNING",
                                    "target_mode": "SF_RUNNING",
                                    "target_sf_running": int(target_sf_running),
                                    "observed_sf_running": int(sf_running),
                                    "observed_sf_queued": int(sf_queued),
                                    "observed_sf_blocked": int(sf_blocked),
                                    "observed_sf_resuming": int(sf_resuming),
                                    "deadband_running": int(deadband_running),
                                    "sample_age_seconds": sample_age_s,
                                    "running_workers": int(current_running),
                                    "live_workers": int(current_live),
                                    "desired_workers": int(current_running),
                                    "control_interval_seconds": float(control_interval),
                                    "above_target_streak": int(above_target_streak),
                                }
                                continue

                            if under_target and not pressure:
                                above_target_streak = 0
                            elif over_target or pressure:
                                above_target_streak += 1
                            else:
                                above_target_streak = 0

                            desired = _desired_workers_from_sf_running(
                                current_workers=current_running, sf_running=sf_running
                            )

                            # If the warehouse is queueing or blocked, never scale up.
                            if pressure and desired > current_running:
                                desired = current_running
                            # Under pressure, bias toward scaling down at least 1 worker.
                            if pressure and desired >= current_running:
                                desired = max(min_workers, int(current_running) - 1)
                            if (over_target or pressure) and above_target_streak < 1:
                                # Require sustained overage before scaling down due to over-target
                                # (but pressure can scale down immediately; streak applies mainly
                                # to completion-burst spikes).
                                if not pressure:
                                    desired = current_running

                            if desired != current_running:
                                diff = desired - current_running
                                step_cap = max_step if diff > 0 else max_step_down
                                if abs(diff) > step_cap:
                                    desired = current_running + (
                                        step_cap if diff > 0 else -step_cap
                                    )

                                await _scale_to(target=desired, warmup=warmup)
                                now_running = _active_worker_count()
                                now_live = _live_worker_count()
                                if last_logged_target != desired:
                                    phase = "WARMUP" if warmup else "RUNNING"
                                    controller_logger.info(
                                        "Auto-scale controller (%s): target_sf_running=%d observed_sf_running=%d queued=%d blocked=%d resuming=%d running_workers=%d → %d (live=%d → %d)",
                                        phase,
                                        int(target_sf_running),
                                        int(sf_running),
                                        int(sf_queued),
                                        int(sf_blocked),
                                        int(sf_resuming),
                                        int(current_running),
                                        int(now_running),
                                        int(current_live),
                                        int(now_live),
                                    )
                                    last_logged_target = int(desired)
                                self._qps_controller_state = {
                                    "phase": "WARMUP" if warmup else "RUNNING",
                                    "target_mode": "SF_RUNNING",
                                    "target_sf_running": int(target_sf_running),
                                    "observed_sf_running": int(sf_running),
                                    "observed_sf_queued": int(sf_queued),
                                    "observed_sf_blocked": int(sf_blocked),
                                    "observed_sf_resuming": int(sf_resuming),
                                    "deadband_running": int(deadband_running),
                                    "sample_age_seconds": sample_age_s,
                                    "running_workers": int(now_running),
                                    "live_workers": int(now_live),
                                    "desired_workers": int(desired),
                                    "control_interval_seconds": float(control_interval),
                                    "above_target_streak": int(above_target_streak),
                                }
                            else:
                                self._qps_controller_state = {
                                    "phase": "WARMUP" if warmup else "RUNNING",
                                    "target_mode": "SF_RUNNING",
                                    "target_sf_running": int(target_sf_running),
                                    "observed_sf_running": int(sf_running),
                                    "observed_sf_queued": int(sf_queued),
                                    "observed_sf_blocked": int(sf_blocked),
                                    "observed_sf_resuming": int(sf_resuming),
                                    "deadband_running": int(deadband_running),
                                    "sample_age_seconds": sample_age_s,
                                    "running_workers": int(current_running),
                                    "live_workers": int(current_live),
                                    "desired_workers": int(desired),
                                    "control_interval_seconds": float(control_interval),
                                    "above_target_streak": int(above_target_streak),
                                }
                        else:
                            # Read the latest computed QPS (updated by the metrics collector task).
                            async with self._metrics_lock:
                                qps_raw = float(
                                    getattr(self.metrics, "current_qps", 0.0) or 0.0
                                )
                                qps_smoothed = float(self._qps_smoothed or 0.0)
                                qps_windowed = float(self._qps_windowed or 0.0)

                            qps_used = (
                                qps_windowed
                                if qps_windowed > 0
                                else (qps_smoothed if qps_smoothed > 0 else qps_raw)
                            )

                            # Deadband/hysteresis: ignore small deviations so we don't thrash.
                            # 5% deadband - balance between responsiveness and stability
                            deadband = max(2.0, float(current_target_qps) * 0.05)
                            if (
                                qps_used > 0
                                and abs(qps_used - float(current_target_qps))
                                <= deadband
                            ):
                                above_target_streak = 0
                                under_target_streak = 0
                                self._qps_controller_state = {
                                    "phase": "WARMUP" if warmup else "RUNNING",
                                    "target_mode": "QPS",
                                    "target_qps": float(target_qps),
                                    "observed_qps_raw": float(qps_raw),
                                    "observed_qps_smoothed": float(qps_smoothed),
                                    "observed_qps_windowed": float(qps_windowed),
                                    "observed_qps_used": float(qps_used),
                                    "deadband_qps": float(deadband),
                                    "running_workers": int(current_running),
                                    "live_workers": int(current_live),
                                    "desired_workers": int(current_running),
                                    "control_interval_seconds": float(control_interval),
                                    "above_target_streak": int(above_target_streak),
                                    "under_target_streak": int(under_target_streak),
                                }
                                continue

                            # Under-target: always allow scale-up. Over-target: require sustained
                            # overage before scaling down to avoid reacting to completion bursts.
                            under_target = qps_used < (
                                float(current_target_qps) - float(deadband)
                            )
                            over_target = qps_used > (
                                float(current_target_qps) + float(deadband)
                            )
                            if under_target:
                                above_target_streak = 0
                                under_target_streak += 1
                            elif over_target:
                                above_target_streak += 1
                                under_target_streak = 0
                            else:
                                above_target_streak = 0
                                under_target_streak = 0

                            async with self._metrics_lock:
                                avg_latency = float(
                                    getattr(self.metrics.overall_latency, "avg", 0.0)
                                    or 0.0
                                )

                            desired = _desired_workers_from_qps(
                                current_active=current_running,
                                current_qps=qps_used,
                                target_qps_value=current_target_qps,
                                avg_latency_ms=avg_latency,
                            )
                            if under_target and under_target_streak < 2:
                                desired = current_running
                            if over_target and above_target_streak < 1:
                                desired = current_running
                            if desired != current_running:
                                await _scale_to(target=desired, warmup=warmup)
                                now_running = _active_worker_count()
                                now_live = _live_worker_count()
                                if last_logged_target != desired:
                                    phase = "WARMUP" if warmup else "RUNNING"
                                    controller_logger.info(
                                        "QPS controller (%s): target_qps=%.2f observed_qps=%.2f (windowed=%.2f smoothed=%.2f raw=%.2f) running_workers=%d → %d (live=%d → %d)",
                                        phase,
                                        float(current_target_qps),
                                        float(qps_used),
                                        float(qps_windowed),
                                        float(qps_smoothed),
                                        float(qps_raw),
                                        int(current_running),
                                        int(now_running),
                                        int(current_live),
                                        int(now_live),
                                    )
                                    last_logged_target = int(desired)
                                self._qps_controller_state = {
                                    "phase": "WARMUP" if warmup else "RUNNING",
                                    "target_mode": "QPS",
                                    "target_qps": float(current_target_qps),
                                    "observed_qps_raw": float(qps_raw),
                                    "observed_qps_smoothed": float(qps_smoothed),
                                    "observed_qps_windowed": float(qps_windowed),
                                    "observed_qps_used": float(qps_used),
                                    "deadband_qps": float(deadband),
                                    "running_workers": int(now_running),
                                    "live_workers": int(now_live),
                                    "desired_workers": int(desired),
                                    "control_interval_seconds": float(control_interval),
                                    "above_target_streak": int(above_target_streak),
                                    "under_target_streak": int(under_target_streak),
                                    "autoscale_worker_count": autoscale_worker_count,
                                }
                            else:
                                self._qps_controller_state = {
                                    "phase": "WARMUP" if warmup else "RUNNING",
                                    "target_mode": "QPS",
                                    "target_qps": float(current_target_qps),
                                    "observed_qps_raw": float(qps_raw),
                                    "observed_qps_smoothed": float(qps_smoothed),
                                    "observed_qps_windowed": float(qps_windowed),
                                    "observed_qps_used": float(qps_used),
                                    "deadband_qps": float(deadband),
                                    "running_workers": int(current_running),
                                    "live_workers": int(current_live),
                                    "desired_workers": int(desired),
                                    "control_interval_seconds": float(control_interval),
                                    "above_target_streak": int(above_target_streak),
                                    "under_target_streak": int(under_target_streak),
                                    "autoscale_worker_count": autoscale_worker_count,
                                }

                # Warmup period (ramp workers so the measurement phase starts close to target).
                estimated_workers = min_workers
                if self.scenario.warmup_seconds > 0:
                    target_label = (
                        "target_sf_running" if use_sf_running_target else "target_qps"
                    )
                    target_val = (
                        float(target_sf_running)
                        if use_sf_running_target
                        else float(target_qps)
                    )
                    logger.info(
                        "Warming up for %ds (QPS ramp: %s=%.2f, min_workers=%d, max_workers=%d)...",
                        int(self.scenario.warmup_seconds),
                        str(target_label),
                        float(target_val),
                        int(min_workers),
                        int(max_workers),
                    )
                    await _scale_to(target=min_workers, warmup=True)
                    qps_controller_task = asyncio.create_task(
                        _qps_controller(warmup=True)
                    )
                    await asyncio.sleep(self.scenario.warmup_seconds)
                    qps_controller_task.cancel()
                    try:
                        await qps_controller_task
                    except asyncio.CancelledError:
                        pass
                    except Exception:
                        pass
                    qps_controller_task = None

                    _prune_done()
                    estimated_workers = _active_worker_count()
                    logger.info(
                        "Warmup complete. Estimated starting workers for measurement: %d (max=%d).",
                        int(estimated_workers),
                        int(max_workers),
                    )

                    # Stop warmup workers before resetting metrics so warmup ops can't leak into the
                    # measurement window.
                    #
                    # NOTE: Do not stop/wait for workers here. Under heavy load, Snowflake
                    # operations can hit connector timeouts/retries and take a long time to
                    # finish. Waiting would extend the warmup phase and delay measurement.
                    #
                    # Instead, we advance to measurement immediately and use a metrics epoch
                    # to ensure late warmup completions cannot pollute the measurement window.

                # Transition QUERY_TAG from :phase=WARMUP to :phase=RUNNING before resetting
                # metrics so SF QUERY_HISTORY can distinguish warmup vs measurement queries.
                await self._transition_to_measurement_phase()

                # Reset metrics after warmup (measurement window begins here).
                async with self._metrics_lock:
                    self._metrics_epoch += 1
                    self._measurement_active = True
                    self.metrics = Metrics()
                    self.metrics.timestamp = datetime.now(UTC)
                    self._last_snapshot_time = None
                    self._last_snapshot_mono = None
                    self._last_snapshot_ops = 0
                    self._qps_smoothed = None
                    self._qps_windowed = None
                    self._qps_samples.clear()
                    self._qps_controller_state = {}
                    self._latencies_ms.clear()
                    self._sql_error_categories.clear()
                    self._latency_sf_execution_ms.clear()
                    self._latency_network_overhead_ms.clear()
                self._measurement_start_time = datetime.now(UTC)

                # Start measurement with the warmup-derived estimate.
                start_workers = int(
                    max(min_workers, min(max_workers, estimated_workers))
                )
                target_label = (
                    "target_sf_running" if use_sf_running_target else "target_qps"
                )
                target_val = (
                    float(target_sf_running)
                    if use_sf_running_target
                    else float(target_qps)
                )
                logger.info(
                    "Starting measurement (QPS mode): %s=%.2f, initial_workers=%d, max_workers=%d, run_seconds=%d",
                    str(target_label),
                    float(target_val),
                    int(start_workers),
                    int(max_workers),
                    int(self.scenario.duration_seconds),
                )
                await _scale_to(target=start_workers, warmup=False)
                qps_controller_task = asyncio.create_task(_qps_controller(warmup=False))

                await asyncio.sleep(self.scenario.duration_seconds)

                # Stop controller before stopping workers.
                qps_controller_task.cancel()
                try:
                    await qps_controller_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
                qps_controller_task = None

                logger.info("Stopping workers...")
                self._stop_event.set()
                await _stop_all_workers(timeout_seconds=2.0)

            elif load_mode == "FIND_MAX_CONCURRENCY":
                # FIND_MAX_CONCURRENCY mode: step-load test to find max sustainable concurrency
                start_cc = int(getattr(self.scenario, "start_concurrency", 5) or 5)
                increment = int(
                    getattr(self.scenario, "concurrency_increment", 10) or 10
                )
                step_dur = int(
                    getattr(self.scenario, "step_duration_seconds", 30) or 30
                )
                max_cc = int(self.scenario.total_threads or 100)
                qps_stability_pct = float(
                    getattr(self.scenario, "qps_stability_pct", 5.0) or 5.0
                )
                latency_stability_pct = float(
                    getattr(self.scenario, "latency_stability_pct", 20.0) or 20.0
                )
                max_error_rate_pct = float(
                    getattr(self.scenario, "max_error_rate_pct", 1.0) or 1.0
                )

                start_cc = max(1, min(start_cc, max_cc))
                increment = max(1, increment)

                # Optional hard SLO constraints from template config (per query kind).
                tpl_cfg = getattr(self, "_template_config", None)

                def _tpl_float(key: str, default: float) -> float:
                    if not isinstance(tpl_cfg, dict):
                        return float(default)
                    try:
                        v = float(tpl_cfg.get(key, default))
                    except Exception:
                        return float(default)
                    return float(v) if math.isfinite(v) else float(default)

                def _pctile(samples: list[float], pct: float) -> float | None:
                    if not samples:
                        return None
                    sorted_lat = sorted(samples)
                    idx = int(len(sorted_lat) * (float(pct) / 100.0))
                    idx = min(max(0, idx), len(sorted_lat) - 1)
                    return float(sorted_lat[idx])

                fmc_slo_by_kind: dict[str, dict[str, float]] = {
                    "POINT_LOOKUP": {
                        "weight_pct": max(
                            0.0, _tpl_float("custom_point_lookup_pct", 0.0)
                        ),
                        "target_p95_ms": _tpl_float(
                            "target_point_lookup_p95_latency_ms", -1.0
                        ),
                        "target_p99_ms": _tpl_float(
                            "target_point_lookup_p99_latency_ms", -1.0
                        ),
                        "target_err_pct": _tpl_float(
                            "target_point_lookup_error_rate_pct", -1.0
                        ),
                    },
                    "RANGE_SCAN": {
                        "weight_pct": max(
                            0.0, _tpl_float("custom_range_scan_pct", 0.0)
                        ),
                        "target_p95_ms": _tpl_float(
                            "target_range_scan_p95_latency_ms", -1.0
                        ),
                        "target_p99_ms": _tpl_float(
                            "target_range_scan_p99_latency_ms", -1.0
                        ),
                        "target_err_pct": _tpl_float(
                            "target_range_scan_error_rate_pct", -1.0
                        ),
                    },
                    "INSERT": {
                        "weight_pct": max(0.0, _tpl_float("custom_insert_pct", 0.0)),
                        "target_p95_ms": _tpl_float(
                            "target_insert_p95_latency_ms", -1.0
                        ),
                        "target_p99_ms": _tpl_float(
                            "target_insert_p99_latency_ms", -1.0
                        ),
                        "target_err_pct": _tpl_float(
                            "target_insert_error_rate_pct", -1.0
                        ),
                    },
                    "UPDATE": {
                        "weight_pct": max(0.0, _tpl_float("custom_update_pct", 0.0)),
                        "target_p95_ms": _tpl_float(
                            "target_update_p95_latency_ms", -1.0
                        ),
                        "target_p99_ms": _tpl_float(
                            "target_update_p99_latency_ms", -1.0
                        ),
                        "target_err_pct": _tpl_float(
                            "target_update_error_rate_pct", -1.0
                        ),
                    },
                }

                fmc_workers: dict[int, tuple[asyncio.Task, asyncio.Event]] = {}
                fmc_next_worker_id = 0

                def _fmc_sync_worker_list() -> None:
                    self.workers = [t for t, _ in fmc_workers.values() if not t.done()]

                def _fmc_prune_done() -> None:
                    for wid, (t, _) in list(fmc_workers.items()):
                        if t.done():
                            fmc_workers.pop(wid, None)

                def _fmc_running_worker_ids() -> list[int]:
                    out: list[int] = []
                    for wid, (t, stop_signal) in fmc_workers.items():
                        if t.done() or stop_signal.is_set():
                            continue
                        out.append(int(wid))
                    return out

                def _fmc_active_worker_count() -> int:
                    return len(_fmc_running_worker_ids())

                async def _fmc_spawn_one(*, warmup: bool) -> None:
                    nonlocal fmc_next_worker_id
                    wid = int(fmc_next_worker_id)
                    fmc_next_worker_id += 1
                    stop_signal = asyncio.Event()
                    task = asyncio.create_task(
                        self._controlled_worker(
                            worker_id=wid, warmup=warmup, stop_signal=stop_signal
                        )
                    )
                    fmc_workers[wid] = (task, stop_signal)
                    _fmc_sync_worker_list()

                async def _fmc_scale_to(*, target: int, warmup: bool) -> None:
                    _fmc_prune_done()
                    target = int(max(1, min(max_cc, target)))
                    self._target_workers = target  # Track target for metrics
                    running_ids = sorted(_fmc_running_worker_ids())
                    running = len(running_ids)

                    if running < target:
                        for _ in range(target - running):
                            await _fmc_spawn_one(warmup=warmup)
                    elif running > target:
                        to_stop = running_ids[target:]
                        for wid in to_stop:
                            if wid in fmc_workers:
                                _, stop_sig = fmc_workers[wid]
                                stop_sig.set()

                async def _fmc_stop_all_workers(timeout_seconds: float = 2.0) -> None:
                    for _, (_, stop_signal) in fmc_workers.items():
                        stop_signal.set()
                    tasks = [t for t, _ in fmc_workers.values() if not t.done()]
                    if tasks:
                        try:
                            await asyncio.wait_for(
                                asyncio.gather(*tasks, return_exceptions=True),
                                timeout=timeout_seconds,
                            )
                        except asyncio.TimeoutError:
                            logger.warning(
                                "Timed out waiting for FMC workers to stop after %.1fs",
                                timeout_seconds,
                            )
                    fmc_workers.clear()
                    _fmc_sync_worker_list()

                @dataclass
                class StepResult:
                    step_num: int
                    concurrency: int
                    qps: float
                    p95_latency_ms: float
                    p99_latency_ms: float
                    error_rate_pct: float
                    stable: bool
                    kind_metrics: dict[str, dict[str, float | None]] = field(
                        default_factory=dict
                    )
                    stop_reason: str | None = None
                    is_backoff: bool = False

                step_results: list[StepResult] = []
                best_concurrency = start_cc
                best_qps = 0.0
                baseline_p95_latency: float | None = (
                    None  # First step's latency as baseline
                )
                baseline_p99_latency: float | None = (
                    None  # First step's p99 as baseline
                )
                termination_reason: str | None = None

                # Warmup period (optional, at start_concurrency level)
                if self.scenario.warmup_seconds > 0:
                    controller_logger.info(
                        f"FIND_MAX: Warmup for {self.scenario.warmup_seconds}s at {start_cc} workers..."
                    )
                    await _fmc_scale_to(target=start_cc, warmup=True)
                    await asyncio.sleep(self.scenario.warmup_seconds)

                # Transition to measurement phase
                await self._transition_to_measurement_phase()
                async with self._metrics_lock:
                    self._metrics_epoch += 1
                    self._measurement_active = True
                    self.metrics = Metrics()
                    self.metrics.timestamp = datetime.now(UTC)
                    self._measurement_start_time = datetime.now(UTC)
                    self._last_snapshot_time = None
                    self._last_snapshot_mono = None
                    self._last_snapshot_ops = 0
                    self._qps_smoothed = None
                    self._qps_windowed = None
                    self._qps_samples.clear()
                    self._latencies_ms.clear()
                    self._sql_error_categories.clear()
                    self._latency_sf_execution_ms.clear()
                    self._latency_network_overhead_ms.clear()
                    # FMC per-step stats are reset at each step; ensure they start clean.
                    self._find_max_step_collecting = False
                    for k in self._find_max_step_lat_by_kind_ms:
                        self._find_max_step_lat_by_kind_ms[k].clear()
                        self._find_max_step_ops_by_kind[k] = 0
                        self._find_max_step_errors_by_kind[k] = 0

                current_cc = start_cc
                step_num = 0
                backoff_attempts = 0
                max_backoff_attempts = 3

                def _build_step_history():
                    return [
                        {
                            "step": s.step_num,
                            "concurrency": s.concurrency,
                            "qps": s.qps,
                            "p95_latency_ms": s.p95_latency_ms,
                            "p99_latency_ms": s.p99_latency_ms,
                            "error_rate_pct": s.error_rate_pct,
                            "kind_metrics": s.kind_metrics,
                            "stable": s.stable,
                            "stop_reason": s.stop_reason,
                            "is_backoff": s.is_backoff,
                        }
                        for s in step_results
                    ]

                async def run_step(cc: int, is_backoff: bool = False) -> StepResult:
                    nonlocal step_num, baseline_p95_latency, baseline_p99_latency
                    step_num += 1
                    step_label = f"Step {step_num}" + (
                        " (backoff)" if is_backoff else ""
                    )
                    controller_logger.info(
                        f"FIND_MAX: {step_label} - Testing {cc} workers for {step_dur}s..."
                    )

                    # Scale to current concurrency level
                    await _fmc_scale_to(target=cc, warmup=False)

                    # Clear latency buffer for fresh per-step measurement
                    async with self._metrics_lock:
                        self._latencies_ms.clear()
                        self._find_max_step_collecting = True
                        for k in self._find_max_step_lat_by_kind_ms:
                            self._find_max_step_lat_by_kind_ms[k].clear()
                            self._find_max_step_ops_by_kind[k] = 0
                            self._find_max_step_errors_by_kind[k] = 0

                    # Settling period when scaling down (backoff) to let system stabilize
                    if is_backoff:
                        settle_seconds = min(5, step_dur // 3)
                        controller_logger.info(
                            f"FIND_MAX: Settling for {settle_seconds}s after scale-down..."
                        )
                        await asyncio.sleep(settle_seconds)
                        # Clear again after settling to discard transition samples
                        async with self._metrics_lock:
                            self._latencies_ms.clear()
                            for k in self._find_max_step_lat_by_kind_ms:
                                self._find_max_step_lat_by_kind_ms[k].clear()
                                self._find_max_step_ops_by_kind[k] = 0
                                self._find_max_step_errors_by_kind[k] = 0

                    # Reset per-step metrics
                    step_start_ops = self.metrics.total_operations
                    step_start_errors = self.metrics.failed_operations
                    step_start_time = asyncio.get_running_loop().time()

                    # Publish controller timing/state so the dashboard can show a live countdown
                    # (WebSocket payloads are driven by the metrics loop; we include absolute times
                    # here so the UI can tick locally between payloads).
                    try:
                        step_started_at_epoch_ms: int | None = int(time.time() * 1000)
                    except Exception:
                        step_started_at_epoch_ms = None
                    step_end_at_epoch_ms: int | None = (
                        int(step_started_at_epoch_ms + (step_dur * 1000))
                        if step_started_at_epoch_ms is not None
                        else None
                    )
                    next_planned_concurrency = int(
                        min(max_cc, int(cc) + int(increment))
                    )
                    self._find_max_controller_state = {
                        "mode": "FIND_MAX_CONCURRENCY",
                        "status": "STEP_RUNNING",
                        "current_step": int(step_num),
                        "current_concurrency": int(cc),
                        "active_worker_count": int(_fmc_active_worker_count()),
                        "next_planned_concurrency": int(next_planned_concurrency),
                        "step_duration_seconds": int(step_dur),
                        "step_started_at_epoch_ms": step_started_at_epoch_ms,
                        "step_end_at_epoch_ms": step_end_at_epoch_ms,
                        "is_backoff": bool(is_backoff),
                        # Controller configuration (for UI display/debugging)
                        "start_concurrency": int(start_cc),
                        "concurrency_increment": int(increment),
                        "max_concurrency": int(max_cc),
                        "qps_stability_pct": float(qps_stability_pct),
                        "latency_stability_pct": float(latency_stability_pct),
                        "max_error_rate_pct": float(max_error_rate_pct),
                        # Rolling best + baseline (baseline established after step 1)
                        "best_concurrency": int(best_concurrency),
                        "best_qps": float(best_qps),
                        "baseline_p95_latency_ms": baseline_p95_latency,
                        "baseline_p99_latency_ms": baseline_p99_latency,
                        # Completed steps so far
                        "step_history": _build_step_history(),
                    }

                    # Run for step duration, collecting metrics
                    step_elapsed = 0.0
                    while step_elapsed < step_dur and not self._stop_event.is_set():
                        await asyncio.sleep(1.0)
                        step_elapsed = (
                            asyncio.get_running_loop().time() - step_start_time
                        )

                    # Capture latencies from the rolling buffer
                    async with self._metrics_lock:
                        # Stop collecting before snapshotting so late completions don't leak
                        # across the step boundary.
                        self._find_max_step_collecting = False
                        step_latencies = list(self._latencies_ms)
                        step_kind_latencies = {
                            k: list(v)
                            for k, v in self._find_max_step_lat_by_kind_ms.items()
                        }
                        step_kind_ops = dict(self._find_max_step_ops_by_kind)
                        step_kind_errors = dict(self._find_max_step_errors_by_kind)

                    # Calculate step metrics
                    step_end_ops = self.metrics.total_operations
                    step_end_errors = self.metrics.failed_operations
                    step_ops = step_end_ops - step_start_ops
                    step_errors = step_end_errors - step_start_errors
                    step_qps = step_ops / step_dur if step_dur > 0 else 0.0
                    step_error_rate = (
                        (step_errors / step_ops * 100.0) if step_ops > 0 else 0.0
                    )

                    # Calculate p95/p99 latency for this step
                    step_p95_latency = 0.0
                    step_p99_latency = 0.0
                    if step_latencies:
                        sorted_lat = sorted(step_latencies)
                        p95_idx = int(len(sorted_lat) * 0.95)
                        step_p95_latency = sorted_lat[min(p95_idx, len(sorted_lat) - 1)]
                        p99_idx = int(len(sorted_lat) * 0.99)
                        step_p99_latency = sorted_lat[min(p99_idx, len(sorted_lat) - 1)]

                    def _step_pct(values: list[float], pct: float) -> float | None:
                        if not values:
                            return None
                        sorted_vals = sorted(values)
                        idx = int(len(sorted_vals) * (pct / 100.0))
                        idx = min(max(idx, 0), len(sorted_vals) - 1)
                        return float(sorted_vals[idx])

                    step_kind_metrics: dict[str, dict[str, float | None]] = {}
                    for kind, values in step_kind_latencies.items():
                        ops = int(step_kind_ops.get(kind, 0) or 0)
                        errs = int(step_kind_errors.get(kind, 0) or 0)
                        p95 = _step_pct(values, 95.0)
                        p99 = _step_pct(values, 99.0)
                        err_rate = (errs / ops * 100.0) if ops > 0 else None
                        step_kind_metrics[kind] = {
                            "p95_latency_ms": p95,
                            "p99_latency_ms": p99,
                            "error_rate_pct": err_rate,
                        }

                    # Set baseline from first step
                    if baseline_p95_latency is None:
                        baseline_p95_latency = step_p95_latency
                    if baseline_p99_latency is None:
                        baseline_p99_latency = step_p99_latency

                    # Determine stability
                    stable = True
                    stop_reason = None

                    # Check error rate
                    if step_error_rate > max_error_rate_pct:
                        stable = False
                        stop_reason = (
                            f"Error rate {step_error_rate:.2f}% > {max_error_rate_pct}%"
                        )

                    # Check queue buildup (Snowflake only)
                    if stable and self._warehouse_query_status:
                        queued = int(self._warehouse_query_status.get("queued", 0) or 0)
                        blocked = int(
                            self._warehouse_query_status.get("blocked", 0) or 0
                        )
                        if queued > 0 or blocked > 0:
                            stable = False
                            stop_reason = (
                                f"Queue buildup: {queued} queued, {blocked} blocked"
                            )

                    # Hard SLO constraints (per query kind) from template config.
                    #
                    # If any enabled target (P95/P99/error%) fails for a non-zero-weight query kind,
                    # the step is considered unstable and FIND_MAX stops/searches accordingly.
                    if stable:
                        kind_labels = {
                            "POINT_LOOKUP": "Point Lookup",
                            "RANGE_SCAN": "Range Scan",
                            "INSERT": "Insert",
                            "UPDATE": "Update",
                        }
                        for kind in ["POINT_LOOKUP", "RANGE_SCAN", "INSERT", "UPDATE"]:
                            cfg = fmc_slo_by_kind.get(kind) or {}
                            weight = float(cfg.get("weight_pct", 0.0) or 0.0)
                            if not math.isfinite(weight) or weight <= 0:
                                continue

                            t95 = float(cfg.get("target_p95_ms", -1.0) or -1.0)
                            t99 = float(cfg.get("target_p99_ms", -1.0) or -1.0)
                            terr = float(cfg.get("target_err_pct", -1.0) or -1.0)
                            p95_enabled = math.isfinite(t95) and t95 > 0
                            p99_enabled = math.isfinite(t99) and t99 > 0
                            err_enabled = math.isfinite(terr) and terr >= 0

                            if not (p95_enabled or p99_enabled or err_enabled):
                                continue

                            ops = int(step_kind_ops.get(kind, 0) or 0)
                            errs = int(step_kind_errors.get(kind, 0) or 0)
                            label = kind_labels.get(kind, kind)

                            if ops <= 0:
                                stable = False
                                stop_reason = (
                                    f"{label}: no operations observed to evaluate SLOs "
                                    f"(step too short?)"
                                )
                                break

                            err_pct = (errs / ops * 100.0) if ops > 0 else 0.0
                            if err_enabled and err_pct > terr:
                                stable = False
                                stop_reason = (
                                    f"{label}: error rate {err_pct:.2f}% > {terr}%"
                                )
                                break

                            samples = step_kind_latencies.get(kind) or []
                            if p99_enabled:
                                o99 = _pctile(samples, 99.0)
                                if o99 is None:
                                    stable = False
                                    stop_reason = (
                                        f"{label}: no successful samples to evaluate P99 "
                                        f"(target {t99:.1f}ms)"
                                    )
                                    break
                                if o99 > t99:
                                    stable = False
                                    stop_reason = (
                                        f"{label}: P99 {o99:.1f}ms > target {t99:.1f}ms"
                                    )
                                    break

                            if p95_enabled:
                                o95 = _pctile(samples, 95.0)
                                if o95 is None:
                                    stable = False
                                    stop_reason = (
                                        f"{label}: no successful samples to evaluate P95 "
                                        f"(target {t95:.1f}ms)"
                                    )
                                    break
                                if o95 > t95:
                                    stable = False
                                    stop_reason = (
                                        f"{label}: P95 {o95:.1f}ms > target {t95:.1f}ms"
                                    )
                                    break

                    # Compare to previous step (if exists and not a backoff step comparing to failed step)
                    prev_stable_results = [
                        r for r in step_results if r.stable and not r.is_backoff
                    ]
                    if stable and prev_stable_results:
                        prev = prev_stable_results[-1]

                        # QPS should not decrease significantly vs previous stable step
                        if prev.qps > 0:
                            qps_change_pct = ((step_qps - prev.qps) / prev.qps) * 100.0
                            if qps_change_pct < -qps_stability_pct:
                                stable = False
                                stop_reason = (
                                    f"QPS dropped {-qps_change_pct:.1f}% vs previous "
                                    f"({prev.qps:.1f} → {step_qps:.1f}); "
                                    f"qps_stability_pct={qps_stability_pct:.1f}%"
                                )

                        # Latency should not increase too much vs the previous stable step.
                        #
                        # Guard against a false stop when the previous step was unusually fast
                        # (e.g. cache warmth), but the absolute latency is still under the
                        # baseline established at step 1.
                        if stable and prev.p95_latency_ms > 0 and step_p95_latency > 0:
                            ref_latency = float(prev.p95_latency_ms)
                            ref_label = "previous"
                            if (
                                baseline_p95_latency is not None
                                and math.isfinite(float(baseline_p95_latency))
                                and float(baseline_p95_latency) > ref_latency
                            ):
                                ref_latency = float(baseline_p95_latency)
                                ref_label = "baseline"

                            latency_change_pct = (
                                (float(step_p95_latency) - float(ref_latency))
                                / float(ref_latency)
                            ) * 100.0
                            if latency_change_pct > latency_stability_pct:
                                stable = False
                                if ref_label == "previous":
                                    stop_reason = (
                                        f"P95 latency increased {latency_change_pct:.1f}% "
                                        f"vs previous ({prev.p95_latency_ms:.1f}ms → {step_p95_latency:.1f}ms)"
                                    )
                                else:
                                    stop_reason = (
                                        f"P95 latency increased {latency_change_pct:.1f}% "
                                        f"vs baseline ({baseline_p95_latency:.1f}ms → {step_p95_latency:.1f}ms) "
                                        f"(previous stable was {prev.p95_latency_ms:.1f}ms)"
                                    )

                    # Also check against baseline (first step) - catch gradual drift
                    if (
                        stable
                        and baseline_p95_latency
                        and baseline_p95_latency > 0
                        and len(prev_stable_results) >= 2
                    ):
                        baseline_change_pct = (
                            (step_p95_latency - baseline_p95_latency)
                            / baseline_p95_latency
                        ) * 100.0
                        # Allow more drift from baseline (2x the per-step threshold)
                        if baseline_change_pct > latency_stability_pct * 2:
                            stable = False
                            stop_reason = f"P95 latency increased {baseline_change_pct:.1f}% vs baseline ({baseline_p95_latency:.1f}ms → {step_p95_latency:.1f}ms)"

                    result = StepResult(
                        step_num=step_num,
                        concurrency=cc,
                        qps=step_qps,
                        p95_latency_ms=step_p95_latency,
                        p99_latency_ms=step_p99_latency,
                        error_rate_pct=step_error_rate,
                        kind_metrics=step_kind_metrics,
                        stable=stable,
                        stop_reason=stop_reason,
                        is_backoff=is_backoff,
                    )

                    comparison_info = ""
                    if prev_stable_results:
                        prev = prev_stable_results[-1]
                        comparison_info = f" (vs step {prev.step_num}: {prev.qps:.1f} QPS, p95={prev.p95_latency_ms:.1f}ms)"
                    controller_logger.info(
                        f"FIND_MAX: {step_label} complete - {cc} workers: {step_qps:.1f} QPS, p95={step_p95_latency:.1f}ms, errors={step_error_rate:.2f}%, stable={stable}{comparison_info}"
                    )

                    return result

                while current_cc <= max_cc and not self._stop_event.is_set():
                    step_result = await run_step(current_cc)
                    step_results.append(step_result)

                    # Update best if stable
                    if step_result.stable and step_result.qps >= best_qps:
                        best_concurrency = current_cc
                        best_qps = step_result.qps

                    # Update controller state for WebSocket
                    step_started_at_epoch_ms = self._find_max_controller_state.get(
                        "step_started_at_epoch_ms"
                    )
                    step_end_at_epoch_ms = self._find_max_controller_state.get(
                        "step_end_at_epoch_ms"
                    )
                    next_planned_concurrency = (
                        int(min(max_cc, int(current_cc) + int(increment)))
                        if step_result.stable
                        else int(best_concurrency)
                    )
                    self._find_max_controller_state = {
                        "mode": "FIND_MAX_CONCURRENCY",
                        "status": "STEP_COMPLETE",
                        "current_step": step_num,
                        "current_concurrency": current_cc,
                        "active_worker_count": int(_fmc_active_worker_count()),
                        "current_qps": step_result.qps,
                        "current_p95_latency_ms": step_result.p95_latency_ms,
                        "current_p99_latency_ms": step_result.p99_latency_ms,
                        "current_error_rate_pct": step_result.error_rate_pct,
                        "stable": step_result.stable,
                        "stop_reason": step_result.stop_reason,
                        "step_duration_seconds": int(step_dur),
                        "step_started_at_epoch_ms": step_started_at_epoch_ms,
                        "step_end_at_epoch_ms": step_end_at_epoch_ms,
                        "next_planned_concurrency": int(next_planned_concurrency),
                        # Controller configuration (for UI display/debugging)
                        "start_concurrency": int(start_cc),
                        "concurrency_increment": int(increment),
                        "max_concurrency": int(max_cc),
                        "qps_stability_pct": float(qps_stability_pct),
                        "latency_stability_pct": float(latency_stability_pct),
                        "max_error_rate_pct": float(max_error_rate_pct),
                        "best_concurrency": best_concurrency,
                        "best_qps": best_qps,
                        "baseline_p95_latency_ms": baseline_p95_latency,
                        "baseline_p99_latency_ms": baseline_p99_latency,
                        "step_history": _build_step_history(),
                    }

                    if not step_result.stable:
                        # Record termination reason from the failing step. We may still run
                        # backoff/midpoint probes for best-concurrency verification, but the
                        # overall run should report why it initially stopped.
                        if termination_reason is None and step_result.stop_reason:
                            termination_reason = (
                                str(step_result.stop_reason).strip() or None
                            )

                        recovered = False
                        # Try backoff: go back to best_concurrency and verify it's still stable
                        if (
                            backoff_attempts < max_backoff_attempts
                            and best_concurrency < current_cc
                        ):
                            backoff_attempts += 1
                            controller_logger.info(
                                f"FIND_MAX: Degradation detected at {current_cc}. Backing off to verify {best_concurrency} is stable..."
                            )

                            backoff_result = await run_step(
                                best_concurrency, is_backoff=True
                            )
                            step_results.append(backoff_result)

                            if backoff_result.stable:
                                controller_logger.info(
                                    f"FIND_MAX: Backoff confirmed - {best_concurrency} workers is stable @ {backoff_result.qps:.1f} QPS"
                                )
                                # Try a midpoint between best and failed
                                midpoint = (
                                    best_concurrency
                                    + (current_cc - best_concurrency) // 2
                                )
                                if (
                                    midpoint > best_concurrency
                                    and midpoint < current_cc
                                ):
                                    controller_logger.info(
                                        f"FIND_MAX: Trying midpoint {midpoint} workers..."
                                    )
                                    mid_result = await run_step(midpoint)
                                    step_results.append(mid_result)
                                    if mid_result.stable and mid_result.qps >= best_qps:
                                        if mid_result.qps > best_qps:
                                            best_concurrency = midpoint
                                            best_qps = mid_result.qps
                                            controller_logger.info(
                                                f"FIND_MAX: Midpoint {midpoint} is better! New best: {best_qps:.1f} QPS"
                                            )
                                        recovered = True
                                        next_cc = min(
                                            max_cc, int(midpoint) + int(increment)
                                        )
                                        controller_logger.info(
                                            "FIND_MAX: Recovery observed at %d workers; continuing search (backoff %d/%d) at %d workers",
                                            midpoint,
                                            backoff_attempts,
                                            max_backoff_attempts,
                                            next_cc,
                                        )
                                        current_cc = next_cc
                            else:
                                controller_logger.info(
                                    f"FIND_MAX: Backoff to {best_concurrency} also unstable - stopping"
                                )

                        if recovered:
                            continue

                        controller_logger.info(
                            f"FIND_MAX: Stopping - {step_result.stop_reason}"
                        )
                        break

                    # Increase concurrency for next step
                    current_cc += increment

                if self._find_max_controller_state:
                    self._find_max_controller_state["step_history"] = (
                        _build_step_history()
                    )

                # Final result
                if termination_reason:
                    final_reason = termination_reason
                elif step_results and (not step_results[-1].stable):
                    final_reason = (
                        str(step_results[-1].stop_reason or "").strip()
                        or "Degradation detected"
                    )
                else:
                    final_reason = "Reached max workers"
                controller_logger.info(
                    f"✅ FIND_MAX complete: Best concurrency = {best_concurrency} workers @ {best_qps:.1f} QPS"
                )
                self._find_max_controller_state["completed"] = True
                self._find_max_controller_state["status"] = "COMPLETED"
                self._find_max_controller_state["final_best_concurrency"] = (
                    best_concurrency
                )
                self._find_max_controller_state["final_best_qps"] = best_qps
                self._find_max_controller_state["final_reason"] = final_reason
                self._find_max_controller_state["baseline_p95_latency_ms"] = (
                    baseline_p95_latency
                )
                self._find_max_controller_state["baseline_p99_latency_ms"] = (
                    baseline_p99_latency
                )

                # Stop all workers
                controller_logger.info("Stopping workers...")
                self._stop_event.set()
                await _fmc_stop_all_workers(timeout_seconds=2.0)

            else:
                # Warmup period
                if self.scenario.warmup_seconds > 0:
                    logger.info(f"Warming up for {self.scenario.warmup_seconds}s...")
                    await self._warmup()

                # Transition QUERY_TAG from :phase=WARMUP to :phase=RUNNING before resetting
                # metrics so SF QUERY_HISTORY can distinguish warmup vs measurement queries.
                await self._transition_to_measurement_phase()

                # Reset metrics after warmup
                async with self._metrics_lock:
                    self._metrics_epoch += 1
                    self._measurement_active = True
                    self.metrics = Metrics()
                    self.metrics.timestamp = datetime.now(UTC)
                    # From this point forward we consider the measurement window started.
                    self._measurement_start_time = datetime.now(UTC)
                    # Reset snapshot state so current QPS doesn't get skewed by warmup counters.
                    self._last_snapshot_time = None
                    self._last_snapshot_mono = None
                    self._last_snapshot_ops = 0
                    self._qps_smoothed = None
                    self._qps_windowed = None
                    self._qps_samples.clear()
                    self._qps_controller_state = {}
                    self._latencies_ms.clear()
                    self._sql_error_categories.clear()
                    self._latency_sf_execution_ms.clear()
                    self._latency_network_overhead_ms.clear()

                # Spawn workers
                self._target_workers = int(
                    self.scenario.total_threads
                )  # Track target for metrics
                logger.info(
                    "Spawning %d worker tasks...",
                    int(self.scenario.total_threads),
                )
                self.workers = [
                    asyncio.create_task(self._worker(worker_id))
                    for worker_id in range(self.scenario.total_threads)
                ]

                # Run for duration
                await asyncio.sleep(self.scenario.duration_seconds)

                # Stop workers
                logger.info("Stopping workers...")
                self._stop_event.set()

                # Wait for workers to finish
                await asyncio.gather(*self.workers, return_exceptions=True)

            # Stop metrics collector
            metrics_task.cancel()
            try:
                await metrics_task
            except asyncio.CancelledError:
                pass

            # Finalize
            self.end_time = datetime.now(UTC)
            self.status = TestStatus.COMPLETED

            # Build result
            self.test_result = await self._build_result()

            logger.info(
                f"✅ Test complete: {self.test_result.total_operations} queries, {self.test_result.qps:.2f} QPS"
            )
            logger.info(
                f"📊 Query distribution - Reads: {self._point_lookup_count} point lookups, {self._range_scan_count} range scans | Writes: {self._insert_count} inserts, {self._update_count} updates"
            )

            return self.test_result

        except asyncio.CancelledError:
            # Ensure worker tasks don't leak on cancellation (important for dev reload/shutdown).
            #
            # IMPORTANT: Cancel metrics_task FIRST to stop streaming immediately.
            # Workers may be blocked on I/O that doesn't respond quickly to cancellation,
            # and waiting for them would leave the metrics loop running indefinitely.
            if metrics_task is not None:
                metrics_task.cancel()
                try:
                    await metrics_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass

            # Ensure FIND_MAX_CONCURRENCY produces a human-readable conclusion reason
            # even when the run is cancelled by the user.
            try:
                load_mode = str(getattr(self.scenario, "load_mode", "") or "").upper()
                if load_mode == "FIND_MAX_CONCURRENCY":
                    if not isinstance(self._find_max_controller_state, dict):
                        self._find_max_controller_state = {}
                    self._find_max_controller_state.setdefault(
                        "mode", "FIND_MAX_CONCURRENCY"
                    )
                    self._find_max_controller_state["completed"] = True
                    self._find_max_controller_state["status"] = "CANCELLED"
                    self._find_max_controller_state.setdefault(
                        "final_reason", "Cancelled by user"
                    )
            except Exception:
                pass

            if qps_controller_task is not None:
                try:
                    qps_controller_task.cancel()
                except Exception:
                    pass

            self._stop_event.set()
            for t in self.workers:
                try:
                    t.cancel()
                except Exception:
                    pass

            # Wait for workers with a timeout to avoid hanging indefinitely
            # if workers are blocked on uninterruptible I/O operations.
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self.workers, return_exceptions=True), timeout=5.0
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "Timed out waiting for workers to stop (some may still be running)"
                )
            raise

        except Exception as e:
            self._setup_error = f"Error during test execution: {e}"
            logger.error(self._setup_error)
            self.status = TestStatus.FAILED
            self.end_time = datetime.now(UTC)

            # Stop metrics collector immediately to stop WebSocket streaming
            if metrics_task is not None:
                metrics_task.cancel()
                try:
                    await metrics_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass

            # Build partial result
            self.test_result = await self._build_result()
            return self.test_result

        finally:
            # Ensure the QPS controller is not left running if we exit early.
            if qps_controller_task is not None and not qps_controller_task.done():
                try:
                    qps_controller_task.cancel()
                except Exception:
                    pass
            # Ensure metrics task is cancelled (belt and suspenders for edge cases)
            if metrics_task is not None and not metrics_task.done():
                try:
                    metrics_task.cancel()
                except Exception:
                    pass

    async def teardown(self) -> bool:
        """
        Teardown test environment.

        - Drop tables
        - Clean up resources

        Returns:
            bool: True if teardown successful
        """
        try:
            logger.info(f"Tearing down test: {self.scenario.name}")

            # Teardown tables in parallel
            teardown_tasks = [manager.teardown() for manager in self.table_managers]
            await asyncio.gather(*teardown_tasks, return_exceptions=True)

            logger.info("✅ Test teardown complete")
            return True

        except Exception as e:
            logger.error(f"Error during test teardown: {e}")
            return False

    async def _warmup(self):
        """Execute warmup period."""
        warmup_workers = [
            asyncio.create_task(self._worker(i, warmup=True))
            for i in range(min(5, self.scenario.total_threads))
        ]
        try:
            await asyncio.sleep(self.scenario.warmup_seconds)
        finally:
            # Always stop warmup workers, even if the task is cancelled.
            self._stop_event.set()
            for t in warmup_workers:
                try:
                    t.cancel()
                except Exception:
                    pass
            await asyncio.gather(*warmup_workers, return_exceptions=True)
            self._stop_event.clear()

    async def _transition_to_measurement_phase(self) -> None:
        """
        Transition from warmup to measurement phase.

        Updates the Snowflake QUERY_TAG from :phase=WARMUP to :phase=RUNNING on all
        pool connections so Snowflake's QUERY_HISTORY can distinguish warmup queries
        from measurement queries.
        """
        running_tag = getattr(self, "_benchmark_query_tag_running", None)
        if not running_tag:
            return

        # Update the executor's current tag (used for SF sampler filtering).
        self._benchmark_query_tag = str(running_tag)

        # Update QUERY_TAG on all pool connections.
        pool = getattr(self, "_snowflake_pool_override", None)
        if pool is None:
            logger.warning(
                "No pool available for QUERY_TAG update during phase transition"
            )
            return
        if not hasattr(pool, "update_query_tag"):
            logger.warning(
                "Pool does not support update_query_tag method; QUERY_TAG not updated"
            )
            return
        try:
            updated = await pool.update_query_tag(str(running_tag))
            if updated == 0:
                logger.warning(
                    "QUERY_TAG update returned 0 connections; pool may have no active connections"
                )
            else:
                logger.info(
                    "Transitioned to measurement phase: QUERY_TAG updated on %d connections",
                    int(updated),
                )
        except Exception as e:
            logger.warning("Failed to update QUERY_TAG on pool connections: %s", str(e))

    async def _controlled_worker(
        self,
        *,
        worker_id: int,
        warmup: bool,
        stop_signal: asyncio.Event,
    ) -> None:
        """
        Worker loop with a per-worker stop signal (used for QPS mode scale-down).

        Unlike `_worker`, this never applies per-worker rate limiting. QPS mode
        controls offered load via the number of active workers.
        """
        operations_executed = 0
        target_ops = self.scenario.operations_per_connection
        effective_warmup = bool(warmup)

        try:
            while not self._stop_event.is_set():
                if stop_signal.is_set():
                    break

                # Check if we've hit operation limit
                if target_ops and operations_executed >= target_ops:
                    break

                # QPS workers spawned during warmup should automatically start contributing
                # to measurement once the measurement window begins.
                effective_warmup = bool(warmup) and not bool(self._measurement_active)
                await self._execute_operation(worker_id, effective_warmup)
                operations_executed += 1

                # If we're asked to stop, exit after completing the operation (no think-time delay).
                if stop_signal.is_set():
                    break

                # Think time
                if self.scenario.think_time_ms > 0:
                    await asyncio.sleep(self.scenario.think_time_ms / 1000.0)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            if not effective_warmup:
                logger.error("Worker %s error: %s", worker_id, e)
                async with self._metrics_lock:
                    self.metrics.failed_operations += 1

    async def _worker(self, worker_id: int, warmup: bool = False):
        """
        Worker task that executes operations.

        Args:
            worker_id: Worker identifier
            warmup: If True, this is a warmup run
        """
        operations_executed = 0
        target_ops = self.scenario.operations_per_connection

        try:
            while not self._stop_event.is_set():
                # Check if we've hit operation limit
                if target_ops and operations_executed >= target_ops:
                    break

                # Execute operation based on workload type
                await self._execute_operation(worker_id, warmup)
                operations_executed += 1

                # Think time
                if self.scenario.think_time_ms > 0:
                    await asyncio.sleep(self.scenario.think_time_ms / 1000.0)

                # Rate limiting
                if self.scenario.target_qps:
                    # Simple rate limiting (can be improved)
                    await asyncio.sleep(1.0 / self.scenario.target_qps)

        except asyncio.CancelledError:
            pass
        except Exception as e:
            if not warmup:
                logger.error(f"Worker {worker_id} error: {e}")
                async with self._metrics_lock:
                    self.metrics.failed_operations += 1

    async def _execute_operation(self, worker_id: int, warmup: bool = False):
        """
        Execute a single operation based on workload type.

        Args:
            worker_id: Worker identifier
            warmup: If True, don't record metrics
        """
        workload = self.scenario.workload_type

        # Determine operation type
        if workload == WorkloadType.READ_ONLY:
            await self._execute_read(worker_id, warmup)
        elif workload == WorkloadType.WRITE_ONLY:
            await self._execute_write(worker_id, warmup)
        elif workload == WorkloadType.READ_HEAVY:
            # 80% reads, 20% writes
            import random

            if random.random() < 0.8:
                await self._execute_read(worker_id, warmup)
            else:
                await self._execute_write(worker_id, warmup)
        elif workload == WorkloadType.WRITE_HEAVY:
            # 20% reads, 80% writes
            import random

            if random.random() < 0.2:
                await self._execute_read(worker_id, warmup)
            else:
                await self._execute_write(worker_id, warmup)
        elif workload == WorkloadType.MIXED:
            # 50/50 reads/writes
            import random

            if random.random() < 0.5:
                await self._execute_read(worker_id, warmup)
            else:
                await self._execute_write(worker_id, warmup)
        elif workload == WorkloadType.CUSTOM:
            await self._execute_custom(worker_id, warmup)

    async def _execute_read(self, worker_id: int, warmup: bool = False):
        """Execute read operation."""
        start_wall = datetime.now(UTC)
        start_perf = time.perf_counter()
        epoch_at_start = int(self._metrics_epoch)

        query: str = ""
        params: Optional[list] = None
        query_kind = "RANGE_SCAN"
        full_name: str | None = None
        pool = None

        try:
            # Select random table
            import random

            manager = random.choice(self.table_managers)
            full_name = manager.get_full_table_name()
            pool = getattr(manager, "pool", None)

            batch_size = self.scenario.read_batch_size
            self._table_state.setdefault(full_name, _TableRuntimeState())
            state = self._table_state[full_name]
            profile = state.profile

            # Choose read shape:
            # - Point lookup (by ID) when possible
            # - Otherwise range scan (time-based if possible, else id-based)
            point_lookup_ratio = (
                self.scenario.point_lookup_ratio
                if hasattr(self.scenario, "point_lookup_ratio")
                else 0.5
            )
            do_point_lookup = random.random() < point_lookup_ratio
            used_point_lookup = False

            if (
                profile
                and profile.id_column
                and do_point_lookup
                and (
                    bool(self._pool_values("KEY", profile.id_column))
                    or (
                        profile.id_min is not None
                        and profile.id_max is not None
                        and profile.id_max >= profile.id_min
                    )
                )
            ):
                pooled = self._next_from_pool(worker_id, "KEY", profile.id_column)
                if pooled is None:
                    row_id = random.randint(profile.id_min, profile.id_max)  # type: ignore[arg-type]
                else:
                    row_id = pooled
                select_list = self._select_list_sql()
                id_col_quoted = self._quote_column(profile.id_column, pool)
                query = (
                    f"SELECT {select_list} FROM {full_name} WHERE {id_col_quoted} = ?"
                )
                params = [row_id]
                used_point_lookup = True
                query_kind = "POINT_LOOKUP"
            else:
                query, params = self._build_range_scan(
                    full_name, profile, batch_size, worker_id=worker_id, pool=pool
                )

            # Execute query via pool
            sf_execution_ms: Optional[float] = None
            if pool is not None:
                if hasattr(pool, "execute_query_with_info"):
                    # Snowflake
                    query = self._annotate_query_for_sf_kind(query, query_kind)
                    result, info = await pool.execute_query_with_info(
                        query, params=params, fetch=True
                    )
                    sf_query_id = str(info.get("query_id") or "")
                    sf_rowcount = info.get("rowcount")
                    sf_execution_ms = info.get("total_elapsed_time_ms")
                else:
                    # Postgres
                    result = await pool.fetch_all(query)
                    sf_query_id = f"LOCAL_{uuid4()}"
                    sf_rowcount = None

                end_wall = datetime.now(UTC)
                app_elapsed_ms = (time.perf_counter() - start_perf) * 1000.0

                # Update metrics
                duration_ms = app_elapsed_ms

                # Always update the real-time counters used by the websocket + METRICS_SNAPSHOTS,
                # including during warmup. We still reset metrics after warmup so the final
                # summary remains measurement-window only.
                counted = False
                async with self._metrics_lock:
                    if int(self._metrics_epoch) == int(epoch_at_start):
                        counted = True
                        self.metrics.total_operations += 1
                        self.metrics.successful_operations += 1
                        self.metrics.read_metrics.count += 1
                        self.metrics.read_metrics.success_count += 1
                        self.metrics.read_metrics.total_duration_ms += duration_ms
                        self.metrics.rows_read += len(result)
                        self._latencies_ms.append(duration_ms)
                        if (
                            self._find_max_step_collecting
                            and query_kind in self._find_max_step_ops_by_kind
                        ):
                            self._find_max_step_ops_by_kind[query_kind] = (
                                int(self._find_max_step_ops_by_kind.get(query_kind, 0))
                                + 1
                            )
                            self._find_max_step_lat_by_kind_ms[query_kind].append(
                                duration_ms
                            )
                        if sf_execution_ms is not None and sf_execution_ms >= 0:
                            self._latency_sf_execution_ms.append(sf_execution_ms)
                            network_overhead = max(
                                0.0, app_elapsed_ms - sf_execution_ms
                            )
                            self._latency_network_overhead_ms.append(network_overhead)

                # Summary-only breakdowns exclude warmup.
                if counted and not warmup:
                    if used_point_lookup:
                        self._point_lookup_count += 1
                    else:
                        self._range_scan_count += 1

                    self._lat_read_ms.append(duration_ms)
                    self._lat_by_kind_ms[query_kind].append(duration_ms)

                # Capture per-operation record if enabled for this scenario.
                #
                # Even when full query history capture is disabled, we still capture warmup
                # operations so they can be persisted for troubleshooting / visibility.
                if warmup or getattr(self.scenario, "collect_query_history", False):
                    pool_obj = getattr(manager, "pool", None)
                    pool_warehouse = (
                        str(getattr(pool_obj, "warehouse", "")).strip() or None
                    )
                    network_overhead_ms = None
                    if sf_execution_ms is not None and sf_execution_ms >= 0:
                        network_overhead_ms = max(0.0, app_elapsed_ms - sf_execution_ms)
                    self._query_execution_records.append(
                        _QueryExecutionRecord(
                            execution_id=str(uuid4()),
                            test_id=str(self.test_id),
                            query_id=sf_query_id or f"LOCAL_{uuid4()}",
                            query_text=query,
                            start_time=start_wall,
                            end_time=end_wall,
                            duration_ms=app_elapsed_ms,
                            success=True,
                            error=None,
                            warehouse=pool_warehouse,
                            rows_affected=int(sf_rowcount)
                            if sf_rowcount is not None
                            else len(result),
                            bytes_scanned=None,
                            connection_id=None,
                            custom_metadata={
                                "rows_returned": len(result),
                                "network_overhead_ms": network_overhead_ms,
                            },
                            query_kind=query_kind,
                            worker_id=worker_id,
                            warmup=warmup,
                            app_elapsed_ms=app_elapsed_ms,
                            sf_execution_ms=sf_execution_ms,
                        )
                    )

        except Exception as e:
            end_wall = datetime.now(UTC)
            app_elapsed_ms = (time.perf_counter() - start_perf) * 1000.0
            # Per-operation SQL failures are expected under load (locks/timeouts, etc).
            # Do not log each failure individually; rely on aggregated metrics/logging.

            # Track failures during warmup too (metrics are reset after warmup).
            category = self._classify_sql_error(e)
            counted = False
            sample_n: int | None = None
            async with self._metrics_lock:
                if int(self._metrics_epoch) == int(epoch_at_start):
                    counted = True
                    self.metrics.total_operations += 1
                    self.metrics.failed_operations += 1
                    self.metrics.read_metrics.count += 1
                    self.metrics.read_metrics.error_count += 1
                    if (
                        self._find_max_step_collecting
                        and query_kind in self._find_max_step_ops_by_kind
                    ):
                        self._find_max_step_ops_by_kind[query_kind] = (
                            int(self._find_max_step_ops_by_kind.get(query_kind, 0)) + 1
                        )
                        self._find_max_step_errors_by_kind[query_kind] = (
                            int(self._find_max_step_errors_by_kind.get(query_kind, 0))
                            + 1
                        )
                    self._sql_error_categories[category] = (
                        int(self._sql_error_categories.get(category, 0)) + 1
                    )
                    sample_n = int(self._sql_error_sample_counts.get(category, 0)) + 1
                    self._sql_error_sample_counts[category] = int(sample_n)

            # Sampled detailed log (first 10 per category per run) for debugging.
            if counted and sample_n is not None and int(sample_n) <= 10:
                backend = "unknown"
                try:
                    if pool is not None and self._is_postgres_pool(pool):
                        backend = "postgres"
                    elif pool is not None and hasattr(pool, "warehouse"):
                        backend = "snowflake"
                except Exception:
                    backend = "unknown"

                payload = {
                    "category": category,
                    "sample_n": int(sample_n),
                    "sample_max": 10,
                    "backend": backend,
                    "query_kind": str(query_kind or ""),
                    "table": str(full_name or ""),
                    "worker_id": int(worker_id),
                    "warmup": bool(warmup),
                    "elapsed_ms": round(float(app_elapsed_ms), 2),
                    "exception": {
                        "type": type(e).__name__,
                        "message": self._truncate_str_for_log(e, max_chars=800),
                        **self._sql_error_meta_for_log(e),
                    },
                    "query": self._preview_query_for_log(query, max_chars=2000),
                    "params": self._preview_params_for_log(
                        cast(Optional[list[Any]], params),
                        max_items=10,
                        max_value_chars=200,
                    ),
                }
                logger.warning(
                    "SQL_ERROR_SAMPLE %s",
                    json.dumps(payload, ensure_ascii=False),
                )

            if warmup or getattr(self.scenario, "collect_query_history", False):
                self._query_execution_records.append(
                    _QueryExecutionRecord(
                        execution_id=str(uuid4()),
                        test_id=str(self.test_id),
                        query_id=f"LOCAL_{uuid4()}",
                        query_text=query or "READ_FAILED",
                        start_time=start_wall,
                        end_time=end_wall,
                        duration_ms=app_elapsed_ms,
                        success=False,
                        error=str(e),
                        warehouse=None,
                        rows_affected=None,
                        bytes_scanned=None,
                        connection_id=None,
                        custom_metadata={"params_count": len(params or [])},
                        query_kind=query_kind,
                        worker_id=worker_id,
                        warmup=warmup,
                        app_elapsed_ms=app_elapsed_ms,
                    )
                )

    def _build_range_scan(
        self,
        full_name: str,
        profile: Optional[TableProfile],
        batch_size: int,
        range_width: int = 100,
        *,
        worker_id: int = 0,
        pool=None,
    ) -> tuple[str, Optional[list]]:
        import random

        if profile and profile.time_column and profile.time_max is not None:
            # Prefer a persisted pool of cutoffs (reduces identical range scans under concurrency).
            pooled = self._next_from_pool(worker_id, "RANGE", profile.time_column)
            if pooled is not None:
                cutoff = pooled
            elif profile.time_min is not None and profile.time_min < profile.time_max:
                # Pick a cutoff inside [min,max] so it should match some data.
                #
                # Randomize the choice (vs a constant midpoint) to avoid accidentally
                # generating identical range scans when RANGE pools are absent.
                span = profile.time_max - profile.time_min
                cutoff = profile.time_min + (span * random.random())
            else:
                cutoff = profile.time_max - timedelta(days=7)

            select_list = self._select_list_sql()
            time_col_quoted = self._quote_column(profile.time_column, pool)
            query = (
                f"SELECT {select_list} FROM {full_name} WHERE {time_col_quoted} >= ? "
                f"ORDER BY {time_col_quoted} DESC LIMIT {batch_size}"
            )
            return query, [cutoff]

        if (
            profile
            and profile.id_column
            and profile.id_min is not None
            and profile.id_max is not None
        ):
            start_id = profile.id_min
            if profile.id_max > profile.id_min + range_width:
                start_id = random.randint(profile.id_min, profile.id_max - range_width)
            end_id = start_id + range_width

            select_list = self._select_list_sql()
            id_col_quoted = self._quote_column(profile.id_column, pool)
            query = (
                f"SELECT {select_list} FROM {full_name} WHERE {id_col_quoted} BETWEEN ? AND ? "
                f"ORDER BY {id_col_quoted} LIMIT {batch_size}"
            )
            return query, [start_id, end_id]

        select_list = self._select_list_sql()
        return f"SELECT {select_list} FROM {full_name} LIMIT {batch_size}", None

    async def _execute_write(self, worker_id: int, warmup: bool = False):
        """Execute write operation."""
        start_wall = datetime.now(UTC)
        start_perf = time.perf_counter()
        epoch_at_start = int(self._metrics_epoch)

        query: str = ""
        params: Optional[list] = None
        query_kind = "INSERT"
        rows_written = 0
        full_name: str | None = None
        pool = None

        try:
            # Select random table
            import random

            manager = random.choice(self.table_managers)
            full_name = manager.get_full_table_name()
            pool = getattr(manager, "pool", None)
            self._table_state.setdefault(full_name, _TableRuntimeState())
            state = self._table_state[full_name]
            profile = state.profile

            # Decide between insert/update. If update_ratio isn't set, default to 30% updates.
            update_ratio = (
                self.scenario.update_ratio if self.scenario.update_ratio > 0 else 0.3
            )
            do_update = (
                profile is not None
                and profile.id_column is not None
                and (
                    bool(self._pool_values("KEY", profile.id_column))
                    or (
                        profile.id_min is not None
                        and profile.id_max is not None
                        and profile.id_max >= profile.id_min
                    )
                )
                and random.random() < update_ratio
            )

            if do_update:
                # `do_update` guarantees profile is present and has id bounds.
                if profile is None:
                    raise ValueError(
                        "Internal error: do_update=True but profile is None"
                    )
                if full_name is None:
                    raise ValueError("Internal error: missing table name for UPDATE")
                query, params = self._build_update(
                    full_name, manager, profile, pool=pool
                )
                rows_written = 1
                query_kind = "UPDATE"
                if not warmup:
                    self._update_count += 1
            else:
                query, params = self._build_insert(full_name, manager, state, pool=pool)
                rows_written = self.scenario.write_batch_size
                query_kind = "INSERT"
                if not warmup:
                    self._insert_count += 1

            # Execute write
            sf_execution_ms: Optional[float] = None
            if pool is not None:
                if hasattr(pool, "execute_query_with_info"):
                    # Snowflake
                    query = self._annotate_query_for_sf_kind(query, query_kind)
                    _, info = await pool.execute_query_with_info(
                        query, params=params, fetch=False
                    )
                    sf_query_id = str(info.get("query_id") or "")
                    sf_rowcount = info.get("rowcount")
                    sf_execution_ms = info.get("total_elapsed_time_ms")
                else:
                    # Postgres
                    await pool.execute_query(query)
                    sf_query_id = f"LOCAL_{uuid4()}"
                    sf_rowcount = None

                end_wall = datetime.now(UTC)
                app_elapsed_ms = (time.perf_counter() - start_perf) * 1000.0

                # Update metrics
                duration_ms = app_elapsed_ms

                # Always update the real-time counters used by the websocket + METRICS_SNAPSHOTS,
                # including during warmup. We still reset metrics after warmup so the final
                # summary remains measurement-window only.
                counted = False
                async with self._metrics_lock:
                    if int(self._metrics_epoch) == int(epoch_at_start):
                        counted = True
                        self.metrics.total_operations += 1
                        self.metrics.successful_operations += 1
                        self.metrics.write_metrics.count += 1
                        self.metrics.write_metrics.success_count += 1
                        self.metrics.write_metrics.total_duration_ms += duration_ms
                        self.metrics.rows_written += rows_written
                        self._latencies_ms.append(duration_ms)
                        if (
                            self._find_max_step_collecting
                            and query_kind in self._find_max_step_ops_by_kind
                        ):
                            self._find_max_step_ops_by_kind[query_kind] = (
                                int(self._find_max_step_ops_by_kind.get(query_kind, 0))
                                + 1
                            )
                            self._find_max_step_lat_by_kind_ms[query_kind].append(
                                duration_ms
                            )
                        if sf_execution_ms is not None and sf_execution_ms >= 0:
                            self._latency_sf_execution_ms.append(sf_execution_ms)
                            network_overhead = max(
                                0.0, app_elapsed_ms - sf_execution_ms
                            )
                            self._latency_network_overhead_ms.append(network_overhead)

                # Summary-only breakdowns exclude warmup.
                if counted and not warmup:
                    self._lat_write_ms.append(duration_ms)
                    self._lat_by_kind_ms[query_kind].append(duration_ms)

                if warmup or getattr(self.scenario, "collect_query_history", False):
                    pool_obj = getattr(manager, "pool", None)
                    pool_warehouse = (
                        str(getattr(pool_obj, "warehouse", "")).strip() or None
                    )
                    network_overhead_ms = None
                    if sf_execution_ms is not None and sf_execution_ms >= 0:
                        network_overhead_ms = max(0.0, app_elapsed_ms - sf_execution_ms)
                    self._query_execution_records.append(
                        _QueryExecutionRecord(
                            execution_id=str(uuid4()),
                            test_id=str(self.test_id),
                            query_id=sf_query_id or f"LOCAL_{uuid4()}",
                            query_text=query,
                            start_time=start_wall,
                            end_time=end_wall,
                            duration_ms=app_elapsed_ms,
                            success=True,
                            error=None,
                            warehouse=pool_warehouse,
                            rows_affected=int(sf_rowcount)
                            if sf_rowcount is not None
                            else rows_written,
                            bytes_scanned=None,
                            connection_id=None,
                            custom_metadata={
                                "rows_written": rows_written,
                                "network_overhead_ms": network_overhead_ms,
                            },
                            query_kind=query_kind,
                            worker_id=worker_id,
                            warmup=warmup,
                            app_elapsed_ms=app_elapsed_ms,
                            sf_execution_ms=sf_execution_ms,
                        )
                    )

        except Exception as e:
            end_wall = datetime.now(UTC)
            app_elapsed_ms = (time.perf_counter() - start_perf) * 1000.0
            # Per-operation SQL failures are expected under load (locks/timeouts, etc).
            # Do not log each failure individually; rely on aggregated metrics/logging.

            # Track failures during warmup too (metrics are reset after warmup).
            category = self._classify_sql_error(e)
            counted = False
            sample_n: int | None = None
            async with self._metrics_lock:
                if int(self._metrics_epoch) == int(epoch_at_start):
                    counted = True
                    self.metrics.total_operations += 1
                    self.metrics.failed_operations += 1
                    self.metrics.write_metrics.count += 1
                    self.metrics.write_metrics.error_count += 1
                    if (
                        self._find_max_step_collecting
                        and query_kind in self._find_max_step_ops_by_kind
                    ):
                        self._find_max_step_ops_by_kind[query_kind] = (
                            int(self._find_max_step_ops_by_kind.get(query_kind, 0)) + 1
                        )
                        self._find_max_step_errors_by_kind[query_kind] = (
                            int(self._find_max_step_errors_by_kind.get(query_kind, 0))
                            + 1
                        )
                    self._sql_error_categories[category] = (
                        int(self._sql_error_categories.get(category, 0)) + 1
                    )
                    sample_n = int(self._sql_error_sample_counts.get(category, 0)) + 1
                    self._sql_error_sample_counts[category] = int(sample_n)

            if counted and sample_n is not None and int(sample_n) <= 10:
                backend = "unknown"
                try:
                    if pool is not None and self._is_postgres_pool(pool):
                        backend = "postgres"
                    elif pool is not None and hasattr(pool, "warehouse"):
                        backend = "snowflake"
                except Exception:
                    backend = "unknown"

                payload = {
                    "category": category,
                    "sample_n": int(sample_n),
                    "sample_max": 10,
                    "backend": backend,
                    "query_kind": str(query_kind or ""),
                    "table": str(full_name or ""),
                    "worker_id": int(worker_id),
                    "warmup": bool(warmup),
                    "elapsed_ms": round(float(app_elapsed_ms), 2),
                    "exception": {
                        "type": type(e).__name__,
                        "message": self._truncate_str_for_log(e, max_chars=800),
                        **self._sql_error_meta_for_log(e),
                    },
                    "query": self._preview_query_for_log(query, max_chars=2000),
                    "params": self._preview_params_for_log(
                        cast(Optional[list[Any]], params),
                        max_items=10,
                        max_value_chars=200,
                    ),
                }
                logger.warning(
                    "SQL_ERROR_SAMPLE %s",
                    json.dumps(payload, ensure_ascii=False),
                )

            if warmup or getattr(self.scenario, "collect_query_history", False):
                self._query_execution_records.append(
                    _QueryExecutionRecord(
                        execution_id=str(uuid4()),
                        test_id=str(self.test_id),
                        query_id=f"LOCAL_{uuid4()}",
                        query_text=query or "WRITE_FAILED",
                        start_time=start_wall,
                        end_time=end_wall,
                        duration_ms=app_elapsed_ms,
                        success=False,
                        error=str(e),
                        warehouse=None,
                        rows_affected=None,
                        bytes_scanned=None,
                        connection_id=None,
                        custom_metadata={"params_count": len(params or [])},
                        query_kind=query_kind,
                        worker_id=worker_id,
                        warmup=warmup,
                        app_elapsed_ms=app_elapsed_ms,
                    )
                )

    def get_query_execution_records(self) -> list[_QueryExecutionRecord]:
        """Get captured per-operation records (including warmup if enabled)."""
        return list(self._query_execution_records)

    def _build_insert(
        self,
        full_name: str,
        manager: TableManager,
        state: _TableRuntimeState,
        pool=None,
    ) -> tuple[str, Optional[list]]:
        import random

        columns = list(manager.config.columns.keys())
        batch_size = self.scenario.write_batch_size
        id_col = state.profile.id_column if state.profile else None
        is_postgres = self._is_postgres_pool(pool)
        use_params = hasattr(getattr(manager, "pool", None), "execute_query_with_info")

        # If template specifies explicit insert columns, honor them.
        tpl_cfg = getattr(self, "_template_config", None)
        if isinstance(tpl_cfg, dict):
            ai_cfg = tpl_cfg.get("ai_workload")
            if isinstance(ai_cfg, dict) and isinstance(
                ai_cfg.get("insert_columns"), list
            ):
                desired = [
                    str(c).upper()
                    for c in ai_cfg.get("insert_columns")
                    if str(c).strip()
                ]
                available = {str(c).upper() for c in manager.config.columns.keys()}
                chosen = [c for c in desired if c in available]
                if chosen:
                    columns = chosen
                    # Ensure key column is inserted if required.
                    if (
                        id_col
                        and id_col.upper() in available
                        and id_col.upper() not in columns
                    ):
                        columns = [id_col.upper(), *columns]

        if not use_params:
            # Legacy literal SQL path (used by non-Snowflake pools)
            values_list: list[str] = []
            for _ in range(batch_size):
                row_values: list[str] = []
                for col in columns:
                    col_upper = col.upper()
                    col_type = manager.config.columns[col].upper()

                    if (
                        id_col
                        and col_upper == id_col.upper()
                        and any(
                            t in col_type
                            for t in ("NUMBER", "INT", "DECIMAL", "SERIAL")
                        )
                    ):
                        if state.next_insert_id is None:
                            state.next_insert_id = 1
                        row_values.append(str(state.next_insert_id))
                        state.next_insert_id += 1
                        continue

                    if "TIMESTAMP" in col_type:
                        row_values.append("CURRENT_TIMESTAMP")
                    elif "DATE" in col_type:
                        row_values.append("CURRENT_DATE")
                    elif (
                        "NUMBER" in col_type
                        or "INT" in col_type
                        or "DECIMAL" in col_type
                        or "SERIAL" in col_type
                    ):
                        row_values.append(str(random.randint(1, 1_000_000)))
                    elif (
                        "VARCHAR" in col_type
                        or "TEXT" in col_type
                        or "STRING" in col_type
                    ):
                        # Keep literals short; this path is primarily for debugging/non-Snowflake pools.
                        row_values.append(f"'T{random.randint(0, 9)}'")
                    else:
                        row_values.append("NULL")

                values_list.append(f"({', '.join(row_values)})")

            # For PostgreSQL, use unquoted lowercase column names
            if is_postgres:
                cols_sql = ", ".join(c.lower() for c in columns)
            else:
                cols_sql = ", ".join(columns)
            return (
                f"INSERT INTO {full_name} ({cols_sql}) VALUES {', '.join(values_list)}",
                None,
            )

        # Parameterized Snowflake insert - generate synthetic values.
        placeholders: list[str] = []
        params: list[Any] = []
        for i in range(batch_size):
            row_ph: list[str] = []
            for col in columns:
                col_upper = col.upper()
                col_type = manager.config.columns[col].upper()

                if id_col and col_upper == id_col.upper():
                    # Prefer monotonic unique IDs for numeric keys; otherwise use UUID.
                    if any(
                        t in col_type for t in ("NUMBER", "INT", "DECIMAL", "SERIAL")
                    ):
                        seq = state.insert_id_seq or count(1)
                        state.insert_id_seq = seq
                        params.append(next(seq))
                    else:
                        params.append(str(uuid4()))
                    row_ph.append("?")
                    continue

                if "TIMESTAMP" in col_type:
                    params.append(datetime.now(UTC))
                elif "DATE" in col_type:
                    params.append(datetime.now(UTC).date())
                elif (
                    "NUMBER" in col_type
                    or "INT" in col_type
                    or "DECIMAL" in col_type
                    or "SERIAL" in col_type
                ):
                    params.append(random.randint(1, 1000000))
                elif (
                    "VARCHAR" in col_type or "TEXT" in col_type or "STRING" in col_type
                ):
                    params.append(f"TEST_{random.randint(1, 1000000)}")
                else:
                    params.append(None)
                row_ph.append("?")

            placeholders.append(f"({', '.join(row_ph)})")

        # For PostgreSQL, use unquoted lowercase column names
        if is_postgres:
            cols_sql = ", ".join(c.lower() for c in columns)
        else:
            cols_sql = ", ".join(f'"{c.upper()}"' for c in columns)
        query = f"INSERT INTO {full_name} ({cols_sql}) VALUES {', '.join(placeholders)}"
        return query, params

    def _build_update(
        self, full_name: str, manager: TableManager, profile: TableProfile, pool=None
    ) -> tuple[str, list]:
        import random

        if not profile.id_column:
            raise ValueError("Cannot build update without id_column")
        id_col = str(profile.id_column)

        # Prefer pooled keys if available.
        pooled = self._next_from_pool(0, "KEY", id_col)
        if pooled is not None:
            target_id = pooled
        elif profile.id_min is not None and profile.id_max is not None:
            target_id = random.randint(profile.id_min, profile.id_max)
        else:
            raise ValueError("Cannot build update without key pool or id_min/id_max")

        # If template specifies update columns, honor that first.
        tpl_cfg = getattr(self, "_template_config", None)
        update_cols = []
        if isinstance(tpl_cfg, dict):
            ai_cfg = tpl_cfg.get("ai_workload")
            if isinstance(ai_cfg, dict) and isinstance(
                ai_cfg.get("update_columns"), list
            ):
                update_cols = [
                    str(c).upper()
                    for c in ai_cfg.get("update_columns")
                    if str(c).strip()
                ]

        is_postgres = self._is_postgres_pool(pool)
        candidates = update_cols or [c.upper() for c in manager.config.columns.keys()]
        for col_upper in candidates:
            if col_upper == id_col.upper():
                continue
            col_type_raw = manager.config.columns.get(
                col_upper
            ) or manager.config.columns.get(col_upper.lower())
            col_type = str(col_type_raw or "").upper()

            id_col_quoted = self._quote_column(id_col, pool)
            col_name = col_upper.lower() if is_postgres else col_upper
            col_quoted = self._quote_column(col_name, pool)

            if "TIMESTAMP" in col_type or "DATE" in col_type:
                query = f"UPDATE {full_name} SET {col_quoted} = CURRENT_TIMESTAMP WHERE {id_col_quoted} = ?"
                return query, [target_id]

            if "VARCHAR" in col_type or "TEXT" in col_type or "STRING" in col_type:
                query = (
                    f"UPDATE {full_name} SET {col_quoted} = ? WHERE {id_col_quoted} = ?"
                )
                return query, [f"TEST_{random.randint(1, 1000000)}", target_id]

            if "NUMBER" in col_type or "INT" in col_type or "DECIMAL" in col_type:
                query = (
                    f"UPDATE {full_name} SET {col_quoted} = ? WHERE {id_col_quoted} = ?"
                )
                return query, [random.randint(1, 1000000), target_id]

        id_col_quoted = self._quote_column(id_col, pool)
        query = f"UPDATE {full_name} SET {id_col_quoted} = {id_col_quoted} WHERE {id_col_quoted} = ?"
        return query, [target_id]

    async def _execute_custom(self, worker_id: int, warmup: bool = False):
        """
        Execute a CUSTOM operation.

        CUSTOM workloads are authoritative for templates:
        - Deterministic selection according to stored weights (exact over a full cycle)
        - SQL comes from template config (scenario.custom_queries), with `{table}` substituted
        - Params are generated for the canonical 4-query workload (POINT_LOOKUP/RANGE_SCAN/INSERT/UPDATE)
        """
        start_wall = datetime.now(UTC)
        start_perf = time.perf_counter()
        epoch_at_start = int(self._metrics_epoch)

        query_kind = self._custom_next_kind(worker_id)
        is_read = query_kind in {"POINT_LOOKUP", "RANGE_SCAN"}

        # Select random table (templates typically include 1 table; keep generic).
        import random

        manager = random.choice(self.table_managers)
        full_name = manager.get_full_table_name()
        self._table_state.setdefault(full_name, _TableRuntimeState())
        state = self._table_state[full_name]
        profile = state.profile

        sql_tpl = self._custom_sql_by_kind.get(query_kind)
        if not sql_tpl:
            raise ValueError(f"No SQL found for custom query kind {query_kind}")
        query = sql_tpl.replace("{table}", full_name)

        params: Optional[list[Any]] = None
        rows_written_expected = 0

        def _choose_id() -> Any:
            if profile and profile.id_column:
                pooled = self._next_from_pool(worker_id, "KEY", profile.id_column)
                if pooled is not None:
                    return pooled
                # Fallback: use ROW pool when KEY pool is missing.
                row_pool = self._pool_values("ROW", None)
                if row_pool:
                    key_col = getattr(self, "_ai_workload", {}).get("key_column")
                    key_col_u = str(key_col or "").strip().upper()
                    for row in row_pool:
                        if not isinstance(row, dict):
                            continue
                        if key_col_u and key_col_u in row:
                            return row.get(key_col_u)
                        if profile.id_column:
                            col_u = str(profile.id_column).strip().upper()
                            if col_u in row:
                                return row.get(col_u)
                # Fallback: use id bounds from profile.
                if (
                    profile.id_min is not None
                    and profile.id_max is not None
                    and profile.id_max >= profile.id_min
                ):
                    return random.randint(profile.id_min, profile.id_max)
            raise ValueError("Cannot choose key value (missing KEY pool and id bounds)")

        # Template-provided workload metadata (populated by UI adjustment + persisted on save).
        tpl_cfg = getattr(self, "_template_config", None)
        ai_cfg = None
        if isinstance(tpl_cfg, dict):
            ai_cfg = tpl_cfg.get("ai_workload")

        insert_cols: list[str] = []
        update_cols: list[str] = []
        if isinstance(ai_cfg, dict):
            if isinstance(ai_cfg.get("insert_columns"), list):
                insert_cols = [
                    str(c).upper()
                    for c in ai_cfg.get("insert_columns")
                    if str(c).strip()
                ]
            if isinstance(ai_cfg.get("update_columns"), list):
                update_cols = [
                    str(c).upper()
                    for c in ai_cfg.get("update_columns")
                    if str(c).strip()
                ]

        def _count_placeholders(sql: str) -> int:
            q_count = sql.count("?")
            if q_count > 0:
                return q_count
            pg_matches = re.findall(r"\$\d+", sql)
            return len(pg_matches)

        def _col_type(col_upper: str) -> str:
            raw = manager.config.columns.get(col_upper) or manager.config.columns.get(
                col_upper.lower()
            )
            return str(raw or "").upper()

        def _new_value_for(col_upper: str) -> Any:
            typ = _col_type(col_upper)
            if "TIMESTAMP" in typ:
                return datetime.now(UTC)
            if "DATE" in typ:
                return datetime.now(UTC).date()
            if "NUMBER" in typ or "INT" in typ or "DECIMAL" in typ:
                import random

                m = re.search(r"\((\d+)\s*,\s*(\d+)\)", typ)
                if m:
                    scale = int(m.group(2))
                    if scale > 0:
                        # Keep values small-ish but valid for the declared scale.
                        return round(random.random() * 1000.0, scale)
                return random.randint(1, 1_000_000)
            # Strings
            import random

            max_len: int | None = None
            m = re.search(r"(VARCHAR|CHAR|CHARACTER)\s*\(\s*(\d+)\s*\)", typ)
            if m:
                try:
                    max_len = int(m.group(2))
                except Exception:
                    max_len = None
            if max_len is not None and max_len <= 0:
                max_len = None
            if max_len == 1:
                return random.choice(list("ABCDEFGHIJKLMNOPQRSTUVWXYZ"))
            base = f"TEST_{random.randint(1, 1_000_000)}"
            return base[:max_len] if max_len else base

        def _split_top_level_csv(s: str) -> list[str]:
            out: list[str] = []
            cur: list[str] = []
            depth = 0
            in_sq = False
            for ch in s:
                if in_sq:
                    cur.append(ch)
                    if ch == "'":
                        in_sq = False
                    continue
                if ch == "'":
                    in_sq = True
                    cur.append(ch)
                    continue
                if ch == "(":
                    depth += 1
                elif ch == ")":
                    depth = max(0, depth - 1)
                if ch == "," and depth == 0:
                    token = "".join(cur).strip()
                    if token:
                        out.append(token)
                    cur = []
                    continue
                cur.append(ch)
            tail = "".join(cur).strip()
            if tail:
                out.append(tail)
            return out

        def _clean_ident(tok: str) -> str:
            t = str(tok or "").strip()
            # Strip double quotes used for identifiers.
            if len(t) >= 2 and t[0] == '"' and t[-1] == '"':
                t = t[1:-1]
            return t.strip().upper()

        def _insert_placeholder_columns(sql: str) -> list[str]:
            m = re.search(
                r"(?is)\binsert\s+into\s+.+?\(\s*(?P<cols>.*?)\s*\)\s*values\s*\(\s*(?P<vals>.*?)\s*\)",
                sql,
            )
            if not m:
                return []
            cols_raw = m.group("cols")
            vals_raw = m.group("vals")
            cols = [_clean_ident(c) for c in _split_top_level_csv(cols_raw)]
            vals = _split_top_level_csv(vals_raw)
            if not cols or not vals or len(cols) != len(vals):
                return []
            out: list[str] = []
            for col, val_expr in zip(cols, vals):
                n = str(val_expr).count("?")
                if n <= 0:
                    pg_matches = re.findall(r"\$\d+", val_expr)
                    n = len(pg_matches)
                if n <= 0:
                    continue
                out.extend([col] * n)
            return out

        if query_kind == "POINT_LOOKUP":
            target_id = _choose_id()
            params = [target_id]
            if not warmup:
                self._point_lookup_count += 1
        elif query_kind == "RANGE_SCAN":
            ph = _count_placeholders(query)
            if ph == 1:
                # Time-based cutoff preferred; bind from RANGE pool if available, else fallback.
                if profile and profile.time_column:
                    pooled = self._next_from_pool(
                        worker_id, "RANGE", profile.time_column
                    )
                    if pooled is not None:
                        params = [pooled]
                    elif profile.time_max is not None:
                        params = [profile.time_max - timedelta(days=7)]
                    else:
                        # Fallback: use ROW pool when RANGE pool is missing.
                        row_pool = self._pool_values("ROW", None)
                        if row_pool:
                            time_col = getattr(self, "_ai_workload", {}).get(
                                "time_column"
                            )
                            time_col_u = str(time_col or "").strip().upper()
                            for row in row_pool:
                                if not isinstance(row, dict):
                                    continue
                                if time_col_u and time_col_u in row:
                                    params = [row.get(time_col_u)]
                                    break
                                if profile.time_column:
                                    col_u = str(profile.time_column).strip().upper()
                                    if col_u in row:
                                        params = [row.get(col_u)]
                                        break
                        if params is None:
                            raise ValueError(
                                "Cannot choose range cutoff (missing RANGE pool and time bounds)"
                            )
                else:
                    raise ValueError(
                        "Range scan SQL expects 1 param but no time column detected"
                    )
            else:
                # Default: id-based BETWEEN form expects 2 params; bind (start,start).
                start_id = _choose_id()
                params = [start_id, start_id]
            if not warmup:
                self._range_scan_count += 1
        elif query_kind == "INSERT":
            ph = _count_placeholders(query)
            if ph <= 0:
                raise ValueError("INSERT SQL must use placeholders")

            cols_from_sql = _insert_placeholder_columns(query)
            if cols_from_sql and len(cols_from_sql) == ph:
                cols = cols_from_sql
            else:
                # Fallback: use insert_columns ordering if provided; else manager columns.
                cols = insert_cols or [
                    str(c).upper() for c in manager.config.columns.keys()
                ]
                cols = cols[:ph]

            params = []
            for c in cols:
                c_upper = c.upper()
                if (
                    profile
                    and profile.id_column
                    and c_upper == str(profile.id_column).upper()
                ):
                    col_type = _col_type(c_upper)
                    if any(t in col_type for t in ("NUMBER", "INT", "DECIMAL")):
                        if state.insert_id_seq is None:
                            start_id = (profile.id_max or 0) + 1
                            state.insert_id_seq = count(start_id)
                        params.append(next(state.insert_id_seq))
                    else:
                        params.append(str(uuid4()))
                    continue
                params.append(_new_value_for(c_upper))
            rows_written_expected = 1
            if not warmup:
                self._insert_count += 1
        elif query_kind == "UPDATE":
            target_id = _choose_id()
            ph = _count_placeholders(query)
            if ph == 1:
                params = [target_id]
            else:
                set_col = None
                m = re.search(
                    r"(?is)\bset\s+[\"']?(\w+)[\"']?\s*=\s*(?:\?|\$\d+)",
                    query,
                )
                if m:
                    set_col = m.group(1).upper()
                if not set_col:
                    set_col = update_cols[0] if update_cols else None
                new_val = _new_value_for(set_col) if set_col else f"TEST_{uuid4()}"
                params = [new_val, target_id]
            rows_written_expected = 1
            if not warmup:
                self._update_count += 1
        else:
            raise ValueError(f"Unsupported custom query kind {query_kind}")

        try:
            if not hasattr(manager, "pool") or not hasattr(
                manager.pool, "execute_query_with_info"
            ):
                raise ValueError(
                    "CUSTOM workloads require a pool with execute_query_with_info"
                )

            # Only annotate for Snowflake (has warehouse attribute)
            is_snowflake = hasattr(manager.pool, "warehouse")
            if is_snowflake:
                query = self._annotate_query_for_sf_kind(query, query_kind)

            # Execute via pool (supports both Snowflake and Postgres)
            if is_read:
                result, info = await manager.pool.execute_query_with_info(
                    query, params=params, fetch=True
                )
                rows_read = len(result or [])
                sf_query_id = str(info.get("query_id") or "")
                sf_rowcount = info.get("rowcount")
            else:
                _, info = await manager.pool.execute_query_with_info(
                    query, params=params, fetch=False
                )
                rows_read = 0
                sf_query_id = str(info.get("query_id") or "")
                sf_rowcount = info.get("rowcount")

            end_wall = datetime.now(UTC)
            app_elapsed_ms = (time.perf_counter() - start_perf) * 1000.0
            duration_ms = app_elapsed_ms

            counted = False
            async with self._metrics_lock:
                if int(self._metrics_epoch) == int(epoch_at_start):
                    counted = True
                    self.metrics.total_operations += 1
                    self.metrics.successful_operations += 1
                    self._latencies_ms.append(duration_ms)
                    if (
                        self._find_max_step_collecting
                        and query_kind in self._find_max_step_ops_by_kind
                    ):
                        self._find_max_step_ops_by_kind[query_kind] = (
                            int(self._find_max_step_ops_by_kind.get(query_kind, 0)) + 1
                        )
                        self._find_max_step_lat_by_kind_ms[query_kind].append(
                            duration_ms
                        )

                    if is_read:
                        self.metrics.read_metrics.count += 1
                        self.metrics.read_metrics.success_count += 1
                        self.metrics.read_metrics.total_duration_ms += duration_ms
                        self.metrics.rows_read += int(rows_read)
                    else:
                        rows_written = (
                            int(sf_rowcount)
                            if sf_rowcount is not None
                            else int(rows_written_expected)
                        )
                        self.metrics.write_metrics.count += 1
                        self.metrics.write_metrics.success_count += 1
                        self.metrics.write_metrics.total_duration_ms += duration_ms
                        self.metrics.rows_written += rows_written

            if counted and not warmup:
                if is_read:
                    self._lat_read_ms.append(duration_ms)
                else:
                    self._lat_write_ms.append(duration_ms)
                self._lat_by_kind_ms[query_kind].append(duration_ms)

            if warmup or getattr(self.scenario, "collect_query_history", False):
                pool_obj = getattr(manager, "pool", None)
                pool_warehouse = str(getattr(pool_obj, "warehouse", "")).strip() or None
                self._query_execution_records.append(
                    _QueryExecutionRecord(
                        execution_id=str(uuid4()),
                        test_id=str(self.test_id),
                        query_id=sf_query_id or f"LOCAL_{uuid4()}",
                        query_text=query,
                        start_time=start_wall,
                        end_time=end_wall,
                        duration_ms=app_elapsed_ms,
                        success=True,
                        error=None,
                        warehouse=pool_warehouse,
                        rows_affected=(
                            int(sf_rowcount)
                            if sf_rowcount is not None
                            else (
                                int(rows_read)
                                if is_read
                                else int(rows_written_expected)
                            )
                        ),
                        bytes_scanned=None,
                        connection_id=None,
                        custom_metadata={
                            "params_count": len(params or []),
                            "rows_returned": int(rows_read),
                        }
                        if is_read
                        else {"params_count": len(params or [])},
                        query_kind=query_kind,
                        worker_id=worker_id,
                        warmup=warmup,
                        app_elapsed_ms=app_elapsed_ms,
                    )
                )

        except Exception as e:
            end_wall = datetime.now(UTC)
            app_elapsed_ms = (time.perf_counter() - start_perf) * 1000.0
            # Per-operation SQL failures are expected under load (locks/timeouts, etc).
            # Do not log each failure individually; rely on aggregated metrics/logging.

            category = self._classify_sql_error(e)
            counted = False
            sample_n: int | None = None
            async with self._metrics_lock:
                if int(self._metrics_epoch) == int(epoch_at_start):
                    counted = True
                    self.metrics.total_operations += 1
                    self.metrics.failed_operations += 1
                    if is_read:
                        self.metrics.read_metrics.count += 1
                        self.metrics.read_metrics.error_count += 1
                    else:
                        self.metrics.write_metrics.count += 1
                        self.metrics.write_metrics.error_count += 1
                    if (
                        self._find_max_step_collecting
                        and query_kind in self._find_max_step_ops_by_kind
                    ):
                        self._find_max_step_ops_by_kind[query_kind] = (
                            int(self._find_max_step_ops_by_kind.get(query_kind, 0)) + 1
                        )
                        self._find_max_step_errors_by_kind[query_kind] = (
                            int(self._find_max_step_errors_by_kind.get(query_kind, 0))
                            + 1
                        )
                    self._sql_error_categories[category] = (
                        int(self._sql_error_categories.get(category, 0)) + 1
                    )
                    sample_n = int(self._sql_error_sample_counts.get(category, 0)) + 1
                    self._sql_error_sample_counts[category] = int(sample_n)

            if counted and sample_n is not None and int(sample_n) <= 10:
                pool_obj = getattr(manager, "pool", None)
                backend = "unknown"
                try:
                    if pool_obj is not None and self._is_postgres_pool(pool_obj):
                        backend = "postgres"
                    elif pool_obj is not None and hasattr(pool_obj, "warehouse"):
                        backend = "snowflake"
                except Exception:
                    backend = "unknown"

                payload = {
                    "category": category,
                    "sample_n": int(sample_n),
                    "sample_max": 10,
                    "backend": backend,
                    "query_kind": str(query_kind or ""),
                    "table": str(full_name or ""),
                    "worker_id": int(worker_id),
                    "warmup": bool(warmup),
                    "elapsed_ms": round(float(app_elapsed_ms), 2),
                    "exception": {
                        "type": type(e).__name__,
                        "message": self._truncate_str_for_log(e, max_chars=800),
                        **self._sql_error_meta_for_log(e),
                    },
                    "query": self._preview_query_for_log(query, max_chars=2000),
                    "params": self._preview_params_for_log(
                        params,
                        max_items=10,
                        max_value_chars=200,
                    ),
                }
                logger.warning(
                    "SQL_ERROR_SAMPLE %s",
                    json.dumps(payload, ensure_ascii=False),
                )

            if warmup or getattr(self.scenario, "collect_query_history", False):
                self._query_execution_records.append(
                    _QueryExecutionRecord(
                        execution_id=str(uuid4()),
                        test_id=str(self.test_id),
                        query_id=f"LOCAL_{uuid4()}",
                        query_text=query,
                        start_time=start_wall,
                        end_time=end_wall,
                        duration_ms=app_elapsed_ms,
                        success=False,
                        error=str(e),
                        warehouse=None,
                        rows_affected=None,
                        bytes_scanned=None,
                        connection_id=None,
                        custom_metadata={"params_count": len(params or [])},
                        query_kind=query_kind,
                        worker_id=worker_id,
                        warmup=warmup,
                        app_elapsed_ms=app_elapsed_ms,
                    )
                )

    async def _collect_metrics(self):
        """Collect metrics at regular intervals."""
        interval = float(getattr(self.scenario, "metrics_interval_seconds", 1.0) or 1.0)
        if not math.isfinite(interval) or interval <= 0:
            interval = 1.0

        # Use a monotonic clock for QPS so we don't get spikes from wall-clock adjustments.
        loop = asyncio.get_running_loop()
        # QPS EWMA smoothing (seconds). Larger => smoother, less reactive.
        qps_tau_seconds = 5.0

        first_iteration = True
        try:
            while True:
                if first_iteration:
                    first_iteration = False
                else:
                    await asyncio.sleep(interval)

                now = datetime.now(UTC)
                now_mono = float(loop.time())
                self.metrics.timestamp = now

                # Derive "in-flight queries" from the underlying pool(s) so the UI matches
                # Snowflake's notion of concurrent execution (connections currently checked out).
                #
                # This is more reliable than sampling task-level counters at the exact tick
                # boundary (which can appear as 0 if workers are phase-aligned).
                in_use_total = 0
                idle_total = 0
                seen_pools: set[int] = set()
                for manager in self.table_managers:
                    pool = getattr(manager, "pool", None)
                    if pool is None or not hasattr(pool, "get_pool_stats"):
                        continue
                    pid = id(pool)
                    if pid in seen_pools:
                        continue
                    seen_pools.add(pid)
                    try:
                        stats = await pool.get_pool_stats()
                        in_use_total += int(stats.get("in_use") or 0)
                        idle_total += int(stats.get("available") or 0)
                    except Exception:
                        # Best-effort: ignore individual pool stat failures.
                        continue

                # Schedule Snowflake telemetry sampling out-of-band so the metrics loop remains
                # low-latency (critical for stable QPS control).
                #
                # Notes:
                # - We use a dedicated telemetry pool so sampling can't be starved by
                #   overlapping results persistence queries under load.
                # - We never await these calls here; they update in-memory status when finished.
                if self._benchmark_warehouse_name:
                    last = self._last_warehouse_status_mono
                    due = last is None or (now_mono - float(last)) >= 5.0
                    task = self._warehouse_status_task
                    if due and (task is None or task.done()):

                        async def _sample_warehouse_status(warehouse_name: str) -> None:
                            try:
                                rows = await snowflake_pool.get_telemetry_pool().execute_query(
                                    f"SHOW WAREHOUSES LIKE '{warehouse_name}'"
                                )
                                if rows:
                                    row = rows[0]
                                    started = int(row[6]) if row[6] else 0
                                    running = int(row[7]) if row[7] else 0
                                    queued = int(row[8]) if row[8] else 0
                                    self._warehouse_status = {
                                        "warehouse": str(warehouse_name),
                                        "started_clusters": started,
                                        "running": running,
                                        "queued": queued,
                                    }
                            except Exception:
                                pass

                        self._last_warehouse_status_mono = now_mono
                        self._warehouse_status_task = asyncio.create_task(
                            _sample_warehouse_status(
                                str(self._benchmark_warehouse_name)
                            )
                        )

                if self._benchmark_warehouse_name and self._benchmark_query_tag:
                    last = self._last_warehouse_query_status_mono
                    due = last is None or (now_mono - float(last)) >= 5.0
                    task = self._warehouse_query_status_task
                    if due and (task is None or task.done()):

                        async def _sample_bench_query_status(
                            warehouse_name: str, query_tag: str
                        ) -> None:
                            try:
                                sample_mono = float(asyncio.get_running_loop().time())
                                rows = await snowflake_pool.get_telemetry_pool().execute_query(
                                    """
                                    SELECT
                                      SUM(IFF(UPPER(EXECUTION_STATUS) = 'RUNNING', 1, 0)) AS RUNNING,
                                      SUM(IFF(UPPER(EXECUTION_STATUS) = 'QUEUED', 1, 0)) AS QUEUED,
                                      SUM(IFF(UPPER(EXECUTION_STATUS) = 'BLOCKED', 1, 0)) AS BLOCKED,
                                      SUM(IFF(UPPER(EXECUTION_STATUS) = 'RESUMING_WAREHOUSE', 1, 0)) AS RESUMING_WAREHOUSE,

                                      -- Break down RUNNING concurrency by benchmark query kind.
                                      --
                                      -- Preferred signal: our benchmark tag `UB_KIND=...` when present in QUERY_TEXT.
                                      SUM(IFF(
                                        UPPER(EXECUTION_STATUS) = 'RUNNING'
                                        AND REGEXP_LIKE(QUERY_TEXT, 'UB_KIND=POINT_LOOKUP'),
                                        1, 0
                                      )) AS RUNNING_POINT_LOOKUP,
                                      SUM(IFF(
                                        UPPER(EXECUTION_STATUS) = 'RUNNING'
                                        AND REGEXP_LIKE(QUERY_TEXT, 'UB_KIND=RANGE_SCAN'),
                                        1, 0
                                      )) AS RUNNING_RANGE_SCAN,
                                      SUM(IFF(
                                        UPPER(EXECUTION_STATUS) = 'RUNNING'
                                        AND REGEXP_LIKE(QUERY_TEXT, 'UB_KIND=INSERT'),
                                        1, 0
                                      )) AS RUNNING_INSERT,
                                      SUM(IFF(
                                        UPPER(EXECUTION_STATUS) = 'RUNNING'
                                        AND REGEXP_LIKE(QUERY_TEXT, 'UB_KIND=UPDATE'),
                                        1, 0
                                      )) AS RUNNING_UPDATE
                                    FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY(
                                      RESULT_LIMIT => 10000
                                    ))
                                    WHERE QUERY_TAG = ?
                                      AND WAREHOUSE_NAME = ?
                                      -- NOTE: QUEUED queries can have START_TIME = NULL until execution begins.
                                      -- Filtering on START_TIME incorrectly drops queued rows and makes the
                                      -- live "queued" series appear as all zeros even when the warehouse is
                                      -- actively queueing work.
                                      AND UPPER(EXECUTION_STATUS) IN (
                                        'RUNNING', 'QUEUED', 'BLOCKED', 'RESUMING_WAREHOUSE'
                                      )
                                    """,
                                    params=[str(query_tag), str(warehouse_name)],
                                )
                                if rows:
                                    row = rows[0]
                                    running = int(row[0] or 0)
                                    queued = int(row[1] or 0)
                                    blocked = int(row[2] or 0)
                                    resuming = int(row[3] or 0)
                                    running_pl = int(row[4] or 0)
                                    running_rs = int(row[5] or 0)
                                    running_ins = int(row[6] or 0)
                                    running_upd = int(row[7] or 0)
                                    running_read = int(running_pl + running_rs)
                                    running_write = int(running_ins + running_upd)
                                    running_tagged = int(
                                        running_pl
                                        + running_rs
                                        + running_ins
                                        + running_upd
                                    )
                                    running_other = max(
                                        0, int(running) - int(running_tagged)
                                    )
                                    self._warehouse_query_status = {
                                        "sample_mono": float(sample_mono),
                                        "warehouse": str(warehouse_name),
                                        "query_tag": str(query_tag),
                                        "running": running,
                                        "running_tagged": running_tagged,
                                        "running_other": running_other,
                                        "running_read": running_read,
                                        "running_write": running_write,
                                        "running_point_lookup": running_pl,
                                        "running_range_scan": running_rs,
                                        "running_insert": running_ins,
                                        "running_update": running_upd,
                                        "queued": queued,
                                        "blocked": blocked,
                                        "resuming_warehouse": resuming,
                                    }
                            except Exception:
                                pass

                        self._last_warehouse_query_status_mono = now_mono
                        self._warehouse_query_status_task = asyncio.create_task(
                            _sample_bench_query_status(
                                str(self._benchmark_warehouse_name),
                                str(self._benchmark_query_tag),
                            )
                        )

                # Update elapsed time
                base = self._measurement_start_time or self.start_time
                if base:
                    self.metrics.elapsed_seconds = (now - base).total_seconds()

                async with self._metrics_lock:
                    self.metrics.active_connections = max(0, int(in_use_total))
                    self.metrics.idle_connections = max(0, int(idle_total))
                    self.metrics.target_workers = max(0, int(self._target_workers))

                    # Calculate average QPS
                    if self.metrics.elapsed_seconds > 0:
                        self.metrics.avg_qps = (
                            self.metrics.total_operations / self.metrics.elapsed_seconds
                        )

                    # Calculate current QPS (since last snapshot)
                    dt = None
                    if self._last_snapshot_mono is not None:
                        dt = float(now_mono) - float(self._last_snapshot_mono)
                    if dt is not None and math.isfinite(dt) and dt > 0:
                        ops_delta = (
                            self.metrics.total_operations - self._last_snapshot_ops
                        )
                        qps_raw = float(ops_delta) / float(dt)
                        self.metrics.current_qps = qps_raw
                        if self.metrics.current_qps > self.metrics.peak_qps:
                            self.metrics.peak_qps = self.metrics.current_qps

                        # EWMA smoothing of QPS for the controller to avoid 1s burstiness.
                        alpha = 1.0 - math.exp(-float(dt) / float(qps_tau_seconds))
                        if self._qps_smoothed is None:
                            self._qps_smoothed = float(qps_raw)
                        else:
                            self._qps_smoothed = (1.0 - float(alpha)) * float(
                                self._qps_smoothed
                            ) + float(alpha) * float(qps_raw)

                    # Rolling-window QPS (more stable than 1-tick rates).
                    window_s = float(self._qps_window_seconds or 10.0)
                    if not math.isfinite(window_s) or window_s <= 0:
                        window_s = 10.0
                    self._qps_samples.append(
                        (float(now_mono), int(self.metrics.total_operations))
                    )
                    while (
                        len(self._qps_samples) >= 2
                        and (float(now_mono) - float(self._qps_samples[0][0]))
                        > window_s
                    ):
                        self._qps_samples.popleft()
                    qps_windowed = 0.0
                    if len(self._qps_samples) >= 2:
                        t0, o0 = self._qps_samples[0]
                        t1, o1 = self._qps_samples[-1]
                        dtw = float(t1) - float(t0)
                        if math.isfinite(dtw) and dtw > 0:
                            qps_windowed = float(int(o1) - int(o0)) / dtw
                    self._qps_windowed = float(qps_windowed)

                    self._last_snapshot_time = now
                    self._last_snapshot_mono = now_mono
                    self._last_snapshot_ops = self.metrics.total_operations

                    # Best-effort resource sampling (process + host + cgroup limits).
                    if self._process is not None:
                        try:
                            self.metrics.cpu_percent = float(
                                self._process.cpu_percent(interval=None)
                            )
                            self.metrics.memory_mb = float(
                                self._process.memory_info().rss
                            ) / (1024 * 1024)
                        except Exception:
                            pass

                    host_cpu_percent: float | None = None
                    host_memory_mb: float | None = None
                    host_memory_total_mb: float | None = None
                    host_memory_available_mb: float | None = None
                    host_memory_percent: float | None = None
                    host_cpu_cores: int | None = self._host_cpu_cores
                    if self._psutil is not None:
                        try:
                            host_cpu_percent = float(
                                self._psutil.cpu_percent(interval=None)
                            )
                        except Exception:
                            host_cpu_percent = None
                        try:
                            vm = self._psutil.virtual_memory()
                            host_memory_total_mb = float(vm.total) / (1024 * 1024)
                            host_memory_available_mb = float(vm.available) / (
                                1024 * 1024
                            )
                            host_memory_mb = float(vm.total - vm.available) / (
                                1024 * 1024
                            )
                            host_memory_percent = float(vm.percent)
                        except Exception:
                            host_memory_mb = None

                    cgroup_cpu_percent: float | None = None
                    cgroup_cpu_quota_cores: float | None = None
                    cgroup_memory_mb: float | None = None
                    cgroup_memory_limit_mb: float | None = None
                    cgroup_memory_percent: float | None = None
                    cgroup = self._read_cgroup_limits()
                    if cgroup:
                        cgroup_cpu_quota_cores = cgroup.get("cpu_quota_cores")
                        cgroup_memory_mb = cgroup.get("memory_mb")
                        cgroup_memory_limit_mb = cgroup.get("memory_limit_mb")
                        if (
                            cgroup_memory_mb is not None
                            and cgroup_memory_limit_mb is not None
                            and cgroup_memory_limit_mb > 0
                        ):
                            cgroup_memory_percent = (
                                cgroup_memory_mb / cgroup_memory_limit_mb
                            ) * 100.0
                        cgroup_cpu_percent = self._sample_cgroup_cpu_percent(
                            usage=cgroup.get("cpu_usage"),
                            usage_unit=cgroup.get("cpu_usage_unit"),
                            cpu_cores=(
                                cgroup_cpu_quota_cores
                                or float(host_cpu_cores or 0)
                                or None
                            ),
                        )

                    # Attach controller + warehouse telemetry for the live dashboard.
                    custom = dict(self.metrics.custom_metrics or {})
                    resources = dict(custom.get("resources") or {})
                    if self.metrics.cpu_percent is not None:
                        resources["cpu_percent"] = float(self.metrics.cpu_percent)
                        resources["process_cpu_percent"] = float(
                            self.metrics.cpu_percent
                        )
                    if self.metrics.memory_mb is not None:
                        resources["memory_mb"] = float(self.metrics.memory_mb)
                        resources["process_memory_mb"] = float(self.metrics.memory_mb)
                    if host_cpu_percent is not None:
                        resources["host_cpu_percent"] = float(host_cpu_percent)
                    if host_cpu_cores is not None:
                        resources["host_cpu_cores"] = int(host_cpu_cores)
                    if host_memory_mb is not None:
                        resources["host_memory_mb"] = float(host_memory_mb)
                    if host_memory_total_mb is not None:
                        resources["host_memory_total_mb"] = float(host_memory_total_mb)
                    if host_memory_available_mb is not None:
                        resources["host_memory_available_mb"] = float(
                            host_memory_available_mb
                        )
                    if host_memory_percent is not None:
                        resources["host_memory_percent"] = float(host_memory_percent)
                    if cgroup_cpu_percent is not None:
                        resources["cgroup_cpu_percent"] = float(cgroup_cpu_percent)
                    if cgroup_cpu_quota_cores is not None:
                        resources["cgroup_cpu_quota_cores"] = float(
                            cgroup_cpu_quota_cores
                        )
                    if cgroup_memory_mb is not None:
                        resources["cgroup_memory_mb"] = float(cgroup_memory_mb)
                    if cgroup_memory_limit_mb is not None:
                        resources["cgroup_memory_limit_mb"] = float(
                            cgroup_memory_limit_mb
                        )
                    if cgroup_memory_percent is not None:
                        resources["cgroup_memory_percent"] = float(
                            cgroup_memory_percent
                        )
                    if resources:
                        custom["resources"] = resources
                    custom["qps"] = {
                        "raw": float(getattr(self.metrics, "current_qps", 0.0) or 0.0),
                        "smoothed": float(self._qps_smoothed or 0.0),
                        "windowed": float(self._qps_windowed or 0.0),
                        "window_seconds": float(window_s),
                    }
                    if self._qps_controller_state:
                        custom["qps_controller"] = dict(self._qps_controller_state)
                    if self._find_max_controller_state:
                        custom["find_max_controller"] = dict(
                            self._find_max_controller_state
                        )
                    if self._warehouse_status:
                        custom["warehouse"] = dict(self._warehouse_status)
                    if self._warehouse_query_status:
                        custom["sf_bench"] = dict(self._warehouse_query_status)

                    # App-level ops breakdown (accurate, real-time counters from the app).
                    # These track actual operations initiated by this test, not SF-side concurrency.
                    elapsed = self.metrics.elapsed_seconds or 0.0
                    custom["app_ops_breakdown"] = {
                        "point_lookup_count": int(self._point_lookup_count),
                        "range_scan_count": int(self._range_scan_count),
                        "insert_count": int(self._insert_count),
                        "update_count": int(self._update_count),
                        "read_count": int(
                            self._point_lookup_count + self._range_scan_count
                        ),
                        "write_count": int(self._insert_count + self._update_count),
                        "total_count": int(
                            self._point_lookup_count
                            + self._range_scan_count
                            + self._insert_count
                            + self._update_count
                        ),
                        # QPS breakdown (based on elapsed time).
                        "point_lookup_ops_sec": float(
                            self._point_lookup_count / elapsed
                        )
                        if elapsed > 0
                        else 0.0,
                        "range_scan_ops_sec": float(self._range_scan_count / elapsed)
                        if elapsed > 0
                        else 0.0,
                        "insert_ops_sec": float(self._insert_count / elapsed)
                        if elapsed > 0
                        else 0.0,
                        "update_ops_sec": float(self._update_count / elapsed)
                        if elapsed > 0
                        else 0.0,
                        "read_ops_sec": float(
                            (self._point_lookup_count + self._range_scan_count)
                            / elapsed
                        )
                        if elapsed > 0
                        else 0.0,
                        "write_ops_sec": float(
                            (self._insert_count + self._update_count) / elapsed
                        )
                        if elapsed > 0
                        else 0.0,
                    }

                    if (
                        self._latency_sf_execution_ms
                        or self._latency_network_overhead_ms
                    ):
                        sf_exec_list = list(self._latency_sf_execution_ms)
                        network_list = list(self._latency_network_overhead_ms)
                        custom["latency_breakdown"] = {
                            "sf_execution_avg_ms": sum(sf_exec_list) / len(sf_exec_list)
                            if sf_exec_list
                            else 0.0,
                            "network_overhead_avg_ms": sum(network_list)
                            / len(network_list)
                            if network_list
                            else 0.0,
                            "sample_count": len(sf_exec_list),
                        }
                    self.metrics.custom_metrics = custom

                    # Latency percentiles over a rolling window
                    if self._latencies_ms:
                        latencies = sorted(self._latencies_ms)

                        def pct(p: float) -> float:
                            if not latencies:
                                return 0.0
                            k = int(round((p / 100.0) * (len(latencies) - 1)))
                            k = max(0, min(k, len(latencies) - 1))
                            return float(latencies[k])

                        self.metrics.overall_latency.p50 = pct(50)
                        self.metrics.overall_latency.p90 = pct(90)
                        self.metrics.overall_latency.p95 = pct(95)
                        self.metrics.overall_latency.p99 = pct(99)
                        self.metrics.overall_latency.min = float(latencies[0])
                        self.metrics.overall_latency.max = float(latencies[-1])
                        self.metrics.overall_latency.avg = float(
                            sum(latencies) / len(latencies)
                        )

                # Invoke callback if set
                if self.metrics_callback:
                    try:
                        self.metrics_callback(self.metrics)
                    except Exception as e:
                        logger.error(f"Metrics callback error: {e}")

        except asyncio.CancelledError:
            pass

    async def _build_result(self) -> TestResult:
        """Build test result from metrics."""
        duration = 0.0
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()

        def _pct(values: list[float], p: float) -> float:
            if not values:
                return 0.0
            xs = sorted(values)
            if len(xs) == 1:
                return float(xs[0])
            k = int(round((p / 100.0) * (len(xs) - 1)))
            k = max(0, min(k, len(xs) - 1))
            return float(xs[k])

        def _min(values: list[float]) -> float:
            return float(min(values)) if values else 0.0

        def _max(values: list[float]) -> float:
            return float(max(values)) if values else 0.0

        def _avg(values: list[float]) -> float:
            return float(sum(values) / len(values)) if values else 0.0

        overall_lat = list(self._latencies_ms)
        overall_p50 = _pct(overall_lat, 50)
        overall_p90 = _pct(overall_lat, 90)
        overall_p95 = _pct(overall_lat, 95)
        overall_p99 = _pct(overall_lat, 99)
        overall_min = _min(overall_lat)
        overall_max = _max(overall_lat)
        overall_avg = _avg(overall_lat)

        read_p50 = _pct(self._lat_read_ms, 50)
        read_p95 = _pct(self._lat_read_ms, 95)
        read_p99 = _pct(self._lat_read_ms, 99)
        read_min = _min(self._lat_read_ms)
        read_max = _max(self._lat_read_ms)

        write_p50 = _pct(self._lat_write_ms, 50)
        write_p95 = _pct(self._lat_write_ms, 95)
        write_p99 = _pct(self._lat_write_ms, 99)
        write_min = _min(self._lat_write_ms)
        write_max = _max(self._lat_write_ms)

        pl = self._lat_by_kind_ms.get("POINT_LOOKUP", [])
        rs = self._lat_by_kind_ms.get("RANGE_SCAN", [])
        ins = self._lat_by_kind_ms.get("INSERT", [])
        upd = self._lat_by_kind_ms.get("UPDATE", [])

        # Get first table info
        table_config = (
            self.scenario.table_configs[0] if self.scenario.table_configs else None
        )

        result = TestResult(
            test_id=self.test_id,
            test_name=f"test_{self.scenario.name}",
            scenario_name=self.scenario.name,
            table_name=table_config.name if table_config else "unknown",
            table_type=table_config.table_type if table_config else "unknown",
            status=self.status,
            start_time=self.start_time or datetime.now(UTC),
            end_time=self.end_time,
            duration_seconds=duration,
            concurrent_connections=self.scenario.total_threads,
            total_operations=self.metrics.total_operations,
            read_operations=self.metrics.read_metrics.count,
            write_operations=self.metrics.write_metrics.count,
            failed_operations=self.metrics.failed_operations,
            failure_reason=self._setup_error,
            qps=self.metrics.avg_qps,
            reads_per_second=self.metrics.read_metrics.count / duration
            if duration > 0
            else 0,
            writes_per_second=self.metrics.write_metrics.count / duration
            if duration > 0
            else 0,
            rows_read=self.metrics.rows_read,
            rows_written=self.metrics.rows_written,
            avg_latency_ms=overall_avg,
            p50_latency_ms=overall_p50,
            p90_latency_ms=overall_p90,
            p95_latency_ms=overall_p95,
            p99_latency_ms=overall_p99,
            min_latency_ms=overall_min,
            max_latency_ms=overall_max,
            read_p50_latency_ms=read_p50,
            read_p95_latency_ms=read_p95,
            read_p99_latency_ms=read_p99,
            read_min_latency_ms=read_min,
            read_max_latency_ms=read_max,
            write_p50_latency_ms=write_p50,
            write_p95_latency_ms=write_p95,
            write_p99_latency_ms=write_p99,
            write_min_latency_ms=write_min,
            write_max_latency_ms=write_max,
            point_lookup_p50_latency_ms=_pct(pl, 50),
            point_lookup_p95_latency_ms=_pct(pl, 95),
            point_lookup_p99_latency_ms=_pct(pl, 99),
            point_lookup_min_latency_ms=_min(pl),
            point_lookup_max_latency_ms=_max(pl),
            range_scan_p50_latency_ms=_pct(rs, 50),
            range_scan_p95_latency_ms=_pct(rs, 95),
            range_scan_p99_latency_ms=_pct(rs, 99),
            range_scan_min_latency_ms=_min(rs),
            range_scan_max_latency_ms=_max(rs),
            insert_p50_latency_ms=_pct(ins, 50),
            insert_p95_latency_ms=_pct(ins, 95),
            insert_p99_latency_ms=_pct(ins, 99),
            insert_min_latency_ms=_min(ins),
            insert_max_latency_ms=_max(ins),
            update_p50_latency_ms=_pct(upd, 50),
            update_p95_latency_ms=_pct(upd, 95),
            update_p99_latency_ms=_pct(upd, 99),
            update_min_latency_ms=_min(upd),
            update_max_latency_ms=_max(upd),
        )

        return result

    def _read_text(self, path: str) -> str | None:
        try:
            with open(path, "r", encoding="utf-8") as handle:
                return handle.read().strip()
        except Exception:
            return None

    def _read_int(self, path: str) -> int | None:
        raw = self._read_text(path)
        if raw is None:
            return None
        try:
            return int(raw.strip())
        except Exception:
            return None

    def _read_cgroup_limits(self) -> dict[str, Any] | None:
        root = "/sys/fs/cgroup"
        if not os.path.isdir(root):
            return None
        # cgroup v2 has cgroup.controllers at the root.
        if os.path.exists(os.path.join(root, "cgroup.controllers")):
            return self._read_cgroup_v2(root)
        return self._read_cgroup_v1()

    def _read_cgroup_v2(self, root: str) -> dict[str, Any] | None:
        cpu_max = self._read_text(os.path.join(root, "cpu.max"))
        cpu_stat = self._read_text(os.path.join(root, "cpu.stat"))
        mem_current = self._read_text(os.path.join(root, "memory.current"))
        mem_max = self._read_text(os.path.join(root, "memory.max"))

        cpu_quota_cores: float | None = None
        if cpu_max:
            parts = cpu_max.split()
            if len(parts) >= 2 and parts[0] != "max":
                try:
                    quota = float(parts[0])
                    period = float(parts[1])
                    if period > 0:
                        cpu_quota_cores = quota / period
                except Exception:
                    cpu_quota_cores = None

        cpu_usage: float | None = None
        if cpu_stat:
            for line in cpu_stat.splitlines():
                if line.startswith("usage_usec"):
                    try:
                        cpu_usage = float(line.split()[1])
                    except Exception:
                        cpu_usage = None
                    break

        memory_mb: float | None = None
        if mem_current:
            try:
                memory_mb = float(mem_current) / (1024 * 1024)
            except Exception:
                memory_mb = None

        memory_limit_mb: float | None = None
        if mem_max and mem_max != "max":
            try:
                memory_limit_mb = float(mem_max) / (1024 * 1024)
            except Exception:
                memory_limit_mb = None

        if (
            cpu_quota_cores is None
            and cpu_usage is None
            and memory_mb is None
            and memory_limit_mb is None
        ):
            return None

        return {
            "cpu_quota_cores": cpu_quota_cores,
            "cpu_usage": cpu_usage,
            "cpu_usage_unit": "usec",
            "memory_mb": memory_mb,
            "memory_limit_mb": memory_limit_mb,
        }

    def _read_cgroup_v1(self) -> dict[str, Any] | None:
        cpu_dir = "/sys/fs/cgroup/cpu"
        cpuacct_dir = "/sys/fs/cgroup/cpuacct"
        mem_dir = "/sys/fs/cgroup/memory"

        cpu_quota_cores: float | None = None
        quota_us = self._read_int(os.path.join(cpu_dir, "cpu.cfs_quota_us"))
        period_us = self._read_int(os.path.join(cpu_dir, "cpu.cfs_period_us"))
        if (
            quota_us is not None
            and period_us is not None
            and quota_us > 0
            and period_us > 0
        ):
            cpu_quota_cores = float(quota_us) / float(period_us)

        cpu_usage: float | None = None
        usage_ns = self._read_int(os.path.join(cpuacct_dir, "cpuacct.usage"))
        if usage_ns is not None:
            cpu_usage = float(usage_ns)

        memory_mb: float | None = None
        mem_usage = self._read_int(os.path.join(mem_dir, "memory.usage_in_bytes"))
        if mem_usage is not None:
            memory_mb = float(mem_usage) / (1024 * 1024)

        memory_limit_mb: float | None = None
        mem_limit = self._read_int(os.path.join(mem_dir, "memory.limit_in_bytes"))
        if mem_limit is not None:
            # Ignore "unlimited" sentinel values.
            if mem_limit < (1 << 60):
                memory_limit_mb = float(mem_limit) / (1024 * 1024)

        if (
            cpu_quota_cores is None
            and cpu_usage is None
            and memory_mb is None
            and memory_limit_mb is None
        ):
            return None

        return {
            "cpu_quota_cores": cpu_quota_cores,
            "cpu_usage": cpu_usage,
            "cpu_usage_unit": "nsec",
            "memory_mb": memory_mb,
            "memory_limit_mb": memory_limit_mb,
        }

    def _sample_cgroup_cpu_percent(
        self,
        *,
        usage: float | None,
        usage_unit: str | None,
        cpu_cores: float | None,
    ) -> float | None:
        if usage is None or cpu_cores is None or cpu_cores <= 0:
            return None
        now_mono = time.monotonic()
        if self._cgroup_prev_usage is None or self._cgroup_prev_time_mono is None:
            self._cgroup_prev_usage = usage
            self._cgroup_prev_time_mono = now_mono
            return None
        delta_t = now_mono - self._cgroup_prev_time_mono
        delta_usage = usage - self._cgroup_prev_usage
        self._cgroup_prev_usage = usage
        self._cgroup_prev_time_mono = now_mono
        if delta_t <= 0 or delta_usage < 0:
            return None
        if usage_unit == "nsec":
            used_seconds = delta_usage / 1e9
        else:
            used_seconds = delta_usage / 1e6
        pct = (used_seconds / (delta_t * cpu_cores)) * 100.0
        if math.isfinite(pct):
            return pct
        return None

    def set_metrics_callback(self, callback: Callable[[Metrics], None]):
        """
        Set callback for real-time metrics updates.

        Args:
            callback: Function to call with metrics updates
        """
        self.metrics_callback = callback
