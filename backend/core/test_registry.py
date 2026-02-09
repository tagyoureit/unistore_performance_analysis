"""
Test Registry

Template CRUD and scenario construction utilities.

NOTE: Execution logic has been removed. All test runs now go through
OrchestratorService which writes to RUN_STATUS and coordinates workers.
This module is retained for:
- Template loading (`_load_template`)
- Scenario construction (`_scenario_from_template_config`)
- Warehouse config extraction (`_warehouse_from_config`)
"""

from __future__ import annotations

import json
import logging
from typing import Any, Optional

from backend.config import settings
from backend.connectors import snowflake_pool
from backend.models.test_config import (
    TableConfig,
    TableType,
    TestScenario,
    WorkloadType,
)

logger = logging.getLogger(__name__)


def _default_columns() -> dict[str, str]:
    # Minimal schema aligned with the app's query patterns.
    return {
        "id": "NUMBER",
        "data": "VARCHAR(255)",
        "timestamp": "TIMESTAMP_NTZ",
    }


def _parse_csv(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    return [v.strip() for v in str(value).split(",") if v.strip()]


def _workload_type(value: Any) -> WorkloadType:
    """
    Parse workload type from config.
    
    NOTE: All templates are now normalized to CUSTOM during save.
    This function exists for backward compatibility but always returns CUSTOM.
    """
    return WorkloadType.CUSTOM


def _table_type(value: Any) -> TableType:
    if not value:
        return TableType.STANDARD
    raw = str(value).strip().lower()
    return TableType(raw)


class TestRegistry:
    """
    Registry for test templates and scenario construction.

    NOTE: Execution methods (start_from_template, start_prepared, stop,
    _run_and_persist) have been removed. All runs now go through
    OrchestratorService.
    """

    def __init__(self) -> None:
        # No longer tracking in-memory tests - all runs use RUN_STATUS table
        pass

    async def shutdown(self, *, timeout_seconds: float = 5.0) -> None:
        """
        Shutdown hook for graceful termination.
        No-op since execution is now handled by OrchestratorService.
        """
        pass

    async def get(self, test_id: str) -> None:
        """
        Legacy method for backward compatibility.
        Always returns None since tests are no longer tracked in-memory.
        Use RUN_STATUS table queries instead.
        """
        return None

    async def _load_template(self, template_id: str) -> dict[str, Any]:
        pool = snowflake_pool.get_default_pool()
        query = f"""
        SELECT TEMPLATE_ID, TEMPLATE_NAME, CONFIG, TAGS
        FROM {settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}.TEST_TEMPLATES
        WHERE TEMPLATE_ID = ?
        """
        rows = await pool.execute_query(query, params=[template_id])
        if not rows:
            raise KeyError(template_id)
        _, name, config, tags = rows[0]
        if isinstance(config, str):
            config = json.loads(config)
        if isinstance(tags, str):
            tags = json.loads(tags)
        return {
            "template_id": template_id,
            "template_name": name,
            "config": config,
            "tags": tags or {},
        }

    def _warehouse_from_config(self, cfg: dict[str, Any]) -> Optional[str]:
        table_type = _table_type(cfg.get("table_type") or "STANDARD")
        if table_type == TableType.POSTGRES:
            return None

        warehouse = str(cfg.get("warehouse_name") or "").strip()
        if not warehouse:
            raise ValueError(
                "warehouse_name is required (select an existing warehouse)"
            )
        return warehouse

    def _scenario_from_template_config(
        self, template_name: str, cfg: dict[str, Any]
    ) -> TestScenario:
        table_name = str(cfg.get("table_name") or "").strip()
        db = str(cfg.get("database") or "").strip()
        schema = str(cfg.get("schema") or "").strip()
        if not table_name:
            raise ValueError("table_name is required (select an existing table/view)")
        if not db:
            raise ValueError("database is required (select an existing database)")
        if not schema:
            raise ValueError("schema is required (select an existing schema)")

        table_type = _table_type(cfg.get("table_type") or "STANDARD")
        columns_raw = cfg.get("columns")
        columns = columns_raw if isinstance(columns_raw, dict) else {}
        connection_id = cfg.get("connection_id")  # For Postgres stored connections

        table_config = TableConfig(
            name=table_name,
            table_type=table_type,
            database=db,
            schema_name=schema,
            columns=columns,
            connection_id=connection_id,
            clustering_keys=None,
            primary_key=None,
            indexes=None,
            initial_row_count=int(cfg.get("initial_row_count") or 0),
        )

        workload_type = _workload_type(cfg.get("workload_type"))
        custom_queries: list[dict[str, Any]] | None = None
        if workload_type == WorkloadType.CUSTOM:
            # Templates persist the canonical 4-query workload as explicit CUSTOM weights + SQL.
            def _pct(key: str) -> int:
                return int(cfg.get(key) or 0)

            def _sql(key: str) -> str:
                return str(cfg.get(key) or "").strip()

            pct_fields = (
                "custom_point_lookup_pct",
                "custom_range_scan_pct",
                "custom_insert_pct",
                "custom_update_pct",
            )
            total = sum(_pct(k) for k in pct_fields)
            logger.info(
                "Building custom_queries: workload_type=%s, pct_total=%d, pcts=%s",
                workload_type,
                total,
                {k: _pct(k) for k in pct_fields},
            )
            if total != 100:
                raise ValueError(
                    f"Template CUSTOM percentages must sum to 100 (currently {total})."
                )

            items = [
                (
                    "POINT_LOOKUP",
                    "custom_point_lookup_pct",
                    "custom_point_lookup_query",
                ),
                ("RANGE_SCAN", "custom_range_scan_pct", "custom_range_scan_query"),
                ("INSERT", "custom_insert_pct", "custom_insert_query"),
                ("UPDATE", "custom_update_pct", "custom_update_query"),
            ]
            custom_queries = []
            for kind, pct_k, sql_k in items:
                pct = _pct(pct_k)
                sql = _sql(sql_k)
                if pct <= 0:
                    continue
                if not sql:
                    raise ValueError(f"{sql_k} is required when {pct_k} > 0")
                custom_queries.append(
                    {"query_kind": kind, "weight_pct": pct, "sql": sql}
                )

        load_mode = (
            str(cfg.get("load_mode") or "CONCURRENCY").strip().upper() or "CONCURRENCY"
        )
        scaling_cfg = cfg.get("scaling")
        if not isinstance(scaling_cfg, dict):
            scaling_cfg = {}
        target_qps_raw = cfg.get("target_qps")
        target_qps = float(target_qps_raw) if target_qps_raw is not None else None
        # Support both new and old field names for backwards compatibility
        starting_threads_raw = cfg.get("starting_threads") or cfg.get("starting_qps")
        starting_threads = float(starting_threads_raw) if starting_threads_raw is not None else None
        max_thread_increase_raw = cfg.get("max_thread_increase") or cfg.get("max_qps_increase")
        max_thread_increase = float(max_thread_increase_raw) if max_thread_increase_raw is not None else None

        # QPS mode supports concurrent_connections=-1 in the *template config* to mean
        # "no user cap". For runtime execution we must choose an effective cap bounded
        # by engine limits (Option A).
        raw_cc = cfg.get("concurrent_connections")
        cc = int(raw_cc) if raw_cc is not None else int(settings.DEFAULT_CONCURRENCY)
        if load_mode == "QPS" and cc == -1:
            if table_type == TableType.POSTGRES:
                cc = int(settings.POSTGRES_POOL_MAX_SIZE)
            else:
                cc = int(settings.SNOWFLAKE_BENCHMARK_EXECUTOR_MAX_WORKERS)
        max_connections = scaling_cfg.get("max_connections")
        if load_mode == "QPS" and max_connections is not None:
            try:
                max_connections_value = int(max_connections)
            except Exception:
                max_connections_value = None
            if max_connections_value and max_connections_value > 0:
                if cc == -1:
                    cc = max_connections_value
                else:
                    cc = min(cc, max_connections_value)

        # FIND_MAX_CONCURRENCY mode settings
        start_concurrency = int(cfg.get("start_concurrency") or 5)
        concurrency_increment = int(cfg.get("concurrency_increment") or 10)
        step_duration_seconds = int(cfg.get("step_duration_seconds") or 30)
        qps_stability_pct = float(cfg.get("qps_stability_pct") or 5.0)
        latency_stability_pct = float(cfg.get("latency_stability_pct") or 20.0)
        max_error_rate_pct = float(cfg.get("max_error_rate_pct") or 1.0)

        scenario = TestScenario(
            name=template_name,
            description=str(cfg.get("description") or ""),
            duration_seconds=int(cfg.get("duration") or settings.DEFAULT_TEST_DURATION),
            warmup_seconds=int(cfg.get("warmup") or 0),
            concurrent_connections=int(cc),
            load_mode=load_mode,
            target_qps=target_qps,
            starting_threads=starting_threads,
            max_thread_increase=max_thread_increase,
            min_connections=int(scaling_cfg.get("min_connections") or 1),
            start_concurrency=start_concurrency,
            concurrency_increment=concurrency_increment,
            step_duration_seconds=step_duration_seconds,
            qps_stability_pct=qps_stability_pct,
            latency_stability_pct=latency_stability_pct,
            max_error_rate_pct=max_error_rate_pct,
            think_time_ms=int(cfg.get("think_time") or 0),
            workload_type=workload_type,
            custom_queries=custom_queries,
            table_configs=[table_config],
        )

        # Collect per-operation query history for template runs so we can:
        # - compute per-query-type latencies
        # - enrich from Snowflake QUERY_HISTORY post-run
        # Warmup operations are also captured (flagged) for troubleshooting.
        scenario.collect_query_history = True

        return scenario


registry = TestRegistry()
