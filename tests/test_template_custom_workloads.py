import pytest


def test_templates_reject_non_custom_workload_type():
    from backend.api.routes import templates as templates_api

    with pytest.raises(ValueError, match="expected 'CUSTOM'"):
        templates_api._normalize_template_config({"workload_type": "MIXED"})


def test_templates_normalize_custom_requires_sum_100():
    from backend.api.routes import templates as templates_api

    cfg = {
        "workload_type": "CUSTOM",
        "custom_point_lookup_query": "SELECT 1",
        "custom_range_scan_query": "SELECT 1",
        "custom_insert_query": "INSERT INTO t VALUES (1)",
        "custom_update_query": "UPDATE t SET x=1",
        "custom_point_lookup_pct": 50,
        "custom_range_scan_pct": 0,
        "custom_insert_pct": 10,
        "custom_update_pct": 10,
    }
    with pytest.raises(ValueError, match="sum to 100"):
        templates_api._normalize_template_config(cfg)


def test_templates_normalize_custom_requires_sql_when_pct_gt_0():
    from backend.api.routes import templates as templates_api

    cfg = {
        "workload_type": "CUSTOM",
        "custom_point_lookup_query": "",
        "custom_range_scan_query": "SELECT 1",
        "custom_insert_query": "INSERT INTO t VALUES (1)",
        "custom_update_query": "UPDATE t SET x=1",
        "custom_point_lookup_pct": 100,
        "custom_range_scan_pct": 0,
        "custom_insert_pct": 0,
        "custom_update_pct": 0,
    }
    with pytest.raises(ValueError, match="custom_point_lookup_query"):
        templates_api._normalize_template_config(cfg)


def test_templates_normalize_qps_scaling_min_connections():
    from backend.api.routes import templates as templates_api

    cfg = {
        "workload_type": "CUSTOM",
        "custom_point_lookup_pct": 25,
        "custom_range_scan_pct": 25,
        "custom_insert_pct": 35,
        "custom_update_pct": 15,
        "load_mode": "QPS",
        "target_qps": 100,
        "concurrent_connections": 10,
        "scaling": {"min_connections": 2},
    }
    out = templates_api._normalize_template_config(cfg)
    assert out["scaling"]["min_connections"] == 2
    assert out["concurrent_connections"] == 10


def test_templates_rejects_min_concurrency():
    from backend.api.routes import templates as templates_api

    cfg = {
        "workload_type": "CUSTOM",
        "custom_point_lookup_pct": 25,
        "custom_range_scan_pct": 25,
        "custom_insert_pct": 35,
        "custom_update_pct": 15,
        "load_mode": "QPS",
        "target_qps": 10,
        "min_concurrency": 2,
    }
    with pytest.raises(ValueError, match="min_concurrency was renamed"):
        templates_api._normalize_template_config(cfg)


def test_custom_schedule_exact_counts():
    from backend.core.test_executor import TestExecutor

    weights = {"POINT_LOOKUP": 25, "RANGE_SCAN": 25, "INSERT": 35, "UPDATE": 15}
    schedule = TestExecutor._build_smooth_weighted_schedule(weights)
    assert len(schedule) == 100
    for k, w in weights.items():
        assert schedule.count(k) == w


@pytest.mark.asyncio
async def test_failure_records_preserve_query_text_and_kind():
    """
    Regression: failures must persist real SQL + correct query_kind so UI can later
    display true error reasons by operation type.
    """
    from backend.core.test_executor import TestExecutor
    from backend.models import TableConfig, TableType, TestScenario, WorkloadType

    class StubPool:
        warehouse = "TEST_WH"

        async def execute_query(self, *args, **kwargs):  # noqa: ANN001, D401
            raise RuntimeError("boom")

        async def execute_query_with_info(self, *args, **kwargs):  # noqa: ANN001, D401
            raise RuntimeError("boom")

    class StubManager:
        def __init__(self):
            self.config = TableConfig(
                name="T1",
                table_type=TableType.STANDARD,
                columns={
                    "id": "NUMBER",
                    "data": "VARCHAR",
                    "timestamp": "TIMESTAMP_NTZ",
                },
                database="DB",
                schema_name="SCHEMA",
            )
            self.pool = StubPool()

        def get_full_table_name(self) -> str:  # noqa: D401
            return "DB.SCHEMA.T1"

    scenario = TestScenario(
        name="failure-records",
        duration_seconds=1,
        concurrent_connections=1,
        workload_type=WorkloadType.MIXED,
        table_configs=[
            TableConfig(
                name="T1",
                table_type=TableType.STANDARD,
                columns={
                    "id": "NUMBER",
                    "data": "VARCHAR",
                    "timestamp": "TIMESTAMP_NTZ",
                },
                database="DB",
                schema_name="SCHEMA",
            )
        ],
        collect_query_history=True,
    )

    ex = TestExecutor(scenario)
    ex.table_managers = [StubManager()]  # type: ignore[assignment]

    await ex._execute_read(worker_id=0, warmup=False)
    await ex._execute_write(worker_id=0, warmup=False)

    recs = list(ex.get_query_execution_records())
    assert recs, "expected query execution records"

    read_fail = next(
        r
        for r in recs
        if not r.success and r.query_kind in {"RANGE_SCAN", "POINT_LOOKUP"}
    )
    assert read_fail.query_text != "READ_FAILED"
    assert "SELECT" in read_fail.query_text.upper()

    write_fail = next(
        r for r in recs if not r.success and r.query_kind in {"INSERT", "UPDATE"}
    )
    assert write_fail.query_text != "WRITE_FAILED"
    assert any(k in write_fail.query_text.upper() for k in ("INSERT", "UPDATE"))


@pytest.mark.asyncio
async def test_sql_error_categories_aggregate_lock_waiters():
    from backend.core.test_executor import TestExecutor
    from backend.models import TableConfig, TableType, TestScenario, WorkloadType

    class StubPool:
        warehouse = "TEST_WH"

        async def execute_query(self, *args, **kwargs):  # noqa: ANN001, D401
            raise RuntimeError(
                "000625 (57014): statement aborted because the number of waiters for this lock exceeds the 20 statements limit."
            )

        async def execute_query_with_info(self, *args, **kwargs):  # noqa: ANN001, D401
            raise RuntimeError(
                "000625 (57014): statement aborted because the number of waiters for this lock exceeds the 20 statements limit."
            )

    class StubManager:
        def __init__(self):
            self.config = TableConfig(
                name="T1",
                table_type=TableType.STANDARD,
                columns={
                    "id": "NUMBER",
                    "data": "VARCHAR",
                    "timestamp": "TIMESTAMP_NTZ",
                },
                database="DB",
                schema_name="SCHEMA",
            )
            self.pool = StubPool()

        def get_full_table_name(self) -> str:  # noqa: D401
            return "DB.SCHEMA.T1"

    scenario = TestScenario(
        name="sql-error-categories",
        duration_seconds=1,
        concurrent_connections=1,
        workload_type=WorkloadType.MIXED,
        table_configs=[
            TableConfig(
                name="T1",
                table_type=TableType.STANDARD,
                columns={
                    "id": "NUMBER",
                    "data": "VARCHAR",
                    "timestamp": "TIMESTAMP_NTZ",
                },
                database="DB",
                schema_name="SCHEMA",
            )
        ],
        collect_query_history=False,
    )

    ex = TestExecutor(scenario)
    ex.table_managers = [StubManager()]  # type: ignore[assignment]

    await ex._execute_read(worker_id=0, warmup=False)
    await ex._execute_write(worker_id=0, warmup=False)

    snap = ex.get_sql_error_category_snapshot()
    cats = snap.get("categories") or {}
    assert cats.get("SF_LOCK_WAITER_LIMIT") == 2


@pytest.mark.asyncio
async def test_custom_value_pools_skip_profiling_sets_profile_for_param_binding():
    """
    Regression: when TEMPLATE_VALUE_POOLS exist we skip expensive profiling, but CUSTOM workloads
    still need id/time column names to bind params. Missing profile fields used to cause
    "Cannot choose key value" / "no time column detected" errors and massive error counts.
    """
    from backend.core.test_executor import TestExecutor
    from backend.models import TableConfig, TableType, TestScenario, WorkloadType

    class StubPool:
        warehouse = "TEST_WH"

        async def execute_query_with_info(self, *args, **kwargs):  # noqa: ANN001, D401
            return [], {"query_id": "Q1", "rowcount": 0}

    class StubManager:
        def __init__(self):
            self.config = TableConfig(
                name="T1",
                table_type=TableType.STANDARD,
                columns={
                    "ID": "NUMBER",
                    "TS": "TIMESTAMP_NTZ",
                    "VAL": "VARCHAR",
                },
                database="DB",
                schema_name="SCHEMA",
            )
            self.pool = StubPool()

        def get_full_table_name(self) -> str:  # noqa: D401
            return "DB.SCHEMA.T1"

    scenario = TestScenario(
        name="custom-pools",
        duration_seconds=1,
        warmup_seconds=0,
        concurrent_connections=1,
        workload_type=WorkloadType.CUSTOM,
        custom_queries=[
            {
                "query_kind": "POINT_LOOKUP",
                "weight_pct": 50,
                "sql": 'SELECT * FROM {table} WHERE "ID" = ?',
            },
            {
                "query_kind": "RANGE_SCAN",
                "weight_pct": 50,
                "sql": 'SELECT * FROM {table} WHERE "TS" >= ? LIMIT 100',
            },
        ],
        table_configs=[
            TableConfig(
                name="T1",
                table_type=TableType.STANDARD,
                columns={
                    "ID": "NUMBER",
                    "TS": "TIMESTAMP_NTZ",
                    "VAL": "VARCHAR",
                },
                database="DB",
                schema_name="SCHEMA",
            )
        ],
        collect_query_history=False,
    )

    ex = TestExecutor(scenario)
    ex.table_managers = [StubManager()]  # type: ignore[assignment]

    async def _fake_load_value_pools() -> None:
        ex._value_pools = {
            "KEY": {"ID": [1, 2, 3]},
            "RANGE": {"TS": ["2026-01-01T00:00:00"]},
        }
        ex._ai_workload = {"key_column": "ID", "time_column": "TS"}

    ex._load_value_pools = _fake_load_value_pools  # type: ignore[method-assign]

    await ex._profile_tables()
    state = ex._table_state["DB.SCHEMA.T1"]
    assert state.profile is not None
    assert state.profile.id_column == "ID"
    assert state.profile.time_column == "TS"

    # Should be able to bind both custom read shapes without raising parameter-selection errors.
    await ex._execute_custom(worker_id=0, warmup=False)
    await ex._execute_custom(worker_id=0, warmup=False)


@pytest.mark.asyncio
async def test_custom_param_binding_falls_back_to_row_pool_when_key_or_range_pools_missing():
    """
    Robustness: if only a ROW pool exists (no KEY/RANGE pools), CUSTOM param binding should
    still be able to choose key/time values from sampled rows instead of erroring.
    """
    from backend.core.test_executor import TestExecutor
    from backend.models import TableConfig, TableType, TestScenario, WorkloadType

    class StubPool:
        warehouse = "TEST_WH"

        async def execute_query_with_info(self, *args, **kwargs):  # noqa: ANN001, D401
            return [], {"query_id": "Q1", "rowcount": 0}

    class StubManager:
        def __init__(self):
            self.config = TableConfig(
                name="T1",
                table_type=TableType.STANDARD,
                columns={
                    "ID": "NUMBER",
                    "TS": "TIMESTAMP_NTZ",
                    "VAL": "VARCHAR",
                },
                database="DB",
                schema_name="SCHEMA",
            )
            self.pool = StubPool()

        def get_full_table_name(self) -> str:  # noqa: D401
            return "DB.SCHEMA.T1"

    scenario = TestScenario(
        name="custom-row-pool-fallback",
        duration_seconds=1,
        warmup_seconds=0,
        concurrent_connections=1,
        workload_type=WorkloadType.CUSTOM,
        custom_queries=[
            {
                "query_kind": "POINT_LOOKUP",
                "weight_pct": 50,
                "sql": 'SELECT * FROM {table} WHERE "ID" = ?',
            },
            {
                "query_kind": "RANGE_SCAN",
                "weight_pct": 50,
                "sql": 'SELECT * FROM {table} WHERE "TS" >= ? LIMIT 100',
            },
        ],
        table_configs=[
            TableConfig(
                name="T1",
                table_type=TableType.STANDARD,
                columns={
                    "ID": "NUMBER",
                    "TS": "TIMESTAMP_NTZ",
                    "VAL": "VARCHAR",
                },
                database="DB",
                schema_name="SCHEMA",
            )
        ],
        collect_query_history=False,
    )

    ex = TestExecutor(scenario)
    ex.table_managers = [StubManager()]  # type: ignore[assignment]

    async def _fake_load_value_pools() -> None:
        ex._value_pools = {
            "ROW": {
                None: [
                    {
                        "ID": 123,
                        "TS": "2026-01-01T00:00:00",
                        "VAL": "X",
                    }
                ]
            }
        }
        ex._ai_workload = {"key_column": "ID", "time_column": "TS"}

    ex._load_value_pools = _fake_load_value_pools  # type: ignore[method-assign]

    await ex._profile_tables()
    await ex._execute_custom(worker_id=0, warmup=False)
    await ex._execute_custom(worker_id=0, warmup=False)
