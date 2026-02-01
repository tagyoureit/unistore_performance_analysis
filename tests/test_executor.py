#!/usr/bin/env python3
"""
Test script for TestExecutor.

Tests test orchestration and workload generation (without DB connections).
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.models import (
    TableConfig,
    TableType,
    TestScenario,
    WorkloadType,
    WarehouseConfig,
    WarehouseSize,
)
from backend.core.test_executor import TestExecutor


def test_executor_creation():
    """Test TestExecutor initialization."""
    print("\n🔍 Testing TestExecutor Creation")
    print("=" * 60)

    try:
        # Create test scenario
        table = TableConfig(
            name="test_table",
            table_type=TableType.STANDARD,
            columns={
                "id": "NUMBER",
                "date": "DATE",
                "value": "VARCHAR(100)",
            },
        )

        warehouse = WarehouseConfig(
            name="TEST_WH",
            size=WarehouseSize.SMALL,
        )

        scenario = TestScenario(
            name="read_heavy_test",
            description="80/20 read/write test",
            duration_seconds=10,
            warmup_seconds=2,
            concurrent_connections=5,
            workload_type=WorkloadType.READ_HEAVY,
            read_batch_size=100,
            write_batch_size=10,
            metrics_interval_seconds=1.0,
            table_configs=[table],
            warehouse_configs=[warehouse],
        )

        # Create executor
        executor = TestExecutor(scenario)

        print("✅ TestExecutor created")
        print(f"   Scenario: {executor.scenario.name}")
        print(f"   Duration: {executor.scenario.duration_seconds}s")
        print(f"   Connections: {executor.scenario.total_threads}")
        print(f"   Workload: {executor.scenario.workload_type}")
        print(f"   Test ID: {executor.test_id}")

        # Verify state
        assert executor.status.value == "pending"
        assert len(executor.table_managers) == 0  # Not setup yet
        assert len(executor.workers) == 0

        print("✅ Initial state verified")

        return True

    except Exception as e:
        print(f"❌ Executor creation test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_workload_types():
    """Test different workload type configurations."""
    print("\n🔍 Testing Workload Types")
    print("=" * 60)

    try:
        table = TableConfig(
            name="test_table",
            table_type=TableType.STANDARD,
            columns={"id": "NUMBER", "value": "VARCHAR"},
        )

        workload_types = [
            WorkloadType.READ_ONLY,
            WorkloadType.WRITE_ONLY,
            WorkloadType.READ_HEAVY,
            WorkloadType.WRITE_HEAVY,
            WorkloadType.MIXED,
        ]

        for workload in workload_types:
            scenario = TestScenario(
                name=f"test_{workload.value}",
                duration_seconds=5,
                concurrent_connections=3,
                workload_type=workload,
                table_configs=[table],
            )

            TestExecutor(scenario)
            print(f"✅ Created executor for {workload.value} workload")

        # Custom workload (requires custom_queries)
        scenario = TestScenario(
            name="custom_test",
            duration_seconds=5,
            concurrent_connections=3,
            workload_type=WorkloadType.CUSTOM,
            custom_queries=[
                {
                    "query_kind": "POINT_LOOKUP",
                    "weight_pct": 70,
                    "sql": "SELECT * FROM {table} WHERE id = ?",
                },
                {
                    "query_kind": "INSERT",
                    "weight_pct": 30,
                    "sql": "INSERT INTO {table} (id, value) VALUES (?, ?)",
                },
            ],
            table_configs=[table],
        )

        TestExecutor(scenario)
        print("✅ Created executor for custom workload")

        return True

    except Exception as e:
        print(f"❌ Workload types test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_multi_table_scenario():
    """Test scenario with multiple tables."""
    print("\n🔍 Testing Multi-Table Scenario")
    print("=" * 60)

    try:
        # Standard table
        standard = TableConfig(
            name="events",
            table_type=TableType.STANDARD,
            columns={"id": "NUMBER", "date": "DATE", "type": "VARCHAR"},
        )

        # Hybrid table
        hybrid = TableConfig(
            name="users",
            table_type=TableType.HYBRID,
            columns={"id": "NUMBER", "email": "VARCHAR", "created": "DATE"},
        )

        scenario = TestScenario(
            name="multi_table_test",
            duration_seconds=10,
            concurrent_connections=10,
            workload_type=WorkloadType.MIXED,
            table_configs=[standard, hybrid],
        )

        TestExecutor(scenario)

        print("✅ Multi-table executor created")
        print(f"   Tables: {len(scenario.table_configs)}")
        print(f"   - {standard.name} ({standard.table_type})")
        print(f"   - {hybrid.name} ({hybrid.table_type})")

        return True

    except Exception as e:
        print(f"❌ Multi-table test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_rate_limiting():
    """Test rate limiting configuration."""
    print("\n🔍 Testing Rate Limiting")
    print("=" * 60)

    try:
        table = TableConfig(
            name="test_table",
            table_type=TableType.STANDARD,
            columns={"id": "NUMBER"},
        )

        # With rate limiting
        scenario = TestScenario(
            name="rate_limited_test",
            duration_seconds=5,
            concurrent_connections=5,
            workload_type=WorkloadType.READ_ONLY,
            target_qps=100,
            think_time_ms=50,
            table_configs=[table],
        )

        TestExecutor(scenario)

        print("✅ Rate-limited executor created")
        print(f"   Target: {scenario.target_qps} QPS")
        print(f"   Think time: {scenario.think_time_ms}ms")

        # With operation limit
        scenario2 = TestScenario(
            name="limited_ops_test",
            duration_seconds=60,
            concurrent_connections=5,
            operations_per_connection=100,
            workload_type=WorkloadType.WRITE_ONLY,
            table_configs=[table],
        )

        TestExecutor(scenario2)

        print("✅ Operation-limited executor created")
        print(f"   Max ops/connection: {scenario2.operations_per_connection}")

        return True

    except Exception as e:
        print(f"❌ Rate limiting test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_metrics_callback():
    """Test metrics callback setup."""
    print("\n🔍 Testing Metrics Callback")
    print("=" * 60)

    try:
        table = TableConfig(
            name="test_table",
            table_type=TableType.STANDARD,
            columns={"id": "NUMBER"},
        )

        scenario = TestScenario(
            name="metrics_test",
            duration_seconds=5,
            concurrent_connections=5,
            workload_type=WorkloadType.READ_ONLY,
            metrics_interval_seconds=0.5,
            table_configs=[table],
        )

        executor = TestExecutor(scenario)

        # Set callback
        callback_called = []

        def metrics_callback(metrics):
            callback_called.append(metrics.total_operations)

        executor.set_metrics_callback(metrics_callback)

        print("✅ Metrics callback configured")
        print(f"   Interval: {scenario.metrics_interval_seconds}s")

        assert executor.metrics_callback is not None

        return True

    except Exception as e:
        print(f"❌ Metrics callback test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_worker_group_sharding_offsets_pool_indices():
    table = TableConfig(
        name="test_table",
        table_type=TableType.STANDARD,
        columns={"id": "NUMBER"},
    )
    scenario0 = TestScenario(
        name="sharding_test",
        description="Worker group sharding offset test",
        duration_seconds=5,
        warmup_seconds=1,
        concurrent_connections=2,
        workload_type=WorkloadType.READ_HEAVY,
        read_batch_size=10,
        write_batch_size=1,
        metrics_interval_seconds=1.0,
        table_configs=[table],
        worker_group_id=0,
        worker_group_count=2,
    )
    scenario1 = TestScenario(
        name="sharding_test",
        description="Worker group sharding offset test",
        duration_seconds=5,
        warmup_seconds=1,
        concurrent_connections=2,
        workload_type=WorkloadType.READ_HEAVY,
        read_batch_size=10,
        write_batch_size=1,
        metrics_interval_seconds=1.0,
        table_configs=[table],
        worker_group_id=1,
        worker_group_count=2,
    )

    executor0 = TestExecutor(scenario0)
    executor1 = TestExecutor(scenario1)

    pool = list(range(10))
    executor0._value_pools = {"POINT_LOOKUP": {"ID": pool}}
    executor1._value_pools = {"POINT_LOOKUP": {"ID": pool}}

    first0 = executor0._next_from_pool(0, "POINT_LOOKUP", "ID")
    first1 = executor1._next_from_pool(0, "POINT_LOOKUP", "ID")

    assert first0 == pool[0]
    assert first1 == pool[2]


def main():
    """Run all test executor tests."""
    print("=" * 60)
    print("🧪 Running Test Executor Tests")
    print("=" * 60)
    print("\nThese tests validate:")
    print("- TestExecutor initialization")
    print("- Workload type configuration")
    print("- Multi-table scenarios")
    print("- Rate limiting and operation limits")
    print("- Metrics callback setup")
    print("\nNOTE: These tests do NOT require database connections.")
    print("Full integration tests require valid credentials.")

    tests = [
        ("Executor Creation", test_executor_creation),
        ("Workload Types", test_workload_types),
        ("Multi-Table Scenario", test_multi_table_scenario),
        ("Rate Limiting", test_rate_limiting),
        ("Metrics Callback", test_metrics_callback),
    ]

    results = []
    for name, test_func in tests:
        result = test_func()
        results.append((name, result))

    print("\n" + "=" * 60)
    print("📊 Test Results Summary")
    print("=" * 60)

    passed = sum(1 for _, result in results if result)
    total = len(results)

    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status} - {name}")

    print()
    print(f"Total: {passed}/{total} tests passed")

    if passed == total:
        print("\n🎉 All tests passed!")
        return 0
    else:
        print(f"\n⚠️  {total - passed} test(s) failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())
