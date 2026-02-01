#!/usr/bin/env python3
"""
Test script for Pydantic data models.

Validates model creation, validation, and serialization.
"""

import sys
from pathlib import Path
from datetime import datetime

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.models import (
    TableType,
    TableConfig,
    WarehouseSize,
    ScalingPolicy,
    WarehouseConfig,
    WorkloadType,
    TestScenario,
    TestStatus,
    TestResult,
    TestRun,
    Metrics,
    MetricsSnapshot,
    LatencyPercentiles,
    OperationMetrics,
)


def test_table_config():
    """Test TableConfig model."""
    print("\n🔍 Testing TableConfig")
    print("=" * 60)

    try:
        # Standard table
        standard_table = TableConfig(
            name="test_standard",
            table_type=TableType.STANDARD,
            columns={
                "id": "NUMBER",
                "date": "DATE",
                "customer_id": "VARCHAR",
                "amount": "DECIMAL(10,2)",
            },
            initial_row_count=1000000,
        )
        print(f"✅ Standard table created: {standard_table.name}")

        # Hybrid table (existing table; primary key is not required by the app)
        hybrid_table = TableConfig(
            name="test_hybrid",
            table_type=TableType.HYBRID,
            columns={
                "id": "NUMBER",
                "customer_id": "VARCHAR",
                "date": "DATE",
                "amount": "DECIMAL(10,2)",
            },
            initial_row_count=100000,
        )
        print(f"✅ Hybrid table created: {hybrid_table.name}")

        # Interactive table (requires CLUSTER BY)
        interactive_table = TableConfig(
            name="test_interactive",
            table_type=TableType.INTERACTIVE,
            cluster_by=["date", "region"],
            columns={
                "id": "NUMBER",
                "date": "DATE",
                "region": "VARCHAR",
                "value": "DECIMAL",
            },
            cache_warming_enabled=True,
        )
        print(f"✅ Interactive table created: {interactive_table.name}")

        # Hybrid without PK should still validate (table creation is disabled)
        try:
            TableConfig(
                name="bad_hybrid",
                table_type=TableType.HYBRID,
                columns={"id": "NUMBER"},
            )
            print("✅ Hybrid config without PK validated (expected)")
        except ValueError as e:
            print(f"❌ Unexpected validation error: {e}")
            return False

        return True

    except Exception as e:
        print(f"❌ TableConfig test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_warehouse_config():
    """Test WarehouseConfig model."""
    print("\n🔍 Testing WarehouseConfig")
    print("=" * 60)

    try:
        # Single-cluster warehouse
        small_wh = WarehouseConfig(
            name="TEST_SMALL_WH",
            size=WarehouseSize.SMALL,
            auto_suspend_seconds=300,
            auto_resume=True,
        )
        print(f"✅ Single-cluster warehouse: {small_wh.name} ({small_wh.size})")

        # Multi-cluster warehouse
        large_wh = WarehouseConfig(
            name="TEST_LARGE_WH",
            size=WarehouseSize.LARGE,
            min_cluster_count=2,
            max_cluster_count=5,
            scaling_policy=ScalingPolicy.ECONOMY,
        )
        print(
            f"✅ Multi-cluster warehouse: {large_wh.name} ({large_wh.min_cluster_count}-{large_wh.max_cluster_count})"
        )

        # Test validation (max < min should fail)
        try:
            WarehouseConfig(
                name="BAD_WH",
                size=WarehouseSize.MEDIUM,
                min_cluster_count=5,
                max_cluster_count=2,
            )
            print("❌ Should have failed: max < min clusters")
            return False
        except ValueError as e:
            print(f"✅ Validation works: {e}")

        return True

    except Exception as e:
        print(f"❌ WarehouseConfig test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_test_scenario():
    """Test TestScenario model."""
    print("\n🔍 Testing TestScenario")
    print("=" * 60)

    try:
        # Read-heavy workload
        table = TableConfig(
            name="test_table",
            table_type=TableType.STANDARD,
            columns={"id": "NUMBER", "value": "VARCHAR"},
        )

        warehouse = WarehouseConfig(
            name="TEST_WH",
            size=WarehouseSize.MEDIUM,
        )

        scenario = TestScenario(
            name="read_heavy_test",
            description="80/20 read/write test",
            duration_seconds=120,
            warmup_seconds=10,
            concurrent_connections=50,
            workload_type=WorkloadType.READ_HEAVY,
            read_batch_size=100,
            write_batch_size=10,
            metrics_interval_seconds=1.0,
            table_configs=[table],
            warehouse_configs=[warehouse],
            tags={"test_type": "performance", "environment": "dev"},
        )
        print(f"✅ Test scenario created: {scenario.name}")
        print(f"   Duration: {scenario.duration_seconds}s")
        print(f"   Connections: {scenario.total_threads}")
        print(f"   Workload: {scenario.workload_type}")

        # Custom query scenario
        custom_scenario = TestScenario(
            name="custom_queries",
            duration_seconds=60,
            concurrent_connections=10,
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
        print(f"✅ Custom scenario created: {custom_scenario.name}")

        # Test validation (CUSTOM without queries should fail)
        try:
            TestScenario(
                name="bad_custom",
                duration_seconds=60,
                concurrent_connections=10,
                workload_type=WorkloadType.CUSTOM,
                table_configs=[table],
            )
            print("❌ Should have failed: CUSTOM without queries")
            return False
        except ValueError as e:
            print(f"✅ Validation works: {e}")

        return True

    except Exception as e:
        print(f"❌ TestScenario test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_test_result():
    """Test TestResult model."""
    print("\n🔍 Testing TestResult")
    print("=" * 60)

    try:
        result = TestResult(
            test_name="performance_test_1",
            scenario_name="read_heavy_test",
            table_name="test_standard",
            table_type="standard",
            warehouse="TEST_WH",
            warehouse_size="Medium",
            status=TestStatus.COMPLETED,
            start_time=datetime.now(),
            end_time=datetime.now(),
            duration_seconds=120.5,
            concurrent_connections=50,
            total_operations=12000,
            read_operations=9600,
            write_operations=2400,
            qps=99.6,
            avg_latency_ms=15.2,
            p95_latency_ms=45.3,
            p99_latency_ms=78.9,
            bytes_read=1024000,
            bytes_written=256000,
        )

        print(f"✅ Test result created: {result.test_name}")
        print(f"   Test ID: {result.test_id}")
        print(f"   Status: {result.status}")
        print(f"   Operations: {result.total_operations}")
        print(f"   QPS: {result.qps:.2f}")
        print(f"   P95 latency: {result.p95_latency_ms:.2f}ms")

        # Test JSON serialization
        json_data = result.model_dump_json()
        print(f"✅ JSON serialization works ({len(json_data)} bytes)")

        return True

    except Exception as e:
        print(f"❌ TestResult test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_test_run():
    """Test TestRun model."""
    print("\n🔍 Testing TestRun")
    print("=" * 60)

    try:
        run = TestRun(
            run_name="comparison_run_1",
            description="Compare standard vs hybrid tables",
            status=TestStatus.RUNNING,
            start_time=datetime.now(),
            snowflake_account="myaccount.us-east-1",
            client_version="0.1.0",
        )

        # Add test results
        result1 = TestResult(
            test_name="standard_table_test",
            scenario_name="read_heavy",
            table_name="test_standard",
            table_type="standard",
            status=TestStatus.COMPLETED,
            start_time=datetime.now(),
            concurrent_connections=50,
            qps=120.5,
        )

        result2 = TestResult(
            test_name="hybrid_table_test",
            scenario_name="read_heavy",
            table_name="test_hybrid",
            table_type="hybrid",
            status=TestStatus.COMPLETED,
            start_time=datetime.now(),
            concurrent_connections=50,
            qps=145.8,
        )

        run.add_test_result(result1)
        run.add_test_result(result2)
        run.calculate_summary()

        print(f"✅ Test run created: {run.run_name}")
        print(f"   Run ID: {run.run_id}")
        print(f"   Total tests: {run.total_tests}")
        print(f"   Successful: {run.successful_tests}")

        return True

    except Exception as e:
        print(f"❌ TestRun test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_metrics():
    """Test Metrics and MetricsSnapshot models."""
    print("\n🔍 Testing Metrics")
    print("=" * 60)

    try:
        metrics = Metrics(
            total_operations=1000,
            successful_operations=990,
            failed_operations=10,
            read_metrics=OperationMetrics(
                count=800,
                success_count=790,
                error_count=10,
                total_duration_ms=40000.0,
                latency=LatencyPercentiles(p50=40.0, p95=80.0, p99=150.0, avg=50.0),
            ),
            write_metrics=OperationMetrics(
                count=200,
                success_count=200,
                error_count=0,
                total_duration_ms=10000.0,
                latency=LatencyPercentiles(p50=30.0, p95=60.0, p99=90.0, avg=40.0),
            ),
            overall_latency=LatencyPercentiles(p50=40.0, p95=80.0, p99=150.0, avg=50.0),
        )
        print(f"✅ Metrics created: {metrics.total_operations} operations")
        print(f"   Average latency: {metrics.overall_latency.avg:.2f}ms")
        print(f"   Success rate: {metrics.success_rate * 100:.1f}%")

        # Test snapshot
        MetricsSnapshot.from_metrics(metrics)
        print("✅ Metrics snapshot created")

        return True

    except Exception as e:
        print(f"❌ Metrics test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_scenario_total_threads_attribute():
    """Test TestScenario total_threads attribute access (regression test for template creation)."""
    print("\n🔍 Testing TestScenario total_threads attribute access")
    print("=" * 60)

    try:
        table = TableConfig(
            name="test_table",
            table_type=TableType.STANDARD,
            columns={"id": "NUMBER", "value": "VARCHAR"},
        )

        # Create scenario with new field name
        scenario = TestScenario.model_validate(
            {
                "name": "threads_test",
                "duration_seconds": 60,
                "total_threads": 25,
                "workload_type": WorkloadType.READ_HEAVY,
                "table_configs": [table],
            }
        )
        assert scenario.total_threads == 25, "total_threads attribute access failed"
        print(f"✅ scenario.total_threads = {scenario.total_threads}")

        # Create scenario with alias (backward compatibility)
        scenario_alias = TestScenario(
            name="alias_test",
            duration_seconds=60,
            concurrent_connections=30,  # Using alias
            workload_type=WorkloadType.READ_HEAVY,
            table_configs=[table],
        )
        # Access via NEW field name (should work due to alias)
        assert scenario_alias.total_threads == 30, "alias should map to total_threads"
        print(
            f"✅ scenario (via alias) .total_threads = {scenario_alias.total_threads}"
        )

        # Test min_threads_per_worker attribute
        scenario_min = TestScenario.model_validate(
            {
                "name": "min_threads_test",
                "duration_seconds": 60,
                "total_threads": 50,
                "min_threads_per_worker": 5,
                "workload_type": WorkloadType.READ_HEAVY,
                "table_configs": [table],
            }
        )
        assert scenario_min.min_threads_per_worker == 5, (
            "min_threads_per_worker access failed"
        )
        print(
            f"✅ scenario.min_threads_per_worker = {scenario_min.min_threads_per_worker}"
        )

        # Test min_threads_per_worker via alias
        scenario_min_alias = TestScenario(
            name="min_alias_test",
            duration_seconds=60,
            concurrent_connections=50,
            min_connections=8,  # Using alias
            workload_type=WorkloadType.READ_HEAVY,
            table_configs=[table],
        )
        assert scenario_min_alias.min_threads_per_worker == 8, (
            "alias should map to min_threads_per_worker"
        )
        print(
            f"✅ scenario (via alias) .min_threads_per_worker = {scenario_min_alias.min_threads_per_worker}"
        )

        return True

    except Exception as e:
        print(f"❌ TestScenario total_threads test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def main():
    """Run all model tests."""
    print("=" * 60)
    print("🧪 Running Data Model Tests")
    print("=" * 60)

    tests = [
        ("TableConfig", test_table_config),
        ("WarehouseConfig", test_warehouse_config),
        ("TestScenario", test_test_scenario),
        ("TestResult", test_test_result),
        ("TestRun", test_test_run),
        ("Metrics", test_metrics),
        ("TestScenario total_threads", test_scenario_total_threads_attribute),
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
