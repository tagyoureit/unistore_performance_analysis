#!/usr/bin/env python3
"""
Test script for table managers.

Tests table manager factory, validation, and basic interface.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from backend.models import TableType, TableConfig
from backend.core.table_managers import (
    create_table_manager,
    StandardTableManager,
    HybridTableManager,
    InteractiveTableManager,
    PostgresTableManager,
)


def test_factory():
    """Test table manager factory."""
    print("\n🔍 Testing Table Manager Factory")
    print("=" * 60)

    try:
        # Standard table
        standard_config = TableConfig(
            name="test_standard",
            table_type=TableType.STANDARD,
            columns={"id": "NUMBER", "value": "VARCHAR"},
        )
        manager = create_table_manager(standard_config)
        assert isinstance(manager, StandardTableManager)
        print(f"✅ Standard table manager created: {manager.table_name}")

        # Hybrid table
        hybrid_config = TableConfig(
            name="test_hybrid",
            table_type=TableType.HYBRID,
            columns={"id": "NUMBER", "value": "VARCHAR"},
        )
        manager = create_table_manager(hybrid_config)
        assert isinstance(manager, HybridTableManager)
        print(f"✅ Hybrid table manager created: {manager.table_name}")

        # Interactive table (placeholder)
        interactive_config = TableConfig(
            name="test_interactive",
            table_type=TableType.INTERACTIVE,
            cluster_by=["date"],
            columns={"id": "NUMBER", "date": "DATE"},
        )
        manager = create_table_manager(interactive_config)
        assert isinstance(manager, InteractiveTableManager)
        print(
            f"✅ Interactive table manager created (placeholder): {manager.table_name}"
        )

        # Postgres table
        postgres_config = TableConfig(
            name="test_postgres",
            table_type=TableType.POSTGRES,
            columns={"id": "NUMBER", "value": "VARCHAR"},
        )
        manager = create_table_manager(postgres_config)
        assert isinstance(manager, PostgresTableManager)
        print(f"✅ Postgres table manager created: {manager.table_name}")

        return True

    except Exception as e:
        print(f"❌ Factory test failed: {e}")
        import traceback

        traceback.print_exc()
        return False


def test_standard_manager():
    """Test standard table manager validation."""
    config = TableConfig(
        name="test_standard",
        table_type=TableType.STANDARD,
        columns={
            "id": "NUMBER",
            "date": "DATE",
            "customer_id": "VARCHAR(100)",
            "amount": "DECIMAL(10,2)",
        },
        data_retention_days=7,
        database="TEST_DB",
        schema_name="PUBLIC",
    )

    manager = StandardTableManager(config)

    # Full table name is still derived from config (Snowflake-style qualification).
    assert manager.get_full_table_name() == "TEST_DB.PUBLIC.test_standard"

    # No schema introspection happens until setup(); object_type starts unset.
    assert manager.object_type is None
    assert isinstance(manager.stats, dict)


def test_hybrid_manager():
    """Test hybrid table manager validation."""
    config = TableConfig(
        name="test_hybrid",
        table_type=TableType.HYBRID,
        columns={
            "id": "NUMBER",
            "customer_id": "VARCHAR(100)",
            "date": "DATE",
            "amount": "DECIMAL(10,2)",
        },
    )

    manager = HybridTableManager(config)
    assert manager.object_type is None

    # Hybrid tables no longer require PK metadata (existing objects only).
    bad_config = TableConfig(
        name="bad_hybrid",
        table_type=TableType.HYBRID,
        columns={"id": "NUMBER"},
    )
    HybridTableManager(bad_config)


def test_postgres_manager():
    """Test Postgres table manager qualification behavior."""
    config = TableConfig(
        name="test_postgres",
        table_type=TableType.POSTGRES,
        columns={"id": "NUMBER", "value": "VARCHAR"},
    )

    manager = PostgresTableManager(config)
    assert manager.get_full_table_name() == "public.test_postgres"
    assert manager.object_type is None


def test_postgres_column_type_mapping_preserves_string_lengths():
    """
    Postgres schema introspection must preserve CHAR/VARCHAR lengths so custom workloads
    don't generate values that violate CHAR(n) constraints (e.g. CHAR(1)).
    """
    assert PostgresTableManager._map_column_type("character", "bpchar", 1) == "CHAR(1)"
    assert (
        PostgresTableManager._map_column_type("character varying", "varchar", 15)
        == "VARCHAR(15)"
    )
    assert PostgresTableManager._map_column_type("text", "text", None) == "VARCHAR"


def test_interactive_manager():
    """Test interactive table manager placeholder."""
    config = TableConfig(
        name="test_interactive",
        table_type=TableType.INTERACTIVE,
        cluster_by=["date"],
        columns={"id": "NUMBER", "date": "DATE"},
    )
    _manager = InteractiveTableManager(config)

    # Interactive tables are treated like standard tables in this app (existing objects only);
    # type-specific DDL requirements are intentionally not enforced at config time.
    bad_config = TableConfig(
        name="bad_interactive",
        table_type=TableType.INTERACTIVE,
        columns={"id": "NUMBER"},
    )
    InteractiveTableManager(bad_config)


def main():
    """Run all table manager tests."""
    print("=" * 60)
    print("🧪 Running Table Manager Tests")
    print("=" * 60)
    print("\nThese tests validate:")
    print("- Factory pattern and manager creation")
    print("- Configuration validation")
    print("- Type conversions and SQL generation")
    print("\nNOTE: These tests do NOT require database connections.")
    print("Database integration tests require valid credentials.")

    tests = [
        ("Factory", test_factory),
        ("StandardTableManager", test_standard_manager),
        ("HybridTableManager", test_hybrid_manager),
        ("PostgresTableManager", test_postgres_manager),
        ("InteractiveTableManager", test_interactive_manager),
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
