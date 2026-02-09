"""
Table Managers

Factory and exports for different table manager types.
"""

from backend.core.table_managers.base import TableManager
from backend.core.table_managers.standard import StandardTableManager
from backend.core.table_managers.hybrid import HybridTableManager
from backend.core.table_managers.interactive import InteractiveTableManager
from backend.core.table_managers.dynamic import DynamicTableManager
from backend.core.table_managers.postgres import PostgresTableManager

from backend.models.test_config import TableConfig, TableType


def create_table_manager(config: TableConfig) -> TableManager:
    """
    Factory function to create appropriate table manager.

    Args:
        config: Table configuration

    Returns:
        TableManager instance for the table type

    Raises:
        ValueError: If table type is not supported
    """
    table_type = config.table_type

    if table_type == TableType.STANDARD:
        return StandardTableManager(config)
    elif table_type == TableType.HYBRID:
        return HybridTableManager(config)
    elif table_type == TableType.INTERACTIVE:
        return InteractiveTableManager(config)
    elif table_type == TableType.DYNAMIC:
        return DynamicTableManager(config)
    elif table_type == TableType.POSTGRES:
        return PostgresTableManager(config)
    else:
        raise ValueError(f"Unsupported table type: {table_type}")


__all__ = [
    "TableManager",
    "StandardTableManager",
    "HybridTableManager",
    "InteractiveTableManager",
    "DynamicTableManager",
    "PostgresTableManager",
    "create_table_manager",
]
