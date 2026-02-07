"""
Test Executor Package

This package provides the TestExecutor class for orchestrating performance tests.

For backward compatibility, TestExecutor is imported from the original module.

Usage:
    from backend.core.executor import TestExecutor

    # Or the traditional import still works:
    from backend.core.test_executor import TestExecutor
"""

# Re-export from original module for backward compatibility
from backend.core.test_executor import TestExecutor

# Export types for use by other modules
from .types import QueryExecutionRecord, StepResult, TableRuntimeState

# Export helper functions
from .helpers import (
    annotate_query_for_sf_kind,
    build_smooth_weighted_schedule,
    classify_sql_error,
    is_postgres_pool,
    quote_column,
)

__all__ = [
    "TestExecutor",
    "QueryExecutionRecord",
    "StepResult",
    "TableRuntimeState",
    "annotate_query_for_sf_kind",
    "build_smooth_weighted_schedule",
    "classify_sql_error",
    "is_postgres_pool",
    "quote_column",
]
