"""
Pre-flight warnings for test configurations.

Checks for configurations that are likely to hit Snowflake limits.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


async def generate_preflight_warnings(
    scenario_config: dict[str, Any]
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
    table_type = str(
        scenario_config.get("table_type", "standard")
    ).lower()
    workload_cfg = scenario_config.get("workload", {})
    custom_queries = workload_cfg.get("custom_queries", [])
    total_threads = int(scenario_config.get("total_threads", 10))
    table_name = str(scenario_config.get("table_name", ""))

    # Calculate write percentage from CUSTOM query weights.
    # Runtime is CUSTOM-only, but tolerate legacy key names in persisted rows.
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
            # weight_pct is stored as percentage points (0.00-100.00).
            normalized_weight = max(0.0, min(weight / 100.0, 1.0))
            operation_type = str(q.get("operation_type") or "").upper()
            is_write = kind in ("INSERT", "UPDATE", "DELETE") or (
                kind == "GENERIC_SQL" and operation_type == "WRITE"
            )
            if is_write and normalized_weight > 0:
                write_pct += normalized_weight
    write_pct = max(0.0, min(write_pct, 1.0))

    # Calculate expected concurrent writers
    expected_concurrent_writes = total_threads * write_pct

    # Check for lock contention risk on standard tables
    # Snowflake limit: 20 statements waiting for a table lock
    LOCK_WAITER_LIMIT = 20

    if table_type == "standard" and expected_concurrent_writes > LOCK_WAITER_LIMIT:
        warnings.append({
            "severity": "high",
            "title": "Lock Contention Risk",
            "message": (
                f"Standard tables use TABLE-LEVEL LOCKING for writes. "
                f"With {total_threads} threads and ~{write_pct*100:.0f}% writes, "
                f"you may have ~{expected_concurrent_writes:.0f} concurrent write attempts. "
                f"Snowflake's lock waiter limit is {LOCK_WAITER_LIMIT} statements. "
                f"If any write takes >1 second, you WILL hit SF_LOCK_WAITER_LIMIT errors."
            ),
            "recommendations": [
                "Use a HYBRID table for concurrent write workloads (row-level locking)",
                f"Reduce concurrency to ≤{int(LOCK_WAITER_LIMIT / write_pct) if write_pct > 0 else total_threads} threads",
                "For read-only benchmarking, keep CUSTOM and set all WRITE operations to 0%",
            ],
            "details": {
                "table_type": table_type,
                "table_name": table_name,
                "total_threads": total_threads,
                "write_percentage": round(write_pct * 100, 1),
                "expected_concurrent_writes": round(expected_concurrent_writes, 1),
                "lock_waiter_limit": LOCK_WAITER_LIMIT,
            },
        })
    elif table_type == "standard" and expected_concurrent_writes > LOCK_WAITER_LIMIT * 0.5:
        # Warning for approaching the limit (>50% of limit)
        warnings.append({
            "severity": "medium",
            "title": "Potential Lock Contention",
            "message": (
                f"With {total_threads} threads and ~{write_pct*100:.0f}% writes on a STANDARD table, "
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
                "expected_concurrent_writes": round(expected_concurrent_writes, 1),
                "lock_waiter_limit": LOCK_WAITER_LIMIT,
            },
        })

    # -------------------------------------------------------------------------
    # Interactive table checks
    # -------------------------------------------------------------------------
    if table_type == "interactive":
        # Check: no clustering key specified
        clustering_keys = scenario_config.get("clustering_keys", [])
        if not clustering_keys:
            warnings.append({
                "severity": "high",
                "title": "Missing Clustering Key",
                "message": (
                    "Interactive tables require a CLUSTER BY clause for partition pruning. "
                    "Without it, the interactive warehouse scans all partitions and queries "
                    "hit the 5-second statement timeout, falling back to standard execution."
                ),
                "recommendations": [
                    "Add clustering_keys to the table configuration matching your query predicates",
                    "For point lookups: cluster by the lookup column (e.g. account_id)",
                    "For composite access: put the most selective column first",
                ],
                "details": {
                    "table_type": table_type,
                    "table_name": table_name,
                },
            })

        # Check: warehouse type mismatch
        warehouse_cfg = scenario_config.get("warehouse", {})
        wh_type = str(warehouse_cfg.get("type", "")).lower()
        if wh_type and wh_type != "interactive":
            warnings.append({
                "severity": "high",
                "title": "Warehouse Type Mismatch",
                "message": (
                    f"Table type is INTERACTIVE but warehouse type is '{wh_type.upper()}'. "
                    "Interactive tables must be served by an interactive warehouse to get "
                    "sub-second latency. On a standard warehouse they behave like regular tables."
                ),
                "recommendations": [
                    "Use an interactive warehouse (CREATE INTERACTIVE WAREHOUSE ...)",
                    "Attach the table: ALTER WAREHOUSE <iwh> ADD TABLES (<table>)",
                ],
                "details": {
                    "table_type": table_type,
                    "table_name": table_name,
                    "warehouse_type": wh_type,
                },
            })

        # Check: write workload on interactive table
        if write_pct > 0:
            warnings.append({
                "severity": "medium",
                "title": "Writes on Interactive Table",
                "message": (
                    f"Configuration includes ~{write_pct*100:.0f}% write operations. "
                    "Interactive tables on interactive warehouses are optimized for reads. "
                    "Writes go through the standard execution path and may affect read latency."
                ),
                "recommendations": [
                    "Use interactive tables for read-only serving workloads",
                    "Route writes to a standard warehouse or use dynamic interactive tables",
                ],
                "details": {
                    "table_type": table_type,
                    "write_percentage": round(write_pct * 100, 1),
                },
            })

    return warnings
