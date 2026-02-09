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
                "For read-only benchmarking, keep CUSTOM and set INSERT/UPDATE mix to 0%",
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

    return warnings
