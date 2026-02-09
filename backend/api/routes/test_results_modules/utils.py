"""
Utility functions for test results API.

Contains helper functions for cost calculation, error normalization, etc.
"""

from __future__ import annotations

import re
from typing import Any

from backend.config import settings


_TXN_RE = re.compile(r"\btransaction\s+\d+\b", re.IGNORECASE)
_SF_QUERY_ID_PREFIX_RE = re.compile(
    r"(\(\s*\d{5}\s*\)\s*:)\s*[0-9a-zA-Z-]{12,}\s*:",
    re.IGNORECASE,
)
_SF_ERROR_PREFIX_RE = re.compile(r"^\s*(\d+)\s*\(\s*(\d{5})\s*\)", re.IGNORECASE)
_ABORTED_BECAUSE_RE = re.compile(
    r"\bwas\s+aborted\s+because\b\s*(.*?)(?:\.|$)", re.IGNORECASE
)
_SQL_COMPILATION_RE = re.compile(
    r"\bsql\s+compilation\s+error\b\s*:\s*(.*?)(?:\.|$)", re.IGNORECASE
)

LATENCY_AGGREGATION_METHOD = "slowest_worker_approximation"


def get_prefix() -> str:
    """Get the database.schema prefix for results tables."""
    return f"{settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}"


def build_cost_fields(
    duration_seconds: float,
    warehouse_size: str | None,
    total_operations: int = 0,
    qps: float = 0.0,
    table_type: str | None = None,
    postgres_instance_size: str | None = None,
) -> dict[str, Any]:
    """
    Build cost-related fields for API responses.

    Args:
        duration_seconds: Test duration in seconds
        warehouse_size: Warehouse size string (e.g., "XSMALL", "MEDIUM")
        total_operations: Total operations executed (for efficiency metrics)
        qps: Queries per second (for efficiency metrics)
        table_type: Table type (e.g., "HYBRID", "POSTGRES")
        postgres_instance_size: For Postgres, explicit instance size override.

    Returns:
        Dictionary with cost fields to merge into response
    """
    from backend.core.cost_calculator import (
        calculate_estimated_cost,
        calculate_cost_efficiency,
        get_postgres_instance_size_by_host,
    )

    # For Postgres, look up the actual instance size from the configured host
    effective_postgres_size = postgres_instance_size
    if not effective_postgres_size and table_type:
        table_type_upper = table_type.upper().strip()
        if table_type_upper == "POSTGRES":
            actual_size = get_postgres_instance_size_by_host(settings.POSTGRES_HOST)
            if actual_size:
                effective_postgres_size = actual_size
            else:
                effective_postgres_size = settings.POSTGRES_INSTANCE_SIZE

    cost_info = calculate_estimated_cost(
        duration_seconds=duration_seconds,
        warehouse_size=warehouse_size,
        dollars_per_credit=settings.COST_DOLLARS_PER_CREDIT,
        table_type=table_type,
        postgres_instance_size=effective_postgres_size,
    )

    result: dict[str, Any] = {
        "credits_used": cost_info["credits_used"],
        "estimated_cost_usd": cost_info["estimated_cost_usd"],
        "cost_per_hour": cost_info["cost_per_hour"],
        "credits_per_hour": cost_info.get("credits_per_hour", 0.0),
        "cost_calculation_method": cost_info["calculation_method"],
        "postgres_instance_size": (
            effective_postgres_size
            if table_type and table_type.upper().strip() == "POSTGRES"
            else None
        ),
    }

    # Add efficiency metrics if we have operation data AND have a cost to work with
    if (total_operations > 0 or qps > 0) and cost_info["estimated_cost_usd"] > 0:
        efficiency = calculate_cost_efficiency(
            total_cost=cost_info["estimated_cost_usd"],
            total_operations=total_operations,
            qps=qps,
            duration_seconds=duration_seconds,
        )
        result["cost_per_operation"] = efficiency["cost_per_operation"]
        result["cost_per_1000_ops"] = efficiency["cost_per_1000_ops"]
        result["cost_per_1k_ops"] = efficiency["cost_per_1000_ops"]  # Alias for frontend
        result["cost_per_1000_qps"] = efficiency["cost_per_1000_qps"]

    return result


def error_reason(msg: str) -> str:
    """
    Extract a short, human-readable reason for UI summaries.

    Keep this low-cardinality and derived from the normalized message.
    """
    s = str(msg or "").strip()
    if not s:
        return ""

    m = _ABORTED_BECAUSE_RE.search(s)
    if m:
        return str(m.group(1) or "").strip()

    m = _SQL_COMPILATION_RE.search(s)
    if m:
        detail = str(m.group(1) or "").strip()
        return f"SQL compilation error: {detail}" if detail else "SQL compilation error"

    return ""


def normalize_error_message(msg: Any) -> str:
    """
    Normalize error messages to reduce high-cardinality IDs in grouping.

    Example: lock errors often embed statement IDs and transaction numbers.
    """
    s = str(msg or "").strip()
    if not s:
        return ""

    # Normalize the Snowflake query-id prefix
    s = _SF_QUERY_ID_PREFIX_RE.sub(r"\1 <query_id>:", s)

    # Normalize common Snowflake lock error noise while preserving table names
    s = re.sub(
        r"Statement\s+'[^']+'", "Statement '<statement_id>'", s, flags=re.IGNORECASE
    )
    s = re.sub(
        r"Your statement\s+'[^']+'",
        "Your statement '<statement_id>'",
        s,
        flags=re.IGNORECASE,
    )
    s = _TXN_RE.sub("transaction <txn>", s)

    # Collapse whitespace/newlines for UI table readability
    s = " ".join(s.split())
    return s


def to_float_or_none(v: Any) -> float | None:
    """Convert value to float or return None."""
    if v is None:
        return None
    try:
        return float(v)
    except Exception:
        return None


def compute_latency_spread(p50: float | None, p95: float | None) -> dict[str, Any]:
    """
    Compute latency spread ratio (P95/P50) and warning flag.

    The spread ratio indicates latency variance - a high ratio means tail latencies
    are much worse than typical (median) latencies.

    Args:
        p50: P50 (median) latency in milliseconds
        p95: P95 latency in milliseconds

    Returns:
        Dictionary with latency_spread_ratio and latency_spread_warning
    """
    if not p50 or not p95 or p50 <= 0:
        return {
            "latency_spread_ratio": None,
            "latency_spread_warning": False,
        }

    ratio = p95 / p50
    return {
        "latency_spread_ratio": round(ratio, 1),
        "latency_spread_warning": ratio > 5.0,
    }


def compute_aggregated_find_max(worker_results: list[dict]) -> dict:
    """
    Compute true aggregate metrics across all workers' find_max_result.

    For each concurrency level (step), aggregates:
    - Total QPS (sum across workers)
    - Max P95/P99 latencies (worst case)
    - Number of active workers at each step
    """
    if not worker_results:
        return {}

    steps_by_concurrency: dict[int, dict[int, dict]] = {}
    all_baselines_p95 = []
    all_baselines_p99 = []

    for worker in worker_results:
        fmr = worker.get("find_max_result", {})
        if not fmr:
            continue

        worker_idx = worker.get("worker_index", 0)
        if fmr.get("baseline_p95_latency_ms"):
            all_baselines_p95.append(fmr["baseline_p95_latency_ms"])
        if fmr.get("baseline_p99_latency_ms"):
            all_baselines_p99.append(fmr["baseline_p99_latency_ms"])

        step_history = fmr.get("step_history", [])
        for step in step_history:
            cc = step.get("concurrency")
            if cc is not None:
                if cc not in steps_by_concurrency:
                    steps_by_concurrency[cc] = {}
                if worker_idx not in steps_by_concurrency[cc]:
                    steps_by_concurrency[cc][worker_idx] = {
                        "worker_index": worker_idx,
                        **step,
                    }

    aggregated_steps = []
    total_workers = len(worker_results)

    for cc in sorted(steps_by_concurrency.keys()):
        worker_steps = list(steps_by_concurrency[cc].values())
        active_workers = len(worker_steps)

        total_qps = sum(s.get("qps", 0) or 0 for s in worker_steps)
        max_p95 = max((s.get("p95_latency_ms") or 0 for s in worker_steps), default=0)
        max_p99 = max((s.get("p99_latency_ms") or 0 for s in worker_steps), default=0)
        avg_p95 = (
            sum(s.get("p95_latency_ms") or 0 for s in worker_steps) / active_workers
            if active_workers > 0
            else 0
        )
        avg_p99 = (
            sum(s.get("p99_latency_ms") or 0 for s in worker_steps) / active_workers
            if active_workers > 0
            else 0
        )

        any_degraded = any(s.get("degraded") for s in worker_steps)
        reasons = [
            s.get("degrade_reason") for s in worker_steps if s.get("degrade_reason")
        ]

        aggregated_steps.append(
            {
                "concurrency": cc,
                "total_concurrency": cc * active_workers,
                "qps": round(total_qps, 2),
                "p95_latency_ms": round(max_p95, 2),
                "p99_latency_ms": round(max_p99, 2),
                "avg_p95_latency_ms": round(avg_p95, 2),
                "avg_p99_latency_ms": round(avg_p99, 2),
                "active_workers": active_workers,
                "total_workers": total_workers,
                "degraded": any_degraded,
                "degrade_reasons": reasons if reasons else None,
            }
        )

    best_step = None
    for step in aggregated_steps:
        if step["active_workers"] == total_workers and not step["degraded"]:
            if best_step is None or step["qps"] > best_step["qps"]:
                best_step = step

    if best_step is None and aggregated_steps:
        non_degraded = [s for s in aggregated_steps if not s["degraded"]]
        if non_degraded:
            best_step = max(non_degraded, key=lambda s: s["qps"])
        else:
            best_step = aggregated_steps[0]

    return {
        "step_history": aggregated_steps,
        "baseline_p95_latency_ms": max(all_baselines_p95) if all_baselines_p95 else None,
        "baseline_p99_latency_ms": max(all_baselines_p99) if all_baselines_p99 else None,
        "final_best_concurrency": best_step["concurrency"] if best_step else None,
        "final_best_qps": best_step["qps"] if best_step else None,
        "total_workers": total_workers,
        "is_aggregate": True,
    }
