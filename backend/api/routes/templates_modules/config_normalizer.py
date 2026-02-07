"""
Template configuration normalization and validation.
"""

import math
from typing import Any

from backend.core.mode_config import ModeConfig

from .constants import (
    _CUSTOM_PCT_FIELDS,
    _CUSTOM_QUERY_FIELDS,
    _DEFAULT_CUSTOM_QUERIES_POSTGRES,
    _DEFAULT_CUSTOM_QUERIES_SNOWFLAKE,
    _PRESET_PCTS,
)
from .utils import _coerce_int, _is_postgres_family_table_type


def _normalize_template_config(cfg: Any) -> dict[str, Any]:
    """
    Normalize template config to an authoritative CUSTOM workload definition.

    Contract:
    - Any preset workload_type (READ_ONLY/WRITE_ONLY/READ_HEAVY/WRITE_HEAVY/MIXED)
      is rewritten to workload_type=CUSTOM with explicit custom_*_pct + custom_*_query.
    - CUSTOM workloads are validated server-side (pct sum, required SQL).
    """
    if not isinstance(cfg, dict):
        raise ValueError("Template config must be a JSON object")

    out: dict[str, Any] = dict(cfg)
    wt_raw = str(out.get("workload_type") or "").strip()
    wt = wt_raw.upper() if wt_raw else "MIXED"

    if wt != "CUSTOM":
        if wt not in _PRESET_PCTS:
            raise ValueError(
                f"Invalid workload_type: {wt_raw!r} (expected one of "
                f"{sorted([*list(_PRESET_PCTS.keys()), 'CUSTOM'])})"
            )
        out["workload_type"] = "CUSTOM"
        # Templates persist SQL per-backend (Snowflake vs Postgres-family).
        defaults = (
            _DEFAULT_CUSTOM_QUERIES_POSTGRES
            if _is_postgres_family_table_type(out.get("table_type"))
            else _DEFAULT_CUSTOM_QUERIES_SNOWFLAKE
        )
        out.update(defaults)
        out.update(_PRESET_PCTS[wt])
    else:
        # CUSTOM: normalize and validate.
        out["workload_type"] = "CUSTOM"

        # Normalize query strings.
        for k in _CUSTOM_QUERY_FIELDS:
            out[k] = str(out.get(k) or "").strip()

        # Normalize pct fields.
        for k in _CUSTOM_PCT_FIELDS:
            out[k] = _coerce_int(out.get(k) or 0, label=k)
            if out[k] < 0 or out[k] > 100:
                raise ValueError(f"{k} must be between 0 and 100 (got {out[k]})")

        total = sum(int(out[k]) for k in _CUSTOM_PCT_FIELDS)
        if total != 100:
            raise ValueError(
                f"Custom query percentages must sum to 100 (currently {total})."
            )

        required_pairs = [
            ("custom_point_lookup_pct", "custom_point_lookup_query"),
            ("custom_range_scan_pct", "custom_range_scan_query"),
            ("custom_insert_pct", "custom_insert_query"),
            ("custom_update_pct", "custom_update_query"),
        ]
        for pct_k, sql_k in required_pairs:
            if int(out.get(pct_k) or 0) > 0 and not str(out.get(sql_k) or "").strip():
                raise ValueError(f"{sql_k} is required when {pct_k} > 0")

    # Targets (SLOs): required for any query kind that has weight > 0.
    def _coerce_num(v: Any, *, label: str) -> float:
        if v is None:
            return -1.0
        if isinstance(v, bool):
            return float(v)
        s = str(v).strip()
        if not s:
            return -1.0
        try:
            n = float(s)
        except Exception as e:
            raise ValueError(f"Invalid {label}: {v!r}") from e
        if not math.isfinite(n):
            return -1.0
        return float(n)

    target_fields = {
        "POINT_LOOKUP": (
            "custom_point_lookup_pct",
            "target_point_lookup_p95_latency_ms",
            "target_point_lookup_p99_latency_ms",
            "target_point_lookup_error_rate_pct",
        ),
        "RANGE_SCAN": (
            "custom_range_scan_pct",
            "target_range_scan_p95_latency_ms",
            "target_range_scan_p99_latency_ms",
            "target_range_scan_error_rate_pct",
        ),
        "INSERT": (
            "custom_insert_pct",
            "target_insert_p95_latency_ms",
            "target_insert_p99_latency_ms",
            "target_insert_error_rate_pct",
        ),
        "UPDATE": (
            "custom_update_pct",
            "target_update_p95_latency_ms",
            "target_update_p99_latency_ms",
            "target_update_error_rate_pct",
        ),
    }

    for _pct_k, p95_k, p99_k, err_k in target_fields.values():
        # Default all target fields to -1 (disabled), even if pct is 0, so templates
        # have stable keys and a single sentinel for "disabled".
        p95 = _coerce_num(out.get(p95_k), label=p95_k)
        p99 = _coerce_num(out.get(p99_k), label=p99_k)
        err = _coerce_num(out.get(err_k), label=err_k)

        # Latency target: -1 disabled, else must be > 0.
        if p95 < 0:
            out[p95_k] = -1.0
        elif p95 == 0:
            raise ValueError(f"{p95_k} must be > 0 or -1 to disable")
        else:
            out[p95_k] = float(p95)

        # Latency target: -1 disabled, else must be > 0.
        if p99 < 0:
            out[p99_k] = -1.0
        elif p99 == 0:
            raise ValueError(f"{p99_k} must be > 0 or -1 to disable")
        else:
            out[p99_k] = float(p99)

        # Error target: -1 disabled, else must be within [0, 100].
        if err < 0:
            out[err_k] = -1.0
        else:
            if err > 100:
                raise ValueError(f"{err_k} must be between 0 and 100 (got {err})")
            out[err_k] = float(err)

        # Targets are optional (default -1). When enabled, validate ranges above.

    # Load mode: fixed workers vs auto-scale (QPS target).
    # - QPS mode targets app-side throughput (ops/sec). Snowflake RUNNING is observed separately.
    # Use ModeConfig for unified parsing (strict=True raises ValueError on invalid)
    scaling_cfg = out.get("scaling")
    if not isinstance(scaling_cfg, dict):
        scaling_cfg = {}
    out["scaling"] = scaling_cfg

    _norm_mode_cfg = ModeConfig.from_config(
        scaling_cfg=scaling_cfg, workload_cfg=out, strict=True
    )
    load_mode = _norm_mode_cfg.load_mode
    out["load_mode"] = load_mode
    scaling_cfg["mode"] = _norm_mode_cfg.scaling_mode
    scaling_mode = _norm_mode_cfg.scaling_mode

    if "min_concurrency" in out:
        raise ValueError("min_concurrency was renamed to scaling.min_connections")

    def _coerce_optional_int(
        value: Any, *, label: str, allow_unbounded: bool = False
    ) -> int | None:
        if value is None:
            return None
        if isinstance(value, str) and not value.strip():
            return None
        coerced = _coerce_int(value, label=label)
        if allow_unbounded and coerced == -1:
            return None
        return coerced

    min_workers = _coerce_optional_int(
        scaling_cfg.get("min_workers"), label="min_workers"
    )
    max_workers = _coerce_optional_int(
        scaling_cfg.get("max_workers"), label="max_workers", allow_unbounded=True
    )
    min_connections = _coerce_optional_int(
        scaling_cfg.get("min_connections"), label="min_connections"
    )
    max_connections = _coerce_optional_int(
        scaling_cfg.get("max_connections"),
        label="max_connections",
        allow_unbounded=True,
    )

    if scaling_mode == "FIXED":
        if "min_workers" not in scaling_cfg or min_workers is None:
            raise ValueError("FIXED mode requires scaling.min_workers to be set")
        if "min_connections" not in scaling_cfg or min_connections is None:
            raise ValueError("FIXED mode requires scaling.min_connections to be set")

    min_workers = int(min_workers or 1)
    min_connections = int(min_connections or 1)
    if min_workers < 1:
        raise ValueError("min_workers must be >= 1")
    if min_connections < 1:
        raise ValueError("min_connections must be >= 1")
    if max_workers is not None and max_workers < 1:
        raise ValueError("max_workers must be >= 1 or null")
    if max_connections is not None and max_connections < 1:
        raise ValueError("max_connections must be >= 1 or null")
    if max_workers is not None and min_workers > max_workers:
        raise ValueError("min_workers must be <= max_workers")
    if max_connections is not None and min_connections > max_connections:
        raise ValueError("min_connections must be <= max_connections")

    scaling_cfg["min_workers"] = int(min_workers)
    scaling_cfg["max_workers"] = int(max_workers) if max_workers is not None else None
    scaling_cfg["min_connections"] = int(min_connections)
    scaling_cfg["max_connections"] = (
        int(max_connections) if max_connections is not None else None
    )

    if load_mode == "QPS":
        try:
            target_qps = float(out.get("target_qps") or 0)
        except Exception as e:
            raise ValueError(f"Invalid target_qps: {out.get('target_qps')!r}") from e
        if not math.isfinite(target_qps) or target_qps <= 0:
            raise ValueError("target_qps must be > 0 when load_mode=QPS")
        out["target_qps"] = float(target_qps)

        min_connections = int(scaling_cfg.get("min_connections") or 1)
        # In QPS mode, concurrent_connections represents the MAX worker cap.
        # Allow -1 to mean "no user cap" (still bounded at runtime by engine caps).
        raw_max = out.get("concurrent_connections")
        if raw_max is None or (isinstance(raw_max, str) and not raw_max.strip()):
            raw_max = -1
        max_concurrency = _coerce_int(raw_max, label="concurrent_connections")
        if max_concurrency != -1 and max_concurrency < 1:
            raise ValueError(
                "concurrent_connections must be >= 1 or -1 (no user cap) in QPS mode"
            )
        if max_concurrency != -1 and min_connections > max_concurrency:
            raise ValueError(
                "min_connections must be <= concurrent_connections (or set concurrent_connections=-1)"
            )
        out["concurrent_connections"] = int(max_concurrency)
    else:
        # Keep keys stable but default to "disabled" values.
        out["target_qps"] = (
            out.get("target_qps") if out.get("target_qps") is not None else None
        )
        scaling_cfg["min_connections"] = int(scaling_cfg.get("min_connections") or 1)

    if scaling_mode == "FIXED" and load_mode == "CONCURRENCY":
        out["concurrent_connections"] = int(min_workers) * int(min_connections)

    # FIND_MAX_CONCURRENCY mode doesn't use concurrent_connections - it discovers the max dynamically
    # Clear any saved concurrent_connections value to prevent it from being used as a ceiling
    if load_mode == "FIND_MAX_CONCURRENCY":
        out["concurrent_connections"] = None

    def _resolve_effective_max_connections() -> int | None:
        per_worker_cap = None
        if load_mode == "QPS":
            raw_cap = out.get("concurrent_connections")
            if isinstance(raw_cap, (int, float)) and int(raw_cap) != -1:
                per_worker_cap = int(raw_cap)
        elif load_mode != "FIND_MAX_CONCURRENCY" and isinstance(out.get("concurrent_connections"), (int, float)):
            # FIND_MAX_CONCURRENCY doesn't use concurrent_connections - it discovers the max dynamically
            per_worker_cap = int(out.get("concurrent_connections") or 0)
        if max_connections is not None:
            if per_worker_cap is None:
                return int(max_connections)
            return int(min(per_worker_cap, max_connections))
        return per_worker_cap

    effective_max_connections = _resolve_effective_max_connections()
    if max_workers is not None and effective_max_connections is not None:
        max_total = int(max_workers) * int(effective_max_connections)
        if load_mode in {"CONCURRENCY", "FIND_MAX_CONCURRENCY"}:
            target_total = int(out.get("concurrent_connections") or 0)
            if target_total > max_total:
                raise ValueError(
                    f"Target concurrency {target_total} unreachable with max {max_workers} workers × {effective_max_connections} connections"
                )
        elif load_mode == "QPS":
            target_total = float(out.get("target_qps") or 0.0)
            if target_total > float(max_total):
                raise ValueError(
                    f"Target QPS {target_total} unreachable with max {max_workers} workers × {effective_max_connections} connections"
                )

    # Resource guardrails (for AUTO and BOUNDED modes).
    # Derive autoscale_enabled from scaling_mode for backward compatibility.
    autoscale_enabled = scaling_mode != "FIXED"
    out["autoscale_enabled"] = autoscale_enabled

    # Initialize guardrails block, migrating from legacy fields if needed.
    guardrails_cfg = out.get("guardrails") or {}
    if not isinstance(guardrails_cfg, dict):
        guardrails_cfg = {}

    # Check if guardrails are explicitly disabled
    guardrails_enabled = guardrails_cfg.get("enabled")
    if guardrails_enabled is None:
        guardrails_enabled = True  # Default to enabled for backward compatibility

    if guardrails_enabled:
        # Get max_cpu_percent from guardrails or legacy field
        max_cpu_raw = guardrails_cfg.get("max_cpu_percent")
        if max_cpu_raw is None:
            max_cpu_raw = out.get("autoscale_max_cpu_percent")
        max_cpu = _coerce_int(max_cpu_raw or 80, label="guardrails.max_cpu_percent")

        # Get max_memory_percent from guardrails or legacy field
        max_mem_raw = guardrails_cfg.get("max_memory_percent")
        if max_mem_raw is None:
            max_mem_raw = out.get("autoscale_max_memory_percent")
        max_mem = _coerce_int(max_mem_raw or 85, label="guardrails.max_memory_percent")

        # Validate guardrails for non-FIXED modes
        if autoscale_enabled:
            if max_cpu <= 0 or max_cpu > 100:
                raise ValueError("guardrails.max_cpu_percent must be within (0, 100]")
            if max_mem <= 0 or max_mem > 100:
                raise ValueError(
                    "guardrails.max_memory_percent must be within (0, 100]"
                )

        # Store both new and legacy formats for compatibility
        out["guardrails"] = {
            "enabled": True,
            "max_cpu_percent": int(max_cpu),
            "max_memory_percent": int(max_mem),
        }
        # Keep legacy fields for backward compatibility
        out["autoscale_max_cpu_percent"] = int(max_cpu)
        out["autoscale_max_memory_percent"] = int(max_mem)
    else:
        # Guardrails disabled - set to None so orchestrator skips checks
        out["guardrails"] = {
            "enabled": False,
            "max_cpu_percent": None,
            "max_memory_percent": None,
        }
        out["autoscale_max_cpu_percent"] = None
        out["autoscale_max_memory_percent"] = None

    return out
