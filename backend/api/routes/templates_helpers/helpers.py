"""
Helper functions and constants for template routes.

Utility functions for identifier validation, SQL building, and template normalization.
"""

import math
import re
from typing import Any

from backend.config import settings

_IDENT_RE = re.compile(r"^[A-Z0-9_]+$")

_CUSTOM_QUERY_FIELDS: tuple[str, str, str, str] = (
    "custom_point_lookup_query",
    "custom_range_scan_query",
    "custom_insert_query",
    "custom_update_query",
)
_CUSTOM_PCT_FIELDS: tuple[str, str, str, str] = (
    "custom_point_lookup_pct",
    "custom_range_scan_pct",
    "custom_insert_pct",
    "custom_update_pct",
)

# Canonical SQL templates for saved CUSTOM workloads.
_DEFAULT_CUSTOM_QUERIES_SNOWFLAKE: dict[str, str] = {
    "custom_point_lookup_query": "SELECT * FROM {table} WHERE id = ?",
    "custom_range_scan_query": ("SELECT * FROM {table} WHERE id BETWEEN ? AND ? + 100"),
    "custom_insert_query": (
        "INSERT INTO {table} (id, data, timestamp) VALUES (?, ?, CURRENT_TIMESTAMP)"
    ),
    "custom_update_query": (
        "UPDATE {table} SET data = ?, timestamp = CURRENT_TIMESTAMP WHERE id = ?"
    ),
}

_DEFAULT_CUSTOM_QUERIES_POSTGRES: dict[str, str] = {
    "custom_point_lookup_query": "SELECT * FROM {table} WHERE id = $1",
    "custom_range_scan_query": "SELECT * FROM {table} WHERE id BETWEEN $1 AND $2 LIMIT 100",
    "custom_insert_query": "INSERT INTO {table} (id, data, timestamp) VALUES ($1, $2, $3)",
    "custom_update_query": "UPDATE {table} SET data = $1, timestamp = $2 WHERE id = $3",
}

def upper_str(v: Any) -> str:
    """Convert value to uppercase string."""
    return str(v or "").strip().upper()


def validate_ident(name: Any, *, label: str) -> str:
    """
    Validate a Snowflake identifier component (DATABASE / SCHEMA / TABLE / COLUMN).

    For now we intentionally restrict to unquoted identifiers:
    - letters, digits, underscore
    - uppercased

    This avoids SQL injection and keeps generated SQL predictable.
    """
    value = upper_str(name)
    if not value:
        raise ValueError(f"Missing {label}")
    if not _IDENT_RE.fullmatch(value):
        raise ValueError(f"Invalid {label}: {value!r} (expected [A-Z0-9_]+)")
    return value


def quote_ident(name: str) -> str:
    """Quote a Snowflake identifier."""
    return f'"{name}"'


def is_postgres_family_table_type(table_type_raw: Any) -> bool:
    """
    True when the template targets a Postgres-family backend:
    - POSTGRES
    """
    t = upper_str(table_type_raw)
    return t == "POSTGRES"


def pg_quote_ident(name: str) -> str:
    """
    Quote a Postgres identifier defensively.

    Postgres does not support binding identifiers, so we must ensure safe quoting when we
    build introspection/sample queries against customer objects.
    """
    s = str(name or "")
    return '"' + s.replace('"', '""') + '"'


def pg_qualified_name(schema: str, table: str) -> str:
    """Build a fully qualified Postgres table name."""
    sch = str(schema or "").strip()
    tbl = str(table or "").strip()
    if not sch or not tbl:
        raise ValueError("schema and table_name are required")
    return f"{pg_quote_ident(sch)}.{pg_quote_ident(tbl)}"


def pg_placeholders(n: int, *, start: int = 1) -> str:
    """Generate Postgres placeholder string ($1, $2, ...)."""
    if int(n) <= 0:
        return ""
    s = int(start)
    return ", ".join(f"${i}" for i in range(s, s + int(n)))


def full_table_name(database: str, schema: str, table: str) -> str:
    """Build a fully qualified Snowflake table name."""
    db = validate_ident(database, label="database")
    sch = validate_ident(schema, label="schema")
    tbl = validate_ident(table, label="table")
    return f"{quote_ident(db)}.{quote_ident(sch)}.{quote_ident(tbl)}"


def sample_clause(is_view: bool, limit: int, percentage: int = 10) -> str:
    """
    Return appropriate SQL sampling clause.

    TABLESAMPLE SYSTEM is fast (block-level) but doesn't work on views.
    For views, we just use LIMIT which gets sequential rows (not random but fast).
    """
    if is_view:
        return f"LIMIT {limit}"
    else:
        return f"TABLESAMPLE SYSTEM ({percentage}) LIMIT {limit}"


def results_prefix() -> str:
    """Get the database.schema prefix for results tables."""
    return f"{settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}"


def coerce_int(v: Any, *, label: str) -> int:
    """Coerce value to integer."""
    try:
        out = int(float(v))
    except Exception as e:
        raise ValueError(f"Invalid {label}: {v!r}") from e
    return out


def coerce_num(v: Any, *, label: str) -> float:
    """Coerce value to float, returning -1.0 for invalid/missing values."""
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


def normalize_template_config(cfg: Any) -> dict[str, Any]:
    """
    Normalize template config to the authoritative CUSTOM workload definition.

    Contract:
    - workload_type must be CUSTOM (or omitted).
    - CUSTOM payload is validated server-side (pct sum, required SQL).
    """
    if not isinstance(cfg, dict):
        raise ValueError("Template config must be a JSON object")

    out: dict[str, Any] = dict(cfg)
    wt_raw = str(out.get("workload_type") or "").strip()
    wt = wt_raw.upper() if wt_raw else "CUSTOM"

    if wt != "CUSTOM":
        raise ValueError("Invalid workload_type: expected 'CUSTOM'")
    out["workload_type"] = "CUSTOM"

    defaults = (
        _DEFAULT_CUSTOM_QUERIES_POSTGRES
        if is_postgres_family_table_type(out.get("table_type"))
        else _DEFAULT_CUSTOM_QUERIES_SNOWFLAKE
    )
    for k in _CUSTOM_QUERY_FIELDS:
        out[k] = str(out.get(k) or defaults.get(k) or "").strip()

    for k in _CUSTOM_PCT_FIELDS:
        out[k] = coerce_int(out.get(k) or 0, label=k)
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

    # Normalize target fields
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

    for kind, (pct_k, p95_k, p99_k, err_k) in target_fields.items():
        pct = int(out.get(pct_k) or 0)
        if pct > 0:
            out[p95_k] = coerce_num(out.get(p95_k), label=p95_k)
            out[p99_k] = coerce_num(out.get(p99_k), label=p99_k)
            out[err_k] = coerce_num(out.get(err_k), label=err_k)
        else:
            out[p95_k] = -1.0
            out[p99_k] = -1.0
            out[err_k] = -1.0

    return out


def row_to_dict(row, columns):
    """Convert a database row to a dictionary."""
    return dict(zip(columns, row))
