"""
Constants and default values for template management.
"""

import re

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
#
# These are intentionally generic starting points (phantom table) and are stored per-template.
# Execution substitutes `{table}` with the fully-qualified table name.
#
# IMPORTANT: Postgres templates must store Postgres SQL; Snowflake templates must store Snowflake SQL.
# Note: Range scan uses BETWEEN without LIMIT - the offset (100) constrains row count,
# avoiding LIMIT-based early termination optimization which can skew benchmark results.
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
    # Prefer fully parameterized inserts/updates so executors can generate values.
    "custom_insert_query": "INSERT INTO {table} (id, data, timestamp) VALUES ($1, $2, $3)",
    "custom_update_query": "UPDATE {table} SET data = $1, timestamp = $2 WHERE id = $3",
}

