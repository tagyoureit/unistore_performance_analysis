"""
Table Profiler (Snowflake-first)

Provides lightweight, low-cost table "profiling" to support adaptive workload
query generation for existing production schemas being evaluated for migration.

This module intentionally avoids heavy scans. It uses:
- DESCRIBE TABLE (column names/types)
- MIN/MAX aggregates on chosen key columns (ID + optional time column)

Note: Hybrid tables are detected and handled specially - SAMPLE queries are
avoided because hybrid tables don't support efficient random sampling (no
micro-partition optimization).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Sequence

import logging

logger = logging.getLogger(__name__)


async def _is_hybrid_table(pool, full_table_name: str) -> bool:
    """
    Detect if a table is a hybrid table using SHOW TABLES.

    Hybrid tables require special handling because:
    - SAMPLE queries are inefficient (no micro-partition optimization)
    - No query result caching
    - Row-level storage optimized for OLTP, not analytical sampling

    Returns True if the table is a hybrid table, False otherwise.
    """
    # Parse the fully qualified table name: "DB"."SCHEMA"."TABLE" or DB.SCHEMA.TABLE
    # Handle both quoted and unquoted identifiers
    parts = re.split(r'\.(?=(?:[^"]*"[^"]*")*[^"]*$)', full_table_name)
    if len(parts) != 3:
        logger.warning(
            "Could not parse table name %s for hybrid detection, assuming standard table",
            full_table_name,
        )
        return False

    db_name = parts[0].strip('"')
    schema_name = parts[1].strip('"')
    table_name = parts[2].strip('"')

    try:
        # Escape single quotes for safety (SHOW TABLES uses string literals).
        safe_name = str(table_name).replace("'", "''")
        show_sql = (
            f'SHOW TABLES LIKE \'{safe_name}\' IN SCHEMA "{db_name}"."{schema_name}"'
        )

        # Preferred path: if this is our SnowflakeConnectionPool, use cursor.description
        # to reliably locate the `is_hybrid` column (Snowflake sometimes inserts columns,
        # shifting tuple indices across versions).
        if hasattr(pool, "get_connection") and hasattr(pool, "_run_in_executor"):
            async with pool.get_connection() as conn:
                cursor = await pool._run_in_executor(conn.cursor)
                try:
                    await pool._run_in_executor(cursor.execute, show_sql)
                    desc = list(getattr(cursor, "description", None) or [])
                    rows = await pool._run_in_executor(cursor.fetchall)
                finally:
                    await pool._run_in_executor(cursor.close)

            if desc:
                col_names = [str(d[0] or "").strip().lower() for d in desc]
                try:
                    idx = col_names.index("is_hybrid")
                except ValueError:
                    idx = None

                if idx is not None:
                    for row in rows:
                        if (
                            row
                            and len(row) > idx
                            and str(row[idx]).strip().upper() == "Y"
                        ):
                            logger.debug(
                                "Table %s detected as hybrid table", full_table_name
                            )
                            return True
                    return False

            # Fall through to tuple-based heuristics using the fetched rows.
            rows_fallback = rows
        else:
            rows_fallback = await pool.execute_query(show_sql)

        if not rows_fallback:
            return False

        # Heuristic fallback: in current SHOW TABLES output, the last 4 Y/N flags are
        # typically: is_hybrid, is_iceberg, is_dynamic, is_immutable.
        for row in rows_fallback:
            if not row or len(row) < 4:
                continue
            tail4 = [str(v or "").strip().upper() for v in row[-4:]]
            if all(v in {"Y", "N"} for v in tail4):
                return tail4[0] == "Y"

        return False
    except Exception as e:
        logger.warning(
            "Failed to detect hybrid table status for %s: %s, assuming standard table",
            full_table_name,
            e,
        )
        return False


async def _is_view(pool, full_table_name: str) -> bool:
    """
    Detect if the object is a view (not a table).

    Views don't support TABLESAMPLE or efficient SAMPLE operations.
    For views, we skip sample-based analysis to avoid timeouts.
    """
    # Parse the fully qualified table name: "DB"."SCHEMA"."TABLE"
    parts = re.split(r'\.(?=(?:[^"]*"[^"]*")*[^"]*$)', full_table_name)
    if len(parts) != 3:
        return False

    db_name = parts[0].strip('"')
    schema_name = parts[1].strip('"')
    table_name = parts[2].strip('"')

    try:
        rows = await pool.execute_query(
            f"""
            SELECT TABLE_TYPE
            FROM "{db_name}".INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema_name}' AND TABLE_NAME = '{table_name}'
            """
        )
        if rows and str(rows[0][0]).upper() == "VIEW":
            logger.debug("Object %s detected as VIEW", full_table_name)
            return True
        return False
    except Exception as e:
        logger.warning(
            "Failed to detect view status for %s: %s, assuming table",
            full_table_name,
            e,
        )
        return False


@dataclass(frozen=True)
class TableProfile:
    full_table_name: str

    id_column: Optional[str] = None
    id_min: Optional[int] = None
    id_max: Optional[int] = None

    time_column: Optional[str] = None
    time_min: Optional[datetime] = None
    time_max: Optional[datetime] = None


def _pick_id_column(columns: dict[str, str]) -> Optional[str]:
    """
    Pick a likely key column (used for point lookups / updates).

    This is intentionally heuristic because Snowflake constraints are often absent
    on standard tables. We prioritize common naming conventions and numeric types.
    """

    def _is_numeric_type(typ: str) -> bool:
        t = str(typ or "").upper()
        return any(x in t for x in ("NUMBER", "INT", "BIGINT", "DECIMAL"))

    # Prefer exact ID, then *_ID / *ID numeric-ish columns, then *_KEY / *KEY numeric-ish.
    if "ID" in columns:
        return "ID"

    id_like: list[str] = []
    key_like: list[str] = []
    contains_id: list[str] = []
    contains_key: list[str] = []

    for name, typ in columns.items():
        if not _is_numeric_type(typ):
            continue
        n = str(name or "").upper()
        if n.endswith("_ID") or (n.endswith("ID") and n != "ID"):
            id_like.append(n)
            continue
        if n.endswith("_KEY") or n.endswith("KEY"):
            key_like.append(n)
            continue
        if "ID" in n:
            contains_id.append(n)
            continue
        if "KEY" in n:
            contains_key.append(n)
            continue

    for bucket in (id_like, key_like, contains_id, contains_key):
        if bucket:
            return bucket[0]

    return None


def _pick_time_column(columns: dict[str, str]) -> Optional[str]:
    # Prefer common time columns, else any DATE/TIMESTAMP-ish column.
    preferred = [
        "TIMESTAMP",
        "CREATED_AT",
        "UPDATED_AT",
        "EVENT_TIME",
        "CREATED",
        "UPDATED",
        "DATE",
    ]
    for name in preferred:
        if name in columns and any(
            t in columns[name] for t in ("TIMESTAMP", "DATE", "TIME")
        ):
            return name

    for name, typ in columns.items():
        if any(t in typ for t in ("TIMESTAMP", "DATE", "TIME")):
            return name

    return None


def _id_candidates(columns: dict[str, str]) -> list[str]:
    """
    Return ordered candidate key columns based on naming + type heuristics.
    """

    def _is_numeric_type(typ: str) -> bool:
        t = str(typ or "").upper()
        return any(x in t for x in ("NUMBER", "INT", "BIGINT", "DECIMAL"))

    # Prefer exact ID if present (even if not numeric), but still allow other candidates.
    candidates: list[str] = []
    if "ID" in columns:
        candidates.append("ID")

    id_like: list[str] = []
    key_like: list[str] = []
    contains_id: list[str] = []
    contains_key: list[str] = []

    for name, typ in columns.items():
        n = str(name or "").upper()
        if n == "ID":
            continue
        if not _is_numeric_type(typ):
            continue
        if n.endswith("_ID") or n.endswith("ID"):
            id_like.append(n)
            continue
        if n.endswith("_KEY") or n.endswith("KEY"):
            key_like.append(n)
            continue
        if "ID" in n:
            contains_id.append(n)
            continue
        if "KEY" in n:
            contains_key.append(n)
            continue

    for bucket in (id_like, key_like, contains_id, contains_key):
        for c in bucket:
            if c not in candidates:
                candidates.append(c)
    return candidates


async def _best_key_candidate_from_sample(
    pool, full_table_name: str, candidates: Sequence[str], *, sample_rows: int
) -> Optional[str]:
    """
    Choose the best key candidate by distinctness on a random SAMPLE.

    This avoids full-table COUNT(DISTINCT ...) on very large customer tables.
    """
    eval_cols = [str(c).upper() for c in candidates if str(c).strip()][:8]
    if not eval_cols:
        return None

    sample_n = max(1000, min(int(sample_rows), 20000))
    parts: list[str] = []
    for idx, col in enumerate(eval_cols):
        parts.append(f'COUNT_IF("{col}" IS NULL) AS NULLS_{idx}')
        parts.append(f'COUNT(DISTINCT "{col}") AS DISTINCT_{idx}')

    rows = await pool.execute_query(
        f"""
        SELECT
          COUNT(*) AS N,
          {", ".join(parts)}
        FROM {full_table_name}
        SAMPLE ({sample_n} ROWS)
        """
    )
    if not rows or not rows[0]:
        return eval_cols[0]

    row = rows[0]
    try:
        n = int(row[0] or 0)
    except Exception:
        n = 0
    if n <= 0:
        return eval_cols[0]

    best_col: str | None = None
    best_ratio = -1.0
    best_distinct = -1

    # Columns appear as: N, NULLS_0, DISTINCT_0, NULLS_1, DISTINCT_1, ...
    base = 1
    for idx, col in enumerate(eval_cols):
        nulls_raw = row[base + (2 * idx)]
        dist_raw = row[base + (2 * idx) + 1]
        nulls = int(nulls_raw or 0)
        distinct = int(dist_raw or 0)
        non_null = max(1, n - nulls)
        ratio = float(distinct) / float(non_null)

        if ratio > best_ratio or (ratio == best_ratio and distinct > best_distinct):
            best_ratio = ratio
            best_distinct = distinct
            best_col = col

    # "Usable key" threshold: nearly unique in sample and not mostly NULL.
    if best_col is not None and best_ratio >= 0.98:
        return best_col
    return None


async def profile_snowflake_table(
    pool,
    full_table_name: str,
    *,
    include_bounds: bool = True,
) -> TableProfile:
    """
    Profile a Snowflake table using minimal metadata queries.

    pool must provide: `await pool.execute_query(query, params=None)`

    Note: For hybrid tables, we skip SAMPLE-based key candidate detection
    because SAMPLE is inefficient on hybrid tables (no micro-partition
    optimization, different storage architecture).
    """
    desc_rows = await pool.execute_query(f"DESCRIBE TABLE {full_table_name}")
    # Rows look like: (name, type, kind, null?, default?, ..., comment)
    columns: dict[str, str] = {}
    for row in desc_rows:
        if not row:
            continue
        col_name = str(row[0]).upper()
        col_type = str(row[1]).upper() if len(row) > 1 else ""
        columns[col_name] = col_type

    # Detect hybrid table or view to skip expensive SAMPLE queries
    is_hybrid = await _is_hybrid_table(pool, full_table_name)
    is_view_obj = await _is_view(pool, full_table_name)
    skip_sample = is_hybrid or is_view_obj

    if is_hybrid:
        logger.info(
            "Table %s is a hybrid table - skipping SAMPLE-based key detection",
            full_table_name,
        )
    if is_view_obj:
        logger.info(
            "Object %s is a view - skipping SAMPLE-based key detection",
            full_table_name,
        )

    candidates = _id_candidates(columns)
    id_col: Optional[str]
    if not candidates:
        id_col = None
    elif len(candidates) == 1:
        id_col = candidates[0]
    elif skip_sample:
        # For hybrid tables and views, use heuristic selection (first candidate) to avoid
        # expensive SAMPLE queries that can timeout.
        id_col = candidates[0]
        logger.debug(
            "Using heuristic key selection for %s: %s",
            full_table_name,
            id_col,
        )
    else:
        # Break ties via SAMPLE-based distinctness (cheap) before falling back to first candidate.
        id_col = await _best_key_candidate_from_sample(
            pool, full_table_name, candidates, sample_rows=5000
        )
        if id_col is None:
            id_col = candidates[0]
    time_col = _pick_time_column(columns)

    id_min: Optional[int] = None
    id_max: Optional[int] = None
    time_min: Optional[datetime] = None
    time_max: Optional[datetime] = None

    if include_bounds:
        if id_col:
            rows = await pool.execute_query(
                f'SELECT MIN("{id_col}") AS MIN_ID, MAX("{id_col}") AS MAX_ID FROM {full_table_name}'
            )
            if rows and len(rows[0]) >= 2:
                id_min = int(rows[0][0]) if rows[0][0] is not None else None
                id_max = int(rows[0][1]) if rows[0][1] is not None else None

        if time_col:
            rows = await pool.execute_query(
                f'SELECT MIN("{time_col}") AS MIN_T, MAX("{time_col}") AS MAX_T FROM {full_table_name}'
            )
            if rows and len(rows[0]) >= 2:
                time_min = rows[0][0]
                time_max = rows[0][1]

    profile = TableProfile(
        full_table_name=full_table_name,
        id_column=id_col,
        id_min=id_min,
        id_max=id_max,
        time_column=time_col,
        time_min=time_min,
        time_max=time_max,
    )

    logger.info(
        "Profiled Snowflake table %s: id=%s[%s..%s], time=%s[%s..%s]",
        full_table_name,
        profile.id_column,
        profile.id_min,
        profile.id_max,
        profile.time_column,
        profile.time_min,
        profile.time_max,
    )
    return profile


def _pick_id_column_case_insensitive(columns: dict[str, str]) -> Optional[str]:
    """
    Pick a likely key column (used for point lookups / updates) - case insensitive.

    For PostgreSQL which stores unquoted identifiers in lowercase.
    """

    def _is_numeric_type(typ: str) -> bool:
        t = str(typ or "").upper()
        return any(x in t for x in ("NUMBER", "INT", "BIGINT", "DECIMAL", "SERIAL"))

    upper_to_orig = {k.upper(): k for k in columns}

    if "ID" in upper_to_orig:
        return upper_to_orig["ID"]

    id_like: list[str] = []
    key_like: list[str] = []
    contains_id: list[str] = []
    contains_key: list[str] = []

    for name, typ in columns.items():
        if not _is_numeric_type(typ):
            continue
        n = str(name or "").upper()
        if n == "ID":
            continue
        if n.endswith("_ID") or n.endswith("ID"):
            id_like.append(name)
            continue
        if n.endswith("_KEY") or n.endswith("KEY"):
            key_like.append(name)
            continue
        if "ID" in n:
            contains_id.append(name)
            continue
        if "KEY" in n:
            contains_key.append(name)
            continue

    for bucket in (id_like, key_like, contains_id, contains_key):
        if bucket:
            return bucket[0]

    return None


def _pick_time_column_case_insensitive(columns: dict[str, str]) -> Optional[str]:
    """
    Pick a time column - case insensitive.

    For PostgreSQL which stores unquoted identifiers in lowercase.
    """
    upper_to_orig = {k.upper(): k for k in columns}

    preferred = [
        "TIMESTAMP",
        "CREATED_AT",
        "UPDATED_AT",
        "EVENT_TIME",
        "CREATED",
        "UPDATED",
        "DATE",
    ]
    for name in preferred:
        if name in upper_to_orig:
            orig_name = upper_to_orig[name]
            typ = columns.get(orig_name, "")
            if any(t in typ.upper() for t in ("TIMESTAMP", "DATE", "TIME")):
                return orig_name

    for name, typ in columns.items():
        if any(t in typ.upper() for t in ("TIMESTAMP", "DATE", "TIME")):
            return name

    return None


async def profile_postgres_table(
    pool,
    full_table_name: str,
    *,
    include_bounds: bool = True,
) -> TableProfile:
    """
    Profile a PostgreSQL table using minimal metadata queries.

    Unlike profile_snowflake_table, this preserves original column case
    and uses unquoted identifiers in queries (PostgreSQL folds to lowercase).

    pool must provide: `await pool.fetch_all(query, *params)`
    """
    parts = full_table_name.split(".")
    if len(parts) == 2:
        schema_name, table_name = parts
    else:
        schema_name = "public"
        table_name = full_table_name

    rows = await pool.fetch_all(
        """
        SELECT column_name, data_type, udt_name
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = $2
        ORDER BY ordinal_position
        """,
        schema_name,
        table_name,
    )

    columns: dict[str, str] = {}
    for row in rows:
        if not row:
            continue
        col_name = str(row["column_name"] if hasattr(row, "__getitem__") else row[0])
        data_type = str(row["data_type"] if hasattr(row, "__getitem__") else row[1])
        columns[col_name] = data_type.upper()

    id_col = _pick_id_column_case_insensitive(columns)
    time_col = _pick_time_column_case_insensitive(columns)

    id_min: Optional[int] = None
    id_max: Optional[int] = None
    time_min: Optional[datetime] = None
    time_max: Optional[datetime] = None

    if include_bounds:
        if id_col:
            bound_rows = await pool.fetch_all(
                f"SELECT MIN({id_col}) AS min_id, MAX({id_col}) AS max_id FROM {full_table_name}"
            )
            if bound_rows and len(bound_rows) > 0:
                row = bound_rows[0]
                min_val = row["min_id"] if hasattr(row, "__getitem__") else row[0]
                max_val = row["max_id"] if hasattr(row, "__getitem__") else row[1]
                id_min = int(min_val) if min_val is not None else None
                id_max = int(max_val) if max_val is not None else None

        if time_col:
            bound_rows = await pool.fetch_all(
                f"SELECT MIN({time_col}) AS min_t, MAX({time_col}) AS max_t FROM {full_table_name}"
            )
            if bound_rows and len(bound_rows) > 0:
                row = bound_rows[0]
                time_min = row["min_t"] if hasattr(row, "__getitem__") else row[0]
                time_max = row["max_t"] if hasattr(row, "__getitem__") else row[1]

    profile = TableProfile(
        full_table_name=full_table_name,
        id_column=id_col,
        id_min=id_min,
        id_max=id_max,
        time_column=time_col,
        time_min=time_min,
        time_max=time_max,
    )

    logger.info(
        "Profiled PostgreSQL table %s: id=%s[%s..%s], time=%s[%s..%s]",
        full_table_name,
        profile.id_column,
        profile.id_min,
        profile.id_max,
        profile.time_column,
        profile.time_min,
        profile.time_max,
    )
    return profile
