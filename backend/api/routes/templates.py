"""
API routes for test scenario template management.

Manages templates stored in Snowflake TEST_TEMPLATES table.
"""

from datetime import UTC, datetime
import logging
import ssl
from uuid import uuid4
from typing import List, Dict, Any, Optional

import asyncpg
from fastapi import APIRouter, HTTPException, status

from backend.config import settings
from backend.connectors import postgres_pool, snowflake_pool
from backend.core import connection_manager
from backend.api.error_handling import http_exception
from backend.core.table_profiler import profile_snowflake_table

# Import from extracted modules
from backend.api.routes.templates_modules.constants import (
    _CUSTOM_QUERY_FIELDS,
    _CUSTOM_PCT_FIELDS,
    _DEFAULT_CUSTOM_QUERIES_SNOWFLAKE,
    _DEFAULT_CUSTOM_QUERIES_POSTGRES,
)
from backend.api.routes.templates_modules.utils import (
    _upper_str,
    _validate_ident,
    _quote_ident,
    _is_postgres_family_table_type,
    _pg_quote_ident,
    _pg_qualified_name,
    _pg_placeholders,
    _full_table_name,
    _sample_clause,
    _results_prefix,
    _coerce_int,
    _enrich_postgres_instance_size,
    _row_to_dict,
)
from backend.api.routes.templates_modules.config_normalizer import _normalize_template_config
from backend.api.routes.templates_modules.models import (
    TemplateConfig,
    TemplateCreate,
    TemplateUpdate,
    TemplateResponse,
    AiPrepareResponse,
    AiAdjustSqlRequest,
    AiAdjustSqlResponse,
)

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/", response_model=List[TemplateResponse])
async def list_templates():
    """
    List all available test configuration templates.

    Returns:
        List of all templates with metadata
    """
    try:
        pool = snowflake_pool.get_default_pool()

        query = f"""
        SELECT 
            TEMPLATE_ID,
            TEMPLATE_NAME,
            DESCRIPTION,
            CONFIG,
            CREATED_AT,
            UPDATED_AT,
            CREATED_BY,
            TAGS,
            USAGE_COUNT,
            LAST_USED_AT
        FROM {_results_prefix()}.TEST_TEMPLATES
        ORDER BY UPDATED_AT DESC
        """

        results = await pool.execute_query(query)

        columns = [
            "TEMPLATE_ID",
            "TEMPLATE_NAME",
            "DESCRIPTION",
            "CONFIG",
            "CREATED_AT",
            "UPDATED_AT",
            "CREATED_BY",
            "TAGS",
            "USAGE_COUNT",
            "LAST_USED_AT",
        ]

        templates = []
        for row in results:
            row_dict = _row_to_dict(row, columns)
            # Parse JSON strings from VARIANT columns
            import json

            config = row_dict["config"]
            if isinstance(config, str):
                config = json.loads(config)
            tags = row_dict["tags"]
            if isinstance(tags, str) and tags:
                tags = json.loads(tags)

            templates.append(
                {
                    "template_id": row_dict["template_id"],
                    "template_name": row_dict["template_name"],
                    "description": row_dict["description"],
                    "config": config,
                    "created_at": row_dict["created_at"],
                    "updated_at": row_dict["updated_at"],
                    "created_by": row_dict["created_by"],
                    "tags": tags,
                    "usage_count": row_dict["usage_count"] or 0,
                    "last_used_at": row_dict["last_used_at"],
                }
            )

        return templates

    except Exception as e:
        raise http_exception("list templates", e)


async def _ai_adjust_sql_postgres(sf_pool, cfg: dict[str, Any]) -> AiAdjustSqlResponse:
    """
    AI SQL adjustment for Postgres-family templates.

    This endpoint introspects the *actual* Postgres table to choose key/time columns and
    safe insert/update columns.
    """
    import json

    # Connection target (Postgres-family):
    # - database: Postgres database name (case-sensitive at connect time)
    # - schema/table_name: used for metadata queries and sample reads
    db = str(cfg.get("database") or "").strip()
    schema = str(cfg.get("schema") or "").strip()
    table = str(cfg.get("table_name") or "").strip()
    if not db:
        raise ValueError("database is required (select an existing Postgres database)")
    if not schema:
        raise ValueError("schema is required (select an existing Postgres schema)")
    if not table:
        raise ValueError(
            "table_name is required (select an existing Postgres table/view)"
        )

    table_type = _upper_str(cfg.get("table_type") or "")
    connection_id = cfg.get("connection_id")
    
    # Create pool using stored connection credentials if available
    if connection_id:
        conn_params = await connection_manager.get_connection_for_pool(connection_id)
        if not conn_params:
            raise ValueError(f"Connection not found: {connection_id}")
        if conn_params.get("connection_type") != "POSTGRES":
            raise ValueError(f"Connection {connection_id} is not a Postgres connection")
        
        # Create SSL context for Snowflake Postgres (self-signed certs)
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        pg_pool = postgres_pool.PostgresConnectionPool(
            host=conn_params["host"],
            port=conn_params["port"],
            database=db,
            user=conn_params["user"],
            password=conn_params["password"],
            min_size=1,
            max_size=2,
            pool_name="ai_adjust_sql",
            ssl=ssl_context,
        )
    else:
        # Fallback to environment variables (backward compat)
        pg_pool = postgres_pool.get_pool_for_database(db)
    
    full_name = _pg_qualified_name(schema, table)

    concurrency = int(cfg.get("concurrent_connections") or 1)
    concurrency = max(1, concurrency)

    # ------------------------------------------------------------------
    # 1) Cortex availability check (fast, via Snowflake pool)
    # ------------------------------------------------------------------
    ai_available = False
    ai_error: str | None = None
    try:
        await sf_pool.execute_query("SELECT AI_COMPLETE('claude-4-sonnet', 'test')")
        ai_available = True
    except Exception as e:
        ai_available = False
        ai_error = str(e)

    # ------------------------------------------------------------------
    # 2) Introspect columns from information_schema (Postgres)
    # ------------------------------------------------------------------
    desc_rows = await pg_pool.fetch_all(
        """
        SELECT
          column_name,
          data_type,
          udt_name,
          is_nullable,
          column_default
        FROM information_schema.columns
        WHERE table_schema = $1
          AND table_name = $2
        ORDER BY ordinal_position
        """,
        schema,
        table,
    )
    if not desc_rows:
        raise ValueError(f"No columns found for {schema}.{table}")

    col_types: dict[str, str] = {}
    col_null_ok: dict[str, bool] = {}
    col_default: dict[str, str] = {}
    col_orig: dict[str, str] = {}
    for row in desc_rows:
        name_raw = str(row.get("column_name") or "").strip()
        if not name_raw:
            continue
        name_u = name_raw.upper()
        typ_raw = str(row.get("data_type") or row.get("udt_name") or "").strip()
        typ_u = typ_raw.upper()
        col_types[name_u] = typ_u
        col_orig[name_u] = name_raw
        null_raw = str(row.get("is_nullable") or "").strip().upper()
        col_null_ok[name_u] = null_raw != "NO"
        col_default[name_u] = str(row.get("column_default") or "").strip()

    def _is_key_or_id_like(col: str) -> bool:
        c = str(col or "").strip().upper()
        if not c:
            return False
        return c == "ID" or c.endswith("ID") or c.endswith("KEY")

    def _is_numeric_type(typ: str) -> bool:
        t = str(typ or "").upper()
        return any(
            x in t
            for x in (
                "INT",
                "BIGINT",
                "SMALLINT",
                "NUMERIC",
                "DECIMAL",
                "REAL",
                "DOUBLE",
                "FLOAT",
                "SERIAL",
            )
        )

    def _is_time_type(typ: str) -> bool:
        t = str(typ or "").upper()
        return any(x in t for x in ("TIMESTAMP", "DATE", "TIME"))

    # Pick key/time columns (heuristic but based on actual schema)
    key_col: str | None = None
    time_col: str | None = None

    if "ID" in col_types and _is_numeric_type(col_types.get("ID", "")):
        key_col = "ID"
    else:
        for c in col_types.keys():
            if _is_key_or_id_like(c) and _is_numeric_type(col_types.get(c, "")):
                key_col = c
                break
        if key_col is None:
            for c in col_types.keys():
                if c.endswith("_ID") and _is_numeric_type(col_types.get(c, "")):
                    key_col = c
                    break
        if key_col is None:
            for c in col_types.keys():
                if c.endswith("_KEY") and _is_numeric_type(col_types.get(c, "")):
                    key_col = c
                    break

    preferred_time = [
        "TIMESTAMP",
        "CREATED_AT",
        "UPDATED_AT",
        "EVENT_TIME",
        "CREATED",
        "UPDATED",
        "DATE",
    ]
    for c in preferred_time:
        if c in col_types and _is_time_type(col_types.get(c, "")):
            time_col = c
            break
    if time_col is None:
        for c, typ in col_types.items():
            if _is_time_type(typ):
                time_col = c
                break

    def _pick_update_column() -> str | None:
        if not key_col:
            return None
        preferred = ["UPDATED_AT", "STATUS", "STATE", "UPDATED", "MODIFIED_AT"]
        for c in preferred:
            if c in col_types and (not _is_key_or_id_like(c)) and c != key_col:
                return c
        for c in col_types.keys():
            if c != key_col and not _is_key_or_id_like(c):
                return c
        return None

    update_col = _pick_update_column()
    if update_col and (_is_key_or_id_like(update_col) or update_col == key_col):
        update_col = None

    issues: list[str] = []
    range_mode: str | None = None

    def _has_default(col: str) -> bool:
        d = (col_default.get(col) or "").strip()
        return bool(d and d.upper() not in {"NULL", "NONE"})

    required_cols: list[str] = []
    for c in col_types.keys():
        if not col_null_ok.get(c, True) and not _has_default(c):
            required_cols.append(c)
    if len(required_cols) > 8:
        issues.append(
            f"Table has {len(required_cols)} required columns; INSERT may need manual adjustment."
        )

    insert_cols: list[str] = []
    for c in required_cols[:8]:
        insert_cols.append(c)
    if key_col and key_col not in insert_cols:
        insert_cols.append(key_col)
    if time_col and time_col not in insert_cols:
        insert_cols.append(time_col)
    for c, typ in col_types.items():
        if c in insert_cols:
            continue
        if any(x in typ for x in ("CHAR", "VARCHAR", "TEXT")):
            insert_cols.append(c)
        if len(insert_cols) >= 6:
            break
    if not insert_cols:
        insert_cols = list(col_types.keys())[:3]

    # Optional: ask Cortex (Snowflake AI_COMPLETE) to refine based on schema + sample rows.
    ai_summary_from_model: str | None = None
    if ai_available:
        try:
            sample_cols: list[str] = []
            if key_col:
                sample_cols.append(key_col)
            if time_col and time_col not in sample_cols:
                sample_cols.append(time_col)
            for c in col_types.keys():
                if c in sample_cols:
                    continue
                if len(sample_cols) >= 12:
                    break
                sample_cols.append(c)

            select_list = ", ".join(
                _pg_quote_ident(col_orig.get(c, c)) for c in sample_cols
            )
            sample_rows = await pg_pool.fetch_all(
                f"SELECT {select_list} FROM {full_name} LIMIT 20"
            )
            sample_payload = [dict(r) for r in sample_rows]

            cols_for_prompt = [
                {"name": c, "type": col_types.get(c, "")}
                for c in list(col_types.keys())[:250]
            ]

            prompt = (
                "You are adjusting a 4-statement benchmark workload for a Postgres table.\n"
                "Your output will be shown to the user.\n\n"
                f"POSTGRES_DATABASE: {db}\n"
                f"SCHEMA: {schema}\n"
                f"TABLE: {table}\n"
                f"KEY_COLUMN (may be null): {key_col or None}\n"
                f"TIME_COLUMN (may be null): {time_col or None}\n\n"
                "COLUMNS (name/type):\n"
                f"{json.dumps(cols_for_prompt, ensure_ascii=False)}\n\n"
                "REQUIRED_COLUMNS (must be included in insert_columns if feasible):\n"
                f"{json.dumps(required_cols, ensure_ascii=False)}\n\n"
                "SAMPLE_ROWS (JSON objects):\n"
                f"{json.dumps(sample_payload, ensure_ascii=False, default=str)}\n\n"
                "Return STRICT JSON ONLY with:\n"
                "- summary: string (1-2 sentences describing what you changed)\n"
                "- insert_columns: [string] (columns to include in INSERT placeholders)\n"
                "- update_columns: [string] (columns to include in UPDATE set clause)\n"
                '- range_mode: one of ["TIME_CUTOFF","ID_BETWEEN",null]\n'
                "- issues: [string] (empty if all OK)\n"
                "\n"
                "Rules:\n"
                "- If KEY_COLUMN is null: update_columns must be empty.\n"
                "- update_columns must NOT include any columns ending with ID or KEY.\n"
                "- If TIME_COLUMN is null and KEY_COLUMN is null: range_mode must be null.\n"
                "- Prefer TIME_CUTOFF if TIME_COLUMN exists; otherwise ID_BETWEEN if KEY_COLUMN exists.\n"
                "- Keep insert_columns <= 8 and update_columns <= 2.\n"
            )

            ai_resp = await sf_pool.execute_query(
                "SELECT AI_COMPLETE(model => ?, prompt => ?, model_parameters => PARSE_JSON(?), response_format => PARSE_JSON(?)) AS RESP",
                params=[
                    "claude-4-sonnet",
                    prompt,
                    json.dumps({"temperature": 0, "max_tokens": 600}),
                    json.dumps(
                        {
                            "type": "json",
                            "schema": {
                                "type": "object",
                                "properties": {
                                    "summary": {"type": "string"},
                                    "insert_columns": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                    },
                                    "update_columns": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                    },
                                    "range_mode": {
                                        "type": ["string", "null"],
                                    },
                                    "issues": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                    },
                                },
                                "required": [
                                    "summary",
                                    "insert_columns",
                                    "update_columns",
                                    "range_mode",
                                    "issues",
                                ],
                            },
                        }
                    ),
                ],
            )

            raw = ai_resp[0][0] if ai_resp and ai_resp[0] else None
            parsed: dict[str, Any] | None = None
            if isinstance(raw, dict):
                parsed = raw
            elif isinstance(raw, str) and raw.strip():
                parsed = json.loads(raw)

            if isinstance(parsed, dict):
                ai_summary_from_model = str(parsed.get("summary") or "").strip() or None
                ai_issues = parsed.get("issues")
                if isinstance(ai_issues, list):
                    issues.extend(str(x) for x in ai_issues if str(x).strip())

                def _sanitize_cols(value: Any) -> list[str]:
                    if not isinstance(value, list):
                        return []
                    out: list[str] = []
                    for v in value:
                        s = str(v or "").strip().upper()
                        if not s:
                            continue
                        if s in col_types:
                            out.append(s)
                    # de-dupe, preserve order
                    seen: set[str] = set()
                    deduped: list[str] = []
                    for c in out:
                        if c in seen:
                            continue
                        seen.add(c)
                        deduped.append(c)
                    return deduped

                ai_ins = _sanitize_cols(parsed.get("insert_columns"))[:8]
                if ai_ins:
                    insert_cols = ai_ins

                ai_upd = _sanitize_cols(parsed.get("update_columns"))[:2]
                if key_col and ai_upd:
                    upd_filtered = [
                        c
                        for c in ai_upd
                        if (not _is_key_or_id_like(c)) and c != key_col
                    ]
                    if upd_filtered:
                        update_col = upd_filtered[0]

                rm = parsed.get("range_mode")
                if rm in ("TIME_CUTOFF", "ID_BETWEEN"):
                    range_mode = str(rm)
        except Exception as e:
            logger.debug("AI planning failed in /ai/adjust-sql (postgres): %s", e)

    # ------------------------------------------------------------------
    # 3) Build Postgres SQL templates
    # ------------------------------------------------------------------
    select_list = "*"
    point_sql = ""
    update_sql = ""

    key_expr = _pg_quote_ident(col_orig.get(key_col, key_col)) if key_col else ""
    time_expr = _pg_quote_ident(col_orig.get(time_col, time_col)) if time_col else ""
    update_expr = (
        _pg_quote_ident(col_orig.get(update_col, update_col)) if update_col else ""
    )

    if key_col:
        point_sql = f"SELECT {select_list} FROM {{table}} WHERE {key_expr} = $1"
        if update_col:
            update_sql = (
                f"UPDATE {{table}} SET {update_expr} = $1 WHERE {key_expr} = $2"
            )

    range_sql = ""
    if range_mode == "ID_BETWEEN":
        if key_col:
            range_sql = (
                f"SELECT {select_list} FROM {{table}} WHERE {key_expr} BETWEEN $1 AND $2 "
                f"ORDER BY {key_expr} LIMIT 100"
            )
        else:
            range_mode = None
    elif time_col:
        range_sql = (
            f"SELECT {select_list} FROM {{table}} WHERE {time_expr} >= $1 "
            f"ORDER BY {time_expr} DESC LIMIT 100"
        )
        range_mode = "TIME_CUTOFF"
    elif key_col:
        range_sql = (
            f"SELECT {select_list} FROM {{table}} WHERE {key_expr} BETWEEN $1 AND $2 "
            f"ORDER BY {key_expr} LIMIT 100"
        )
        range_mode = "ID_BETWEEN"

    insert_sql = ""
    if insert_cols:
        cols_sql = ", ".join(_pg_quote_ident(col_orig.get(c, c)) for c in insert_cols)
        ph_sql = _pg_placeholders(len(insert_cols), start=1)
        insert_sql = f"INSERT INTO {{table}} ({cols_sql}) VALUES ({ph_sql})"

    # Percentages: keep the user's mix by default.
    p_point = int(cfg.get("custom_point_lookup_pct") or 0)
    p_range = int(cfg.get("custom_range_scan_pct") or 0)
    p_ins = int(cfg.get("custom_insert_pct") or 0)
    p_upd = int(cfg.get("custom_update_pct") or 0)

    # Only adjust mix if we cannot generate SQL for an operation that currently has weight > 0.
    disabled: list[tuple[str, str]] = []
    if p_point > 0 and not point_sql:
        disabled.append(
            ("POINT_LOOKUP", "No usable key column detected; POINT_LOOKUP disabled.")
        )
    if p_range > 0 and not range_sql:
        disabled.append(
            ("RANGE_SCAN", "No usable time/key column detected; RANGE_SCAN disabled.")
        )
    if p_ins > 0 and not insert_sql:
        disabled.append(
            ("INSERT", "No usable insert columns detected; INSERT disabled.")
        )
    if p_upd > 0 and not update_sql:
        disabled.append(
            ("UPDATE", "No usable update/key column detected; UPDATE disabled.")
        )

    if disabled:
        for _, msg in disabled:
            issues.append(msg)

        removed = 0
        if any(k == "POINT_LOOKUP" for k, _ in disabled):
            removed += p_point
            p_point = 0
        if any(k == "RANGE_SCAN" for k, _ in disabled):
            removed += p_range
            p_range = 0
        if any(k == "INSERT" for k, _ in disabled):
            removed += p_ins
            p_ins = 0
        if any(k == "UPDATE" for k, _ in disabled):
            removed += p_upd
            p_upd = 0

        remaining = [
            ("POINT_LOOKUP", p_point),
            ("RANGE_SCAN", p_range),
            ("INSERT", p_ins),
            ("UPDATE", p_upd),
        ]
        remaining_total = sum(v for _, v in remaining if v > 0)
        if remaining_total > 0 and removed > 0:
            # Redistribute proportionally across remaining non-zero operations.
            alloc: dict[str, int] = {}
            used = 0
            for kind, v in remaining:
                if v <= 0:
                    continue
                add = int((removed * v) // remaining_total)
                if add > 0:
                    alloc[kind] = add
                    used += add
            leftover = max(0, removed - used)
            # Deterministic leftover distribution: highest current weight first.
            order = [
                k
                for k, v in sorted(remaining, key=lambda kv: int(kv[1]), reverse=True)
                if v > 0
            ]
            i = 0
            while leftover > 0 and order:
                alloc[order[i % len(order)]] = (
                    int(alloc.get(order[i % len(order)], 0)) + 1
                )
                leftover -= 1
                i += 1

            p_point += int(alloc.get("POINT_LOOKUP", 0))
            p_range += int(alloc.get("RANGE_SCAN", 0))
            p_ins += int(alloc.get("INSERT", 0))
            p_upd += int(alloc.get("UPDATE", 0))
        elif removed > 0:
            # Nothing left with weight; fall back to any supported query kind.
            fallback_kind = None
            if range_sql:
                fallback_kind = "RANGE_SCAN"
            elif insert_sql:
                fallback_kind = "INSERT"
            elif point_sql:
                fallback_kind = "POINT_LOOKUP"
            elif update_sql:
                fallback_kind = "UPDATE"
            if fallback_kind:
                p_point = 100 if fallback_kind == "POINT_LOOKUP" else 0
                p_range = 100 if fallback_kind == "RANGE_SCAN" else 0
                p_ins = 100 if fallback_kind == "INSERT" else 0
                p_upd = 100 if fallback_kind == "UPDATE" else 0
                issues.append(
                    f"All configured operations were unsupported; falling back to {fallback_kind}=100%."
                )

        # Guardrail: keep sum at 100; assign rounding drift to INSERT.
        total = p_point + p_range + p_ins + p_upd
        if total != 100:
            p_ins += 100 - total

    # Toast only when unexpected (mix had to change due to missing SQL).
    toast_level = "warning" if issues else "success"
    ai_summary = ai_summary_from_model or ""
    if not ai_summary:
        ai_summary = "Generated workload SQL."
    if issues:
        ai_summary = ai_summary + " Issues: " + " ".join(issues)

    cols_map: dict[str, str] = {}
    for c in {
        *(insert_cols or []),
        *([key_col] if key_col else []),
        *([time_col] if time_col else []),
    }:
        if not c:
            continue
        cols_map[str(c).upper()] = col_types.get(str(c).upper(), "VARCHAR")

    ai_workload = {
        "available": ai_available,
        "model": "claude-4-sonnet",
        "availability_error": ai_error,
        "key_column": key_col,
        "time_column": time_col,
        "range_mode": range_mode,
        "insert_columns": insert_cols,
        "update_columns": [update_col] if update_col else [],
        "postgres_database": db,
        "postgres_schema": schema,
        "postgres_table": table,
    }

    return AiAdjustSqlResponse(
        custom_point_lookup_query=point_sql,
        custom_range_scan_query=range_sql,
        custom_insert_query=insert_sql,
        custom_update_query=update_sql,
        custom_point_lookup_pct=p_point,
        custom_range_scan_pct=p_range,
        custom_insert_pct=p_ins,
        custom_update_pct=p_upd,
        columns=cols_map,
        ai_workload=ai_workload,
        toast_level=toast_level,
        summary=ai_summary,
    )


@router.post("/ai/adjust-sql", response_model=AiAdjustSqlResponse)
async def ai_adjust_sql(req: AiAdjustSqlRequest):
    """
    Preview-only AI adjustment for the canonical 4-query CUSTOM workload.

    Contract:
    - Does NOT write anything to Snowflake results tables.
    - Generates SQL for the canonical 4-query CUSTOM workload.
    - Preserves the user's mix % by default.
    - Only if an operation's SQL cannot be generated *and it currently has weight > 0*:
      that operation is disabled (pct=0) and the remainder is redistributed to keep total=100.
    - Toasts are only emitted by the client when the result is "unexpected" (i.e., issues).
    """
    try:
        pool = snowflake_pool.get_default_pool()
        cfg = _normalize_template_config(req.config)

        table_type = _upper_str(cfg.get("table_type") or "")
        if _is_postgres_family_table_type(table_type):
            return await _ai_adjust_sql_postgres(pool, cfg)

        db = _validate_ident(cfg.get("database"), label="database")
        sch = _validate_ident(cfg.get("schema"), label="schema")
        tbl = _validate_ident(cfg.get("table_name"), label="table")
        full_name = _full_table_name(db, sch, tbl)
        is_hybrid = table_type == "HYBRID"
        is_interactive = table_type == "INTERACTIVE"

        # Check if object is a view (TABLESAMPLE doesn't work on views)
        is_view = False
        try:
            view_check = await pool.execute_query(
                f"""
                SELECT TABLE_TYPE
                FROM {_quote_ident(db)}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                """,
                params=[sch, tbl],
            )
            if view_check and str(view_check[0][0]).upper() == "VIEW":
                is_view = True
        except Exception:
            pass  # If check fails, assume table

        # ------------------------------------------------------------------
        # Interactive Table validation: fetch cluster key and check warehouse type
        # ------------------------------------------------------------------
        cluster_by_columns: list[str] = []
        interactive_warnings: list[str] = []
        
        if is_interactive:
            # Fetch cluster key from SHOW INTERACTIVE TABLES
            try:
                interactive_rows = await pool.execute_query(
                    f"SHOW INTERACTIVE TABLES LIKE '{tbl}' IN SCHEMA {db}.{sch}"
                )
                if interactive_rows and len(interactive_rows) > 0:
                    # Column 4 is cluster_by, e.g., "(O_ORDERKEY)" or "(O_ORDERDATE, O_CUSTKEY)"
                    cluster_by_raw = str(interactive_rows[0][4] or "").strip()
                    if cluster_by_raw:
                        # Parse "(col1, col2)" into ["COL1", "COL2"]
                        cluster_by_raw = cluster_by_raw.strip("()")
                        cluster_by_columns = [
                            c.strip().upper() for c in cluster_by_raw.split(",") if c.strip()
                        ]
                        logger.info(f"Interactive Table {tbl} cluster key: {cluster_by_columns}")
            except Exception as e:
                logger.debug(f"Could not fetch cluster key for {tbl}: {e}")

            # Check warehouse type - Interactive Tables perform best with Interactive Warehouses
            warehouse_name = str(cfg.get("warehouse_name") or "").strip()
            if warehouse_name:
                try:
                    wh_rows = await pool.execute_query(f"SHOW WAREHOUSES LIKE '{warehouse_name}'")
                    if wh_rows and len(wh_rows) > 0:
                        # Column 2 is warehouse type (STANDARD vs INTERACTIVE)
                        wh_type = str(wh_rows[0][2] or "STANDARD").upper()
                        if wh_type != "INTERACTIVE":
                            interactive_warnings.append(
                                f"⚠️ Warehouse '{warehouse_name}' is type '{wh_type}'. "
                                f"Interactive Tables perform best with INTERACTIVE warehouses. "
                                f"Consider using an Interactive Warehouse for optimal query performance."
                            )
                except Exception as e:
                    logger.debug(f"Could not check warehouse type for {warehouse_name}: {e}")

        # For pool sizing in preparation, treat concurrent_connections as:
        # - CONCURRENCY mode: fixed worker count
        # - QPS mode: max worker cap (may be -1 => no user cap; use engine cap)
        load_mode = (
            str(cfg.get("load_mode") or "CONCURRENCY").strip().upper() or "CONCURRENCY"
        )
        raw_cc = cfg.get("concurrent_connections")
        cc = int(raw_cc) if raw_cc is not None else 1
        if load_mode == "QPS" and cc == -1:
            cc = int(settings.SNOWFLAKE_BENCHMARK_EXECUTOR_MAX_WORKERS)

        # AI availability check (fast).
        ai_available = False
        ai_error: str | None = None
        try:
            await pool.execute_query("SELECT AI_COMPLETE('claude-4-sonnet', 'test')")
            ai_available = True
        except Exception as e:
            ai_available = False
            ai_error = str(e)

        # Profile table for key/time columns (cheap).
        # For SQL adjustment we only need key/time column names; avoid MIN/MAX scans which can
        # time out on large hybrid tables.
        prof = await profile_snowflake_table(pool, full_name, include_bounds=False)
        key_col = prof.id_column
        time_col = prof.time_column

        # Choose insert/update columns (heuristic; AI can refine here when available).
        desc_rows = await pool.execute_query(f"DESCRIBE TABLE {full_name}")
        col_types: dict[str, str] = {}
        col_null_ok: dict[str, bool] = {}
        col_default: dict[str, str] = {}
        for row in desc_rows:
            if not row:
                continue
            kind = (
                str(row[2]).upper() if len(row) > 2 and row[2] is not None else "COLUMN"
            )
            if kind != "COLUMN":
                continue
            name = str(row[0]).upper()
            typ = str(row[1]).upper() if len(row) > 1 else ""
            col_types[name] = typ
            null_raw = (
                str(row[3]).strip().upper()
                if len(row) > 3 and row[3] is not None
                else ""
            )
            default_raw = (
                str(row[4]).strip() if len(row) > 4 and row[4] is not None else ""
            )
            col_null_ok[name] = null_raw != "N"
            col_default[name] = default_raw

        def _is_key_or_id_like(col: str) -> bool:
            c = str(col or "").strip().upper()
            if not c:
                return False
            if c == (key_col or ""):
                return True
            # Treat common identifier/key suffixes as non-updatable.
            return c == "ID" or c.endswith("ID") or c.endswith("KEY")

        def _pick_update_column() -> str | None:
            preferred = ["UPDATED_AT", "STATUS", "STATE", "UPDATED", "MODIFIED_AT"]
            for c in preferred:
                if c in col_types and (not _is_key_or_id_like(c)):
                    return c
            for c in col_types.keys():
                if not _is_key_or_id_like(c):
                    return c
            return None

        update_col = _pick_update_column()
        if update_col and _is_key_or_id_like(update_col):
            update_col = None

        issues: list[str] = []

        # Range scan strategy:
        # - HYBRID: prefer key-range scans to avoid expensive time-cutoff + sort patterns
        # - STANDARD: prefer time-cutoff if available; else key-range
        range_mode: str | None = "ID_BETWEEN" if (is_hybrid and key_col) else None

        # Collect key column distribution stats for smarter range scan offset calculation.
        # This helps generate BETWEEN queries that return ~100 rows without relying on LIMIT.
        key_stats: dict[str, Any] = {}
        if key_col:
            try:
                stats_sql = f'''
                    SELECT 
                        MIN("{key_col}") AS key_min,
                        MAX("{key_col}") AS key_max,
                        COUNT(*) AS row_count
                    FROM {full_name}
                '''
                stats_rows = await pool.execute_query(stats_sql)
                if stats_rows and stats_rows[0]:
                    row = stats_rows[0]
                    key_min = row[0]
                    key_max = row[1]
                    row_count = row[2]
                    if (
                        key_min is not None
                        and key_max is not None
                        and row_count
                        and row_count > 0
                    ):
                        key_stats = {
                            "key_min": key_min,
                            "key_max": key_max,
                            "row_count": int(row_count),
                            "key_range": key_max - key_min
                            if isinstance(key_max, (int, float))
                            and isinstance(key_min, (int, float))
                            else None,
                        }
                        if (
                            key_stats["key_range"] is not None
                            and key_stats["row_count"] > 0
                        ):
                            key_stats["avg_gap"] = (
                                key_stats["key_range"] / key_stats["row_count"]
                            )
                            # Calculate offset to get ~100 rows: offset = avg_gap * target_rows
                            key_stats["suggested_offset_for_100_rows"] = (
                                int(key_stats["avg_gap"] * 100) or 100
                            )
            except Exception as e:
                logger.debug(
                    "Failed to collect key stats for range scan optimization: %s", e
                )

        # Default range offset (will be refined by AI or heuristics)
        range_offset: int = key_stats.get("suggested_offset_for_100_rows", 100)

        def _has_default(col: str) -> bool:
            d = (col_default.get(col) or "").strip()
            return bool(d and d.upper() not in {"NULL", "NONE"})

        required_cols: list[str] = []
        for c in col_types.keys():
            if not col_null_ok.get(c, True) and not _has_default(c):
                required_cols.append(c)
        if len(required_cols) > 8:
            issues.append(
                f"Table has {len(required_cols)} required columns; INSERT may need manual adjustment."
            )

        insert_cols: list[str] = []
        # Prefer required + key + time + a couple of string columns.
        for c in required_cols[:8]:
            insert_cols.append(c)
        if key_col:
            kc = key_col.upper()
            if kc not in insert_cols:
                insert_cols.append(kc)
        if time_col and time_col.upper() not in insert_cols:
            insert_cols.append(time_col.upper())
        for c, typ in col_types.items():
            if c in insert_cols:
                continue
            if any(x in typ for x in ("VARCHAR", "STRING", "TEXT")):
                insert_cols.append(c)
            if len(insert_cols) >= 6:
                break
        if not insert_cols:
            insert_cols = list(col_types.keys())[:3]

        # Optional: ask Cortex to refine insert/update/range choices and provide a user-facing summary.
        ai_summary_from_model: str | None = None
        if ai_available:
            try:
                import json

                # Sample a small set of columns + rows for better column/domain selection.
                sample_cols: list[str] = []
                if key_col:
                    sample_cols.append(key_col.upper())
                if time_col and time_col.upper() not in sample_cols:
                    sample_cols.append(time_col.upper())
                for c, typ in col_types.items():
                    if c in sample_cols:
                        continue
                    if len(sample_cols) >= 12:
                        break
                    sample_cols.append(c)

                obj_parts: list[str] = []
                for c in sample_cols:
                    c_ident = _validate_ident(c, label="column")
                    obj_parts.append(f"'{c_ident}'")
                    obj_parts.append(_quote_ident(c_ident))
                obj_expr = (
                    f"OBJECT_CONSTRUCT_KEEP_NULL({', '.join(obj_parts)})"
                    if obj_parts
                    else "OBJECT_CONSTRUCT()"
                )
                if is_view:
                    sample_rows = await pool.execute_query(
                        f"SELECT {obj_expr} FROM {full_name} LIMIT 20"
                    )
                else:
                    sample_rows = await pool.execute_query(
                        f"SELECT {obj_expr} FROM {full_name} TABLESAMPLE SYSTEM (1) LIMIT 20"
                    )
                sample_payload = [r[0] for r in sample_rows if r]

                cols_for_prompt = []
                for c in list(col_types.keys())[:250]:
                    cols_for_prompt.append({"name": c, "type": col_types.get(c, "")})

                # Build key stats section for the prompt if available
                key_stats_section = ""
                if key_stats:
                    key_stats_section = (
                        "\nKEY_COLUMN_STATS (for range scan optimization):\n"
                        f"{json.dumps(key_stats, ensure_ascii=False, default=str)}\n"
                    )

                # Build interactive table note for the prompt
                interactive_note = ""
                if is_interactive:
                    interactive_note = (
                        "\nIMPORTANT: This is an INTERACTIVE TABLE. Interactive tables do NOT support "
                        "DML operations (INSERT, UPDATE, DELETE). Only read operations are allowed.\n"
                        "Return empty arrays for insert_columns and update_columns.\n\n"
                    )

                prompt = (
                    "You are adjusting a 4-statement benchmark workload for a Snowflake table.\n"
                    "Your output will be shown to the user.\n\n"
                    f"TABLE: {db}.{sch}.{tbl}\n"
                    f"TABLE_TYPE: {table_type}\n"
                    f"{interactive_note}"
                    f"KEY_COLUMN (may be null): {key_col or None}\n"
                    f"TIME_COLUMN (may be null): {time_col or None}\n"
                    f"{key_stats_section}\n"
                    "COLUMNS (name/type):\n"
                    f"{json.dumps(cols_for_prompt, ensure_ascii=False)}\n\n"
                    "REQUIRED_COLUMNS (must be included in insert_columns if feasible):\n"
                    f"{json.dumps(required_cols, ensure_ascii=False)}\n\n"
                    "SAMPLE_ROWS (JSON objects):\n"
                    f"{json.dumps(sample_payload, ensure_ascii=False, default=str)}\n\n"
                    "Return STRICT JSON ONLY with:\n"
                    "- summary: string (1-2 sentences describing what you changed)\n"
                    "- insert_columns: [string] (columns to include in INSERT placeholders; empty for INTERACTIVE tables)\n"
                    "- update_columns: [string] (columns to include in UPDATE set clause; empty for INTERACTIVE tables)\n"
                    '- range_mode: one of ["ID_BETWEEN","TIME_CUTOFF",null]\n'
                    "- range_offset: integer (for ID_BETWEEN: the offset to add to start key to get ~100 rows; use KEY_COLUMN_STATS.suggested_offset_for_100_rows if available, else 100)\n"
                    "- issues: [string] (empty if all OK)\n"
                    "\n"
                    "Rules:\n"
                    "- If TABLE_TYPE is INTERACTIVE: insert_columns and update_columns MUST be empty (DML not supported).\n"
                    "- If KEY_COLUMN is null: update_columns must be empty.\n"
                    "- update_columns must NOT include any columns ending with ID or KEY.\n"
                    "- If TIME_COLUMN is null and KEY_COLUMN is null: range_mode must be null.\n"
                    "- IMPORTANT: Prefer ID_BETWEEN over TIME_CUTOFF when KEY_COLUMN exists - it provides more predictable scan sizes.\n"
                    "- ID_BETWEEN uses: WHERE key BETWEEN ? AND ? (returns ~100 rows based on range_offset, no LIMIT needed)\n"
                    "- TIME_CUTOFF uses: WHERE time >= ? LIMIT 100 (relies on LIMIT for row count, may cause early termination optimization)\n"
                    "- If KEY_COLUMN_STATS is provided, use suggested_offset_for_100_rows as range_offset.\n"
                    "- Keep insert_columns <= 8 and update_columns <= 2.\n"
                )

                ai_resp = await pool.execute_query(
                    "SELECT AI_COMPLETE(model => ?, prompt => ?, model_parameters => PARSE_JSON(?), response_format => PARSE_JSON(?)) AS RESP",
                    params=[
                        "claude-4-sonnet",
                        prompt,
                        json.dumps({"temperature": 0, "max_tokens": 600}),
                        json.dumps(
                            {
                                "type": "json",
                                "schema": {
                                    "type": "object",
                                    "properties": {
                                        "summary": {"type": "string"},
                                        "insert_columns": {
                                            "type": "array",
                                            "items": {"type": "string"},
                                        },
                                        "update_columns": {
                                            "type": "array",
                                            "items": {"type": "string"},
                                        },
                                        "range_mode": {
                                            "type": ["string", "null"],
                                        },
                                        "range_offset": {
                                            "type": "integer",
                                        },
                                        "issues": {
                                            "type": "array",
                                            "items": {"type": "string"},
                                        },
                                    },
                                    "required": [
                                        "summary",
                                        "insert_columns",
                                        "update_columns",
                                        "range_mode",
                                        "range_offset",
                                        "issues",
                                    ],
                                },
                            }
                        ),
                    ],
                )

                raw = ai_resp[0][0] if ai_resp and ai_resp[0] else None
                parsed: dict[str, Any] | None = None
                if isinstance(raw, dict):
                    parsed = raw
                elif isinstance(raw, str) and raw.strip():
                    try:
                        parsed = json.loads(raw)
                    except Exception:
                        parsed = None

                if isinstance(parsed, dict):
                    ai_summary_from_model = (
                        str(parsed.get("summary") or "").strip() or None
                    )
                    ai_issues = parsed.get("issues")
                    if isinstance(ai_issues, list):
                        issues.extend(str(x) for x in ai_issues if str(x).strip())

                    ai_ins = parsed.get("insert_columns")
                    if isinstance(ai_ins, list) and ai_ins:
                        cleaned: list[str] = []
                        for c in ai_ins:
                            cu = _upper_str(c)
                            if cu and cu in col_types:
                                cleaned.append(cu)
                            if len(cleaned) >= 8:
                                break
                        if cleaned:
                            insert_cols = cleaned

                    ai_upd = parsed.get("update_columns")
                    if isinstance(ai_upd, list) and ai_upd and key_col:
                        cleaned_u: list[str] = []
                        for c in ai_upd:
                            cu = _upper_str(c)
                            if not cu or cu == (key_col or ""):
                                continue
                            if _is_key_or_id_like(cu):
                                continue
                            if cu in col_types:
                                cleaned_u.append(cu)
                            if len(cleaned_u) >= 2:
                                break
                        if cleaned_u:
                            update_col = cleaned_u[0]

                    rm = parsed.get("range_mode")
                    if rm in ("TIME_CUTOFF", "ID_BETWEEN"):
                        range_mode = str(rm)

                    # Extract range_offset from AI response
                    ai_offset = parsed.get("range_offset")
                    if isinstance(ai_offset, (int, float)) and ai_offset > 0:
                        range_offset = int(ai_offset)
            except Exception as e:
                # If Cortex fails mid-flight, fall back to heuristics (do not warn the user unless
                # there are actual workload issues like missing key/time columns or blank SQL).
                logger.debug("AI planning failed in /ai/adjust-sql: %s", e)

        # Build SQL templates (blank + pct=0 if missing key/time as requested).
        # Prefer a narrow projection to avoid SELECT * on wide tables.
        projection_cols: list[str] = []

        def _add_proj(col: str | None) -> None:
            if not col:
                return
            c = str(col).strip().upper()
            if not c or c not in col_types:
                return
            if c not in projection_cols:
                projection_cols.append(c)

        # Always include key/time/update + insert columns (bounded).
        _add_proj(key_col)
        _add_proj(time_col)
        _add_proj(update_col)
        for c in (insert_cols or [])[:12]:
            _add_proj(c)

        # Fill with a few additional simple columns for realism (bounded).
        for c, typ in col_types.items():
            if len(projection_cols) >= 12:
                break
            if c in projection_cols:
                continue
            t = str(typ or "").upper()
            if any(x in t for x in ("VARIANT", "OBJECT", "ARRAY", "BINARY")):
                continue
            _add_proj(c)

        select_list = (
            ", ".join(f'"{c}"' for c in projection_cols) if projection_cols else "*"
        )
        point_sql = ""
        update_sql = ""
        if key_col:
            point_sql = f'SELECT {select_list} FROM {{table}} WHERE "{key_col}" = ?'
            # Interactive tables do NOT support UPDATE (only INSERT OVERWRITE is allowed).
            if update_col and not is_interactive:
                update_sql = (
                    f'UPDATE {{table}} SET "{update_col}" = ? WHERE "{key_col}" = ?'
                )

        # Range scan: prefer ID_BETWEEN when key exists (more predictable scan sizes);
        # fall back to TIME_CUTOFF only when no key column is available.
        # ID_BETWEEN: Uses calculated offset to return ~100 rows without LIMIT (avoids early termination optimization)
        # TIME_CUTOFF: Uses >= with LIMIT 100 (may benefit from early termination, less predictable workload)
        range_sql = ""
        if range_mode == "ID_BETWEEN":
            # Explicit override (only valid when key exists).
            if key_col:
                # Use calculated range_offset; no LIMIT needed since we're constraining by range
                range_sql = f'SELECT {select_list} FROM {{table}} WHERE "{key_col}" BETWEEN ? AND ? + {range_offset}'
            else:
                range_mode = None
        elif key_col:
            # Prefer ID_BETWEEN when key column exists (changed from preferring TIME_CUTOFF)
            range_sql = f'SELECT {select_list} FROM {{table}} WHERE "{key_col}" BETWEEN ? AND ? + {range_offset}'
            range_mode = "ID_BETWEEN"
        elif time_col:
            # Only use TIME_CUTOFF when no key column is available
            range_sql = (
                f'SELECT {select_list} FROM {{table}} WHERE "{time_col}" >= ? LIMIT 100'
            )
            range_mode = "TIME_CUTOFF"

        # ------------------------------------------------------------------
        # Interactive Table: Validate WHERE clause against cluster key
        # ------------------------------------------------------------------
        if is_interactive and cluster_by_columns:
            # Check if point lookup key matches cluster key
            if key_col and key_col.upper() not in cluster_by_columns:
                interactive_warnings.append(
                    f"⚠️ Point lookup uses '{key_col}' but table is clustered on {cluster_by_columns}. "
                    f"Queries on non-clustered columns will be slow (5-second timeout). "
                    f"For optimal performance, query on: {', '.join(cluster_by_columns)}"
                )
            
            # Check if range scan key matches cluster key (first column typically most selective)
            if range_mode == "ID_BETWEEN" and key_col:
                if key_col.upper() not in cluster_by_columns:
                    interactive_warnings.append(
                        f"⚠️ Range scan uses '{key_col}' but table is clustered on {cluster_by_columns}. "
                        f"Consider clustering on '{key_col}' or using a table clustered appropriately."
                    )
            elif range_mode == "TIME_CUTOFF" and time_col:
                if time_col.upper() not in cluster_by_columns:
                    interactive_warnings.append(
                        f"⚠️ Range scan uses '{time_col}' but table is clustered on {cluster_by_columns}. "
                        f"Time-based queries may be slow if not aligned with cluster key."
                    )

        # Insert (always placeholders; params generated in executor).
        # NOTE: Interactive tables do NOT support DML (INSERT, UPDATE, DELETE).
        # Only INSERT OVERWRITE is allowed, which is not supported in benchmark workloads.
        if is_interactive:
            insert_sql = ""
        else:
            cols_sql = ", ".join(f'"{c}"' for c in insert_cols)
            ph_sql = ", ".join("?" for _ in insert_cols)
            insert_sql = (
                f"INSERT INTO {{table}} ({cols_sql}) VALUES ({ph_sql})"
                if insert_cols
                else ""
            )

        # Percentages: keep the user's mix by default.
        p_point = int(cfg.get("custom_point_lookup_pct") or 0)
        p_range = int(cfg.get("custom_range_scan_pct") or 0)
        p_ins = int(cfg.get("custom_insert_pct") or 0)
        p_upd = int(cfg.get("custom_update_pct") or 0)

        # Only adjust mix if we cannot generate SQL for an operation that currently has weight > 0.
        disabled: list[tuple[str, str]] = []
        if p_point > 0 and not point_sql:
            disabled.append(
                (
                    "POINT_LOOKUP",
                    "No usable key column detected; POINT_LOOKUP disabled.",
                )
            )
        if p_range > 0 and not range_sql:
            disabled.append(
                (
                    "RANGE_SCAN",
                    "No usable time/key column detected; RANGE_SCAN disabled.",
                )
            )
        if p_ins > 0 and not insert_sql:
            if is_interactive:
                disabled.append(
                    ("INSERT", "Interactive tables do not support INSERT; disabled.")
                )
            else:
                disabled.append(
                    ("INSERT", "No usable insert columns detected; INSERT disabled.")
                )
        if p_upd > 0 and not update_sql:
            if is_interactive:
                disabled.append(
                    ("UPDATE", "Interactive tables do not support UPDATE; disabled.")
                )
            else:
                disabled.append(
                    ("UPDATE", "No usable update/key column detected; UPDATE disabled.")
                )

        if disabled:
            for _, msg in disabled:
                issues.append(msg)

            removed = 0
            if any(k == "POINT_LOOKUP" for k, _ in disabled):
                removed += p_point
                p_point = 0
            if any(k == "RANGE_SCAN" for k, _ in disabled):
                removed += p_range
                p_range = 0
            if any(k == "INSERT" for k, _ in disabled):
                removed += p_ins
                p_ins = 0
            if any(k == "UPDATE" for k, _ in disabled):
                removed += p_upd
                p_upd = 0

            remaining = [
                ("POINT_LOOKUP", p_point),
                ("RANGE_SCAN", p_range),
                ("INSERT", p_ins),
                ("UPDATE", p_upd),
            ]
            remaining_total = sum(v for _, v in remaining if v > 0)
            if remaining_total > 0 and removed > 0:
                alloc: dict[str, int] = {}
                used = 0
                for kind, v in remaining:
                    if v <= 0:
                        continue
                    add = int((removed * v) // remaining_total)
                    if add > 0:
                        alloc[kind] = add
                        used += add
                leftover = max(0, removed - used)
                order = [
                    k
                    for k, v in sorted(
                        remaining, key=lambda kv: int(kv[1]), reverse=True
                    )
                    if v > 0
                ]
                i = 0
                while leftover > 0 and order:
                    alloc[order[i % len(order)]] = (
                        int(alloc.get(order[i % len(order)], 0)) + 1
                    )
                    leftover -= 1
                    i += 1

                p_point += int(alloc.get("POINT_LOOKUP", 0))
                p_range += int(alloc.get("RANGE_SCAN", 0))
                p_ins += int(alloc.get("INSERT", 0))
                p_upd += int(alloc.get("UPDATE", 0))
            elif removed > 0:
                fallback_kind = None
                if range_sql:
                    fallback_kind = "RANGE_SCAN"
                elif insert_sql:
                    fallback_kind = "INSERT"
                elif point_sql:
                    fallback_kind = "POINT_LOOKUP"
                elif update_sql:
                    fallback_kind = "UPDATE"
                if fallback_kind:
                    p_point = 100 if fallback_kind == "POINT_LOOKUP" else 0
                    p_range = 100 if fallback_kind == "RANGE_SCAN" else 0
                    p_ins = 100 if fallback_kind == "INSERT" else 0
                    p_upd = 100 if fallback_kind == "UPDATE" else 0
                    issues.append(
                        f"All configured operations were unsupported; falling back to {fallback_kind}=100%."
                    )

            # Guardrail: keep sum at 100; assign rounding drift to INSERT.
            total = p_point + p_range + p_ins + p_upd
            if total != 100:
                p_ins += 100 - total

        # Toast only when unexpected (mix had to change due to missing SQL).
        toast_level = "warning" if issues else "success"
        ai_summary = ai_summary_from_model or ""
        if not ai_summary:
            ai_summary = "Generated workload SQL."
        if issues:
            ai_summary = ai_summary + " Issues: " + " ".join(issues)

        # Return a minimal columns map so the saved template can validate against the customer table.
        cols_map: dict[str, str] = {}
        for c in {
            *(insert_cols or []),
            *([key_col] if key_col else []),
            *([time_col] if time_col else []),
        }:
            if not c:
                continue
            c_u = str(c).upper()
            cols_map[c_u] = col_types.get(c_u, "VARCHAR")

        ai_workload = {
            "available": ai_available,
            "model": "claude-4-sonnet",
            "availability_error": ai_error,
            "key_column": key_col,
            "time_column": time_col,
            "range_mode": range_mode,
            "range_offset": range_offset,
            "insert_columns": insert_cols,
            "update_columns": [update_col] if update_col else [],
            "projection_columns": projection_cols,
        }

        return AiAdjustSqlResponse(
            custom_point_lookup_query=point_sql,
            custom_range_scan_query=range_sql,
            custom_insert_query=insert_sql,
            custom_update_query=update_sql,
            custom_point_lookup_pct=p_point,
            custom_range_scan_pct=p_range,
            custom_insert_pct=p_ins,
            custom_update_pct=p_upd,
            columns=cols_map,
            ai_workload=ai_workload,
            toast_level="warning" if interactive_warnings else toast_level,
            summary=ai_summary,
            cluster_by=cluster_by_columns if cluster_by_columns else None,
            warnings=interactive_warnings,
        )
    except Exception as e:
        raise http_exception("ai adjust sql", e)


@router.get("/{template_id}", response_model=TemplateResponse)
async def get_template(template_id: str):
    """
    Get a specific template by ID.

    Args:
        template_id: UUID of the template

    Returns:
        Template data
    """
    try:
        pool = snowflake_pool.get_default_pool()

        query = f"""
        SELECT 
            TEMPLATE_ID,
            TEMPLATE_NAME,
            DESCRIPTION,
            CONFIG,
            CREATED_AT,
            UPDATED_AT,
            CREATED_BY,
            TAGS,
            USAGE_COUNT,
            LAST_USED_AT
        FROM {_results_prefix()}.TEST_TEMPLATES
        WHERE TEMPLATE_ID = '{template_id}'
        """

        results = await pool.execute_query(query)

        if not results:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Template not found: {template_id}",
            )

        columns = [
            "TEMPLATE_ID",
            "TEMPLATE_NAME",
            "DESCRIPTION",
            "CONFIG",
            "CREATED_AT",
            "UPDATED_AT",
            "CREATED_BY",
            "TAGS",
            "USAGE_COUNT",
            "LAST_USED_AT",
        ]
        row_dict = _row_to_dict(results[0], columns)

        # Parse JSON strings from VARIANT columns
        import json

        config = row_dict["config"]
        if isinstance(config, str):
            config = json.loads(config)
        tags = row_dict["tags"]
        if isinstance(tags, str) and tags:
            tags = json.loads(tags)

        return {
            "template_id": row_dict["template_id"],
            "template_name": row_dict["template_name"],
            "description": row_dict["description"],
            "config": config,
            "created_at": row_dict["created_at"],
            "updated_at": row_dict["updated_at"],
            "created_by": row_dict["created_by"],
            "tags": tags,
            "usage_count": row_dict["usage_count"] or 0,
            "last_used_at": row_dict["last_used_at"],
        }

    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("get template", e)


async def _check_pgbouncer_requirements(config: dict) -> None:
    """
    Validate PgBouncer requirements if use_pgbouncer is enabled.

    Checks if the snowflake_pooler extension is installed in the target database.
    Returns a specific error code if missing so the frontend can prompt for installation.

    Args:
        config: Template configuration dictionary

    Raises:
        HTTPException: If PgBouncer is enabled but extension is not installed
    """
    use_pgbouncer = config.get("use_pgbouncer", False)
    if not use_pgbouncer:
        return

    table_type = str(config.get("table_type", "")).upper()
    if table_type != "POSTGRES":
        return

    database = config.get("database", "")
    connection_id = config.get("connection_id", "")
    if not database or not connection_id:
        return

    try:
        # Get connection credentials
        conn_params = await connection_manager.get_connection_for_pool(connection_id)
        if not conn_params:
            raise ValueError(f"Connection not found: {connection_id}")
        
        if conn_params.get("connection_type") != "POSTGRES":
            raise ValueError(f"Connection {connection_id} is not a Postgres connection")
        
        # Create SSL context for Snowflake Postgres
        ssl_context = ssl.create_default_context()
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_NONE
        
        # Connect using direct port (not PgBouncer) to check extension
        conn = await asyncpg.connect(
            host=conn_params["host"],
            port=conn_params["port"],  # Direct port, not PgBouncer
            database=database,
            user=conn_params["user"],
            password=conn_params["password"],
            timeout=15,
            ssl=ssl_context,
        )
        
        try:
            # Check if snowflake_pooler extension is installed
            rows = await conn.fetch(
                "SELECT extname FROM pg_extension WHERE extname = 'snowflake_pooler'"
            )

            if not rows:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail={
                        "error": "pgbouncer_extension_missing",
                        "database": database,
                        "message": (
                            f"PgBouncer requires the 'snowflake_pooler' extension in database '{database}'. "
                            f"Connect as snowflake_admin and run: CREATE EXTENSION snowflake_pooler;"
                        ),
                        "docs_url": "https://docs.snowflake.com/en/user-guide/snowflake-postgres/postgres-connection-pooling",
                    },
                )

            # Check if current user is a superuser or has replication privilege
            # PgBouncer does not allow connections from either type
            role_rows = await conn.fetch(
                "SELECT rolsuper, rolreplication FROM pg_roles WHERE rolname = current_user"
            )

            if role_rows:
                is_superuser = role_rows[0][0]
                has_replication = role_rows[0][1]
                if is_superuser or has_replication:
                    reasons = []
                    if is_superuser:
                        reasons.append("superuser")
                    if has_replication:
                        reasons.append("replication")
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail={
                            "error": "pgbouncer_privileged_user_not_allowed",
                            "message": (
                                f"PgBouncer does not allow connections from users with "
                                f"{' or '.join(reasons)} privileges. "
                                f"Create a non-privileged application role to use PgBouncer, "
                                f"or disable the PgBouncer option."
                            ),
                            "docs_url": "https://docs.snowflake.com/en/user-guide/snowflake-postgres/postgres-connection-pooling",
                        },
                    )
        finally:
            await conn.close()

    except HTTPException:
        raise
    except Exception as e:
        logger.warning("Failed to check PgBouncer extension: %s: %s", type(e).__name__, e)
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail={
                "error": "pgbouncer_check_failed",
                "message": (
                    f"Failed to verify PgBouncer requirements for database '{database}': {e}. "
                    f"Either disable PgBouncer or ensure the database is accessible."
                ),
            },
        )


@router.post("/", response_model=TemplateResponse, status_code=status.HTTP_201_CREATED)
async def create_template(template: TemplateCreate):
    """
    Create a new test configuration template.

    Args:
        template: Template data

    Returns:
        Created template with generated ID
    """
    try:
        pool = snowflake_pool.get_default_pool()

        template_id = str(uuid4())
        now = datetime.now(UTC)

        import json

        normalized_cfg = _normalize_template_config(template.config)
        # Enrich with postgres_instance_size if applicable
        normalized_cfg = _enrich_postgres_instance_size(normalized_cfg)

        # Validate PgBouncer requirements before saving
        await _check_pgbouncer_requirements(normalized_cfg)

        # If a template payload includes AI workload prep artifacts (e.g. from duplicating
        # an existing template), they are not valid for a *new* template.
        # Pools are keyed by TEMPLATE_ID, so carrying over the old pool_id would create
        # a confusing "prepared" UI state while having no backing rows.
        ai_workload = normalized_cfg.get("ai_workload")
        if isinstance(ai_workload, dict):
            ai_workload.pop("pool_id", None)
            ai_workload.pop("pools", None)
            ai_workload.pop("prepared_at", None)
            normalized_cfg["ai_workload"] = ai_workload
        config_json = json.dumps(normalized_cfg)
        tags_json = json.dumps(template.tags) if template.tags else None
        now_iso = now.isoformat()

        # Use bound parameters to avoid JSON parsing issues from string interpolation
        # (e.g., escaped quotes/backslashes inside the JSON payload).
        #
        # Note: Snowflake can reject PARSE_JSON(?) inside a VALUES clause with qmark binding,
        # so we use INSERT ... SELECT ... instead.
        query = f"""
        INSERT INTO {_results_prefix()}.TEST_TEMPLATES (
            TEMPLATE_ID, TEMPLATE_NAME, DESCRIPTION, CONFIG, CREATED_AT, UPDATED_AT, TAGS, USAGE_COUNT
        )
        SELECT
            ?,
            ?,
            ?,
            PARSE_JSON(?),
            ?,
            ?,
            PARSE_JSON(?),
            0
        """
        await pool.execute_query(
            query,
            params=[
                template_id,
                template.template_name,
                template.description,
                config_json,
                now_iso,
                now_iso,
                tags_json,
            ],
        )

        return await get_template(template_id)

    except Exception as e:
        raise http_exception("create template", e)


@router.put("/{template_id}", response_model=TemplateResponse)
async def update_template(template_id: str, template: TemplateUpdate):
    """
    Update an existing template.

    Args:
        template_id: UUID of the template to update
        template: Updated template data

    Returns:
        Updated template
    """
    try:
        pool = snowflake_pool.get_default_pool()

        # Check if template exists by trying to get it
        try:
            existing = await get_template(template_id)
        except HTTPException as e:
            if e.status_code == status.HTTP_404_NOT_FOUND:
                raise
            raise

        usage_count = int(existing.get("usage_count") or 0)
        if usage_count > 0:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    "This template has test results and can no longer be edited. "
                    "Copy it to create an editable version."
                ),
            )

        updates = []
        params: list[Any] = []

        if template.template_name is not None:
            updates.append("TEMPLATE_NAME = ?")
            params.append(template.template_name)

        if template.description is not None:
            updates.append("DESCRIPTION = ?")
            params.append(template.description)

        if template.config is not None:
            import json

            normalized_cfg = _normalize_template_config(template.config)
            # Enrich with postgres_instance_size if applicable
            normalized_cfg = _enrich_postgres_instance_size(normalized_cfg)

            # Validate PgBouncer requirements before saving
            await _check_pgbouncer_requirements(normalized_cfg)

            config_json = json.dumps(normalized_cfg)
            updates.append("CONFIG = PARSE_JSON(?)")
            params.append(config_json)

        if template.tags is not None:
            import json

            tags_json = json.dumps(template.tags)
            updates.append("TAGS = PARSE_JSON(?)")
            params.append(tags_json)

        updates.append("UPDATED_AT = ?")
        params.append(datetime.now(UTC).isoformat())

        query = f"""
        UPDATE {_results_prefix()}.TEST_TEMPLATES
        SET {", ".join(updates)}
        WHERE TEMPLATE_ID = ?
        """

        params.append(template_id)
        await pool.execute_query(query, params=params)

        return await get_template(template_id)

    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("update template", e)


@router.post("/{template_id}/ai/prepare", response_model=AiPrepareResponse)
async def prepare_ai_template(template_id: str):
    """
    Prepare a template for "AI adjusted" workloads:
    - Check Cortex availability (AI_COMPLETE smoke test)
    - Profile the target table (key/time columns)
    - Generate and persist value pools using Snowflake SAMPLE into TEMPLATE_VALUE_POOLS
    - Persist the resulting metadata into TEST_TEMPLATES.CONFIG (no runtime AI calls)
    """
    try:
        pool = snowflake_pool.get_default_pool()

        tpl = await get_template(template_id)
        usage_count = int(tpl.get("usage_count") or 0)
        if usage_count > 0:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=(
                    "This template has test results and can no longer be edited. "
                    "Copy it to create an editable version."
                ),
            )

        cfg = tpl.get("config") or {}
        if not isinstance(cfg, dict):
            raise HTTPException(
                status_code=400, detail="Template config must be a JSON object"
            )

        db = _validate_ident(cfg.get("database"), label="database")
        sch = _validate_ident(cfg.get("schema"), label="schema")
        tbl = _validate_ident(cfg.get("table_name"), label="table")
        full_name = _full_table_name(db, sch, tbl)

        concurrency = int(cfg.get("concurrent_connections") or 1)
        concurrency = max(1, concurrency)
        table_type = _upper_str(cfg.get("table_type") or "")
        is_hybrid = table_type == "HYBRID"

        # ------------------------------------------------------------------
        # 1) Cortex availability check (fast)
        # ------------------------------------------------------------------
        ai_available = False
        ai_error: str | None = None
        try:
            await pool.execute_query("SELECT AI_COMPLETE('claude-4-sonnet', 'test')")
            ai_available = True
        except Exception as e:
            ai_available = False
            ai_error = str(e)

        # ------------------------------------------------------------------
        # 2) Profile table (heuristics) for key/time columns
        # ------------------------------------------------------------------
        # For HYBRID templates we typically use ID_BETWEEN range scans and do not need
        # expensive MIN/MAX bounds (which can time out on large hybrid tables).
        profile = await profile_snowflake_table(
            pool,
            full_name,
            include_bounds=not is_hybrid,
        )
        key_col = profile.id_column
        time_col = profile.time_column
        range_mode: str | None = "ID_BETWEEN" if (is_hybrid and key_col) else None

        # Check if object is a view (TABLESAMPLE doesn't work on views)
        is_view = False
        try:
            view_check = await pool.execute_query(
                f"""
                SELECT TABLE_TYPE
                FROM {_quote_ident(db)}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = ? AND TABLE_NAME = ?
                """,
                params=[sch, tbl],
            )
            if view_check and str(view_check[0][0]).upper() == "VIEW":
                is_view = True
        except Exception:
            pass  # If check fails, assume table

        # Pull full DESCRIBE metadata so we can choose safe insert/update columns.
        desc_rows = await pool.execute_query(f"DESCRIBE TABLE {full_name}")
        col_types: dict[str, str] = {}
        col_null_ok: dict[str, bool] = {}
        col_default: dict[str, str] = {}
        for row in desc_rows:
            if not row:
                continue
            name = str(row[0]).upper()
            typ = str(row[1]).upper() if len(row) > 1 else ""
            kind = str(row[2]).upper() if len(row) > 2 else "COLUMN"
            if kind != "COLUMN":
                continue
            null_raw = (
                str(row[3]).upper() if len(row) > 3 and row[3] is not None else "Y"
            )
            default_raw = (
                str(row[4]).strip() if len(row) > 4 and row[4] is not None else ""
            )
            col_types[name] = typ
            col_null_ok[name] = null_raw != "N"
            col_default[name] = default_raw

        def _has_default(col: str) -> bool:
            d = (col_default.get(col) or "").strip()
            return bool(d) and d.upper() != "NULL"

        required_cols = [
            c
            for c in col_types.keys()
            if (not col_null_ok.get(c, True)) and (not _has_default(c))
        ]

        def _is_simple_type(typ: str) -> bool:
            t = (typ or "").upper()
            return any(
                x in t
                for x in (
                    "NUMBER",
                    "INT",
                    "DECIMAL",
                    "FLOAT",
                    "DOUBLE",
                    "VARCHAR",
                    "CHAR",
                    "STRING",
                    "TEXT",
                    "BOOLEAN",
                    "DATE",
                    "TIME",
                    "TIMESTAMP",
                )
            )

        def _is_complex_type(typ: str) -> bool:
            t = (typ or "").upper()
            return any(
                x in t
                for x in (
                    "VARIANT",
                    "OBJECT",
                    "ARRAY",
                    "GEOGRAPHY",
                    "GEOMETRY",
                    "BINARY",
                )
            )

        # Heuristic insert/update column selection:
        # - Always include required columns (or inserts will fail)
        # - Prefer including key/time columns if present
        # - Avoid complex types unless required
        insert_cols: list[str] = []
        insert_cols.extend(required_cols)
        if key_col and key_col not in insert_cols:
            insert_cols.append(key_col)
        if (
            time_col
            and time_col not in insert_cols
            and _is_simple_type(col_types.get(time_col.upper(), ""))
        ):
            insert_cols.append(time_col)

        # Add a few optional simple columns to improve realism (cap total width).
        for c, typ in col_types.items():
            if c in insert_cols:
                continue
            if _is_complex_type(typ):
                continue
            if len(insert_cols) >= 20:
                break
            if any(
                k in c
                for k in (
                    "STATUS",
                    "STATE",
                    "TYPE",
                    "CATEGORY",
                    "AMOUNT",
                    "PRICE",
                    "NAME",
                    "REGION",
                )
            ):
                insert_cols.append(c)

        # If we still have very few columns, fill with additional simple columns.
        if len(insert_cols) < 8:
            for c, typ in col_types.items():
                if c in insert_cols:
                    continue
                if _is_complex_type(typ):
                    continue
                if not _is_simple_type(typ):
                    continue
                if len(insert_cols) >= 12:
                    break
                insert_cols.append(c)

        update_cols: list[str] = []

        def _is_key_or_id_like_col(col: str) -> bool:
            c = str(col or "").strip().upper()
            if not c:
                return False
            if c == (key_col or ""):
                return True
            return c == "ID" or c.endswith("ID") or c.endswith("KEY")

        preferred_update = [
            "UPDATED_AT",
            "UPDATED",
            "STATUS",
            "STATE",
            "LAST_UPDATED",
            "MODIFIED_AT",
        ]
        for p in preferred_update:
            if (
                p in col_types
                and p != (key_col or "")
                and not _is_key_or_id_like_col(p)
                and not _is_complex_type(col_types[p])
            ):
                update_cols = [p]
                break
        if not update_cols:
            for c, typ in col_types.items():
                if c == (key_col or ""):
                    continue
                if _is_key_or_id_like_col(c):
                    continue
                if _is_complex_type(typ):
                    continue
                if _is_simple_type(typ):
                    update_cols = [c]
                    break

        projection_cols: list[str] = []
        domain_label: str | None = None
        ai_notes: str | None = None

        # ------------------------------------------------------------------
        # 2.5) If Cortex is available, ask AI for a refined plan (bounded + validated)
        # ------------------------------------------------------------------
        ai_plan: dict[str, Any] | None = None
        if ai_available:
            try:
                import json

                # Sample a small set of columns and rows for domain inference.
                sample_cols: list[str] = []
                if key_col:
                    sample_cols.append(key_col.upper())
                if time_col and time_col.upper() not in sample_cols:
                    sample_cols.append(time_col.upper())
                for c, typ in col_types.items():
                    if c in sample_cols:
                        continue
                    if _is_complex_type(typ):
                        continue
                    if len(sample_cols) >= 12:
                        break
                    sample_cols.append(c)

                obj_parts: list[str] = []
                for c in sample_cols:
                    c_ident = _validate_ident(c, label="column")
                    obj_parts.append(f"'{c_ident}'")
                    obj_parts.append(_quote_ident(c_ident))
                obj_expr = (
                    f"OBJECT_CONSTRUCT_KEEP_NULL({', '.join(obj_parts)})"
                    if obj_parts
                    else "OBJECT_CONSTRUCT()"
                )
                if is_view or is_hybrid:
                    # Views and hybrid tables don't support TABLESAMPLE
                    sample_rows = await pool.execute_query(
                        f"SELECT {obj_expr} FROM {full_name} LIMIT 20"
                    )
                else:
                    sample_rows = await pool.execute_query(
                        f"SELECT {obj_expr} FROM {full_name} TABLESAMPLE SYSTEM (1) LIMIT 20"
                    )
                sample_payload = [r[0] for r in sample_rows if r]

                cols_for_prompt = []
                for c in list(col_types.keys())[:200]:
                    cols_for_prompt.append(
                        {
                            "name": c,
                            "type": col_types.get(c, ""),
                            "nullable": bool(col_null_ok.get(c, True)),
                            "default": col_default.get(c, "") or None,
                        }
                    )

                prompt = (
                    "You are helping configure a benchmark workload for a Snowflake table.\n"
                    f"TABLE: {db}.{sch}.{tbl}\n\n"
                    "COLUMNS (name/type/nullable/default):\n"
                    f"{json.dumps(cols_for_prompt, ensure_ascii=False)}\n\n"
                    "REQUIRED_COLUMNS (must be included in insert_columns):\n"
                    f"{json.dumps(required_cols)}\n\n"
                    "SAMPLE_ROWS (JSON objects):\n"
                    f"{json.dumps(sample_payload, ensure_ascii=False, default=str)}\n\n"
                    "Return STRICT JSON only with this schema:\n"
                    "{\n"
                    '  "domain_label": string,\n'
                    '  "insert_columns": [string],\n'
                    '  "update_columns": [string],\n'
                    '  "projection_columns": [string],\n'
                    '  "notes": string\n'
                    "}\n\n"
                    "Rules:\n"
                    "- Column names MUST be UPPERCASE and must exist in the provided columns list.\n"
                    "- insert_columns MUST include all REQUIRED_COLUMNS.\n"
                    "- update_columns MUST NOT include any columns ending with ID or KEY.\n"
                    "- Avoid VARIANT/OBJECT/ARRAY/BINARY unless REQUIRED.\n"
                    "- Keep insert_columns <= 20 and projection_columns <= 20.\n"
                )

                ai_rows = await pool.execute_query(
                    "SELECT AI_COMPLETE('claude-4-sonnet', ?)", params=[prompt]
                )
                raw = str(ai_rows[0][0]) if ai_rows and ai_rows[0] else ""
                try:
                    parsed = json.loads(raw)
                except Exception:
                    # Best-effort recovery if the model wrapped JSON with prose/code fences.
                    start = raw.find("{")
                    end = raw.rfind("}")
                    if start >= 0 and end > start:
                        parsed = json.loads(raw[start : end + 1])
                    else:
                        raise
                # Some models return a JSON *string* containing JSON. Normalize that too.
                if isinstance(parsed, str):
                    parsed_str = parsed.strip()
                    if parsed_str.startswith("{") and parsed_str.endswith("}"):
                        parsed = json.loads(parsed_str)
                if isinstance(parsed, dict):
                    ai_plan = parsed
            except Exception as e:
                logger.debug("AI plan generation failed; using heuristics: %s", e)

        def _sanitize_cols(value: Any) -> list[str]:
            if not isinstance(value, list):
                return []
            out: list[str] = []
            for v in value:
                s = str(v or "").strip().upper()
                if not s:
                    continue
                if s in col_types:
                    out.append(s)
            # de-dupe, preserve order
            seen: set[str] = set()
            deduped: list[str] = []
            for c in out:
                if c in seen:
                    continue
                seen.add(c)
                deduped.append(c)
            return deduped

        if ai_plan:
            domain_label = (
                str(ai_plan.get("domain_label")).strip()
                if ai_plan.get("domain_label") is not None
                else None
            )
            ai_notes = (
                str(ai_plan.get("notes")).strip()
                if ai_plan.get("notes") is not None
                else None
            )

            proposed_insert = _sanitize_cols(ai_plan.get("insert_columns"))[:20]
            proposed_update = _sanitize_cols(ai_plan.get("update_columns"))[:5]
            proposed_proj = _sanitize_cols(ai_plan.get("projection_columns"))[:20]

            # Never update key/id-like columns.
            filtered_update: list[str] = []
            for c in proposed_update:
                if _is_key_or_id_like_col(c):
                    continue
                filtered_update.append(c)
            proposed_update = filtered_update[:5]

            # Ensure required columns are present in insert list.
            for c in required_cols:
                if c not in proposed_insert:
                    proposed_insert.insert(0, c)

            # Filter out complex types unless required.
            filtered_insert: list[str] = []
            for c in proposed_insert:
                if c in required_cols:
                    filtered_insert.append(c)
                    continue
                if _is_complex_type(col_types.get(c, "")):
                    continue
                filtered_insert.append(c)
            proposed_insert = filtered_insert[:20]

            if proposed_insert:
                insert_cols = proposed_insert
            if proposed_update:
                update_cols = proposed_update[:1]
            if proposed_proj:
                projection_cols = proposed_proj

        # Heuristic fallback: if the model didn't supply projection columns, prefer a narrow
        # select list to avoid SELECT * on wide customer tables.
        if not projection_cols:
            # Key + time + update + required + a few simple columns.
            candidates: list[str] = []
            if key_col:
                candidates.append(str(key_col).upper())
            if time_col:
                candidates.append(str(time_col).upper())
            candidates.extend([c for c in update_cols if c])
            candidates.extend([c for c in required_cols if c])
            for c in candidates:
                c_u = str(c).strip().upper()
                if c_u and c_u in col_types and c_u not in projection_cols:
                    projection_cols.append(c_u)
            for c, typ in col_types.items():
                if len(projection_cols) >= 12:
                    break
                if c in projection_cols:
                    continue
                t = str(typ or "").upper()
                if any(x in t for x in ("VARIANT", "OBJECT", "ARRAY", "BINARY")):
                    continue
                projection_cols.append(c)
            projection_cols = projection_cols[:20]

        # Range scan strategy: prefer key-range for HYBRID, else time-cutoff when available.
        if range_mode is None:
            if time_col:
                range_mode = "TIME_CUTOFF"
            elif key_col:
                range_mode = "ID_BETWEEN"

        # ------------------------------------------------------------------
        # 3) Build value pools in Snowflake (no large data transfer through API)
        # ------------------------------------------------------------------
        pool_id = str(uuid4())

        # If template already has pools, keep history (new POOL_ID) but avoid reusing it.
        # (We intentionally do not delete old pools; template config will point at the new pool_id.)

        pools_created: dict[str, int] = {}

        # 3.1 Key pool (for point lookups / updates)
        if key_col:
            # Use a big enough pool to reduce collisions under high concurrency.
            target_n = max(5000, concurrency * 50)
            target_n = min(1_000_000, target_n)
            sample_n = min(1_000_000, max(target_n * 2, target_n))

            key_ident = _validate_ident(key_col, label="key_column")
            key_expr = _quote_ident(key_ident)

            # Use TABLESAMPLE SYSTEM for fast block-level sampling on tables.
            # For views and hybrid tables, use simple LIMIT (not random, but fast - avoids timeout).
            if is_view or is_hybrid:
                insert_key_pool = f"""
                INSERT INTO {_results_prefix()}.TEMPLATE_VALUE_POOLS (
                    POOL_ID, TEMPLATE_ID, POOL_KIND, COLUMN_NAME, SEQ, VALUE
                )
                SELECT
                    ?, ?, 'KEY', ?, SEQ4(), TO_VARIANT(KEY_VAL)
                FROM (
                    SELECT DISTINCT {key_expr} AS KEY_VAL
                    FROM {full_name}
                    WHERE {key_expr} IS NOT NULL
                    LIMIT {sample_n}
                )
                LIMIT {target_n}
                """
            else:
                insert_key_pool = f"""
                INSERT INTO {_results_prefix()}.TEMPLATE_VALUE_POOLS (
                    POOL_ID, TEMPLATE_ID, POOL_KIND, COLUMN_NAME, SEQ, VALUE
                )
                SELECT
                    ?, ?, 'KEY', ?, SEQ4(), TO_VARIANT(KEY_VAL)
                FROM (
                    SELECT DISTINCT {key_expr} AS KEY_VAL
                    FROM {full_name} TABLESAMPLE SYSTEM (10)
                    WHERE {key_expr} IS NOT NULL
                    LIMIT {sample_n}
                )
                LIMIT {target_n}
                """
            await pool.execute_query(
                insert_key_pool, params=[pool_id, template_id, key_ident]
            )
            pools_created["KEY"] = int(target_n)

        # 3.2 Range pool (time cutoffs) for time-based scans
        if time_col and range_mode == "TIME_CUTOFF":
            target_n = max(2000, concurrency * 10)
            target_n = min(1_000_000, target_n)
            sample_n = min(1_000_000, max(target_n * 2, target_n))

            time_ident = _validate_ident(time_col, label="time_column")
            time_expr = _quote_ident(time_ident)

            # Prefer generating "recent" cutoffs near time_max to avoid scans that touch most
            # of the table (e.g., cutoff in early history). This is especially important
            # for HYBRID tables.
            time_max = profile.time_max
            time_type = str(col_types.get(time_ident, "")).upper()
            if time_max is not None and any(
                x in time_type for x in ("DATE", "TIMESTAMP")
            ):
                window_days = 30
                max_param = (
                    time_max.isoformat()
                    if hasattr(time_max, "isoformat")
                    else str(time_max)
                )
                max_expr = (
                    "TO_DATE(?)" if "DATE" in time_type else "TO_TIMESTAMP_NTZ(?)"
                )
                insert_time_pool = f"""
                INSERT INTO {_results_prefix()}.TEMPLATE_VALUE_POOLS (
                    POOL_ID, TEMPLATE_ID, POOL_KIND, COLUMN_NAME, SEQ, VALUE
                )
                SELECT
                    ?, ?, 'RANGE', ?, SEQ4(),
                    TO_VARIANT(DATEADD('day', -UNIFORM(0, {window_days}, RANDOM()), {max_expr}))
                FROM TABLE(GENERATOR(ROWCOUNT => {target_n}))
                """
                await pool.execute_query(
                    insert_time_pool,
                    params=[pool_id, template_id, time_ident, max_param],
                )
                pools_created["RANGE"] = int(target_n)
            else:
                # Fallback: sample distinct time values
                if is_view or is_hybrid:
                    # Views and hybrid tables don't support TABLESAMPLE
                    insert_time_pool = f"""
                    INSERT INTO {_results_prefix()}.TEMPLATE_VALUE_POOLS (
                        POOL_ID, TEMPLATE_ID, POOL_KIND, COLUMN_NAME, SEQ, VALUE
                    )
                    SELECT
                        ?, ?, 'RANGE', ?, SEQ4(), TO_VARIANT(T_VAL)
                    FROM (
                        SELECT DISTINCT {time_expr} AS T_VAL
                        FROM {full_name}
                        WHERE {time_expr} IS NOT NULL
                        LIMIT {sample_n}
                    )
                    LIMIT {target_n}
                    """
                else:
                    insert_time_pool = f"""
                    INSERT INTO {_results_prefix()}.TEMPLATE_VALUE_POOLS (
                        POOL_ID, TEMPLATE_ID, POOL_KIND, COLUMN_NAME, SEQ, VALUE
                    )
                    SELECT
                        ?, ?, 'RANGE', ?, SEQ4(), TO_VARIANT(T_VAL)
                    FROM (
                        SELECT DISTINCT {time_expr} AS T_VAL
                        FROM {full_name} TABLESAMPLE SYSTEM (10)
                        WHERE {time_expr} IS NOT NULL
                        LIMIT {sample_n}
                    )
                    LIMIT {target_n}
                    """
                await pool.execute_query(
                    insert_time_pool, params=[pool_id, template_id, time_ident]
                )
                pools_created["RANGE"] = int(target_n)

        # ------------------------------------------------------------------
        # 4) Count inserted pool sizes (exact) and persist plan metadata into template config
        # ------------------------------------------------------------------
        counts: dict[str, int] = {}
        count_rows = await pool.execute_query(
            f"""
            SELECT POOL_KIND, COUNT(*) AS N
            FROM {_results_prefix()}.TEMPLATE_VALUE_POOLS
            WHERE TEMPLATE_ID = ?
              AND POOL_ID = ?
            GROUP BY POOL_KIND
            """,
            params=[template_id, pool_id],
        )
        for kind, n in count_rows:
            counts[str(kind)] = int(n or 0)

        ai_workload = {
            "available": ai_available,
            "model": "claude-4-sonnet",
            "availability_error": ai_error,
            "prepared_at": datetime.now(UTC).isoformat(),
            "pool_id": pool_id,
            "key_column": key_col,
            "time_column": time_col,
            "range_mode": range_mode,
            "concurrency": concurrency,
            "pools": counts,
            "insert_columns": insert_cols,
            "update_columns": update_cols,
            "projection_columns": projection_cols,
            "domain_label": domain_label,
            "ai_notes": ai_notes,
            # Track source table for staleness detection when editing templates
            "source_database": db,
            "source_schema": sch,
            "source_table": tbl,
        }

        # Store only the columns we will touch (keeps CONFIG small and avoids inserting into arbitrary columns).
        selected_cols: dict[str, str] = {}
        cols_to_store: set[str] = set()
        cols_to_store.update([c for c in insert_cols if c])
        cols_to_store.update([c for c in update_cols if c])
        if key_col:
            cols_to_store.add(key_col)
        if time_col:
            cols_to_store.add(time_col)
        for c in cols_to_store:
            c_ident = _validate_ident(c, label="column")
            selected_cols[c_ident] = col_types.get(c_ident, "VARCHAR")

        cfg2 = {**cfg, "columns": selected_cols, "ai_workload": ai_workload}
        # Keep existing template_name/description at top-level columns; only CONFIG changes here.
        await update_template(template_id, TemplateUpdate(config=cfg2))

        msg = (
            "AI workloads prepared."
            if ai_available
            else "AI not available in this account; persisted heuristic pools only."
        )
        return AiPrepareResponse(
            template_id=template_id,
            ai_available=ai_available,
            ai_error=ai_error,
            pool_id=pool_id,
            key_column=key_col,
            time_column=time_col,
            insert_columns=sorted({c for c in insert_cols if c}),
            update_columns=update_cols,
            projection_columns=projection_cols,
            domain_label=domain_label,
            pools=counts,
            message=msg,
        )

    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("prepare AI template", e)


@router.delete("/{template_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_template(template_id: str):
    """
    Delete a template.

    Args:
        template_id: UUID of the template to delete
    """
    try:
        pool = snowflake_pool.get_default_pool()

        # Check if template exists by trying to get it first
        try:
            await get_template(template_id)
        except HTTPException as e:
            if e.status_code == status.HTTP_404_NOT_FOUND:
                raise
            raise

        prefix = _results_prefix()

        # Delete all test artifacts associated with this template_id.
        # Template id is stored inside TEST_RESULTS.TEST_CONFIG (VARIANT).
        #
        # Child tables first (for cleanliness; constraints are informational in Snowflake).
        await pool.execute_query(
            f"""
            DELETE FROM {prefix}.TEST_LOGS
            WHERE TEST_ID IN (
                SELECT TEST_ID
                FROM {prefix}.TEST_RESULTS
                WHERE TEST_CONFIG:"template_id"::string = ?
            )
            """,
            params=[template_id],
        )
        await pool.execute_query(
            f"""
            DELETE FROM {prefix}.METRICS_SNAPSHOTS
            WHERE TEST_ID IN (
                SELECT TEST_ID
                FROM {prefix}.TEST_RESULTS
                WHERE TEST_CONFIG:"template_id"::string = ?
            )
            """,
            params=[template_id],
        )
        await pool.execute_query(
            f"""
            DELETE FROM {prefix}.QUERY_EXECUTIONS
            WHERE TEST_ID IN (
                SELECT TEST_ID
                FROM {prefix}.TEST_RESULTS
                WHERE TEST_CONFIG:"template_id"::string = ?
            )
            """,
            params=[template_id],
        )
        # Delete from RUN_ID-keyed tables (parent test run_id = test_id)
        await pool.execute_query(
            f"""
            DELETE FROM {prefix}.WORKER_METRICS_SNAPSHOTS
            WHERE RUN_ID IN (
                SELECT TEST_ID
                FROM {prefix}.TEST_RESULTS
                WHERE TEST_CONFIG:"template_id"::string = ?
                  AND RUN_ID = TEST_ID
            )
            """,
            params=[template_id],
        )
        await pool.execute_query(
            f"""
            DELETE FROM {prefix}.WAREHOUSE_POLL_SNAPSHOTS
            WHERE RUN_ID IN (
                SELECT TEST_ID
                FROM {prefix}.TEST_RESULTS
                WHERE TEST_CONFIG:"template_id"::string = ?
                  AND RUN_ID = TEST_ID
            )
            """,
            params=[template_id],
        )
        await pool.execute_query(
            f"""
            DELETE FROM {prefix}.FIND_MAX_STEP_HISTORY
            WHERE RUN_ID IN (
                SELECT TEST_ID
                FROM {prefix}.TEST_RESULTS
                WHERE TEST_CONFIG:"template_id"::string = ?
                  AND RUN_ID = TEST_ID
            )
            """,
            params=[template_id],
        )
        await pool.execute_query(
            f"""
            DELETE FROM {prefix}.TEST_RESULTS
            WHERE TEST_CONFIG:"template_id"::string = ?
            """,
            params=[template_id],
        )

        # Delete from Hybrid control tables (children first due to FK constraints).
        # RUN_STATUS.RUN_ID matches TEST_RESULTS.TEST_ID for parent runs.
        await pool.execute_query(
            f"""
            DELETE FROM {prefix}.WORKER_HEARTBEATS
            WHERE RUN_ID IN (
                SELECT RUN_ID FROM {prefix}.RUN_STATUS
                WHERE TEMPLATE_ID = ?
            )
            """,
            params=[template_id],
        )
        await pool.execute_query(
            f"""
            DELETE FROM {prefix}.RUN_CONTROL_EVENTS
            WHERE RUN_ID IN (
                SELECT RUN_ID FROM {prefix}.RUN_STATUS
                WHERE TEMPLATE_ID = ?
            )
            """,
            params=[template_id],
        )
        await pool.execute_query(
            f"DELETE FROM {prefix}.RUN_STATUS WHERE TEMPLATE_ID = ?",
            params=[template_id],
        )

        # Pools are keyed by template_id (not test_id).
        await pool.execute_query(
            f"DELETE FROM {prefix}.TEMPLATE_VALUE_POOLS WHERE TEMPLATE_ID = ?",
            params=[template_id],
        )

        # Finally delete the template record.
        await pool.execute_query(
            f"DELETE FROM {prefix}.TEST_TEMPLATES WHERE TEMPLATE_ID = ?",
            params=[template_id],
        )

    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("delete template", e)


@router.post("/{template_id}/use", response_model=Dict[str, Any])
async def use_template(template_id: str):
    """
    Mark a template as used and increment usage counter.

    Args:
        template_id: UUID of the template

    Returns:
        Success message and updated usage count
    """
    try:
        pool = snowflake_pool.get_default_pool()

        now = datetime.now(UTC).isoformat()
        query = f"""
        UPDATE {_results_prefix()}.TEST_TEMPLATES
        SET 
            USAGE_COUNT = USAGE_COUNT + 1,
            LAST_USED_AT = '{now}'
        WHERE TEMPLATE_ID = '{template_id}'
        """

        await pool.execute_query(query)

        template = await get_template(template_id)

        return {
            "message": "Template usage recorded",
            "template_id": template_id,
            "usage_count": template["usage_count"],
        }

    except HTTPException:
        raise
    except Exception as e:
        raise http_exception("record template usage", e)
