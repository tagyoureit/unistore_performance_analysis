"""
API routes for database/schema/table discovery.

This powers the Configure UI dropdowns so users select from *existing* objects.

Supported backends:
- Snowflake (standard/hybrid/interactive tables): list databases, schemas, tables, and views
- Postgres: list databases, schemas, tables, and views

Notes:
- For Postgres-family connections, we dynamically query available databases by connecting to the
  `postgres` database (which always exists), then connect to the user-selected database for
  schema/table discovery.
- These endpoints intentionally avoid returning full DDL or any data; they only return names/types.
"""

from __future__ import annotations

import logging
import re
import ssl
from typing import Any, Optional

import asyncpg
from fastapi import APIRouter, HTTPException, Query, status

from backend.api.error_handling import http_exception
from backend.connectors import postgres_pool, snowflake_pool
from backend.core import connection_manager
from backend.models.test_config import TableType

router = APIRouter()
logger = logging.getLogger(__name__)


async def _postgres_fetch(
    connection_id: str,
    database: str,
    query: str,
    *args,
    timeout: float = 30.0,
) -> list[asyncpg.Record]:
    """
    Execute a query against a specific Postgres database using connection credentials.
    
    This connects directly to Postgres (not PgBouncer) for catalog operations.
    Uses SSL for Snowflake Postgres instances.
    """
    conn_params = await connection_manager.get_connection_for_pool(connection_id)
    if not conn_params:
        raise ValueError(f"Connection not found: {connection_id}")
    
    if conn_params.get("connection_type") != "POSTGRES":
        raise ValueError(f"Connection {connection_id} is not a Postgres connection")
    
    # Create SSL context that doesn't verify certificates (Snowflake Postgres uses self-signed)
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    conn = None
    try:
        conn = await asyncpg.connect(
            host=conn_params["host"],
            port=conn_params["port"],  # Direct port, not PgBouncer
            database=database,
            user=conn_params["user"],
            password=conn_params["password"],
            timeout=timeout,
            ssl=ssl_context,
        )
        return await conn.fetch(query, *args, timeout=timeout)
    finally:
        if conn:
            await conn.close()

_IDENT_RE = re.compile(r"^[A-Z0-9_]+$")


def _upper_str(v: Any) -> str:
    return str(v or "").strip().upper()


def _validate_ident(value: Any, *, label: str) -> str:
    """
    Validate identifier components we interpolate into SHOW commands.

    Snowflake SHOW statements do not support bind variables for object identifiers, so we enforce:
    - unquoted identifiers only
    - [A-Z0-9_]+
    """

    name = _upper_str(value)
    if not name:
        raise ValueError(f"Missing {label}")
    if not _IDENT_RE.fullmatch(name):
        raise ValueError(f"Invalid {label}: {name!r} (expected [A-Z0-9_]+)")
    return name


def _table_type(value: Any) -> TableType:
    if not value:
        return TableType.STANDARD
    raw = str(value).strip().lower()
    return TableType(raw)


def _is_postgres_family(t: TableType) -> bool:
    return t == TableType.POSTGRES


def _get_postgres_pool(table_type: TableType):
    return postgres_pool.get_default_pool()


def _postgres_pool_type(table_type: TableType) -> str:
    """Return pool_type string for fetch_from_database()."""
    return "default"


@router.get("/databases", response_model=list[dict[str, Any]])
async def list_databases(
    table_type: str = Query("standard"),
    connection_id: Optional[str] = Query(None),
):
    """
    List available databases.

    - Snowflake: returns all databases visible to the configured role.
    - Postgres-family: connects to 'postgres' DB and queries pg_database.
    """

    t = _table_type(table_type)
    if _is_postgres_family(t):
        if not connection_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="connection_id is required for Postgres"
            )
        try:
            # Connect to 'postgres' database (always exists) to list available databases
            # Catalog operations use direct PostgreSQL (port 5432), not PgBouncer
            rows = await _postgres_fetch(
                connection_id=connection_id,
                database="postgres",
                query="""
                    SELECT datname
                    FROM pg_database
                    WHERE datistemplate = false
                      AND datallowconn = true
                    ORDER BY datname
                """,
            )
            # Preserve original case for Postgres (it's case-sensitive)
            return [{"name": str(r["datname"]), "type": "DATABASE"} for r in rows]
        except Exception as e:
            raise http_exception("list Postgres databases", e)

    sf = snowflake_pool.get_default_pool()
    rows = await sf.execute_query("SHOW DATABASES")

    out: list[dict[str, Any]] = []
    for row in rows:
        # SHOW DATABASES: name is typically at index 1 (created_on, name, ...)
        name = None
        if row and len(row) > 1:
            name = row[1]
        elif row:
            name = row[0]
        name_str = _upper_str(name)
        if name_str:
            out.append({"name": name_str, "type": "DATABASE"})

    out.sort(key=lambda x: x["name"])
    return out


@router.get("/schemas", response_model=list[dict[str, Any]])
async def list_schemas(
    table_type: str = Query("standard"),
    database: str | None = Query(None),
    connection_id: Optional[str] = Query(None),
):
    """
    List schemas in a database.

    - Snowflake: requires database, lists schemas in that database.
    - Postgres-family: requires database and connection_id, connects to that database to list schemas.
    """

    t = _table_type(table_type)
    if _is_postgres_family(t):
        db_name = str(database or "").strip()
        if not db_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Missing database parameter",
            )
        if not connection_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="connection_id is required for Postgres"
            )
        try:
            rows = await _postgres_fetch(
                connection_id=connection_id,
                database=db_name,
                query="""
                    SELECT schema_name
                    FROM information_schema.schemata
                    ORDER BY schema_name
                """,
            )
            return [{"name": str(r["schema_name"]), "type": "SCHEMA"} for r in rows]
        except Exception as e:
            raise http_exception(f"list schemas in {db_name}", e)

    try:
        db = _validate_ident(database, label="database")
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)
        ) from e

    sf = snowflake_pool.get_default_pool()
    rows = await sf.execute_query(f"SHOW SCHEMAS IN DATABASE {db}")

    out: list[dict[str, Any]] = []
    for row in rows:
        # SHOW SCHEMAS: name is typically at index 1
        name = None
        if row and len(row) > 1:
            name = row[1]
        elif row:
            name = row[0]
        name_str = _upper_str(name)
        if name_str:
            out.append({"name": name_str, "type": "SCHEMA"})

    out.sort(key=lambda x: x["name"])
    return out


def _include_views(t: TableType) -> bool:
    """
    Determine whether to include views for a given table type.

    - STANDARD: tables + views (views can be built on standard tables)
    - HYBRID / INTERACTIVE: tables only (views don't make sense on these)
    - Postgres family: tables + views
    """
    if t in (TableType.HYBRID, TableType.INTERACTIVE):
        return False
    return True


def _matches_filter(detected_type: str, filter_type: str | None) -> bool:
    """Check if a detected_type matches the requested filter_type."""
    if not filter_type or filter_type.upper() == "ALL":
        return True
    return detected_type.upper() == filter_type.upper()


@router.get("/objects", response_model=list[dict[str, Any]])
async def list_objects(
    table_type: str = Query("standard"),
    database: str | None = Query(None),
    schema: str | None = Query(None),
    filter_type: str | None = Query(None),
    connection_id: Optional[str] = Query(None),
):
    """
    List tables (and optionally views) in a schema.

    Returns objects as:
      { "name": "...", "type": "TABLE" | "VIEW", "detected_type": "STANDARD" | "HYBRID" | "DYNAMIC" | "INTERACTIVE" | "VIEW" }

    Detection logic:
    - HYBRID: IS_HYBRID=YES in INFORMATION_SCHEMA
    - INTERACTIVE: IS_DYNAMIC=YES AND appears in SHOW INTERACTIVE TABLES (Unistore)
    - DYNAMIC: IS_DYNAMIC=YES AND does NOT appear in SHOW INTERACTIVE TABLES
    - STANDARD: Everything else

    Note: In Unistore-enabled accounts, tables created with CREATE DYNAMIC TABLE 
    appear in SHOW INTERACTIVE TABLES (not SHOW DYNAMIC TABLES) and are treated
    as Interactive Tables for benchmarking purposes.

    For HYBRID and INTERACTIVE table types, only tables are returned (no views).
    For STANDARD and Postgres-family types, both tables and views are returned.

    The `filter_type` parameter filters results by detected_type:
      - ALL (default): return all objects
      - STANDARD: only standard tables
      - HYBRID: only hybrid tables
      - DYNAMIC: only dynamic tables (traditional, non-Unistore)
      - INTERACTIVE: only interactive tables (Unistore)
      - VIEW: only views
    """

    t = _table_type(table_type)
    include_views = _include_views(t)

    if _is_postgres_family(t):
        db_name = str(database or "").strip()
        schema_name = str(schema or "").strip()
        if not db_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Missing database"
            )
        if not schema_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="Missing schema"
            )
        if not connection_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="connection_id is required for Postgres"
            )

        try:
            out: list[dict[str, Any]] = []

            # Tables (detected_type is TABLE for Postgres)
            if _matches_filter("TABLE", filter_type):
                table_rows = await _postgres_fetch(
                    connection_id,
                    db_name,
                    """
                        SELECT table_name
                        FROM information_schema.tables
                        WHERE table_schema = $1
                          AND table_type = 'BASE TABLE'
                        ORDER BY table_name
                    """,
                    schema_name,
                )
                out.extend(
                    {
                        "name": str(r["table_name"]),
                        "type": "TABLE",
                        "detected_type": "TABLE",
                    }
                    for r in table_rows
                )

            # Views (only if applicable for this table type)
            if include_views and _matches_filter("VIEW", filter_type):
                view_rows = await _postgres_fetch(
                    connection_id,
                    db_name,
                    """
                        SELECT table_name
                        FROM information_schema.views
                        WHERE table_schema = $1
                        ORDER BY table_name
                    """,
                    schema_name,
                )
                out.extend(
                    {
                        "name": str(r["table_name"]),
                        "type": "VIEW",
                        "detected_type": "VIEW",
                    }
                    for r in view_rows
                )

            out.sort(key=lambda x: (x["detected_type"], x["name"]))
            return out
        except Exception as e:
            raise http_exception(f"list objects in {db_name}.{schema_name}", e)

    try:
        db = _validate_ident(database, label="database")
        sch = _validate_ident(schema, label="schema")
    except ValueError as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)
        ) from e

    sf = snowflake_pool.get_default_pool()

    out: list[dict[str, Any]] = []

    # First, get list of Interactive Tables (Unistore) via SHOW INTERACTIVE TABLES
    # This is needed because IS_DYNAMIC=YES in INFORMATION_SCHEMA doesn't distinguish
    # between Dynamic Tables and Interactive Tables
    interactive_table_names: set[str] = set()
    try:
        interactive_rows = await sf.execute_query(
            f"SHOW INTERACTIVE TABLES IN SCHEMA {db}.{sch}"
        )
        for row in interactive_rows:
            # SHOW INTERACTIVE TABLES: name is at index 1
            if row and len(row) > 1:
                interactive_table_names.add(_upper_str(row[1]))
    except Exception as e:
        # SHOW INTERACTIVE TABLES may not be available in all accounts
        logger.debug(f"SHOW INTERACTIVE TABLES not available: {e}")

    # Query INFORMATION_SCHEMA.TABLES for type detection (IS_HYBRID, IS_DYNAMIC)
    # This gives us more metadata than SHOW TABLES
    try:
        info_schema_rows = await sf.execute_query(
            f"""
            SELECT
                TABLE_NAME,
                TABLE_TYPE,
                COALESCE(IS_HYBRID, 'NO') AS IS_HYBRID,
                COALESCE(IS_DYNAMIC, 'NO') AS IS_DYNAMIC
            FROM {db}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{sch}'
              AND TABLE_TYPE IN ('BASE TABLE', 'VIEW', 'INTERACTIVE TABLE')
            ORDER BY TABLE_NAME
            """
        )

        for row in info_schema_rows:
            # Row format: (TABLE_NAME, TABLE_TYPE, IS_HYBRID, IS_DYNAMIC)
            name_str = _upper_str(row[0]) if row else None
            sf_table_type = str(row[1]).upper() if row and len(row) > 1 else "BASE TABLE"
            is_hybrid = str(row[2]).upper() if row and len(row) > 2 else "NO"
            is_dynamic = str(row[3]).upper() if row and len(row) > 3 else "NO"

            if not name_str:
                continue

            # Determine detected_type
            # Priority: INTERACTIVE TABLE (static) > VIEW > HYBRID > INTERACTIVE (dynamic) > DYNAMIC > STANDARD
            # Static Interactive Tables have TABLE_TYPE='INTERACTIVE TABLE'
            # Dynamic Interactive Tables have IS_DYNAMIC=YES and appear in SHOW INTERACTIVE TABLES
            if sf_table_type == "INTERACTIVE TABLE":
                # Static Interactive Table - true Unistore table
                detected_type = "INTERACTIVE"
                obj_type = "TABLE"
            elif sf_table_type == "VIEW":
                detected_type = "VIEW"
                obj_type = "VIEW"
            elif is_hybrid == "YES":
                detected_type = "HYBRID"
                obj_type = "TABLE"
            elif is_dynamic == "YES" and name_str in interactive_table_names:
                # Dynamic Interactive Table (Unistore) - detected via SHOW INTERACTIVE TABLES
                detected_type = "INTERACTIVE"
                obj_type = "TABLE"
            elif is_dynamic == "YES":
                # Dynamic Table (materialized view) - not in SHOW INTERACTIVE TABLES
                detected_type = "DYNAMIC"
                obj_type = "TABLE"
            else:
                detected_type = "STANDARD"
                obj_type = "TABLE"

            # Skip views if not included for this table_type
            if obj_type == "VIEW" and not include_views:
                continue

            # Apply filter
            if not _matches_filter(detected_type, filter_type):
                continue

            out.append({
                "name": name_str,
                "type": obj_type,
                "detected_type": detected_type,
            })

    except Exception as e:
        # Fallback to SHOW TABLES if INFORMATION_SCHEMA query fails
        # (e.g., older Snowflake versions without IS_HYBRID/IS_DYNAMIC)
        logger.warning(
            f"INFORMATION_SCHEMA query failed, falling back to SHOW: {e}"
        )

        table_rows = await sf.execute_query(f"SHOW TABLES IN SCHEMA {db}.{sch}")

        for row in table_rows:
            name = None
            if row and len(row) > 1:
                name = row[1]
            elif row:
                name = row[0]
            name_str = _upper_str(name)
            if name_str:
                # Without INFORMATION_SCHEMA, we can't detect type - default to STANDARD
                if _matches_filter("STANDARD", filter_type):
                    out.append({
                        "name": name_str,
                        "type": "TABLE",
                        "detected_type": "STANDARD",
                    })

        # Views (only for STANDARD table type)
        if include_views and _matches_filter("VIEW", filter_type):
            view_rows = await sf.execute_query(f"SHOW VIEWS IN SCHEMA {db}.{sch}")
            for row in view_rows:
                name = None
                if row and len(row) > 1:
                    name = row[1]
                elif row:
                    name = row[0]
                name_str = _upper_str(name)
                if name_str:
                    out.append({
                        "name": name_str,
                        "type": "VIEW",
                        "detected_type": "VIEW",
                    })

    out.sort(key=lambda x: (x["detected_type"], x["name"]))
    return out


@router.get("/postgres/capabilities")
async def get_postgres_capabilities(
    table_type: str = Query(..., description="POSTGRES"),
    database: str = Query(..., description="Target database name"),
    connection_id: Optional[str] = Query(None, description="Connection ID for Postgres"),
) -> dict[str, Any]:
    """
    Check PostgreSQL server capabilities for pg_stat_statements enrichment.

    Returns:
        - capabilities: Server capability flags (pg_stat_statements, track_io_timing, etc.)
        - warnings: User-facing warnings for missing capabilities with remediation steps
        - available: True if pg_stat_statements is available for enrichment

    This endpoint should be called when loading a test configuration to notify users
    if server-side metrics won't be available.
    """
    import asyncpg

    from backend.core.postgres_stats import get_capability_warnings, get_pg_capabilities

    table_type_upper = table_type.strip().upper()
    if table_type_upper != "POSTGRES":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Invalid table_type: {table_type}. Must be POSTGRES.",
        )

    if not connection_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="connection_id is required for Postgres"
        )

    conn_params = await connection_manager.get_connection_for_pool(connection_id)
    if not conn_params:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Connection not found: {connection_id}"
        )
    
    if conn_params.get("connection_type") != "POSTGRES":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Connection {connection_id} is not a Postgres connection"
        )

    # Create SSL context that doesn't verify certificates (Snowflake Postgres uses self-signed)
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    conn = None
    try:
        conn = await asyncpg.connect(
            host=conn_params["host"],
            port=conn_params["port"],  # Direct port, not PgBouncer
            database=database,
            user=conn_params["user"],
            password=conn_params["password"],
            timeout=10.0,
            ssl=ssl_context,
        )

        capabilities = await get_pg_capabilities(conn)
        warnings = get_capability_warnings(capabilities)

        return {
            "available": capabilities.pg_stat_statements_available,
            "capabilities": {
                "pg_stat_statements_available": capabilities.pg_stat_statements_available,
                "track_io_timing": capabilities.track_io_timing,
                "track_planning": capabilities.track_planning,
                "shared_buffers": capabilities.shared_buffers,
                "pg_version": capabilities.pg_version,
            },
            "warnings": warnings,
        }

    except asyncpg.InvalidCatalogNameError:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Database '{database}' not found.",
        )
    except Exception as e:
        raise http_exception(f"check Postgres capabilities for {database}", e)
    finally:
        if conn:
            await conn.close()
