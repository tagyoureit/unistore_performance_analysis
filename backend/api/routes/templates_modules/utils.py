"""
Utility functions for template management.
"""

import logging
from typing import Any

from backend.config import settings

from .constants import _IDENT_RE

logger = logging.getLogger(__name__)


def _upper_str(v: Any) -> str:
    return str(v or "").strip().upper()


def _validate_ident(name: Any, *, label: str) -> str:
    """
    Validate a Snowflake identifier component (DATABASE / SCHEMA / TABLE / COLUMN).

    For now we intentionally restrict to unquoted identifiers:
    - letters, digits, underscore
    - uppercased

    This avoids SQL injection and keeps generated SQL predictable.
    """
    value = _upper_str(name)
    if not value:
        raise ValueError(f"Missing {label}")
    if not _IDENT_RE.fullmatch(value):
        raise ValueError(f"Invalid {label}: {value!r} (expected [A-Z0-9_]+)")
    return value


def _quote_ident(name: str) -> str:
    # Identifiers are already validated to [A-Z0-9_]+, so this is safe.
    return f'"{name}"'


def _is_postgres_family_table_type(table_type_raw: Any) -> bool:
    """
    True when the template targets a Postgres-family backend:
    - POSTGRES
    """
    t = _upper_str(table_type_raw)
    return t == "POSTGRES"


def _pg_quote_ident(name: str) -> str:
    """
    Quote a Postgres identifier defensively.

    Postgres does not support binding identifiers, so we must ensure safe quoting when we
    build introspection/sample queries against customer objects.
    """
    s = str(name or "")
    return '"' + s.replace('"', '""') + '"'


def _pg_qualified_name(schema: str, table: str) -> str:
    sch = str(schema or "").strip()
    tbl = str(table or "").strip()
    if not sch or not tbl:
        raise ValueError("schema and table_name are required")
    return f"{_pg_quote_ident(sch)}.{_pg_quote_ident(tbl)}"


def _pg_placeholders(n: int, *, start: int = 1) -> str:
    if int(n) <= 0:
        return ""
    s = int(start)
    return ", ".join(f"${i}" for i in range(s, s + int(n)))


def _full_table_name(database: str, schema: str, table: str) -> str:
    db = _validate_ident(database, label="database")
    sch = _validate_ident(schema, label="schema")
    tbl = _validate_ident(table, label="table")
    return f"{_quote_ident(db)}.{_quote_ident(sch)}.{_quote_ident(tbl)}"


def _sample_clause(is_view: bool, limit: int, percentage: int = 10) -> str:
    """
    Return appropriate SQL sampling clause.

    TABLESAMPLE SYSTEM is fast (block-level) but doesn't work on views.
    For views, we just use LIMIT which gets sequential rows (not random but fast).
    """
    if is_view:
        # Views don't support TABLESAMPLE; use simple LIMIT (not random but fast)
        return f"LIMIT {limit}"
    else:
        # Tables: use fast block-level sampling
        return f"TABLESAMPLE SYSTEM ({percentage}) LIMIT {limit}"


def _results_prefix() -> str:
    return f"{settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}"


def _coerce_int(v: Any, *, label: str) -> int:
    try:
        # Allow numeric strings and floats; coerce to int.
        out = int(float(v))
    except Exception as e:
        raise ValueError(f"Invalid {label}: {v!r}") from e
    return out


def _enrich_postgres_instance_size(cfg: dict[str, Any]) -> dict[str, Any]:
    """
    Enrich template config with postgres_instance_size for Postgres table types.
    
    Looks up the actual instance size from:
    1. The stored connection's host (if connection_id is set)
    2. The configured POSTGRES_HOST environment variable (fallback)
    
    This is only populated for POSTGRES table types.
    For other table types, postgres_instance_size is set to None.
    """
    table_type = str(cfg.get("table_type") or "").upper().strip()
    
    if table_type != "POSTGRES":
        # Not a Postgres template - clear any stale value
        cfg["postgres_instance_size"] = None
        return cfg
    
    # Try to look up the actual instance size
    try:
        from backend.core.cost_calculator import get_postgres_instance_size_by_host
        
        # First try to get host from stored connection
        connection_id = cfg.get("connection_id")
        host_to_lookup = None
        
        if connection_id:
            # Try to get host from connection (sync lookup via cached connections)
            try:
                import asyncio
                from backend.core import connection_manager
                
                async def _get_host():
                    conn_params = await connection_manager.get_connection_for_pool(connection_id)
                    return conn_params.get("host") if conn_params else None
                
                # Try to run in existing event loop or create one
                try:
                    loop = asyncio.get_running_loop()
                    # Can't await in sync context, fall back to env var
                    host_to_lookup = settings.POSTGRES_HOST
                except RuntimeError:
                    # No running loop, create one
                    host_to_lookup = asyncio.run(_get_host())
            except Exception as e:
                logger.debug(f"Could not get host from connection {connection_id}: {e}")
                host_to_lookup = settings.POSTGRES_HOST
        else:
            # Fall back to environment variable
            host_to_lookup = settings.POSTGRES_HOST
        
        if host_to_lookup:
            actual_size = get_postgres_instance_size_by_host(host_to_lookup)
            if actual_size:
                cfg["postgres_instance_size"] = actual_size
                logger.debug(f"Set postgres_instance_size to {actual_size} from host {host_to_lookup}")
            else:
                # Fall back to default from settings
                cfg["postgres_instance_size"] = settings.POSTGRES_INSTANCE_SIZE or "STANDARD_M"
                logger.debug(f"Using default postgres_instance_size: {cfg['postgres_instance_size']}")
        else:
            cfg["postgres_instance_size"] = settings.POSTGRES_INSTANCE_SIZE or "STANDARD_M"
    except Exception as e:
        logger.warning(f"Failed to lookup Postgres instance size: {e}")
        cfg["postgres_instance_size"] = settings.POSTGRES_INSTANCE_SIZE or "STANDARD_M"
    
    return cfg


def _row_to_dict(row, columns):
    """Convert result row tuple to dictionary."""
    return dict(zip([col.lower() for col in columns], row))
