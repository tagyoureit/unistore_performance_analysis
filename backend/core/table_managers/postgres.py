"""
Postgres Table Manager (Existing Objects Only)

Benchmarks existing Postgres tables and views across:
- Standalone Postgres
- Snowflake via Postgres protocol

Table creation is intentionally not supported by this app.
"""

from __future__ import annotations

import asyncio
import logging
import ssl
from typing import Any

from backend.connectors import postgres_pool
from backend.core.table_managers.base import TableManager
from backend.models.test_config import TableType

logger = logging.getLogger(__name__)


class PostgresTableManager(TableManager):
    """Manages existing Postgres tables/views (no DDL)."""

    def __init__(self, config):
        super().__init__(config)

        # Get database from config (selected in UI), with fallback
        database = str(getattr(config, "database", "") or "").strip()
        if not database:
            # Fallback to env var if no database in config
            from backend.config import settings

            database = settings.POSTGRES_DATABASE

        self._database = database
        self._connection_id = getattr(config, "connection_id", None)
        self._pool_initialized = False
        self.pool = None  # Will be initialized lazily
        
    async def _ensure_pool(self):
        """Initialize the connection pool if not already done."""
        if self._pool_initialized:
            return
            
        if self._connection_id:
            # Use stored connection credentials with SSL
            from backend.core import connection_manager
            
            conn_params = await connection_manager.get_connection_for_pool(self._connection_id)
            if not conn_params:
                raise ValueError(f"Connection {self._connection_id} not found")
            
            # Create SSL context for Snowflake Postgres (self-signed certs)
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            self.pool = postgres_pool.PostgresConnectionPool(
                host=conn_params["host"],
                port=conn_params["port"],
                database=self._database,
                user=conn_params["user"],
                password=conn_params["password"],
                min_size=1,
                max_size=2,
                pool_name="table_manager",
                ssl=ssl_context,
            )
            await self.pool.initialize()
            logger.info(
                "PostgresTableManager: Using stored connection %s for %s",
                self._connection_id,
                self._database,
            )
        else:
            # Fallback to environment variables (backward compat)
            self.pool = postgres_pool.get_pool_for_database(self._database)
            await self.pool.initialize()
            logger.info(
                "PostgresTableManager: Using env-based pool for %s",
                self._database,
            )
        
        self._pool_initialized = True

    def get_full_table_name(self) -> str:
        """
        Return a Postgres-qualified object name.

        Postgres does not support cross-database references within a single query,
        so we use only schema + table. The database is determined by the pool connection.
        """
        schema = str(self.schema_name or "public").strip()
        name = str(self.table_name or "").strip()
        return f"{schema}.{name}" if schema else name

    @staticmethod
    def _map_column_type(
        data_type: str, udt_name: str, char_max_len: int | None
    ) -> str:
        """
        Map Postgres information_schema types to the simplified type strings used
        by the benchmark workload generators.
        """
        t = str(data_type or "").strip().upper()
        u = str(udt_name or "").strip().upper()
        raw = t or u

        if "TIMESTAMP" in raw:
            return "TIMESTAMP"
        if raw == "DATE" or "DATE" in raw:
            return "DATE"
        if any(
            x in raw for x in ("INT", "NUMERIC", "DECIMAL", "REAL", "DOUBLE", "FLOAT")
        ):
            return "NUMBER"

        # Strings: preserve CHAR/VARCHAR lengths when available so generated values
        # do not violate CHAR(n) constraints (e.g. CHAR(1) order status columns).
        try:
            n = int(char_max_len) if char_max_len is not None else None
        except Exception:
            n = None
        if n is not None and n <= 0:
            n = None

        if u == "TEXT" or raw == "TEXT" or "TEXT" in raw:
            return "VARCHAR"
        if u == "VARCHAR" or "CHARACTER VARYING" in raw or "VARCHAR" in raw:
            return f"VARCHAR({n})" if n is not None else "VARCHAR"
        if (
            u in {"BPCHAR", "CHAR"}
            or raw == "CHARACTER"
            or ("CHAR" in raw and "VARYING" not in raw)
        ):
            return f"CHAR({n})" if n is not None else "CHAR"

        if "BOOL" in raw:
            return "BOOLEAN"
        return raw or "VARCHAR"

    async def get_table_stats(self) -> dict[str, Any]:
        """Get basic table/view statistics."""
        await self._ensure_pool()
        full_name = self.get_full_table_name()
        stats: dict[str, Any] = {}

        try:
            row_count = await self.pool.fetch_val(f"SELECT COUNT(*) FROM {full_name}")
            stats["row_count"] = int(row_count or 0)
        except Exception as e:
            stats["row_count"] = None
            stats["row_count_error"] = str(e)

        if self.object_type:
            stats["object_type"] = self.object_type

        # Best-effort size for tables.
        if (self.object_type or "").upper() == "TABLE":
            try:
                size_bytes = await self.pool.fetch_val(
                    f"SELECT pg_total_relation_size('{full_name}')"
                )
                if size_bytes is not None:
                    size_int = int(size_bytes)
                    stats["bytes"] = size_int
                    stats["size_mb"] = round(size_int / (1024 * 1024), 2)
            except Exception:
                pass

        return stats

    async def table_exists(self) -> bool:
        """Check if the selected object exists as a table or view."""
        await self._ensure_pool()
        schema = str(self.schema_name or "public").strip()
        name = str(self.table_name or "").strip()
        if not name:
            return False

        try:
            is_table = bool(
                await self.pool.fetch_val(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.tables
                        WHERE table_schema = $1
                          AND table_name = $2
                          AND table_type = 'BASE TABLE'
                    )
                    """,
                    schema,
                    name,
                )
            )
            if is_table:
                self.object_type = "TABLE"
                return True

            is_view = bool(
                await self.pool.fetch_val(
                    """
                    SELECT EXISTS (
                        SELECT 1
                        FROM information_schema.views
                        WHERE table_schema = $1
                          AND table_name = $2
                    )
                    """,
                    schema,
                    name,
                )
            )
            if is_view:
                self.object_type = "VIEW"
                return True

            return False
        except Exception as e:
            logger.debug("Error checking object existence: %s", e)
            return False

    async def validate_schema(self) -> bool:
        """Introspect the existing table/view schema and hydrate `config.columns`."""
        await self._ensure_pool()
        schema = str(self.schema_name or "public").strip()
        name = str(self.table_name or "").strip()
        if not name:
            return False

        try:
            rows = await self.pool.fetch_all(
                """
                SELECT column_name, data_type, udt_name, character_maximum_length
                FROM information_schema.columns
                WHERE table_schema = $1
                  AND table_name = $2
                ORDER BY ordinal_position
                """,
                schema,
                name,
            )
            if not rows:
                return False

            columns: dict[str, str] = {}
            for row in rows:
                col_name = str(row["column_name"]).strip()
                if not col_name:
                    continue
                columns[col_name] = self._map_column_type(
                    row["data_type"],
                    row["udt_name"],
                    row["character_maximum_length"],
                )

            if not columns:
                return False

            # If object type wasn't resolved during existence check, infer best-effort here.
            if self.object_type is None:
                try:
                    is_table = bool(
                        await self.pool.fetch_val(
                            """
                            SELECT EXISTS (
                                SELECT 1
                                FROM information_schema.tables
                                WHERE table_schema = $1
                                  AND table_name = $2
                                  AND table_type = 'BASE TABLE'
                            )
                            """,
                            schema,
                            name,
                        )
                    )
                    self.object_type = "TABLE" if is_table else "VIEW"
                except Exception:
                    pass

            self.config.columns = columns
            logger.info("✅ Schema validation passed: %s", self.get_full_table_name())
            return True
        except Exception as e:
            logger.error("Schema validation failed: %s", e)
            return False
