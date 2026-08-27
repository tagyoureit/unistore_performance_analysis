"""
Interactive Table Manager

Manages Snowflake interactive tables (GA).

Interactive tables use CREATE INTERACTIVE TABLE ... CLUSTER BY and must be
served by an interactive warehouse. This manager extends StandardTableManager
with interactive-specific setup: verifying clustering keys, checking warehouse
attachment, and warming the cache before benchmarks begin.
"""

from __future__ import annotations

import logging
import time
from typing import Any

from backend.core.table_managers.standard import StandardTableManager
from backend.connectors import snowflake_pool

logger = logging.getLogger(__name__)


class InteractiveTableManager(StandardTableManager):
    """
    Manages Snowflake interactive tables.

    Key differences from standard tables:
    - Requires CLUSTER BY (mandatory, not optional)
    - Must be attached to an interactive warehouse for fast reads
    - Cache warming matters: first access is 5-7x slower than warm
    - 5-second statement timeout on interactive execution path
    """

    async def get_table_stats(self) -> dict[str, Any]:
        """
        Get interactive table statistics including clustering info.
        """
        stats = await super().get_table_stats()

        full_name = self.get_full_table_name()
        try:
            rows = await self.pool.execute_query(
                f"SHOW INTERACTIVE TABLES LIKE '{self.table_name}'"
                + (
                    f" IN {self.database}.{self.schema_name}"
                    if self.database and self.schema_name
                    else ""
                )
            )
            if rows:
                stats["is_interactive_table"] = True
                stats["table_format"] = "INTERACTIVE"
            else:
                stats["is_interactive_table"] = False
                stats["table_format"] = "STANDARD"
        except Exception as e:
            logger.debug("SHOW INTERACTIVE TABLES failed: %s", e)
            stats["is_interactive_table"] = None

        return stats

    async def table_exists(self) -> bool:
        """
        Check if the interactive table exists.

        Tries SHOW INTERACTIVE TABLES first, falls back to standard check
        (the table might be a standard table attached to an interactive WH).
        """
        try:
            check = f"SHOW INTERACTIVE TABLES LIKE '{self.table_name}'"
            if self.database and self.schema_name:
                check += f" IN {self.database}.{self.schema_name}"

            rows = await self.pool.execute_query(check)
            if rows:
                self.object_type = "INTERACTIVE TABLE"
                return True
        except Exception as e:
            logger.debug("SHOW INTERACTIVE TABLES failed, trying standard: %s", e)

        # Fall back to standard table check (supports standard tables on IWH)
        return await super().table_exists()

    async def validate_schema(self) -> bool:
        """
        Validate schema and check for clustering key presence.

        Interactive tables without a clustering key cannot be pruned by the
        interactive warehouse, causing queries to scan all partitions and
        potentially hit the 5-second timeout.
        """
        valid = await super().validate_schema()
        if not valid:
            return False

        stats = self._stats or await self.get_table_stats()
        clustering_key = stats.get("clustering_key")

        if not clustering_key:
            logger.warning(
                "Interactive table %s has no clustering key. "
                "Queries may hit the 5-second timeout without partition pruning.",
                self.get_full_table_name(),
            )

        return True

    async def check_warehouse_attachment(self, warehouse_name: str) -> bool:
        """
        Verify the table is attached to the specified interactive warehouse.

        Args:
            warehouse_name: Name of the interactive warehouse to check

        Returns:
            True if the table is attached to the warehouse
        """
        full_name = self.get_full_table_name()
        try:
            rows = await self.pool.execute_query(
                f"SHOW TABLES ATTACHED TO WAREHOUSE {warehouse_name}"
            )
            if rows:
                attached_tables = [str(r[0]).upper() for r in rows if r]
                return full_name.upper() in attached_tables
        except Exception as e:
            logger.debug("Could not check warehouse attachment: %s", e)

        return False

    async def warm_cache(self, warehouse_name: str, timeout_s: int = 120) -> dict[str, Any]:
        """
        Warm the interactive cache by running a lightweight probe query.

        The first query after table attachment fetches from remote storage
        (typically 5-7x slower than warm). This method runs a probe query
        repeatedly until latency stabilizes, indicating the cache is warm.

        Args:
            warehouse_name: Interactive warehouse to warm
            timeout_s: Maximum seconds to wait for warming

        Returns:
            Dict with warming stats (cold_ms, warm_ms, iterations)
        """
        full_name = self.get_full_table_name()
        probe_sql = f"SELECT 1 FROM {full_name} LIMIT 1"
        warm_stats: dict[str, Any] = {"iterations": 0}

        start = time.time()
        prev_ms = None

        while (time.time() - start) < timeout_s:
            warm_stats["iterations"] += 1
            try:
                q_start = time.time()
                await self.pool.execute_query(
                    f"USE WAREHOUSE {warehouse_name}"
                )
                await self.pool.execute_query(probe_sql)
                q_ms = (time.time() - q_start) * 1000

                if warm_stats["iterations"] == 1:
                    warm_stats["cold_ms"] = round(q_ms, 1)

                # Consider warm when latency drops below 200ms or stabilizes
                if q_ms < 200 or (prev_ms and abs(q_ms - prev_ms) < 50):
                    warm_stats["warm_ms"] = round(q_ms, 1)
                    warm_stats["warmed"] = True
                    logger.info(
                        "Cache warm for %s on %s: cold=%sms warm=%sms (%d iterations)",
                        full_name,
                        warehouse_name,
                        warm_stats.get("cold_ms"),
                        warm_stats["warm_ms"],
                        warm_stats["iterations"],
                    )
                    return warm_stats

                prev_ms = q_ms
            except Exception as e:
                logger.warning("Warm probe failed: %s", e)
                break

        warm_stats["warmed"] = False
        warm_stats["timeout"] = True
        logger.warning("Cache warming timed out for %s after %ds", full_name, timeout_s)
        return warm_stats
