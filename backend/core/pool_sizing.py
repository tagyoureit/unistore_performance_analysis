"""Consolidated pool sizing logic for Snowflake and Postgres connections.

Consolidates duplicate pool sizing code from:
- scripts/run_worker.py:643-668, 697-738
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

ConnectionType = Literal["snowflake", "postgres_direct", "postgres_pgbouncer"]


# Pool sizing constants based on database limits
# Snowflake: No hard limit, but we provide sensible defaults
# PostgreSQL: max_connections=500 (default), leave buffer for admin connections
# PgBouncer: default_pool_size=497 server-side, can accept many more clients

DIRECT_POSTGRES_CAP = 200  # Safe limit for direct PostgreSQL connections
PGBOUNCER_CLIENT_CAP = 1000  # PgBouncer handles multiplexing, can have many clients
PGBOUNCER_INITIAL_CAP = 100  # Conservative to avoid client_login_timeout during init
DIRECT_INITIAL_CAP = 497  # PostgreSQL max_connections is 500, leave small buffer


@dataclass(frozen=True, slots=True)
class PoolSizeConfig:
    """Configuration for connection pool sizing.

    Attributes:
        connection_type: Type of connection (snowflake, postgres_direct, postgres_pgbouncer)
        initial_target: Initial target number of connections
        max_workers: Maximum number of workers/connections
    """

    connection_type: ConnectionType
    initial_target: int
    max_workers: int

    def calculate(self) -> tuple[int, int]:
        """Calculate optimal pool sizing.

        Returns:
            Tuple of (initial_pool_size, overflow_or_max_size):
            - For Snowflake: (initial_pool, overflow) where total = initial + overflow
            - For Postgres: (initial_pool, effective_max_workers)
        """
        if self.connection_type == "snowflake":
            return self._calculate_snowflake()
        elif self.connection_type == "postgres_pgbouncer":
            return self._calculate_postgres_pgbouncer()
        else:  # postgres_direct
            return self._calculate_postgres_direct()

    def _calculate_snowflake(self) -> tuple[int, int]:
        """Calculate Snowflake pool sizing.

        Returns:
            (initial_pool, overflow) where max_total = initial_pool + overflow
        """
        max_workers = max(1, self.max_workers)
        initial_pool = max(1, min(max_workers, max(1, self.initial_target)))
        overflow = max(0, max_workers - initial_pool)
        return (initial_pool, overflow)

    def _calculate_postgres_pgbouncer(self) -> tuple[int, int]:
        """Calculate Postgres pool sizing with PgBouncer.

        PgBouncer multiplexes client connections over server-side pool.
        Start smaller to avoid overwhelming the pooler during init.

        Returns:
            (initial_pool, effective_max_workers)
        """
        # Cap at PgBouncer client limit
        effective_max = min(self.max_workers, PGBOUNCER_CLIENT_CAP)

        # Start smaller, let pool grow on demand
        initial_pool = min(PGBOUNCER_INITIAL_CAP, max(1, self.initial_target))

        return (initial_pool, effective_max)

    def _calculate_postgres_direct(self) -> tuple[int, int]:
        """Calculate Postgres pool sizing for direct connections.

        Direct connections have lower limit to stay within max_connections.

        Returns:
            (initial_pool, effective_max_workers)
        """
        # Cap at direct connection limit
        effective_max = min(self.max_workers, DIRECT_POSTGRES_CAP)

        # Can initialize closer to max since we connect directly
        initial_pool = max(1, min(effective_max, max(1, self.initial_target)))
        initial_pool = min(initial_pool, DIRECT_INITIAL_CAP)

        return (initial_pool, effective_max)

    @classmethod
    def for_snowflake(cls, initial_target: int, max_workers: int) -> "PoolSizeConfig":
        """Create config for Snowflake connection pool."""
        return cls(
            connection_type="snowflake",
            initial_target=initial_target,
            max_workers=max_workers,
        )

    @classmethod
    def for_postgres(
        cls, initial_target: int, max_workers: int, *, use_pgbouncer: bool
    ) -> "PoolSizeConfig":
        """Create config for Postgres connection pool.

        Args:
            initial_target: Initial target number of connections
            max_workers: Maximum number of workers
            use_pgbouncer: Whether connecting through PgBouncer (port 5431) or direct (5432)
        """
        conn_type: ConnectionType = (
            "postgres_pgbouncer" if use_pgbouncer else "postgres_direct"
        )
        return cls(
            connection_type=conn_type,
            initial_target=initial_target,
            max_workers=max_workers,
        )

    def get_effective_max_workers(self) -> int:
        """Get the effective maximum workers after applying caps."""
        if self.connection_type == "snowflake":
            return self.max_workers
        elif self.connection_type == "postgres_pgbouncer":
            return min(self.max_workers, PGBOUNCER_CLIENT_CAP)
        else:  # postgres_direct
            return min(self.max_workers, DIRECT_POSTGRES_CAP)
