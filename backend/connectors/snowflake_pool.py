"""
Snowflake Connection Pool Manager

Manages connection pooling for Snowflake with health checks, retry logic,
and support for multiple warehouse connections.
"""

import logging
from typing import Dict, Optional, Any, List, cast
from contextlib import asynccontextmanager, suppress
import asyncio
from datetime import datetime
from concurrent.futures import Executor, ThreadPoolExecutor

import snowflake.connector
from snowflake.connector import SnowflakeConnection
from snowflake.connector.errors import (
    OperationalError,
)

from backend.config import settings


class PoolInitializationError(Exception):
    """Raised when the connection pool fails to initialize enough connections."""

    def __init__(self, message: str, errors: List[str]):
        super().__init__(message)
        self.errors = errors


logger = logging.getLogger(__name__)


class SnowflakeConnectionPool:
    """
    Connection pool for Snowflake with health monitoring and retry logic.
    """

    def __init__(
        self,
        account: str,
        user: str,
        password: Optional[str] = None,
        warehouse: Optional[str] = None,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        role: Optional[str] = None,
        pool_size: int = 5,
        max_overflow: int = 10,
        timeout: int = 30,
        recycle: int = 3600,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        *,
        executor: Executor | None = None,
        owns_executor: bool = False,
        max_parallel_creates: int = 8,
        connect_login_timeout: int | None = None,
        connect_network_timeout: int | None = None,
        connect_socket_timeout: int | None = None,
        session_parameters: Optional[Dict[str, Any]] = None,
        pool_name: str = "default",
    ):
        """
        Initialize Snowflake connection pool.

        Args:
            account: Snowflake account identifier
            user: Username
            password: Password (or use private key)
            warehouse: Default warehouse
            database: Default database
            schema: Default schema
            role: Default role
            pool_size: Base pool size
            max_overflow: Max additional connections
            timeout: Connection timeout in seconds
            recycle: Recycle connections after N seconds
            max_retries: Max retry attempts for transient failures
            retry_delay: Delay between retries in seconds
        """
        self.account = account
        self.user = user
        self.password = password
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role

        self.pool_size = pool_size
        self.max_overflow = max_overflow
        self.timeout = timeout
        self.recycle = recycle
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        # Connection pool (list of available connections)
        self._pool: List[SnowflakeConnection] = []
        self._in_use: Dict[int, SnowflakeConnection] = {}
        self._connection_times: Dict[int, datetime] = {}
        # Number of connection creations currently in flight. We must count these
        # towards max connections to avoid a connection "stampede" under high
        # concurrency where many tasks simultaneously decide to create a new
        # connection before any have been checked out.
        self._pending_creates: int = 0
        # Tracks the last time we ran a lightweight health check against a connection.
        # This avoids spamming `SELECT 1` on every checkout/return, which can dominate
        # benchmarks and pollute Snowsight query history.
        self._last_health_check: Dict[int, datetime] = {}
        self._lock = asyncio.Lock()
        self._initialized = False
        self._executor: Executor | None = executor
        self._owns_executor: bool = bool(owns_executor)
        # Limit how many blocking connect() calls we issue concurrently.
        self._max_parallel_creates: int = max(1, int(max_parallel_creates))
        self._create_semaphore = asyncio.Semaphore(self._max_parallel_creates)
        self._connect_login_timeout = (
            int(connect_login_timeout)
            if connect_login_timeout is not None
            else settings.SNOWFLAKE_CONNECT_LOGIN_TIMEOUT
        )
        self._connect_network_timeout = (
            int(connect_network_timeout)
            if connect_network_timeout is not None
            else settings.SNOWFLAKE_CONNECT_NETWORK_TIMEOUT
        )
        self._connect_socket_timeout = (
            int(connect_socket_timeout)
            if connect_socket_timeout is not None
            else settings.SNOWFLAKE_CONNECT_SOCKET_TIMEOUT
        )
        self._session_parameters: Dict[str, Any] = dict(session_parameters or {})
        self._pool_name: str = pool_name
        # Minimum interval between health checks per connection.
        # (We still recycle connections via `self.recycle`.)
        self._health_check_interval_seconds: float = 30.0

        logger.info(
            f"[{pool_name}] Initialized Snowflake pool: {user}@{account}, "
            f"pool_size={pool_size}, max_overflow={max_overflow}"
        )

    def _run_in_executor(self, func, *args):
        loop = asyncio.get_running_loop()
        return loop.run_in_executor(self._executor, func, *args)

    def max_connections(self) -> int:
        return int(self.pool_size) + int(self.max_overflow)

    async def initialize(self):
        """Initialize the connection pool by creating base connections."""
        if self._initialized:
            return

        async with self._lock:
            if self._initialized:
                return

            total = int(self.pool_size)
            logger.info(
                "[%s] Creating %d initial Snowflake connections (max_parallel_creates=%d)...",
                self._pool_name,
                total,
                int(self._max_parallel_creates),
            )
            t0 = asyncio.get_running_loop().time()
            next_pct_log = 10

            # Create initial pool connections with bounded parallelism.
            # Fail-fast: if error rate exceeds threshold, abort early rather than
            # waiting for all connections to fail (which can take minutes).
            FAIL_FAST_ERROR_THRESHOLD = 0.5  # Abort if >=50% of attempts fail
            FAIL_FAST_MIN_ATTEMPTS = min(16, total)  # Scale down for small pools

            connections: list[SnowflakeConnection | BaseException] = []
            remaining = total
            created_ok = 0
            total_errors = 0
            batches_completed = 0
            while remaining > 0:
                batch_n = min(remaining, self._max_parallel_creates)
                tasks = [self._create_connection() for _ in range(batch_n)]
                batch = await asyncio.gather(*tasks, return_exceptions=True)
                connections.extend(batch)
                remaining -= batch_n
                batch_ok = sum(1 for c in batch if not isinstance(c, BaseException))
                batch_errors = batch_n - batch_ok
                created_ok += batch_ok
                total_errors += batch_errors
                attempted = total - remaining
                batches_completed += 1

                # Fail-fast check: abort early on high error rates
                # - If entire batch failed (100% errors in batch), abort immediately after 1st batch
                # - Otherwise, wait for min attempts and check overall error rate
                should_fail_fast = False
                if batch_errors == batch_n and batch_n > 0:
                    # Entire batch failed - likely a connectivity issue
                    should_fail_fast = True
                elif attempted >= FAIL_FAST_MIN_ATTEMPTS and total_errors > 0:
                    error_rate = total_errors / attempted
                    if error_rate >= FAIL_FAST_ERROR_THRESHOLD:
                        should_fail_fast = True

                if should_fail_fast and remaining > 0:
                    elapsed = asyncio.get_running_loop().time() - t0
                    error_rate = total_errors / attempted if attempted > 0 else 1.0
                    # Collect error messages from failed connections
                    error_msgs = [
                        str(c) for c in connections if isinstance(c, BaseException)
                    ]
                    logger.error(
                        "🔴 Fail-fast triggered: %d/%d connections failed (%.0f%% error rate) after %.1fs. Aborting pool initialization.",
                        total_errors,
                        attempted,
                        error_rate * 100,
                        elapsed,
                    )
                    raise PoolInitializationError(
                        f"Connection error rate too high ({total_errors}/{attempted} failed, {error_rate * 100:.0f}%). Aborting.",
                        errors=error_msgs[:5],  # First 5 errors
                    )

                if total > 0:
                    pct = int((attempted * 100) / total)
                else:
                    pct = 100
                if remaining > 0 and pct >= next_pct_log:
                    errors = attempted - created_ok
                    elapsed = asyncio.get_running_loop().time() - t0
                    logger.debug(
                        "[%s] Pool init progress: %d/%d attempted, %d created, %d errors (%.1fs elapsed)",
                        self._pool_name,
                        attempted,
                        total,
                        created_ok,
                        errors,
                        elapsed,
                    )
                    while next_pct_log <= pct:
                        next_pct_log += 10

            connection_errors: List[str] = []
            for conn in connections:
                if isinstance(conn, BaseException):
                    error_msg = str(conn)
                    logger.error(f"Failed to create initial connection: {conn}")
                    connection_errors.append(error_msg)
                else:
                    self._pool.append(conn)
                    self._connection_times[id(conn)] = datetime.now()
                    self._last_health_check[id(conn)] = datetime.now()

            self._initialized = True
            logger.info(
                f"[{self._pool_name}] Connection pool initialized with {len(self._pool)} connections"
            )

            # If we have connection errors, raise with details so callers can handle
            if connection_errors:
                error_msg = f"Failed to create {len(connection_errors)}/{total} connections during pool initialization"
                logger.error(
                    "🔴 %s. First error: %s",
                    error_msg,
                    connection_errors[0][:200] if connection_errors else "unknown",
                )
                raise PoolInitializationError(error_msg, errors=connection_errors)

    def _get_connection_params(self) -> Dict[str, Any]:
        """Get connection parameters for snowflake.connector."""
        session_params: Dict[str, Any] = {"QUERY_TAG": "flakebench"}
        session_params.update(self._session_parameters)
        params = {
            "account": self.account,
            "user": self.user,
            # We use `?` placeholders throughout the codebase. Snowflake's Python connector
            # defaults to `pyformat`, which can raise `TypeError: not all arguments converted
            # during string formatting` when passing params with `?` placeholders.
            # Setting qmark ensures consistent server-side binding for `?`.
            "paramstyle": "qmark",
            # Fail fast on degraded connectivity so reload/shutdown doesn't hang waiting for
            # threadpool connect() calls to finish.
            "login_timeout": self._connect_login_timeout,
            "network_timeout": self._connect_network_timeout,
            "socket_timeout": self._connect_socket_timeout,
            "session_parameters": {
                **session_params,
            },
        }

        if self.password:
            params["password"] = self.password
        # TODO: Add private key authentication support

        if self.warehouse:
            params["warehouse"] = self.warehouse
        if self.database:
            params["database"] = self.database
        if self.schema:
            params["schema"] = self.schema
        if self.role:
            params["role"] = self.role

        return params

    async def _create_connection(self) -> SnowflakeConnection:
        """
        Create a new Snowflake connection with retry logic.

        Returns:
            SnowflakeConnection: New connection

        Raises:
            SnowflakeError: If connection fails after retries
        """
        params = self._get_connection_params()

        async with self._create_semaphore:
            for attempt in range(self.max_retries):
                try:
                    conn = cast(
                        SnowflakeConnection,
                        await self._run_in_executor(
                            lambda: snowflake.connector.connect(**params)
                        ),
                    )

                    logger.debug(f"Created new Snowflake connection: {id(conn)}")
                    return conn

                except OperationalError as e:
                    if attempt < self.max_retries - 1:
                        logger.warning(
                            f"Connection attempt {attempt + 1} failed, retrying: {e}"
                        )
                        await asyncio.sleep(self.retry_delay * (attempt + 1))
                    else:
                        logger.error(
                            f"Failed to create connection after {self.max_retries} attempts"
                        )
                        raise
                except Exception as e:
                    logger.error(f"Unexpected error creating connection: {e}")
                    raise

        raise RuntimeError("Failed to create Snowflake connection")

    async def _is_connection_valid(self, conn: SnowflakeConnection) -> bool:
        """
        Check if a connection is still valid.

        Args:
            conn: Connection to check

        Returns:
            bool: True if connection is valid
        """
        try:
            # Check if connection is closed
            if conn.is_closed():
                return False

            # Check connection age for recycling
            conn_id = id(conn)
            if conn_id in self._connection_times:
                age = (datetime.now() - self._connection_times[conn_id]).total_seconds()
                if age > self.recycle:
                    logger.debug(f"Connection {conn_id} expired (age: {age}s)")
                    return False

            # Run a lightweight query occasionally to verify the connection is still usable.
            # This must not run on every checkout/return; doing so can serialize high
            # concurrency workloads and overwhelm query history with `SELECT 1`.
            last = self._last_health_check.get(conn_id)
            if (
                last is None
                or (datetime.now() - last).total_seconds()
                >= self._health_check_interval_seconds
            ):
                cursor = await self._run_in_executor(conn.cursor)
                try:
                    await self._run_in_executor(cursor.execute, "SELECT 1")
                finally:
                    try:
                        await self._run_in_executor(cursor.close)
                    except RuntimeError:
                        # Executor already shut down - cursor will be cleaned up with connection
                        pass
                self._last_health_check[conn_id] = datetime.now()

            return True

        except Exception as e:
            logger.debug(f"Connection validation failed: {e}")
            return False

    @asynccontextmanager
    async def get_connection(self):
        """
        Get a connection from the pool (async context manager).

        Usage:
            async with pool.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")

        Yields:
            SnowflakeConnection: Connection from pool
        """
        if not self._initialized:
            await self.initialize()

        conn = None

        try:
            # Acquire a connection without holding the pool lock across network I/O.
            #
            # Holding the lock while awaiting `_is_connection_valid()` (which can hit
            # Snowflake) serializes checkouts under concurrency and can make a "10
            # concurrent workers" run look like far fewer in Snowsight.
            while conn is None:
                candidate: Optional[SnowflakeConnection] = None
                need_create = False
                reserved_create = False

                async with self._lock:
                    if self._pool:
                        candidate = self._pool.pop()
                    else:
                        total_connections = (
                            len(self._pool) + len(self._in_use) + self._pending_creates
                        )
                        if total_connections < self.max_connections():
                            need_create = True
                            self._pending_creates += 1
                            reserved_create = True
                        else:
                            raise Exception(
                                f"Connection pool exhausted "
                                f"(max: {self.max_connections()})"
                            )

                if need_create:
                    try:
                        candidate = await self._create_connection()
                        now = datetime.now()
                        self._connection_times[id(candidate)] = now
                        self._last_health_check[id(candidate)] = now
                    except Exception:
                        async with self._lock:
                            self._pending_creates = max(0, self._pending_creates - 1)
                        raise

                if candidate is None:
                    continue

                if await self._is_connection_valid(candidate):
                    conn = candidate
                    break

                # Close invalid connection (best effort) and try again.
                try:
                    candidate.close()
                except Exception:
                    pass
                cid = id(candidate)
                self._connection_times.pop(cid, None)
                self._last_health_check.pop(cid, None)
                if reserved_create:
                    async with self._lock:
                        self._pending_creates = max(0, self._pending_creates - 1)

            async with self._lock:
                if reserved_create:
                    self._pending_creates = max(0, self._pending_creates - 1)
                self._in_use[id(conn)] = conn

            yield conn

        finally:
            # Return connection to pool
            if conn is not None:
                async with self._lock:
                    conn_id = id(conn)
                    if conn_id in self._in_use:
                        del self._in_use[conn_id]

                    # Always return to the pool; the next checkout will validate.
                    # (Do NOT validate here; that can double health-check traffic.)
                    self._pool.append(conn)

    async def execute_query(
        self,
        query: str,
        params: Optional[object] = None,
        warehouse: Optional[str] = None,
    ) -> List[tuple]:
        """
        Execute a query and return results.

        Args:
            query: SQL query to execute
            params: Query parameters (for binding)
            warehouse: Optional warehouse override

        Returns:
            List of result tuples
        """
        async with self.get_connection() as conn:
            # Switch warehouse if specified
            if warehouse and warehouse != self.warehouse:
                cursor = await self._run_in_executor(conn.cursor)
                await self._run_in_executor(
                    cursor.execute, f"USE WAREHOUSE {warehouse}"
                )
                await self._run_in_executor(cursor.close)

            # Execute query
            cursor = await self._run_in_executor(conn.cursor)

            try:
                if params is None:
                    await self._run_in_executor(cursor.execute, query)
                else:
                    # Snowflake connector supports both:
                    # - qmark params: Sequence[Any] for `?` placeholders
                    # - pyformat params: Mapping for `%(name)s` placeholders
                    await self._run_in_executor(cursor.execute, query, params)

                results = await self._run_in_executor(cursor.fetchall)
                return results

            finally:
                try:
                    await self._run_in_executor(cursor.close)
                except RuntimeError:
                    # Executor already shut down - cursor will be cleaned up with connection
                    pass

    async def execute_query_with_info(
        self,
        query: str,
        params: Optional[object] = None,
        warehouse: Optional[str] = None,
        *,
        fetch: bool = True,
    ) -> tuple[List[tuple], dict[str, Any]]:
        """
        Execute a query and return results + execution info.

        This is primarily used for per-query logging/enrichment workflows.

        Returns:
            (results, info) where info includes:
              - query_id: Snowflake QUERY_ID (cursor.sfqid) when available
              - rowcount: cursor.rowcount (may be -1 depending on statement)
              - total_elapsed_time_ms: Snowflake execution time in milliseconds (if available)
        """
        async with self.get_connection() as conn:
            if warehouse and warehouse != self.warehouse:
                cursor = await self._run_in_executor(conn.cursor)
                await self._run_in_executor(
                    cursor.execute, f"USE WAREHOUSE {warehouse}"
                )
                await self._run_in_executor(cursor.close)

            cursor = await self._run_in_executor(conn.cursor)
            try:
                if params is None:
                    await self._run_in_executor(cursor.execute, query)
                else:
                    await self._run_in_executor(cursor.execute, query, params)

                query_id = getattr(cursor, "sfqid", None)
                rowcount = getattr(cursor, "rowcount", None)
                total_elapsed_time_ms: Optional[float] = None
                query_result_format = getattr(cursor, "_query_result_format", None)
                if query_result_format and hasattr(query_result_format, "get"):
                    elapsed_raw = query_result_format.get("total_elapsed_time")
                    if elapsed_raw is not None:
                        try:
                            total_elapsed_time_ms = float(elapsed_raw)
                        except (ValueError, TypeError):
                            pass
                results: List[tuple] = []
                if fetch:
                    results = await self._run_in_executor(cursor.fetchall)

                return results, {
                    "query_id": query_id,
                    "rowcount": rowcount,
                    "total_elapsed_time_ms": total_elapsed_time_ms,
                }
            finally:
                try:
                    await self._run_in_executor(cursor.close)
                except RuntimeError:
                    # Executor already shut down - cursor will be cleaned up with connection
                    pass

    async def update_query_tag(self, new_tag: str) -> int:
        """
        Update the QUERY_TAG session parameter on all existing connections.

        This is used to transition from warmup to measurement phase so that
        Snowflake's QUERY_HISTORY can distinguish warmup vs measurement queries.

        Args:
            new_tag: New QUERY_TAG value (e.g., "flakebench:test_id=XXX:phase=RUNNING")

        Returns:
            Number of connections successfully updated
        """
        # Update the stored session parameter so new connections also get the new tag.
        self._session_parameters["QUERY_TAG"] = str(new_tag)

        # Gather all connections (pooled + in-use) under lock, then update outside lock.
        async with self._lock:
            all_conns: list[SnowflakeConnection] = list(self._pool) + list(
                self._in_use.values()
            )

        if not all_conns:
            return 0

        # Run ALTER SESSION on each connection (best-effort, parallel).
        alter_sql = f"ALTER SESSION SET QUERY_TAG = '{new_tag}'"
        updated = 0
        sem = asyncio.Semaphore(min(16, len(all_conns)))

        async def _update_conn(conn: SnowflakeConnection) -> bool:
            async with sem:
                try:

                    def _do_alter():
                        cur = conn.cursor()
                        try:
                            cur.execute(alter_sql)
                        finally:
                            cur.close()

                    await self._run_in_executor(_do_alter)
                    return True
                except Exception:
                    # Best-effort: if ALTER fails, the connection may still work.
                    return False

        results = await asyncio.gather(
            *[_update_conn(c) for c in all_conns], return_exceptions=True
        )
        updated = sum(1 for r in results if r is True)
        logger.debug(
            "Updated QUERY_TAG on %d/%d connections to: %s",
            updated,
            len(all_conns),
            new_tag[:100],
        )
        return updated

    async def get_pool_stats(self) -> Dict[str, Any]:
        """
        Get connection pool statistics.

        Returns:
            Dict with pool statistics
        """
        async with self._lock:
            return {
                "pool_size": self.pool_size,
                "max_overflow": self.max_overflow,
                "available": len(self._pool),
                "in_use": len(self._in_use),
                "pending_creates": self._pending_creates,
                "total": len(self._pool) + len(self._in_use),
                "initialized": self._initialized,
            }

    async def prewarm(self, target_size: int) -> int:
        """
        Pre-create connections up to target_size in parallel.

        This is useful when scaling up workers dynamically (e.g., FIND_MAX mode)
        to ensure connections are ready before workers start requesting them.
        Without prewarming, workers would wait for on-demand connection creation.

        Args:
            target_size: Target total pool size (available + in_use)

        Returns:
            Number of new connections created
        """
        async with self._lock:
            current_total = len(self._pool) + len(self._in_use)
            max_allowed = int(self.pool_size) + int(self.max_overflow)
            target_size = min(target_size, max_allowed)
            to_create = max(0, target_size - current_total)

            logger.info(
                "[%s] prewarm called: target=%d, current_total=%d (pool=%d, in_use=%d), max_allowed=%d, to_create=%d",
                self._pool_name,
                target_size,
                current_total,
                len(self._pool),
                len(self._in_use),
                max_allowed,
                to_create,
            )

            if to_create == 0:
                logger.info("[%s] prewarm: nothing to create, already at target", self._pool_name)
                return 0

            logger.info(
                "[%s] Prewarming pool: creating %d connections (current=%d, target=%d)",
                self._pool_name,
                to_create,
                current_total,
                target_size,
            )

        t0 = asyncio.get_running_loop().time()
        connections: list[SnowflakeConnection | BaseException] = []
        remaining = to_create

        while remaining > 0:
            batch_n = min(remaining, self._max_parallel_creates)
            tasks = [self._create_connection() for _ in range(batch_n)]
            batch = await asyncio.gather(*tasks, return_exceptions=True)
            connections.extend(batch)
            remaining -= batch_n

        created_ok = 0
        async with self._lock:
            for conn in connections:
                if isinstance(conn, BaseException):
                    logger.warning("Prewarm connection creation failed: %s", conn)
                else:
                    self._pool.append(conn)
                    self._connection_times[id(conn)] = datetime.now()
                    self._last_health_check[id(conn)] = datetime.now()
                    created_ok += 1

        elapsed = asyncio.get_running_loop().time() - t0
        logger.info(
            "[%s] Prewarm complete: created %d/%d connections in %.2fs",
            self._pool_name,
            created_ok,
            to_create,
            elapsed,
        )
        return created_ok

    async def close_all(self):
        """
        Close all connections in the pool (best effort).

        Closing Snowflake connections can occasionally block on network calls (session delete,
        telemetry flush, etc). To avoid stalling the app during test PROCESSING/teardown,
        we:
        - close connections outside the pool lock
        - disable connector retries for close()
        - bound close() by temporarily lowering the connection's network/socket timeouts
        - close in parallel with bounded concurrency
        - suppress connector network ERROR spam during close and emit a single summary log
        """
        # Copy connections and clear state under lock, then close outside the lock.
        async with self._lock:
            logger.info("Closing all Snowflake connections...")
            conns: list[SnowflakeConnection] = list(self._pool) + list(
                self._in_use.values()
            )
            self._pool.clear()
            self._in_use.clear()
            self._connection_times.clear()
            self._last_health_check.clear()
            self._initialized = False

        if not conns:
            logger.info("All connections closed (0 connections)")
        else:
            # Bound shutdown behavior (seconds). We intentionally keep this low so teardown
            # cannot hang for minutes on degraded connectivity.
            close_timeout_s = 10
            close_parallel = max(1, min(32, int(self._max_parallel_creates) * 4))
            close_parallel = min(close_parallel, len(conns))
            sem = asyncio.Semaphore(int(close_parallel))

            # Suppress connector-internal ERROR logs during close; we will log one summary.
            net_logger = logging.getLogger("snowflake.connector.network")
            prev_level = net_logger.level
            net_logger.setLevel(logging.CRITICAL)

            closed_count = 0
            try:

                def _close_conn(c: SnowflakeConnection) -> None:
                    # Best-effort: shorten timeouts so close() cannot block for long.
                    with suppress(Exception):
                        setattr(
                            c,
                            "_network_timeout",
                            min(
                                int(
                                    getattr(c, "_network_timeout", close_timeout_s)
                                    or close_timeout_s
                                ),
                                int(close_timeout_s),
                            ),
                        )
                    with suppress(Exception):
                        setattr(
                            c,
                            "_socket_timeout",
                            min(
                                int(
                                    getattr(c, "_socket_timeout", close_timeout_s)
                                    or close_timeout_s
                                ),
                                int(close_timeout_s),
                            ),
                        )
                    with suppress(Exception):
                        c.close(retry=False)

                async def _close_one(c: SnowflakeConnection) -> None:
                    nonlocal closed_count
                    async with sem:
                        await self._run_in_executor(_close_conn, c)
                    closed_count += 1

                tasks = [asyncio.create_task(_close_one(c)) for c in conns]
                await asyncio.gather(*tasks, return_exceptions=True)
            finally:
                net_logger.setLevel(prev_level)

            logger.info(
                "All connections closed (%d/%d attempted, parallel=%d, close_timeout=%ds)",
                int(closed_count),
                int(len(conns)),
                int(close_parallel),
                int(close_timeout_s),
            )

        # Shut down any owned executor outside the lock (best effort).
        if self._owns_executor and self._executor is not None:
            try:
                self._executor.shutdown(wait=False)
            except Exception:
                pass
            self._executor = None
            self._owns_executor = False


# Global connection pool instance
_default_pool: Optional[SnowflakeConnectionPool] = None
_default_executor: Executor | None = None


def get_default_pool() -> SnowflakeConnectionPool:
    """
    Get or create the default Snowflake connection pool.

    Returns:
        SnowflakeConnectionPool: Default pool instance
    """
    global _default_pool

    if _default_pool is None:
        global _default_executor
        if _default_executor is None:
            _default_executor = ThreadPoolExecutor(
                max_workers=max(
                    1, int(settings.SNOWFLAKE_RESULTS_EXECUTOR_MAX_WORKERS)
                ),
                thread_name_prefix="sf-results",
            )
        _default_pool = SnowflakeConnectionPool(
            account=settings.SNOWFLAKE_ACCOUNT,
            user=settings.SNOWFLAKE_USER,
            password=settings.SNOWFLAKE_PASSWORD,
            warehouse=settings.SNOWFLAKE_WAREHOUSE,
            database=settings.SNOWFLAKE_DATABASE,
            schema=settings.SNOWFLAKE_SCHEMA,
            role=settings.SNOWFLAKE_ROLE,
            pool_size=settings.SNOWFLAKE_POOL_SIZE,
            max_overflow=settings.SNOWFLAKE_MAX_OVERFLOW,
            timeout=settings.SNOWFLAKE_POOL_TIMEOUT,
            recycle=settings.SNOWFLAKE_POOL_RECYCLE,
            executor=_default_executor,
            owns_executor=False,
            max_parallel_creates=settings.SNOWFLAKE_POOL_MAX_PARALLEL_CREATES,
            pool_name="control",
        )

    return _default_pool


async def close_default_pool():
    """Close the default connection pool."""
    global _default_pool
    if _default_pool is not None:
        await _default_pool.close_all()
        _default_pool = None
    global _default_executor
    if _default_executor is not None:
        try:
            _default_executor.shutdown(wait=False)
        except Exception:
            pass
        _default_executor = None


# Telemetry pool (isolated from results persistence)
_telemetry_pool: Optional[SnowflakeConnectionPool] = None
_telemetry_executor: Executor | None = None


def get_telemetry_pool() -> SnowflakeConnectionPool:
    """Get or create a dedicated Snowflake pool for telemetry sampling."""
    global _telemetry_pool, _telemetry_executor

    if _telemetry_pool is None:
        if _telemetry_executor is None:
            _telemetry_executor = ThreadPoolExecutor(
                max_workers=max(
                    1, min(4, int(settings.SNOWFLAKE_RESULTS_EXECUTOR_MAX_WORKERS))
                ),
                thread_name_prefix="sf-telemetry",
            )
        _telemetry_pool = SnowflakeConnectionPool(
            account=settings.SNOWFLAKE_ACCOUNT,
            user=settings.SNOWFLAKE_USER,
            password=settings.SNOWFLAKE_PASSWORD,
            warehouse=settings.SNOWFLAKE_WAREHOUSE,
            database=settings.SNOWFLAKE_DATABASE,
            schema=settings.SNOWFLAKE_SCHEMA,
            role=settings.SNOWFLAKE_ROLE,
            pool_size=1,
            max_overflow=0,
            timeout=settings.SNOWFLAKE_POOL_TIMEOUT,
            recycle=settings.SNOWFLAKE_POOL_RECYCLE,
            executor=_telemetry_executor,
            owns_executor=True,
            max_parallel_creates=1,
            pool_name="telemetry",
        )

    return _telemetry_pool


async def close_telemetry_pool() -> None:
    """Close the dedicated telemetry pool (best effort)."""
    global _telemetry_pool, _telemetry_executor
    if _telemetry_pool is not None:
        await _telemetry_pool.close_all()
        _telemetry_pool = None
    # close_all() shuts down owned executors; clear the global handle too.
    _telemetry_executor = None
