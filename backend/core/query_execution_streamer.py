"""
Query Execution Streamer

Streams query execution records to Snowflake with periodic background flushes,
replacing the previous approach of buffering all records until shutdown.

Key features:
- Unbounded buffer (no record loss from FIFO eviction)
- Background flush task (30s interval OR 5000 records threshold)
- Batched INSERTs (2000 rows per statement)
- Failure handling (re-queue once, then drop batch)
- Graceful shutdown with flush-latency-based timeout
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections import deque
from contextlib import suppress
from dataclasses import asdict
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from backend.connectors.snowflake_pool import SnowflakeConnectionPool

logger = logging.getLogger(__name__)


class QueryExecutionStreamer:
    """
    Streams query execution records to Snowflake via periodic background flushes.

    This class manages:
    - Unbounded in-memory buffer for query execution records
    - Background flush task that persists records to QUERY_EXECUTIONS table
    - Retry logic with single re-queue on failure, drop on second failure
    - Graceful shutdown that waits for final flush to complete

    Usage:
        streamer = QueryExecutionStreamer(pool=control_pool, test_id="abc-123")
        await streamer.start()
        # ... during test execution ...
        await streamer.append(record)
        # ... at shutdown ...
        await streamer.shutdown(flush_p99_ms=150.0)
    """

    def __init__(
        self,
        pool: SnowflakeConnectionPool,
        test_id: str,
        *,
        flush_interval_seconds: float = 30.0,
        flush_threshold_records: int = 5000,
        batch_insert_size: int = 2000,
        results_prefix: str | None = None,
    ) -> None:
        """
        Initialize the query execution streamer.

        Args:
            pool: Snowflake connection pool (control pool) for persistence.
            test_id: Test identifier for all records.
            flush_interval_seconds: Time between flushes (default 30s).
            flush_threshold_records: Flush when buffer reaches this size (default 5000).
            batch_insert_size: Rows per INSERT statement (default 2000).
            results_prefix: Schema prefix for QUERY_EXECUTIONS table.
        """
        self._pool = pool
        self._test_id = test_id
        self._flush_interval = flush_interval_seconds
        self._flush_threshold = flush_threshold_records
        self._batch_size = batch_insert_size
        self._results_prefix = results_prefix

        # Unbounded buffer (no maxlen - prevents record loss)
        self._buffer: deque[dict[str, Any]] = deque()
        self._buffer_lock = asyncio.Lock()

        # Background task state
        self._flush_task: asyncio.Task[None] | None = None
        self._stop_event = asyncio.Event()

        # Metrics for monitoring and shutdown timeout calculation
        self._flush_count = 0
        self._total_records_flushed = 0
        self._dropped_batches = 0
        self._flush_latencies_ms: deque[float] = deque(maxlen=100)

        # Track partial run state (set if we had to drop batches)
        self._run_partial = False

    @property
    def run_partial(self) -> bool:
        """True if any batches were dropped during this run."""
        return self._run_partial

    @property
    def stats(self) -> dict[str, Any]:
        """Return current streamer statistics."""
        return {
            "buffer_size": len(self._buffer),
            "flush_count": self._flush_count,
            "total_records_flushed": self._total_records_flushed,
            "dropped_batches": self._dropped_batches,
            "flush_p50_ms": self._percentile(50),
            "flush_p99_ms": self._percentile(99),
            "run_partial": self._run_partial,
        }

    def _percentile(self, p: int) -> float | None:
        """Calculate percentile from flush latencies."""
        if not self._flush_latencies_ms:
            return None
        sorted_latencies = sorted(self._flush_latencies_ms)
        idx = int(len(sorted_latencies) * p / 100)
        idx = min(idx, len(sorted_latencies) - 1)
        return sorted_latencies[idx]

    async def append(self, record: Any) -> None:
        """
        Add a record to the buffer (100% capture, no sampling).

        This method is designed to be fast to avoid blocking the query execution
        hot path. Dataclass conversion is deferred to flush time.

        Args:
            record: QueryExecutionRecord dataclass or dict.
        """
        # Store record as-is; convert to dict lazily during flush
        # This avoids expensive asdict() calls on every append
        try:
            async with self._buffer_lock:
                self._buffer.append(record)
        except Exception:
            # Silently ignore - streaming is best-effort, don't crash the caller
            pass

    async def start(self) -> None:
        """Start the background flush task."""
        if self._flush_task is not None:
            logger.warning("QueryExecutionStreamer already started")
            return

        self._stop_event.clear()
        self._flush_task = asyncio.create_task(
            self._flush_loop(), name=f"streamer-flush-{self._test_id[:8]}"
        )
        logger.info(
            f"QueryExecutionStreamer started: test_id={self._test_id}, "
            f"interval={self._flush_interval}s, threshold={self._flush_threshold}"
        )

    async def shutdown(self, flush_p99_ms: float | None = None) -> None:
        """
        Flush remaining buffer and stop the background task.

        Args:
            flush_p99_ms: External p99 latency hint for timeout calculation.
                         If None, uses internal measurements.
        """
        if self._flush_task is None:
            logger.warning("QueryExecutionStreamer not started, nothing to shutdown")
            return

        # Signal stop
        self._stop_event.set()

        # Calculate shutdown timeout: 2x flush p99 (min 30s)
        measured_p99 = self._percentile(99)
        effective_p99 = flush_p99_ms or measured_p99 or 5000.0
        timeout_seconds = max(30.0, (effective_p99 * 2) / 1000.0)

        logger.info(
            f"QueryExecutionStreamer shutting down: "
            f"buffer_size={len(self._buffer)}, timeout={timeout_seconds:.1f}s"
        )

        try:
            # Wait for flush task to complete
            await asyncio.wait_for(self._flush_task, timeout=timeout_seconds)
        except asyncio.TimeoutError:
            logger.error(
                f"QueryExecutionStreamer shutdown timed out after {timeout_seconds:.1f}s, "
                f"cancelling flush task. {len(self._buffer)} records may be lost."
            )
            self._flush_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._flush_task
            self._run_partial = True
        except asyncio.CancelledError:
            logger.warning("QueryExecutionStreamer shutdown cancelled")
            self._flush_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._flush_task
            raise

        self._flush_task = None
        logger.info(
            f"QueryExecutionStreamer shutdown complete: "
            f"flushed={self._total_records_flushed}, dropped_batches={self._dropped_batches}"
        )

    async def _flush_loop(self) -> None:
        """Background loop that periodically flushes the buffer."""
        while not self._stop_event.is_set():
            try:
                # Wait for interval or stop signal
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=self._flush_interval
                )
                # If we get here, stop was signaled
                break
            except asyncio.TimeoutError:
                # Normal timeout - time to flush
                pass

            # Check if buffer needs flushing
            if len(self._buffer) > 0:
                await self._flush_buffer()

        # Final flush on shutdown
        if len(self._buffer) > 0:
            logger.info(
                f"QueryExecutionStreamer final flush: {len(self._buffer)} records"
            )
            await self._flush_buffer()

    async def _flush_buffer(self) -> None:
        """Flush current buffer contents to Snowflake."""
        # Atomically grab all buffered records
        async with self._buffer_lock:
            if not self._buffer:
                return
            raw_records = list(self._buffer)
            self._buffer.clear()

        # Convert dataclasses to dicts (deferred from append for performance)
        records: list[dict[str, Any]] = []
        for r in raw_records:
            if hasattr(r, "__dataclass_fields__"):
                records.append(asdict(r))
            else:
                records.append(r)

        start_time = time.monotonic()
        success = await self._persist_records(records, retry=True)
        elapsed_ms = (time.monotonic() - start_time) * 1000

        self._flush_latencies_ms.append(elapsed_ms)
        self._flush_count += 1

        if success:
            self._total_records_flushed += len(records)
            logger.debug(
                f"QueryExecutionStreamer flush: {len(records)} records in {elapsed_ms:.0f}ms"
            )
        else:
            self._dropped_batches += 1
            self._run_partial = True
            logger.warning(
                f"QueryExecutionStreamer dropped batch: {len(records)} records after retry"
            )

    async def _persist_records(
        self, records: list[dict[str, Any]], *, retry: bool = True
    ) -> bool:
        """
        Persist records to QUERY_EXECUTIONS table in batches.

        Args:
            records: List of record dicts to persist.
            retry: If True, re-queue once on failure.

        Returns:
            True if all records persisted successfully, False otherwise.
        """
        if not records:
            return True

        # Build INSERT statement
        cols = [
            "EXECUTION_ID",
            "TEST_ID",
            "QUERY_ID",
            "QUERY_TEXT",
            "START_TIME",
            "END_TIME",
            "DURATION_MS",
            "ROWS_AFFECTED",
            "BYTES_SCANNED",
            "WAREHOUSE",
            "SUCCESS",
            "ERROR",
            "CONNECTION_ID",
            "CUSTOM_METADATA",
            "QUERY_KIND",
            "WORKER_ID",
            "WARMUP",
            "APP_ELAPSED_MS",
            "SF_ROWS_INSERTED",
            "SF_ROWS_UPDATED",
            "SF_ROWS_DELETED",
        ]

        # Build SELECT expressions (handle VARIANT type for CUSTOM_METADATA)
        custom_idx = cols.index("CUSTOM_METADATA") + 1
        select_exprs = []
        for i, col in enumerate(cols, start=1):
            if i == custom_idx:
                select_exprs.append(f"TRY_PARSE_JSON(COLUMN{i}) AS {col}")
            else:
                select_exprs.append(f"COLUMN{i} AS {col}")

        # Determine table prefix
        prefix = self._results_prefix or self._get_results_prefix()

        insert_prefix = f"""
        INSERT INTO {prefix}.QUERY_EXECUTIONS (
            {", ".join(cols)}
        )
        SELECT
            {", ".join(select_exprs)}
        FROM VALUES
        """

        # Process in batches
        try:
            i = 0
            while i < len(records):
                batch = records[i : i + self._batch_size]
                await self._insert_batch(insert_prefix, cols, batch)
                i += self._batch_size
            return True

        except Exception as e:
            logger.warning(f"QueryExecutionStreamer persist failed: {e}")
            if retry:
                # Re-queue once, then try again without retry
                logger.info(
                    f"QueryExecutionStreamer retrying {len(records)} records..."
                )
                await asyncio.sleep(1.0)  # Brief backoff
                return await self._persist_records(records, retry=False)
            return False

    async def _insert_batch(
        self, insert_prefix: str, cols: list[str], batch: list[dict[str, Any]]
    ) -> None:
        """Insert a single batch of records."""
        row_tpl = "(" + ", ".join(["?"] * len(cols)) + ")"
        values_sql = ",\n".join([row_tpl] * len(batch))
        query = insert_prefix + values_sql

        params: list[Any] = []
        for r in batch:
            params.extend(self._row_params(r))

        await self._pool.execute_query(query, params=params)

    def _row_params(self, r: dict[str, Any]) -> list[Any]:
        """Convert a record dict to INSERT parameter list."""
        query_kind = (r.get("query_kind") or "").strip().upper()
        rows_affected = r.get("rows_affected")
        sf_rows_inserted = rows_affected if query_kind == "INSERT" else None
        sf_rows_updated = rows_affected if query_kind == "UPDATE" else None
        sf_rows_deleted = rows_affected if query_kind == "DELETE" else None

        return [
            r.get("execution_id"),
            self._test_id,
            r.get("query_id"),
            r.get("query_text"),
            r.get("start_time"),
            r.get("end_time"),
            r.get("duration_ms"),
            rows_affected,
            r.get("bytes_scanned"),
            r.get("warehouse"),
            bool(r.get("success")),
            r.get("error"),
            r.get("connection_id"),
            json.dumps(r.get("custom_metadata") or {}),
            r.get("query_kind"),
            r.get("worker_id"),
            bool(r.get("warmup")),
            r.get("app_elapsed_ms"),
            sf_rows_inserted,
            sf_rows_updated,
            sf_rows_deleted,
        ]

    def _get_results_prefix(self) -> str:
        """Get the results table prefix from settings."""
        # Import here to avoid circular imports
        from backend.config import settings

        db = settings.RESULTS_DATABASE
        schema = settings.RESULTS_SCHEMA
        return f"{db}.{schema}"
