#!/usr/bin/env python3
"""
Unit tests for QueryExecutionStreamer.

Tests continuous streaming of query execution records to Snowflake.
"""

import sys
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock
from datetime import datetime, UTC

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from backend.core.query_execution_streamer import QueryExecutionStreamer

pytestmark = pytest.mark.asyncio

# Default test prefix to avoid settings import
TEST_PREFIX = "TEST_DB.TEST_SCHEMA"


def _make_mock_record(query_id: str = "test-query-001") -> dict:
    """Create a mock query execution record as a dict."""
    now = datetime.now(UTC)
    return {
        "execution_id": f"exec-{query_id}",
        "query_id": query_id,
        "test_id": "test-123",
        "worker_id": "worker-1",
        "connection_id": 0,
        "query_kind": "POINT_LOOKUP",
        "start_time": now.isoformat(),
        "end_time": now.isoformat(),
        "duration_ms": 15.5,
        "latency_ms": 15.5,
        "success": True,
        "error": None,
        "rows_affected": 1,
        "bytes_scanned": 100,
        "warehouse": "TEST_WH",
        "query_text": "SELECT * FROM test WHERE id = 1",
        "custom_metadata": {"bind_values": {"id": 1}},
        "warmup": False,
        "app_elapsed_ms": 15.5,
    }


def _make_mock_pool() -> AsyncMock:
    """Create a mock Snowflake connection pool."""
    pool = AsyncMock()
    pool.execute_query = AsyncMock(return_value=[])
    return pool


async def test_streamer_creation():
    """Test QueryExecutionStreamer initialization."""
    pool = _make_mock_pool()

    streamer = QueryExecutionStreamer(
        pool=pool,
        test_id="test-123",
        flush_interval_seconds=30.0,
        flush_threshold_records=5000,
        batch_insert_size=2000,
        results_prefix=TEST_PREFIX,
    )

    assert streamer._test_id == "test-123"
    assert streamer._flush_interval == 30.0
    assert streamer._flush_threshold == 5000
    assert streamer._batch_size == 2000
    assert streamer._flush_task is None  # Not started yet
    assert not streamer.run_partial


async def test_append_records():
    """Test appending records to buffer."""
    pool = _make_mock_pool()
    streamer = QueryExecutionStreamer(
        pool=pool,
        test_id="test-123",
        flush_interval_seconds=60.0,  # Long interval to prevent auto-flush
        flush_threshold_records=1000,
        results_prefix=TEST_PREFIX,
    )

    await streamer.start()

    # Append some records
    for i in range(10):
        record = _make_mock_record(f"query-{i}")
        await streamer.append(record)

    # Check buffer size
    assert len(streamer._buffer) == 10

    await streamer.shutdown()


async def test_flush_on_interval():
    """Test automatic flush on time interval."""
    pool = _make_mock_pool()

    streamer = QueryExecutionStreamer(
        pool=pool,
        test_id="test-123",
        flush_interval_seconds=0.05,  # Very short interval (50ms)
        flush_threshold_records=1000,  # High threshold
        batch_insert_size=100,
        results_prefix=TEST_PREFIX,
    )

    await streamer.start()

    # Append a few records (below threshold)
    for i in range(3):
        record = _make_mock_record(f"query-{i}")
        await streamer.append(record)

    # Wait for interval flush
    await asyncio.sleep(0.15)

    # Verify flush was triggered by interval
    assert pool.execute_query.call_count >= 1

    await streamer.shutdown()


async def test_batch_insert_size():
    """Test that records are batched correctly."""
    pool = _make_mock_pool()

    streamer = QueryExecutionStreamer(
        pool=pool,
        test_id="test-123",
        flush_interval_seconds=0.05,  # Short interval to trigger flush
        flush_threshold_records=100,
        batch_insert_size=3,  # Batch size of 3
        results_prefix=TEST_PREFIX,
    )

    await streamer.start()

    # Append 10 records (should create 4 batches: 3+3+3+1)
    for i in range(10):
        record = _make_mock_record(f"query-{i}")
        await streamer.append(record)

    # Wait for flush to process
    await asyncio.sleep(0.15)

    # Should have 4 execute_query calls (ceil(10/3) = 4 batches)
    assert pool.execute_query.call_count == 4

    await streamer.shutdown()


async def test_requeue_on_failure():
    """Test that failed batches are re-queued once."""
    pool = _make_mock_pool()

    # First call fails, subsequent calls succeed
    call_count = [0]

    async def mock_execute(*args, **kwargs):
        call_count[0] += 1
        if call_count[0] == 1:
            raise Exception("Simulated failure")
        return []

    pool.execute_query = mock_execute

    streamer = QueryExecutionStreamer(
        pool=pool,
        test_id="test-123",
        flush_interval_seconds=0.05,
        flush_threshold_records=100,
        batch_insert_size=5,
        results_prefix=TEST_PREFIX,
    )

    await streamer.start()

    # Append records
    for i in range(5):
        record = _make_mock_record(f"query-{i}")
        await streamer.append(record)

    # Wait for flush and retry (need extra time for 1s backoff)
    await asyncio.sleep(1.3)

    # Should have retried (2 calls: 1 fail + 1 success)
    assert call_count[0] == 2

    # Stats should show successful flush
    stats = streamer.stats
    assert stats["total_records_flushed"] == 5
    assert stats["dropped_batches"] == 0

    await streamer.shutdown()


async def test_drop_on_second_failure():
    """Test that batches are dropped after second failure."""
    pool = _make_mock_pool()

    # All calls fail
    pool.execute_query = AsyncMock(side_effect=Exception("Persistent failure"))

    streamer = QueryExecutionStreamer(
        pool=pool,
        test_id="test-123",
        flush_interval_seconds=0.05,
        flush_threshold_records=100,
        batch_insert_size=5,
        results_prefix=TEST_PREFIX,
    )

    await streamer.start()

    # Append records
    for i in range(5):
        record = _make_mock_record(f"query-{i}")
        await streamer.append(record)

    # Wait for flush and retry attempts (1s backoff between retries)
    await asyncio.sleep(1.5)

    # Stats should show dropped batch
    stats = streamer.stats
    assert stats["dropped_batches"] >= 1
    assert streamer.run_partial

    await streamer.shutdown()


async def test_graceful_shutdown_flushes_buffer():
    """Test that shutdown flushes remaining buffer."""
    pool = _make_mock_pool()

    streamer = QueryExecutionStreamer(
        pool=pool,
        test_id="test-123",
        flush_interval_seconds=60.0,  # Long interval
        flush_threshold_records=1000,  # High threshold
        batch_insert_size=100,
        results_prefix=TEST_PREFIX,
    )

    await streamer.start()

    # Append records (below threshold, won't auto-flush before interval)
    for i in range(10):
        record = _make_mock_record(f"query-{i}")
        await streamer.append(record)

    # No flush yet (below threshold, before interval)
    assert pool.execute_query.call_count == 0

    # Shutdown should flush remaining records
    await streamer.shutdown()

    # Verify flush happened during shutdown
    assert pool.execute_query.call_count >= 1
    assert streamer.stats["total_records_flushed"] == 10


async def test_shutdown_timeout_calculation():
    """Test dynamic shutdown timeout based on flush p99."""
    pool = _make_mock_pool()

    # Make execute_query take some time
    async def slow_execute(*args, **kwargs):
        await asyncio.sleep(0.02)
        return []

    pool.execute_query = slow_execute

    streamer = QueryExecutionStreamer(
        pool=pool,
        test_id="test-123",
        flush_interval_seconds=0.03,
        flush_threshold_records=1000,
        batch_insert_size=100,
        results_prefix=TEST_PREFIX,
    )

    await streamer.start()

    # Append some records and let a few flushes happen
    for i in range(5):
        record = _make_mock_record(f"query-{i}")
        await streamer.append(record)

    await asyncio.sleep(0.15)  # Let some flushes complete

    # Shutdown with p99 hint (simulating 100ms p99)
    await streamer.shutdown(flush_p99_ms=100.0)

    # Just verify it completed without timeout issues
    assert streamer.stats["total_records_flushed"] >= 5


async def test_stats_tracking():
    """Test statistics are tracked correctly."""
    pool = _make_mock_pool()

    streamer = QueryExecutionStreamer(
        pool=pool,
        test_id="test-123",
        flush_interval_seconds=0.05,
        flush_threshold_records=100,
        batch_insert_size=5,
        results_prefix=TEST_PREFIX,
    )

    await streamer.start()

    # Initial stats
    stats = streamer.stats
    assert stats["total_records_flushed"] == 0
    assert stats["flush_count"] == 0
    assert stats["dropped_batches"] == 0

    # Append records
    for i in range(5):
        record = _make_mock_record(f"query-{i}")
        await streamer.append(record)

    await asyncio.sleep(0.15)

    # Check updated stats
    stats = streamer.stats
    assert stats["total_records_flushed"] == 5
    assert stats["flush_count"] == 1
    assert stats["dropped_batches"] == 0
    assert "flush_p99_ms" in stats

    await streamer.shutdown()


async def test_multiple_flush_cycles():
    """Test multiple flush cycles work correctly."""
    pool = _make_mock_pool()

    streamer = QueryExecutionStreamer(
        pool=pool,
        test_id="test-123",
        flush_interval_seconds=0.03,  # Short interval
        flush_threshold_records=1000,
        batch_insert_size=100,
        results_prefix=TEST_PREFIX,
    )

    await streamer.start()

    # Append records over multiple cycles
    for cycle in range(3):
        for i in range(5):
            record = _make_mock_record(f"cycle{cycle}-query-{i}")
            await streamer.append(record)
        await asyncio.sleep(0.05)

    await streamer.shutdown()

    # All records should be flushed
    assert streamer.stats["total_records_flushed"] == 15
    assert streamer.stats["flush_count"] >= 1


async def test_empty_buffer_shutdown():
    """Test shutdown with empty buffer."""
    pool = _make_mock_pool()

    streamer = QueryExecutionStreamer(
        pool=pool,
        test_id="test-123",
        results_prefix=TEST_PREFIX,
    )

    await streamer.start()

    # Shutdown without adding any records
    await streamer.shutdown()

    # Should complete cleanly
    assert streamer.stats["total_records_flushed"] == 0
    assert streamer.stats["flush_count"] == 0
    assert not streamer.run_partial


async def test_results_prefix():
    """Test custom results prefix in queries."""
    pool = _make_mock_pool()

    streamer = QueryExecutionStreamer(
        pool=pool,
        test_id="test-123",
        results_prefix="CUSTOM_DB.CUSTOM_SCHEMA",
        flush_interval_seconds=0.05,
        batch_insert_size=1,
    )

    await streamer.start()

    record = _make_mock_record("query-1")
    await streamer.append(record)

    await asyncio.sleep(0.15)

    # Verify the query used custom prefix
    assert pool.execute_query.call_count >= 1
    call_args = pool.execute_query.call_args
    assert call_args is not None
    query = call_args[0][0]
    assert "CUSTOM_DB.CUSTOM_SCHEMA.QUERY_EXECUTIONS" in query

    await streamer.shutdown()


async def test_concurrent_appends():
    """Test thread-safe concurrent appends."""
    pool = _make_mock_pool()

    streamer = QueryExecutionStreamer(
        pool=pool,
        test_id="test-123",
        flush_interval_seconds=60.0,
        flush_threshold_records=1000,
        batch_insert_size=100,
        results_prefix=TEST_PREFIX,
    )

    await streamer.start()

    # Simulate concurrent appends from multiple workers
    async def append_records(worker_id: int, count: int):
        for i in range(count):
            record = _make_mock_record(f"worker{worker_id}-query-{i}")
            await streamer.append(record)

    # Run 5 concurrent workers each appending 20 records
    tasks = [append_records(w, 20) for w in range(5)]
    await asyncio.gather(*tasks)

    # All 100 records should be in buffer
    assert len(streamer._buffer) == 100

    await streamer.shutdown()

    # All should be flushed
    assert streamer.stats["total_records_flushed"] == 100


async def test_not_started_shutdown():
    """Test shutdown when streamer was never started."""
    pool = _make_mock_pool()

    streamer = QueryExecutionStreamer(
        pool=pool,
        test_id="test-123",
        results_prefix=TEST_PREFIX,
    )

    # Should not raise
    await streamer.shutdown()

    assert pool.execute_query.call_count == 0


async def test_double_start():
    """Test that starting twice is handled gracefully."""
    pool = _make_mock_pool()

    streamer = QueryExecutionStreamer(
        pool=pool,
        test_id="test-123",
        flush_interval_seconds=60.0,
        results_prefix=TEST_PREFIX,
    )

    await streamer.start()
    first_task = streamer._flush_task

    # Second start should be no-op
    await streamer.start()
    assert streamer._flush_task is first_task

    await streamer.shutdown()


def main():
    """Run all QueryExecutionStreamer tests."""
    print("=" * 60)
    print("Running QueryExecutionStreamer Tests")
    print("=" * 60)

    # Run with pytest
    import subprocess

    result = subprocess.run(
        ["python", "-m", "pytest", __file__, "-v"],
        cwd=Path(__file__).parent.parent,
    )
    return result.returncode


if __name__ == "__main__":
    sys.exit(main())
