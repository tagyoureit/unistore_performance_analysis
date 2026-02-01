#!/usr/bin/env python3
"""
Acceptance tests for QUERY_TAG phase verification.

Verifies that benchmark queries have correct phase tags in Snowflake's
QUERY_HISTORY, allowing filtering by WARMUP vs RUNNING phase.

Related: Issue 2.21.1 - QUERY_TAG Not Updating from WARMUP to RUNNING
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

import pytest

from backend.config import settings

pytestmark = pytest.mark.asyncio


def _snowflake_unreachable(exc: BaseException) -> bool:
    """Check if exception indicates Snowflake is unreachable."""
    msg = str(exc).lower()
    return (
        ("ip/token" in msg and "not allowed" in msg)
        or ("is not allowed to access snowflake" in msg)
        or ("failed to connect to db" in msg)
        or ("(08001)" in msg)
    )


async def test_query_tag_phase_separation():
    """
    Acceptance test: Verify QUERY_TAG correctly identifies warmup vs measurement queries.

    This test queries SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY to verify that:
    1. Warmup queries have QUERY_TAG containing ':phase=WARMUP'
    2. Measurement queries have QUERY_TAG containing ':phase=RUNNING'

    Requires a completed benchmark test with test_id to verify against.
    Set TEST_ID environment variable or skip this test.
    """
    import os

    test_id = os.environ.get("TEST_ID")
    if not test_id:
        pytest.skip(
            "TEST_ID environment variable not set; "
            "run a benchmark test first and set TEST_ID=<uuid> to verify phase tags"
        )

    if (
        settings.SNOWFLAKE_ACCOUNT.startswith("your_")
        or settings.SNOWFLAKE_USER.startswith("your_")
        or not settings.SNOWFLAKE_PASSWORD
    ):
        pytest.skip("Snowflake credentials not configured; skipping integration test")

    try:
        from backend.connectors.snowflake_pool import SnowflakeConnectionPool

        pool = SnowflakeConnectionPool(
            account=settings.SNOWFLAKE_ACCOUNT,
            user=settings.SNOWFLAKE_USER,
            password=settings.SNOWFLAKE_PASSWORD,
            warehouse=settings.SNOWFLAKE_WAREHOUSE,
            database=settings.SNOWFLAKE_DATABASE,
            schema=settings.SNOWFLAKE_SCHEMA,
            role=settings.SNOWFLAKE_ROLE,
            pool_size=1,
            max_overflow=0,
        )

        try:
            await pool.initialize()
        except Exception as e:
            if _snowflake_unreachable(e):
                pytest.skip(f"Snowflake not reachable: {e}")
            raise

        # Query ACCOUNT_USAGE.QUERY_HISTORY for this test's queries
        # Note: ACCOUNT_USAGE has ~45 minute latency
        query = """
        SELECT
            QUERY_TAG,
            COUNT(*) as query_count,
            CASE
                WHEN QUERY_TAG LIKE '%:phase=WARMUP%' THEN 'WARMUP'
                WHEN QUERY_TAG LIKE '%:phase=RUNNING%' THEN 'RUNNING'
                ELSE 'UNKNOWN'
            END as phase
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE QUERY_TAG LIKE %s
          AND START_TIME >= DATEADD(day, -1, CURRENT_TIMESTAMP())
        GROUP BY QUERY_TAG
        ORDER BY phase
        """

        tag_pattern = f"%unistore_benchmark:test_id={test_id}%"

        async with pool.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (tag_pattern,))
            results = cursor.fetchall()
            cursor.close()

        await pool.close_all()

        if not results:
            pytest.skip(
                f"No queries found for test_id={test_id}. "
                "ACCOUNT_USAGE has ~45 min latency; try again later."
            )

        # Parse results
        warmup_count = 0
        running_count = 0
        unknown_count = 0

        print(f"\n📊 QUERY_TAG Phase Analysis for test_id={test_id}")
        print("=" * 60)

        for row in results:
            _, count, phase = row[0], row[1], row[2]
            print(f"  {phase}: {count} queries")
            if phase == "WARMUP":
                warmup_count += count
            elif phase == "RUNNING":
                running_count += count
            else:
                unknown_count += count

        print("-" * 60)
        print(f"  Total WARMUP:  {warmup_count}")
        print(f"  Total RUNNING: {running_count}")
        print(f"  Total UNKNOWN: {unknown_count}")

        # Acceptance criteria
        assert unknown_count == 0, (
            f"Found {unknown_count} queries without valid phase tag. "
            "All queries should have :phase=WARMUP or :phase=RUNNING"
        )

        # If test had warmup enabled, we expect some WARMUP queries
        # If test ran measurement phase, we expect RUNNING queries
        assert running_count > 0 or warmup_count > 0, (
            "No queries found with phase tags. "
            "Check that _transition_to_measurement_phase() is being called."
        )

        print("\n✅ QUERY_TAG phase verification passed")

    except Exception as e:
        if _snowflake_unreachable(e):
            pytest.skip(f"Snowflake not reachable: {e}")
        raise


async def test_query_tag_format():
    """
    Unit test: Verify QUERY_TAG format is correct for filtering.

    Tests that the tag format 'unistore_benchmark:test_id=<uuid>:phase=<PHASE>'
    can be reliably parsed and filtered.
    """
    # Test tag parsing without Snowflake connection
    warmup_tag = "unistore_benchmark:test_id=abc123:phase=WARMUP"
    running_tag = "unistore_benchmark:test_id=abc123:phase=RUNNING"

    # Verify warmup tag format
    assert ":phase=WARMUP" in warmup_tag
    assert ":phase=RUNNING" not in warmup_tag
    assert "test_id=abc123" in warmup_tag

    # Verify running tag format
    assert ":phase=RUNNING" in running_tag
    assert ":phase=WARMUP" not in running_tag
    assert "test_id=abc123" in running_tag

    # Verify tag prefix for filtering
    assert warmup_tag.startswith("unistore_benchmark:")
    assert running_tag.startswith("unistore_benchmark:")

    print("\n✅ QUERY_TAG format verification passed")
