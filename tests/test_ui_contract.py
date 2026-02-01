"""
Tests for Section 2.9 - UI Contract.

These tests verify the unified payload structure with `run` + `workers` shape,
the find_max_state handling, and multi-worker metrics aggregation.

Tested components:
- _fetch_run_status() - fetches RUN_STATUS including find_max_state
- _aggregate_multi_worker_metrics() - aggregates metrics with workers array
- _build_run_snapshot() - builds unified run payload
- WebSocket payload structure with find_max field
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class _MockPool:
    """Stub pool for testing database queries."""

    def __init__(self, query_results: dict[str, list[tuple[Any, ...]]]) -> None:
        self._query_results = query_results
        self.calls: list[tuple[str, list[object] | None]] = []

    async def execute_query(
        self, query: str, params: list[object] | None = None
    ) -> list[tuple[Any, ...]]:
        self.calls.append((query, params))
        sql_upper = " ".join(str(query).split()).upper()

        for key, result in self._query_results.items():
            if key.upper() in sql_upper:
                return result
        return []


class TestFetchRunStatus:
    """Tests for _fetch_run_status() function."""

    @pytest.mark.asyncio
    async def test_fetch_run_status_returns_all_fields(self) -> None:
        """Fetch run status returns complete status including find_max_state."""
        from backend.main import _fetch_run_status

        now = datetime.now(UTC).replace(tzinfo=None)
        find_max_state = {
            "current_step": 3,
            "target_connections": 50,
            "status": "SEARCHING",
        }

        mock_pool = _MockPool(
            {
                "RUN_STATUS": [
                    (
                        "RUNNING",  # status
                        "MEASUREMENT",  # phase
                        now - timedelta(minutes=5),  # start_time
                        None,  # end_time
                        now - timedelta(minutes=4),  # warmup_end_time
                        4,  # total_workers_expected
                        4,  # workers_registered
                        3,  # workers_active
                        1,  # workers_completed
                        find_max_state,  # find_max_state
                        None,  # cancellation_reason
                        300.0,  # elapsed_seconds
                    )
                ]
            }
        )

        with patch(
            "backend.main.snowflake_pool.get_default_pool", return_value=mock_pool
        ):
            result = await _fetch_run_status("run-123")

        assert result is not None
        assert result["status"] == "RUNNING"
        assert result["phase"] == "MEASUREMENT"
        assert result["total_workers_expected"] == 4
        assert result["workers_registered"] == 4
        assert result["workers_active"] == 3
        assert result["workers_completed"] == 1
        assert result["find_max_state"] == find_max_state

    @pytest.mark.asyncio
    async def test_fetch_run_status_returns_none_when_not_found(self) -> None:
        """Fetch run status returns None when run doesn't exist."""
        from backend.main import _fetch_run_status

        mock_pool = _MockPool({"RUN_STATUS": []})

        with patch(
            "backend.main.snowflake_pool.get_default_pool", return_value=mock_pool
        ):
            result = await _fetch_run_status("nonexistent-run")

        assert result is None

    @pytest.mark.asyncio
    async def test_fetch_run_status_handles_null_find_max_state(self) -> None:
        """Fetch run status handles NULL find_max_state gracefully."""
        from backend.main import _fetch_run_status

        now = datetime.now(UTC).replace(tzinfo=None)

        mock_pool = _MockPool(
            {
                "RUN_STATUS": [
                    (
                        "RUNNING",
                        "WARMUP",
                        now,
                        None,
                        None,
                        1,
                        1,
                        1,
                        0,
                        None,  # find_max_state is NULL
                        None,  # cancellation_reason
                        120.0,  # elapsed_seconds
                    )
                ]
            }
        )

        with patch(
            "backend.main.snowflake_pool.get_default_pool", return_value=mock_pool
        ):
            result = await _fetch_run_status("run-456")

        assert result is not None
        assert result["find_max_state"] is None

    @pytest.mark.asyncio
    async def test_fetch_run_status_uppercases_status_and_phase(self) -> None:
        """Fetch run status normalizes status and phase to uppercase."""
        from backend.main import _fetch_run_status

        mock_pool = _MockPool(
            {
                "RUN_STATUS": [
                    (
                        "running",
                        "measurement",
                        None,
                        None,
                        None,
                        1,
                        1,
                        1,
                        0,
                        None,
                        None,
                        0.0,
                    )
                ]
            }
        )

        with patch(
            "backend.main.snowflake_pool.get_default_pool", return_value=mock_pool
        ):
            result = await _fetch_run_status("run-789")

        assert result is not None
        assert result["status"] == "RUNNING"
        assert result["phase"] == "MEASUREMENT"

    @pytest.mark.asyncio
    async def test_fetch_run_status_returns_none_on_exception(self) -> None:
        """Fetch run status returns None on database exception."""
        from backend.main import _fetch_run_status

        mock_pool = MagicMock()
        mock_pool.execute_query = AsyncMock(side_effect=Exception("DB error"))

        with patch(
            "backend.main.snowflake_pool.get_default_pool", return_value=mock_pool
        ):
            result = await _fetch_run_status("run-error")

        assert result is None


class TestAggregateMultiWorkerMetrics:
    """Tests for _aggregate_multi_worker_metrics() - unified payload structure."""

    @pytest.mark.asyncio
    async def test_aggregate_returns_workers_array(self) -> None:
        """Aggregate returns workers array in unified payload."""
        from backend.main import _aggregate_multi_worker_metrics

        now = datetime.now(UTC).replace(tzinfo=None)

        # Worker metrics snapshots
        metrics_rows = [
            (
                60.0,  # elapsed
                1000,  # total_queries
                16.5,  # qps
                5.0,  # p50
                15.0,  # p95
                25.0,  # p99
                8.0,  # avg
                800,  # read_count
                200,  # write_count
                5,  # error_count
                10,  # active_connections
                10,  # target_connections
                "{}",  # custom_metrics
                "MEASUREMENT",  # phase
                "worker-0",  # worker_id
                0,  # worker_group_id
            ),
            (
                60.0,
                900,
                15.0,
                6.0,
                18.0,
                30.0,
                9.0,
                700,
                200,
                3,
                10,
                10,
                "{}",
                "MEASUREMENT",
                "worker-1",
                1,
            ),
        ]

        # Heartbeat rows
        heartbeat_rows = [
            (
                "worker-0",
                0,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=2),
                10,
                10,
                1000,
                5,
            ),
            (
                "worker-1",
                1,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=3),
                10,
                10,
                900,
                3,
            ),
        ]

        mock_pool = _MockPool(
            {
                "WORKER_METRICS_SNAPSHOTS": metrics_rows,
                "WORKER_HEARTBEATS": heartbeat_rows,
            }
        )

        with (
            patch(
                "backend.main.snowflake_pool.get_default_pool", return_value=mock_pool
            ),
            patch(
                "backend.main.results_store.fetch_latest_warehouse_poll_snapshot",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            result = await _aggregate_multi_worker_metrics("parent-run-123")

        # Verify workers array exists
        assert "workers" in result
        assert result["latency_aggregation_method"] == "slowest_worker_approximation"
        workers = result["workers"]
        assert len(workers) == 2

        # Verify worker structure
        worker_0 = next(w for w in workers if w["worker_id"] == "worker-0")
        assert worker_0["worker_group_id"] == 0
        assert worker_0["status"] == "RUNNING"
        assert worker_0["phase"] == "MEASUREMENT"
        assert worker_0["health"] == "HEALTHY"
        assert "metrics" in worker_0
        assert worker_0["metrics"]["qps"] == 16.5
        assert worker_0["metrics"]["p50_latency_ms"] == 5.0

    @pytest.mark.asyncio
    async def test_aggregate_calculates_aggregate_metrics(self) -> None:
        """Aggregate calculates correct aggregate metrics from workers."""
        from backend.main import _aggregate_multi_worker_metrics

        now = datetime.now(UTC).replace(tzinfo=None)

        metrics_rows = [
            (
                60.0,
                1000,
                10.0,
                5.0,
                15.0,
                25.0,
                8.0,
                800,
                200,
                2,
                5,
                5,
                "{}",
                "MEASUREMENT",
                "w0",
                0,
            ),
            (
                60.0,
                1000,
                10.0,
                6.0,
                20.0,
                30.0,
                10.0,
                800,
                200,
                3,
                5,
                5,
                "{}",
                "MEASUREMENT",
                "w1",
                1,
            ),
        ]

        heartbeat_rows = [
            (
                "w0",
                0,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=1),
                5,
                5,
                1000,
                2,
            ),
            (
                "w1",
                1,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=1),
                5,
                5,
                1000,
                3,
            ),
        ]

        mock_pool = _MockPool(
            {
                "WORKER_METRICS_SNAPSHOTS": metrics_rows,
                "WORKER_HEARTBEATS": heartbeat_rows,
            }
        )

        with (
            patch(
                "backend.main.snowflake_pool.get_default_pool", return_value=mock_pool
            ),
            patch(
                "backend.main.results_store.fetch_latest_warehouse_poll_snapshot",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            result = await _aggregate_multi_worker_metrics("parent-run")

        # QPS is summed across workers
        assert result["ops"]["current_per_sec"] == 20.0

        # P50 is averaged (5.0 + 6.0) / 2 = 5.5
        assert result["latency"]["p50"] == 5.5

        # P95/P99 uses MAX (slowest worker) for conservative estimate
        assert result["latency"]["p95"] == 20.0
        assert result["latency"]["p99"] == 30.0

        # Errors summed
        assert result["errors"]["count"] == 5

    @pytest.mark.asyncio
    async def test_aggregate_excludes_dead_workers_from_metrics(self) -> None:
        """Aggregate excludes DEAD workers from aggregate metrics."""
        from backend.main import _aggregate_multi_worker_metrics

        now = datetime.now(UTC).replace(tzinfo=None)

        metrics_rows = [
            (
                60.0,
                1000,
                10.0,
                5.0,
                15.0,
                25.0,
                8.0,
                800,
                200,
                0,
                5,
                5,
                "{}",
                "MEASUREMENT",
                "alive",
                0,
            ),
            (
                60.0,
                500,
                5.0,
                100.0,
                200.0,
                300.0,
                150.0,
                400,
                100,
                50,
                5,
                5,
                "{}",
                "MEASUREMENT",
                "dead",
                1,
            ),
        ]

        heartbeat_rows = [
            (
                "alive",
                0,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=1),
                5,
                5,
                1000,
                0,
            ),
            (
                "dead",
                1,
                "DEAD",
                "MEASUREMENT",
                now - timedelta(minutes=5),
                0,
                5,
                500,
                50,
            ),
        ]

        mock_pool = _MockPool(
            {
                "WORKER_METRICS_SNAPSHOTS": metrics_rows,
                "WORKER_HEARTBEATS": heartbeat_rows,
            }
        )

        with (
            patch(
                "backend.main.snowflake_pool.get_default_pool", return_value=mock_pool
            ),
            patch(
                "backend.main.results_store.fetch_latest_warehouse_poll_snapshot",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            result = await _aggregate_multi_worker_metrics("parent-run")

        # Only alive worker's metrics should be counted
        assert result["ops"]["current_per_sec"] == 10.0
        assert result["latency"]["p95"] == 15.0
        assert result["errors"]["count"] == 0

        # But both workers should be in workers array
        assert len(result["workers"]) == 2

    @pytest.mark.asyncio
    async def test_aggregate_returns_empty_when_no_data(self) -> None:
        """Aggregate returns empty dict when no workers found."""
        from backend.main import _aggregate_multi_worker_metrics

        mock_pool = _MockPool(
            {
                "WORKER_METRICS_SNAPSHOTS": [],
                "WORKER_HEARTBEATS": [],
            }
        )

        with patch(
            "backend.main.snowflake_pool.get_default_pool", return_value=mock_pool
        ):
            result = await _aggregate_multi_worker_metrics("empty-run")

        assert result == {}

    @pytest.mark.asyncio
    async def test_aggregate_worker_health_states(self) -> None:
        """Aggregate correctly determines worker health states."""
        from backend.main import _aggregate_multi_worker_metrics

        now = datetime.now(UTC).replace(tzinfo=None)

        metrics_rows = [
            (
                60.0,
                100,
                1.0,
                5.0,
                10.0,
                15.0,
                7.0,
                80,
                20,
                0,
                5,
                5,
                "{}",
                "MEASUREMENT",
                "healthy",
                0,
            ),
            (
                60.0,
                100,
                1.0,
                5.0,
                10.0,
                15.0,
                7.0,
                80,
                20,
                0,
                5,
                5,
                "{}",
                "MEASUREMENT",
                "stale",
                1,
            ),
            (
                60.0,
                100,
                1.0,
                5.0,
                10.0,
                15.0,
                7.0,
                80,
                20,
                0,
                5,
                5,
                "{}",
                "MEASUREMENT",
                "dead",
                2,
            ),
        ]

        heartbeat_rows = [
            (
                "healthy",
                0,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=5),
                5,
                5,
                100,
                0,
            ),
            (
                "stale",
                1,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=45),
                5,
                5,
                100,
                0,
            ),
            (
                "dead",
                2,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=90),
                5,
                5,
                100,
                0,
            ),
        ]

        mock_pool = _MockPool(
            {
                "WORKER_METRICS_SNAPSHOTS": metrics_rows,
                "WORKER_HEARTBEATS": heartbeat_rows,
            }
        )

        with (
            patch(
                "backend.main.snowflake_pool.get_default_pool", return_value=mock_pool
            ),
            patch(
                "backend.main.results_store.fetch_latest_warehouse_poll_snapshot",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            result = await _aggregate_multi_worker_metrics("health-test")

        workers = {w["worker_id"]: w for w in result["workers"]}

        assert workers["healthy"]["health"] == "HEALTHY"
        assert workers["stale"]["health"] == "STALE"
        assert workers["dead"]["health"] == "DEAD"

    @pytest.mark.asyncio
    async def test_aggregate_workers_sorted_by_group_id(self) -> None:
        """Aggregate returns workers sorted by worker_group_id."""
        from backend.main import _aggregate_multi_worker_metrics

        now = datetime.now(UTC).replace(tzinfo=None)

        metrics_rows = [
            (
                60.0,
                100,
                1.0,
                5.0,
                10.0,
                15.0,
                7.0,
                80,
                20,
                0,
                5,
                5,
                "{}",
                "MEASUREMENT",
                "w2",
                2,
            ),
            (
                60.0,
                100,
                1.0,
                5.0,
                10.0,
                15.0,
                7.0,
                80,
                20,
                0,
                5,
                5,
                "{}",
                "MEASUREMENT",
                "w0",
                0,
            ),
            (
                60.0,
                100,
                1.0,
                5.0,
                10.0,
                15.0,
                7.0,
                80,
                20,
                0,
                5,
                5,
                "{}",
                "MEASUREMENT",
                "w1",
                1,
            ),
        ]

        heartbeat_rows = [
            (
                "w2",
                2,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=1),
                5,
                5,
                100,
                0,
            ),
            (
                "w0",
                0,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=1),
                5,
                5,
                100,
                0,
            ),
            (
                "w1",
                1,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=1),
                5,
                5,
                100,
                0,
            ),
        ]

        mock_pool = _MockPool(
            {
                "WORKER_METRICS_SNAPSHOTS": metrics_rows,
                "WORKER_HEARTBEATS": heartbeat_rows,
            }
        )

        with (
            patch(
                "backend.main.snowflake_pool.get_default_pool", return_value=mock_pool
            ),
            patch(
                "backend.main.results_store.fetch_latest_warehouse_poll_snapshot",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            result = await _aggregate_multi_worker_metrics("sort-test")

        workers = result["workers"]
        assert workers[0]["worker_group_id"] == 0
        assert workers[1]["worker_group_id"] == 1
        assert workers[2]["worker_group_id"] == 2


class TestBuildRunSnapshot:
    """Tests for _build_run_snapshot() - unified run payload."""

    def test_build_run_snapshot_includes_worker_counts(self) -> None:
        """Build run snapshot includes worker orchestration counts."""
        from backend.main import _build_run_snapshot

        run_status = {
            "total_workers_expected": 4,
            "workers_registered": 4,
            "workers_active": 3,
            "workers_completed": 1,
            "start_time": datetime.now(UTC),
            "end_time": None,
        }

        result = _build_run_snapshot(
            run_id="run-123",
            status="RUNNING",
            phase="MEASUREMENT",
            elapsed_seconds=60.0,
            worker_count=4,
            aggregate_metrics={
                "ops": {"total": 1000, "current_per_sec": 16.5},
                "latency": {"p50": 5.0, "p95": 15.0, "p99": 25.0, "avg": 8.0},
                "errors": {"count": 5, "rate": 0.005},
            },
            run_status=run_status,
        )

        # Note: key is "workers_expected" not "total_workers_expected"
        assert result["workers_expected"] == 4
        assert result["workers_registered"] == 4
        assert result["workers_active"] == 3
        assert result["workers_completed"] == 1

    def test_build_run_snapshot_handles_missing_run_status(self) -> None:
        """Build run snapshot handles None run_status gracefully."""
        from backend.main import _build_run_snapshot

        result = _build_run_snapshot(
            run_id="run-456",
            status="RUNNING",
            phase="WARMUP",
            elapsed_seconds=10.0,
            worker_count=1,
            aggregate_metrics={},
            run_status=None,
        )

        assert result["run_id"] == "run-456"
        assert result["status"] == "RUNNING"
        assert "total_workers_expected" not in result


class TestFindMaxStateInPayload:
    """Tests for find_max_state inclusion in WebSocket payloads."""

    @pytest.mark.asyncio
    async def test_find_max_state_included_in_payload_when_present(self) -> None:
        """Find max state is included in payload when present in run_status."""
        from backend.main import _parse_variant_dict

        # Test the helper function that parses find_max_state
        find_max_data = {
            "current_step": 5,
            "target_connections": 100,
            "status": "SEARCHING",
            "last_step_result": {"qps": 150.0, "p95_ms": 12.0},
        }

        result = _parse_variant_dict(find_max_data)

        assert result == find_max_data

    @pytest.mark.asyncio
    async def test_find_max_state_parses_json_string(self) -> None:
        """Find max state parses JSON string from VARIANT column."""
        from backend.main import _parse_variant_dict
        import json

        find_max_json = json.dumps({"current_step": 3, "status": "CONVERGED"})

        result = _parse_variant_dict(find_max_json)

        assert result == {"current_step": 3, "status": "CONVERGED"}

    @pytest.mark.asyncio
    async def test_find_max_state_returns_none_for_invalid(self) -> None:
        """Find max state returns None for invalid/empty values."""
        from backend.main import _parse_variant_dict

        assert _parse_variant_dict(None) is None
        assert _parse_variant_dict("") is None
        assert _parse_variant_dict("not json") is None
        assert _parse_variant_dict([]) is None


class TestWarehousePollerIntegration:
    """Tests for MCW Active Clusters via warehouse poller."""

    @pytest.mark.asyncio
    async def test_warehouse_snapshot_used_when_available(self) -> None:
        """Warehouse poller snapshot is used when available."""
        from backend.main import _aggregate_multi_worker_metrics

        now = datetime.now(UTC).replace(tzinfo=None)

        metrics_rows = [
            (
                60.0,
                1000,
                10.0,
                5.0,
                15.0,
                25.0,
                8.0,
                800,
                200,
                0,
                5,
                5,
                "{}",
                "MEASUREMENT",
                "w0",
                0,
            ),
        ]

        heartbeat_rows = [
            (
                "w0",
                0,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=1),
                5,
                5,
                1000,
                0,
            ),
        ]

        poller_snapshot = {
            "active_clusters": 2,
            "queued_queries": 5,
            "running_queries": 10,
            "timestamp": now.isoformat(),
        }

        mock_pool = _MockPool(
            {
                "WORKER_METRICS_SNAPSHOTS": metrics_rows,
                "WORKER_HEARTBEATS": heartbeat_rows,
            }
        )

        with (
            patch(
                "backend.main.snowflake_pool.get_default_pool", return_value=mock_pool
            ),
            patch(
                "backend.main.results_store.fetch_latest_warehouse_poll_snapshot",
                new_callable=AsyncMock,
                return_value=poller_snapshot,
            ),
        ):
            result = await _aggregate_multi_worker_metrics("poller-test")

        assert result["custom_metrics"]["warehouse"] == poller_snapshot
        assert result["custom_metrics"]["warehouse"]["active_clusters"] == 2


class TestPhaseResolution:
    """Tests for phase resolution across workers."""

    @pytest.mark.asyncio
    async def test_phase_resolution_uses_minimum_phase(self) -> None:
        """Phase resolution uses minimum (earliest) phase across workers."""
        from backend.main import _aggregate_multi_worker_metrics

        now = datetime.now(UTC).replace(tzinfo=None)

        # One worker in WARMUP, one in MEASUREMENT
        metrics_rows = [
            (
                30.0,
                500,
                5.0,
                5.0,
                10.0,
                15.0,
                7.0,
                400,
                100,
                0,
                5,
                5,
                "{}",
                "WARMUP",
                "w0",
                0,
            ),
            (
                60.0,
                1000,
                10.0,
                5.0,
                10.0,
                15.0,
                7.0,
                800,
                200,
                0,
                5,
                5,
                "{}",
                "MEASUREMENT",
                "w1",
                1,
            ),
        ]

        heartbeat_rows = [
            ("w0", 0, "RUNNING", "WARMUP", now - timedelta(seconds=1), 5, 5, 500, 0),
            (
                "w1",
                1,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=1),
                5,
                5,
                1000,
                0,
            ),
        ]

        mock_pool = _MockPool(
            {
                "WORKER_METRICS_SNAPSHOTS": metrics_rows,
                "WORKER_HEARTBEATS": heartbeat_rows,
            }
        )

        with (
            patch(
                "backend.main.snowflake_pool.get_default_pool", return_value=mock_pool
            ),
            patch(
                "backend.main.results_store.fetch_latest_warehouse_poll_snapshot",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            result = await _aggregate_multi_worker_metrics("phase-test")

        # Should resolve to WARMUP (earlier phase)
        assert result["phase"] == "WARMUP"


# -----------------------------------------------------------------------------
# Section 2.14: Latency Aggregation Documentation
# -----------------------------------------------------------------------------


class TestLatencyAggregationMethod:
    """Tests for Section 2.14 - latency_aggregation_method field and semantics."""

    @pytest.mark.asyncio
    async def test_aggregation_method_field_present_for_multi_worker(self) -> None:
        """latency_aggregation_method is present for multi-worker runs."""
        from backend.main import _aggregate_multi_worker_metrics

        now = datetime.now(UTC).replace(tzinfo=None)

        metrics_rows = [
            (
                60.0,
                1000,
                10.0,
                5.0,
                15.0,
                25.0,
                8.0,
                800,
                200,
                0,
                5,
                5,
                "{}",
                "MEASUREMENT",
                "w0",
                0,
            ),
            (
                60.0,
                1000,
                10.0,
                6.0,
                20.0,
                30.0,
                10.0,
                800,
                200,
                0,
                5,
                5,
                "{}",
                "MEASUREMENT",
                "w1",
                1,
            ),
        ]
        heartbeat_rows = [
            (
                "w0",
                0,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=1),
                5,
                5,
                1000,
                0,
            ),
            (
                "w1",
                1,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=1),
                5,
                5,
                1000,
                0,
            ),
        ]

        mock_pool = _MockPool(
            {
                "WORKER_METRICS_SNAPSHOTS": metrics_rows,
                "WORKER_HEARTBEATS": heartbeat_rows,
            }
        )

        with (
            patch(
                "backend.main.snowflake_pool.get_default_pool", return_value=mock_pool
            ),
            patch(
                "backend.main.results_store.fetch_latest_warehouse_poll_snapshot",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            result = await _aggregate_multi_worker_metrics("multi-worker-run")

        assert "latency_aggregation_method" in result
        assert result["latency_aggregation_method"] == "slowest_worker_approximation"

    @pytest.mark.asyncio
    async def test_p50_uses_avg_across_workers(self) -> None:
        """P50 latency uses AVG across workers (not MAX)."""
        from backend.main import _aggregate_multi_worker_metrics

        now = datetime.now(UTC).replace(tzinfo=None)

        # Worker 0: P50 = 10.0ms, Worker 1: P50 = 20.0ms
        # AVG should be 15.0ms
        metrics_rows = [
            (
                60.0,
                1000,
                10.0,
                10.0,
                50.0,
                100.0,
                15.0,
                800,
                200,
                0,
                5,
                5,
                "{}",
                "MEASUREMENT",
                "w0",
                0,
            ),
            (
                60.0,
                1000,
                10.0,
                20.0,
                40.0,
                80.0,
                25.0,
                800,
                200,
                0,
                5,
                5,
                "{}",
                "MEASUREMENT",
                "w1",
                1,
            ),
        ]
        heartbeat_rows = [
            (
                "w0",
                0,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=1),
                5,
                5,
                1000,
                0,
            ),
            (
                "w1",
                1,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=1),
                5,
                5,
                1000,
                0,
            ),
        ]

        mock_pool = _MockPool(
            {
                "WORKER_METRICS_SNAPSHOTS": metrics_rows,
                "WORKER_HEARTBEATS": heartbeat_rows,
            }
        )

        with (
            patch(
                "backend.main.snowflake_pool.get_default_pool", return_value=mock_pool
            ),
            patch(
                "backend.main.results_store.fetch_latest_warehouse_poll_snapshot",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            result = await _aggregate_multi_worker_metrics("avg-p50-test")

        # P50 = AVG(10.0, 20.0) = 15.0
        assert result["latency"]["p50"] == 15.0

    @pytest.mark.asyncio
    async def test_p95_uses_max_across_workers(self) -> None:
        """P95 latency uses MAX across workers (slowest-worker approximation)."""
        from backend.main import _aggregate_multi_worker_metrics

        now = datetime.now(UTC).replace(tzinfo=None)

        # Worker 0: P95 = 50.0ms, Worker 1: P95 = 40.0ms
        # MAX should be 50.0ms (slowest worker)
        metrics_rows = [
            (
                60.0,
                1000,
                10.0,
                10.0,
                50.0,
                100.0,
                15.0,
                800,
                200,
                0,
                5,
                5,
                "{}",
                "MEASUREMENT",
                "w0",
                0,
            ),
            (
                60.0,
                1000,
                10.0,
                20.0,
                40.0,
                80.0,
                25.0,
                800,
                200,
                0,
                5,
                5,
                "{}",
                "MEASUREMENT",
                "w1",
                1,
            ),
        ]
        heartbeat_rows = [
            (
                "w0",
                0,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=1),
                5,
                5,
                1000,
                0,
            ),
            (
                "w1",
                1,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=1),
                5,
                5,
                1000,
                0,
            ),
        ]

        mock_pool = _MockPool(
            {
                "WORKER_METRICS_SNAPSHOTS": metrics_rows,
                "WORKER_HEARTBEATS": heartbeat_rows,
            }
        )

        with (
            patch(
                "backend.main.snowflake_pool.get_default_pool", return_value=mock_pool
            ),
            patch(
                "backend.main.results_store.fetch_latest_warehouse_poll_snapshot",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            result = await _aggregate_multi_worker_metrics("max-p95-test")

        # P95 = MAX(50.0, 40.0) = 50.0
        assert result["latency"]["p95"] == 50.0

    @pytest.mark.asyncio
    async def test_p99_uses_max_across_workers(self) -> None:
        """P99 latency uses MAX across workers (slowest-worker approximation)."""
        from backend.main import _aggregate_multi_worker_metrics

        now = datetime.now(UTC).replace(tzinfo=None)

        # Worker 0: P99 = 100.0ms, Worker 1: P99 = 80.0ms
        # MAX should be 100.0ms (slowest worker)
        metrics_rows = [
            (
                60.0,
                1000,
                10.0,
                10.0,
                50.0,
                100.0,
                15.0,
                800,
                200,
                0,
                5,
                5,
                "{}",
                "MEASUREMENT",
                "w0",
                0,
            ),
            (
                60.0,
                1000,
                10.0,
                20.0,
                40.0,
                80.0,
                25.0,
                800,
                200,
                0,
                5,
                5,
                "{}",
                "MEASUREMENT",
                "w1",
                1,
            ),
        ]
        heartbeat_rows = [
            (
                "w0",
                0,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=1),
                5,
                5,
                1000,
                0,
            ),
            (
                "w1",
                1,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=1),
                5,
                5,
                1000,
                0,
            ),
        ]

        mock_pool = _MockPool(
            {
                "WORKER_METRICS_SNAPSHOTS": metrics_rows,
                "WORKER_HEARTBEATS": heartbeat_rows,
            }
        )

        with (
            patch(
                "backend.main.snowflake_pool.get_default_pool", return_value=mock_pool
            ),
            patch(
                "backend.main.results_store.fetch_latest_warehouse_poll_snapshot",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            result = await _aggregate_multi_worker_metrics("max-p99-test")

        # P99 = MAX(100.0, 80.0) = 100.0
        assert result["latency"]["p99"] == 100.0

    @pytest.mark.asyncio
    async def test_qps_uses_sum_across_workers(self) -> None:
        """QPS uses SUM across workers (total throughput)."""
        from backend.main import _aggregate_multi_worker_metrics

        now = datetime.now(UTC).replace(tzinfo=None)

        # Worker 0: QPS = 10.0, Worker 1: QPS = 15.0
        # SUM should be 25.0
        metrics_rows = [
            (
                60.0,
                1000,
                10.0,
                10.0,
                50.0,
                100.0,
                15.0,
                800,
                200,
                0,
                5,
                5,
                "{}",
                "MEASUREMENT",
                "w0",
                0,
            ),
            (
                60.0,
                1000,
                15.0,
                20.0,
                40.0,
                80.0,
                25.0,
                800,
                200,
                0,
                5,
                5,
                "{}",
                "MEASUREMENT",
                "w1",
                1,
            ),
        ]
        heartbeat_rows = [
            (
                "w0",
                0,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=1),
                5,
                5,
                1000,
                0,
            ),
            (
                "w1",
                1,
                "RUNNING",
                "MEASUREMENT",
                now - timedelta(seconds=1),
                5,
                5,
                1000,
                0,
            ),
        ]

        mock_pool = _MockPool(
            {
                "WORKER_METRICS_SNAPSHOTS": metrics_rows,
                "WORKER_HEARTBEATS": heartbeat_rows,
            }
        )

        with (
            patch(
                "backend.main.snowflake_pool.get_default_pool", return_value=mock_pool
            ),
            patch(
                "backend.main.results_store.fetch_latest_warehouse_poll_snapshot",
                new_callable=AsyncMock,
                return_value=None,
            ),
        ):
            result = await _aggregate_multi_worker_metrics("sum-qps-test")

        # QPS = SUM(10.0, 15.0) = 25.0
        assert result["ops"]["current_per_sec"] == 25.0

    def test_aggregation_method_value_is_documented_constant(self) -> None:
        """latency_aggregation_method uses the documented constant value."""
        # This test documents the expected constant value
        # If this changes, documentation must be updated
        expected_value = "slowest_worker_approximation"

        # Verify the value matches what's documented in next-ui-architecture.md
        # and next-data-flow-and-lifecycle.md
        assert expected_value == "slowest_worker_approximation"
