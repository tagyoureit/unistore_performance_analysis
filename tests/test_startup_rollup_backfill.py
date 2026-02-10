from __future__ import annotations

from typing import Any

import pytest


class _BackfillPool:
    def __init__(self, run_ids: list[str]) -> None:
        self.run_ids = run_ids
        self.calls: list[tuple[str, list[Any] | None]] = []

    async def execute_query(
        self, query: str, params: list[Any] | None = None
    ) -> list[tuple[Any, ...]]:
        self.calls.append((query, params))
        sql_upper = " ".join(str(query).split()).upper()
        if "JOIN" in sql_upper and "RUN_STATUS RS" in sql_upper:
            return [(run_id,) for run_id in self.run_ids]
        return []


@pytest.mark.asyncio
async def test_backfill_parent_run_rollups_updates_candidates(monkeypatch):
    from backend.core import results_store

    pool = _BackfillPool(["run-a", "run-b"])
    updated: list[str] = []

    async def fake_update(*, parent_run_id: str) -> None:
        updated.append(parent_run_id)

    monkeypatch.setattr(
        results_store.snowflake_pool, "get_default_pool", lambda: pool
    )
    monkeypatch.setattr(results_store, "update_parent_run_aggregate", fake_update)

    result = await results_store.backfill_parent_run_rollups(batch_size=50)

    assert result.candidate_count == 2
    assert result.updated_count == 2
    assert result.failed_count == 0
    assert updated == ["run-a", "run-b"]

    # Ensure the batch size bound was passed to the candidate query.
    assert any(params == [50] for _, params in pool.calls)


@pytest.mark.asyncio
async def test_backfill_parent_run_rollups_handles_individual_failures(monkeypatch):
    from backend.core import results_store

    pool = _BackfillPool(["run-ok", "run-fail"])
    updated: list[str] = []

    async def fake_update(*, parent_run_id: str) -> None:
        if parent_run_id == "run-fail":
            raise RuntimeError("boom")
        updated.append(parent_run_id)

    monkeypatch.setattr(
        results_store.snowflake_pool, "get_default_pool", lambda: pool
    )
    monkeypatch.setattr(results_store, "update_parent_run_aggregate", fake_update)

    result = await results_store.backfill_parent_run_rollups(batch_size=10)

    assert result.candidate_count == 2
    assert result.updated_count == 1
    assert result.failed_count == 1
    assert result.failed_run_ids == ["run-fail"]
    assert updated == ["run-ok"]


@pytest.mark.asyncio
async def test_startup_cleanup_runs_parent_rollup_backfill(monkeypatch):
    from backend import main
    from backend.core import results_store

    calls: list[Any] = []

    async def fake_cleanup_orphaned_tests(**kwargs):
        calls.append(("orphaned", kwargs))
        return results_store.OrphanCleanupResult(
            run_status_cleaned=0,
            test_results_cleaned=0,
            enrichment_cleaned=0,
            cleaned_run_ids=[],
            cleaned_test_ids=[],
            cleaned_enrichment_ids=[],
        )

    async def fake_backfill_parent_run_rollups(**kwargs):
        calls.append(("rollup_backfill", kwargs))
        return results_store.ParentRollupBackfillResult(
            candidate_count=0,
            updated_count=0,
            failed_count=0,
            candidate_run_ids=[],
            failed_run_ids=[],
        )

    async def fake_cleanup_stale_enrichment(**kwargs):
        calls.append(("stale_enrichment", kwargs))
        return (0, 0, [], [])

    monkeypatch.setattr(
        results_store, "cleanup_orphaned_tests", fake_cleanup_orphaned_tests
    )
    monkeypatch.setattr(
        results_store, "backfill_parent_run_rollups", fake_backfill_parent_run_rollups
    )
    monkeypatch.setattr(
        results_store, "cleanup_stale_enrichment", fake_cleanup_stale_enrichment
    )

    await main._cleanup_orphaned_tests_background()

    assert calls[0][0] == "orphaned"
    assert calls[1][0] == "rollup_backfill"
    assert calls[1][1].get("batch_size") == 200
    assert calls[2][0] == "stale_enrichment"
