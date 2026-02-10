from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock

import pytest


class _StubPool:
    async def execute_query(self, query: str, params=None):
        sql_upper = " ".join(str(query).split()).upper()

        if "SELECT TR.TEST_ID" in sql_upper and "FROM" in sql_upper:
            return [
                (
                    "run-123",
                    "run-123",
                    "stale-parent-run",
                    "POSTGRES",
                    "MEDIUM",
                    datetime(2026, 2, 9, 15, 30, 0),
                    100.0,
                    10.0,
                    20.0,
                    0.01,
                    "RUNNING",
                    10,
                    30.0,
                    None,
                    "SKIPPED",
                    "STANDARD_M",
                    "COMPLETED",
                    "COMPLETED",
                )
            ]

        if "SELECT COUNT(*)" in sql_upper:
            return [(1,)]

        return []


@pytest.mark.asyncio
async def test_list_tests_prefers_run_status_for_parent_rows(monkeypatch):
    from backend.api.routes import test_results as routes

    monkeypatch.setattr(routes.snowflake_pool, "get_default_pool", lambda: _StubPool())
    monkeypatch.setattr(routes.registry, "get", AsyncMock(return_value=None))

    payload = await routes.list_tests(status_filter="")
    assert payload["total_pages"] == 1
    assert len(payload["results"]) == 1
    row = payload["results"][0]
    assert row["status"] == "COMPLETED"
    assert row["phase"] == "COMPLETED"
