"""
In-memory live controller state cache.

Used to expose orchestrator controller state (FIND_MAX/QPS) to WebSocket clients
without waiting for database round-trips.
"""

from __future__ import annotations

import asyncio
from typing import Any


class LiveControllerStateCache:
    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._find_max_by_run: dict[str, dict[str, Any]] = {}
        self._qps_by_run: dict[str, dict[str, Any]] = {}

    async def set_find_max_state(self, *, run_id: str, state: dict[str, Any]) -> None:
        rid = str(run_id).strip()
        if not rid:
            return
        incoming = dict(state or {})
        async with self._lock:
            current = self._find_max_by_run.get(rid)
            if current is not None and not _should_replace_find_max_state(
                current, incoming
            ):
                return
            self._find_max_by_run[rid] = incoming

    async def get_find_max_state(self, *, run_id: str) -> dict[str, Any] | None:
        rid = str(run_id).strip()
        if not rid:
            return None
        async with self._lock:
            state = self._find_max_by_run.get(rid)
            return dict(state) if isinstance(state, dict) else None

    async def set_qps_state(self, *, run_id: str, state: dict[str, Any]) -> None:
        rid = str(run_id).strip()
        if not rid:
            return
        async with self._lock:
            self._qps_by_run[rid] = dict(state or {})

    async def get_qps_state(self, *, run_id: str) -> dict[str, Any] | None:
        rid = str(run_id).strip()
        if not rid:
            return None
        async with self._lock:
            state = self._qps_by_run.get(rid)
            return dict(state) if isinstance(state, dict) else None

    async def clear_run(self, *, run_id: str) -> None:
        rid = str(run_id).strip()
        if not rid:
            return
        async with self._lock:
            self._find_max_by_run.pop(rid, None)
            self._qps_by_run.pop(rid, None)


live_controller_state = LiveControllerStateCache()


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except Exception:
        return None


def _find_max_status_rank(value: Any) -> int:
    status = str(value or "").upper()
    if status == "STEP_STARTING":
        return 1
    if status == "STEP_RUNNING":
        return 2
    if status == "TRANSITIONING":
        return 3
    if status == "STEP_COMPLETE":
        return 4
    if status == "COMPLETED":
        return 5
    return 0


def _should_replace_find_max_state(
    current: dict[str, Any], incoming: dict[str, Any]
) -> bool:
    current_step = _int_or_none(current.get("current_step"))
    incoming_step = _int_or_none(incoming.get("current_step"))
    if current_step is not None and incoming_step is not None:
        if incoming_step > current_step:
            return True
        if incoming_step < current_step:
            return False

    current_rank = _find_max_status_rank(current.get("status"))
    incoming_rank = _find_max_status_rank(incoming.get("status"))
    if incoming_rank > current_rank:
        return True
    if incoming_rank < current_rank:
        return False

    # Guard against regressions where an older same-step snapshot (often STEP_RUNNING)
    # overwrites a newer transition snapshot with a later countdown endpoint.
    current_end = _int_or_none(current.get("step_end_at_epoch_ms"))
    incoming_end = _int_or_none(incoming.get("step_end_at_epoch_ms"))
    if (
        current_end is not None
        and incoming_end is not None
        and incoming_end < current_end
        and str(current.get("status") or "").upper() in {"TRANSITIONING", "STEP_COMPLETE"}
        and str(incoming.get("status") or "").upper() in {"STEP_RUNNING", "STEP_COMPLETE"}
    ):
        return False

    return True
