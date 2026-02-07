"""Consolidated worker pool management for dynamic scaling.

Consolidates duplicate worker pool implementations from:
- test_executor.py:1250-1375 (QPS mode)
- test_executor.py:2038-2091 (FMC mode)
- scripts/run_worker.py:1165-1211
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any, Callable, Coroutine

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class WorkerPool:
    """Manages a pool of async worker tasks with dynamic scaling.

    This class consolidates the worker management pattern used across
    QPS mode, FIND_MAX_CONCURRENCY mode, and headless worker execution.

    Attributes:
        min_workers: Minimum number of workers to maintain
        max_workers: Maximum number of workers allowed
    """

    def __init__(
        self,
        *,
        worker_factory: Callable[[int, bool, asyncio.Event], Coroutine[Any, Any, None]],
        min_workers: int = 1,
        max_workers: int = 100,
        use_lock: bool = False,
        on_workers_changed: Callable[[], None] | None = None,
    ) -> None:
        """Initialize the worker pool.

        Args:
            worker_factory: Async function that creates a worker task.
                           Signature: (worker_id: int, warmup: bool, stop_signal: Event) -> Coroutine
            min_workers: Minimum number of workers to maintain
            max_workers: Maximum number of workers allowed
            use_lock: Whether to use an asyncio.Lock for thread safety
            on_workers_changed: Optional callback when workers list changes
        """
        self._worker_factory = worker_factory
        self.min_workers = max(0, min_workers)
        self.max_workers = max(self.min_workers, max_workers)
        self._scale_lock = asyncio.Lock() if use_lock else None
        self._on_workers_changed = on_workers_changed

        self._worker_tasks: dict[int, tuple[asyncio.Task[None], asyncio.Event]] = {}
        self._next_worker_id = 0
        self._target_workers = 0

    @property
    def count(self) -> int:
        """Current number of live (not done) workers."""
        return len(self.live_worker_ids())

    @property
    def target(self) -> int:
        """Current target worker count."""
        return self._target_workers

    def running_worker_ids(self) -> list[int]:
        """Get IDs of workers that are running and not stop-signaled.

        These workers are actively processing work.
        """
        out: list[int] = []
        for wid, (task, stop_signal) in self._worker_tasks.items():
            if task.done():
                continue
            if stop_signal.is_set():
                continue
            out.append(int(wid))
        return out

    def live_worker_ids(self) -> list[int]:
        """Get IDs of all workers that haven't completed yet.

        Includes stop-signaled workers that are still finishing.
        """
        out: list[int] = []
        for wid, (task, _) in self._worker_tasks.items():
            if task.done():
                continue
            out.append(int(wid))
        return out

    def prune_completed(self) -> None:
        """Remove completed worker tasks from the pool."""
        for wid, (task, _) in list(self._worker_tasks.items()):
            if task.done():
                self._worker_tasks.pop(wid, None)

    def _notify_changed(self) -> None:
        """Notify that workers list has changed."""
        if self._on_workers_changed is not None:
            self._on_workers_changed()

    async def spawn_one(self, *, warmup: bool) -> int:
        """Spawn a single new worker.

        Args:
            warmup: Whether the worker should start in warmup mode

        Returns:
            The worker ID of the spawned worker
        """
        wid = int(self._next_worker_id)
        self._next_worker_id += 1
        stop_signal = asyncio.Event()
        task = asyncio.create_task(
            self._worker_factory(wid, warmup, stop_signal)
        )
        self._worker_tasks[wid] = (task, stop_signal)
        self._notify_changed()
        return wid

    async def scale_to(
        self,
        target: int,
        *,
        warmup: bool,
        prewarm_callback: Callable[[int], Coroutine[Any, Any, None]] | None = None,
    ) -> None:
        """Scale the worker pool to a target size.

        Args:
            target: Desired number of workers
            warmup: Whether new workers should start in warmup mode
            prewarm_callback: Optional async callback to prewarm resources (e.g., connection pool)
        """
        if self._scale_lock:
            async with self._scale_lock:
                await self._scale_to_impl(
                    target, warmup=warmup, prewarm_callback=prewarm_callback
                )
        else:
            await self._scale_to_impl(
                target, warmup=warmup, prewarm_callback=prewarm_callback
            )

    async def _scale_to_impl(
        self,
        target: int,
        *,
        warmup: bool,
        prewarm_callback: Callable[[int], Coroutine[Any, Any, None]] | None = None,
    ) -> None:
        """Internal implementation of scale_to."""
        self.prune_completed()
        target = int(target)
        target = max(self.min_workers, min(self.max_workers, target))
        self._target_workers = target

        running_ids = sorted(self.running_worker_ids())
        running = len(running_ids)
        live = len(self.live_worker_ids())

        # Scale up:
        # - Never exceed max_workers by counting ALL live tasks,
        #   including stop-signaled ones that are still finishing.
        if running < target:
            logger.info(
                "[WorkerPool] Scale up: running=%d < target=%d, prewarm_callback=%s",
                running, target, "present" if prewarm_callback else "None"
            )
            if prewarm_callback is not None:
                await prewarm_callback(target)
            spawn_n = min((target - running), max(0, target - live))
            for _ in range(spawn_n):
                await self.spawn_one(warmup=warmup)

        # Scale down:
        # - Only mark currently running workers to stop; workers already
        #   stop-signaled still occupy capacity until they fully exit.
        elif running > target:
            stop_n = running - target
            stop_ids = list(reversed(running_ids))[:stop_n]
            for wid in stop_ids:
                entry = self._worker_tasks.get(wid)
                if entry is not None:
                    _, stop_signal = entry
                    stop_signal.set()
            self._notify_changed()

    async def stop_all(self, *, timeout_seconds: float = 2.0) -> None:
        """Stop all workers gracefully.

        Args:
            timeout_seconds: Maximum time to wait for workers to stop
        """
        if self._scale_lock:
            async with self._scale_lock:
                await self._stop_all_impl(timeout_seconds=timeout_seconds)
        else:
            await self._stop_all_impl(timeout_seconds=timeout_seconds)

    async def _stop_all_impl(self, *, timeout_seconds: float) -> None:
        """Internal implementation of stop_all."""
        self.prune_completed()

        # Signal all workers to stop
        for _, stop_signal in self._worker_tasks.values():
            stop_signal.set()
        self._notify_changed()

        tasks = [t for t, _ in self._worker_tasks.values()]
        if not tasks:
            self._worker_tasks.clear()
            return

        # Wait for workers to finish (never cancel - may leave threads running)
        try:
            await asyncio.wait_for(
                asyncio.gather(*tasks, return_exceptions=True),
                timeout=timeout_seconds,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "Timed out waiting for workers to stop after %.1fs", timeout_seconds
            )

        self._worker_tasks.clear()
        self._notify_changed()

    def get_tasks(self) -> list[asyncio.Task[None]]:
        """Get list of all worker tasks (for external monitoring)."""
        return [t for t, _ in self._worker_tasks.values() if not t.done()]
