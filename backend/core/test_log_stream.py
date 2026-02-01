"""
Per-test log capture and streaming helpers.

This module bridges Python logging records into structured "log events" that can be:
- streamed over WebSockets to the dashboard, and
- persisted to Snowflake (see TEST_RESULTS.TEST_LOGS).
"""

from __future__ import annotations

import asyncio
import logging
from contextvars import ContextVar
from datetime import UTC, datetime
from typing import Any, Optional
from uuid import uuid4

CURRENT_TEST_ID: ContextVar[Optional[str]] = ContextVar("CURRENT_TEST_ID", default=None)
CURRENT_WORKER_ID: ContextVar[Optional[str]] = ContextVar(
    "CURRENT_WORKER_ID", default=None
)


class TestLogQueueHandler(logging.Handler):
    """
    Logging handler that emits structured per-test log events into an asyncio.Queue.

    The current test is identified via the CURRENT_TEST_ID contextvar. This allows
    per-test isolation even when multiple tests execute concurrently.
    """

    def __init__(
        self,
        *,
        test_id: str,
        queue: asyncio.Queue,
        max_message_chars: int = 20_000,
    ) -> None:
        super().__init__(level=logging.NOTSET)
        self._test_id = test_id
        self._queue = queue
        self._loop = asyncio.get_running_loop()
        self._seq = 0
        self._max_message_chars = max_message_chars
        self._exc_formatter = logging.Formatter()

    def emit(self, record: logging.LogRecord) -> None:
        try:
            # Allow internal components to log to the app console without also
            # duplicating into the per-test dashboard log stream.
            if bool(getattr(record, "skip_test_log_stream", False)):
                return

            current = CURRENT_TEST_ID.get()
            if current != self._test_id:
                return

            self._seq += 1
            ts = datetime.now(UTC).isoformat()

            msg = record.getMessage()
            if msg and len(msg) > self._max_message_chars:
                msg = msg[: self._max_message_chars] + "…[truncated]"

            exc_text: Optional[str] = None
            if record.exc_info:
                try:
                    exc_text = self._exc_formatter.formatException(record.exc_info)
                except Exception:
                    exc_text = None

            worker_id = getattr(record, "worker_id", None)
            if worker_id is None:
                worker_id = CURRENT_WORKER_ID.get()
            if worker_id is not None:
                worker_id = str(worker_id).strip()
            if not worker_id:
                worker_id = None

            event: dict[str, Any] = {
                "kind": "log",
                "log_id": str(uuid4()),
                "test_id": self._test_id,
                "seq": self._seq,
                "timestamp": ts,
                "level": str(record.levelname),
                "logger": str(record.name),
                "message": msg,
                "exception": exc_text,
                "worker_id": worker_id,
            }

            def _put() -> None:
                try:
                    self._queue.put_nowait(event)
                except asyncio.QueueFull:
                    # Drop logs if UI/db can't keep up; avoid blocking test execution.
                    return

            # Be safe even if logging occurs off-thread (e.g. connector internals).
            self._loop.call_soon_threadsafe(_put)
        except Exception:
            # Never allow logging failures to crash execution.
            return
