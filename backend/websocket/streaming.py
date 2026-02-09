"""
WebSocket streaming for real-time test metrics.
"""

import asyncio
import logging
from datetime import UTC, datetime
from typing import Any

from fastapi import WebSocket
from starlette.websockets import WebSocketState

from backend.core import results_store
from backend.core.live_controller_state import live_controller_state
from backend.core.live_metrics_cache import live_metrics_cache

from .helpers import _parse_variant_dict
from .metrics import (
    aggregate_multi_worker_metrics,
    build_aggregate_metrics,
    build_run_snapshot,
)
from .queries import (
    fetch_enrichment_progress,
    fetch_logs_for_tests,
    fetch_parent_enrichment_status,
    fetch_run_status,
    fetch_run_test_ids,
    fetch_warehouse_context,
    get_parent_test_status,
)

logger = logging.getLogger(__name__)


async def stream_run_metrics(websocket: WebSocket, test_id: str) -> None:
    """Stream real-time metrics for a test run over WebSocket."""
    await websocket.send_json(
        {
            "status": "connected",
            "test_id": test_id,
            "timestamp": datetime.now(UTC).isoformat(),
        }
    )

    poll_interval = 1.0
    last_sent_phase: str | None = None  # Track phase to ensure PROCESSING is shown
    last_log_seq: int = 0  # Track last log sequence to fetch only new logs
    last_log_seq_by_test: dict[str, int] = {str(test_id): 0}
    known_test_ids: list[str] = [str(test_id)]
    warehouse_name: str | None = None
    warehouse_table_type: str | None = None
    loop = asyncio.get_running_loop()
    last_poll_at: dict[str, float] = {}
    pending_tasks: dict[str, asyncio.Task[Any]] = {}
    cached_run_status: dict[str, Any] | None = None
    cached_parent_status: str | None = None
    cached_test_ids: list[str] = list(known_test_ids)
    cached_enrichment_status: str | None = None
    cached_enrichment_progress: dict[str, Any] | None = None
    cached_warehouse_details: dict[str, Any] | None = None
    warehouse_details_payload: dict[str, Any] | None = None
    logs_buffer: list[dict[str, Any]] = []

    # Fetch initial run_status synchronously before poll loop to ensure first
    # WebSocket message has the correct phase (avoids fallback to status="RUNNING")
    cached_run_status = await fetch_run_status(test_id)

    RUN_STATUS_TTL_TRANSITION = 0.5
    RUN_STATUS_TTL_ACTIVE = 1.0
    RUN_STATUS_TTL_TERMINAL = 5.0
    PARENT_STATUS_TTL = 5.0
    TEST_IDS_TTL = 10.0
    LOGS_TTL = 3.0
    ENRICHMENT_STATUS_TTL = 5.0
    ENRICHMENT_PROGRESS_TTL = 5.0
    WAREHOUSE_CONTEXT_TTL = 10.0
    WAREHOUSE_DETAILS_TTL = 30.0

    def _should_poll(key: str, ttl: float, now: float) -> bool:
        last = last_poll_at.get(key)
        return last is None or now - last >= ttl

    def _start_task(key: str, coro: Any) -> None:
        if key in pending_tasks:
            coro.close()
            return
        pending_tasks[key] = asyncio.create_task(coro)

    def _consume_task(key: str, now: float) -> tuple[Any | None, bool]:
        task = pending_tasks.get(key)
        if not task or not task.done():
            return None, False
        pending_tasks.pop(key, None)
        last_poll_at[key] = now
        try:
            return task.result(), True
        except Exception:
            return None, True

    while True:
        recv_task = asyncio.create_task(websocket.receive())
        sleep_task = asyncio.create_task(asyncio.sleep(poll_interval))
        done, pending = await asyncio.wait(
            {recv_task, sleep_task}, return_when=asyncio.FIRST_COMPLETED
        )
        for task in pending:
            task.cancel()

        if recv_task in done:
            msg = recv_task.result()
            if msg.get("type") == "websocket.disconnect":
                break
            continue

        if websocket.client_state != WebSocketState.CONNECTED:
            break

        now = loop.time()
        run_status_result, run_status_done = _consume_task("run_status", now)
        if run_status_done and run_status_result is not None:
            cached_run_status = run_status_result

        parent_status_result, parent_status_done = _consume_task("parent_status", now)
        if parent_status_done and parent_status_result:
            cached_parent_status = parent_status_result

        test_ids_result, test_ids_done = _consume_task("test_ids", now)
        if test_ids_done and test_ids_result:
            cached_test_ids = test_ids_result

        logs_result, logs_done = _consume_task("logs", now)
        if logs_done:
            logs_buffer = logs_result or []

        enrichment_status_result, enrichment_status_done = _consume_task(
            "enrichment_status", now
        )
        if enrichment_status_done and enrichment_status_result:
            cached_enrichment_status = enrichment_status_result

        enrichment_progress_result, enrichment_progress_done = _consume_task(
            "enrichment_progress", now
        )
        if enrichment_progress_done and enrichment_progress_result:
            cached_enrichment_progress = enrichment_progress_result

        warehouse_context_result, warehouse_context_done = _consume_task(
            "warehouse_context", now
        )
        if warehouse_context_done and isinstance(warehouse_context_result, tuple):
            context_warehouse, context_table_type = warehouse_context_result
            if context_warehouse or context_table_type:
                warehouse_name = context_warehouse
                warehouse_table_type = context_table_type

        warehouse_details_result, warehouse_details_done = _consume_task(
            "warehouse_details", now
        )
        if warehouse_details_done and warehouse_details_result:
            cached_warehouse_details = warehouse_details_result
            warehouse_details_payload = {
                "test_id": test_id,
                "warehouse": warehouse_details_result,
            }

        run_status = cached_run_status
        status = run_status.get("status") if run_status else None
        if not status:
            status = cached_parent_status
        status = status or "RUNNING"
        status_upper = str(status or "").upper()
        live_snapshot = await live_metrics_cache.get_run_snapshot(run_id=test_id)
        snapshot_test_ids: list[str] | None = None
        if live_snapshot:
            metrics = dict(live_snapshot.metrics)
            workers = list(live_snapshot.workers)
            snapshot_test_ids = list(live_snapshot.test_ids)
        else:
            metrics = await aggregate_multi_worker_metrics(test_id)
            metrics = metrics or {}
            workers = metrics.pop("workers", [])
        metrics_phase = metrics.pop("phase", None)
        phase = (
            run_status.get("phase")
            if run_status and run_status.get("phase")
            else metrics_phase
        )
        if not phase:
            phase = status_upper
        if status_upper in {"STOPPED", "FAILED", "CANCELLED", "COMPLETED"}:
            phase = status_upper
        # Show PROCESSING phase during worker draining (STOPPING) and enrichment
        if status_upper == "STOPPING":
            phase = "PROCESSING"
        if status_upper == "COMPLETED":
            enrichment_status = cached_enrichment_status
            # Treat None as PENDING to handle race condition where RUN_STATUS is
            # updated before TEST_RESULTS.ENRICHMENT_STATUS is set
            if enrichment_status in ("PENDING", None):
                phase = "PROCESSING"
            elif last_sent_phase not in {"PROCESSING", "COMPLETED"}:
                # Ensure PROCESSING is shown at least once before COMPLETED
                # even if enrichment finished before we polled
                phase = "PROCESSING"

        elapsed_seconds = None
        if run_status:
            elapsed_raw = run_status.get("elapsed_seconds")
            if isinstance(elapsed_raw, (int, float)):
                elapsed_seconds = float(elapsed_raw)
        if elapsed_seconds is None:
            elapsed_raw = metrics.get("elapsed")
            if isinstance(elapsed_raw, (int, float)):
                elapsed_seconds = float(elapsed_raw)

        ops = metrics.get("ops")
        latency = metrics.get("latency")
        errors = metrics.get("errors")
        connections = metrics.get("connections")
        operations = metrics.get("operations")
        aggregate_metrics = build_aggregate_metrics(
            ops=ops,
            latency=latency,
            errors=errors,
            connections=connections,
            operations=operations,
        )
        run_snapshot = build_run_snapshot(
            run_id=test_id,
            status=status_upper,
            phase=phase,
            elapsed_seconds=elapsed_seconds,
            worker_count=len(workers),
            aggregate_metrics=aggregate_metrics,
            run_status=run_status,
        )
        payload = {
            "test_id": test_id,
            "status": status_upper,
            "phase": phase,
            "timestamp": datetime.now(UTC).isoformat(),
            "run": run_snapshot,
            "workers": workers,
            **metrics,
        }
        if run_status and run_status.get("cancellation_reason"):
            payload["cancellation_reason"] = run_status.get("cancellation_reason")
        if run_status and run_status.get("failure_reason"):
            payload["error"] = {
                "type": "setup_error",
                "message": run_status.get("failure_reason"),
            }
        # Warehouse MCW data comes from orchestrator's WAREHOUSE_POLL_SNAPSHOTS, not workers
        warehouse_snapshot = await results_store.fetch_latest_warehouse_poll_snapshot(
            run_id=test_id
        )
        if warehouse_snapshot:
            payload["warehouse"] = warehouse_snapshot
        if elapsed_seconds is not None:
            payload["elapsed"] = float(elapsed_seconds)
            timing = {"elapsed_display_seconds": round(float(elapsed_seconds), 1)}
            run_snapshot["timing"] = timing
            payload["timing"] = timing
        find_max_state_db = (
            _parse_variant_dict(run_status.get("find_max_state"))
            if run_status
            else None
        )
        find_max_state_live_mem = await live_controller_state.get_find_max_state(
            run_id=test_id
        )
        find_max_state = (
            find_max_state_live_mem
            if find_max_state_live_mem is not None
            else find_max_state_db
        )
        # Debug: Log find_max sources
        fmc_from_live = None
        custom_metrics = metrics.get("custom_metrics") if metrics else None
        if custom_metrics and isinstance(custom_metrics, dict):
            fmc_from_live = custom_metrics.get("find_max_controller")
        if find_max_state_db or find_max_state_live_mem or fmc_from_live:
            db_step = (
                find_max_state_db.get("current_step") if find_max_state_db else None
            )
            db_target = (
                find_max_state_db.get("target_workers") if find_max_state_db else None
            )
            db_end_ms = (
                find_max_state_db.get("step_end_at_epoch_ms")
                if find_max_state_db
                else None
            )
            mem_step = (
                find_max_state_live_mem.get("current_step")
                if find_max_state_live_mem
                else None
            )
            mem_target = (
                find_max_state_live_mem.get("target_workers")
                if find_max_state_live_mem
                else None
            )
            mem_end_ms = (
                find_max_state_live_mem.get("step_end_at_epoch_ms")
                if find_max_state_live_mem
                else None
            )
            live_step = fmc_from_live.get("current_step") if fmc_from_live else None
            live_target = fmc_from_live.get("target_workers") if fmc_from_live else None
            live_end_ms = fmc_from_live.get("step_end_at_epoch_ms") if fmc_from_live else None
            logger.info(
                "[WS] find_max sources: DB(step=%s,target=%s,end_ms=%s) MEM(step=%s,target=%s,end_ms=%s) LIVE(step=%s,target=%s,end_ms=%s)",
                db_step,
                db_target,
                db_end_ms,
                mem_step,
                mem_target,
                mem_end_ms,
                live_step,
                live_target,
                live_end_ms,
            )
        if find_max_state is not None:
            payload["find_max"] = find_max_state

        # QPS controller state (for FIXED/BOUNDED modes with target QPS)
        qps_controller_state_db = (
            _parse_variant_dict(run_status.get("qps_controller_state"))
            if run_status
            else None
        )
        qps_controller_state_live_mem = await live_controller_state.get_qps_state(
            run_id=test_id
        )
        qps_controller_state = (
            qps_controller_state_live_mem
            if qps_controller_state_live_mem is not None
            else qps_controller_state_db
        )
        if qps_controller_state is not None:
            payload["qps_controller_state"] = qps_controller_state

        if cached_test_ids:
            known_test_ids = cached_test_ids
        else:
            known_test_ids = [str(test_id)]
        if snapshot_test_ids:
            known_test_ids = snapshot_test_ids
        for tid in known_test_ids:
            last_log_seq_by_test.setdefault(tid, 0)

        # Fetch new logs since last sequence (all child tests)
        new_logs = logs_buffer
        logs_buffer = []
        if new_logs:
            payload["logs"] = new_logs
            last_log_seq = max(last_log_seq, last_log_seq_by_test.get(str(test_id), 0))

        phase_upper = str(phase or "").upper()
        if status_upper == "COMPLETED" or phase_upper == "PROCESSING":
            enrichment_progress = cached_enrichment_progress
            if enrichment_progress:
                payload["enrichment_progress"] = enrichment_progress
        is_postgres_table = warehouse_table_type == "postgres"
        if warehouse_details_payload:
            payload["warehouse_details"] = warehouse_details_payload
            warehouse_details_payload = None

        is_terminal_status = status_upper in {
            "STOPPED",
            "FAILED",
            "CANCELLED",
            "COMPLETED",
        }
        is_transition_status = status_upper in {
            "STARTING",
            "READY",
            "STOPPING",
            "CANCELLING",
        } or phase_upper in {"PREPARING", "PROCESSING"}
        run_status_ttl = (
            RUN_STATUS_TTL_TRANSITION
            if is_transition_status
            else RUN_STATUS_TTL_TERMINAL
            if is_terminal_status
            else RUN_STATUS_TTL_ACTIVE
        )
        if _should_poll("run_status", run_status_ttl, now):
            _start_task("run_status", fetch_run_status(test_id))
        if (run_status is None or not run_status.get("status")) and _should_poll(
            "parent_status", PARENT_STATUS_TTL, now
        ):
            _start_task("parent_status", get_parent_test_status(test_id))
        if _should_poll("test_ids", TEST_IDS_TTL, now):
            _start_task("test_ids", fetch_run_test_ids(test_id))
        if known_test_ids and _should_poll("logs", LOGS_TTL, now):
            _start_task(
                "logs", fetch_logs_for_tests(known_test_ids, last_log_seq_by_test)
            )
        if status_upper == "COMPLETED" and _should_poll(
            "enrichment_status", ENRICHMENT_STATUS_TTL, now
        ):
            _start_task("enrichment_status", fetch_parent_enrichment_status(test_id))
        if (
            status_upper == "COMPLETED" or phase_upper == "PROCESSING"
        ) and _should_poll("enrichment_progress", ENRICHMENT_PROGRESS_TTL, now):
            _start_task("enrichment_progress", fetch_enrichment_progress(test_id))
        if warehouse_name is None and _should_poll(
            "warehouse_context", WAREHOUSE_CONTEXT_TTL, now
        ):
            _start_task("warehouse_context", fetch_warehouse_context(test_id))
        if (
            warehouse_name
            and not is_postgres_table
            and phase_upper in {"WARMUP", "MEASUREMENT", "RUNNING"}
            and (
                cached_warehouse_details is None
                or _should_poll("warehouse_details", WAREHOUSE_DETAILS_TTL, now)
            )
        ):
            _start_task(
                "warehouse_details",
                results_store.fetch_warehouse_config_snapshot(warehouse_name),
            )

        await websocket.send_json({"event": "RUN_UPDATE", "data": payload})
        last_sent_phase = phase  # Track what we sent
        # Only break when phase reaches COMPLETED (cleanup finished).
        # For FAILED/CANCELLED: orchestrator continues through PROCESSING phase
        # to drain workers, flush logs, and run enrichment before setting phase=COMPLETED.
        if phase == "COMPLETED":
            # Grace period to ensure frontend receives final message before socket closes
            await asyncio.sleep(0.5)
            break
