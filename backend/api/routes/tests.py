"""
API routes for test management and execution.
"""

from fastapi import APIRouter
from pydantic import BaseModel
from uuid import uuid4
from datetime import datetime, UTC

router = APIRouter()


class TestStartRequest(BaseModel):
    """Request to start a new test"""

    duration: int = 300
    concurrent_connections: int = 10
    workload_type: str = "CUSTOM"
    table_type: str = "STANDARD"


class TestStartResponse(BaseModel):
    """Response when test is started"""

    test_id: str
    status: str
    started_at: str


@router.post("/start", response_model=TestStartResponse)
async def start_test(request: TestStartRequest):
    """
    Start a new performance test.
    """
    test_id = str(uuid4())

    # TODO: Actually create and start test executor
    # For now, just return a test ID

    return TestStartResponse(
        test_id=test_id, status="running", started_at=datetime.now(UTC).isoformat()
    )


@router.post("/create", response_model=TestStartResponse)
async def create_test(request: dict):
    """
    Create a new performance test from configuration.

    This endpoint is called from the Configure page.
    """
    test_id = str(uuid4())

    # TODO: Actually create and start test executor with full config
    # For now, just return a test ID

    return TestStartResponse(
        test_id=test_id, status="created", started_at=datetime.now(UTC).isoformat()
    )


@router.post("/{test_id}/pause")
async def pause_test(test_id: str):
    """
    Pause a running test.
    """
    # TODO: Implement pause logic
    return {"test_id": test_id, "status": "paused"}


@router.post("/{test_id}/stop")
async def stop_test(test_id: str):
    """
    Stop a running test.
    """
    # TODO: Implement stop logic
    return {"test_id": test_id, "status": "stopped"}


@router.get("/{test_id}")
async def get_test_status(test_id: str):
    """
    Get current status of a test.
    """
    # TODO: Implement real status lookup
    return {"test_id": test_id, "status": "running", "duration": 300, "elapsed": 45}
