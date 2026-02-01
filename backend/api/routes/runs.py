"""
API routes for orchestrator-backed run control.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from backend.api.error_handling import http_exception
from backend.core.orchestrator import orchestrator
from backend.core.test_registry import registry

router = APIRouter()


class RunCreateRequest(BaseModel):
    template_id: str


class RunCreateResponse(BaseModel):
    run_id: str
    status: str
    dashboard_url: str


class RunActionResponse(BaseModel):
    run_id: str
    status: str


@router.post("/", response_model=RunCreateResponse, status_code=status.HTTP_201_CREATED)
async def create_run(request: RunCreateRequest) -> RunCreateResponse:
    """
    Create a new orchestrator-backed run (PREPARED).
    """
    try:
        template = await registry._load_template(request.template_id)
        template_config = dict(template.get("config") or {})
        template_name = str(template.get("template_name") or "")
        scenario = registry._scenario_from_template_config(
            template_name, template_config
        )
        run_id = await orchestrator.create_run(
            template_id=str(template.get("template_id") or request.template_id),
            template_config=template_config,
            scenario=scenario,
        )
        return RunCreateResponse(
            run_id=run_id,
            status="PREPARED",
            dashboard_url=f"/dashboard/{run_id}",
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="Template not found")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise http_exception("create run", e)


@router.post(
    "/{run_id}/start",
    response_model=RunActionResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def start_run(run_id: str) -> RunActionResponse:
    """
    Start a prepared run via the orchestrator.
    """
    try:
        await orchestrator.start_run(run_id=run_id)
        status_row = await orchestrator.get_run_status(run_id)
        status_val = (
            str(status_row.get("status") or "").upper()
            if status_row is not None
            else "RUNNING"
        )
        return RunActionResponse(run_id=run_id, status=status_val)
    except ValueError:
        raise HTTPException(status_code=404, detail="Run not found")
    except Exception as e:
        raise http_exception("start run", e)


@router.post(
    "/{run_id}/stop",
    response_model=RunActionResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
async def stop_run(run_id: str) -> RunActionResponse:
    """
    Stop a run via the orchestrator.
    """
    try:
        await orchestrator.stop_run(run_id=run_id)
        status_row = await orchestrator.get_run_status(run_id)
        status_val = (
            str(status_row.get("status") or "").upper()
            if status_row is not None
            else "CANCELLING"
        )
        return RunActionResponse(run_id=run_id, status=status_val)
    except ValueError:
        raise HTTPException(status_code=404, detail="Run not found")
    except Exception as e:
        raise http_exception("stop run", e)
