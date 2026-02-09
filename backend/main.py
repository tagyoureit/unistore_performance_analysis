"""
Unistore Benchmark - Main Application Entry Point

FastAPI application with real-time WebSocket support for database performance benchmarking.
"""

from contextlib import asynccontextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import asyncio
import json
from fastapi import FastAPI, Request, WebSocket, WebSocketDisconnect
from starlette.websockets import WebSocketState
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, RedirectResponse
import logging

from backend.config import settings
from backend.core import results_store
from backend.core.live_metrics_cache import live_metrics_cache
from backend.core.test_registry import registry
from backend.core.connection_manager import is_using_default_encryption_key
from backend.connectors import snowflake_pool

# Configure logging
# **IMPORTANT**: Use uvicorn's colored "LEVEL:" format for ALL loggers.
# This provides consistent colored output like "INFO:    Application startup complete."
from uvicorn.logging import DefaultFormatter

console_handler = logging.StreamHandler()
console_handler.setFormatter(DefaultFormatter(fmt="%(levelprefix)s %(asctime)s - %(message)s", use_colors=True))

logging.basicConfig(
    level=getattr(logging, settings.LOG_LEVEL),
    handlers=[console_handler],
)

# Suppress verbose Snowflake connector internal logging (connection handshake details)
logging.getLogger("snowflake.connector.connection").setLevel(logging.WARNING)
logging.getLogger("snowflake.connector.network").setLevel(logging.WARNING)


class EndpointFilter(logging.Filter):
    """Filter out high-frequency endpoints from access logs."""

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        if "/metrics/live" in msg:
            return False
        return True


logging.getLogger("uvicorn.access").addFilter(EndpointFilter())

logger = logging.getLogger(__name__)

# Snowsight link support (derived from Snowflake session context).
_snowsight_org_account_path: str | None = None
_snowsight_org_account_lock = asyncio.Lock()


async def _get_snowsight_org_account_path() -> str:
    """
    Resolve the Snowsight URL path segment: "<ORG>/<ACCOUNT>".

    This is fetched from Snowflake once per process (cached) using:
      SELECT CURRENT_ORGANIZATION_NAME() || '/' || CURRENT_ACCOUNT_NAME();
    """
    global _snowsight_org_account_path

    if _snowsight_org_account_path is not None:
        return _snowsight_org_account_path

    async with _snowsight_org_account_lock:
        if _snowsight_org_account_path is not None:
            return _snowsight_org_account_path

        try:
            from backend.connectors import snowflake_pool

            pool = snowflake_pool.get_default_pool()
            rows = await pool.execute_query(
                "SELECT CURRENT_ORGANIZATION_NAME() || '/' || CURRENT_ACCOUNT_NAME()"
            )
            value = str(rows[0][0]).strip() if rows and rows[0] and rows[0][0] else ""
            _snowsight_org_account_path = value
        except Exception as e:
            logger.debug("Failed to resolve Snowsight org/account: %s", e)
            _snowsight_org_account_path = ""

        return _snowsight_org_account_path


# Base directory for templates and static files
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"


async def _cleanup_orphaned_tests_background() -> None:
    """
    Background task to clean up orphaned tests on startup.

    Runs asynchronously so it doesn't block app startup. Tests stuck in
    PREPARED, STARTING, RUNNING, or CANCELLING states (with no recent activity)
    are marked as CANCELLED.

    Also handles stale enrichment:
    - Parent tests with pending enrichment: retry enrichment
    - Child tests or non-completed: mark enrichment as SKIPPED
    """
    try:
        from backend.core.results_store import cleanup_orphaned_tests, cleanup_stale_enrichment

        logger.info("🧹 Checking for orphaned tests...")
        result = await cleanup_orphaned_tests(
            stale_minutes_prepared=60,  # PREPARED/STARTING older than 1 hour
            stale_minutes_active=10,    # RUNNING/CANCELLING with no activity for 10 min
        )

        total_cleaned = result.run_status_cleaned + result.test_results_cleaned
        if total_cleaned > 0:
            logger.info(
                "🧹 Orphan cleanup complete: %d runs, %d tests marked as CANCELLED",
                result.run_status_cleaned,
                result.test_results_cleaned,
            )
        else:
            logger.info("🧹 No orphaned tests found")

        # Clean up stale enrichment
        logger.info("🔄 Checking for stale enrichment...")
        retried, skipped, retried_ids, skipped_ids = await cleanup_stale_enrichment(
            stale_minutes=60,  # Enrichment pending for more than 1 hour
        )
        if retried > 0 or skipped > 0:
            logger.info(
                "🔄 Stale enrichment: %d retrying, %d skipped",
                retried,
                skipped,
            )
        else:
            logger.info("🔄 No stale enrichment found")

    except Exception as e:
        # Don't fail startup if cleanup fails - just log and continue
        logger.warning("⚠️  Orphan cleanup failed (non-fatal): %s", e)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan manager - handles startup and shutdown events.
    """
    # Startup
    logger.info("🚀 FlakeBench starting up...")
    logger.info(f"📁 Templates directory: {TEMPLATES_DIR}")
    logger.info(f"📁 Static files directory: {STATIC_DIR}")
    logger.info(
        f"🔧 Environment: {'Development' if settings.APP_DEBUG else 'Production'}"
    )

    # Initialize database connection pools
    try:
        from backend.connectors import snowflake_pool, postgres_pool
        from backend.core.cost_calculator import load_postgres_instances

        logger.info("📊 Initializing Snowflake connection pool...")
        sf_pool = snowflake_pool.get_default_pool()
        await sf_pool.initialize()
        logger.info("✅ Snowflake pool initialized")

        # Load Postgres instance info for cost calculations
        try:
            instances = await load_postgres_instances()
            if instances:
                logger.info(f"💰 Loaded {len(instances)} Postgres instance sizes for cost calculations")
        except Exception as e:
            logger.warning(f"⚠️  Could not load Postgres instances for cost calculations: {e}")

        # Schedule orphan cleanup as background task (non-blocking)
        asyncio.create_task(_cleanup_orphaned_tests_background())

        if settings.ENABLE_POSTGRES and settings.POSTGRES_CONNECT_ON_STARTUP:
            logger.info("🐘 Initializing Postgres connection pool...")
            pg_pool = postgres_pool.get_default_pool()
            await pg_pool.initialize()
            logger.info("✅ Postgres pool initialized")
        elif settings.ENABLE_POSTGRES:
            logger.info(
                "🐘 Postgres enabled but not connecting on startup "
                "(set POSTGRES_CONNECT_ON_STARTUP=true to initialize at boot)"
            )

    except Exception as e:
        logger.error(f"❌ Failed to initialize connection pools: {e}")
        logger.warning("⚠️  Application starting without database connections")

    # TODO: Load test templates from config

    yield

    # Shutdown
    logger.info("🛑 Unistore Benchmark shutting down...")

    # Cancel in-flight benchmark tasks (important for `--reload`)
    try:
        await registry.shutdown(timeout_seconds=5.0)
    except Exception as e:
        logger.warning("Registry shutdown encountered an error: %s", e)

    # Close database connections
    try:
        from backend.connectors import snowflake_pool, postgres_pool

        logger.info("Closing database connection pools...")
        await snowflake_pool.close_telemetry_pool()
        await snowflake_pool.close_default_pool()
        await postgres_pool.close_all_pools()
        logger.info("✅ All connection pools closed")

    except Exception as e:
        logger.error(f"Error closing connection pools: {e}")

    # TODO: Clean up temporary files


# Initialize FastAPI application
app = FastAPI(
        title="FlakeBench",
    description="Performance benchmarking tool for Snowflake and Postgres - 3DMark for databases",
    version="0.1.0",
    lifespan=lifespan,
    docs_url="/api/docs",
    redoc_url="/api/redoc",
)

# Configure CORS for local development
if settings.APP_DEBUG:
    app.add_middleware(
        cast(Any, CORSMiddleware),
        allow_origins=settings.CORS_ORIGINS,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    logger.info(f"🔓 CORS enabled for origins: {settings.CORS_ORIGINS}")

# Mount static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Initialize Jinja2 templates
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Add custom template filters/functions
templates.env.globals.update(
    {
        "app_name": "Unistore Benchmark",
        "app_version": "0.1.0",
    }
)


# ============================================================================
# Health Check & Info Endpoints
# ============================================================================


@app.get("/", response_class=HTMLResponse, include_in_schema=False)
async def root(request: Request):
    """
    Root endpoint - renders templates page (tests are run from templates).
    """
    is_htmx = request.headers.get("HX-Request") == "true"
    template = "pages/templates.html" if not is_htmx else "pages/templates.html"
    return templates.TemplateResponse(
        template, {"request": request, "is_htmx": is_htmx}
    )


@app.get("/dashboard", response_class=HTMLResponse, include_in_schema=False)
async def dashboard(request: Request):
    """
    Dashboard page - real-time test metrics and control.
    """
    is_htmx = request.headers.get("HX-Request") == "true"
    template = "pages/dashboard.html" if not is_htmx else "pages/dashboard.html"
    return templates.TemplateResponse(
        template, {"request": request, "is_htmx": is_htmx}
    )


@app.get("/dashboard/{test_id}", response_class=HTMLResponse, include_in_schema=False)
async def dashboard_test(request: Request, test_id: str):
    """
    Dashboard page for a specific test - real-time test metrics and control.
    """
    # For terminal runs, prefer the read-only analysis view which includes the
    # final (post-processed) metrics. But do NOT redirect prepared/running tests,
    # since this route is used to start/monitor runs.
    history_url = f"/dashboard/history/{test_id}"
    running = await registry.get(test_id)

    # If not in memory, check database for PREPARED status (autoscale tests are
    # persisted to DB but not registered in memory until started).
    if running is None:
        try:
            pool = snowflake_pool.get_default_pool()
            rows = await pool.execute_query(
                f"""
                SELECT STATUS
                FROM {settings.RESULTS_DATABASE}.{settings.RESULTS_SCHEMA}.TEST_RESULTS
                WHERE TEST_ID = ?
                LIMIT 1
                """,
                params=[test_id],
            )
            if rows:
                db_status = str(rows[0][0] or "").upper()
                if db_status in {
                    "PREPARED",
                    "READY",
                    "PENDING",
                    "RUNNING",
                    "CANCELLING",
                }:
                    # Test exists in DB with a live status - serve live dashboard
                    is_htmx = request.headers.get("HX-Request") == "true"
                    template = "pages/dashboard.html"
                    return templates.TemplateResponse(
                        template,
                        {"request": request, "is_htmx": is_htmx, "test_id": test_id},
                    )
        except Exception:
            # Fall through to redirect on any DB error
            pass
        return RedirectResponse(url=history_url, status_code=302)

    status = str(getattr(running, "status", "") or "").upper()
    live_statuses = {
        "PREPARED",
        "READY",
        "PENDING",
        "RUNNING",
        "CANCELLING",
        "STARTING",
    }
    if status not in live_statuses:
        if request.headers.get("HX-Request") == "true":
            resp = HTMLResponse("")
            resp.headers["HX-Redirect"] = history_url
            return resp
        return RedirectResponse(url=history_url, status_code=302)

    is_htmx = request.headers.get("HX-Request") == "true"
    template = "pages/dashboard.html" if not is_htmx else "pages/dashboard.html"
    return templates.TemplateResponse(
        template, {"request": request, "is_htmx": is_htmx, "test_id": test_id}
    )


@app.get(
    "/dashboard/history/{test_id}", response_class=HTMLResponse, include_in_schema=False
)
async def dashboard_history_test(request: Request, test_id: str):
    """
    History dashboard page for a specific test - read-only analysis view.
    """
    is_htmx = request.headers.get("HX-Request") == "true"
    template = (
        "pages/dashboard_history.html"
        if not is_htmx
        else "pages/dashboard_history.html"
    )
    return templates.TemplateResponse(
        template, {"request": request, "is_htmx": is_htmx, "test_id": test_id}
    )


@app.get(
    "/dashboard/history/{test_id}/data",
    response_class=HTMLResponse,
    include_in_schema=False,
)
async def dashboard_history_data(request: Request, test_id: str):
    """
    Drilldown page for persisted per-operation query executions.

    Query params:
    - kinds: comma-separated QUERY_KIND list (e.g. POINT_LOOKUP,RANGE_SCAN)
    """
    is_htmx = request.headers.get("HX-Request") == "true"
    kinds = request.query_params.get("kinds", "") or ""
    snowsight_org_account_path = await _get_snowsight_org_account_path()
    template = (
        "pages/dashboard_history_data.html"
        if not is_htmx
        else "pages/dashboard_history_data.html"
    )
    return templates.TemplateResponse(
        template,
        {
            "request": request,
            "is_htmx": is_htmx,
            "test_id": test_id,
            "kinds": kinds,
            "snowsight_org_account_path": snowsight_org_account_path,
        },
    )


@app.get("/configure", response_class=HTMLResponse, include_in_schema=False)
async def configure(request: Request):
    """
    Test configuration page - design custom performance tests.
    """
    is_htmx = request.headers.get("HX-Request") == "true"
    template = "pages/configure.html" if not is_htmx else "pages/configure.html"
    return templates.TemplateResponse(
        template,
        {
            "request": request,
            "is_htmx": is_htmx,
            "default_database": settings.SNOWFLAKE_DATABASE,
        },
    )


@app.get("/comparison", response_class=HTMLResponse, include_in_schema=False)
async def comparison(request: Request):
    """
    Deprecated: comparison is now part of /history.
    """
    is_htmx = request.headers.get("HX-Request") == "true"
    if is_htmx:
        # HTMX doesn't always push the redirected URL the way we want; tell it to
        # navigate directly to the new location.
        return HTMLResponse("", headers={"HX-Redirect": "/history"})
    return RedirectResponse(url="/history", status_code=303)


@app.get("/history", response_class=HTMLResponse, include_in_schema=False)
async def history(request: Request):
    """
    Test history page - browse and manage previous test results.
    """
    is_htmx = request.headers.get("HX-Request") == "true"
    template = "pages/history.html" if not is_htmx else "pages/history.html"
    return templates.TemplateResponse(
        template, {"request": request, "is_htmx": is_htmx}
    )


@app.get("/history/compare", response_class=HTMLResponse, include_in_schema=False)
async def history_compare(request: Request):
    """
    Deep comparison view for two tests.

    Query params:
    - ids: comma-separated two TEST_ID values (UUIDs)
    """
    is_htmx = request.headers.get("HX-Request") == "true"
    ids_raw = (request.query_params.get("ids") or "").strip()
    ids_list = [p.strip() for p in ids_raw.split(",") if p and p.strip()]
    error = None
    if len(ids_list) != 2:
        error = "Provide exactly 2 test ids via ?ids=<id1>,<id2>."

    template = (
        "pages/history_compare.html" if not is_htmx else "pages/history_compare.html"
    )
    return templates.TemplateResponse(
        template,
        {
            "request": request,
            "is_htmx": is_htmx,
            "ids_raw": ids_raw,
            "ids": ids_list,
            "error": error,
        },
    )


@app.get("/templates", response_class=HTMLResponse, include_in_schema=False)
async def templates_page(request: Request):
    """
    Templates page - manage and reuse test configuration templates.
    """
    is_htmx = request.headers.get("HX-Request") == "true"
    template = "pages/templates.html" if not is_htmx else "pages/templates.html"
    return templates.TemplateResponse(
        template, {"request": request, "is_htmx": is_htmx}
    )


@app.get("/settings", response_class=HTMLResponse, include_in_schema=False)
async def settings_page(request: Request):
    """
    Settings page - manage database connections and application configuration.
    """
    is_htmx = request.headers.get("HX-Request") == "true"
    template = "pages/settings.html"
    return templates.TemplateResponse(
        template, {"request": request, "is_htmx": is_htmx}
    )


@app.get("/health")
async def health_check():
    """
    Health check endpoint for monitoring and load balancers.

    Returns:
        dict: Service health status and version information
    """
    health_status: dict[str, Any] = {
        "status": "healthy",
        "service": "flakebench",
        "version": "0.1.0",
        "environment": "development" if settings.APP_DEBUG else "production",
        "checks": {},
    }

    # Check Snowflake connection
    try:
        from backend.connectors import snowflake_pool

        sf_pool = snowflake_pool.get_default_pool()
        stats = await sf_pool.get_pool_stats()
        health_status["checks"]["snowflake"] = {
            "status": "healthy" if stats["initialized"] else "not_initialized",
            "pool": stats,
        }
    except Exception as e:
        health_status["checks"]["snowflake"] = {"status": "unhealthy", "error": str(e)}
        health_status["status"] = "degraded"

    # Check Postgres connection (if enabled)
    if settings.ENABLE_POSTGRES:
        try:
            from backend.connectors import postgres_pool

            pg_pool = postgres_pool.get_default_pool()
            stats = await pg_pool.get_pool_stats()
            is_healthy = await pg_pool.is_healthy()
            health_status["checks"]["postgres"] = {
                "status": "healthy" if is_healthy else "unhealthy",
                "pool": stats,
            }
        except Exception as e:
            health_status["checks"]["postgres"] = {
                "status": "unhealthy",
                "error": str(e),
            }

    return health_status


@app.get("/api/info")
async def api_info():
    """
    API information endpoint.

    Returns:
        dict: Application configuration and capabilities
    """
    # Build security warnings list
    security_warnings = []
    if is_using_default_encryption_key():
        security_warnings.append({
            "code": "DEFAULT_ENCRYPTION_KEY",
            "message": "Using default encryption key for credentials. Set FLAKEBENCH_CREDENTIAL_KEY for production.",
            "severity": "warning",
        })
    
    return {
        "name": "Unistore Benchmark",
        "version": "0.1.0",
        "description": "Performance benchmarking tool for Snowflake and Postgres",
        "results_warehouse": settings.SNOWFLAKE_WAREHOUSE,
        "features": {
            "table_types": ["standard", "hybrid", "interactive", "postgres"],
            "real_time_metrics": True,
            "max_comparisons": 5,
            "websocket_support": True,
        },
        "endpoints": {
            "api_docs": "/api/docs",
            "health": "/health",
            "dashboard": "/dashboard",
            "configure": "/configure",
            "history": "/history",
        },
        "security_warnings": security_warnings,
    }


# ============================================================================
# Import API Routes (will be created in next steps)
# ============================================================================

# Import and include API routers
from backend.api.routes import runs  # noqa: E402
from backend.api.routes import tests  # noqa: E402
from backend.api.routes import templates as templates_router  # noqa: E402
from backend.api.routes import warehouses  # noqa: E402
from backend.api.routes import catalog  # noqa: E402
from backend.api.routes import test_results  # noqa: E402
from backend.api.routes import connections  # noqa: E402

app.include_router(runs.router, prefix="/api/runs", tags=["runs"])
app.include_router(tests.router, prefix="/api/test", tags=["tests"])
app.include_router(templates_router.router, prefix="/api/templates", tags=["templates"])
app.include_router(warehouses.router, prefix="/api/warehouses", tags=["warehouses"])
app.include_router(catalog.router, prefix="/api/catalog", tags=["catalog"])
app.include_router(test_results.router, prefix="/api/tests", tags=["test_results"])
app.include_router(connections.router, prefix="/api/connections", tags=["connections"])

# TODO: Import additional routers as they're created
# from backend.api.routes import comparison, history
# app.include_router(comparison.router, prefix="/comparison", tags=["comparison"])
# app.include_router(history.router, prefix="/history", tags=["history"])


# ============================================================================
# WebSocket endpoint - uses websocket package for streaming implementation
# ============================================================================

from backend.websocket import stream_run_metrics


@app.websocket("/ws/test/{test_id}")
async def websocket_test_metrics(websocket: WebSocket, test_id: str):
    """
    WebSocket endpoint for real-time test metrics streaming.

    All runs now use the unified orchestrator-based streaming which polls
    RUN_STATUS and WORKER_METRICS_SNAPSHOTS for metrics.

    Args:
        websocket: WebSocket connection
        test_id: Unique test identifier (run_id)
    """
    await websocket.accept()
    logger.info(f"📡 WebSocket connected for test: {test_id}")

    try:
        await stream_run_metrics(websocket, test_id)
    except WebSocketDisconnect:
        logger.info(f"📡 WebSocket disconnected for test: {test_id}")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        try:
            await websocket.close()
        except Exception:
            pass


if __name__ == "__main__":
    import uvicorn

    # **IMPORTANT**: log_config=None prevents uvicorn from overriding our logging setup.
    # Without this, uvicorn applies its default config which uses "INFO:" format.
    uvicorn.run(
        "backend.main:app",
        host=settings.APP_HOST,
        port=settings.APP_PORT,
        reload=settings.APP_RELOAD,
        log_level=settings.LOG_LEVEL.lower(),
        log_config=None,
    )
