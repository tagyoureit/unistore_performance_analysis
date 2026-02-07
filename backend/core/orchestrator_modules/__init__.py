"""
Orchestrator Package

This package contains the orchestration service for benchmark runs,
split into logical modules for better maintainability.

Modules:
- models: Data classes (RunContext)
- utils: Utility functions (build_worker_targets, stream_worker_output)
- preflight: Pre-flight warning generation
- qps_controller: QPS-based thread scaling for orchestrator

The main OrchestratorService class remains in the parent module
for backward compatibility.
"""

from .models import RunContext
from .utils import (
    uv_available,
    build_worker_targets,
    stream_worker_output,
)
from .preflight import generate_preflight_warnings
from .qps_controller import (
    QPSControllerState,
    QPSScaleDecision,
    compute_desired_threads,
    evaluate_qps_scaling,
    distribute_threads_to_workers,
)

__all__ = [
    # Models
    "RunContext",
    # Utilities
    "uv_available",
    "build_worker_targets",
    "stream_worker_output",
    # Preflight
    "generate_preflight_warnings",
    # QPS Controller
    "QPSControllerState",
    "QPSScaleDecision",
    "compute_desired_threads",
    "evaluate_qps_scaling",
    "distribute_threads_to_workers",
]
