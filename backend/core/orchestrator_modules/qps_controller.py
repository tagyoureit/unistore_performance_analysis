"""
QPS Controller for Orchestrator-Centric Scaling

Provides centralized QPS-based thread scaling for FIXED and BOUNDED modes.
The orchestrator monitors aggregate QPS from worker metrics and emits
SET_WORKER_TARGET events to scale threads up/down to meet the target QPS.

Algorithm ported from v1 test_executor._qps_controller with modifications
for orchestrator-centric operation (slower control loop due to Snowflake polling).
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any


@dataclass
class QPSControllerState:
    """Tracks QPS controller state across control loop iterations."""

    # Configuration
    target_qps: float = 0.0
    min_threads: int = 1
    max_threads: int = 100
    control_interval_seconds: float = 5.0

    # Deadband: ignore deviations smaller than this percentage of target
    deadband_pct: float = 0.05  # 5%

    # Streak counters for hysteresis (avoid reacting to transient spikes)
    under_target_streak: int = 0
    over_target_streak: int = 0

    # Required streak count before scaling
    # Lower threshold for scaling up (1) - we want to be responsive when under target
    # Higher threshold for scaling down (2) - avoid reacting to transient completion bursts
    streak_threshold_up: int = 1  # Scale up after 1 under-target reading
    streak_threshold_down: int = 2  # Scale down after 2 consecutive over-target

    # Rate limiting: max percentage change per control tick
    # Set to 25% to allow faster convergence (orchestrator loop is slow ~5-6s)
    max_change_pct: float = 0.25  # 25% max change per tick

    # Current state
    current_threads: int = 0
    last_observed_qps: float = 0.0
    last_scale_time: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize state for persistence/debugging."""
        return {
            "target_qps": float(self.target_qps),
            "min_threads": int(self.min_threads),
            "max_threads": int(self.max_threads),
            "control_interval_seconds": float(self.control_interval_seconds),
            "deadband_pct": float(self.deadband_pct),
            "under_target_streak": int(self.under_target_streak),
            "over_target_streak": int(self.over_target_streak),
            "current_threads": int(self.current_threads),
            "last_observed_qps": float(self.last_observed_qps),
            "last_scale_time": (
                self.last_scale_time.isoformat() if self.last_scale_time else None
            ),
        }


@dataclass
class QPSScaleDecision:
    """Result of QPS controller evaluation."""

    should_scale: bool = False
    new_target: int = 0
    reason: str = ""
    debug_info: dict[str, Any] = field(default_factory=dict)


def compute_desired_threads(
    *,
    current_threads: int,
    current_qps: float,
    target_qps: float,
    avg_latency_ms: float | None,
    min_threads: int,
    max_threads: int,
) -> int:
    """
    Compute desired thread count based on observed vs target QPS.

    Uses proportional control with variable gain based on error magnitude.
    Includes latency-based estimation as a secondary signal.

    Args:
        current_threads: Current number of threads
        current_qps: Observed QPS from metrics
        target_qps: Target QPS to achieve
        avg_latency_ms: Average latency in ms (optional, for estimation)
        min_threads: Minimum allowed threads
        max_threads: Maximum allowed threads

    Returns:
        Desired thread count (clamped to [min_threads, max_threads])
    """
    if current_threads <= 0:
        return min_threads

    if (
        target_qps is None
        or not math.isfinite(target_qps)
        or target_qps <= 0
    ):
        return current_threads

    # If no QPS observed, use latency-based estimation
    if (
        current_qps is None
        or not math.isfinite(current_qps)
        or current_qps <= 0
    ):
        if (
            avg_latency_ms
            and math.isfinite(avg_latency_ms)
            and avg_latency_ms > 0
        ):
            ops_per_thread = 1000.0 / avg_latency_ms
            theoretical = int(math.ceil(target_qps / ops_per_thread))
            return max(min_threads, min(max_threads, theoretical))
        return current_threads

    # Proportional control with variable gain
    error = target_qps - current_qps
    error_ratio = error / target_qps
    abs_error_ratio = abs(error_ratio)

    # Gain selection based on error magnitude
    # More aggressive when far from target, conservative when close
    # Increased gains from v1 to account for orchestrator's slower control loop
    if abs_error_ratio > 0.50:
        gain = 0.70  # Very aggressive when very far off (>50% error)
    elif abs_error_ratio > 0.30:
        gain = 0.55  # Aggressive when far off (>30% error)
    elif abs_error_ratio > 0.15:
        gain = 0.40
    elif abs_error_ratio > 0.05:
        gain = 0.30  # Still responsive in 5-15% range
    else:
        gain = 0.15  # Conservative near target (<5%)

    adjustment_ratio = error_ratio * gain
    desired_float = float(current_threads) * (1.0 + adjustment_ratio)

    # Use latency-based estimate as guidance when under target
    if (
        avg_latency_ms
        and math.isfinite(avg_latency_ms)
        and avg_latency_ms > 0
    ):
        ops_per_thread = 1000.0 / avg_latency_ms
        theoretical = target_qps / ops_per_thread
        if error_ratio > 0.10:  # Under target by >10%
            desired_float = max(desired_float, theoretical * 0.9)

    desired = int(round(desired_float))
    return max(min_threads, min(max_threads, desired))


def evaluate_qps_scaling(
    state: QPSControllerState,
    *,
    current_qps: float,
    current_threads: int,
    avg_latency_ms: float | None = None,
) -> QPSScaleDecision:
    """
    Evaluate whether to scale threads based on current QPS.

    Applies deadband and streak-based hysteresis to avoid thrashing.

    Args:
        state: Current controller state (will be mutated)
        current_qps: Observed aggregate QPS from worker metrics
        current_threads: Current total thread count across all workers
        avg_latency_ms: Average latency (optional, for estimation)

    Returns:
        QPSScaleDecision with scaling recommendation
    """
    decision = QPSScaleDecision()
    decision.debug_info = {
        "target_qps": state.target_qps,
        "current_qps": current_qps,
        "current_threads": current_threads,
        "min_threads": state.min_threads,
        "max_threads": state.max_threads,
    }

    # Update state tracking
    state.current_threads = current_threads
    state.last_observed_qps = current_qps

    # Validate inputs
    if state.target_qps <= 0:
        decision.reason = "no_target_qps"
        return decision

    if current_threads <= 0:
        decision.should_scale = True
        decision.new_target = state.min_threads
        decision.reason = "no_threads_running"
        return decision

    # Calculate deadband thresholds
    deadband = max(2.0, state.target_qps * state.deadband_pct)
    lower_bound = state.target_qps - deadband
    upper_bound = state.target_qps + deadband

    decision.debug_info["deadband"] = deadband
    decision.debug_info["lower_bound"] = lower_bound
    decision.debug_info["upper_bound"] = upper_bound

    # Check if within deadband (no action needed)
    if lower_bound <= current_qps <= upper_bound:
        state.under_target_streak = 0
        state.over_target_streak = 0
        decision.reason = "within_deadband"
        decision.debug_info["under_target_streak"] = 0
        decision.debug_info["over_target_streak"] = 0
        return decision

    # Determine if under or over target
    under_target = current_qps < lower_bound
    over_target = current_qps > upper_bound

    # Update streak counters
    if under_target:
        state.under_target_streak += 1
        state.over_target_streak = 0
    elif over_target:
        state.over_target_streak += 1
        state.under_target_streak = 0
    else:
        state.under_target_streak = 0
        state.over_target_streak = 0

    decision.debug_info["under_target"] = under_target
    decision.debug_info["over_target"] = over_target
    decision.debug_info["under_target_streak"] = state.under_target_streak
    decision.debug_info["over_target_streak"] = state.over_target_streak

    # Check if streak threshold met
    if under_target and state.under_target_streak < state.streak_threshold_up:
        decision.reason = "under_target_streak_not_met"
        return decision

    if over_target and state.over_target_streak < state.streak_threshold_down:
        decision.reason = "over_target_streak_not_met"
        return decision

    # Compute desired threads
    desired = compute_desired_threads(
        current_threads=current_threads,
        current_qps=current_qps,
        target_qps=state.target_qps,
        avg_latency_ms=avg_latency_ms,
        min_threads=state.min_threads,
        max_threads=state.max_threads,
    )

    decision.debug_info["raw_desired"] = desired

    # Rate limiting: max N% change per tick
    max_change = max(2, int(current_threads * state.max_change_pct))
    if abs(desired - current_threads) > max_change:
        if desired > current_threads:
            desired = current_threads + max_change
        else:
            desired = current_threads - max_change

    decision.debug_info["rate_limited_desired"] = desired
    decision.debug_info["max_change"] = max_change

    # Only scale if there's an actual change
    if desired == current_threads:
        decision.reason = "no_change_after_rate_limit"
        return decision

    # At ceiling or floor?
    if desired >= state.max_threads and current_threads >= state.max_threads:
        decision.reason = "at_max_threads"
        decision.debug_info["at_ceiling"] = True
        return decision

    if desired <= state.min_threads and current_threads <= state.min_threads:
        decision.reason = "at_min_threads"
        decision.debug_info["at_floor"] = True
        return decision

    # Scale!
    decision.should_scale = True
    decision.new_target = desired
    decision.reason = "under_target" if under_target else "over_target"
    state.last_scale_time = datetime.now(UTC)

    return decision


def distribute_threads_to_workers(
    *,
    total_threads: int,
    worker_count: int,
    min_threads_per_worker: int = 1,
    max_threads_per_worker: int | None = None,
) -> list[int]:
    """
    Distribute total threads evenly across workers.

    Args:
        total_threads: Total threads to distribute
        worker_count: Number of workers
        min_threads_per_worker: Minimum threads per worker
        max_threads_per_worker: Maximum threads per worker (optional)

    Returns:
        List of thread counts per worker
    """
    if worker_count <= 0:
        return []

    # Base distribution
    base = total_threads // worker_count
    remainder = total_threads % worker_count

    # Clamp base to min
    base = max(base, min_threads_per_worker)

    # Build distribution
    distribution = []
    for i in range(worker_count):
        threads = base + (1 if i < remainder else 0)
        if max_threads_per_worker is not None:
            threads = min(threads, max_threads_per_worker)
        threads = max(threads, min_threads_per_worker)
        distribution.append(threads)

    return distribution
