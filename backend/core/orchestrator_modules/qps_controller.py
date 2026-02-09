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
    starting_threads: float = 0.0  # Initial thread count (0 = ramp from min_threads)
    max_thread_increase: float = 15.0  # Max threads to add per ~10s interval (default 15, max 200)
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

    # Current state
    current_threads: int = 0
    last_observed_qps: float = 0.0
    last_scale_time: datetime | None = None

    def to_dict(self) -> dict[str, Any]:
        """Serialize state for persistence/debugging."""
        return {
            "target_qps": float(self.target_qps),
            "starting_threads": float(self.starting_threads),
            "max_thread_increase": float(self.max_thread_increase),
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
    max_thread_increase: float = 15.0,
) -> tuple[int, dict]:
    """
    Compute desired thread count based on observed vs target QPS.

    Uses gap-closing approach: estimate how many threads needed to hit target,
    then decide what fraction of that gap to close based on error magnitude.
    More aggressive when far from target, conservative when close.

    Args:
        current_threads: Current number of threads
        current_qps: Observed QPS from metrics
        target_qps: Target QPS to achieve
        avg_latency_ms: Average latency for estimation (optional)
        min_threads: Minimum allowed threads
        max_threads: Maximum allowed threads
        max_thread_increase: Maximum threads to add per tick

    Returns:
        Tuple of (desired thread count, debug info dict)
    """
    debug = {}
    
    if current_threads <= 0:
        return min_threads, {"reason": "no_current_threads"}

    if (
        target_qps is None
        or not math.isfinite(target_qps)
        or target_qps <= 0
    ):
        return current_threads, {"reason": "invalid_target_qps"}

    # Calculate error metrics
    error = target_qps - current_qps
    error_ratio = error / target_qps if target_qps > 0 else 0
    abs_error_ratio = abs(error_ratio)
    debug["error_ratio"] = error_ratio
    debug["abs_error_ratio"] = abs_error_ratio

    # If no QPS observed, use latency-based estimation for cold start
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
            debug["cold_start_latency_estimate"] = theoretical
            return max(min_threads, min(max_threads, theoretical)), debug
        return current_threads, {"reason": "no_qps_no_latency"}

    # Estimate QPS per thread from current observations
    qps_per_thread = current_qps / current_threads
    debug["qps_per_thread"] = qps_per_thread
    
    # Estimate how many threads we'd need to hit target (ideal case)
    if qps_per_thread > 0:
        estimated_target_threads = target_qps / qps_per_thread
    else:
        estimated_target_threads = current_threads
    debug["estimated_target_threads"] = estimated_target_threads
    
    # Calculate the thread gap (how far we are from estimated target)
    thread_gap = estimated_target_threads - current_threads
    debug["thread_gap"] = thread_gap
    
    # Determine what fraction of the gap to close based on error magnitude
    # When far from target: close a larger fraction (more aggressive)
    # When close to target: close a smaller fraction (conservative)
    #
    # These values are tuned for ~10 second control intervals.
    # Modeled after v1 gains (0.10 - 0.35) but adapted for gap-closing approach.
    #
    # At >50% error: close 25% of gap
    # At >30% error: close 20% of gap  
    # At >15% error: close 15% of gap
    # At >5% error:  close 10% of gap
    # At <5% error:  close 6% of gap - very conservative near target
    if abs_error_ratio > 0.50:
        gap_fraction = 0.25
    elif abs_error_ratio > 0.30:
        gap_fraction = 0.20
    elif abs_error_ratio > 0.15:
        gap_fraction = 0.15
    elif abs_error_ratio > 0.05:
        gap_fraction = 0.10
    else:
        gap_fraction = 0.06
    
    debug["gap_fraction"] = gap_fraction
    
    # Calculate desired threads by closing that fraction of the gap
    adjustment = thread_gap * gap_fraction
    debug["raw_adjustment"] = adjustment
    
    # Apply caps on adjustment
    # max_thread_increase directly limits how many threads can be added per tick
    max_down = -8  # Max threads to remove per tick
    max_up = max_thread_increase  # Directly use as thread limit (default 15)
    debug["max_up"] = max_up
    
    if adjustment > max_up:
        adjustment = max_up
        debug["capped_up"] = True
    elif adjustment < max_down:
        adjustment = max_down
        debug["capped_down"] = True
    
    desired_float = current_threads + adjustment
    debug["adjustment"] = adjustment
    debug["desired_float"] = desired_float
    
    # Use latency-based estimate as a floor when significantly under target
    # This helps when qps_per_thread is underestimated
    if (
        avg_latency_ms
        and math.isfinite(avg_latency_ms)
        and avg_latency_ms > 0
        and error_ratio > 0.20  # Under target by >20%
    ):
        ops_per_thread_latency = 1000.0 / avg_latency_ms
        theoretical = target_qps / ops_per_thread_latency
        # Use 70% of theoretical as a floor (conservative)
        latency_floor = theoretical * 0.70 * gap_fraction + current_threads * (1 - gap_fraction)
        debug["latency_theoretical"] = theoretical
        debug["latency_floor"] = latency_floor
        if latency_floor > desired_float:
            desired_float = latency_floor
            debug["used_latency_floor"] = True

    desired = int(round(desired_float))
    desired = max(min_threads, min(max_threads, desired))
    debug["final_desired"] = desired
    
    return desired, debug


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
        # If starting_threads is set, use it directly
        if state.starting_threads > 0:
            initial_threads = int(state.starting_threads)
            # Clamp to valid range
            initial_threads = max(state.min_threads, min(initial_threads, state.max_threads))
            decision.new_target = initial_threads
            decision.reason = "initial_from_starting_threads"
            decision.debug_info["starting_threads"] = state.starting_threads
        else:
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

    # Compute desired threads using gap-closing approach
    desired, compute_debug = compute_desired_threads(
        current_threads=current_threads,
        current_qps=current_qps,
        target_qps=state.target_qps,
        avg_latency_ms=avg_latency_ms,
        min_threads=state.min_threads,
        max_threads=state.max_threads,
        max_thread_increase=state.max_thread_increase,
    )

    decision.debug_info["compute_debug"] = compute_debug
    decision.debug_info["desired"] = desired

    # Only scale if there's an actual change
    if desired == current_threads:
        decision.reason = "no_change_needed"
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
