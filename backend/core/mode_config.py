"""Unified mode configuration parsing for scaling and load modes.

Consolidates duplicate mode parsing logic from:
- orchestrator.py
- test_executor.py
- config_normalizer.py
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Literal

if TYPE_CHECKING:
    pass


ScalingMode = Literal["AUTO", "BOUNDED", "FIXED"]
LoadMode = Literal["CONCURRENCY", "QPS", "FIND_MAX_CONCURRENCY"]

VALID_SCALING_MODES: frozenset[str] = frozenset({"AUTO", "BOUNDED", "FIXED"})
VALID_LOAD_MODES: frozenset[str] = frozenset({"CONCURRENCY", "QPS", "FIND_MAX_CONCURRENCY"})

DEFAULT_SCALING_MODE: ScalingMode = "AUTO"
DEFAULT_LOAD_MODE: LoadMode = "CONCURRENCY"


@dataclass(frozen=True, slots=True)
class ModeConfig:
    """Unified configuration for scaling and load modes.

    Attributes:
        scaling_mode: How workers/connections scale (AUTO, BOUNDED, FIXED)
        load_mode: How load is generated (CONCURRENCY, QPS, FIND_MAX_CONCURRENCY)
    """

    scaling_mode: ScalingMode
    load_mode: LoadMode

    @classmethod
    def from_config(
        cls,
        scaling_cfg: dict[str, Any] | None = None,
        workload_cfg: dict[str, Any] | None = None,
        scenario: Any = None,
        *,
        strict: bool = False,
    ) -> ModeConfig:
        """Parse mode configuration from various config sources.

        Args:
            scaling_cfg: Scaling configuration dict (contains "mode" key)
            workload_cfg: Workload configuration dict (contains "load_mode" key)
            scenario: Scenario object with load_mode attribute
            strict: If True, raise ValueError on invalid modes.
                   If False, use defaults for invalid modes.

        Returns:
            ModeConfig with parsed scaling_mode and load_mode

        Raises:
            ValueError: If strict=True and mode values are invalid
        """
        # Parse scaling mode
        scaling_mode = cls._parse_scaling_mode(scaling_cfg, strict=strict)

        # Parse load mode from workload_cfg or scenario
        load_mode = cls._parse_load_mode(workload_cfg, scenario, strict=strict)

        return cls(scaling_mode=scaling_mode, load_mode=load_mode)

    @classmethod
    def _parse_scaling_mode(
        cls,
        scaling_cfg: dict[str, Any] | None,
        *,
        strict: bool,
    ) -> ScalingMode:
        """Parse scaling mode from config dict."""
        if not isinstance(scaling_cfg, dict):
            scaling_cfg = {}

        raw = scaling_cfg.get("mode")
        mode = str(raw or DEFAULT_SCALING_MODE).strip().upper() or DEFAULT_SCALING_MODE

        if mode not in VALID_SCALING_MODES:
            if strict:
                raise ValueError(f"scaling.mode must be AUTO, BOUNDED, or FIXED (got '{mode}')")
            mode = DEFAULT_SCALING_MODE

        return mode  # type: ignore[return-value]

    @classmethod
    def _parse_load_mode(
        cls,
        workload_cfg: dict[str, Any] | None,
        scenario: Any,
        *,
        strict: bool,
    ) -> LoadMode:
        """Parse load mode from workload config or scenario."""
        raw: str | None = None

        # Try workload_cfg first
        if isinstance(workload_cfg, dict):
            raw = workload_cfg.get("load_mode")

        # Fall back to scenario attribute
        if raw is None and scenario is not None:
            raw = getattr(scenario, "load_mode", None)

        mode = str(raw or DEFAULT_LOAD_MODE).strip().upper() or DEFAULT_LOAD_MODE

        if mode not in VALID_LOAD_MODES:
            if strict:
                raise ValueError(
                    f"load_mode must be CONCURRENCY, QPS, or FIND_MAX_CONCURRENCY (got '{mode}')"
                )
            mode = DEFAULT_LOAD_MODE

        return mode  # type: ignore[return-value]

    @property
    def is_find_max(self) -> bool:
        """True if load_mode is FIND_MAX_CONCURRENCY."""
        return self.load_mode == "FIND_MAX_CONCURRENCY"

    @property
    def is_qps_mode(self) -> bool:
        """True if load_mode is QPS."""
        return self.load_mode == "QPS"

    @property
    def is_concurrency_mode(self) -> bool:
        """True if load_mode is CONCURRENCY."""
        return self.load_mode == "CONCURRENCY"

    @property
    def autoscale_enabled(self) -> bool:
        """True if scaling mode allows autoscaling (AUTO or BOUNDED)."""
        return self.scaling_mode != "FIXED"

    @property
    def is_fixed_scaling(self) -> bool:
        """True if scaling mode is FIXED."""
        return self.scaling_mode == "FIXED"

    @property
    def is_bounded_scaling(self) -> bool:
        """True if scaling mode is BOUNDED."""
        return self.scaling_mode == "BOUNDED"
