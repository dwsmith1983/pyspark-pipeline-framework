"""Pipeline execution result models."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum


class PipelineResultStatus(str, Enum):
    """Overall outcome of a pipeline run."""

    SUCCESS = "success"
    PARTIAL_SUCCESS = "partial_success"
    FAILURE = "failure"


@dataclass
class ComponentResult:
    """Result of executing a single pipeline component."""

    component_name: str
    success: bool
    duration_ms: int
    error: Exception | None = None
    retries: int = 0


@dataclass
class PipelineResult:
    """Aggregate result of a full pipeline run."""

    status: PipelineResultStatus
    pipeline_name: str
    component_results: list[ComponentResult] = field(default_factory=list)
    total_duration_ms: int = 0

    @property
    def completed_components(self) -> list[str]:
        """Return names of components that completed successfully."""
        return [r.component_name for r in self.component_results if r.success]

    @property
    def failed_components(self) -> list[tuple[str, Exception]]:
        """Return (name, error) pairs for components that failed."""
        return [(r.component_name, r.error) for r in self.component_results if not r.success and r.error is not None]
