"""Audit trail hooks for pipeline lifecycle integration."""

from __future__ import annotations

import uuid
from collections.abc import Callable
from datetime import datetime, timezone
from typing import Any

from pyspark_pipeline_framework.core.audit.sinks import AuditSink
from pyspark_pipeline_framework.core.audit.types import (
    AuditAction,
    AuditEvent,
    AuditStatus,
)
from pyspark_pipeline_framework.core.config.component import ComponentConfig
from pyspark_pipeline_framework.core.config.pipeline import PipelineConfig
from pyspark_pipeline_framework.core.resilience.circuit_breaker import CircuitState
from pyspark_pipeline_framework.runner.result import PipelineResultStatus


class AuditHooks:
    """Pipeline hooks that emit audit events at every lifecycle point.

    Each instance carries a ``trace_id`` for correlating events across
    a single pipeline run.

    Args:
        sink: The audit sink to emit events to.
        trace_id: Correlation ID. Defaults to a new UUID4.
        now_fn: Injectable clock for testing.
            Defaults to ``datetime.now(timezone.utc)``.
    """

    def __init__(
        self,
        sink: AuditSink,
        trace_id: str | None = None,
        now_fn: Callable[[], datetime] | None = None,
    ) -> None:
        self._sink = sink
        self._trace_id = trace_id or str(uuid.uuid4())
        self._now_fn = now_fn or (lambda: datetime.now(timezone.utc))

    @property
    def trace_id(self) -> str:
        """Return the trace ID for this hooks instance."""
        return self._trace_id

    def _emit(
        self,
        action: AuditAction | str,
        actor: str,
        resource: str,
        status: AuditStatus,
        metadata: dict[str, str] | None = None,
    ) -> None:
        event = AuditEvent(
            action=action,
            actor=actor,
            resource=resource,
            status=status,
            timestamp=self._now_fn(),
            metadata=metadata or {},
            trace_id=self._trace_id,
        )
        self._sink.emit(event)

    # ------------------------------------------------------------------
    # PipelineHooks protocol
    # ------------------------------------------------------------------

    def before_pipeline(self, config: PipelineConfig) -> None:
        self._emit(
            AuditAction.PIPELINE_STARTED,
            "SimplePipelineRunner",
            config.name,
            AuditStatus.SUCCESS,
            {"component_count": str(len(config.components))},
        )

    def after_pipeline(self, config: PipelineConfig, result: Any) -> None:
        status = (
            AuditStatus.SUCCESS
            if result.status == PipelineResultStatus.SUCCESS
            else AuditStatus.FAILURE
        )
        self._emit(
            AuditAction.PIPELINE_COMPLETED,
            "SimplePipelineRunner",
            config.name,
            status,
            {
                "status": result.status.value,
                "duration_ms": str(result.total_duration_ms),
            },
        )

    def before_component(
        self, config: ComponentConfig, index: int, total: int
    ) -> None:
        self._emit(
            AuditAction.COMPONENT_STARTED,
            config.name,
            config.class_path,
            AuditStatus.SUCCESS,
            {"index": str(index), "total": str(total)},
        )

    def after_component(
        self, config: ComponentConfig, index: int, total: int, duration_ms: int
    ) -> None:
        self._emit(
            AuditAction.COMPONENT_COMPLETED,
            config.name,
            config.class_path,
            AuditStatus.SUCCESS,
            {"duration_ms": str(duration_ms)},
        )

    def on_component_failure(
        self, config: ComponentConfig, index: int, error: Exception
    ) -> None:
        self._emit(
            AuditAction.COMPONENT_FAILED,
            config.name,
            config.class_path,
            AuditStatus.FAILURE,
            {"error": str(error)[:500]},
        )

    def on_retry_attempt(
        self,
        config: ComponentConfig,
        attempt: int,
        max_attempts: int,
        delay_ms: int,
        error: Exception,
    ) -> None:
        self._emit(
            AuditAction.COMPONENT_RETRIED,
            config.name,
            config.class_path,
            AuditStatus.RETRY,
            {
                "attempt": str(attempt),
                "max_attempts": str(max_attempts),
                "delay_ms": str(delay_ms),
            },
        )

    def on_circuit_breaker_state_change(
        self,
        component_name: str,
        old_state: CircuitState,
        new_state: CircuitState,
    ) -> None:
        self._emit(
            "circuit_breaker_changed",
            component_name,
            "circuit_breaker",
            AuditStatus.WARNING,
            {"old_state": old_state.value, "new_state": new_state.value},
        )
