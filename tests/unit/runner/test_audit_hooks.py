"""Tests for AuditHooks."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock

from pyspark_pipeline_framework.core.audit.sinks import AuditSink
from pyspark_pipeline_framework.core.audit.types import AuditAction, AuditStatus
from pyspark_pipeline_framework.core.config.base import ComponentType
from pyspark_pipeline_framework.core.config.component import ComponentConfig
from pyspark_pipeline_framework.core.config.pipeline import PipelineConfig
from pyspark_pipeline_framework.core.config.spark import SparkConfig
from pyspark_pipeline_framework.core.resilience.circuit_breaker import CircuitState
from pyspark_pipeline_framework.runner.audit_hooks import AuditHooks
from pyspark_pipeline_framework.runner.result import PipelineResult, PipelineResultStatus


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_FIXED_TIME = datetime(2026, 1, 15, 12, 0, 0, tzinfo=timezone.utc)


def _make_component_config(name: str = "my_comp") -> ComponentConfig:
    return ComponentConfig(
        name=name,
        component_type=ComponentType.TRANSFORMATION,
        class_path="some.module.Cls",
    )


def _make_pipeline_config() -> PipelineConfig:
    return PipelineConfig(
        name="test-pipeline",
        version="1.0.0",
        spark=SparkConfig(app_name="test"),
        components=[_make_component_config("dummy")],
    )


def _make_hooks(sink: AuditSink | None = None) -> AuditHooks:
    return AuditHooks(
        sink=sink or MagicMock(spec=AuditSink),
        trace_id="test-trace-id",
        now_fn=lambda: _FIXED_TIME,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestAuditHooksConstruction:
    def test_default_trace_id(self) -> None:
        hooks = AuditHooks(sink=MagicMock(spec=AuditSink))
        assert hooks.trace_id  # Non-empty UUID string

    def test_custom_trace_id(self) -> None:
        hooks = AuditHooks(
            sink=MagicMock(spec=AuditSink), trace_id="custom-id"
        )
        assert hooks.trace_id == "custom-id"


class TestAuditHooksBeforePipeline:
    def test_emits_pipeline_started(self) -> None:
        sink = MagicMock(spec=AuditSink)
        hooks = _make_hooks(sink)

        hooks.before_pipeline(_make_pipeline_config())

        sink.emit.assert_called_once()
        event = sink.emit.call_args[0][0]
        assert event.action == AuditAction.PIPELINE_STARTED
        assert event.actor == "SimplePipelineRunner"
        assert event.resource == "test-pipeline"
        assert event.status == AuditStatus.SUCCESS
        assert event.metadata["component_count"] == "1"
        assert event.trace_id == "test-trace-id"
        assert event.timestamp == _FIXED_TIME


class TestAuditHooksAfterPipeline:
    def test_emits_success(self) -> None:
        sink = MagicMock(spec=AuditSink)
        hooks = _make_hooks(sink)
        result = PipelineResult(
            status=PipelineResultStatus.SUCCESS,
            pipeline_name="test-pipeline",
            total_duration_ms=500,
        )

        hooks.after_pipeline(_make_pipeline_config(), result)

        event = sink.emit.call_args[0][0]
        assert event.action == AuditAction.PIPELINE_COMPLETED
        assert event.status == AuditStatus.SUCCESS
        assert event.metadata["duration_ms"] == "500"

    def test_emits_failure(self) -> None:
        sink = MagicMock(spec=AuditSink)
        hooks = _make_hooks(sink)
        result = PipelineResult(
            status=PipelineResultStatus.FAILURE,
            pipeline_name="test-pipeline",
            total_duration_ms=100,
        )

        hooks.after_pipeline(_make_pipeline_config(), result)

        event = sink.emit.call_args[0][0]
        assert event.status == AuditStatus.FAILURE


class TestAuditHooksComponent:
    def test_before_component(self) -> None:
        sink = MagicMock(spec=AuditSink)
        hooks = _make_hooks(sink)

        hooks.before_component(_make_component_config("loader"), 0, 3)

        event = sink.emit.call_args[0][0]
        assert event.action == AuditAction.COMPONENT_STARTED
        assert event.actor == "loader"
        assert event.resource == "some.module.Cls"
        assert event.metadata["index"] == "0"
        assert event.metadata["total"] == "3"

    def test_after_component(self) -> None:
        sink = MagicMock(spec=AuditSink)
        hooks = _make_hooks(sink)

        hooks.after_component(_make_component_config("loader"), 0, 3, 250)

        event = sink.emit.call_args[0][0]
        assert event.action == AuditAction.COMPONENT_COMPLETED
        assert event.metadata["duration_ms"] == "250"

    def test_on_component_failure(self) -> None:
        sink = MagicMock(spec=AuditSink)
        hooks = _make_hooks(sink)

        hooks.on_component_failure(
            _make_component_config("loader"), 0, RuntimeError("boom")
        )

        event = sink.emit.call_args[0][0]
        assert event.action == AuditAction.COMPONENT_FAILED
        assert event.status == AuditStatus.FAILURE
        assert "boom" in event.metadata["error"]

    def test_error_truncated_to_500(self) -> None:
        sink = MagicMock(spec=AuditSink)
        hooks = _make_hooks(sink)

        hooks.on_component_failure(
            _make_component_config(), 0, RuntimeError("x" * 1000)
        )

        event = sink.emit.call_args[0][0]
        assert len(event.metadata["error"]) == 500


class TestAuditHooksRetry:
    def test_on_retry_attempt(self) -> None:
        sink = MagicMock(spec=AuditSink)
        hooks = _make_hooks(sink)

        hooks.on_retry_attempt(
            _make_component_config(), 2, 5, 1000, RuntimeError("err")
        )

        event = sink.emit.call_args[0][0]
        assert event.action == AuditAction.COMPONENT_RETRIED
        assert event.status == AuditStatus.RETRY
        assert event.metadata["attempt"] == "2"
        assert event.metadata["max_attempts"] == "5"
        assert event.metadata["delay_ms"] == "1000"


class TestAuditHooksCircuitBreaker:
    def test_on_state_change(self) -> None:
        sink = MagicMock(spec=AuditSink)
        hooks = _make_hooks(sink)

        hooks.on_circuit_breaker_state_change(
            "comp", CircuitState.CLOSED, CircuitState.OPEN
        )

        event = sink.emit.call_args[0][0]
        assert event.action == "circuit_breaker_changed"
        assert event.actor == "comp"
        assert event.status == AuditStatus.WARNING
        assert event.metadata["old_state"] == "closed"
        assert event.metadata["new_state"] == "open"
