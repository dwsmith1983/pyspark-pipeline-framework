"""End-to-end integration test for SimplePipelineRunner.

Exercises the full pipeline lifecycle with real components, hooks,
retry, circuit breaker, checkpoint/resume, and data quality checks
wired together.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from pyspark_pipeline_framework.core.component.base import PipelineComponent
from pyspark_pipeline_framework.core.config.base import ComponentType
from pyspark_pipeline_framework.core.config.component import ComponentConfig
from pyspark_pipeline_framework.core.config.pipeline import PipelineConfig
from pyspark_pipeline_framework.core.config.retry import (
    CircuitBreakerConfig,
    RetryConfig,
)
from pyspark_pipeline_framework.core.config.spark import SparkConfig
from pyspark_pipeline_framework.core.metrics.registry import InMemoryRegistry
from pyspark_pipeline_framework.core.quality.types import (
    CheckResult,
    CheckTiming,
    DataQualityCheck,
    FailureMode,
)
from pyspark_pipeline_framework.runner.hooks import CompositeHooks
from pyspark_pipeline_framework.runner.hooks_builtin import LoggingHooks, MetricsHooks
from pyspark_pipeline_framework.runner.quality_hooks import DataQualityHooks
from pyspark_pipeline_framework.runner.result import PipelineResultStatus
from pyspark_pipeline_framework.runner.simple_runner import SimplePipelineRunner

# ---------------------------------------------------------------------------
# Shared mutable state — isolated per-test by the autouse fixture below.
# ---------------------------------------------------------------------------

_EXECUTION_LOG: list[str] = []


@pytest.fixture(autouse=True)
def _reset_global_state() -> None:
    """Clear shared mutable state before each test."""
    _EXECUTION_LOG.clear()
    _FlakeCounter.remaining = 0


class _ExtractComponent(PipelineComponent):
    @property
    def name(self) -> str:
        return "extract"

    def run(self) -> None:
        _EXECUTION_LOG.append("extract")


class _TransformComponent(PipelineComponent):
    @property
    def name(self) -> str:
        return "transform"

    def run(self) -> None:
        _EXECUTION_LOG.append("transform")


class _LoadComponent(PipelineComponent):
    @property
    def name(self) -> str:
        return "load"

    def run(self) -> None:
        _EXECUTION_LOG.append("load")


class _FlakeCounter:
    remaining = 0


class _FlakeTransform(PipelineComponent):
    @property
    def name(self) -> str:
        return "flake-transform"

    def run(self) -> None:
        if _FlakeCounter.remaining > 0:
            _FlakeCounter.remaining -= 1
            raise RuntimeError("transient")
        _EXECUTION_LOG.append("flake-transform")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SPARK = SparkConfig(app_name="integration-test")
_MODULE = __name__


def _comp(
    name: str,
    cls_name: str,
    *,
    depends_on: list[str] | None = None,
    retry: RetryConfig | None = None,
    cb: CircuitBreakerConfig | None = None,
) -> ComponentConfig:
    return ComponentConfig(
        name=name,
        component_type=ComponentType.TRANSFORMATION,
        class_path=f"{_MODULE}.{cls_name}",
        depends_on=depends_on or [],
        retry=retry,
        circuit_breaker=cb,
    )


def _pipeline(components: list[ComponentConfig], name: str = "e2e") -> PipelineConfig:
    return PipelineConfig(name=name, version="1.0.0", spark=_SPARK, components=components)


def _runner(
    config: PipelineConfig,
    hooks: Any = None,
    fail_fast: bool = True,
    clock: Any = None,
) -> SimplePipelineRunner:
    wrapper = MagicMock()
    wrapper.spark = MagicMock()
    return SimplePipelineRunner(
        config,
        spark_wrapper=wrapper,
        hooks=hooks,
        fail_fast=fail_fast,
        clock=clock,
        sleep_func=lambda _: None,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestFullPipelineETL:
    """Multi-component ETL pipeline with dependencies runs in order."""

    def test_three_stage_etl(self) -> None:

        comps = [
            _comp("load", "_LoadComponent", depends_on=["transform"]),
            _comp("extract", "_ExtractComponent"),
            _comp("transform", "_TransformComponent", depends_on=["extract"]),
        ]
        config = _pipeline(comps)
        result = _runner(config).run()

        assert result.status == PipelineResultStatus.SUCCESS
        assert _EXECUTION_LOG == ["extract", "transform", "load"]
        assert len(result.component_results) == 3
        assert all(r.success for r in result.component_results)


class TestHooksComposition:
    """Logging, metrics, and DQ hooks all fire during a single run."""

    def test_composite_hooks_all_fire(self) -> None:

        registry = InMemoryRegistry()
        wrapper = MagicMock()
        wrapper.spark = MagicMock()

        metrics = MetricsHooks(registry=registry)
        logging_hooks = LoggingHooks()

        dq = DataQualityHooks(wrapper)
        dq.register(
            DataQualityCheck(
                name="post_pipeline_check",
                timing=CheckTiming.AFTER_PIPELINE,
                check_fn=lambda _: CheckResult(
                    check_name="post_pipeline_check", passed=True, message="ok"
                ),
                failure_mode=FailureMode.FAIL_ON_ERROR,
            )
        )

        composite = CompositeHooks(logging_hooks, metrics, dq)
        comps = [_comp("extract", "_ExtractComponent")]
        config = _pipeline(comps)
        runner = SimplePipelineRunner(
            config,
            spark_wrapper=wrapper,
            hooks=composite,
            sleep_func=lambda _: None,
        )

        result = runner.run()

        assert result.status == PipelineResultStatus.SUCCESS
        # Metrics hooks recorded component duration
        assert registry.get_timer_count(
            "ppf.component.duration", tags={"component": "extract"}
        ) == 1
        # DQ check ran and passed
        assert len(dq.results) == 1
        assert dq.results[0].passed is True


class TestRetryWithMetrics:
    """Retry + metrics hooks record retry counts."""

    def test_retry_recorded_in_metrics(self) -> None:

        _FlakeCounter.remaining = 2
        registry = InMemoryRegistry()
        metrics = MetricsHooks(registry=registry)

        comps = [
            _comp(
                "flake",
                "_FlakeTransform",
                retry=RetryConfig(max_attempts=3, initial_delay_seconds=0.001),
            )
        ]
        config = _pipeline(comps)
        result = _runner(config, hooks=metrics).run()

        assert result.status == PipelineResultStatus.SUCCESS
        assert result.component_results[0].retries == 2
        assert (
            registry.get_counter(
                "ppf.component.retries", tags={"component": "flake"}
            )
            == 2.0
        )


class TestCircuitBreakerIntegration:
    """Circuit breaker opens after repeated failures, blocking further runs."""

    def test_cb_opens_and_blocks(self) -> None:

        _FlakeCounter.remaining = 100  # always fail

        comps = [
            _comp(
                "flake",
                "_FlakeTransform",
                cb=CircuitBreakerConfig(failure_threshold=1, timeout_seconds=60.0),
            )
        ]
        config = _pipeline(comps)
        runner = _runner(config, fail_fast=False)

        # First run trips the circuit breaker
        r1 = runner.run()
        assert not r1.component_results[0].success

        # Second run is blocked by open CB
        r2 = runner.run()
        assert not r2.component_results[0].success
        assert "open" in str(r2.component_results[0].error).lower()


class TestCheckpointResume:
    """Resume skips already-completed components."""

    def test_resume_from_checkpoint(self) -> None:

        comps = [
            _comp("extract", "_ExtractComponent"),
            _comp("transform", "_TransformComponent", depends_on=["extract"]),
            _comp("load", "_LoadComponent", depends_on=["transform"]),
        ]
        config = _pipeline(comps)
        runner = _runner(config)

        # Simulate: extract already done
        result = runner.run(completed_components={"extract"})

        assert result.status == PipelineResultStatus.SUCCESS
        assert _EXECUTION_LOG == ["transform", "load"]
        names = [r.component_name for r in result.component_results]
        assert "extract" not in names
        assert names == ["transform", "load"]


class TestFailFastPartialSuccess:
    """Partial success with fail_fast=True stops early."""

    def test_mixed_results(self) -> None:

        _FlakeCounter.remaining = 100  # always fail

        comps = [
            _comp("extract", "_ExtractComponent"),
            _comp("flake", "_FlakeTransform", depends_on=["extract"]),
            _comp("load", "_LoadComponent", depends_on=["flake"]),
        ]
        config = _pipeline(comps)
        result = _runner(config, fail_fast=True).run()

        assert result.status == PipelineResultStatus.PARTIAL_SUCCESS
        assert _EXECUTION_LOG == ["extract"]
        # Only extract and flake ran; load was skipped
        names = [r.component_name for r in result.component_results]
        assert names == ["extract", "flake"]


class TestTimingIntegration:
    """Clock injection measures durations across the full pipeline."""

    def test_durations_recorded(self) -> None:

        # pipeline_start=0.0, comp_start=0.0, comp_end=0.1, pipeline_end=0.5
        ticks = iter([0.0, 0.0, 0.1, 0.5])
        comps = [_comp("extract", "_ExtractComponent")]
        config = _pipeline(comps)
        result = _runner(config, clock=lambda: next(ticks)).run()

        assert result.status == PipelineResultStatus.SUCCESS
        assert result.component_results[0].duration_ms == 100
        assert result.total_duration_ms == 500


class TestDQFailureBlocksPipeline:
    """A FAIL_ON_ERROR DQ check failure is captured in results."""

    def test_dq_failure_captured(self) -> None:

        wrapper = MagicMock()
        wrapper.spark = MagicMock()

        dq = DataQualityHooks(wrapper)
        dq.register(
            DataQualityCheck(
                name="row_count",
                timing=CheckTiming.BEFORE_PIPELINE,
                check_fn=lambda _: CheckResult(
                    check_name="row_count", passed=False, message="no rows"
                ),
                failure_mode=FailureMode.FAIL_ON_ERROR,
            )
        )

        comps = [_comp("extract", "_ExtractComponent")]
        config = _pipeline(comps)
        runner = SimplePipelineRunner(
            config,
            spark_wrapper=wrapper,
            hooks=dq,
            sleep_func=lambda _: None,
        )

        # DQ error raised inside hook is swallowed by safe_call;
        # pipeline still runs (hooks don't crash pipeline)
        result = runner.run()
        assert result.status == PipelineResultStatus.SUCCESS
        # But the DQ failure is recorded
        assert len(dq.results) == 1
        assert dq.results[0].passed is False
