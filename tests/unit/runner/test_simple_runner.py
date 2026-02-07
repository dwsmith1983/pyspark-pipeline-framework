"""Tests for SimplePipelineRunner."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

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
from pyspark_pipeline_framework.core.resilience.circuit_breaker import CircuitState
from pyspark_pipeline_framework.runner.hooks import NoOpHooks
from pyspark_pipeline_framework.runner.result import (
    PipelineResultStatus,
)
from pyspark_pipeline_framework.runner.simple_runner import SimplePipelineRunner
from pyspark_pipeline_framework.runtime.dataflow.base import DataFlow
from tests.factories import make_mock_spark_wrapper


# ---------------------------------------------------------------------------
# Test helper components
# ---------------------------------------------------------------------------


class _SuccessComponent(PipelineComponent):
    @property
    def name(self) -> str:
        return "success-comp"

    def run(self) -> None:
        pass


class _FailingComponent(PipelineComponent):
    @property
    def name(self) -> str:
        return "failing-comp"

    def run(self) -> None:
        raise RuntimeError("component failed")


class _FlakeyComponent(PipelineComponent):
    """Fails the first N calls, then succeeds."""

    failures_remaining = 0

    @property
    def name(self) -> str:
        return "flakey-comp"

    def run(self) -> None:
        if _FlakeyComponent.failures_remaining > 0:
            _FlakeyComponent.failures_remaining -= 1
            raise RuntimeError("transient failure")


@pytest.fixture(autouse=True)
def _reset_flakey_state() -> None:
    """Reset class-level mutable state before each test."""
    _FlakeyComponent.failures_remaining = 0
    _ResourceComponent.log = []
    _FailingResourceComponent.log = []


class _SparkComponent(DataFlow):
    @property
    def name(self) -> str:
        return "spark-comp"

    def run(self) -> None:
        # Access spark to prove injection worked
        _ = self.spark


class _ResourceComponent(PipelineComponent):
    """Component that implements the Resource protocol."""

    log: list[str] = []

    @property
    def name(self) -> str:
        return "resource-comp"

    def open(self) -> None:
        _ResourceComponent.log.append("open")

    def close(self) -> None:
        _ResourceComponent.log.append("close")

    def run(self) -> None:
        _ResourceComponent.log.append("run")


class _FailingResourceComponent(PipelineComponent):
    """Resource component whose run() raises."""

    log: list[str] = []

    @property
    def name(self) -> str:
        return "failing-resource-comp"

    def open(self) -> None:
        _FailingResourceComponent.log.append("open")

    def close(self) -> None:
        _FailingResourceComponent.log.append("close")

    def run(self) -> None:
        _FailingResourceComponent.log.append("run")
        raise RuntimeError("resource run failed")


# ---------------------------------------------------------------------------
# Helpers for building configs
# ---------------------------------------------------------------------------

_SPARK = SparkConfig(app_name="test-pipeline")


def _make_component_config(
    name: str = "comp-a",
    class_path: str = f"{__name__}._SuccessComponent",
    *,
    enabled: bool = True,
    depends_on: list[str] | None = None,
    retry: RetryConfig | None = None,
    circuit_breaker: CircuitBreakerConfig | None = None,
) -> ComponentConfig:
    return ComponentConfig(
        name=name,
        component_type=ComponentType.TRANSFORMATION,
        class_path=class_path,
        depends_on=depends_on or [],
        enabled=enabled,
        retry=retry,
        circuit_breaker=circuit_breaker,
    )


def _make_pipeline_config(
    components: list[ComponentConfig],
    name: str = "test-pipeline",
) -> PipelineConfig:
    return PipelineConfig(
        name=name,
        version="1.0.0",
        spark=_SPARK,
        components=components,
    )


def _make_runner(
    components: list[ComponentConfig],
    *,
    fail_fast: bool = True,
    hooks: Any = None,
    clock: Any = None,
    validate_before_run: bool = True,
) -> SimplePipelineRunner:
    config = _make_pipeline_config(components)
    return SimplePipelineRunner(
        config,
        spark_wrapper=make_mock_spark_wrapper(),
        hooks=hooks,
        fail_fast=fail_fast,
        clock=clock,
        sleep_func=lambda _: None,
        validate_before_run=validate_before_run,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestHappyPath:
    """All components succeed."""

    def test_all_succeed(self) -> None:
        runner = _make_runner([
            _make_component_config("a"),
            _make_component_config("b"),
        ])
        result = runner.run()

        assert result.status == PipelineResultStatus.SUCCESS
        assert result.pipeline_name == "test-pipeline"
        assert len(result.component_results) == 2
        assert all(r.success for r in result.component_results)


class TestFailFast:
    """fail_fast=True stops on first failure."""

    def test_stops_on_first_failure(self) -> None:
        runner = _make_runner(
            [
                _make_component_config("a"),
                _make_component_config(
                    "b", class_path=f"{__name__}._FailingComponent"
                ),
                _make_component_config("c"),
            ],
            fail_fast=True,
        )
        result = runner.run()

        assert result.status == PipelineResultStatus.PARTIAL_SUCCESS
        # Only a and b ran — c was skipped
        names = [r.component_name for r in result.component_results]
        assert names == ["a", "b"]


class TestFailFastFalse:
    """fail_fast=False continues after failure."""

    def test_continues_after_failure(self) -> None:
        runner = _make_runner(
            [
                _make_component_config(
                    "a", class_path=f"{__name__}._FailingComponent"
                ),
                _make_component_config("b"),
            ],
            fail_fast=False,
        )
        result = runner.run()

        assert result.status == PipelineResultStatus.PARTIAL_SUCCESS
        assert len(result.component_results) == 2
        assert not result.component_results[0].success
        assert result.component_results[1].success


class TestAllFail:
    """All components fail → FAILURE."""

    def test_all_fail(self) -> None:
        runner = _make_runner(
            [
                _make_component_config(
                    "a", class_path=f"{__name__}._FailingComponent"
                ),
                _make_component_config(
                    "b", class_path=f"{__name__}._FailingComponent"
                ),
            ],
            fail_fast=False,
        )
        result = runner.run()

        assert result.status == PipelineResultStatus.FAILURE


class TestDisabledComponent:
    """Disabled components are skipped."""

    def test_disabled_skipped(self) -> None:
        runner = _make_runner([
            _make_component_config("a"),
            _make_component_config("b", enabled=False),
        ])
        result = runner.run()

        assert result.status == PipelineResultStatus.SUCCESS
        assert len(result.component_results) == 1
        assert result.component_results[0].component_name == "a"


class TestExecutionOrder:
    """Components run in topological order."""

    def test_respects_dependencies(self) -> None:
        execution_log: list[str] = []

        class _TrackA(PipelineComponent):
            @property
            def name(self) -> str:
                return "track-a"

            def run(self) -> None:
                execution_log.append("a")

        class _TrackB(PipelineComponent):
            @property
            def name(self) -> str:
                return "track-b"

            def run(self) -> None:
                execution_log.append("b")

        with (
            patch(
                "pyspark_pipeline_framework.runtime.loader.load_component_class",
                side_effect=lambda p: _TrackA if "TrackA" in p else _TrackB,
            ),
            patch(
                "pyspark_pipeline_framework.runtime.loader.instantiate_component",
                side_effect=lambda c: _TrackA() if "TrackA" in c.class_path else _TrackB(),
            ),
        ):
            runner = _make_runner(
                [
                    _make_component_config(
                        "b", class_path="fake.TrackB", depends_on=["a"]
                    ),
                    _make_component_config("a", class_path="fake.TrackA"),
                ],
                validate_before_run=False,
            )
            runner.run()

        assert execution_log == ["a", "b"]


class TestSparkInjection:
    """DataFlow components get spark session."""

    def test_dataflow_gets_spark(self) -> None:
        runner = _make_runner([
            _make_component_config(
                "spark", class_path=f"{__name__}._SparkComponent"
            ),
        ])
        result = runner.run()

        assert result.status == PipelineResultStatus.SUCCESS


class TestRetryIntegration:
    """Retries are counted and component eventually succeeds."""

    def test_retry_then_succeed(self) -> None:
        _FlakeyComponent.failures_remaining = 2
        runner = _make_runner([
            _make_component_config(
                "flakey",
                class_path=f"{__name__}._FlakeyComponent",
                retry=RetryConfig(max_attempts=3, initial_delay_seconds=0.001),
            ),
        ])
        result = runner.run()

        assert result.status == PipelineResultStatus.SUCCESS
        assert result.component_results[0].retries == 2

    def test_retry_exhausted(self) -> None:
        _FlakeyComponent.failures_remaining = 5
        runner = _make_runner([
            _make_component_config(
                "flakey",
                class_path=f"{__name__}._FlakeyComponent",
                retry=RetryConfig(max_attempts=2, initial_delay_seconds=0.001),
            ),
        ])
        result = runner.run()

        assert result.status == PipelineResultStatus.FAILURE
        assert result.component_results[0].error is not None


class TestCircuitBreaker:
    """Circuit breaker opens after threshold failures."""

    def test_circuit_opens_after_failures(self) -> None:
        cb_config = CircuitBreakerConfig(
            failure_threshold=1, timeout_seconds=60.0
        )
        runner = _make_runner(
            [
                _make_component_config(
                    "fail",
                    class_path=f"{__name__}._FailingComponent",
                    circuit_breaker=cb_config,
                ),
            ],
            fail_fast=False,
        )

        # First run — component fails, CB opens
        result1 = runner.run()
        assert not result1.component_results[0].success

        # Second run — CB is OPEN, should fail fast
        result2 = runner.run()
        assert not result2.component_results[0].success
        err = result2.component_results[0].error
        assert err is not None
        assert "open" in str(err).lower()


class TestHooksCalled:
    """Hooks receive lifecycle events."""

    def test_before_after_pipeline(self) -> None:
        hooks = MagicMock(spec=NoOpHooks)
        runner = _make_runner(
            [_make_component_config("a")], hooks=hooks
        )
        runner.run()

        hooks.before_pipeline.assert_called_once()
        hooks.after_pipeline.assert_called_once()

    def test_before_after_component(self) -> None:
        hooks = MagicMock(spec=NoOpHooks)
        runner = _make_runner(
            [_make_component_config("a")], hooks=hooks
        )
        runner.run()

        hooks.before_component.assert_called_once()
        hooks.after_component.assert_called_once()


class TestHookFailureSwallowed:
    """A misbehaving hook does not crash the pipeline."""

    def test_bad_hook_swallowed(self) -> None:

        class _BadHook(NoOpHooks):
            def before_pipeline(self, config: Any) -> None:
                raise RuntimeError("hook exploded")

        runner = _make_runner(
            [_make_component_config("a")], hooks=_BadHook()
        )
        result = runner.run()

        # Pipeline should still succeed
        assert result.status == PipelineResultStatus.SUCCESS


class TestInstantiationFailure:
    """Bad class_path → failed result (not a crash)."""

    def test_bad_class_path_caught_by_validation(self) -> None:
        runner = _make_runner([
            _make_component_config(
                "bad", class_path="no.such.Module"
            ),
        ])
        result = runner.run()

        assert result.status == PipelineResultStatus.FAILURE
        # Validation catches it before execution, so no component results
        assert result.component_results == []

    def test_bad_class_path_without_validation(self) -> None:
        runner = _make_runner(
            [
                _make_component_config(
                    "bad", class_path="no.such.Module"
                ),
            ],
            validate_before_run=False,
        )
        result = runner.run()

        assert result.status == PipelineResultStatus.FAILURE
        assert result.component_results[0].error is not None


class TestDryRun:
    """dry_run validates without executing."""

    def test_valid_classes_no_warnings(self) -> None:
        runner = _make_runner([
            _make_component_config("a"),
        ])
        warnings = runner.dry_run()
        # _SuccessComponent doesn't implement from_config, so one warning
        assert any("from_config" in w for w in warnings)

    def test_invalid_class_warning(self) -> None:
        runner = _make_runner([
            _make_component_config("bad", class_path="no.such.Module"),
        ])
        warnings = runner.dry_run()
        assert any("Cannot load" in w for w in warnings)


class TestFromFile:
    """from_file classmethod loads config."""

    def test_from_file(self, tmp_path: Any) -> None:
        hocon = tmp_path / "pipeline.conf"
        hocon.write_text("""\
{
  name = "file-pipeline"
  version = "2.0.0"
  spark { app_name = "test" }
  components = [
    {
      name = "a"
      component_type = "transformation"
      class_path = "%s._SuccessComponent"
    }
  ]
}
""" % __name__)

        runner = SimplePipelineRunner.from_file(
            hocon, spark_wrapper=make_mock_spark_wrapper(), sleep_func=lambda _: None
        )
        result = runner.run()
        assert result.pipeline_name == "file-pipeline"
        assert result.status == PipelineResultStatus.SUCCESS


class TestComponentDurations:
    """Injectable clock verifies timing is recorded."""

    def test_duration_recorded(self) -> None:
        tick = iter([0.0, 0.0, 0.1, 0.5])
        runner = _make_runner(
            [_make_component_config("a")],
            clock=lambda: next(tick),
        )
        result = runner.run()

        assert result.component_results[0].duration_ms == 100
        assert result.total_duration_ms == 500


class TestResume:
    """completed_components parameter skips already-done components."""

    def test_skips_completed_components(self) -> None:
        runner = _make_runner([
            _make_component_config("a"),
            _make_component_config("b"),
            _make_component_config("c"),
        ])
        result = runner.run(completed_components={"a"})

        assert result.status == PipelineResultStatus.SUCCESS
        names = [r.component_name for r in result.component_results]
        assert names == ["b", "c"]

    def test_none_runs_all(self) -> None:
        runner = _make_runner([
            _make_component_config("a"),
            _make_component_config("b"),
        ])
        result = runner.run(completed_components=None)

        assert result.status == PipelineResultStatus.SUCCESS
        assert len(result.component_results) == 2

    def test_all_completed_returns_success_empty(self) -> None:
        runner = _make_runner([
            _make_component_config("a"),
            _make_component_config("b"),
        ])
        result = runner.run(completed_components={"a", "b"})

        assert result.status == PipelineResultStatus.SUCCESS
        assert result.component_results == []


class TestValidateBeforeRun:
    """Tests for the validate_before_run parameter."""

    def test_enabled_by_default(self) -> None:
        config = _make_pipeline_config([_make_component_config("a")])
        runner = SimplePipelineRunner(
            config,
            spark_wrapper=make_mock_spark_wrapper(),
            sleep_func=lambda _: None,
        )
        assert runner._validate_before_run is True

    def test_valid_config_runs_normally(self) -> None:
        runner = _make_runner([_make_component_config("a")])
        result = runner.run()

        assert result.status == PipelineResultStatus.SUCCESS
        assert len(result.component_results) == 1

    def test_bad_class_path_fails_before_execution(self) -> None:
        runner = _make_runner([
            _make_component_config("bad", class_path="no.such.Module"),
        ])
        result = runner.run()

        assert result.status == PipelineResultStatus.FAILURE
        assert result.component_results == []

    def test_disabled_skips_validation(self) -> None:
        runner = _make_runner(
            [_make_component_config("bad", class_path="no.such.Module")],
            validate_before_run=False,
        )
        result = runner.run()

        # Fails at instantiation, not validation — so we get a component result
        assert result.status == PipelineResultStatus.FAILURE
        assert len(result.component_results) == 1
        assert result.component_results[0].error is not None

    def test_hooks_not_called_on_validation_failure(self) -> None:
        hooks = MagicMock(spec=NoOpHooks)
        runner = _make_runner(
            [_make_component_config("bad", class_path="no.such.Module")],
            hooks=hooks,
        )
        runner.run()

        hooks.before_pipeline.assert_not_called()
        hooks.after_pipeline.assert_not_called()

    def test_from_file_passes_validate_before_run(self, tmp_path: Any) -> None:
        hocon = tmp_path / "pipeline.conf"
        hocon.write_text("""\
{
  name = "file-pipeline"
  version = "2.0.0"
  spark { app_name = "test" }
  components = [
    {
      name = "a"
      component_type = "transformation"
      class_path = "%s._SuccessComponent"
    }
  ]
}
""" % __name__)

        runner = SimplePipelineRunner.from_file(
            hocon,
            spark_wrapper=make_mock_spark_wrapper(),
            sleep_func=lambda _: None,
            validate_before_run=False,
        )
        assert runner._validate_before_run is False


class TestResourceProtocol:
    """Resource protocol lifecycle in the runner."""

    def test_open_and_close_called_around_run(self) -> None:
        comps = [
            _make_component_config(
                "res", f"{__name__}._ResourceComponent"
            )
        ]
        result = _make_runner(comps).run()

        assert result.status == PipelineResultStatus.SUCCESS
        assert _ResourceComponent.log == ["open", "run", "close"]

    def test_close_called_on_failure(self) -> None:
        comps = [
            _make_component_config(
                "res", f"{__name__}._FailingResourceComponent"
            )
        ]
        result = _make_runner(comps, fail_fast=False).run()

        assert not result.component_results[0].success
        assert _FailingResourceComponent.log == ["open", "run", "close"]

    def test_non_resource_component_unchanged(self) -> None:
        comps = [
            _make_component_config("ok", f"{__name__}._SuccessComponent")
        ]
        result = _make_runner(comps).run()

        assert result.status == PipelineResultStatus.SUCCESS
        # No open/close logs — _SuccessComponent doesn't implement Resource
        assert _ResourceComponent.log == []

    def test_close_called_after_retry_exhaustion(self) -> None:
        comps = [
            _make_component_config(
                "res",
                f"{__name__}._FailingResourceComponent",
                retry=RetryConfig(max_attempts=2, initial_delay_seconds=0.001),
            )
        ]
        result = _make_runner(comps, fail_fast=False).run()

        assert not result.component_results[0].success
        # open once, run twice (initial + 1 retry), close once
        assert _FailingResourceComponent.log[0] == "open"
        assert _FailingResourceComponent.log[-1] == "close"
        assert _FailingResourceComponent.log.count("run") == 2
