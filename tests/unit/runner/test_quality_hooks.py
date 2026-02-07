"""Tests for DataQualityHooks."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pyspark_pipeline_framework.core.config.base import ComponentType
from pyspark_pipeline_framework.core.config.component import ComponentConfig
from pyspark_pipeline_framework.core.config.pipeline import PipelineConfig
from pyspark_pipeline_framework.core.config.spark import SparkConfig
from pyspark_pipeline_framework.core.quality.types import (
    CheckResult,
    CheckTiming,
    DataQualityCheck,
    FailureMode,
)
from pyspark_pipeline_framework.core.resilience.circuit_breaker import CircuitState
from pyspark_pipeline_framework.runner.quality_hooks import (
    DataQualityError,
    DataQualityHooks,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


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


def _passing_check(
    timing: CheckTiming = CheckTiming.AFTER_PIPELINE,
    component_name: str | None = None,
    failure_mode: FailureMode = FailureMode.FAIL_ON_ERROR,
) -> DataQualityCheck:
    def fn(_spark: object) -> CheckResult:
        return CheckResult(check_name="pass_check", passed=True, message="ok")

    return DataQualityCheck(
        name="pass_check",
        timing=timing,
        check_fn=fn,
        component_name=component_name,
        failure_mode=failure_mode,
    )


def _failing_check(
    name: str = "fail_check",
    timing: CheckTiming = CheckTiming.AFTER_PIPELINE,
    component_name: str | None = None,
    failure_mode: FailureMode = FailureMode.FAIL_ON_ERROR,
    max_failures: int = 0,
) -> DataQualityCheck:
    def fn(_spark: object) -> CheckResult:
        return CheckResult(check_name=name, passed=False, message="bad data")

    return DataQualityCheck(
        name=name,
        timing=timing,
        check_fn=fn,
        component_name=component_name,
        failure_mode=failure_mode,
        max_failures=max_failures,
    )


def _raising_check(
    timing: CheckTiming = CheckTiming.AFTER_PIPELINE,
) -> DataQualityCheck:
    def fn(_spark: object) -> CheckResult:
        raise RuntimeError("boom")

    return DataQualityCheck(
        name="raise_check",
        timing=timing,
        check_fn=fn,
        failure_mode=FailureMode.FAIL_ON_ERROR,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestDataQualityError:
    def test_message_contains_check_name(self) -> None:
        result = CheckResult(check_name="my_check", passed=False, message="bad")
        err = DataQualityError("my_check", result)
        assert "my_check" in str(err)
        assert err.check_name == "my_check"
        assert err.result is result


class TestDataQualityHooksRegister:
    def test_register_single(self) -> None:
        wrapper = MagicMock()
        hooks = DataQualityHooks(wrapper)
        check = _passing_check()
        hooks.register(check)
        assert len(hooks.checks) == 1

    def test_register_all(self) -> None:
        wrapper = MagicMock()
        hooks = DataQualityHooks(wrapper)
        hooks.register_all([_passing_check(), _failing_check()])
        assert len(hooks.checks) == 2


class TestDataQualityHooksBeforePipeline:
    def test_runs_before_pipeline_checks(self) -> None:
        wrapper = MagicMock()
        hooks = DataQualityHooks(wrapper)
        hooks.register(_passing_check(timing=CheckTiming.BEFORE_PIPELINE))

        hooks.before_pipeline(_make_pipeline_config())

        assert len(hooks.results) == 1
        assert hooks.results[0].passed is True

    def test_clears_state_on_before_pipeline(self) -> None:
        wrapper = MagicMock()
        hooks = DataQualityHooks(wrapper)
        hooks.register(_passing_check(timing=CheckTiming.BEFORE_PIPELINE))

        hooks.before_pipeline(_make_pipeline_config())
        hooks.before_pipeline(_make_pipeline_config())

        # Results from first run should be cleared
        assert len(hooks.results) == 1


class TestDataQualityHooksAfterPipeline:
    def test_runs_after_pipeline_checks(self) -> None:
        wrapper = MagicMock()
        hooks = DataQualityHooks(wrapper)
        hooks.register(_passing_check(timing=CheckTiming.AFTER_PIPELINE))

        hooks.after_pipeline(_make_pipeline_config(), result=None)

        assert len(hooks.results) == 1
        assert hooks.results[0].passed is True


class TestDataQualityHooksAfterComponent:
    def test_runs_matching_component_check(self) -> None:
        wrapper = MagicMock()
        hooks = DataQualityHooks(wrapper)
        hooks.register(
            _passing_check(
                timing=CheckTiming.AFTER_COMPONENT, component_name="my_comp"
            )
        )

        hooks.after_component(_make_component_config("my_comp"), 0, 1, 100)

        assert len(hooks.results) == 1

    def test_skips_non_matching_component(self) -> None:
        wrapper = MagicMock()
        hooks = DataQualityHooks(wrapper)
        hooks.register(
            _passing_check(
                timing=CheckTiming.AFTER_COMPONENT, component_name="other_comp"
            )
        )

        hooks.after_component(_make_component_config("my_comp"), 0, 1, 100)

        assert len(hooks.results) == 0


class TestDataQualityHooksFailureModes:
    def test_fail_on_error_raises(self) -> None:
        wrapper = MagicMock()
        hooks = DataQualityHooks(wrapper)
        hooks.register(
            _failing_check(failure_mode=FailureMode.FAIL_ON_ERROR)
        )

        with pytest.raises(DataQualityError, match="fail_check"):
            hooks.after_pipeline(_make_pipeline_config(), result=None)

    def test_warn_only_does_not_raise(self) -> None:
        wrapper = MagicMock()
        hooks = DataQualityHooks(wrapper)
        hooks.register(
            _failing_check(failure_mode=FailureMode.WARN_ONLY)
        )

        # Should not raise
        hooks.after_pipeline(_make_pipeline_config(), result=None)

        assert len(hooks.results) == 1
        assert hooks.results[0].passed is False

    def test_threshold_allows_up_to_max_failures(self) -> None:
        wrapper = MagicMock()
        hooks = DataQualityHooks(wrapper)
        hooks.register(
            _failing_check(
                failure_mode=FailureMode.THRESHOLD, max_failures=2
            )
        )
        config = _make_pipeline_config()

        # First two failures should be warnings
        hooks.after_pipeline(config, result=None)
        hooks.after_pipeline(config, result=None)
        assert len(hooks.results) == 2

        # Third failure exceeds threshold
        with pytest.raises(DataQualityError):
            hooks.after_pipeline(config, result=None)

    def test_threshold_resets_on_before_pipeline(self) -> None:
        wrapper = MagicMock()
        hooks = DataQualityHooks(wrapper)
        hooks.register(
            _failing_check(
                timing=CheckTiming.AFTER_PIPELINE,
                failure_mode=FailureMode.THRESHOLD,
                max_failures=1,
            )
        )
        config = _make_pipeline_config()

        # First run: 1 failure within threshold
        hooks.before_pipeline(config)
        hooks.after_pipeline(config, result=None)

        # Second run: counters reset, so 1 failure within threshold again
        hooks.before_pipeline(config)
        hooks.after_pipeline(config, result=None)

        # No error raised — counters were reset


class TestDataQualityHooksCheckException:
    def test_check_exception_captured_as_failure(self) -> None:
        wrapper = MagicMock()
        hooks = DataQualityHooks(wrapper)
        hooks.register(_raising_check(timing=CheckTiming.AFTER_PIPELINE))

        with pytest.raises(DataQualityError, match="raise_check"):
            hooks.after_pipeline(_make_pipeline_config(), result=None)

        assert len(hooks.results) == 1
        assert hooks.results[0].passed is False
        assert "exception" in hooks.results[0].message.lower()


class TestDataQualityHooksNoOpMethods:
    """Verify that no-op hook methods don't raise."""

    def test_before_component(self) -> None:
        wrapper = MagicMock()
        hooks = DataQualityHooks(wrapper)
        hooks.before_component(_make_component_config(), 0, 1)

    def test_on_component_failure(self) -> None:
        wrapper = MagicMock()
        hooks = DataQualityHooks(wrapper)
        hooks.on_component_failure(
            _make_component_config(), 0, RuntimeError("err")
        )

    def test_on_retry_attempt(self) -> None:
        wrapper = MagicMock()
        hooks = DataQualityHooks(wrapper)
        hooks.on_retry_attempt(
            _make_component_config(), 1, 3, 1000, RuntimeError("err")
        )

    def test_on_circuit_breaker_state_change(self) -> None:
        wrapper = MagicMock()
        hooks = DataQualityHooks(wrapper)
        hooks.on_circuit_breaker_state_change(
            "comp", CircuitState.CLOSED, CircuitState.OPEN
        )


class TestDataQualityHooksResultsCopy:
    def test_results_returns_copy(self) -> None:
        wrapper = MagicMock()
        hooks = DataQualityHooks(wrapper)
        hooks.register(
            _passing_check(timing=CheckTiming.AFTER_PIPELINE)
        )
        hooks.after_pipeline(_make_pipeline_config(), result=None)

        results = hooks.results
        results.clear()
        assert len(hooks.results) == 1  # Original not affected
