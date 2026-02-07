"""Tests for data quality check types."""

from __future__ import annotations

from unittest.mock import MagicMock

from pyspark_pipeline_framework.core.quality.types import (
    CheckResult,
    CheckTiming,
    DataQualityCheck,
    FailureMode,
)


class TestCheckTiming:
    def test_values(self) -> None:
        assert CheckTiming.BEFORE_PIPELINE == "before_pipeline"
        assert CheckTiming.AFTER_PIPELINE == "after_pipeline"
        assert CheckTiming.AFTER_COMPONENT == "after_component"

    def test_is_str(self) -> None:
        assert isinstance(CheckTiming.BEFORE_PIPELINE, str)


class TestFailureMode:
    def test_values(self) -> None:
        assert FailureMode.FAIL_ON_ERROR == "fail_on_error"
        assert FailureMode.WARN_ONLY == "warn_only"
        assert FailureMode.THRESHOLD == "threshold"

    def test_is_str(self) -> None:
        assert isinstance(FailureMode.FAIL_ON_ERROR, str)


class TestCheckResult:
    def test_basic_construction(self) -> None:
        result = CheckResult(
            check_name="test_check", passed=True, message="ok"
        )
        assert result.check_name == "test_check"
        assert result.passed is True
        assert result.message == "ok"
        assert result.details is None

    def test_with_details(self) -> None:
        result = CheckResult(
            check_name="test_check",
            passed=False,
            message="fail",
            details={"count": 42},
        )
        assert result.details == {"count": 42}


class TestDataQualityCheck:
    def test_basic_construction(self) -> None:
        fn = MagicMock()
        check = DataQualityCheck(
            name="my_check",
            timing=CheckTiming.AFTER_PIPELINE,
            check_fn=fn,
        )
        assert check.name == "my_check"
        assert check.timing == CheckTiming.AFTER_PIPELINE
        assert check.check_fn is fn
        assert check.component_name is None
        assert check.failure_mode == FailureMode.FAIL_ON_ERROR
        assert check.max_failures == 0

    def test_with_component_name(self) -> None:
        fn = MagicMock()
        check = DataQualityCheck(
            name="my_check",
            timing=CheckTiming.AFTER_COMPONENT,
            check_fn=fn,
            component_name="etl_step",
        )
        assert check.component_name == "etl_step"

    def test_threshold_mode(self) -> None:
        fn = MagicMock()
        check = DataQualityCheck(
            name="my_check",
            timing=CheckTiming.AFTER_PIPELINE,
            check_fn=fn,
            failure_mode=FailureMode.THRESHOLD,
            max_failures=3,
        )
        assert check.failure_mode == FailureMode.THRESHOLD
        assert check.max_failures == 3
