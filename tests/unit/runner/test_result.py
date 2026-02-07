"""Tests for pipeline result models."""

from __future__ import annotations

from pyspark_pipeline_framework.runner.result import (
    ComponentResult,
    PipelineResult,
    PipelineResultStatus,
)


class TestPipelineResultStatus:
    """Tests for PipelineResultStatus enum."""

    def test_enum_values(self) -> None:
        assert PipelineResultStatus.SUCCESS == "success"
        assert PipelineResultStatus.PARTIAL_SUCCESS == "partial_success"
        assert PipelineResultStatus.FAILURE == "failure"

    def test_is_str_enum(self) -> None:
        assert isinstance(PipelineResultStatus.SUCCESS, str)


class TestComponentResult:
    """Tests for ComponentResult dataclass."""

    def test_defaults(self) -> None:
        result = ComponentResult(
            component_name="comp-a", success=True, duration_ms=100
        )
        assert result.error is None
        assert result.retries == 0

    def test_with_error(self) -> None:
        err = RuntimeError("boom")
        result = ComponentResult(
            component_name="comp-a",
            success=False,
            duration_ms=50,
            error=err,
            retries=2,
        )
        assert result.error is err
        assert result.retries == 2


class TestPipelineResult:
    """Tests for PipelineResult dataclass."""

    def test_empty_results(self) -> None:
        result = PipelineResult(
            status=PipelineResultStatus.SUCCESS,
            pipeline_name="test-pipeline",
        )
        assert result.component_results == []
        assert result.completed_components == []
        assert result.failed_components == []

    def test_completed_components(self) -> None:
        result = PipelineResult(
            status=PipelineResultStatus.PARTIAL_SUCCESS,
            pipeline_name="test",
            component_results=[
                ComponentResult("a", success=True, duration_ms=10),
                ComponentResult(
                    "b", success=False, duration_ms=5, error=RuntimeError("x")
                ),
                ComponentResult("c", success=True, duration_ms=20),
            ],
        )
        assert result.completed_components == ["a", "c"]

    def test_failed_components(self) -> None:
        err = ValueError("bad")
        result = PipelineResult(
            status=PipelineResultStatus.FAILURE,
            pipeline_name="test",
            component_results=[
                ComponentResult("a", success=False, duration_ms=5, error=err),
                ComponentResult("b", success=True, duration_ms=10),
            ],
        )
        assert result.failed_components == [("a", err)]
