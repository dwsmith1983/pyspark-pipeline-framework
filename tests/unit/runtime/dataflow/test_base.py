"""Tests for DataFlow base class."""

from unittest.mock import MagicMock

import pytest

from pyspark_pipeline_framework.core.component.base import PipelineComponent
from pyspark_pipeline_framework.runtime.dataflow import DataFlow


class FakeDataFlow(DataFlow):
    """Concrete DataFlow for testing."""

    @property
    def name(self) -> str:
        return "fake-flow"

    def run(self) -> None:
        _ = self.spark  # Access spark to prove injection works


class TestDataFlow:
    """Tests for DataFlow."""

    def test_is_pipeline_component(self) -> None:
        """DataFlow instances are PipelineComponents."""
        flow = FakeDataFlow()
        assert isinstance(flow, PipelineComponent)

    def test_spark_property_before_injection(self) -> None:
        """Accessing spark before injection raises RuntimeError."""
        flow = FakeDataFlow()
        with pytest.raises(RuntimeError, match="SparkSession not available"):
            _ = flow.spark

    def test_spark_error_includes_component_name(self) -> None:
        """RuntimeError message includes the component name."""
        flow = FakeDataFlow()
        with pytest.raises(RuntimeError, match="fake-flow"):
            _ = flow.spark

    def test_set_spark_session(self) -> None:
        """set_spark_session makes spark accessible."""
        mock_spark = MagicMock()
        flow = FakeDataFlow()
        flow.set_spark_session(mock_spark)
        assert flow.spark is mock_spark

    def test_logger_property(self) -> None:
        """Logger is named ppf.component.{name}."""
        flow = FakeDataFlow()
        assert flow.logger.name == "ppf.component.fake-flow"

    def test_logger_caching(self) -> None:
        """Same logger instance returned on repeated access."""
        flow = FakeDataFlow()
        logger1 = flow.logger
        logger2 = flow.logger
        assert logger1 is logger2

    def test_run_with_spark(self) -> None:
        """run() can access spark after injection."""
        mock_spark = MagicMock()
        flow = FakeDataFlow()
        flow.set_spark_session(mock_spark)
        flow.run()  # Should not raise

    def test_name_property(self) -> None:
        """name property returns expected value."""
        flow = FakeDataFlow()
        assert flow.name == "fake-flow"

    def test_cannot_instantiate_abstract(self) -> None:
        """Cannot instantiate DataFlow directly."""
        with pytest.raises(TypeError):
            DataFlow()  # type: ignore[abstract]
