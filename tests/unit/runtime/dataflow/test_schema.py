"""Tests for SchemaAwareDataFlow."""

from typing import Any
from unittest.mock import MagicMock

from pyspark_pipeline_framework.runtime.dataflow import DataFlow, SchemaAwareDataFlow


class FakeSchemaFlow(SchemaAwareDataFlow):
    """Concrete SchemaAwareDataFlow for testing."""

    @property
    def name(self) -> str:
        return "schema-flow"

    def run(self) -> None:
        pass


class OverriddenSchemaFlow(SchemaAwareDataFlow):
    """SchemaAwareDataFlow with overridden schemas."""

    @property
    def name(self) -> str:
        return "overridden-schema-flow"

    @property
    def input_schema(self) -> Any:
        return {"col_a": "string"}

    @property
    def output_schema(self) -> Any:
        return {"col_b": "int"}

    def run(self) -> None:
        pass


class TestSchemaAwareDataFlow:
    """Tests for SchemaAwareDataFlow."""

    def test_is_dataflow(self) -> None:
        """SchemaAwareDataFlow is a DataFlow."""
        flow = FakeSchemaFlow()
        assert isinstance(flow, DataFlow)

    def test_defaults_to_none(self) -> None:
        """input_schema and output_schema default to None."""
        flow = FakeSchemaFlow()
        assert flow.input_schema is None
        assert flow.output_schema is None

    def test_schema_overrides(self) -> None:
        """Subclass can override schema properties."""
        flow = OverriddenSchemaFlow()
        assert flow.input_schema == {"col_a": "string"}
        assert flow.output_schema == {"col_b": "int"}

    def test_satisfies_schema_contract(self) -> None:
        """SchemaAwareDataFlow has SchemaContract-compatible interface."""
        flow = FakeSchemaFlow()
        assert hasattr(flow, "input_schema")
        assert hasattr(flow, "output_schema")

    def test_overridden_satisfies_schema_contract(self) -> None:
        """Overridden SchemaAwareDataFlow has SchemaContract-compatible interface."""
        flow = OverriddenSchemaFlow()
        assert hasattr(flow, "input_schema")
        assert hasattr(flow, "output_schema")

    def test_spark_injection_works(self) -> None:
        """SchemaAwareDataFlow inherits spark injection."""
        mock_spark = MagicMock()
        flow = FakeSchemaFlow()
        flow.set_spark_session(mock_spark)
        assert flow.spark is mock_spark

    def test_logger_works(self) -> None:
        """SchemaAwareDataFlow inherits logger."""
        flow = FakeSchemaFlow()
        assert flow.logger.name == "ppf.component.schema-flow"
