"""Tests for core component abstractions."""

from typing import Any

import pytest

from pyspark_pipeline_framework.core.component import (
    ComponentError,
    ComponentExecutionError,
    ComponentInstantiationError,
    ConfigurableInstance,
    PipelineComponent,
)


class TestPipelineComponent:
    """Tests for PipelineComponent ABC."""

    def test_cannot_instantiate_directly(self) -> None:
        """Test that PipelineComponent cannot be instantiated."""
        with pytest.raises(TypeError, match="abstract"):
            PipelineComponent()  # type: ignore[abstract]

    def test_concrete_implementation(self) -> None:
        """Test that concrete implementations work."""

        class ConcreteComponent(PipelineComponent):
            @property
            def name(self) -> str:
                return "test-component"

            def run(self) -> None:
                pass

        component = ConcreteComponent()
        assert component.name == "test-component"
        component.run()  # Should not raise

    def test_missing_name_raises(self) -> None:
        """Test that missing name property raises."""

        class IncompleteComponent(PipelineComponent):
            def run(self) -> None:
                pass

        with pytest.raises(TypeError, match="abstract"):
            IncompleteComponent()  # type: ignore[abstract]

    def test_missing_run_raises(self) -> None:
        """Test that missing run method raises."""

        class IncompleteComponent(PipelineComponent):
            @property
            def name(self) -> str:
                return "incomplete"

        with pytest.raises(TypeError, match="abstract"):
            IncompleteComponent()  # type: ignore[abstract]


class TestExceptions:
    """Tests for component exceptions."""

    def test_component_error_is_base(self) -> None:
        """Test ComponentError is base exception."""
        assert issubclass(ComponentInstantiationError, ComponentError)
        assert issubclass(ComponentExecutionError, ComponentError)

    def test_instantiation_error(self) -> None:
        """Test ComponentInstantiationError attributes."""
        cause = ValueError("bad config")
        error = ComponentInstantiationError("my.module.Class", cause)

        assert error.class_path == "my.module.Class"
        assert error.cause is cause
        assert error.__cause__ is cause
        assert "my.module.Class" in str(error)
        assert "bad config" in str(error)

    def test_execution_error(self) -> None:
        """Test ComponentExecutionError attributes."""
        cause = RuntimeError("runtime failure")
        error = ComponentExecutionError("my-component", cause)

        assert error.component_name == "my-component"
        assert error.cause is cause
        assert error.__cause__ is cause
        assert "my-component" in str(error)


class TestProtocols:
    """Tests for component protocols."""

    def test_configurable_instance_is_runtime_checkable(self) -> None:
        """Test ConfigurableInstance can be used with isinstance."""

        class MyComponent(PipelineComponent):
            def __init__(self, value: int) -> None:
                self._value = value

            @property
            def name(self) -> str:
                return "my-component"

            def run(self) -> None:
                pass

            @classmethod
            def from_config(cls, config: dict[str, Any]) -> "MyComponent":
                return cls(config["value"])

        assert isinstance(MyComponent, ConfigurableInstance)

    def test_non_configurable_component(self) -> None:
        """Test component without from_config is not ConfigurableInstance."""

        class SimpleComponent(PipelineComponent):
            @property
            def name(self) -> str:
                return "simple"

            def run(self) -> None:
                pass

        assert not isinstance(SimpleComponent, ConfigurableInstance)

    def test_schema_contract_protocol(self) -> None:
        """Test SchemaContract protocol structural typing."""

        class SchemaAwareComponent(PipelineComponent):
            @property
            def name(self) -> str:
                return "schema-aware"

            def run(self) -> None:
                pass

            @property
            def input_schema(self) -> None:
                return None

            @property
            def output_schema(self) -> dict[str, str]:
                return {"col": "string"}

        component = SchemaAwareComponent()
        assert component.input_schema is None
        assert component.output_schema == {"col": "string"}
