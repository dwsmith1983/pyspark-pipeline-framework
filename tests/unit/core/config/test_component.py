"""Tests for component configuration models."""

import pytest

from pyspark_pipeline_framework.core.config.base import ComponentType
from pyspark_pipeline_framework.core.config.component import ComponentConfig
from pyspark_pipeline_framework.core.config.retry import (
    CircuitBreakerConfig,
    RetryConfig,
)


class TestComponentConfig:
    """Tests for ComponentConfig."""

    def test_minimal_config(self) -> None:
        """Test minimal required configuration."""
        config = ComponentConfig(
            name="test-component",
            component_type=ComponentType.SOURCE,
            class_path="myapp.components.MySource",
        )
        assert config.name == "test-component"
        assert config.component_type == ComponentType.SOURCE
        assert config.class_path == "myapp.components.MySource"
        assert config.config == {}
        assert config.depends_on == []
        assert config.retry is None
        assert config.circuit_breaker is None
        assert config.enabled is True

    def test_full_config(self) -> None:
        """Test full configuration with all options."""
        retry = RetryConfig(max_attempts=5)
        circuit_breaker = CircuitBreakerConfig(failure_threshold=10)

        config = ComponentConfig(
            name="test-component",
            component_type=ComponentType.TRANSFORMATION,
            class_path="myapp.components.MyTransformation",
            config={"param1": "value1", "param2": 42},
            depends_on=["upstream-component"],
            retry=retry,
            circuit_breaker=circuit_breaker,
            enabled=False,
        )

        assert config.name == "test-component"
        assert config.component_type == ComponentType.TRANSFORMATION
        assert config.class_path == "myapp.components.MyTransformation"
        assert config.config == {"param1": "value1", "param2": 42}
        assert config.depends_on == ["upstream-component"]
        assert config.retry == retry
        assert config.circuit_breaker == circuit_breaker
        assert config.enabled is False

    def test_validation_name_required(self) -> None:
        """Test validation for required name."""
        with pytest.raises(ValueError, match="name is required"):
            ComponentConfig(
                name="",
                component_type=ComponentType.SOURCE,
                class_path="myapp.components.MySource",
            )

    def test_validation_class_path_required(self) -> None:
        """Test validation for required class_path."""
        with pytest.raises(ValueError, match="class_path is required"):
            ComponentConfig(
                name="test-component",
                component_type=ComponentType.SOURCE,
                class_path="",
            )

    def test_validation_self_dependency(self) -> None:
        """Test validation against self-dependency."""
        with pytest.raises(ValueError, match="cannot depend on itself"):
            ComponentConfig(
                name="test-component",
                component_type=ComponentType.SOURCE,
                class_path="myapp.components.MySource",
                depends_on=["test-component"],
            )

    def test_component_types(self) -> None:
        """Test all component types."""
        for comp_type in ComponentType:
            config = ComponentConfig(
                name=f"test-{comp_type.value}",
                component_type=comp_type,
                class_path="myapp.components.MyComponent",
            )
            assert config.component_type == comp_type
