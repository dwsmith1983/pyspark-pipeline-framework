"""Tests for the dynamic component loader."""

from __future__ import annotations

from typing import Any

import pytest

from pyspark_pipeline_framework.core.component.base import PipelineComponent
from pyspark_pipeline_framework.core.component.exceptions import (
    ComponentInstantiationError,
)
from pyspark_pipeline_framework.core.config.base import ComponentType
from pyspark_pipeline_framework.core.config.component import ComponentConfig
from pyspark_pipeline_framework.runtime.loader import (
    instantiate_component,
    list_available_components,
    load_component_class,
    validate_component_class,
)

# ── Helpers ──────────────────────────────────────────────────────────


class _ConcreteComponent(PipelineComponent):
    """Concrete component for testing."""

    def __init__(self, value: int = 0) -> None:
        self._value = value

    @property
    def name(self) -> str:
        return "concrete"

    def run(self) -> None:
        pass


class _ConfigurableComponent(PipelineComponent):
    """Component that implements from_config."""

    def __init__(self, value: int) -> None:
        self._value = value

    @property
    def name(self) -> str:
        return "configurable"

    def run(self) -> None:
        pass

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> _ConfigurableComponent:
        return cls(value=config["value"])


# ── Tests ────────────────────────────────────────────────────────────


class TestLoadComponentClass:
    """Tests for load_component_class."""

    def test_load_valid_class(self) -> None:
        """Successfully loads a valid PipelineComponent subclass."""
        cls = load_component_class(
            "tests.unit.runtime.test_loader._ConcreteComponent"
        )
        assert cls.__name__ == "_ConcreteComponent"
        assert issubclass(cls, PipelineComponent)

    def test_invalid_format_no_dot(self) -> None:
        """Class path without a dot raises ComponentInstantiationError."""
        with pytest.raises(ComponentInstantiationError, match="Invalid class path"):
            load_component_class("NoDot")

    def test_module_not_found(self) -> None:
        """Non-existent module raises ComponentInstantiationError."""
        with pytest.raises(ComponentInstantiationError):
            load_component_class("nonexistent.module.Class")

    def test_class_not_found_in_module(self) -> None:
        """Missing attribute raises ComponentInstantiationError."""
        with pytest.raises(ComponentInstantiationError):
            load_component_class(
                "tests.unit.runtime.test_loader.NoSuchClass"
            )

    def test_not_a_class(self) -> None:
        """Attribute that is not a class raises ComponentInstantiationError."""
        # load_component_class itself is a function, not a class
        with pytest.raises(ComponentInstantiationError, match="not a class"):
            load_component_class(
                "tests.unit.runtime.test_loader.load_component_class"
            )

    def test_not_a_pipeline_component_subclass(self) -> None:
        """A class that isn't a PipelineComponent subclass raises."""
        with pytest.raises(
            ComponentInstantiationError, match="not a PipelineComponent"
        ):
            load_component_class(
                "tests.unit.runtime.test_loader.ComponentConfig"
            )


class TestInstantiateComponent:
    """Tests for instantiate_component."""

    def _make_config(
        self, class_path: str, config: dict[str, Any] | None = None
    ) -> ComponentConfig:
        return ComponentConfig(
            name="test",
            component_type=ComponentType.TRANSFORMATION,
            class_path=class_path,
            config=config or {},
        )

    def test_with_from_config(self) -> None:
        """Uses from_config when available."""
        cfg = self._make_config(
            "tests.unit.runtime.test_loader._ConfigurableComponent",
            {"value": 42},
        )
        component = instantiate_component(cfg)
        assert type(component).__name__ == "_ConfigurableComponent"
        assert component._value == 42  # type: ignore[attr-defined]

    def test_fallback_to_kwargs(self) -> None:
        """Falls back to **kwargs when from_config is absent."""
        cfg = self._make_config(
            "tests.unit.runtime.test_loader._ConcreteComponent",
            {"value": 7},
        )
        component = instantiate_component(cfg)
        assert type(component).__name__ == "_ConcreteComponent"
        assert component._value == 7  # type: ignore[attr-defined]

    def test_instantiation_failure(self) -> None:
        """Wraps instantiation exceptions in ComponentInstantiationError."""
        cfg = self._make_config(
            "tests.unit.runtime.test_loader._ConfigurableComponent",
            {},  # missing required 'value'
        )
        with pytest.raises(ComponentInstantiationError):
            instantiate_component(cfg)


class TestValidateComponentClass:
    """Tests for validate_component_class."""

    def test_valid_with_from_config(self) -> None:
        """No warnings for a fully valid component."""
        warnings = validate_component_class(
            "tests.unit.runtime.test_loader._ConfigurableComponent"
        )
        assert warnings == []

    def test_missing_from_config_warning(self) -> None:
        """Warns when from_config is not implemented."""
        warnings = validate_component_class(
            "tests.unit.runtime.test_loader._ConcreteComponent"
        )
        assert len(warnings) == 1
        assert "from_config" in warnings[0]

    def test_invalid_class_raises(self) -> None:
        """Completely invalid class path raises."""
        with pytest.raises(ComponentInstantiationError):
            validate_component_class("no.such.Module")


class TestListAvailableComponents:
    """Tests for list_available_components."""

    def test_finds_components_in_module(self) -> None:
        """Discovers PipelineComponent subclasses in a module."""
        results = list_available_components("tests.unit.runtime.test_loader")
        assert any("_ConcreteComponent" in r for r in results)
        assert any("_ConfigurableComponent" in r for r in results)

    def test_invalid_package_raises(self) -> None:
        """Non-existent package raises ComponentInstantiationError."""
        with pytest.raises(ComponentInstantiationError):
            list_available_components("completely.fake.package")
