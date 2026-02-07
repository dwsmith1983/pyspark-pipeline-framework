"""Dynamic component loader for pipeline components."""

from __future__ import annotations

import importlib
import logging

from pyspark_pipeline_framework.core.component.base import PipelineComponent
from pyspark_pipeline_framework.core.component.exceptions import ComponentInstantiationError
from pyspark_pipeline_framework.core.config.component import ComponentConfig

logger = logging.getLogger(__name__)


def load_component_class(class_path: str) -> type[PipelineComponent]:
    """Dynamically load a ``PipelineComponent`` subclass by its fully-qualified path.

    Args:
        class_path: Dotted path such as ``"my_package.transforms.MyTransform"``.

    Returns:
        The loaded class (not an instance).

    Raises:
        ComponentInstantiationError: If the path is malformed, the module cannot
            be imported, the attribute does not exist, or the attribute is not a
            valid ``PipelineComponent`` subclass.
    """
    parts = class_path.rsplit(".", 1)
    if len(parts) != 2 or not parts[0] or not parts[1]:
        raise ComponentInstantiationError(
            class_path,
            ValueError(f"Invalid class path format: '{class_path}' (expected 'module.ClassName')"),
        )

    module_path, class_name = parts

    try:
        module = importlib.import_module(module_path)
    except ImportError as exc:
        raise ComponentInstantiationError(class_path, exc) from exc

    try:
        cls = getattr(module, class_name)
    except AttributeError as exc:
        raise ComponentInstantiationError(class_path, exc) from exc

    if not isinstance(cls, type):
        raise ComponentInstantiationError(
            class_path,
            TypeError(f"'{class_name}' is not a class"),
        )

    if not issubclass(cls, PipelineComponent):
        raise ComponentInstantiationError(
            class_path,
            TypeError(f"'{class_name}' is not a PipelineComponent subclass"),
        )

    return cls  # type: ignore[return-value]


def instantiate_component(config: ComponentConfig) -> PipelineComponent:
    """Create a component instance from a ``ComponentConfig``.

    Uses ``from_config(config.config)`` if the class provides it,
    otherwise falls back to ``cls(**config.config)``.

    Args:
        config: Component configuration containing class_path and config dict.

    Returns:
        An instantiated ``PipelineComponent``.

    Raises:
        ComponentInstantiationError: If loading or instantiation fails.
    """
    cls = load_component_class(config.class_path)

    try:
        if hasattr(cls, "from_config") and callable(cls.from_config):
            instance: PipelineComponent = cls.from_config(config.config)  # type: ignore[attr-defined]
            return instance
        return cls(**config.config)
    except Exception as exc:
        raise ComponentInstantiationError(config.class_path, exc) from exc


def validate_component_class(class_path: str) -> list[str]:
    """Validate a component class and return any warnings.

    Args:
        class_path: Fully-qualified path to the component class.

    Returns:
        A list of warning messages. An empty list means the class is valid
        with no concerns.

    Raises:
        ComponentInstantiationError: If the class cannot be loaded at all.
    """
    cls = load_component_class(class_path)
    warnings: list[str] = []

    if not (hasattr(cls, "from_config") and callable(cls.from_config)):
        warnings.append(
            f"'{class_path}' does not implement from_config(); " f"will fall back to **kwargs instantiation"
        )

    abstract_methods: frozenset[str] = getattr(cls, "__abstractmethods__", frozenset())
    if abstract_methods:
        warnings.append(f"'{class_path}' has unimplemented abstract methods: " f"{', '.join(sorted(abstract_methods))}")

    return warnings


def list_available_components(package: str) -> list[str]:
    """List all ``PipelineComponent`` subclasses in a given package.

    Args:
        package: Dotted package path to scan (e.g. ``"my_app.transforms"``).

    Returns:
        A sorted list of fully-qualified class paths.

    Raises:
        ComponentInstantiationError: If the package cannot be imported.
    """
    try:
        module = importlib.import_module(package)
    except ImportError as exc:
        raise ComponentInstantiationError(package, exc) from exc

    results: list[str] = []
    for name, obj in vars(module).items():
        if isinstance(obj, type) and issubclass(obj, PipelineComponent) and obj is not PipelineComponent:
            results.append(f"{package}.{name}")

    return sorted(results)
