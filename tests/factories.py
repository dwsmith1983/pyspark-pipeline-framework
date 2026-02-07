"""Shared test factories for building configs and mock objects.

These factories consolidate duplicated helpers that previously appeared
across multiple test modules.  Import them directly::

    from tests.factories import make_component_config, make_pipeline_config
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

from pyspark_pipeline_framework.core.config.base import ComponentType
from pyspark_pipeline_framework.core.config.component import ComponentConfig
from pyspark_pipeline_framework.core.config.pipeline import PipelineConfig
from pyspark_pipeline_framework.core.config.retry import (
    CircuitBreakerConfig,
    RetryConfig,
)
from pyspark_pipeline_framework.core.config.spark import SparkConfig
from pyspark_pipeline_framework.runner.simple_runner import SimplePipelineRunner
from pyspark_pipeline_framework.runtime.session.wrapper import SparkSessionWrapper


def make_component_config(
    name: str = "test-comp",
    class_path: str = "fake.module.Component",
    *,
    component_type: ComponentType = ComponentType.TRANSFORMATION,
    enabled: bool = True,
    depends_on: list[str] | None = None,
    retry: RetryConfig | None = None,
    circuit_breaker: CircuitBreakerConfig | None = None,
    config: dict[str, Any] | None = None,
) -> ComponentConfig:
    """Build a ``ComponentConfig`` with sensible test defaults."""
    return ComponentConfig(
        name=name,
        component_type=component_type,
        class_path=class_path,
        depends_on=depends_on or [],
        enabled=enabled,
        retry=retry,
        circuit_breaker=circuit_breaker,
        config=config or {},
    )


def make_pipeline_config(
    components: list[ComponentConfig] | None = None,
    name: str = "test-pipeline",
    version: str = "1.0.0",
    app_name: str = "test",
) -> PipelineConfig:
    """Build a ``PipelineConfig`` with sensible test defaults."""
    if components is None:
        components = [make_component_config("dummy")]
    return PipelineConfig(
        name=name,
        version=version,
        spark=SparkConfig(app_name=app_name),
        components=components,
    )


def make_mock_spark_wrapper() -> MagicMock:
    """Create a mock ``SparkSessionWrapper`` with a nested mock spark session."""
    wrapper = MagicMock(spec=SparkSessionWrapper)
    wrapper.spark = MagicMock()
    return wrapper


def make_runner(
    components: list[ComponentConfig] | None = None,
    *,
    config: PipelineConfig | None = None,
    fail_fast: bool = True,
    hooks: Any = None,
    clock: Any = None,
) -> SimplePipelineRunner:
    """Build a ``SimplePipelineRunner`` with mock Spark and no-op sleep."""
    if config is None:
        config = make_pipeline_config(components)
    return SimplePipelineRunner(
        config,
        spark_wrapper=make_mock_spark_wrapper(),
        hooks=hooks,
        fail_fast=fail_fast,
        clock=clock,
        sleep_func=lambda _: None,
    )
