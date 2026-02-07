"""Core component abstractions for pipeline components.

This module provides the base classes and protocols that all
pipeline components must implement.
"""

from pyspark_pipeline_framework.core.component.base import PipelineComponent
from pyspark_pipeline_framework.core.component.exceptions import (
    ComponentError,
    ComponentExecutionError,
    ComponentInstantiationError,
)
from pyspark_pipeline_framework.core.component.protocols import ConfigurableInstance, SchemaContract

__all__ = [
    "ComponentError",
    "ComponentExecutionError",
    "ComponentInstantiationError",
    "ConfigurableInstance",
    "PipelineComponent",
    "SchemaContract",
]
