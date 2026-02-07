"""Pipeline configuration validation without Spark.

Provides lightweight validation (class path resolution, protocol checks)
and full dry-run validation (component instantiation without execution).
Both modes are designed for CI/CD pre-flight checks.
"""

from __future__ import annotations

import enum
import logging
from dataclasses import dataclass, field

from pyspark_pipeline_framework.core.component.base import PipelineComponent
from pyspark_pipeline_framework.core.config.pipeline import PipelineConfig
from pyspark_pipeline_framework.runtime.loader import (
    instantiate_component,
    load_component_class,
    validate_component_class,
)

logger = logging.getLogger(__name__)


class ValidationPhase(str, enum.Enum):
    """Phase in which a validation error occurred."""

    CONFIG_SYNTAX = "config-syntax"
    REQUIRED_FIELDS = "required-fields"
    TYPE_RESOLUTION = "type-resolution"
    COMPONENT_CONFIG = "component-config"


@dataclass
class ValidationError:
    """A single validation error.

    Args:
        phase: The validation phase that produced this error.
        message: Human-readable error description.
        component_name: Name of the component involved, if applicable.
    """

    phase: ValidationPhase
    message: str
    component_name: str | None = None


@dataclass
class ValidationResult:
    """Outcome of a pipeline validation.

    Args:
        errors: Fatal issues that would prevent pipeline execution.
        warnings: Non-fatal concerns worth noting.
    """

    errors: list[ValidationError] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """Return ``True`` if no errors were found."""
        return len(self.errors) == 0


@dataclass
class DryRunResult:
    """Outcome of a pipeline dry run.

    Args:
        errors: Components that failed instantiation.
        instantiated: Names of components that instantiated successfully.
    """

    errors: list[ValidationError] = field(default_factory=list)
    instantiated: list[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        """Return ``True`` if all components instantiated successfully."""
        return len(self.errors) == 0


def validate_pipeline(config: PipelineConfig) -> ValidationResult:
    """Validate a pipeline configuration without Spark.

    Checks that all component class paths resolve, are proper
    ``PipelineComponent`` subclasses, and notes missing ``from_config``
    methods as warnings.

    Args:
        config: Pipeline configuration to validate.

    Returns:
        A ``ValidationResult`` with errors and warnings.
    """
    result = ValidationResult()

    # Phase: required fields
    if not config.name:
        result.errors.append(ValidationError(ValidationPhase.REQUIRED_FIELDS, "Pipeline name is empty"))
    if not config.components:
        result.errors.append(ValidationError(ValidationPhase.REQUIRED_FIELDS, "Pipeline has no components"))

    # Phase: type resolution + component config
    for comp in config.components:
        if not comp.enabled:
            continue

        # Check class path resolves to a valid PipelineComponent
        try:
            load_component_class(comp.class_path)
        except Exception as exc:
            result.errors.append(
                ValidationError(
                    ValidationPhase.TYPE_RESOLUTION,
                    f"Cannot load '{comp.class_path}': {exc}",
                    component_name=comp.name,
                )
            )
            continue

        # Check for from_config and abstract methods (warnings)
        try:
            warnings = validate_component_class(comp.class_path)
            for w in warnings:
                result.warnings.append(f"[{comp.name}] {w}")
        except Exception as exc:
            result.errors.append(
                ValidationError(
                    ValidationPhase.COMPONENT_CONFIG,
                    f"Validation failed for '{comp.class_path}': {exc}",
                    component_name=comp.name,
                )
            )

    return result


def dry_run(config: PipelineConfig) -> DryRunResult:
    """Instantiate all pipeline components without executing them.

    This goes further than :func:`validate_pipeline` by actually calling
    ``from_config()`` (or the constructor) on each component. Useful for
    catching config shape mismatches that static validation cannot detect.

    Args:
        config: Pipeline configuration to dry-run.

    Returns:
        A ``DryRunResult`` with per-component outcomes.
    """
    result = DryRunResult()

    for comp in config.components:
        if not comp.enabled:
            continue

        try:
            instance = instantiate_component(comp)
            if not isinstance(instance, PipelineComponent):
                result.errors.append(
                    ValidationError(
                        ValidationPhase.COMPONENT_CONFIG,
                        f"'{comp.class_path}' instantiated but is not a PipelineComponent",
                        component_name=comp.name,
                    )
                )
            else:
                result.instantiated.append(comp.name)
        except Exception as exc:
            result.errors.append(
                ValidationError(
                    ValidationPhase.COMPONENT_CONFIG,
                    f"Failed to instantiate '{comp.class_path}': {exc}",
                    component_name=comp.name,
                )
            )

    return result
