"""Component configuration models."""

from dataclasses import dataclass, field
from typing import Any

from pyspark_pipeline_framework.core.config.base import ComponentType
from pyspark_pipeline_framework.core.config.retry import CircuitBreakerConfig, ResiliencePolicy, RetryConfig


@dataclass
class ComponentConfig:
    """Configuration for a pipeline component.

    Components are the building blocks of pipelines - sources, transformations, and sinks.
    """

    name: str
    """Unique component name within the pipeline (required)"""

    component_type: ComponentType
    """Type of component - source, transformation, or sink (required)"""

    class_path: str
    """Fully qualified Python class path to instantiate (required)"""

    config: dict[str, Any] = field(default_factory=dict)
    """Component-specific configuration (default: {})"""

    depends_on: list[str] = field(default_factory=list)
    """Names of prerequisite components that must complete first (default: [])"""

    retry: RetryConfig | None = None
    """Retry configuration for this component (optional)"""

    circuit_breaker: CircuitBreakerConfig | None = None
    """Circuit breaker configuration for this component (optional)"""

    resilience: ResiliencePolicy | None = None
    """Bundled resilience policy (optional).

    Mutually exclusive with individual ``retry`` / ``circuit_breaker``
    fields.  When set, populates both from the policy.
    """

    enabled: bool = True
    """Whether this component is enabled (default: True)"""

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if not self.name:
            raise ValueError("name is required")

        if not self.class_path:
            raise ValueError("class_path is required")

        # Validate no circular dependencies at the component level
        if self.name in self.depends_on:
            raise ValueError(f"Component '{self.name}' cannot depend on itself")

        # Resilience policy expansion
        if self.resilience is not None:
            if self.retry is not None or self.circuit_breaker is not None:
                raise ValueError("Cannot set both 'resilience' and individual 'retry'/'circuit_breaker' fields")
            self.retry = self.resilience.retry
            self.circuit_breaker = self.resilience.circuit_breaker
