"""Pipeline configuration models."""

from dataclasses import dataclass, field

from .base import Environment, PipelineMode
from .component import ComponentConfig
from .hooks import HooksConfig
from .secrets import SecretsConfig
from .spark import SparkConfig


@dataclass
class PipelineConfig:
    """Top-level configuration for a pipeline.

    This is the main configuration object that defines a complete pipeline
    including Spark settings, components, hooks, and optional features.
    """

    name: str
    """Pipeline name (required)"""

    version: str
    """Pipeline version (required)"""

    spark: SparkConfig
    """Spark runtime configuration (required)"""

    components: list[ComponentConfig]
    """List of pipeline components (required)"""

    environment: Environment = Environment.DEV
    """Deployment environment (default: dev)"""

    mode: PipelineMode = PipelineMode.BATCH
    """Pipeline execution mode (default: batch)"""

    hooks: HooksConfig = field(default_factory=HooksConfig)
    """Lifecycle hooks configuration (default: HooksConfig with defaults)"""

    secrets: SecretsConfig | None = None
    """Secrets management configuration (optional)"""

    tags: dict[str, str] = field(default_factory=dict)
    """Arbitrary key-value tags for metadata (default: {})"""

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if not self.name:
            raise ValueError("name is required")

        if not self.version:
            raise ValueError("version is required")

        if not self.components:
            raise ValueError("At least one component is required")

        # Validate unique component names
        component_names = [c.name for c in self.components]
        if len(component_names) != len(set(component_names)):
            raise ValueError("Component names must be unique")

        # Validate dependencies reference existing components
        for component in self.components:
            for dep in component.depends_on:
                if dep not in component_names:
                    raise ValueError(f"Component '{component.name}' depends on unknown component '{dep}'")

        # Detect circular dependencies
        self._validate_no_circular_dependencies()

    def _validate_no_circular_dependencies(self) -> None:
        """Validate that there are no circular dependencies between components."""
        # Build adjacency list
        graph: dict[str, list[str]] = {c.name: c.depends_on for c in self.components}

        # Track visited nodes and recursion stack
        visited: set[str] = set()
        rec_stack: set[str] = set()

        def has_cycle(node: str) -> bool:
            """DFS to detect cycles."""
            visited.add(node)
            rec_stack.add(node)

            for neighbor in graph.get(node, []):
                if neighbor not in visited:
                    if has_cycle(neighbor):
                        return True
                elif neighbor in rec_stack:
                    return True

            rec_stack.remove(node)
            return False

        # Check each component
        for component in self.components:
            if component.name not in visited and has_cycle(component.name):
                raise ValueError(f"Circular dependency detected involving component '{component.name}'")

    def get_component(self, name: str) -> ComponentConfig | None:
        """Get a component by name.

        Args:
            name: Component name to look up.

        Returns:
            ComponentConfig if found, None otherwise.
        """
        for component in self.components:
            if component.name == name:
                return component
        return None

    def get_execution_order(self) -> list[str]:
        """Get component names in topologically sorted execution order.

        Returns:
            List of component names in execution order.

        Raises:
            ValueError: If circular dependencies are detected.
        """
        # Build adjacency list
        graph: dict[str, list[str]] = {c.name: c.depends_on for c in self.components}
        in_degree: dict[str, int] = {c.name: len(c.depends_on) for c in self.components}

        # Queue of nodes with no dependencies
        queue: list[str] = [name for name, degree in in_degree.items() if degree == 0]
        result: list[str] = []

        while queue:
            node = queue.pop(0)
            result.append(node)

            # Reduce in-degree for neighbors
            for other_name, deps in graph.items():
                if node in deps:
                    in_degree[other_name] -= 1
                    if in_degree[other_name] == 0:
                        queue.append(other_name)

        if len(result) != len(self.components):
            raise ValueError("Circular dependency detected in components")

        return result
