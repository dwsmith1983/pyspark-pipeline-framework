"""Component protocols for structural typing."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Protocol, TypeVar, runtime_checkable

if TYPE_CHECKING:
    from pyspark_pipeline_framework.core.component.base import PipelineComponent

T_co = TypeVar("T_co", bound="PipelineComponent", covariant=True)


@runtime_checkable
class ConfigurableInstance(Protocol[T_co]):
    """Protocol for components that can be instantiated from configuration.

    Components implementing this protocol can be dynamically loaded
    by the component loader using their class_path and config dict.

    Example::

        class MyTransform(DataFlow):
            @classmethod
            def from_config(cls, config: dict[str, Any]) -> MyTransform:
                return cls(**config)
    """

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> T_co:
        """Create component instance from configuration dict."""
        ...


class SchemaContract(Protocol):
    """Protocol for components that declare input/output schemas.

    Used for compile-time validation of pipeline data flow.
    Schema types will be defined in core.schema module (ppf-1si).
    """

    @property
    def input_schema(self) -> Any | None:
        """Schema this component expects as input, or None if not declared."""
        ...

    @property
    def output_schema(self) -> Any | None:
        """Schema this component produces as output, or None if not declared."""
        ...
