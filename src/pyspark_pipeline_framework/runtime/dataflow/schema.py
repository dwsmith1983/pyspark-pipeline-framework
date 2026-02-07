"""Schema-aware DataFlow for components with input/output contracts."""

from __future__ import annotations

from typing import Any

from pyspark_pipeline_framework.runtime.dataflow.base import DataFlow


class SchemaAwareDataFlow(DataFlow):
    """DataFlow subclass that declares input and output schemas.

    Satisfies the ``SchemaContract`` protocol from ``core.component.protocols``.
    Schema types are ``Any`` until ``core.schema.SchemaDefinition`` is
    implemented (ppf-1si).

    Override ``input_schema`` and/or ``output_schema`` to declare contracts:

        >>> class MyTransform(SchemaAwareDataFlow):
        ...     @property
        ...     def name(self) -> str:
        ...         return "typed-transform"
        ...
        ...     @property
        ...     def input_schema(self) -> Any:
        ...         return {"col_a": "string", "col_b": "int"}
        ...
        ...     def run(self) -> None:
        ...         ...
    """

    @property
    def input_schema(self) -> Any | None:
        """Schema this component expects as input, or None if undeclared."""
        return None

    @property
    def output_schema(self) -> Any | None:
        """Schema this component produces as output, or None if undeclared."""
        return None
