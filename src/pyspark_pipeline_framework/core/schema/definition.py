"""Schema definition models for pipeline data contracts."""

from __future__ import annotations

import contextlib
from dataclasses import dataclass, field
from enum import Enum


class DataType(str, Enum):
    """Platform-independent data types for schema definitions.

    The ``str`` mixin allows natural string comparison without ``.value``.
    """

    STRING = "string"
    INTEGER = "integer"
    LONG = "long"
    FLOAT = "float"
    DOUBLE = "double"
    BOOLEAN = "boolean"
    TIMESTAMP = "timestamp"
    DATE = "date"
    BINARY = "binary"
    ARRAY = "array"
    MAP = "map"
    STRUCT = "struct"


@dataclass
class SchemaField:
    """A single field within a schema definition.

    Args:
        name: Column/field name.
        data_type: A ``DataType`` enum member or a string for complex types
            (e.g. ``"array<string>"``).
        nullable: Whether the field accepts null values.
        metadata: Arbitrary key-value metadata for the field.
    """

    name: str
    data_type: DataType | str
    nullable: bool = True
    metadata: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Coerce string values to DataType when possible."""
        if isinstance(self.data_type, str):
            with contextlib.suppress(ValueError):
                self.data_type = DataType(self.data_type)


@dataclass
class SchemaDefinition:
    """A collection of fields describing a dataset's schema.

    Args:
        fields: Ordered list of schema fields.
        description: Optional human-readable description.
    """

    fields: list[SchemaField]
    description: str | None = None

    def field_names(self) -> set[str]:
        """Return the set of all field names."""
        return {f.name for f in self.fields}

    def get_field(self, name: str) -> SchemaField | None:
        """Look up a field by name.

        Returns:
            The matching ``SchemaField``, or ``None`` if not found.
        """
        for f in self.fields:
            if f.name == name:
                return f
        return None
