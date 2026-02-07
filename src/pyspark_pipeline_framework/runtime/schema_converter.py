"""Bidirectional conversion between SchemaDefinition and PySpark StructType.

Provides ``to_struct_type`` and ``from_struct_type`` for converting between
the framework's platform-independent schema model and PySpark's native types.
PySpark imports are deferred to function bodies so the module can be imported
without a Spark runtime.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from pyspark_pipeline_framework.core.schema.definition import DataType, SchemaDefinition, SchemaField

if TYPE_CHECKING:
    from pyspark.sql.types import DataType as SparkDataType
    from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)


def _get_spark_type_map() -> dict[DataType, SparkDataType]:
    """Return mapping from framework DataType to PySpark type instances.

    Imports PySpark types lazily so this module remains importable
    without a Spark installation.
    """
    from pyspark.sql.types import (
        BinaryType,
        BooleanType,
        DateType,
        DoubleType,
        FloatType,
        IntegerType,
        LongType,
        StringType,
        TimestampType,
    )

    return {
        DataType.STRING: StringType(),
        DataType.INTEGER: IntegerType(),
        DataType.LONG: LongType(),
        DataType.FLOAT: FloatType(),
        DataType.DOUBLE: DoubleType(),
        DataType.BOOLEAN: BooleanType(),
        DataType.TIMESTAMP: TimestampType(),
        DataType.DATE: DateType(),
        DataType.BINARY: BinaryType(),
    }


def _get_reverse_type_map() -> dict[str, DataType]:
    """Return mapping from PySpark type class name to framework DataType."""
    return {
        "StringType": DataType.STRING,
        "IntegerType": DataType.INTEGER,
        "LongType": DataType.LONG,
        "FloatType": DataType.FLOAT,
        "DoubleType": DataType.DOUBLE,
        "BooleanType": DataType.BOOLEAN,
        "TimestampType": DataType.TIMESTAMP,
        "DateType": DataType.DATE,
        "BinaryType": DataType.BINARY,
    }


_COMPLEX_TYPES = frozenset({DataType.ARRAY, DataType.MAP, DataType.STRUCT})


def _data_type_to_spark(dt: DataType | str) -> SparkDataType:
    """Convert a single framework data type to its PySpark equivalent.

    Args:
        dt: A ``DataType`` enum member or string representation.

    Returns:
        The corresponding PySpark ``DataType`` instance.

    Raises:
        ValueError: If the type cannot be mapped (e.g. complex types without
            nested type information).
    """
    type_map = _get_spark_type_map()

    if isinstance(dt, DataType):
        if dt in _COMPLEX_TYPES:
            raise ValueError(
                f"Complex type '{dt.value}' requires nested type information "
                f"that SchemaField does not carry. Use PySpark types directly "
                f"for complex schemas."
            )
        spark_type = type_map.get(dt)
        if spark_type is None:
            raise ValueError(f"Unsupported DataType: {dt}")
        return spark_type

    # String value — try to coerce to DataType enum first
    try:
        enum_dt = DataType(dt)
        return _data_type_to_spark(enum_dt)
    except ValueError:
        raise ValueError(f"Cannot convert data type '{dt}' to a PySpark type") from None


def _spark_to_data_type(spark_type: SparkDataType) -> DataType | str:
    """Convert a PySpark data type to the framework equivalent.

    Args:
        spark_type: A PySpark ``DataType`` instance.

    Returns:
        The corresponding ``DataType`` enum member, or a string
        description for complex types.
    """
    from pyspark.sql.types import ArrayType, MapType, StructType

    reverse_map = _get_reverse_type_map()
    type_name = type(spark_type).__name__

    if type_name in reverse_map:
        return reverse_map[type_name]

    if isinstance(spark_type, ArrayType):
        return DataType.ARRAY
    if isinstance(spark_type, MapType):
        return DataType.MAP
    if isinstance(spark_type, StructType):
        return DataType.STRUCT

    return type_name


def to_struct_type(schema: SchemaDefinition) -> StructType:
    """Convert a ``SchemaDefinition`` to a PySpark ``StructType``.

    Args:
        schema: The framework schema definition.

    Returns:
        A PySpark ``StructType`` with corresponding fields.

    Raises:
        ValueError: If any field type cannot be mapped to a PySpark type.
    """
    from pyspark.sql.types import StructField, StructType

    fields = []
    for sf in schema.fields:
        spark_type = _data_type_to_spark(sf.data_type)
        metadata = sf.metadata if sf.metadata else None
        fields.append(StructField(sf.name, spark_type, nullable=sf.nullable, metadata=metadata))

    return StructType(fields)


def from_struct_type(
    struct_type: StructType,
    description: str | None = None,
) -> SchemaDefinition:
    """Convert a PySpark ``StructType`` to a ``SchemaDefinition``.

    Args:
        struct_type: A PySpark ``StructType``.
        description: Optional description for the resulting schema.

    Returns:
        A ``SchemaDefinition`` with corresponding fields.
    """
    fields = []
    for spark_field in struct_type.fields:
        dt = _spark_to_data_type(spark_field.dataType)
        metadata = dict(spark_field.metadata) if spark_field.metadata else {}
        fields.append(
            SchemaField(
                name=spark_field.name,
                data_type=dt,
                nullable=spark_field.nullable,
                metadata=metadata,
            )
        )

    return SchemaDefinition(fields=fields, description=description)
