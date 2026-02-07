"""Integration tests for schema_converter with a real SparkSession."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from pyspark_pipeline_framework.core.schema.definition import (
    DataType,
    SchemaDefinition,
    SchemaField,
)
from pyspark_pipeline_framework.runtime.schema_converter import (
    from_struct_type,
    to_struct_type,
)

pytestmark = [pytest.mark.spark, pytest.mark.integration]


class TestToStructType:
    """Convert SchemaDefinition → StructType and use it with real DataFrames."""

    def test_create_dataframe_with_converted_schema(self, spark: SparkSession) -> None:
        schema_def = SchemaDefinition(
            fields=[
                SchemaField("id", DataType.INTEGER, nullable=False),
                SchemaField("name", DataType.STRING),
                SchemaField("score", DataType.DOUBLE),
            ]
        )
        struct = to_struct_type(schema_def)

        data = [(1, "Alice", 95.5), (2, "Bob", 87.3)]
        df = spark.createDataFrame(data, schema=struct)

        assert df.count() == 2
        assert df.schema == struct
        assert df.schema["id"].dataType == IntegerType()
        assert df.schema["name"].dataType == StringType()
        assert df.schema["score"].dataType == DoubleType()

    def test_nullable_flags_preserved(self, spark: SparkSession) -> None:
        schema_def = SchemaDefinition(
            fields=[
                SchemaField("pk", DataType.LONG, nullable=False),
                SchemaField("value", DataType.STRING, nullable=True),
            ]
        )
        struct = to_struct_type(schema_def)
        df = spark.createDataFrame([(1, "a")], schema=struct)

        assert not df.schema["pk"].nullable
        assert df.schema["value"].nullable


class TestFromStructType:
    """Convert StructType → SchemaDefinition from a real DataFrame schema."""

    def test_round_trip(self, spark: SparkSession) -> None:
        original = SchemaDefinition(
            fields=[
                SchemaField("id", DataType.INTEGER, nullable=False),
                SchemaField("name", DataType.STRING),
                SchemaField("active", DataType.BOOLEAN),
            ]
        )
        struct = to_struct_type(original)
        df = spark.createDataFrame([(1, "Alice", True)], schema=struct)

        recovered = from_struct_type(df.schema, description="round-trip")

        assert recovered.description == "round-trip"
        assert len(recovered.fields) == 3
        assert recovered.fields[0].name == "id"
        assert recovered.fields[0].data_type == DataType.INTEGER
        assert recovered.fields[1].data_type == DataType.STRING
        assert recovered.fields[2].data_type == DataType.BOOLEAN

    def test_from_inferred_schema(self, spark: SparkSession) -> None:
        """from_struct_type works on schemas inferred from data."""
        df = spark.createDataFrame(
            [(1, "a", True), (2, "b", False)],
            schema=["id", "name", "flag"],
        )
        schema_def = from_struct_type(df.schema)

        names = {f.name for f in schema_def.fields}
        assert names == {"id", "name", "flag"}
