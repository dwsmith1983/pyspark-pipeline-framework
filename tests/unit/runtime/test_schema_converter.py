"""Tests for runtime.schema_converter."""

from __future__ import annotations

import pytest
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
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


class TestToStructType:
    """Tests for to_struct_type()."""

    def test_simple_schema(self) -> None:
        schema = SchemaDefinition(
            fields=[
                SchemaField(name="id", data_type=DataType.INTEGER, nullable=False),
                SchemaField(name="name", data_type=DataType.STRING),
                SchemaField(name="score", data_type=DataType.DOUBLE),
            ]
        )
        result = to_struct_type(schema)
        assert isinstance(result, StructType)
        assert len(result.fields) == 3
        assert result.fields[0] == StructField("id", IntegerType(), nullable=False)
        assert result.fields[1] == StructField("name", StringType(), nullable=True)
        assert result.fields[2] == StructField("score", DoubleType(), nullable=True)

    def test_all_simple_types(self) -> None:
        type_mapping = {
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
        for dt, expected_spark in type_mapping.items():
            schema = SchemaDefinition(
                fields=[SchemaField(name="col", data_type=dt)]
            )
            result = to_struct_type(schema)
            assert result.fields[0].dataType == expected_spark, f"Failed for {dt}"

    def test_string_data_type_coercion(self) -> None:
        """String values that match DataType enum are auto-coerced."""
        schema = SchemaDefinition(
            fields=[SchemaField(name="col", data_type="string")]
        )
        result = to_struct_type(schema)
        assert result.fields[0].dataType == StringType()

    def test_metadata_preserved(self) -> None:
        schema = SchemaDefinition(
            fields=[
                SchemaField(
                    name="col",
                    data_type=DataType.STRING,
                    metadata={"comment": "test"},
                )
            ]
        )
        result = to_struct_type(schema)
        assert result.fields[0].metadata == {"comment": "test"}

    def test_nullable_preserved(self) -> None:
        schema = SchemaDefinition(
            fields=[
                SchemaField(name="a", data_type=DataType.INTEGER, nullable=False),
                SchemaField(name="b", data_type=DataType.INTEGER, nullable=True),
            ]
        )
        result = to_struct_type(schema)
        assert result.fields[0].nullable is False
        assert result.fields[1].nullable is True

    def test_complex_type_raises(self) -> None:
        for dt in (DataType.ARRAY, DataType.MAP, DataType.STRUCT):
            schema = SchemaDefinition(
                fields=[SchemaField(name="col", data_type=dt)]
            )
            with pytest.raises(ValueError, match="Complex type"):
                to_struct_type(schema)

    def test_unknown_string_type_raises(self) -> None:
        field = SchemaField.__new__(SchemaField)
        field.name = "col"
        field.data_type = "not_a_real_type"
        field.nullable = True
        field.metadata = {}
        schema = SchemaDefinition(fields=[field])
        with pytest.raises(ValueError, match="Cannot convert"):
            to_struct_type(schema)

    def test_empty_schema(self) -> None:
        schema = SchemaDefinition(fields=[])
        result = to_struct_type(schema)
        assert isinstance(result, StructType)
        assert len(result.fields) == 0


class TestFromStructType:
    """Tests for from_struct_type()."""

    def test_simple_struct(self) -> None:
        struct = StructType(
            [
                StructField("id", IntegerType(), nullable=False),
                StructField("name", StringType(), nullable=True),
            ]
        )
        result = from_struct_type(struct)
        assert isinstance(result, SchemaDefinition)
        assert len(result.fields) == 2
        assert result.fields[0].name == "id"
        assert result.fields[0].data_type == DataType.INTEGER
        assert result.fields[0].nullable is False
        assert result.fields[1].name == "name"
        assert result.fields[1].data_type == DataType.STRING
        assert result.fields[1].nullable is True

    def test_all_simple_types_roundtrip(self) -> None:
        spark_to_dt = {
            StringType(): DataType.STRING,
            IntegerType(): DataType.INTEGER,
            LongType(): DataType.LONG,
            FloatType(): DataType.FLOAT,
            DoubleType(): DataType.DOUBLE,
            BooleanType(): DataType.BOOLEAN,
            TimestampType(): DataType.TIMESTAMP,
            DateType(): DataType.DATE,
            BinaryType(): DataType.BINARY,
        }
        for spark_type, expected_dt in spark_to_dt.items():
            struct = StructType([StructField("col", spark_type)])
            result = from_struct_type(struct)
            assert result.fields[0].data_type == expected_dt, f"Failed for {spark_type}"

    def test_description_passed_through(self) -> None:
        struct = StructType([StructField("col", StringType())])
        result = from_struct_type(struct, description="my schema")
        assert result.description == "my schema"

    def test_metadata_preserved(self) -> None:
        struct = StructType(
            [StructField("col", StringType(), metadata={"comment": "test"})]
        )
        result = from_struct_type(struct)
        assert result.fields[0].metadata == {"comment": "test"}

    def test_complex_type_returns_enum(self) -> None:
        struct = StructType(
            [
                StructField("arr", ArrayType(StringType())),
                StructField("m", MapType(StringType(), IntegerType())),
                StructField("s", StructType([StructField("x", IntegerType())])),
            ]
        )
        result = from_struct_type(struct)
        assert result.fields[0].data_type == DataType.ARRAY
        assert result.fields[1].data_type == DataType.MAP
        assert result.fields[2].data_type == DataType.STRUCT

    def test_empty_struct(self) -> None:
        result = from_struct_type(StructType([]))
        assert len(result.fields) == 0


class TestRoundTrip:
    """Test converting SchemaDefinition → StructType → SchemaDefinition."""

    def test_simple_roundtrip(self) -> None:
        original = SchemaDefinition(
            fields=[
                SchemaField(name="id", data_type=DataType.LONG, nullable=False),
                SchemaField(name="name", data_type=DataType.STRING),
                SchemaField(
                    name="active",
                    data_type=DataType.BOOLEAN,
                    metadata={"tag": "flag"},
                ),
            ],
            description="test schema",
        )
        struct = to_struct_type(original)
        restored = from_struct_type(struct, description="test schema")

        assert len(restored.fields) == len(original.fields)
        for orig, rest in zip(original.fields, restored.fields, strict=True):
            assert orig.name == rest.name
            assert orig.data_type == rest.data_type
            assert orig.nullable == rest.nullable
        assert restored.description == original.description
