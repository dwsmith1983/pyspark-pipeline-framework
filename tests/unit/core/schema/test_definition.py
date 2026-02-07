"""Tests for schema definition models."""

from pyspark_pipeline_framework.core.schema.definition import (
    DataType,
    SchemaDefinition,
    SchemaField,
)


class TestDataType:
    """Tests for the DataType enum."""

    def test_string_comparison(self) -> None:
        """DataType members compare equal to their string values."""
        assert DataType.STRING == "string"
        assert DataType.INTEGER == "integer"
        assert DataType.BOOLEAN == "boolean"

    def test_all_members_exist(self) -> None:
        """All expected platform-independent types are defined."""
        expected = {
            "STRING",
            "INTEGER",
            "LONG",
            "FLOAT",
            "DOUBLE",
            "BOOLEAN",
            "TIMESTAMP",
            "DATE",
            "BINARY",
            "ARRAY",
            "MAP",
            "STRUCT",
        }
        assert {m.name for m in DataType} == expected


class TestSchemaField:
    """Tests for SchemaField dataclass."""

    def test_default_values(self) -> None:
        """SchemaField has sensible defaults."""
        field = SchemaField(name="col", data_type=DataType.STRING)
        assert field.name == "col"
        assert field.data_type is DataType.STRING
        assert field.nullable is True
        assert field.metadata == {}

    def test_string_coercion_to_datatype(self) -> None:
        """A string matching an enum value is coerced to DataType."""
        field = SchemaField(name="id", data_type="integer")
        assert field.data_type is DataType.INTEGER

    def test_complex_type_stays_string(self) -> None:
        """Complex types like 'array<string>' are kept as plain strings."""
        field = SchemaField(name="tags", data_type="array<string>")
        assert field.data_type == "array<string>"
        assert isinstance(field.data_type, str)

    def test_metadata_preserved(self) -> None:
        """Custom metadata is preserved."""
        meta = {"source": "csv", "description": "User ID"}
        field = SchemaField(
            name="user_id", data_type=DataType.LONG, metadata=meta
        )
        assert field.metadata == meta

    def test_non_nullable_field(self) -> None:
        """Fields can be marked as non-nullable."""
        field = SchemaField(
            name="pk", data_type=DataType.LONG, nullable=False
        )
        assert field.nullable is False


class TestSchemaDefinition:
    """Tests for SchemaDefinition dataclass."""

    def test_field_names(self) -> None:
        """field_names() returns the set of all field names."""
        schema = SchemaDefinition(
            fields=[
                SchemaField(name="a", data_type=DataType.STRING),
                SchemaField(name="b", data_type=DataType.INTEGER),
            ]
        )
        assert schema.field_names() == {"a", "b"}

    def test_get_field_found(self) -> None:
        """get_field returns the matching field."""
        field_a = SchemaField(name="a", data_type=DataType.STRING)
        schema = SchemaDefinition(fields=[field_a])
        assert schema.get_field("a") is field_a

    def test_get_field_not_found(self) -> None:
        """get_field returns None for unknown names."""
        schema = SchemaDefinition(
            fields=[SchemaField(name="a", data_type=DataType.STRING)]
        )
        assert schema.get_field("z") is None

    def test_empty_fields_list(self) -> None:
        """A schema with no fields is valid."""
        schema = SchemaDefinition(fields=[])
        assert schema.field_names() == set()
        assert schema.get_field("x") is None

    def test_description(self) -> None:
        """Schema description is stored."""
        schema = SchemaDefinition(fields=[], description="Test schema")
        assert schema.description == "Test schema"
