"""Tests for schema validator."""

from pyspark_pipeline_framework.core.schema.definition import (
    DataType,
    SchemaDefinition,
    SchemaField,
)
from pyspark_pipeline_framework.core.schema.validator import (
    SchemaValidator,
    ValidationSeverity,
)


class TestSchemaValidator:
    """Tests for SchemaValidator."""

    def setup_method(self) -> None:
        self.validator = SchemaValidator()

    def test_both_none_non_strict_is_valid(self) -> None:
        """Both schemas None in non-strict mode → valid."""
        result = self.validator.validate(None, None, "src", "tgt")
        assert result.valid is True
        assert result.issues == []

    def test_both_none_strict_is_error(self) -> None:
        """Both schemas None in strict mode → error."""
        result = self.validator.validate(None, None, "src", "tgt", strict=True)
        assert result.valid is False
        assert len(result.errors) == 1
        assert "strict" in result.errors[0].message.lower()

    def test_one_none_is_valid(self) -> None:
        """One schema None → valid (can't validate partial)."""
        schema = SchemaDefinition(
            fields=[SchemaField(name="a", data_type=DataType.STRING)]
        )
        result_no_output = self.validator.validate(None, schema, "src", "tgt")
        assert result_no_output.valid is True

        result_no_input = self.validator.validate(schema, None, "src", "tgt")
        assert result_no_input.valid is True

    def test_missing_required_field_is_error(self) -> None:
        """Missing non-nullable field in output → error."""
        output = SchemaDefinition(fields=[])
        input_schema = SchemaDefinition(
            fields=[
                SchemaField(name="id", data_type=DataType.LONG, nullable=False)
            ]
        )
        result = self.validator.validate(output, input_schema, "src", "tgt")
        assert result.valid is False
        assert len(result.errors) == 1
        assert "id" in result.errors[0].message

    def test_type_mismatch_is_error(self) -> None:
        """Type mismatch between fields → error."""
        output = SchemaDefinition(
            fields=[SchemaField(name="val", data_type=DataType.STRING)]
        )
        input_schema = SchemaDefinition(
            fields=[SchemaField(name="val", data_type=DataType.INTEGER)]
        )
        result = self.validator.validate(output, input_schema, "src", "tgt")
        assert result.valid is False
        assert len(result.errors) == 1
        assert "mismatch" in result.errors[0].message.lower()

    def test_nullability_violation_is_error(self) -> None:
        """Nullable output feeding non-nullable input → error."""
        output = SchemaDefinition(
            fields=[
                SchemaField(
                    name="val", data_type=DataType.STRING, nullable=True
                )
            ]
        )
        input_schema = SchemaDefinition(
            fields=[
                SchemaField(
                    name="val", data_type=DataType.STRING, nullable=False
                )
            ]
        )
        result = self.validator.validate(output, input_schema, "src", "tgt")
        assert result.valid is False
        assert any("nullable" in e.message.lower() for e in result.errors)

    def test_extra_output_fields_are_warnings(self) -> None:
        """Extra fields in output not in input → warnings only."""
        output = SchemaDefinition(
            fields=[
                SchemaField(name="id", data_type=DataType.LONG),
                SchemaField(name="extra", data_type=DataType.STRING),
            ]
        )
        input_schema = SchemaDefinition(
            fields=[SchemaField(name="id", data_type=DataType.LONG)]
        )
        result = self.validator.validate(output, input_schema, "src", "tgt")
        assert result.valid is True
        assert len(result.warnings) == 1
        assert "extra" in result.warnings[0].message

    def test_all_fields_match_is_valid(self) -> None:
        """Matching schemas produce no issues."""
        fields = [
            SchemaField(name="id", data_type=DataType.LONG, nullable=False),
            SchemaField(name="name", data_type=DataType.STRING),
        ]
        output = SchemaDefinition(fields=list(fields))
        input_schema = SchemaDefinition(fields=list(fields))
        result = self.validator.validate(output, input_schema, "src", "tgt")
        assert result.valid is True
        assert result.issues == []

    def test_multiple_issues_combined(self) -> None:
        """Multiple validation problems produce multiple issues."""
        output = SchemaDefinition(
            fields=[
                SchemaField(
                    name="a", data_type=DataType.STRING, nullable=True
                ),
                SchemaField(name="extra", data_type=DataType.BOOLEAN),
            ]
        )
        input_schema = SchemaDefinition(
            fields=[
                SchemaField(
                    name="a", data_type=DataType.INTEGER, nullable=False
                ),
                SchemaField(
                    name="b", data_type=DataType.LONG, nullable=False
                ),
            ]
        )
        result = self.validator.validate(output, input_schema, "src", "tgt")
        assert result.valid is False
        # a: type mismatch + nullability; b: missing required; extra: warning
        assert len(result.errors) >= 3
        assert len(result.warnings) == 1

    def test_validation_result_errors_property(self) -> None:
        """ValidationResult.errors filters by ERROR severity."""
        output = SchemaDefinition(
            fields=[
                SchemaField(name="id", data_type=DataType.LONG),
                SchemaField(name="extra", data_type=DataType.STRING),
            ]
        )
        input_schema = SchemaDefinition(
            fields=[
                SchemaField(
                    name="id", data_type=DataType.STRING, nullable=False
                )
            ]
        )
        result = self.validator.validate(output, input_schema, "src", "tgt")
        assert all(
            e.severity is ValidationSeverity.ERROR for e in result.errors
        )
        assert all(
            w.severity is ValidationSeverity.WARNING for w in result.warnings
        )

    def test_empty_schemas_valid(self) -> None:
        """Two schemas with no fields are valid."""
        output = SchemaDefinition(fields=[])
        input_schema = SchemaDefinition(fields=[])
        result = self.validator.validate(output, input_schema, "src", "tgt")
        assert result.valid is True
        assert result.issues == []

    def test_complex_type_mismatch(self) -> None:
        """Complex string types that differ produce an error."""
        output = SchemaDefinition(
            fields=[
                SchemaField(name="col", data_type="array<string>")
            ]
        )
        input_schema = SchemaDefinition(
            fields=[
                SchemaField(name="col", data_type="array<integer>")
            ]
        )
        result = self.validator.validate(output, input_schema, "src", "tgt")
        assert result.valid is False
        assert len(result.errors) == 1

    def test_result_component_names(self) -> None:
        """ValidationResult captures source and target component names."""
        result = self.validator.validate(None, None, "source_a", "target_b")
        assert result.source_component == "source_a"
        assert result.target_component == "target_b"
