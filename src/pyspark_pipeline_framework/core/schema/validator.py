"""Schema validation for pipeline data flow contracts."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from pyspark_pipeline_framework.core.schema.definition import SchemaDefinition


class ValidationSeverity(Enum):
    """Severity level for schema validation issues."""

    ERROR = "error"
    WARNING = "warning"


@dataclass
class ValidationIssue:
    """A single issue discovered during schema validation.

    Args:
        severity: Whether this is a blocking error or an informational warning.
        field_name: The field that caused the issue, or ``None`` for schema-level issues.
        message: Human-readable description of the issue.
        source_component: Name of the upstream (output) component.
        target_component: Name of the downstream (input) component.
    """

    severity: ValidationSeverity
    field_name: str | None
    message: str
    source_component: str
    target_component: str


@dataclass
class ValidationResult:
    """Outcome of validating an output schema against an input schema.

    Args:
        valid: ``True`` when there are no ERROR-level issues.
        issues: All discovered issues (errors and warnings).
        source_component: Name of the upstream component.
        target_component: Name of the downstream component.
    """

    valid: bool
    issues: list[ValidationIssue]
    source_component: str
    target_component: str

    @property
    def errors(self) -> list[ValidationIssue]:
        """Return only ERROR-level issues."""
        return [i for i in self.issues if i.severity is ValidationSeverity.ERROR]

    @property
    def warnings(self) -> list[ValidationIssue]:
        """Return only WARNING-level issues."""
        return [i for i in self.issues if i.severity is ValidationSeverity.WARNING]


class SchemaValidator:
    """Validates that an output schema is compatible with an input schema.

    Rules applied:
    - Both ``None`` + not strict → valid (nothing to check).
    - Both ``None`` + strict → ERROR (schemas must be declared).
    - One ``None`` → valid (partial schemas cannot be validated).
    - Missing required input field in output → ERROR.
    - Type mismatch between matching fields → ERROR.
    - Non-nullable input field backed by nullable output → ERROR.
    - Extra output fields not in input → WARNING.
    """

    def validate(
        self,
        output_schema: SchemaDefinition | None,
        input_schema: SchemaDefinition | None,
        source_component: str,
        target_component: str,
        *,
        strict: bool = False,
    ) -> ValidationResult:
        """Validate compatibility between an output and input schema.

        Args:
            output_schema: Schema produced by the source component.
            input_schema: Schema expected by the target component.
            source_component: Name of the source component.
            target_component: Name of the target component.
            strict: When ``True``, require both schemas to be declared.

        Returns:
            A ``ValidationResult`` summarising all issues found.
        """
        issues: list[ValidationIssue] = []

        # Both None
        if output_schema is None and input_schema is None:
            if strict:
                issues.append(
                    ValidationIssue(
                        severity=ValidationSeverity.ERROR,
                        field_name=None,
                        message="Both components must declare schemas in strict mode",
                        source_component=source_component,
                        target_component=target_component,
                    )
                )
                return ValidationResult(
                    valid=False,
                    issues=issues,
                    source_component=source_component,
                    target_component=target_component,
                )
            return ValidationResult(
                valid=True,
                issues=[],
                source_component=source_component,
                target_component=target_component,
            )

        # One None → can't validate partial
        if output_schema is None or input_schema is None:
            return ValidationResult(
                valid=True,
                issues=[],
                source_component=source_component,
                target_component=target_component,
            )

        # Both present — field-level checks
        output_names = output_schema.field_names()

        for input_field in input_schema.fields:
            output_field = output_schema.get_field(input_field.name)

            if output_field is None:
                if not input_field.nullable:
                    issues.append(
                        ValidationIssue(
                            severity=ValidationSeverity.ERROR,
                            field_name=input_field.name,
                            message=(
                                f"Required field '{input_field.name}' is missing from output of '{source_component}'"
                            ),
                            source_component=source_component,
                            target_component=target_component,
                        )
                    )
                continue

            # Type mismatch
            if str(output_field.data_type) != str(input_field.data_type):
                issues.append(
                    ValidationIssue(
                        severity=ValidationSeverity.ERROR,
                        field_name=input_field.name,
                        message=(
                            f"Type mismatch for field '{input_field.name}': "
                            f"output is '{output_field.data_type}', "
                            f"input expects '{input_field.data_type}'"
                        ),
                        source_component=source_component,
                        target_component=target_component,
                    )
                )

            # Nullability: non-nullable input fed by nullable output
            if not input_field.nullable and output_field.nullable:
                issues.append(
                    ValidationIssue(
                        severity=ValidationSeverity.ERROR,
                        field_name=input_field.name,
                        message=(
                            f"Nullability mismatch for field '{input_field.name}': "
                            f"output is nullable but input requires non-nullable"
                        ),
                        source_component=source_component,
                        target_component=target_component,
                    )
                )

        # Extra output fields → warning
        input_names = input_schema.field_names()
        for extra_name in sorted(output_names - input_names):
            issues.append(
                ValidationIssue(
                    severity=ValidationSeverity.WARNING,
                    field_name=extra_name,
                    message=(
                        f"Extra field '{extra_name}' in output of "
                        f"'{source_component}' not consumed by '{target_component}'"
                    ),
                    source_component=source_component,
                    target_component=target_component,
                )
            )

        has_errors = any(i.severity is ValidationSeverity.ERROR for i in issues)
        return ValidationResult(
            valid=not has_errors,
            issues=issues,
            source_component=source_component,
            target_component=target_component,
        )
