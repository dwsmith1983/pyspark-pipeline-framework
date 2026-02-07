"""Built-in data quality check factories."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark_pipeline_framework.core.quality.types import CheckResult, CheckTiming, DataQualityCheck
from pyspark_pipeline_framework.core.schema.definition import SchemaDefinition

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


def row_count_check(
    table: str,
    min_rows: int,
    timing: CheckTiming = CheckTiming.AFTER_PIPELINE,
    component_name: str | None = None,
) -> DataQualityCheck:
    """Check that a table has at least *min_rows* rows."""

    def check(spark: SparkSession) -> CheckResult:
        count = spark.table(table).count()
        passed = count >= min_rows
        return CheckResult(
            check_name=f"row_count_{table}",
            passed=passed,
            message=f"Table {table} has {count} rows (min: {min_rows})",
            details={"count": count, "min_rows": min_rows},
        )

    return DataQualityCheck(
        name=f"row_count_{table}",
        timing=timing,
        check_fn=check,
        component_name=component_name,
    )


def null_check(
    table: str,
    column: str,
    max_null_pct: float = 0.0,
    timing: CheckTiming = CheckTiming.AFTER_PIPELINE,
    component_name: str | None = None,
) -> DataQualityCheck:
    """Check that *column* has at most *max_null_pct* % null values."""

    def check(spark: SparkSession) -> CheckResult:
        df = spark.table(table)
        total = df.count()
        nulls = df.filter(f"{column} IS NULL").count()
        pct = (nulls / total * 100) if total > 0 else 0.0
        passed = pct <= max_null_pct
        return CheckResult(
            check_name=f"null_check_{table}_{column}",
            passed=passed,
            message=f"Column {column} has {pct:.2f}% nulls (max: {max_null_pct}%)",
            details={"null_count": nulls, "total": total, "pct": pct},
        )

    return DataQualityCheck(
        name=f"null_check_{table}_{column}",
        timing=timing,
        check_fn=check,
        component_name=component_name,
    )


def unique_check(
    table: str,
    columns: str | list[str],
    timing: CheckTiming = CheckTiming.AFTER_PIPELINE,
    component_name: str | None = None,
) -> DataQualityCheck:
    """Check that *columns* form a unique key (no duplicates).

    Args:
        table: Registered temp view or table name.
        columns: Single column name or list of columns for composite
            uniqueness.
        timing: When to run the check.
        component_name: For ``AFTER_COMPONENT`` timing, which component
            to attach the check to.
    """
    cols = [columns] if isinstance(columns, str) else list(columns)
    col_label = "_".join(cols)

    def check(spark: SparkSession) -> CheckResult:
        df = spark.table(table)
        total = df.count()
        distinct = df.select(*cols).distinct().count()
        duplicates = total - distinct
        passed = duplicates == 0
        return CheckResult(
            check_name=f"unique_{table}_{col_label}",
            passed=passed,
            message=(f"Columns [{', '.join(cols)}] in {table}: {duplicates} duplicate rows out of {total}"),
            details={"total": total, "distinct": distinct, "duplicates": duplicates},
        )

    return DataQualityCheck(
        name=f"unique_{table}_{col_label}",
        timing=timing,
        check_fn=check,
        component_name=component_name,
    )


def range_check(
    table: str,
    column: str,
    min_value: float | int | None = None,
    max_value: float | int | None = None,
    timing: CheckTiming = CheckTiming.AFTER_PIPELINE,
    component_name: str | None = None,
) -> DataQualityCheck:
    """Check that all values in *column* fall within [min_value, max_value].

    At least one of *min_value* or *max_value* must be provided.

    Args:
        table: Registered temp view or table name.
        column: Numeric column to validate.
        min_value: Minimum allowed value (inclusive), or ``None`` for no
            lower bound.
        max_value: Maximum allowed value (inclusive), or ``None`` for no
            upper bound.
        timing: When to run the check.
        component_name: For ``AFTER_COMPONENT`` timing, which component
            to attach the check to.

    Raises:
        ValueError: If neither *min_value* nor *max_value* is provided.
    """
    if min_value is None and max_value is None:
        msg = "At least one of min_value or max_value must be provided"
        raise ValueError(msg)

    def check(spark: SparkSession) -> CheckResult:
        df = spark.table(table)
        conditions: list[str] = []
        if min_value is not None:
            conditions.append(f"{column} < {min_value}")
        if max_value is not None:
            conditions.append(f"{column} > {max_value}")
        filter_expr = " OR ".join(conditions)
        violations = df.filter(filter_expr).count()
        total = df.count()
        passed = violations == 0
        if min_value is not None and max_value is not None:
            bound_desc = f"[{min_value}, {max_value}]"
        elif min_value is not None:
            bound_desc = f"[{min_value}, ∞)"
        else:
            bound_desc = f"(-∞, {max_value}]"
        return CheckResult(
            check_name=f"range_{table}_{column}",
            passed=passed,
            message=(f"Column {column} in {table}: {violations} out-of-range rows (expected {bound_desc})"),
            details={
                "violations": violations,
                "total": total,
                "min_value": min_value,
                "max_value": max_value,
            },
        )

    return DataQualityCheck(
        name=f"range_{table}_{column}",
        timing=timing,
        check_fn=check,
        component_name=component_name,
    )


# Mapping from DataType enum values to Spark type name strings.
_DATATYPE_TO_SPARK: dict[str, str] = {
    "string": "string",
    "integer": "int",
    "long": "bigint",
    "float": "float",
    "double": "double",
    "boolean": "boolean",
    "timestamp": "timestamp",
    "date": "date",
    "binary": "binary",
    "array": "array",
    "map": "map",
    "struct": "struct",
}


def schema_check(
    table: str,
    schema: SchemaDefinition,
    check_types: bool = True,
    timing: CheckTiming = CheckTiming.AFTER_PIPELINE,
    component_name: str | None = None,
) -> DataQualityCheck:
    """Check that *table* matches the expected *schema*.

    Verifies that all fields defined in *schema* exist in the table.
    When *check_types* is ``True`` (the default), also verifies that
    each field's data type matches.

    Args:
        table: Registered temp view or table name.
        schema: Expected schema definition.
        check_types: Whether to validate data types in addition to
            column names.
        timing: When to run the check.
        component_name: For ``AFTER_COMPONENT`` timing, which component
            to attach the check to.
    """

    def check(spark: SparkSession) -> CheckResult:
        df = spark.table(table)
        actual_dtypes = dict(df.dtypes)  # {"col": "type_string"}
        missing: list[str] = []
        type_mismatches: list[str] = []

        for field in schema.fields:
            if field.name not in actual_dtypes:
                missing.append(field.name)
            elif check_types:
                dt_key = field.data_type.value if hasattr(field.data_type, "value") else str(field.data_type)
                expected_type = _DATATYPE_TO_SPARK.get(dt_key, dt_key)
                actual_type = actual_dtypes[field.name]
                if actual_type != expected_type:
                    type_mismatches.append(f"{field.name}: expected {expected_type}, got {actual_type}")

        passed = len(missing) == 0 and len(type_mismatches) == 0
        issues: list[str] = []
        if missing:
            issues.append(f"missing columns: {missing}")
        if type_mismatches:
            issues.append(f"type mismatches: {type_mismatches}")

        return CheckResult(
            check_name=f"schema_{table}",
            passed=passed,
            message=(f"Schema check for {table}: {'OK' if passed else '; '.join(issues)}"),
            details={
                "missing": missing,
                "type_mismatches": type_mismatches,
            },
        )

    return DataQualityCheck(
        name=f"schema_{table}",
        timing=timing,
        check_fn=check,
        component_name=component_name,
    )


def custom_sql_check(
    name: str,
    sql: str,
    timing: CheckTiming = CheckTiming.AFTER_PIPELINE,
    component_name: str | None = None,
) -> DataQualityCheck:
    """Run an arbitrary SQL expression as a data quality check.

    The *sql* statement must return a single row with at least a
    ``passed`` column (boolean).  An optional ``message`` column
    provides a custom result message.

    Example::

        custom_sql_check(
            name="no_negative_amounts",
            sql="SELECT COUNT(*) = 0 AS passed FROM orders WHERE amount < 0",
        )

    Args:
        name: Unique check name.
        sql: SQL query returning a row with a ``passed`` boolean column.
        timing: When to run the check.
        component_name: For ``AFTER_COMPONENT`` timing, which component
            to attach the check to.
    """

    def check(spark: SparkSession) -> CheckResult:
        row = spark.sql(sql).head()
        if row is None:
            return CheckResult(
                check_name=name,
                passed=False,
                message=f"Custom SQL check '{name}': query returned no rows",
                details={"sql": sql},
            )
        passed = bool(row["passed"])
        message = str(row["message"]) if "message" in row.asDict() else ""
        return CheckResult(
            check_name=name,
            passed=passed,
            message=message or f"Custom SQL check '{name}': {'PASSED' if passed else 'FAILED'}",
            details={"sql": sql},
        )

    return DataQualityCheck(
        name=name,
        timing=timing,
        check_fn=check,
        component_name=component_name,
    )
