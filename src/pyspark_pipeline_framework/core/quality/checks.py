"""Built-in data quality check factories."""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark_pipeline_framework.core.quality.types import (
    CheckResult,
    CheckTiming,
    DataQualityCheck,
)

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
