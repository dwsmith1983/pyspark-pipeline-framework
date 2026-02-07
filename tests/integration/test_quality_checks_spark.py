"""Integration tests for DQ check factories with a real SparkSession."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from pyspark_pipeline_framework.core.quality.checks import (
    custom_sql_check,
    null_check,
    range_check,
    row_count_check,
    schema_check,
    unique_check,
)
from pyspark_pipeline_framework.core.schema.definition import (
    DataType,
    SchemaDefinition,
    SchemaField,
)

pytestmark = [pytest.mark.spark, pytest.mark.integration]

VIEW = "dq_test_data"


@pytest.fixture(autouse=True)
def _register_view(spark: SparkSession) -> None:
    """Register a shared test view for all DQ tests."""
    df = spark.createDataFrame(
        [
            (1, "Alice", 25, 85.0),
            (2, "Bob", 30, 92.5),
            (3, "Alice", 35, 78.0),
            (4, None, 40, None),
        ],
        schema=["id", "name", "age", "score"],
    )
    df.createOrReplaceTempView(VIEW)


class TestRowCountCheck:
    def test_passes_when_enough_rows(self, spark: SparkSession) -> None:
        check = row_count_check(VIEW, min_rows=3)
        result = check.check_fn(spark)
        assert result.passed

    def test_fails_when_too_few_rows(self, spark: SparkSession) -> None:
        check = row_count_check(VIEW, min_rows=10)
        result = check.check_fn(spark)
        assert not result.passed
        assert result.details is not None
        assert result.details["count"] == 4


class TestNullCheck:
    def test_passes_for_non_null_column(self, spark: SparkSession) -> None:
        check = null_check(VIEW, "id")
        result = check.check_fn(spark)
        assert result.passed

    def test_fails_for_nullable_column(self, spark: SparkSession) -> None:
        check = null_check(VIEW, "name", max_null_pct=0.0)
        result = check.check_fn(spark)
        assert not result.passed
        assert result.details is not None
        assert result.details["null_count"] == 1

    def test_passes_with_threshold(self, spark: SparkSession) -> None:
        check = null_check(VIEW, "name", max_null_pct=30.0)
        result = check.check_fn(spark)
        assert result.passed


class TestUniqueCheck:
    def test_unique_column_passes(self, spark: SparkSession) -> None:
        check = unique_check(VIEW, "id")
        result = check.check_fn(spark)
        assert result.passed
        assert result.details is not None
        assert result.details["duplicates"] == 0

    def test_non_unique_column_fails(self, spark: SparkSession) -> None:
        check = unique_check(VIEW, "name")
        result = check.check_fn(spark)
        assert not result.passed
        assert result.details is not None
        assert result.details["duplicates"] > 0

    def test_composite_key(self, spark: SparkSession) -> None:
        check = unique_check(VIEW, ["id", "name"])
        result = check.check_fn(spark)
        assert result.passed


class TestRangeCheck:
    def test_in_range_passes(self, spark: SparkSession) -> None:
        check = range_check(VIEW, "age", min_value=20, max_value=50)
        result = check.check_fn(spark)
        assert result.passed

    def test_out_of_range_fails(self, spark: SparkSession) -> None:
        check = range_check(VIEW, "age", min_value=30, max_value=50)
        result = check.check_fn(spark)
        assert not result.passed
        assert result.details is not None
        assert result.details["violations"] == 1  # age=25 is below 30

    def test_min_only(self, spark: SparkSession) -> None:
        check = range_check(VIEW, "age", min_value=0)
        result = check.check_fn(spark)
        assert result.passed

    def test_max_only(self, spark: SparkSession) -> None:
        check = range_check(VIEW, "age", max_value=100)
        result = check.check_fn(spark)
        assert result.passed


class TestSchemaCheck:
    def test_matching_schema_passes(self, spark: SparkSession) -> None:
        schema = SchemaDefinition(
            fields=[
                SchemaField("id", DataType.LONG),
                SchemaField("name", DataType.STRING),
            ]
        )
        check = schema_check(VIEW, schema)
        result = check.check_fn(spark)
        assert result.passed

    def test_missing_column_fails(self, spark: SparkSession) -> None:
        schema = SchemaDefinition(
            fields=[
                SchemaField("id", DataType.LONG),
                SchemaField("nonexistent", DataType.STRING),
            ]
        )
        check = schema_check(VIEW, schema)
        result = check.check_fn(spark)
        assert not result.passed
        assert result.details is not None
        assert "nonexistent" in result.details["missing"]

    def test_type_mismatch_fails(self, spark: SparkSession) -> None:
        schema = SchemaDefinition(
            fields=[
                SchemaField("id", DataType.STRING),  # actual is long
            ]
        )
        check = schema_check(VIEW, schema)
        result = check.check_fn(spark)
        assert not result.passed
        assert result.details is not None
        assert len(result.details["type_mismatches"]) == 1

    def test_names_only_ignores_types(self, spark: SparkSession) -> None:
        schema = SchemaDefinition(
            fields=[
                SchemaField("id", DataType.STRING),  # wrong type, but OK
            ]
        )
        check = schema_check(VIEW, schema, check_types=False)
        result = check.check_fn(spark)
        assert result.passed


class TestCustomSqlCheck:
    def test_passing_sql(self, spark: SparkSession) -> None:
        check = custom_sql_check(
            name="all_positive_ids",
            sql=f"SELECT COUNT(*) = 0 AS passed FROM {VIEW} WHERE id < 0",
        )
        result = check.check_fn(spark)
        assert result.passed

    def test_failing_sql(self, spark: SparkSession) -> None:
        check = custom_sql_check(
            name="no_nulls",
            sql=f"SELECT COUNT(*) = 0 AS passed FROM {VIEW} WHERE name IS NULL",
        )
        result = check.check_fn(spark)
        assert not result.passed

    def test_custom_message(self, spark: SparkSession) -> None:
        check = custom_sql_check(
            name="with_msg",
            sql=f"SELECT true AS passed, 'all good' AS message",
        )
        result = check.check_fn(spark)
        assert result.passed
        assert result.message == "all good"
