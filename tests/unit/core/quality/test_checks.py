"""Tests for built-in data quality check factories."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pyspark_pipeline_framework.core.quality.checks import (
    custom_sql_check,
    null_check,
    range_check,
    row_count_check,
    schema_check,
    unique_check,
)
from pyspark_pipeline_framework.core.quality.types import (
    CheckTiming,
)
from pyspark_pipeline_framework.core.schema.definition import (
    DataType,
    SchemaDefinition,
    SchemaField,
)


class TestRowCountCheck:
    def test_passes_when_enough_rows(self) -> None:
        spark = MagicMock()
        spark.table.return_value.count.return_value = 100

        check = row_count_check("my_table", min_rows=50)
        result = check.check_fn(spark)

        assert result.passed is True
        assert result.check_name == "row_count_my_table"
        assert result.details == {"count": 100, "min_rows": 50}
        spark.table.assert_called_once_with("my_table")

    def test_fails_when_too_few_rows(self) -> None:
        spark = MagicMock()
        spark.table.return_value.count.return_value = 5

        check = row_count_check("my_table", min_rows=50)
        result = check.check_fn(spark)

        assert result.passed is False

    def test_custom_timing_and_component(self) -> None:
        check = row_count_check(
            "my_table",
            min_rows=1,
            timing=CheckTiming.AFTER_COMPONENT,
            component_name="loader",
        )
        assert check.timing == CheckTiming.AFTER_COMPONENT
        assert check.component_name == "loader"


class TestNullCheck:
    def test_passes_when_no_nulls(self) -> None:
        spark = MagicMock()
        df = MagicMock()
        spark.table.return_value = df
        df.count.return_value = 100
        df.filter.return_value.count.return_value = 0

        check = null_check("my_table", "col_a")
        result = check.check_fn(spark)

        assert result.passed is True
        assert result.check_name == "null_check_my_table_col_a"
        df.filter.assert_called_once_with("col_a IS NULL")

    def test_fails_when_too_many_nulls(self) -> None:
        spark = MagicMock()
        df = MagicMock()
        spark.table.return_value = df
        df.count.return_value = 100
        df.filter.return_value.count.return_value = 20

        check = null_check("my_table", "col_a", max_null_pct=10.0)
        result = check.check_fn(spark)

        assert result.passed is False
        assert result.details is not None
        assert result.details["pct"] == 20.0

    def test_empty_table(self) -> None:
        spark = MagicMock()
        df = MagicMock()
        spark.table.return_value = df
        df.count.return_value = 0
        df.filter.return_value.count.return_value = 0

        check = null_check("my_table", "col_a")
        result = check.check_fn(spark)

        assert result.passed is True
        assert result.details is not None
        assert result.details["pct"] == 0.0

    def test_custom_timing(self) -> None:
        check = null_check(
            "my_table",
            "col_a",
            timing=CheckTiming.AFTER_COMPONENT,
            component_name="etl",
        )
        assert check.timing == CheckTiming.AFTER_COMPONENT
        assert check.component_name == "etl"


class TestUniqueCheck:
    def test_passes_when_all_unique(self) -> None:
        spark = MagicMock()
        df = MagicMock()
        spark.table.return_value = df
        df.count.return_value = 100
        df.select.return_value.distinct.return_value.count.return_value = 100

        check = unique_check("users", "id")
        result = check.check_fn(spark)

        assert result.passed is True
        assert result.check_name == "unique_users_id"
        assert result.details == {"total": 100, "distinct": 100, "duplicates": 0}
        df.select.assert_called_once_with("id")

    def test_fails_when_duplicates_exist(self) -> None:
        spark = MagicMock()
        df = MagicMock()
        spark.table.return_value = df
        df.count.return_value = 100
        df.select.return_value.distinct.return_value.count.return_value = 90

        check = unique_check("users", "email")
        result = check.check_fn(spark)

        assert result.passed is False
        assert result.details is not None
        assert result.details["duplicates"] == 10

    def test_composite_columns(self) -> None:
        spark = MagicMock()
        df = MagicMock()
        spark.table.return_value = df
        df.count.return_value = 50
        df.select.return_value.distinct.return_value.count.return_value = 50

        check = unique_check("events", ["user_id", "event_type"])
        result = check.check_fn(spark)

        assert result.passed is True
        assert check.name == "unique_events_user_id_event_type"
        df.select.assert_called_once_with("user_id", "event_type")

    def test_custom_timing_and_component(self) -> None:
        check = unique_check(
            "orders",
            "order_id",
            timing=CheckTiming.AFTER_COMPONENT,
            component_name="loader",
        )
        assert check.timing == CheckTiming.AFTER_COMPONENT
        assert check.component_name == "loader"

    def test_message_contains_column_names(self) -> None:
        spark = MagicMock()
        df = MagicMock()
        spark.table.return_value = df
        df.count.return_value = 10
        df.select.return_value.distinct.return_value.count.return_value = 8

        check = unique_check("t", ["a", "b"])
        result = check.check_fn(spark)

        assert "a, b" in result.message
        assert "2 duplicate" in result.message


class TestRangeCheck:
    def test_passes_when_all_in_range(self) -> None:
        spark = MagicMock()
        df = MagicMock()
        spark.table.return_value = df
        df.filter.return_value.count.return_value = 0
        df.count.return_value = 100

        check = range_check("orders", "amount", min_value=0, max_value=10000)
        result = check.check_fn(spark)

        assert result.passed is True
        assert result.check_name == "range_orders_amount"
        assert result.details == {
            "violations": 0,
            "total": 100,
            "min_value": 0,
            "max_value": 10000,
        }

    def test_fails_when_out_of_range(self) -> None:
        spark = MagicMock()
        df = MagicMock()
        spark.table.return_value = df
        df.filter.return_value.count.return_value = 5
        df.count.return_value = 100

        check = range_check("orders", "amount", min_value=0, max_value=10000)
        result = check.check_fn(spark)

        assert result.passed is False
        assert result.details is not None
        assert result.details["violations"] == 5

    def test_min_only(self) -> None:
        spark = MagicMock()
        df = MagicMock()
        spark.table.return_value = df
        df.filter.return_value.count.return_value = 0
        df.count.return_value = 50

        check = range_check("scores", "value", min_value=0)
        result = check.check_fn(spark)

        assert result.passed is True
        df.filter.assert_called_once_with("value < 0")

    def test_max_only(self) -> None:
        spark = MagicMock()
        df = MagicMock()
        spark.table.return_value = df
        df.filter.return_value.count.return_value = 3
        df.count.return_value = 50

        check = range_check("scores", "pct", max_value=100)
        result = check.check_fn(spark)

        assert result.passed is False
        df.filter.assert_called_once_with("pct > 100")

    def test_both_bounds_filter_expression(self) -> None:
        spark = MagicMock()
        df = MagicMock()
        spark.table.return_value = df
        df.filter.return_value.count.return_value = 0
        df.count.return_value = 10

        check = range_check("t", "x", min_value=1, max_value=99)
        check.check_fn(spark)

        df.filter.assert_called_once_with("x < 1 OR x > 99")

    def test_raises_when_no_bounds(self) -> None:
        with pytest.raises(ValueError, match="At least one"):
            range_check("t", "x")

    def test_custom_timing_and_component(self) -> None:
        check = range_check(
            "t",
            "x",
            min_value=0,
            timing=CheckTiming.AFTER_COMPONENT,
            component_name="transform",
        )
        assert check.timing == CheckTiming.AFTER_COMPONENT
        assert check.component_name == "transform"

    def test_message_shows_bounds(self) -> None:
        spark = MagicMock()
        df = MagicMock()
        spark.table.return_value = df
        df.filter.return_value.count.return_value = 2
        df.count.return_value = 10

        check = range_check("t", "x", min_value=0, max_value=100)
        result = check.check_fn(spark)

        assert "[0, 100]" in result.message

    def test_message_shows_open_upper_bound(self) -> None:
        spark = MagicMock()
        df = MagicMock()
        spark.table.return_value = df
        df.filter.return_value.count.return_value = 0
        df.count.return_value = 10

        check = range_check("t", "x", min_value=0)
        result = check.check_fn(spark)

        assert "[0, " in result.message

    def test_message_shows_open_lower_bound(self) -> None:
        spark = MagicMock()
        df = MagicMock()
        spark.table.return_value = df
        df.filter.return_value.count.return_value = 0
        df.count.return_value = 10

        check = range_check("t", "x", max_value=100)
        result = check.check_fn(spark)

        assert "100]" in result.message


class TestSchemaCheck:
    def _make_spark(self, dtypes: list[tuple[str, str]]) -> MagicMock:
        spark = MagicMock()
        spark.table.return_value.dtypes = dtypes
        return spark

    def test_passes_when_schema_matches(self) -> None:
        spark = self._make_spark([("id", "bigint"), ("name", "string")])
        schema = SchemaDefinition(
            fields=[
                SchemaField(name="id", data_type=DataType.LONG),
                SchemaField(name="name", data_type=DataType.STRING),
            ]
        )

        check = schema_check("users", schema)
        result = check.check_fn(spark)

        assert result.passed is True
        assert result.check_name == "schema_users"

    def test_fails_on_missing_column(self) -> None:
        spark = self._make_spark([("id", "bigint")])
        schema = SchemaDefinition(
            fields=[
                SchemaField(name="id", data_type=DataType.LONG),
                SchemaField(name="name", data_type=DataType.STRING),
            ]
        )

        check = schema_check("users", schema)
        result = check.check_fn(spark)

        assert result.passed is False
        assert result.details is not None
        assert "name" in result.details["missing"]

    def test_fails_on_type_mismatch(self) -> None:
        spark = self._make_spark([("id", "int"), ("name", "string")])
        schema = SchemaDefinition(
            fields=[
                SchemaField(name="id", data_type=DataType.LONG),
                SchemaField(name="name", data_type=DataType.STRING),
            ]
        )

        check = schema_check("users", schema)
        result = check.check_fn(spark)

        assert result.passed is False
        assert result.details is not None
        assert len(result.details["type_mismatches"]) == 1
        assert "id" in result.details["type_mismatches"][0]

    def test_skips_type_check_when_disabled(self) -> None:
        spark = self._make_spark([("id", "int"), ("name", "string")])
        schema = SchemaDefinition(
            fields=[
                SchemaField(name="id", data_type=DataType.LONG),
                SchemaField(name="name", data_type=DataType.STRING),
            ]
        )

        check = schema_check("users", schema, check_types=False)
        result = check.check_fn(spark)

        assert result.passed is True

    def test_extra_columns_in_table_are_ok(self) -> None:
        spark = self._make_spark([("id", "bigint"), ("name", "string"), ("extra", "double")])
        schema = SchemaDefinition(
            fields=[
                SchemaField(name="id", data_type=DataType.LONG),
                SchemaField(name="name", data_type=DataType.STRING),
            ]
        )

        check = schema_check("users", schema)
        result = check.check_fn(spark)

        assert result.passed is True

    def test_all_datatype_mappings(self) -> None:
        """Verify that all DataType values map to the correct Spark type string."""
        from pyspark_pipeline_framework.core.quality.checks import _DATATYPE_TO_SPARK

        expected_mappings = {
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
        for dt in DataType:
            assert dt.value in _DATATYPE_TO_SPARK, f"Missing mapping for {dt}"
            assert _DATATYPE_TO_SPARK[dt.value] == expected_mappings[dt.value]

    def test_custom_timing_and_component(self) -> None:
        schema = SchemaDefinition(fields=[SchemaField(name="x", data_type=DataType.STRING)])
        check = schema_check(
            "t",
            schema,
            timing=CheckTiming.AFTER_COMPONENT,
            component_name="loader",
        )
        assert check.timing == CheckTiming.AFTER_COMPONENT
        assert check.component_name == "loader"

    def test_message_shows_issues(self) -> None:
        spark = self._make_spark([("id", "string")])
        schema = SchemaDefinition(
            fields=[
                SchemaField(name="id", data_type=DataType.LONG),
                SchemaField(name="missing_col", data_type=DataType.STRING),
            ]
        )

        check = schema_check("t", schema)
        result = check.check_fn(spark)

        assert "missing columns" in result.message
        assert "type mismatches" in result.message


class TestCustomSqlCheck:
    def test_passes_when_sql_returns_true(self) -> None:
        spark = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, k: True if k == "passed" else None
        row.asDict.return_value = {"passed": True}
        spark.sql.return_value.head.return_value = row

        check = custom_sql_check(
            name="no_negatives",
            sql="SELECT COUNT(*) = 0 AS passed FROM t WHERE x < 0",
        )
        result = check.check_fn(spark)

        assert result.passed is True
        assert result.check_name == "no_negatives"
        assert result.details == {"sql": "SELECT COUNT(*) = 0 AS passed FROM t WHERE x < 0"}

    def test_fails_when_sql_returns_false(self) -> None:
        spark = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, k: False if k == "passed" else None
        row.asDict.return_value = {"passed": False}
        spark.sql.return_value.head.return_value = row

        check = custom_sql_check(name="check_1", sql="SELECT false AS passed")
        result = check.check_fn(spark)

        assert result.passed is False

    def test_custom_message_from_sql(self) -> None:
        spark = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, k: {"passed": True, "message": "All good"}[k]
        row.asDict.return_value = {"passed": True, "message": "All good"}
        spark.sql.return_value.head.return_value = row

        check = custom_sql_check(name="check_msg", sql="SELECT true AS passed, 'All good' AS message")
        result = check.check_fn(spark)

        assert result.passed is True
        assert result.message == "All good"

    def test_default_message_when_no_message_column(self) -> None:
        spark = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, k: True if k == "passed" else None
        row.asDict.return_value = {"passed": True}
        spark.sql.return_value.head.return_value = row

        check = custom_sql_check(name="my_check", sql="SELECT true AS passed")
        result = check.check_fn(spark)

        assert "PASSED" in result.message

    def test_custom_timing_and_component(self) -> None:
        check = custom_sql_check(
            name="c",
            sql="SELECT true AS passed",
            timing=CheckTiming.AFTER_COMPONENT,
            component_name="loader",
        )
        assert check.timing == CheckTiming.AFTER_COMPONENT
        assert check.component_name == "loader"

    def test_no_rows_returned(self) -> None:
        spark = MagicMock()
        spark.sql.return_value.head.return_value = None

        check = custom_sql_check(name="empty", sql="SELECT true AS passed WHERE false")
        result = check.check_fn(spark)

        assert result.passed is False
        assert "no rows" in result.message

    def test_failed_default_message(self) -> None:
        spark = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, k: False if k == "passed" else None
        row.asDict.return_value = {"passed": False}
        spark.sql.return_value.head.return_value = row

        check = custom_sql_check(name="my_check", sql="SELECT false AS passed")
        result = check.check_fn(spark)

        assert "FAILED" in result.message
