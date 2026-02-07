"""Tests for built-in data quality check factories."""

from __future__ import annotations

from unittest.mock import MagicMock

from pyspark_pipeline_framework.core.quality.checks import (
    null_check,
    row_count_check,
)
from pyspark_pipeline_framework.core.quality.types import (
    CheckTiming,
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
