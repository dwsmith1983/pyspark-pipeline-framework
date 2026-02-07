"""Integration tests for DataFlow with a real SparkSession."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from pyspark_pipeline_framework.runtime.dataflow.base import DataFlow

pytestmark = [pytest.mark.spark, pytest.mark.integration]


class _SqlCountComponent(DataFlow):
    """Component that runs SQL against a temp view and writes a result view."""

    @property
    def name(self) -> str:
        return "sql-count"

    def run(self) -> None:
        df = self.spark.sql(
            "SELECT COUNT(*) AS total FROM test_input"
        )
        df.createOrReplaceTempView("test_output")


class _FilterComponent(DataFlow):
    """Component that filters rows above a threshold."""

    @property
    def name(self) -> str:
        return "filter-high"

    def run(self) -> None:
        df = self.spark.sql(
            "SELECT * FROM test_input WHERE value > 50"
        )
        df.createOrReplaceTempView("test_filtered")


class TestDataFlowWithSpark:
    """DataFlow subclasses work correctly with a real session."""

    def test_sql_component(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [(1, "a"), (2, "b"), (3, "c")], schema=["id", "name"]
        )
        df.createOrReplaceTempView("test_input")

        comp = _SqlCountComponent()
        comp.set_spark_session(spark)
        comp.run()

        result = spark.table("test_output").collect()
        assert result[0]["total"] == 3

    def test_filter_component(self, spark: SparkSession) -> None:
        df = spark.createDataFrame(
            [(1, 10), (2, 60), (3, 80)], schema=["id", "value"]
        )
        df.createOrReplaceTempView("test_input")

        comp = _FilterComponent()
        comp.set_spark_session(spark)
        comp.run()

        result = spark.table("test_filtered").collect()
        assert len(result) == 2
        assert {r["id"] for r in result} == {2, 3}

    def test_spark_not_set_raises(self) -> None:
        comp = _SqlCountComponent()
        with pytest.raises(RuntimeError, match="SparkSession not available"):
            comp.run()

    def test_logger_available(self, spark: SparkSession) -> None:
        comp = _SqlCountComponent()
        comp.set_spark_session(spark)
        assert comp.logger.name == "ppf.component.sql-count"
