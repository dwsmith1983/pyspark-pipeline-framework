"""Tests for batch example components."""

from __future__ import annotations

from unittest.mock import MagicMock

from pyspark_pipeline_framework.examples.batch import (
    ReadTable,
    ReadTableConfig,
    SqlTransform,
    SqlTransformConfig,
    WriteTable,
    WriteTableConfig,
)


# ---------------------------------------------------------------------------
# ReadTable
# ---------------------------------------------------------------------------


class TestReadTable:
    def test_name(self) -> None:
        comp = ReadTable(ReadTableConfig(table_name="db.t", output_view="v"))
        assert comp.name == "ReadTable(db.t)"

    def test_run_reads_table_and_creates_view(self) -> None:
        spark = MagicMock()
        df = MagicMock()
        spark.table.return_value = df

        comp = ReadTable(ReadTableConfig(table_name="db.t", output_view="v"))
        comp.set_spark_session(spark)
        comp.run()

        spark.table.assert_called_once_with("db.t")
        df.createOrReplaceTempView.assert_called_once_with("v")

    def test_run_applies_filter(self) -> None:
        spark = MagicMock()
        df = MagicMock()
        filtered_df = MagicMock()
        spark.table.return_value = df
        df.filter.return_value = filtered_df

        comp = ReadTable(ReadTableConfig(
            table_name="db.t", output_view="v", filter_condition="id > 10"
        ))
        comp.set_spark_session(spark)
        comp.run()

        df.filter.assert_called_once_with("id > 10")
        filtered_df.createOrReplaceTempView.assert_called_once_with("v")

    def test_from_config(self) -> None:
        comp = ReadTable.from_config({
            "table_name": "raw.orders",
            "output_view": "orders",
        })
        assert isinstance(comp, ReadTable)
        assert comp.name == "ReadTable(raw.orders)"


# ---------------------------------------------------------------------------
# SqlTransform
# ---------------------------------------------------------------------------


class TestSqlTransform:
    def test_name(self) -> None:
        comp = SqlTransform(SqlTransformConfig(sql="SELECT 1", output_view="v"))
        assert comp.name == "SqlTransform(v)"

    def test_run_executes_sql_and_creates_view(self) -> None:
        spark = MagicMock()
        df = MagicMock()
        spark.sql.return_value = df

        comp = SqlTransform(SqlTransformConfig(
            sql="SELECT * FROM raw", output_view="clean"
        ))
        comp.set_spark_session(spark)
        comp.run()

        spark.sql.assert_called_once_with("SELECT * FROM raw")
        df.createOrReplaceTempView.assert_called_once_with("clean")

    def test_from_config(self) -> None:
        comp = SqlTransform.from_config({
            "sql": "SELECT 1",
            "output_view": "test",
        })
        assert isinstance(comp, SqlTransform)


# ---------------------------------------------------------------------------
# WriteTable
# ---------------------------------------------------------------------------


class TestWriteTable:
    def test_name(self) -> None:
        comp = WriteTable(WriteTableConfig(
            input_view="v", output_table="db.out"
        ))
        assert comp.name == "WriteTable(db.out)"

    def test_run_writes_table(self) -> None:
        spark = MagicMock()
        df = MagicMock()
        writer = MagicMock()
        spark.table.return_value = df
        df.write.mode.return_value = writer

        comp = WriteTable(WriteTableConfig(
            input_view="v", output_table="db.out"
        ))
        comp.set_spark_session(spark)
        comp.run()

        spark.table.assert_called_once_with("v")
        df.write.mode.assert_called_once_with("overwrite")
        writer.saveAsTable.assert_called_once_with("db.out")

    def test_run_with_partition_by(self) -> None:
        spark = MagicMock()
        df = MagicMock()
        writer = MagicMock()
        partitioned_writer = MagicMock()
        spark.table.return_value = df
        df.write.mode.return_value = writer
        writer.partitionBy.return_value = partitioned_writer

        comp = WriteTable(WriteTableConfig(
            input_view="v",
            output_table="db.out",
            partition_by=["region", "date"],
        ))
        comp.set_spark_session(spark)
        comp.run()

        writer.partitionBy.assert_called_once_with("region", "date")
        partitioned_writer.saveAsTable.assert_called_once_with("db.out")

    def test_from_config(self) -> None:
        comp = WriteTable.from_config({
            "input_view": "v",
            "output_table": "db.out",
            "mode": "append",
        })
        assert isinstance(comp, WriteTable)
        assert comp._config.mode == "append"
        assert comp._config.partition_by == []

    def test_from_config_with_partition_by(self) -> None:
        comp = WriteTable.from_config({
            "input_view": "v",
            "output_table": "db.out",
            "partition_by": ["col1"],
        })
        assert comp._config.partition_by == ["col1"]
