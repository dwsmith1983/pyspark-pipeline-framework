"""Integration tests for example batch components with a real SparkSession."""

from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from pyspark_pipeline_framework.examples.batch import (
    ReadCsv,
    ReadCsvConfig,
    SqlTransform,
    SqlTransformConfig,
    WriteCsv,
    WriteCsvConfig,
)

pytestmark = [pytest.mark.spark, pytest.mark.integration]


class TestReadCsv:
    def test_read_and_register_view(
        self, spark: SparkSession, tmp_path: object
    ) -> None:
        csv_dir = str(tmp_path)
        # Write a small CSV for the component to read
        df = spark.createDataFrame(
            [(1, "Alice"), (2, "Bob")], schema=["id", "name"]
        )
        df.write.csv(f"{csv_dir}/input", header=True, mode="overwrite")

        comp = ReadCsv(ReadCsvConfig(
            path=f"{csv_dir}/input",
            output_view="csv_view",
        ))
        comp.set_spark_session(spark)
        comp.run()

        result = spark.table("csv_view")
        assert result.count() == 2
        assert set(result.columns) == {"id", "name"}


class TestSqlTransform:
    def test_transform_creates_view(self, spark: SparkSession) -> None:
        spark.createDataFrame(
            [(1, "alice"), (2, "bob")], schema=["id", "name"]
        ).createOrReplaceTempView("raw")

        comp = SqlTransform(SqlTransformConfig(
            sql="SELECT id, UPPER(name) AS name FROM raw",
            output_view="cleaned",
        ))
        comp.set_spark_session(spark)
        comp.run()

        result = spark.table("cleaned").collect()
        names = {r["name"] for r in result}
        assert names == {"ALICE", "BOB"}


class TestWriteCsv:
    def test_write_csv(
        self, spark: SparkSession, tmp_path: object
    ) -> None:
        out_dir = f"{tmp_path}/output"
        spark.createDataFrame(
            [(1, "x"), (2, "y")], schema=["id", "val"]
        ).createOrReplaceTempView("write_src")

        comp = WriteCsv(WriteCsvConfig(
            input_view="write_src",
            path=out_dir,
        ))
        comp.set_spark_session(spark)
        comp.run()

        # Read back and verify
        read_back = spark.read.csv(out_dir, header=True)
        assert read_back.count() == 2


class TestEndToEndBatch:
    """Chain ReadCsv → SqlTransform → WriteCsv."""

    def test_full_pipeline(
        self, spark: SparkSession, tmp_path: object
    ) -> None:
        csv_dir = str(tmp_path)
        # Prepare input CSV
        spark.createDataFrame(
            [(1, "alice", 90), (2, "bob", 85)],
            schema=["id", "name", "score"],
        ).write.csv(f"{csv_dir}/input", header=True, mode="overwrite")

        # Step 1: Read
        read_comp = ReadCsv(ReadCsvConfig(
            path=f"{csv_dir}/input",
            output_view="e2e_raw",
        ))
        read_comp.set_spark_session(spark)
        read_comp.run()

        # Step 2: Transform
        transform_comp = SqlTransform(SqlTransformConfig(
            sql="SELECT id, UPPER(name) AS name, score FROM e2e_raw",
            output_view="e2e_cleaned",
        ))
        transform_comp.set_spark_session(spark)
        transform_comp.run()

        # Step 3: Write
        write_comp = WriteCsv(WriteCsvConfig(
            input_view="e2e_cleaned",
            path=f"{csv_dir}/output",
        ))
        write_comp.set_spark_session(spark)
        write_comp.run()

        # Verify
        result = spark.read.csv(f"{csv_dir}/output", header=True)
        assert result.count() == 2
        names = {r["name"] for r in result.collect()}
        assert names == {"ALICE", "BOB"}
