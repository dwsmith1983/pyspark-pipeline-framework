"""Integration tests for streaming sources and sinks using a real SparkSession.

Uses Spark's built-in ``rate`` source and ``memory`` / ``json`` sinks to
validate streaming abstractions without external dependencies like Kafka.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.streaming import DataStreamWriter

from pyspark_pipeline_framework.runtime.streaming.base import (
    OutputMode,
    StreamingPipeline,
    StreamingSink,
    StreamingSource,
    TriggerConfig,
    TriggerType,
)
from pyspark_pipeline_framework.runtime.streaming.sinks import (
    ConsoleStreamingSink,
    FileStreamingSink,
)
from pyspark_pipeline_framework.runtime.streaming.sources import (
    FileStreamingSource,
    RateStreamingSource,
)

pytestmark = [pytest.mark.spark, pytest.mark.integration]


# ---------------------------------------------------------------------------
# Memory sink for assertions (Spark built-in, not in our sinks module)
# ---------------------------------------------------------------------------

@dataclass
class _MemorySink(StreamingSink):
    """In-memory sink using Spark's built-in ``memory`` format for testing."""

    query_name_str: str = "test_stream"
    checkpoint_location: str = "/tmp/ppf-test-memory-checkpoint"
    output_mode: OutputMode = OutputMode.APPEND

    def write_stream(self, df: DataFrame) -> DataStreamWriter:
        return df.writeStream.format("memory")

    @property
    def query_name(self) -> str:
        return self.query_name_str


# ---------------------------------------------------------------------------
# Test: RateStreamingSource
# ---------------------------------------------------------------------------


class TestRateStreamingSource:
    def test_rate_source_generates_rows(self, spark: SparkSession) -> None:
        """Rate source produces streaming DataFrame with expected schema."""
        source = RateStreamingSource(rows_per_second=10, num_partitions=1)
        df = source.read_stream(spark)

        assert df.isStreaming
        field_names = [f.name for f in df.schema.fields]
        assert "timestamp" in field_names
        assert "value" in field_names

    def test_rate_source_with_memory_sink(
        self, spark: SparkSession, tmp_path: Path,
    ) -> None:
        """Rate source → memory sink produces queryable table."""
        source = RateStreamingSource(rows_per_second=100, num_partitions=1)
        sink = _MemorySink(
            query_name_str="rate_test",
            checkpoint_location=str(tmp_path / "checkpoint"),
        )

        df = source.read_stream(spark)
        writer = sink.write_stream(df)
        query = (
            writer
            .outputMode(sink.output_mode.value)
            .option("checkpointLocation", sink.checkpoint_location)
            .queryName(sink.query_name)
            .trigger(processingTime="1 second")
            .start()
        )

        try:
            # Wait for at least one micro-batch
            time.sleep(3)
            result = spark.sql("SELECT * FROM rate_test")
            assert result.count() > 0
        finally:
            query.stop()
            query.awaitTermination()


# ---------------------------------------------------------------------------
# Test: FileStreamingSource + FileStreamingSink
# ---------------------------------------------------------------------------


class TestFileStreaming:
    def test_file_source_reads_json_stream(
        self, spark: SparkSession, tmp_path: Path,
    ) -> None:
        """FileStreamingSource reads new JSON files as a stream."""
        input_dir = tmp_path / "input"
        input_dir.mkdir()

        # Write initial data
        df = spark.createDataFrame(
            [(1, "Alice"), (2, "Bob")], schema=["id", "name"],
        )
        df.write.json(str(input_dir / "batch1"), mode="overwrite")

        source = FileStreamingSource(
            path=str(input_dir),
            file_format="json",
            schema="id LONG, name STRING",
        )
        stream_df = source.read_stream(spark)
        assert stream_df.isStreaming

    def test_file_source_to_parquet_sink(
        self, spark: SparkSession, tmp_path: Path,
    ) -> None:
        """FileStreamingSource → FileStreamingSink end-to-end with parquet."""
        input_dir = tmp_path / "input"
        output_dir = tmp_path / "output"
        checkpoint_dir = tmp_path / "checkpoint"
        input_dir.mkdir()

        # Write test data as parquet directly into the watched directory
        df = spark.createDataFrame(
            [(1, "Alice"), (2, "Bob"), (3, "Carol")],
            schema=["id", "name"],
        )
        df.write.parquet(str(input_dir), mode="overwrite")

        source = FileStreamingSource(
            path=str(input_dir),
            file_format="parquet",
            schema="id LONG, name STRING",
        )
        sink = FileStreamingSink(
            path=str(output_dir),
            file_format="parquet",
            checkpoint_location=str(checkpoint_dir),
            output_mode=OutputMode.APPEND,
        )

        stream_df = source.read_stream(spark)
        writer = sink.write_stream(stream_df)
        query = (
            writer
            .outputMode(sink.output_mode.value)
            .option("checkpointLocation", sink.checkpoint_location)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()

        # Verify output
        result = spark.read.parquet(str(output_dir))
        assert result.count() == 3
        names = sorted([row.name for row in result.collect()])
        assert names == ["Alice", "Bob", "Carol"]


# ---------------------------------------------------------------------------
# Test: StreamingPipeline end-to-end
# ---------------------------------------------------------------------------


class _RateToMemoryPipeline(StreamingPipeline):
    """Minimal streaming pipeline for testing: rate → transform → memory."""

    def __init__(
        self,
        query_name: str,
        checkpoint_dir: str,
    ) -> None:
        super().__init__()
        self._source = RateStreamingSource(rows_per_second=100, num_partitions=1)
        self._sink = _MemorySink(
            query_name_str=query_name,
            checkpoint_location=checkpoint_dir,
        )

    @property
    def name(self) -> str:
        return "rate-to-memory"

    @property
    def source(self) -> StreamingSource:
        return self._source

    @property
    def sink(self) -> StreamingSink:
        return self._sink

    @property
    def trigger(self) -> TriggerConfig:
        return TriggerConfig(TriggerType.PROCESSING_TIME, "1 second")

    def transform(self, df: DataFrame) -> DataFrame:
        # Double the value column
        return df.selectExpr("timestamp", "value * 2 AS value")


class TestStreamingPipeline:
    def test_pipeline_start_and_query(
        self, spark: SparkSession, tmp_path: Path,
    ) -> None:
        """StreamingPipeline.start_stream() returns an active query."""
        pipeline = _RateToMemoryPipeline(
            query_name="pipeline_test",
            checkpoint_dir=str(tmp_path / "checkpoint"),
        )
        pipeline.set_spark_session(spark)

        query = pipeline.start_stream()
        try:
            assert query.isActive
            time.sleep(3)

            result = spark.sql("SELECT * FROM pipeline_test")
            assert result.count() > 0

            # Values should be doubled by transform
            values = [row.value for row in result.collect()]
            assert all(v % 2 == 0 for v in values)
        finally:
            query.stop()
            query.awaitTermination()


# ---------------------------------------------------------------------------
# Test: ConsoleStreamingSink (just verify it creates a valid writer)
# ---------------------------------------------------------------------------


class TestConsoleSink:
    def test_console_sink_creates_writer(
        self, spark: SparkSession, tmp_path: Path,
    ) -> None:
        """ConsoleStreamingSink produces a valid DataStreamWriter."""
        source = RateStreamingSource(rows_per_second=10, num_partitions=1)
        sink = ConsoleStreamingSink(
            checkpoint_location=str(tmp_path / "checkpoint"),
        )

        df = source.read_stream(spark)
        writer = sink.write_stream(df)
        # Just verify it returns a DataStreamWriter (don't actually start)
        assert writer is not None
