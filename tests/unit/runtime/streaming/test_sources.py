"""Tests for built-in streaming sources."""

from __future__ import annotations

from unittest.mock import MagicMock

from pyspark_pipeline_framework.runtime.streaming.sources import (
    DeltaStreamingSource,
    FileStreamingSource,
    IcebergStreamingSource,
    KafkaStreamingSource,
    RateStreamingSource,
)


def _self_returning_mock() -> MagicMock:
    """A mock where .option() returns itself (fluent builder pattern)."""
    m = MagicMock()
    m.option.return_value = m
    return m


# ===================================================================
# TestKafkaStreamingSource
# ===================================================================


class TestKafkaStreamingSource:
    def test_read_stream_builds_chain(self) -> None:
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = KafkaStreamingSource(
            bootstrap_servers="broker:9092",
            topics="events",
        )
        src.read_stream(spark)

        spark.readStream.format.assert_called_once_with("kafka")
        reader.option.assert_any_call("kafka.bootstrap.servers", "broker:9092")

    def test_subscribe_topics(self) -> None:
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = KafkaStreamingSource(
            bootstrap_servers="b:9092",
            topics="t1,t2",
        )
        src.read_stream(spark)

        reader.option.assert_any_call("subscribe", "t1,t2")

    def test_starting_offsets_default(self) -> None:
        src = KafkaStreamingSource(
            bootstrap_servers="b:9092", topics="t"
        )
        assert src.starting_offsets == "latest"

    def test_starting_offsets_earliest(self) -> None:
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = KafkaStreamingSource(
            bootstrap_servers="b:9092",
            topics="t",
            starting_offsets="earliest",
        )
        src.read_stream(spark)

        reader.option.assert_any_call("startingOffsets", "earliest")

    def test_watermark_defaults_none(self) -> None:
        src = KafkaStreamingSource(
            bootstrap_servers="b:9092", topics="t"
        )
        assert src.watermark_column is None
        assert src.watermark_delay is None


# ===================================================================
# TestFileStreamingSource
# ===================================================================


class TestFileStreamingSource:
    def test_read_stream_basic(self) -> None:
        spark = MagicMock()
        src = FileStreamingSource(path="/data/input")
        src.read_stream(spark)

        spark.readStream.format.assert_called_once_with("parquet")
        spark.readStream.format.return_value.load.assert_called_once_with(
            "/data/input"
        )

    def test_custom_format(self) -> None:
        spark = MagicMock()
        src = FileStreamingSource(path="/data/csv", file_format="csv")
        src.read_stream(spark)

        spark.readStream.format.assert_called_once_with("csv")

    def test_schema_applied(self) -> None:
        spark = MagicMock()
        src = FileStreamingSource(
            path="/data", schema="id INT, name STRING"
        )
        src.read_stream(spark)

        reader = spark.readStream.format.return_value
        reader.schema.assert_called_once_with("id INT, name STRING")

    def test_schema_not_applied_when_none(self) -> None:
        spark = MagicMock()
        src = FileStreamingSource(path="/data")
        src.read_stream(spark)

        reader = spark.readStream.format.return_value
        reader.schema.assert_not_called()

    def test_options_applied(self) -> None:
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = FileStreamingSource(
            path="/data",
            file_format="csv",
            options={"header": "true", "sep": ","},
        )
        src.read_stream(spark)

        reader.option.assert_any_call("header", "true")
        reader.option.assert_any_call("sep", ",")

    def test_default_format_parquet(self) -> None:
        src = FileStreamingSource(path="/data")
        assert src.file_format == "parquet"


# ===================================================================
# TestDeltaStreamingSource
# ===================================================================


class TestDeltaStreamingSource:
    def test_read_stream_basic(self) -> None:
        spark = MagicMock()
        src = DeltaStreamingSource(path="/delta/table")
        src.read_stream(spark)

        spark.readStream.format.assert_called_once_with("delta")
        spark.readStream.format.return_value.load.assert_called_once_with(
            "/delta/table"
        )

    def test_options_applied(self) -> None:
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = DeltaStreamingSource(
            path="/delta/table",
            options={"ignoreChanges": "true"},
        )
        src.read_stream(spark)

        reader.option.assert_any_call("ignoreChanges", "true")

    def test_no_options(self) -> None:
        spark = MagicMock()
        src = DeltaStreamingSource(path="/delta/table")
        src.read_stream(spark)

        reader = spark.readStream.format.return_value
        reader.option.assert_not_called()

    def test_watermark_defaults_none(self) -> None:
        src = DeltaStreamingSource(path="/delta/table")
        assert src.watermark_column is None
        assert src.watermark_delay is None


# ===================================================================
# TestIcebergStreamingSource
# ===================================================================


class TestIcebergStreamingSource:
    def test_read_stream_basic(self) -> None:
        spark = MagicMock()
        src = IcebergStreamingSource(table="catalog.db.events")
        src.read_stream(spark)

        spark.readStream.format.assert_called_once_with("iceberg")
        spark.readStream.format.return_value.load.assert_called_once_with(
            "catalog.db.events"
        )

    def test_options_applied(self) -> None:
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = IcebergStreamingSource(
            table="catalog.db.events",
            options={"stream-from-timestamp": "1234567890"},
        )
        src.read_stream(spark)

        reader.option.assert_any_call("stream-from-timestamp", "1234567890")

    def test_no_options(self) -> None:
        spark = MagicMock()
        src = IcebergStreamingSource(table="catalog.db.events")
        src.read_stream(spark)

        reader = spark.readStream.format.return_value
        reader.option.assert_not_called()

    def test_watermark_defaults_none(self) -> None:
        src = IcebergStreamingSource(table="catalog.db.events")
        assert src.watermark_column is None
        assert src.watermark_delay is None


# ===================================================================
# TestRateStreamingSource
# ===================================================================


class TestRateStreamingSource:
    def test_read_stream_basic(self) -> None:
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = RateStreamingSource()
        src.read_stream(spark)

        spark.readStream.format.assert_called_once_with("rate")
        reader.option.assert_any_call("rowsPerSecond", 1)
        reader.option.assert_any_call("numPartitions", 1)

    def test_custom_rate(self) -> None:
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = RateStreamingSource(rows_per_second=100, num_partitions=4)
        src.read_stream(spark)

        reader.option.assert_any_call("rowsPerSecond", 100)
        reader.option.assert_any_call("numPartitions", 4)

    def test_defaults(self) -> None:
        src = RateStreamingSource()
        assert src.rows_per_second == 1
        assert src.num_partitions == 1

    def test_watermark_defaults_none(self) -> None:
        src = RateStreamingSource()
        assert src.watermark_column is None
        assert src.watermark_delay is None
