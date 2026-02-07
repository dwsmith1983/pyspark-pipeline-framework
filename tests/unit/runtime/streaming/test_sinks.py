"""Tests for built-in streaming sinks."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pyspark_pipeline_framework.runtime.streaming.base import OutputMode
from pyspark_pipeline_framework.runtime.streaming.sinks import (
    CloudFileFormat,
    CloudStorageStreamingSink,
    ConsoleStreamingSink,
    DeltaStreamingSink,
    FileStreamingSink,
    ForeachBatchSink,
    IcebergStreamingSink,
    KafkaStreamingSink,
)


# ===================================================================
# TestKafkaStreamingSink
# ===================================================================


class TestKafkaStreamingSink:
    def _fluent_df(self) -> MagicMock:
        df = MagicMock()
        writer = MagicMock()
        writer.option.return_value = writer  # fluent chain
        df.writeStream.format.return_value = writer
        return df

    def test_write_stream_builds_chain(self) -> None:
        df = self._fluent_df()
        sink = KafkaStreamingSink(
            bootstrap_servers="broker:9092",
            topic="output",
            checkpoint_location="/ckpt",
        )
        sink.write_stream(df)

        df.writeStream.format.assert_called_once_with("kafka")
        writer = df.writeStream.format.return_value
        writer.option.assert_any_call("kafka.bootstrap.servers", "broker:9092")

    def test_topic_option(self) -> None:
        df = self._fluent_df()
        sink = KafkaStreamingSink(
            bootstrap_servers="b:9092",
            topic="my-topic",
            checkpoint_location="/ckpt",
        )
        sink.write_stream(df)

        writer = df.writeStream.format.return_value
        writer.option.assert_any_call("topic", "my-topic")

    def test_default_output_mode(self) -> None:
        sink = KafkaStreamingSink(
            bootstrap_servers="b:9092",
            topic="t",
            checkpoint_location="/ckpt",
        )
        assert sink.output_mode == OutputMode.APPEND

    def test_checkpoint_location(self) -> None:
        sink = KafkaStreamingSink(
            bootstrap_servers="b:9092",
            topic="t",
            checkpoint_location="/my/ckpt",
        )
        assert sink.checkpoint_location == "/my/ckpt"


# ===================================================================
# TestDeltaStreamingSink
# ===================================================================


class TestDeltaStreamingSink:
    def test_write_stream_builds_chain(self) -> None:
        df = MagicMock()
        sink = DeltaStreamingSink(
            path="/delta/table", checkpoint_location="/ckpt"
        )
        sink.write_stream(df)

        df.writeStream.format.assert_called_once_with("delta")
        chain = df.writeStream.format.return_value
        chain.option.assert_called_once_with("path", "/delta/table")

    def test_partition_by(self) -> None:
        df = MagicMock()
        sink = DeltaStreamingSink(
            path="/delta/table",
            checkpoint_location="/ckpt",
            partition_by=["date", "region"],
        )
        sink.write_stream(df)

        writer = df.writeStream.format.return_value
        writer.partitionBy.assert_called_once_with("date", "region")

    def test_no_partition_by(self) -> None:
        df = MagicMock()
        sink = DeltaStreamingSink(
            path="/delta/table", checkpoint_location="/ckpt"
        )
        sink.write_stream(df)

        writer = df.writeStream.format.return_value
        writer.partitionBy.assert_not_called()

    def test_default_output_mode(self) -> None:
        sink = DeltaStreamingSink(
            path="/delta/table", checkpoint_location="/ckpt"
        )
        assert sink.output_mode == OutputMode.APPEND

    def test_custom_output_mode(self) -> None:
        sink = DeltaStreamingSink(
            path="/p",
            checkpoint_location="/c",
            output_mode=OutputMode.COMPLETE,
        )
        assert sink.output_mode == OutputMode.COMPLETE


# ===================================================================
# TestConsoleStreamingSink
# ===================================================================


class TestConsoleStreamingSink:
    def test_write_stream_builds_chain(self) -> None:
        df = MagicMock()
        sink = ConsoleStreamingSink()
        sink.write_stream(df)

        df.writeStream.format.assert_called_once_with("console")
        chain = df.writeStream.format.return_value
        chain.option.assert_called_once_with("truncate", False)

    def test_truncate_true(self) -> None:
        df = MagicMock()
        sink = ConsoleStreamingSink(truncate=True)
        sink.write_stream(df)

        chain = df.writeStream.format.return_value
        chain.option.assert_called_once_with("truncate", True)

    def test_default_checkpoint(self) -> None:
        sink = ConsoleStreamingSink()
        assert sink.checkpoint_location == "/tmp/console-checkpoint"

    def test_default_output_mode(self) -> None:
        sink = ConsoleStreamingSink()
        assert sink.output_mode == OutputMode.APPEND

    def test_query_name_default_none(self) -> None:
        sink = ConsoleStreamingSink()
        assert sink.query_name is None


# ===================================================================
# TestIcebergStreamingSink
# ===================================================================


class TestIcebergStreamingSink:
    def test_write_stream_builds_chain(self) -> None:
        df = MagicMock()
        sink = IcebergStreamingSink(
            table="catalog.db.events",
            checkpoint_location="/ckpt",
        )
        sink.write_stream(df)

        df.writeStream.format.assert_called_once_with("iceberg")
        chain = df.writeStream.format.return_value
        chain.option.assert_called_once_with("path", "catalog.db.events")

    def test_partition_by(self) -> None:
        df = MagicMock()
        sink = IcebergStreamingSink(
            table="catalog.db.events",
            checkpoint_location="/ckpt",
            partition_by=["date"],
        )
        sink.write_stream(df)

        writer = df.writeStream.format.return_value
        writer.partitionBy.assert_called_once_with("date")

    def test_no_partition_by(self) -> None:
        df = MagicMock()
        sink = IcebergStreamingSink(
            table="catalog.db.events",
            checkpoint_location="/ckpt",
        )
        sink.write_stream(df)

        writer = df.writeStream.format.return_value
        writer.partitionBy.assert_not_called()

    def test_default_output_mode(self) -> None:
        sink = IcebergStreamingSink(
            table="catalog.db.events",
            checkpoint_location="/ckpt",
        )
        assert sink.output_mode == OutputMode.APPEND

    def test_checkpoint_location(self) -> None:
        sink = IcebergStreamingSink(
            table="catalog.db.events",
            checkpoint_location="/my/ckpt",
        )
        assert sink.checkpoint_location == "/my/ckpt"


# ===================================================================
# TestFileStreamingSink
# ===================================================================


class TestFileStreamingSink:
    def test_write_stream_builds_chain(self) -> None:
        df = MagicMock()
        sink = FileStreamingSink(
            path="/output/data", checkpoint_location="/ckpt"
        )
        sink.write_stream(df)

        df.writeStream.format.assert_called_once_with("parquet")
        chain = df.writeStream.format.return_value
        chain.option.assert_called_once_with("path", "/output/data")

    def test_custom_format(self) -> None:
        df = MagicMock()
        sink = FileStreamingSink(
            path="/output/data",
            file_format="json",
            checkpoint_location="/ckpt",
        )
        sink.write_stream(df)

        df.writeStream.format.assert_called_once_with("json")

    def test_partition_by(self) -> None:
        df = MagicMock()
        sink = FileStreamingSink(
            path="/output/data",
            checkpoint_location="/ckpt",
            partition_by=["year", "month"],
        )
        sink.write_stream(df)

        writer = df.writeStream.format.return_value
        writer.partitionBy.assert_called_once_with("year", "month")

    def test_no_partition_by(self) -> None:
        df = MagicMock()
        sink = FileStreamingSink(
            path="/output/data", checkpoint_location="/ckpt"
        )
        sink.write_stream(df)

        writer = df.writeStream.format.return_value
        writer.partitionBy.assert_not_called()

    def test_default_format_parquet(self) -> None:
        sink = FileStreamingSink(path="/output/data", checkpoint_location="/c")
        assert sink.file_format == "parquet"

    def test_default_output_mode(self) -> None:
        sink = FileStreamingSink(path="/output/data", checkpoint_location="/c")
        assert sink.output_mode == OutputMode.APPEND


# ===================================================================
# TestCloudStorageStreamingSink
# ===================================================================


class TestCloudStorageStreamingSink:
    def _fluent_df(self) -> MagicMock:
        df = MagicMock()
        writer = MagicMock()
        writer.option.return_value = writer
        writer.partitionBy.return_value = writer
        df.writeStream.format.return_value = writer
        return df

    def test_write_stream_builds_chain(self) -> None:
        df = self._fluent_df()
        sink = CloudStorageStreamingSink(
            path="s3a://bucket/prefix",
            checkpoint_location="/ckpt",
        )
        sink.write_stream(df)

        df.writeStream.format.assert_called_once_with("parquet")
        writer = df.writeStream.format.return_value
        writer.option.assert_any_call("path", "s3a://bucket/prefix")

    def test_custom_format(self) -> None:
        df = self._fluent_df()
        sink = CloudStorageStreamingSink(
            path="gs://bucket/prefix",
            file_format=CloudFileFormat.JSON,
            checkpoint_location="/ckpt",
        )
        sink.write_stream(df)
        df.writeStream.format.assert_called_once_with("json")

    def test_partition_by(self) -> None:
        df = self._fluent_df()
        sink = CloudStorageStreamingSink(
            path="s3a://bucket/prefix",
            checkpoint_location="/ckpt",
            partition_by=["date", "region"],
        )
        sink.write_stream(df)
        writer = df.writeStream.format.return_value
        writer.partitionBy.assert_called_once_with("date", "region")

    def test_no_partition_by(self) -> None:
        df = self._fluent_df()
        sink = CloudStorageStreamingSink(
            path="s3a://bucket/prefix",
            checkpoint_location="/ckpt",
        )
        sink.write_stream(df)
        writer = df.writeStream.format.return_value
        writer.partitionBy.assert_not_called()

    def test_compression(self) -> None:
        df = self._fluent_df()
        sink = CloudStorageStreamingSink(
            path="s3a://bucket/prefix",
            checkpoint_location="/ckpt",
            compression="gzip",
        )
        sink.write_stream(df)
        writer = df.writeStream.format.return_value
        writer.option.assert_any_call("compression", "gzip")

    def test_no_compression_when_none(self) -> None:
        df = self._fluent_df()
        sink = CloudStorageStreamingSink(
            path="s3a://bucket/prefix",
            checkpoint_location="/ckpt",
        )
        sink.write_stream(df)
        writer = df.writeStream.format.return_value
        for call in writer.option.call_args_list:
            assert call[0][0] != "compression"

    def test_extra_options(self) -> None:
        df = self._fluent_df()
        sink = CloudStorageStreamingSink(
            path="s3a://bucket/prefix",
            checkpoint_location="/ckpt",
            options={"maxRecordsPerFile": "100000"},
        )
        sink.write_stream(df)
        writer = df.writeStream.format.return_value
        writer.option.assert_any_call("maxRecordsPerFile", "100000")

    def test_default_output_mode(self) -> None:
        sink = CloudStorageStreamingSink(
            path="s3a://bucket/prefix",
            checkpoint_location="/ckpt",
        )
        assert sink.output_mode == OutputMode.APPEND

    def test_cloud_file_format_enum(self) -> None:
        assert CloudFileFormat.PARQUET.value == "parquet"
        assert CloudFileFormat.JSON.value == "json"
        assert CloudFileFormat.CSV.value == "csv"
        assert CloudFileFormat.AVRO.value == "avro"
        assert CloudFileFormat.ORC.value == "orc"

    def test_all_format_types(self) -> None:
        for fmt in CloudFileFormat:
            df = self._fluent_df()
            sink = CloudStorageStreamingSink(
                path="s3a://b/p",
                file_format=fmt,
                checkpoint_location="/ckpt",
            )
            sink.write_stream(df)
            df.writeStream.format.assert_called_once_with(fmt.value)


# ===================================================================
# TestForeachBatchSink
# ===================================================================


class TestForeachBatchSink:
    def test_write_stream_uses_foreach_batch(self) -> None:
        df = MagicMock()
        callback = MagicMock()
        sink = ForeachBatchSink(
            process_batch=callback,
            checkpoint_location="/ckpt",
        )
        sink.write_stream(df)
        df.writeStream.foreachBatch.assert_called_once_with(callback)

    def test_default_output_mode(self) -> None:
        sink = ForeachBatchSink(
            process_batch=lambda df, bid: None,
            checkpoint_location="/ckpt",
        )
        assert sink.output_mode == OutputMode.APPEND

    def test_checkpoint_location(self) -> None:
        sink = ForeachBatchSink(
            process_batch=lambda df, bid: None,
            checkpoint_location="/my/ckpt",
        )
        assert sink.checkpoint_location == "/my/ckpt"

    def test_custom_output_mode(self) -> None:
        sink = ForeachBatchSink(
            process_batch=lambda df, bid: None,
            checkpoint_location="/ckpt",
            output_mode=OutputMode.UPDATE,
        )
        assert sink.output_mode == OutputMode.UPDATE

    def test_query_name_default_none(self) -> None:
        sink = ForeachBatchSink(
            process_batch=lambda df, bid: None,
            checkpoint_location="/ckpt",
        )
        assert sink.query_name is None


# ===================================================================
# Parameterized: all sinks default to APPEND output mode
# ===================================================================


@pytest.mark.parametrize(
    "sink_factory",
    [
        pytest.param(
            lambda: KafkaStreamingSink(bootstrap_servers="b:9092", topic="t", checkpoint_location="/ckpt"),
            id="kafka",
        ),
        pytest.param(
            lambda: DeltaStreamingSink(path="/p", checkpoint_location="/ckpt"),
            id="delta",
        ),
        pytest.param(lambda: ConsoleStreamingSink(), id="console"),
        pytest.param(
            lambda: IcebergStreamingSink(table="cat.db.t", checkpoint_location="/ckpt"),
            id="iceberg",
        ),
        pytest.param(
            lambda: FileStreamingSink(path="/p", checkpoint_location="/ckpt"),
            id="file",
        ),
        pytest.param(
            lambda: CloudStorageStreamingSink(path="s3a://b/p", checkpoint_location="/ckpt"),
            id="cloud_storage",
        ),
        pytest.param(
            lambda: ForeachBatchSink(process_batch=lambda df, bid: None, checkpoint_location="/ckpt"),
            id="foreach_batch",
        ),
    ],
)
def test_all_sinks_default_to_append(sink_factory: object) -> None:
    """Every sink defaults to APPEND output mode."""
    sink = sink_factory()  # type: ignore[operator]
    assert sink.output_mode == OutputMode.APPEND
