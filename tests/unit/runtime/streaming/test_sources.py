"""Tests for built-in streaming sources."""

from __future__ import annotations

from unittest.mock import MagicMock

from pyspark_pipeline_framework.runtime.streaming.sources import (
    DeltaStreamingSource,
    EventHubsStartingPosition,
    EventHubsStreamingSource,
    FileStreamingSource,
    IcebergStreamingSource,
    KafkaStreamingSource,
    KinesisStartingPosition,
    KinesisStreamingSource,
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


# ===================================================================
# TestEventHubsStreamingSource
# ===================================================================


class TestEventHubsStreamingSource:
    def test_read_stream_builds_chain(self) -> None:
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = EventHubsStreamingSource(
            connection_string="Endpoint=sb://...",
            event_hub_name="my-hub",
        )
        src.read_stream(spark)

        spark.readStream.format.assert_called_once_with("eventhubs")
        reader.option.assert_any_call("eventhubs.connectionString", "Endpoint=sb://...")
        reader.option.assert_any_call("eventhubs.name", "my-hub")

    def test_consumer_group_default(self) -> None:
        src = EventHubsStreamingSource(
            connection_string="conn", event_hub_name="hub"
        )
        assert src.consumer_group == "$Default"

    def test_consumer_group_custom(self) -> None:
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = EventHubsStreamingSource(
            connection_string="conn",
            event_hub_name="hub",
            consumer_group="my-group",
        )
        src.read_stream(spark)
        reader.option.assert_any_call("eventhubs.consumerGroup", "my-group")

    def test_starting_position_end_of_stream(self) -> None:
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = EventHubsStreamingSource(
            connection_string="conn",
            event_hub_name="hub",
            starting_position=EventHubsStartingPosition.END_OF_STREAM,
        )
        src.read_stream(spark)
        reader.option.assert_any_call(
            "eventhubs.startingPosition", '{"offset": "@latest"}'
        )

    def test_starting_position_start_of_stream(self) -> None:
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = EventHubsStreamingSource(
            connection_string="conn",
            event_hub_name="hub",
            starting_position=EventHubsStartingPosition.START_OF_STREAM,
        )
        src.read_stream(spark)
        reader.option.assert_any_call(
            "eventhubs.startingPosition",
            '{"offset": "-1", "isInclusive": true}',
        )

    def test_max_events_per_trigger(self) -> None:
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = EventHubsStreamingSource(
            connection_string="conn",
            event_hub_name="hub",
            max_events_per_trigger=1000,
        )
        src.read_stream(spark)
        reader.option.assert_any_call("maxEventsPerTrigger", "1000")

    def test_max_events_not_set_when_none(self) -> None:
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = EventHubsStreamingSource(
            connection_string="conn", event_hub_name="hub"
        )
        src.read_stream(spark)
        # Verify maxEventsPerTrigger was NOT set
        for call in reader.option.call_args_list:
            assert call[0][0] != "maxEventsPerTrigger"

    def test_receiver_and_operation_timeout(self) -> None:
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = EventHubsStreamingSource(
            connection_string="conn",
            event_hub_name="hub",
            receiver_timeout="60",
            operation_timeout="120",
        )
        src.read_stream(spark)
        reader.option.assert_any_call("eventhubs.receiverTimeout", "60")
        reader.option.assert_any_call("eventhubs.operationTimeout", "120")

    def test_extra_options(self) -> None:
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = EventHubsStreamingSource(
            connection_string="conn",
            event_hub_name="hub",
            options={"custom.key": "val"},
        )
        src.read_stream(spark)
        reader.option.assert_any_call("custom.key", "val")

    def test_watermark_defaults_none(self) -> None:
        src = EventHubsStreamingSource(
            connection_string="conn", event_hub_name="hub"
        )
        assert src.watermark_column is None
        assert src.watermark_delay is None

    def test_starting_position_enum_values(self) -> None:
        assert EventHubsStartingPosition.START_OF_STREAM.value == "start_of_stream"
        assert EventHubsStartingPosition.END_OF_STREAM.value == "end_of_stream"

    def test_loads_without_start(self) -> None:
        """read_stream calls load() with no arguments (EventHubs pattern)."""
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = EventHubsStreamingSource(
            connection_string="conn", event_hub_name="hub"
        )
        src.read_stream(spark)
        reader.load.assert_called_once_with()


# ===================================================================
# TestKinesisStreamingSource
# ===================================================================


class TestKinesisStreamingSource:
    def test_read_stream_builds_chain(self) -> None:
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = KinesisStreamingSource(
            stream_name="my-stream",
            region="us-east-1",
        )
        src.read_stream(spark)

        spark.readStream.format.assert_called_once_with("kinesis")
        reader.option.assert_any_call("streamName", "my-stream")
        reader.option.assert_any_call("region", "us-east-1")

    def test_starting_position_default_latest(self) -> None:
        src = KinesisStreamingSource(stream_name="s", region="us-east-1")
        assert src.starting_position == KinesisStartingPosition.LATEST

    def test_starting_position_trim_horizon(self) -> None:
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = KinesisStreamingSource(
            stream_name="s",
            region="us-east-1",
            starting_position=KinesisStartingPosition.TRIM_HORIZON,
        )
        src.read_stream(spark)
        reader.option.assert_any_call("startingPosition", "trim_horizon")

    def test_endpoint_url(self) -> None:
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = KinesisStreamingSource(
            stream_name="s",
            region="us-east-1",
            endpoint_url="http://localhost:4566",
        )
        src.read_stream(spark)
        reader.option.assert_any_call("endpointUrl", "http://localhost:4566")

    def test_endpoint_not_set_when_none(self) -> None:
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = KinesisStreamingSource(stream_name="s", region="us-east-1")
        src.read_stream(spark)
        for call in reader.option.call_args_list:
            assert call[0][0] != "endpointUrl"

    def test_max_fetch_options(self) -> None:
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = KinesisStreamingSource(
            stream_name="s",
            region="us-east-1",
            max_fetch_records_per_shard=10000,
            max_fetch_time_per_shard_sec=30,
        )
        src.read_stream(spark)
        reader.option.assert_any_call("maxFetchRecordsPerShard", "10000")
        reader.option.assert_any_call("maxFetchTimePerShardSec", "30")

    def test_extra_options(self) -> None:
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = KinesisStreamingSource(
            stream_name="s",
            region="us-east-1",
            options={"roleArn": "arn:aws:iam::role/my-role"},
        )
        src.read_stream(spark)
        reader.option.assert_any_call("roleArn", "arn:aws:iam::role/my-role")

    def test_watermark_defaults_none(self) -> None:
        src = KinesisStreamingSource(stream_name="s", region="us-east-1")
        assert src.watermark_column is None
        assert src.watermark_delay is None

    def test_starting_position_enum_values(self) -> None:
        assert KinesisStartingPosition.LATEST.value == "latest"
        assert KinesisStartingPosition.TRIM_HORIZON.value == "trim_horizon"

    def test_loads_without_args(self) -> None:
        """read_stream calls load() with no arguments."""
        spark = MagicMock()
        reader = _self_returning_mock()
        spark.readStream.format.return_value = reader
        src = KinesisStreamingSource(stream_name="s", region="us-east-1")
        src.read_stream(spark)
        reader.load.assert_called_once_with()
