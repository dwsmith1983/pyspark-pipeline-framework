"""Built-in streaming sources."""

from __future__ import annotations

import enum
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pyspark_pipeline_framework.runtime.streaming.base import StreamingSource

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


@dataclass
class KafkaStreamingSource(StreamingSource):
    """Kafka streaming source.

    Args:
        bootstrap_servers: Comma-separated Kafka broker addresses.
        topics: Comma-separated topic names to subscribe to.
        starting_offsets: Starting offsets (``"latest"`` or ``"earliest"``).
    """

    bootstrap_servers: str
    topics: str
    starting_offsets: str = "latest"

    def read_stream(self, spark: SparkSession) -> DataFrame:
        return (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("subscribe", self.topics)
            .option("startingOffsets", self.starting_offsets)
            .load()
        )


@dataclass
class FileStreamingSource(StreamingSource):
    """File-based streaming source (CSV, JSON, Parquet, etc.).

    Args:
        path: Directory to watch for new files.
        file_format: File format (e.g. ``"parquet"``, ``"json"``, ``"csv"``).
        schema: Optional DDL schema string.
        options: Additional reader options.
    """

    path: str
    file_format: str = "parquet"
    schema: str | None = None
    options: dict[str, str] = field(default_factory=dict)

    def read_stream(self, spark: SparkSession) -> DataFrame:
        reader = spark.readStream.format(self.file_format)
        if self.schema:
            reader = reader.schema(self.schema)
        for key, value in self.options.items():
            reader = reader.option(key, value)
        return reader.load(self.path)


@dataclass
class DeltaStreamingSource(StreamingSource):
    """Delta Lake streaming source.

    Reads a Delta table as a streaming DataFrame.

    Args:
        path: Path to the Delta table.
        options: Additional reader options (e.g.
            ``{"ignoreChanges": "true"}``).
    """

    path: str
    options: dict[str, str] = field(default_factory=dict)

    def read_stream(self, spark: SparkSession) -> DataFrame:
        reader = spark.readStream.format("delta")
        for key, value in self.options.items():
            reader = reader.option(key, value)
        return reader.load(self.path)


@dataclass
class IcebergStreamingSource(StreamingSource):
    """Apache Iceberg streaming source.

    Reads an Iceberg table as a streaming DataFrame.

    Args:
        table: Fully qualified Iceberg table name
            (e.g. ``"catalog.db.table"``).
        options: Additional reader options.
    """

    table: str
    options: dict[str, str] = field(default_factory=dict)

    def read_stream(self, spark: SparkSession) -> DataFrame:
        reader = spark.readStream.format("iceberg")
        for key, value in self.options.items():
            reader = reader.option(key, value)
        return reader.load(self.table)


@dataclass
class RateStreamingSource(StreamingSource):
    """Spark built-in rate source for testing and benchmarking.

    Generates rows with ``(timestamp, value)`` at a configurable rate.

    Args:
        rows_per_second: Number of rows to generate per second.
        num_partitions: Number of output partitions.
    """

    rows_per_second: int = 1
    num_partitions: int = 1

    def read_stream(self, spark: SparkSession) -> DataFrame:
        return (
            spark.readStream.format("rate")
            .option("rowsPerSecond", self.rows_per_second)
            .option("numPartitions", self.num_partitions)
            .load()
        )


# ---------------------------------------------------------------------------
# Azure EventHubs
# ---------------------------------------------------------------------------


class EventHubsStartingPosition(str, enum.Enum):
    """Starting position for Azure EventHubs consumer."""

    START_OF_STREAM = "start_of_stream"
    END_OF_STREAM = "end_of_stream"


@dataclass
class EventHubsStreamingSource(StreamingSource):
    """Azure EventHubs streaming source.

    Requires the ``azure-eventhubs-spark`` connector on the classpath.

    Args:
        connection_string: EventHubs SAS connection string.
        event_hub_name: Name of the EventHub.
        consumer_group: Consumer group name.
        starting_position: Where to begin reading.
        max_events_per_trigger: Maximum events per micro-batch.
        receiver_timeout: Receiver timeout (e.g. ``"60"``).
        operation_timeout: Operation timeout (e.g. ``"60"``).
        options: Additional reader options.
    """

    connection_string: str
    event_hub_name: str
    consumer_group: str = "$Default"
    starting_position: EventHubsStartingPosition = EventHubsStartingPosition.END_OF_STREAM
    max_events_per_trigger: int | None = None
    receiver_timeout: str | None = None
    operation_timeout: str | None = None
    options: dict[str, str] = field(default_factory=dict)

    def read_stream(self, spark: SparkSession) -> DataFrame:
        reader = spark.readStream.format("eventhubs")
        reader = reader.option("eventhubs.connectionString", self.connection_string)
        reader = reader.option("eventhubs.name", self.event_hub_name)
        reader = reader.option("eventhubs.consumerGroup", self.consumer_group)

        position = (
            '{"offset": "-1", "isInclusive": true}'
            if self.starting_position == EventHubsStartingPosition.START_OF_STREAM
            else '{"offset": "@latest"}'
        )
        reader = reader.option("eventhubs.startingPosition", position)

        if self.max_events_per_trigger is not None:
            reader = reader.option("maxEventsPerTrigger", str(self.max_events_per_trigger))
        if self.receiver_timeout is not None:
            reader = reader.option("eventhubs.receiverTimeout", self.receiver_timeout)
        if self.operation_timeout is not None:
            reader = reader.option("eventhubs.operationTimeout", self.operation_timeout)

        for key, value in self.options.items():
            reader = reader.option(key, value)
        return reader.load()


# ---------------------------------------------------------------------------
# AWS Kinesis
# ---------------------------------------------------------------------------


class KinesisStartingPosition(str, enum.Enum):
    """Starting position for AWS Kinesis consumer."""

    LATEST = "latest"
    TRIM_HORIZON = "trim_horizon"


@dataclass
class KinesisStreamingSource(StreamingSource):
    """AWS Kinesis streaming source.

    Requires the ``spark-sql-kinesis`` connector on the classpath.

    Args:
        stream_name: Kinesis stream name.
        region: AWS region (e.g. ``"us-east-1"``).
        starting_position: Where to begin reading.
        endpoint_url: Custom endpoint URL (e.g. for LocalStack).
        max_fetch_records_per_shard: Max records per shard per fetch.
        max_fetch_time_per_shard_sec: Max time (sec) per shard fetch.
        options: Additional reader options.
    """

    stream_name: str
    region: str
    starting_position: KinesisStartingPosition = KinesisStartingPosition.LATEST
    endpoint_url: str | None = None
    max_fetch_records_per_shard: int | None = None
    max_fetch_time_per_shard_sec: int | None = None
    options: dict[str, str] = field(default_factory=dict)

    def read_stream(self, spark: SparkSession) -> DataFrame:
        reader = spark.readStream.format("kinesis")
        reader = reader.option("streamName", self.stream_name)
        reader = reader.option("region", self.region)
        reader = reader.option("startingPosition", self.starting_position.value)

        if self.endpoint_url is not None:
            reader = reader.option("endpointUrl", self.endpoint_url)
        if self.max_fetch_records_per_shard is not None:
            reader = reader.option("maxFetchRecordsPerShard", str(self.max_fetch_records_per_shard))
        if self.max_fetch_time_per_shard_sec is not None:
            reader = reader.option("maxFetchTimePerShardSec", str(self.max_fetch_time_per_shard_sec))

        for key, value in self.options.items():
            reader = reader.option(key, value)
        return reader.load()
