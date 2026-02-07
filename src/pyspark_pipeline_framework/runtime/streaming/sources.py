"""Built-in streaming sources."""

from __future__ import annotations

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
