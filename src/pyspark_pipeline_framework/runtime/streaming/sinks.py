"""Built-in streaming sinks."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from pyspark_pipeline_framework.runtime.streaming.base import OutputMode, StreamingSink

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.streaming import DataStreamWriter


@dataclass
class KafkaStreamingSink(StreamingSink):
    """Kafka streaming sink.

    Args:
        bootstrap_servers: Comma-separated Kafka broker addresses.
        topic: Target Kafka topic.
        checkpoint_location: Checkpoint directory path.
        output_mode: Streaming output mode.
    """

    bootstrap_servers: str
    topic: str
    checkpoint_location: str
    output_mode: OutputMode = OutputMode.APPEND

    def write_stream(self, df: DataFrame) -> DataStreamWriter:
        return (
            df.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", self.bootstrap_servers)
            .option("topic", self.topic)
        )


@dataclass
class DeltaStreamingSink(StreamingSink):
    """Delta Lake streaming sink.

    Args:
        path: Target Delta table path.
        checkpoint_location: Checkpoint directory path.
        output_mode: Streaming output mode.
        partition_by: Columns to partition the output by.
    """

    path: str
    checkpoint_location: str
    output_mode: OutputMode = OutputMode.APPEND
    partition_by: list[str] = field(default_factory=list)

    def write_stream(self, df: DataFrame) -> DataStreamWriter:
        writer = df.writeStream.format("delta")
        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)
        return writer.option("path", self.path)


@dataclass
class ConsoleStreamingSink(StreamingSink):
    """Console sink for debugging.

    Args:
        checkpoint_location: Checkpoint directory path.
        output_mode: Streaming output mode.
        truncate: Whether to truncate long strings in output.
    """

    checkpoint_location: str = "/tmp/console-checkpoint"
    output_mode: OutputMode = OutputMode.APPEND
    truncate: bool = False

    def write_stream(self, df: DataFrame) -> DataStreamWriter:
        return df.writeStream.format("console").option("truncate", self.truncate)


@dataclass
class IcebergStreamingSink(StreamingSink):
    """Apache Iceberg streaming sink.

    Args:
        table: Fully qualified Iceberg table name
            (e.g. ``"catalog.db.table"``).
        checkpoint_location: Checkpoint directory path.
        output_mode: Streaming output mode.
        partition_by: Columns to partition the output by.
    """

    table: str
    checkpoint_location: str
    output_mode: OutputMode = OutputMode.APPEND
    partition_by: list[str] = field(default_factory=list)

    def write_stream(self, df: DataFrame) -> DataStreamWriter:
        writer = df.writeStream.format("iceberg")
        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)
        return writer.option("path", self.table)


@dataclass
class FileStreamingSink(StreamingSink):
    """File-based streaming sink (Parquet, JSON, CSV, etc.).

    Args:
        path: Output directory path.
        file_format: File format (e.g. ``"parquet"``, ``"json"``, ``"csv"``).
        checkpoint_location: Checkpoint directory path.
        output_mode: Streaming output mode.
        partition_by: Columns to partition the output by.
    """

    path: str
    file_format: str = "parquet"
    checkpoint_location: str = ""
    output_mode: OutputMode = OutputMode.APPEND
    partition_by: list[str] = field(default_factory=list)

    def write_stream(self, df: DataFrame) -> DataStreamWriter:
        writer = df.writeStream.format(self.file_format)
        if self.partition_by:
            writer = writer.partitionBy(*self.partition_by)
        return writer.option("path", self.path)
