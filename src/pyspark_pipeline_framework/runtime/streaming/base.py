"""Streaming abstractions for Spark Structured Streaming pipelines."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING

from pyspark_pipeline_framework.runtime.dataflow.base import DataFlow

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.streaming import DataStreamWriter, StreamingQuery


class OutputMode(str, Enum):
    """Spark Structured Streaming output modes."""

    APPEND = "append"
    COMPLETE = "complete"
    UPDATE = "update"


class TriggerType(str, Enum):
    """Spark Structured Streaming trigger types."""

    PROCESSING_TIME = "processing_time"
    ONCE = "once"
    AVAILABLE_NOW = "available_now"
    CONTINUOUS = "continuous"


@dataclass
class TriggerConfig:
    """Configuration for a streaming trigger.

    Args:
        trigger_type: The type of trigger to use.
        interval: Interval string (e.g. ``"10 seconds"``).  Required for
            ``PROCESSING_TIME`` and ``CONTINUOUS`` triggers.
    """

    trigger_type: TriggerType
    interval: str | None = None

    def __post_init__(self) -> None:
        needs_interval = {TriggerType.PROCESSING_TIME, TriggerType.CONTINUOUS}
        if self.trigger_type in needs_interval and not self.interval:
            raise ValueError(f"interval is required for {self.trigger_type.value} trigger")


class StreamingSource(ABC):
    """Base class for streaming data sources."""

    @abstractmethod
    def read_stream(self, spark: SparkSession) -> DataFrame:
        """Create a streaming DataFrame from this source."""
        ...

    @property
    def watermark_column(self) -> str | None:
        """Column to use for watermarking.  Override if needed."""
        return None

    @property
    def watermark_delay(self) -> str | None:
        """Watermark delay (e.g. ``"10 seconds"``).  Override if needed."""
        return None


class StreamingSink(ABC):
    """Base class for streaming data sinks.

    Concrete subclasses must provide ``output_mode``, ``checkpoint_location``
    (as dataclass fields or properties), and implement ``write_stream()``.
    """

    output_mode: OutputMode
    """Output mode for this sink."""

    checkpoint_location: str
    """Checkpoint location for this stream."""

    @abstractmethod
    def write_stream(self, df: DataFrame) -> DataStreamWriter:
        """Configure the streaming write operation."""
        ...

    @property
    def query_name(self) -> str | None:
        """Optional name for the streaming query."""
        return None


class StreamingPipeline(DataFlow, ABC):
    """Combines a streaming source, optional transformation, and sink.

    Subclasses must define :attr:`source`, :attr:`sink`, and
    :attr:`name`.  Override :meth:`transform` to add logic between
    read and write.

    Two execution modes:

    * ``run()`` — starts the stream and **blocks** until termination.
    * ``start_stream()`` — starts the stream and returns the
      ``StreamingQuery`` handle for programmatic control.
    """

    @property
    @abstractmethod
    def source(self) -> StreamingSource:
        """The streaming source to read from."""
        ...

    @property
    @abstractmethod
    def sink(self) -> StreamingSink:
        """The streaming sink to write to."""
        ...

    @property
    def trigger(self) -> TriggerConfig:
        """Trigger configuration.  Override to customise."""
        return TriggerConfig(TriggerType.PROCESSING_TIME, "10 seconds")

    def transform(self, df: DataFrame) -> DataFrame:
        """Optional transformation applied between source and sink.

        The default implementation is the identity function.
        """
        return df

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    def run(self) -> None:
        """Start streaming and block until terminated."""
        query = self.start_stream()
        query.awaitTermination()

    def start_stream(self) -> StreamingQuery:
        """Start streaming and return the query handle."""
        df = self.source.read_stream(self.spark)

        # Apply watermark if configured
        wm_col = self.source.watermark_column
        wm_delay = self.source.watermark_delay
        if wm_col and wm_delay:
            df = df.withWatermark(wm_col, wm_delay)

        df = self.transform(df)

        writer = self.sink.write_stream(df)
        writer = writer.outputMode(self.sink.output_mode.value)
        writer = writer.option("checkpointLocation", self.sink.checkpoint_location)

        if self.sink.query_name:
            writer = writer.queryName(self.sink.query_name)

        # Apply trigger
        trigger = self.trigger
        if trigger.trigger_type == TriggerType.PROCESSING_TIME:
            assert trigger.interval is not None  # guaranteed by TriggerConfig
            writer = writer.trigger(processingTime=trigger.interval)
        elif trigger.trigger_type == TriggerType.ONCE:
            writer = writer.trigger(once=True)
        elif trigger.trigger_type == TriggerType.AVAILABLE_NOW:
            writer = writer.trigger(availableNow=True)
        elif trigger.trigger_type == TriggerType.CONTINUOUS:
            assert trigger.interval is not None  # guaranteed by TriggerConfig
            writer = writer.trigger(continuous=trigger.interval)

        return writer.start()
