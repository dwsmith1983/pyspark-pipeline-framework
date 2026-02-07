"""Example streaming pipeline components.

Reference implementation of
:class:`~pyspark_pipeline_framework.runtime.streaming.base.StreamingPipeline`
showing how to compose a streaming source, transformation, and sink.

Example usage::

    from pyspark_pipeline_framework.examples.streaming import (
        FileToConsolePipeline,
    )
    from pyspark_pipeline_framework.runtime.streaming.sources import (
        FileStreamingSource,
    )
    from pyspark_pipeline_framework.runtime.streaming.sinks import (
        ConsoleStreamingSink,
    )

    pipeline = FileToConsolePipeline(
        source=FileStreamingSource(path="/data/input", file_format="json"),
        sink=ConsoleStreamingSink(checkpoint_location="/tmp/checkpoint"),
        filter_condition="value IS NOT NULL",
    )
    pipeline.set_spark_session(spark)
    pipeline.run()  # blocks until terminated
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from pyspark_pipeline_framework.runtime.streaming.base import (
    StreamingPipeline,
    StreamingSink,
    StreamingSource,
    TriggerConfig,
    TriggerType,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame


class FileToConsolePipeline(StreamingPipeline):
    """Stream files from a directory to the console with optional filtering.

    A complete streaming pipeline that reads from a file source, applies
    an optional SQL filter, and writes to the console sink. Useful for
    development and debugging.

    Args:
        source: The streaming source to read from.
        sink: The streaming sink to write to.
        filter_condition: Optional SQL WHERE clause to filter rows.
        trigger_interval: Processing time trigger interval.
            Defaults to ``"10 seconds"``.

    Example::

        pipeline = FileToConsolePipeline(
            source=FileStreamingSource(path="/data/events", file_format="json"),
            sink=ConsoleStreamingSink(checkpoint_location="/tmp/ckpt"),
            filter_condition="event_type = 'purchase'",
            trigger_interval="5 seconds",
        )
        pipeline.set_spark_session(spark)
        query = pipeline.start_stream()  # non-blocking
        query.awaitTermination(timeout=60)
    """

    def __init__(
        self,
        source: StreamingSource,
        sink: StreamingSink,
        filter_condition: str | None = None,
        trigger_interval: str = "10 seconds",
    ) -> None:
        super().__init__()
        self._source = source
        self._sink = sink
        self._filter_condition = filter_condition
        self._trigger_interval = trigger_interval

    @property
    def name(self) -> str:
        """Return a descriptive pipeline name."""
        return "FileToConsolePipeline"

    @property
    def source(self) -> StreamingSource:
        """Return the streaming source."""
        return self._source

    @property
    def sink(self) -> StreamingSink:
        """Return the streaming sink."""
        return self._sink

    @property
    def trigger(self) -> TriggerConfig:
        """Return the trigger configuration."""
        return TriggerConfig(
            TriggerType.PROCESSING_TIME, self._trigger_interval
        )

    def transform(self, df: DataFrame) -> DataFrame:
        """Apply the optional filter condition.

        Args:
            df: Incoming streaming DataFrame.

        Returns:
            Filtered DataFrame, or the original if no filter is set.
        """
        if self._filter_condition:
            return df.filter(self._filter_condition)
        return df


class KafkaToDeltaPipeline(StreamingPipeline):
    """Stream records from Kafka to a Delta Lake table.

    A production-oriented streaming pipeline that reads from Kafka,
    applies an optional transformation, and writes to Delta Lake.

    Args:
        source: A Kafka (or other) streaming source.
        sink: A Delta (or other) streaming sink.
        trigger_interval: Processing time trigger interval.
            Defaults to ``"30 seconds"``.

    Example::

        from pyspark_pipeline_framework.runtime.streaming.sources import (
            KafkaStreamingSource,
        )
        from pyspark_pipeline_framework.runtime.streaming.sinks import (
            DeltaStreamingSink,
        )

        pipeline = KafkaToDeltaPipeline(
            source=KafkaStreamingSource(
                bootstrap_servers="broker:9092",
                topics="events",
            ),
            sink=DeltaStreamingSink(
                path="/data/delta/events",
                checkpoint_location="/checkpoints/events",
            ),
        )
        pipeline.set_spark_session(spark)
        pipeline.run()
    """

    def __init__(
        self,
        source: StreamingSource,
        sink: StreamingSink,
        trigger_interval: str = "30 seconds",
    ) -> None:
        super().__init__()
        self._source = source
        self._sink = sink
        self._trigger_interval = trigger_interval

    @property
    def name(self) -> str:
        """Return a descriptive pipeline name."""
        return "KafkaToDeltaPipeline"

    @property
    def source(self) -> StreamingSource:
        """Return the streaming source."""
        return self._source

    @property
    def sink(self) -> StreamingSink:
        """Return the streaming sink."""
        return self._sink

    @property
    def trigger(self) -> TriggerConfig:
        """Return the trigger configuration."""
        return TriggerConfig(
            TriggerType.PROCESSING_TIME, self._trigger_interval
        )
