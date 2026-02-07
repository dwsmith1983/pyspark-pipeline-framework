"""Tests for streaming base abstractions."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pyspark_pipeline_framework.runtime.streaming.base import (
    OutputMode,
    StreamingPipeline,
    StreamingSink,
    StreamingSource,
    TriggerConfig,
    TriggerType,
)

# ---------------------------------------------------------------------------
# Concrete test doubles
# ---------------------------------------------------------------------------


class _StubSource(StreamingSource):
    def __init__(self, *, watermark_col: str | None = None, watermark_delay: str | None = None) -> None:
        self._wm_col = watermark_col
        self._wm_delay = watermark_delay

    def read_stream(self, spark: object) -> object:  # type: ignore[override]
        return spark.readStream.format("stub").load()  # type: ignore[union-attr]

    @property
    def watermark_column(self) -> str | None:
        return self._wm_col

    @property
    def watermark_delay(self) -> str | None:
        return self._wm_delay


class _StubSink(StreamingSink):
    def __init__(
        self,
        checkpoint: str = "/tmp/ckpt",
        mode: OutputMode = OutputMode.APPEND,
        name: str | None = None,
    ) -> None:
        self._checkpoint = checkpoint
        self._mode = mode
        self._name = name

    def write_stream(self, df: object) -> object:  # type: ignore[override]
        return df.writeStream.format("stub")  # type: ignore[union-attr]

    @property
    def output_mode(self) -> OutputMode:
        return self._mode

    @property
    def checkpoint_location(self) -> str:
        return self._checkpoint

    @property
    def query_name(self) -> str | None:
        return self._name


class _StubPipeline(StreamingPipeline):
    def __init__(
        self,
        src: StreamingSource,
        snk: StreamingSink,
        trigger: TriggerConfig | None = None,
    ) -> None:
        super().__init__()
        self._source = src
        self._sink = snk
        self._trigger = trigger or TriggerConfig(TriggerType.PROCESSING_TIME, "10 seconds")

    @property
    def name(self) -> str:
        return "stub-pipeline"

    @property
    def source(self) -> StreamingSource:
        return self._source

    @property
    def sink(self) -> StreamingSink:
        return self._sink

    @property
    def trigger(self) -> TriggerConfig:
        return self._trigger


def _mock_spark() -> MagicMock:
    """Return a MagicMock that supports the readStream/writeStream chain."""
    spark = MagicMock()
    # readStream chain returns itself for fluent calls
    spark.readStream.format.return_value = spark.readStream.format.return_value
    return spark


# ===================================================================
# TestOutputMode
# ===================================================================


class TestOutputMode:
    def test_values(self) -> None:
        assert OutputMode.APPEND.value == "append"
        assert OutputMode.COMPLETE.value == "complete"
        assert OutputMode.UPDATE.value == "update"

    def test_is_str(self) -> None:
        assert isinstance(OutputMode.APPEND, str)


# ===================================================================
# TestTriggerType
# ===================================================================


class TestTriggerType:
    def test_values(self) -> None:
        assert TriggerType.PROCESSING_TIME.value == "processing_time"
        assert TriggerType.ONCE.value == "once"
        assert TriggerType.AVAILABLE_NOW.value == "available_now"
        assert TriggerType.CONTINUOUS.value == "continuous"

    def test_is_str(self) -> None:
        assert isinstance(TriggerType.ONCE, str)


# ===================================================================
# TestTriggerConfig
# ===================================================================


class TestTriggerConfig:
    def test_processing_time_requires_interval(self) -> None:
        with pytest.raises(ValueError, match="interval is required"):
            TriggerConfig(TriggerType.PROCESSING_TIME)

    def test_continuous_requires_interval(self) -> None:
        with pytest.raises(ValueError, match="interval is required"):
            TriggerConfig(TriggerType.CONTINUOUS)

    def test_once_no_interval_ok(self) -> None:
        cfg = TriggerConfig(TriggerType.ONCE)
        assert cfg.interval is None

    def test_available_now_no_interval_ok(self) -> None:
        cfg = TriggerConfig(TriggerType.AVAILABLE_NOW)
        assert cfg.interval is None

    def test_processing_time_with_interval(self) -> None:
        cfg = TriggerConfig(TriggerType.PROCESSING_TIME, "5 seconds")
        assert cfg.interval == "5 seconds"


# ===================================================================
# TestStreamingSource
# ===================================================================


class TestStreamingSource:
    def test_watermark_defaults_none(self) -> None:
        src = _StubSource()
        assert src.watermark_column is None
        assert src.watermark_delay is None

    def test_watermark_override(self) -> None:
        src = _StubSource(watermark_col="ts", watermark_delay="10 seconds")
        assert src.watermark_column == "ts"
        assert src.watermark_delay == "10 seconds"


# ===================================================================
# TestStreamingSink
# ===================================================================


class TestStreamingSink:
    def test_query_name_default_none(self) -> None:
        snk = _StubSink()
        assert snk.query_name is None

    def test_query_name_override(self) -> None:
        snk = _StubSink(name="my-query")
        assert snk.query_name == "my-query"

    def test_abstract_properties(self) -> None:
        snk = _StubSink(checkpoint="/data/ckpt", mode=OutputMode.COMPLETE)
        assert snk.checkpoint_location == "/data/ckpt"
        assert snk.output_mode == OutputMode.COMPLETE


# ===================================================================
# TestStreamingPipeline
# ===================================================================


class TestStreamingPipeline:
    def test_start_stream_basic(self) -> None:
        spark = _mock_spark()
        src = _StubSource()
        snk = _StubSink()
        pipeline = _StubPipeline(src, snk)
        pipeline.set_spark_session(spark)

        pipeline.start_stream()

        # Verify readStream was used
        spark.readStream.format.assert_called_once_with("stub")

    def test_watermark_applied(self) -> None:
        spark = _mock_spark()
        src = _StubSource(watermark_col="event_time", watermark_delay="5 minutes")
        snk = _StubSink()
        pipeline = _StubPipeline(src, snk)
        pipeline.set_spark_session(spark)

        pipeline.start_stream()

        # The DataFrame returned by read_stream should have withWatermark called
        read_df = spark.readStream.format.return_value.load.return_value
        read_df.withWatermark.assert_called_once_with("event_time", "5 minutes")

    def test_watermark_not_applied_when_absent(self) -> None:
        spark = _mock_spark()
        src = _StubSource()
        snk = _StubSink()
        pipeline = _StubPipeline(src, snk)
        pipeline.set_spark_session(spark)

        pipeline.start_stream()

        read_df = spark.readStream.format.return_value.load.return_value
        read_df.withWatermark.assert_not_called()

    def test_checkpoint_location_set(self) -> None:
        spark = _mock_spark()
        src = _StubSource()
        snk = _StubSink(checkpoint="/my/checkpoint")
        pipeline = _StubPipeline(src, snk)
        pipeline.set_spark_session(spark)

        pipeline.start_stream()

        # Find the option("checkpointLocation", ...) call in the chain
        read_df = spark.readStream.format.return_value.load.return_value
        writer = read_df.writeStream.format.return_value
        writer.outputMode.return_value.option.assert_called_once_with(
            "checkpointLocation", "/my/checkpoint"
        )

    def test_query_name_set(self) -> None:
        spark = _mock_spark()
        src = _StubSource()
        snk = _StubSink(name="my-query")
        pipeline = _StubPipeline(src, snk)
        pipeline.set_spark_session(spark)

        pipeline.start_stream()

        # queryName should be called somewhere in the chain
        read_df = spark.readStream.format.return_value.load.return_value
        writer_chain = read_df.writeStream.format.return_value
        # The chain: format -> outputMode -> option -> queryName -> trigger -> start
        option_result = writer_chain.outputMode.return_value.option.return_value
        option_result.queryName.assert_called_once_with("my-query")

    def test_trigger_processing_time(self) -> None:
        spark = _mock_spark()
        src = _StubSource()
        snk = _StubSink()
        trigger = TriggerConfig(TriggerType.PROCESSING_TIME, "30 seconds")
        pipeline = _StubPipeline(src, snk, trigger=trigger)
        pipeline.set_spark_session(spark)

        pipeline.start_stream()

        # Find the trigger call in the write chain
        read_df = spark.readStream.format.return_value.load.return_value
        writer_chain = read_df.writeStream.format.return_value
        option_result = writer_chain.outputMode.return_value.option.return_value
        option_result.trigger.assert_called_once_with(processingTime="30 seconds")

    def test_trigger_once(self) -> None:
        spark = _mock_spark()
        src = _StubSource()
        snk = _StubSink()
        trigger = TriggerConfig(TriggerType.ONCE)
        pipeline = _StubPipeline(src, snk, trigger=trigger)
        pipeline.set_spark_session(spark)

        pipeline.start_stream()

        read_df = spark.readStream.format.return_value.load.return_value
        writer_chain = read_df.writeStream.format.return_value
        option_result = writer_chain.outputMode.return_value.option.return_value
        option_result.trigger.assert_called_once_with(once=True)

    def test_trigger_available_now(self) -> None:
        spark = _mock_spark()
        src = _StubSource()
        snk = _StubSink()
        trigger = TriggerConfig(TriggerType.AVAILABLE_NOW)
        pipeline = _StubPipeline(src, snk, trigger=trigger)
        pipeline.set_spark_session(spark)

        pipeline.start_stream()

        read_df = spark.readStream.format.return_value.load.return_value
        writer_chain = read_df.writeStream.format.return_value
        option_result = writer_chain.outputMode.return_value.option.return_value
        option_result.trigger.assert_called_once_with(availableNow=True)

    def test_trigger_continuous(self) -> None:
        spark = _mock_spark()
        src = _StubSource()
        snk = _StubSink()
        trigger = TriggerConfig(TriggerType.CONTINUOUS, "1 second")
        pipeline = _StubPipeline(src, snk, trigger=trigger)
        pipeline.set_spark_session(spark)

        pipeline.start_stream()

        read_df = spark.readStream.format.return_value.load.return_value
        writer_chain = read_df.writeStream.format.return_value
        option_result = writer_chain.outputMode.return_value.option.return_value
        option_result.trigger.assert_called_once_with(continuous="1 second")

    def test_run_blocks_on_await_termination(self) -> None:
        spark = _mock_spark()
        src = _StubSource()
        snk = _StubSink()
        pipeline = _StubPipeline(src, snk)
        pipeline.set_spark_session(spark)

        pipeline.run()

        # The query returned by start() should have awaitTermination called
        read_df = spark.readStream.format.return_value.load.return_value
        writer_chain = read_df.writeStream.format.return_value
        option_result = writer_chain.outputMode.return_value.option.return_value
        query = option_result.trigger.return_value.start.return_value
        query.awaitTermination.assert_called_once()

    def test_transform_override(self) -> None:
        spark = _mock_spark()
        src = _StubSource()
        snk = _StubSink()
        transformed = MagicMock()

        class _TransformPipeline(_StubPipeline):
            def transform(self, df: object) -> object:  # type: ignore[override]
                return transformed

        pipeline = _TransformPipeline(src, snk)
        pipeline.set_spark_session(spark)

        pipeline.start_stream()

        # The write_stream should receive the transformed df
        transformed.writeStream.format.assert_called_once_with("stub")

    def test_default_trigger(self) -> None:
        src = _StubSource()
        snk = _StubSink()
        pipeline = _StubPipeline(src, snk)
        # Override to use the parent's default
        pipeline._trigger = TriggerConfig(TriggerType.PROCESSING_TIME, "10 seconds")

        trigger = pipeline.trigger
        assert trigger.trigger_type == TriggerType.PROCESSING_TIME
        assert trigger.interval == "10 seconds"
