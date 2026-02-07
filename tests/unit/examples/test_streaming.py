"""Tests for streaming example components."""

from __future__ import annotations

from unittest.mock import MagicMock

from pyspark_pipeline_framework.examples.streaming import (
    FileToConsolePipeline,
    KafkaToDeltaPipeline,
)
from pyspark_pipeline_framework.runtime.streaming.base import (
    StreamingSink,
    StreamingSource,
    TriggerType,
)


def _mock_source() -> StreamingSource:
    return MagicMock(spec=StreamingSource)


def _mock_sink() -> StreamingSink:
    return MagicMock(spec=StreamingSink)


class TestFileToConsolePipeline:
    def test_name(self) -> None:
        pipeline = FileToConsolePipeline(
            source=_mock_source(), sink=_mock_sink()
        )
        assert pipeline.name == "FileToConsolePipeline"

    def test_source_and_sink(self) -> None:
        src = _mock_source()
        snk = _mock_sink()
        pipeline = FileToConsolePipeline(source=src, sink=snk)
        assert pipeline.source is src
        assert pipeline.sink is snk

    def test_default_trigger(self) -> None:
        pipeline = FileToConsolePipeline(
            source=_mock_source(), sink=_mock_sink()
        )
        trigger = pipeline.trigger
        assert trigger.trigger_type == TriggerType.PROCESSING_TIME
        assert trigger.interval == "10 seconds"

    def test_custom_trigger(self) -> None:
        pipeline = FileToConsolePipeline(
            source=_mock_source(),
            sink=_mock_sink(),
            trigger_interval="5 seconds",
        )
        assert pipeline.trigger.interval == "5 seconds"

    def test_transform_no_filter(self) -> None:
        pipeline = FileToConsolePipeline(
            source=_mock_source(), sink=_mock_sink()
        )
        df = MagicMock()
        assert pipeline.transform(df) is df

    def test_transform_with_filter(self) -> None:
        pipeline = FileToConsolePipeline(
            source=_mock_source(),
            sink=_mock_sink(),
            filter_condition="col > 0",
        )
        df = MagicMock()
        filtered = MagicMock()
        df.filter.return_value = filtered

        result = pipeline.transform(df)

        df.filter.assert_called_once_with("col > 0")
        assert result is filtered


class TestKafkaToDeltaPipeline:
    def test_name(self) -> None:
        pipeline = KafkaToDeltaPipeline(
            source=_mock_source(), sink=_mock_sink()
        )
        assert pipeline.name == "KafkaToDeltaPipeline"

    def test_default_trigger(self) -> None:
        pipeline = KafkaToDeltaPipeline(
            source=_mock_source(), sink=_mock_sink()
        )
        assert pipeline.trigger.interval == "30 seconds"

    def test_custom_trigger(self) -> None:
        pipeline = KafkaToDeltaPipeline(
            source=_mock_source(),
            sink=_mock_sink(),
            trigger_interval="1 minute",
        )
        assert pipeline.trigger.interval == "1 minute"
