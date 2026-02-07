"""Tests for audit sinks."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock

from pyspark_pipeline_framework.core.audit.sinks import (
    AuditSink,
    CompositeAuditSink,
    FileAuditSink,
    LoggingAuditSink,
)
from pyspark_pipeline_framework.core.audit.types import (
    AuditAction,
    AuditEvent,
    AuditStatus,
)


def _sample_event(
    status: AuditStatus = AuditStatus.SUCCESS,
) -> AuditEvent:
    return AuditEvent(
        action=AuditAction.PIPELINE_STARTED,
        actor="runner",
        resource="my-pipeline",
        status=status,
        timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


class TestLoggingAuditSink:
    def test_emits_info_on_success(self) -> None:
        sink = LoggingAuditSink()
        # Should not raise
        sink.emit(_sample_event(AuditStatus.SUCCESS))

    def test_emits_warning_on_failure(self) -> None:
        sink = LoggingAuditSink()
        # Should not raise
        sink.emit(_sample_event(AuditStatus.FAILURE))

    def test_custom_logger_name(self) -> None:
        sink = LoggingAuditSink(logger_name="custom.audit")
        sink.emit(_sample_event())

    def test_string_action_in_log(self) -> None:
        sink = LoggingAuditSink()
        event = AuditEvent(
            action="custom_action",
            actor="test",
            resource="res",
            status=AuditStatus.SUCCESS,
            timestamp=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )
        sink.emit(event)


class TestFileAuditSink:
    def test_writes_jsonl(self, tmp_path: Path) -> None:
        path = tmp_path / "audit.jsonl"
        sink = FileAuditSink(path)
        try:
            sink.emit(_sample_event())
            sink.emit(_sample_event(AuditStatus.FAILURE))
        finally:
            sink.close()

        lines = path.read_text().strip().split("\n")
        assert len(lines) == 2
        data = json.loads(lines[0])
        assert data["action"] == "pipeline_started"
        assert data["actor"] == "runner"

    def test_creates_parent_dirs(self, tmp_path: Path) -> None:
        path = tmp_path / "sub" / "dir" / "audit.jsonl"
        sink = FileAuditSink(path)
        try:
            sink.emit(_sample_event())
        finally:
            sink.close()
        assert path.exists()

    def test_close_idempotent(self, tmp_path: Path) -> None:
        path = tmp_path / "audit.jsonl"
        sink = FileAuditSink(path)
        sink.emit(_sample_event())
        sink.close()
        sink.close()  # Should not raise

    def test_close_without_open(self) -> None:
        sink = FileAuditSink("/tmp/never_opened.jsonl")
        sink.close()  # Should not raise

    def test_appends_to_existing(self, tmp_path: Path) -> None:
        path = tmp_path / "audit.jsonl"
        path.write_text('{"existing": true}\n')
        sink = FileAuditSink(path)
        try:
            sink.emit(_sample_event())
        finally:
            sink.close()
        lines = path.read_text().strip().split("\n")
        assert len(lines) == 2


class TestCompositeAuditSink:
    def test_fans_out_to_all_sinks(self) -> None:
        sink1 = MagicMock(spec=AuditSink)
        sink2 = MagicMock(spec=AuditSink)
        composite = CompositeAuditSink(sink1, sink2)

        event = _sample_event()
        composite.emit(event)

        sink1.emit.assert_called_once_with(event)
        sink2.emit.assert_called_once_with(event)

    def test_one_failure_does_not_block_others(self) -> None:
        sink1 = MagicMock(spec=AuditSink)
        sink1.emit.side_effect = RuntimeError("boom")
        sink2 = MagicMock(spec=AuditSink)
        composite = CompositeAuditSink(sink1, sink2)

        composite.emit(_sample_event())

        sink2.emit.assert_called_once()

    def test_close_all(self) -> None:
        sink1 = MagicMock(spec=AuditSink)
        sink2 = MagicMock(spec=AuditSink)
        composite = CompositeAuditSink(sink1, sink2)

        composite.close()

        sink1.close.assert_called_once()
        sink2.close.assert_called_once()

    def test_close_failure_does_not_block_others(self) -> None:
        sink1 = MagicMock(spec=AuditSink)
        sink1.close.side_effect = RuntimeError("boom")
        sink2 = MagicMock(spec=AuditSink)
        composite = CompositeAuditSink(sink1, sink2)

        composite.close()

        sink2.close.assert_called_once()

    def test_emit_all(self) -> None:
        sink = MagicMock(spec=AuditSink)
        composite = CompositeAuditSink(sink)

        events = [_sample_event(), _sample_event(AuditStatus.FAILURE)]
        composite.emit_all(events)

        assert sink.emit.call_count == 2
