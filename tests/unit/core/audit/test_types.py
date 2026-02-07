"""Tests for audit event types."""

from __future__ import annotations

from datetime import datetime, timezone

from pyspark_pipeline_framework.core.audit.types import (
    AuditAction,
    AuditEvent,
    AuditStatus,
)


class TestAuditAction:
    def test_values(self) -> None:
        assert AuditAction.PIPELINE_STARTED == "pipeline_started"
        assert AuditAction.COMPONENT_FAILED == "component_failed"
        assert AuditAction.SECRET_ACCESSED == "secret_accessed"

    def test_is_str(self) -> None:
        assert isinstance(AuditAction.PIPELINE_STARTED, str)


class TestAuditStatus:
    def test_values(self) -> None:
        assert AuditStatus.SUCCESS == "success"
        assert AuditStatus.FAILURE == "failure"
        assert AuditStatus.RETRY == "retry"
        assert AuditStatus.WARNING == "warning"

    def test_is_str(self) -> None:
        assert isinstance(AuditStatus.SUCCESS, str)


class TestAuditEvent:
    def test_basic_construction(self) -> None:
        event = AuditEvent(
            action=AuditAction.PIPELINE_STARTED,
            actor="runner",
            resource="my-pipeline",
            status=AuditStatus.SUCCESS,
        )
        assert event.action == AuditAction.PIPELINE_STARTED
        assert event.actor == "runner"
        assert event.resource == "my-pipeline"
        assert event.status == AuditStatus.SUCCESS
        assert event.metadata == {}
        assert event.trace_id is None
        assert isinstance(event.timestamp, datetime)

    def test_with_metadata_and_trace(self) -> None:
        event = AuditEvent(
            action=AuditAction.COMPONENT_COMPLETED,
            actor="etl",
            resource="loader",
            status=AuditStatus.SUCCESS,
            metadata={"duration": "100"},
            trace_id="abc-123",
        )
        assert event.metadata == {"duration": "100"}
        assert event.trace_id == "abc-123"

    def test_custom_string_action(self) -> None:
        event = AuditEvent(
            action="custom_action",
            actor="test",
            resource="test",
            status=AuditStatus.SUCCESS,
        )
        assert event.action == "custom_action"

    def test_to_dict_with_enum_action(self) -> None:
        ts = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        event = AuditEvent(
            action=AuditAction.PIPELINE_STARTED,
            actor="runner",
            resource="pipe",
            status=AuditStatus.SUCCESS,
            timestamp=ts,
            metadata={"key": "val"},
            trace_id="t-1",
        )
        d = event.to_dict()
        assert d["action"] == "pipeline_started"
        assert d["actor"] == "runner"
        assert d["resource"] == "pipe"
        assert d["status"] == "success"
        assert d["timestamp"] == "2026-01-01T12:00:00+00:00"
        assert d["metadata"] == {"key": "val"}
        assert d["trace_id"] == "t-1"

    def test_to_dict_with_string_action(self) -> None:
        event = AuditEvent(
            action="custom_action",
            actor="test",
            resource="res",
            status=AuditStatus.WARNING,
        )
        d = event.to_dict()
        assert d["action"] == "custom_action"

    def test_default_timestamp_is_utc(self) -> None:
        event = AuditEvent(
            action=AuditAction.CONFIG_LOADED,
            actor="loader",
            resource="config",
            status=AuditStatus.SUCCESS,
        )
        assert event.timestamp.tzinfo is not None
