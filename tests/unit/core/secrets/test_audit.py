"""Tests for SecretsAuditLogger."""

from __future__ import annotations

from unittest.mock import MagicMock, call

from pyspark_pipeline_framework.core.audit.sinks import AuditSink
from pyspark_pipeline_framework.core.audit.types import AuditAction, AuditEvent, AuditStatus
from pyspark_pipeline_framework.core.secrets.audit import SecretsAuditLogger
from pyspark_pipeline_framework.core.secrets.base import (
    SecretResolutionResult,
    SecretResolutionStatus,
    SecretsReference,
)
from pyspark_pipeline_framework.core.secrets.resolver import SecretsResolver


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _resolver_returning(
    status: SecretResolutionStatus,
    value: str | None = None,
    error: str | None = None,
) -> SecretsResolver:
    resolver = MagicMock(spec=SecretsResolver)
    resolver.resolve.side_effect = lambda ref: SecretResolutionResult(
        reference=ref,
        status=status,
        value=value,
        error=error,
    )
    return resolver


def _captured_event(sink: MagicMock) -> AuditEvent:
    """Return the AuditEvent passed to the first ``emit()`` call."""
    sink.emit.assert_called_once()
    return sink.emit.call_args[0][0]


# ---------------------------------------------------------------------------
# TestSecretsAuditLogger
# ---------------------------------------------------------------------------


class TestSecretsAuditLogger:
    def test_delegates_resolve_to_inner(self) -> None:
        resolver = _resolver_returning(SecretResolutionStatus.SUCCESS, value="s3cr3t")
        sink = MagicMock(spec=AuditSink)
        audit = SecretsAuditLogger(resolver, sink)

        ref = SecretsReference(provider="env", key="DB_PASS")
        result = audit.resolve(ref)

        resolver.resolve.assert_called_once_with(ref)
        assert result.status == SecretResolutionStatus.SUCCESS
        assert result.value == "s3cr3t"

    def test_emits_audit_event_on_success(self) -> None:
        resolver = _resolver_returning(SecretResolutionStatus.SUCCESS, value="val")
        sink = MagicMock(spec=AuditSink)
        audit = SecretsAuditLogger(resolver, sink)

        audit.resolve(SecretsReference(provider="aws", key="api-key"))

        event = _captured_event(sink)
        assert event.action == AuditAction.SECRET_ACCESSED
        assert event.status == AuditStatus.SUCCESS
        assert event.resource == "aws:api-key"

    def test_event_metadata_has_provider_and_key(self) -> None:
        resolver = _resolver_returning(SecretResolutionStatus.SUCCESS, value="v")
        sink = MagicMock(spec=AuditSink)
        audit = SecretsAuditLogger(resolver, sink)

        audit.resolve(SecretsReference(provider="vault", key="db/creds"))

        event = _captured_event(sink)
        assert event.metadata["provider"] == "vault"
        assert event.metadata["key"] == "db/creds"
        assert event.metadata["resolution_status"] == "success"

    def test_secret_value_never_in_event(self) -> None:
        resolver = _resolver_returning(SecretResolutionStatus.SUCCESS, value="TOP_SECRET")
        sink = MagicMock(spec=AuditSink)
        audit = SecretsAuditLogger(resolver, sink)

        audit.resolve(SecretsReference(provider="env", key="k"))

        event = _captured_event(sink)
        event_dict = event.to_dict()
        # Value must not appear anywhere in the audit event
        assert "TOP_SECRET" not in str(event_dict)

    def test_error_result_maps_to_failure_status(self) -> None:
        resolver = _resolver_returning(
            SecretResolutionStatus.ERROR, error="connection refused"
        )
        sink = MagicMock(spec=AuditSink)
        audit = SecretsAuditLogger(resolver, sink)

        audit.resolve(SecretsReference(provider="aws", key="k"))

        event = _captured_event(sink)
        assert event.status == AuditStatus.FAILURE
        assert event.metadata["error"] == "connection refused"

    def test_not_found_maps_to_warning_status(self) -> None:
        resolver = _resolver_returning(SecretResolutionStatus.NOT_FOUND)
        sink = MagicMock(spec=AuditSink)
        audit = SecretsAuditLogger(resolver, sink)

        audit.resolve(SecretsReference(provider="env", key="MISSING"))

        event = _captured_event(sink)
        assert event.status == AuditStatus.WARNING
        assert event.metadata["resolution_status"] == "not_found"

    def test_custom_actor(self) -> None:
        resolver = _resolver_returning(SecretResolutionStatus.SUCCESS, value="v")
        sink = MagicMock(spec=AuditSink)
        audit = SecretsAuditLogger(resolver, sink, actor="my-pipeline")

        audit.resolve(SecretsReference(provider="env", key="k"))

        event = _captured_event(sink)
        assert event.actor == "my-pipeline"

    def test_default_actor(self) -> None:
        resolver = _resolver_returning(SecretResolutionStatus.SUCCESS, value="v")
        sink = MagicMock(spec=AuditSink)
        audit = SecretsAuditLogger(resolver, sink)

        audit.resolve(SecretsReference(provider="env", key="k"))

        event = _captured_event(sink)
        assert event.actor == "secrets_resolver"

    def test_resolve_all_emits_per_reference(self) -> None:
        resolver = _resolver_returning(SecretResolutionStatus.SUCCESS, value="v")
        sink = MagicMock(spec=AuditSink)
        audit = SecretsAuditLogger(resolver, sink)

        refs = [
            SecretsReference(provider="env", key="A"),
            SecretsReference(provider="aws", key="B"),
        ]
        results = audit.resolve_all(refs)

        assert len(results) == 2
        assert sink.emit.call_count == 2
        events = [c[0][0] for c in sink.emit.call_args_list]
        assert events[0].resource == "env:A"
        assert events[1].resource == "aws:B"

    def test_sink_exception_does_not_break_resolution(self) -> None:
        resolver = _resolver_returning(SecretResolutionStatus.SUCCESS, value="val")
        sink = MagicMock(spec=AuditSink)
        sink.emit.side_effect = RuntimeError("sink broken")
        audit = SecretsAuditLogger(resolver, sink)

        # Should not raise
        result = audit.resolve(SecretsReference(provider="env", key="k"))

        assert result.status == SecretResolutionStatus.SUCCESS
        assert result.value == "val"

    def test_error_metadata_absent_on_success(self) -> None:
        resolver = _resolver_returning(SecretResolutionStatus.SUCCESS, value="v")
        sink = MagicMock(spec=AuditSink)
        audit = SecretsAuditLogger(resolver, sink)

        audit.resolve(SecretsReference(provider="env", key="k"))

        event = _captured_event(sink)
        assert "error" not in event.metadata

    def test_works_with_secrets_cache(self) -> None:
        """SecretsAuditLogger also works with SecretsCache (duck typing)."""
        from pyspark_pipeline_framework.core.secrets.resolver import SecretsCache

        cache = MagicMock(spec=SecretsCache)
        cache.resolve.return_value = SecretResolutionResult(
            reference=SecretsReference(provider="env", key="k"),
            status=SecretResolutionStatus.SUCCESS,
            value="cached-val",
        )
        sink = MagicMock(spec=AuditSink)
        audit = SecretsAuditLogger(cache, sink)

        result = audit.resolve(SecretsReference(provider="env", key="k"))

        assert result.value == "cached-val"
        cache.resolve.assert_called_once()
        sink.emit.assert_called_once()
