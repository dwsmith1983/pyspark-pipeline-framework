"""Audit-aware wrapper for secrets resolution."""

from __future__ import annotations

import logging

from pyspark_pipeline_framework.core.audit.sinks import AuditSink
from pyspark_pipeline_framework.core.audit.types import AuditAction, AuditEvent, AuditStatus
from pyspark_pipeline_framework.core.secrets.base import (
    SecretResolutionResult,
    SecretResolutionStatus,
    SecretsReference,
)
from pyspark_pipeline_framework.core.secrets.resolver import SecretsCache, SecretsResolver
from pyspark_pipeline_framework.core.utils import safe_call

logger = logging.getLogger(__name__)

_STATUS_MAP: dict[SecretResolutionStatus, AuditStatus] = {
    SecretResolutionStatus.SUCCESS: AuditStatus.SUCCESS,
    SecretResolutionStatus.NOT_FOUND: AuditStatus.WARNING,
    SecretResolutionStatus.ERROR: AuditStatus.FAILURE,
}


class SecretsAuditLogger:
    """Decorator that emits audit events for secret access.

    Wraps a :class:`SecretsResolver` or :class:`SecretsCache` and emits
    an :class:`AuditEvent` with :attr:`AuditAction.SECRET_ACCESSED` for
    every ``resolve()`` call.  The secret **value is never included** in
    the audit trail.

    Args:
        resolver: The underlying resolver or cache to delegate to.
        sink: Audit sink that receives the events.
        actor: Actor name recorded in audit events.
            Defaults to ``"secrets_resolver"``.
    """

    def __init__(
        self,
        resolver: SecretsResolver | SecretsCache,
        sink: AuditSink,
        actor: str = "secrets_resolver",
    ) -> None:
        self._resolver = resolver
        self._sink = sink
        self._actor = actor

    def resolve(self, reference: SecretsReference) -> SecretResolutionResult:
        """Resolve a secret and emit an audit event."""
        result = self._resolver.resolve(reference)
        self._emit(reference, result)
        return result

    def resolve_all(self, references: list[SecretsReference]) -> list[SecretResolutionResult]:
        """Resolve multiple secrets, emitting an audit event for each."""
        return [self.resolve(ref) for ref in references]

    def _emit(self, reference: SecretsReference, result: SecretResolutionResult) -> None:
        audit_status = _STATUS_MAP.get(result.status, AuditStatus.FAILURE)
        metadata: dict[str, str] = {
            "provider": reference.provider,
            "key": reference.key,
            "resolution_status": result.status.value,
        }
        if result.error is not None:
            metadata["error"] = result.error

        event = AuditEvent(
            action=AuditAction.SECRET_ACCESSED,
            actor=self._actor,
            resource=f"{reference.provider}:{reference.key}",
            status=audit_status,
            metadata=metadata,
        )

        safe_call(
            lambda: self._sink.emit(event),
            logger,
            "Failed to emit audit event for secret %s:%s",
            reference.provider,
            reference.key,
        )
