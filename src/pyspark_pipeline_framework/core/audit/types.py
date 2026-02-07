"""Audit event types and models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class AuditAction(str, Enum):
    """Standard audit actions."""

    PIPELINE_STARTED = "pipeline_started"
    PIPELINE_COMPLETED = "pipeline_completed"
    PIPELINE_FAILED = "pipeline_failed"
    COMPONENT_STARTED = "component_started"
    COMPONENT_COMPLETED = "component_completed"
    COMPONENT_FAILED = "component_failed"
    COMPONENT_RETRIED = "component_retried"
    SECRET_ACCESSED = "secret_accessed"
    CONFIG_LOADED = "config_loaded"


class AuditStatus(str, Enum):
    """Audit event status."""

    SUCCESS = "success"
    FAILURE = "failure"
    RETRY = "retry"
    WARNING = "warning"


@dataclass
class AuditEvent:
    """A single audit event.

    Contains all information about an auditable action.

    Args:
        action: The action that occurred (enum or custom string).
        actor: Who performed the action (e.g. runner name, component name).
        resource: What was acted upon (e.g. pipeline name, component class).
        status: Outcome of the action.
        timestamp: When the event occurred.
        metadata: Additional key-value data.
        trace_id: Correlation ID for distributed tracing.
    """

    action: AuditAction | str
    actor: str
    resource: str
    status: AuditStatus
    timestamp: datetime = field(default_factory=_utcnow)
    metadata: dict[str, str] = field(default_factory=dict)
    trace_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to a JSON-serializable dictionary."""
        return {
            "action": self.action.value
            if isinstance(self.action, AuditAction)
            else self.action,
            "actor": self.actor,
            "resource": self.resource,
            "status": self.status.value,
            "timestamp": self.timestamp.isoformat(),
            "metadata": self.metadata,
            "trace_id": self.trace_id,
        }
