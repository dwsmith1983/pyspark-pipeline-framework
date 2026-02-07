"""Audit event sinks for emitting audit trails."""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import IO, Any

from pyspark_pipeline_framework.core.audit.types import AuditEvent, AuditStatus

logger = logging.getLogger(__name__)


class AuditSink(ABC):
    """Base class for audit event sinks."""

    @abstractmethod
    def emit(self, event: AuditEvent) -> None:
        """Emit a single audit event."""
        ...

    def emit_all(self, events: list[AuditEvent]) -> None:
        """Emit multiple audit events."""
        for event in events:
            self.emit(event)

    def close(self) -> None:  # noqa: B027
        """Close the sink and release resources."""


class LoggingAuditSink(AuditSink):
    """Emit audit events to Python logging.

    Args:
        logger_name: Logger name to use. Defaults to ``"ppf.audit"``.
    """

    def __init__(self, logger_name: str = "ppf.audit") -> None:
        self._logger = logging.getLogger(logger_name)

    def emit(self, event: AuditEvent) -> None:
        level = logging.INFO if event.status == AuditStatus.SUCCESS else logging.WARNING
        self._logger.log(
            level,
            "[AUDIT] %s | %s | %s | %s",
            event.action.value if hasattr(event.action, "value") else event.action,
            event.actor,
            event.resource,
            event.status.value,
            extra={"audit_event": event.to_dict()},
        )


class FileAuditSink(AuditSink):
    """Emit audit events to a JSON-lines file.

    The file is opened lazily on the first ``emit()`` call.

    Args:
        path: Path to the audit log file.
    """

    def __init__(self, path: str | Path) -> None:
        self._path = Path(path)
        self._file: IO[Any] | None = None

    def _ensure_open(self) -> None:
        if self._file is None:
            self._path.parent.mkdir(parents=True, exist_ok=True)
            self._file = open(self._path, "a")  # noqa: SIM115

    def emit(self, event: AuditEvent) -> None:
        self._ensure_open()
        assert self._file is not None
        self._file.write(json.dumps(event.to_dict()) + "\n")
        self._file.flush()

    def close(self) -> None:
        if self._file is not None:
            self._file.close()
            self._file = None


class CompositeAuditSink(AuditSink):
    """Fan out audit events to multiple sinks.

    Exceptions raised by individual sinks are caught and logged so
    that one failing sink does not prevent others from receiving events.

    Args:
        sinks: One or more audit sinks to fan out to.
    """

    def __init__(self, *sinks: AuditSink) -> None:
        self._sinks: tuple[AuditSink, ...] = sinks

    def emit(self, event: AuditEvent) -> None:
        for sink in self._sinks:
            try:
                sink.emit(event)
            except Exception:
                logger.warning(
                    "Audit sink %s failed to emit",
                    type(sink).__name__,
                    exc_info=True,
                )

    def close(self) -> None:
        for sink in self._sinks:
            try:
                sink.close()
            except Exception:
                logger.warning(
                    "Audit sink %s failed to close",
                    type(sink).__name__,
                    exc_info=True,
                )
