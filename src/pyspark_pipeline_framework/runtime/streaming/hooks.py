"""Streaming-specific lifecycle hooks."""

from __future__ import annotations

import logging
from typing import Any, Protocol

from pyspark_pipeline_framework.core.utils import safe_call

logger = logging.getLogger(__name__)


class StreamingHooks(Protocol):
    """Protocol for streaming lifecycle callbacks.

    Implementations receive notifications for query-level events
    such as start, batch progress, and termination.
    """

    def on_query_start(self, query_name: str, query_id: str) -> None:
        """Called when a streaming query starts.

        Args:
            query_name: Name of the streaming query.
            query_id: Unique identifier for the query run.
        """
        ...

    def on_batch_progress(
        self,
        query_name: str,
        batch_id: int,
        num_input_rows: int,
        duration_ms: int,
    ) -> None:
        """Called after each micro-batch completes.

        Args:
            query_name: Name of the streaming query.
            batch_id: Sequential batch identifier.
            num_input_rows: Number of rows processed in this batch.
            duration_ms: Batch processing duration in milliseconds.
        """
        ...

    def on_query_terminated(
        self,
        query_name: str,
        query_id: str,
        exception: Exception | None,
    ) -> None:
        """Called when a streaming query terminates.

        Args:
            query_name: Name of the streaming query.
            query_id: Unique identifier for the query run.
            exception: The exception if the query failed, or ``None``.
        """
        ...


class NoOpStreamingHooks:
    """Streaming hooks implementation that does nothing."""

    def on_query_start(self, query_name: str, query_id: str) -> None:
        pass

    def on_batch_progress(
        self,
        query_name: str,
        batch_id: int,
        num_input_rows: int,
        duration_ms: int,
    ) -> None:
        pass

    def on_query_terminated(
        self,
        query_name: str,
        query_id: str,
        exception: Exception | None,
    ) -> None:
        pass


class LoggingStreamingHooks:
    """Streaming hooks that log events.

    Args:
        log: Custom logger instance.  Defaults to ``"ppf.streaming"``.
    """

    def __init__(self, log: logging.Logger | None = None) -> None:
        self._log = log or logging.getLogger("ppf.streaming")

    def on_query_start(self, query_name: str, query_id: str) -> None:
        self._log.info("Query '%s' started (id=%s)", query_name, query_id)

    def on_batch_progress(
        self,
        query_name: str,
        batch_id: int,
        num_input_rows: int,
        duration_ms: int,
    ) -> None:
        self._log.info(
            "Query '%s' batch %d: %d rows in %dms",
            query_name,
            batch_id,
            num_input_rows,
            duration_ms,
        )

    def on_query_terminated(
        self,
        query_name: str,
        query_id: str,
        exception: Exception | None,
    ) -> None:
        if exception is not None:
            self._log.error(
                "Query '%s' terminated with error (id=%s): %s",
                query_name,
                query_id,
                exception,
            )
        else:
            self._log.info("Query '%s' terminated normally (id=%s)", query_name, query_id)


class CompositeStreamingHooks:
    """Broadcasts streaming lifecycle events to multiple hooks.

    Exceptions from individual hooks are caught and logged.

    Args:
        hooks: One or more streaming hooks to fan out to.
    """

    def __init__(self, *hooks: Any) -> None:
        self._hooks: tuple[Any, ...] = hooks

    def _call_all(self, method: str, *args: Any, **kwargs: Any) -> None:
        for hook in self._hooks:

            def _invoke(h: Any = hook) -> None:
                getattr(h, method)(*args, **kwargs)

            safe_call(
                _invoke,
                logger,
                "Streaming hook %s.%s raised an exception",
                type(hook).__name__,
                method,
            )

    def on_query_start(self, query_name: str, query_id: str) -> None:
        self._call_all("on_query_start", query_name, query_id)

    def on_batch_progress(
        self,
        query_name: str,
        batch_id: int,
        num_input_rows: int,
        duration_ms: int,
    ) -> None:
        self._call_all("on_batch_progress", query_name, batch_id, num_input_rows, duration_ms)

    def on_query_terminated(
        self,
        query_name: str,
        query_id: str,
        exception: Exception | None,
    ) -> None:
        self._call_all("on_query_terminated", query_name, query_id, exception)
