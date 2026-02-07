"""Tests for streaming lifecycle hooks."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

from pyspark_pipeline_framework.runtime.streaming.hooks import (
    CompositeStreamingHooks,
    LoggingStreamingHooks,
    NoOpStreamingHooks,
)


class TestNoOpStreamingHooks:
    """All methods are callable and do nothing."""

    def test_on_query_start(self) -> None:
        hooks = NoOpStreamingHooks()
        hooks.on_query_start("q", "id-1")

    def test_on_batch_progress(self) -> None:
        hooks = NoOpStreamingHooks()
        hooks.on_batch_progress("q", 0, 100, 50)

    def test_on_query_terminated(self) -> None:
        hooks = NoOpStreamingHooks()
        hooks.on_query_terminated("q", "id-1", None)

    def test_on_query_terminated_with_error(self) -> None:
        hooks = NoOpStreamingHooks()
        hooks.on_query_terminated("q", "id-1", RuntimeError("boom"))


class TestLoggingStreamingHooks:
    """Logging hooks emit log messages at appropriate levels."""

    def test_default_logger_name(self) -> None:
        hooks = LoggingStreamingHooks()
        assert hooks._log.name == "ppf.streaming"

    def test_custom_logger(self) -> None:
        custom = logging.getLogger("custom.streaming")
        hooks = LoggingStreamingHooks(log=custom)
        assert hooks._log is custom

    def test_on_query_start_logs_info(self) -> None:
        mock_log = MagicMock(spec=logging.Logger)
        hooks = LoggingStreamingHooks(log=mock_log)
        hooks.on_query_start("my-query", "abc-123")
        mock_log.info.assert_called_once()
        args = str(mock_log.info.call_args)
        assert "my-query" in args

    def test_on_batch_progress_logs_info(self) -> None:
        mock_log = MagicMock(spec=logging.Logger)
        hooks = LoggingStreamingHooks(log=mock_log)
        hooks.on_batch_progress("my-query", 5, 1000, 250)
        mock_log.info.assert_called_once()
        args = str(mock_log.info.call_args)
        assert "1000" in args

    def test_on_query_terminated_success_logs_info(self) -> None:
        mock_log = MagicMock(spec=logging.Logger)
        hooks = LoggingStreamingHooks(log=mock_log)
        hooks.on_query_terminated("my-query", "abc-123", None)
        mock_log.info.assert_called_once()
        mock_log.error.assert_not_called()

    def test_on_query_terminated_error_logs_error(self) -> None:
        mock_log = MagicMock(spec=logging.Logger)
        hooks = LoggingStreamingHooks(log=mock_log)
        hooks.on_query_terminated("my-query", "abc-123", RuntimeError("fail"))
        mock_log.error.assert_called_once()
        args = str(mock_log.error.call_args)
        assert "fail" in args


class TestCompositeStreamingHooks:
    """Composite hooks fan out to all children and swallow errors."""

    def test_fans_out_on_query_start(self) -> None:
        h1, h2 = MagicMock(), MagicMock()
        composite = CompositeStreamingHooks(h1, h2)
        composite.on_query_start("q", "id")
        h1.on_query_start.assert_called_once_with("q", "id")
        h2.on_query_start.assert_called_once_with("q", "id")

    def test_fans_out_on_batch_progress(self) -> None:
        h1, h2 = MagicMock(), MagicMock()
        composite = CompositeStreamingHooks(h1, h2)
        composite.on_batch_progress("q", 3, 500, 100)
        h1.on_batch_progress.assert_called_once_with("q", 3, 500, 100)
        h2.on_batch_progress.assert_called_once_with("q", 3, 500, 100)

    def test_fans_out_on_query_terminated(self) -> None:
        h1, h2 = MagicMock(), MagicMock()
        composite = CompositeStreamingHooks(h1, h2)
        err = RuntimeError("boom")
        composite.on_query_terminated("q", "id", err)
        h1.on_query_terminated.assert_called_once_with("q", "id", err)
        h2.on_query_terminated.assert_called_once_with("q", "id", err)

    def test_swallows_exception_from_one_hook(self) -> None:
        h1 = MagicMock()
        h1.on_query_start.side_effect = RuntimeError("h1 broke")
        h2 = MagicMock()
        composite = CompositeStreamingHooks(h1, h2)
        # Should not raise
        composite.on_query_start("q", "id")
        # h2 still called despite h1 failure
        h2.on_query_start.assert_called_once_with("q", "id")

    def test_empty_composite(self) -> None:
        composite = CompositeStreamingHooks()
        # Should not raise
        composite.on_query_start("q", "id")
        composite.on_batch_progress("q", 0, 0, 0)
        composite.on_query_terminated("q", "id", None)
