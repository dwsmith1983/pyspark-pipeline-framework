"""Tests for core.utils."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

from pyspark_pipeline_framework.core.utils import safe_call


class TestSafeCall:
    """Tests for safe_call."""

    def test_calls_function(self) -> None:
        """The wrapped function is actually invoked."""
        fn = MagicMock()
        safe_call(fn, logging.getLogger("test"), "msg")
        fn.assert_called_once()

    def test_swallows_exception(self) -> None:
        """Exceptions from fn are caught, not re-raised."""
        fn = MagicMock(side_effect=RuntimeError("boom"))
        # Should not raise
        safe_call(fn, logging.getLogger("test"), "msg")

    def test_logs_warning_on_exception(self) -> None:
        """A warning with exc_info is logged when fn raises."""
        mock_logger = MagicMock()
        fn = MagicMock(side_effect=ValueError("oops"))

        safe_call(fn, mock_logger, "Component %s failed", "MyComp")

        mock_logger.warning.assert_called_once_with(
            "Component %s failed",
            "MyComp",
            exc_info=True,
        )

    def test_does_not_log_on_success(self) -> None:
        """No logging occurs when fn succeeds."""
        mock_logger = MagicMock()
        safe_call(lambda: None, mock_logger, "should not appear")
        mock_logger.warning.assert_not_called()
