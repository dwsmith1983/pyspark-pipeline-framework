"""Shared utility functions."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any


def safe_call(
    fn: Callable[[], None],
    call_logger: logging.Logger,
    message: str,
    *message_args: Any,
) -> None:
    """Invoke *fn* defensively — exceptions are logged as warnings, not raised.

    Use this to call hooks, sinks, or other extension points where a failure
    should not interrupt the caller.

    Args:
        fn: Zero-argument callable to invoke.
        call_logger: Logger instance for warning output.
        message: Log message template (``%s``-style).
        *message_args: Arguments interpolated into *message*.
    """
    try:
        fn()
    except Exception:
        call_logger.warning(message, *message_args, exc_info=True)
