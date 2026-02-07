"""Configuration filter for redacting sensitive values."""

from __future__ import annotations

from typing import Any

SENSITIVE_PATTERNS: list[str] = [
    "password",
    "secret",
    "token",
    "key",
    "credential",
    "auth",
]


class ConfigFilter:
    """Filter sensitive values from configuration dictionaries.

    Uses substring matching against common sensitive key patterns.
    """

    @classmethod
    def scrub(
        cls,
        data: dict[str, Any],
        replacement: str = "***REDACTED***",
    ) -> dict[str, Any]:
        """Recursively scrub sensitive values from *data*.

        Keys whose lowercase form contains any pattern in
        :data:`SENSITIVE_PATTERNS` are replaced with *replacement*.
        """
        result: dict[str, Any] = {}
        for k, v in data.items():
            if any(p in k.lower() for p in SENSITIVE_PATTERNS):
                result[k] = replacement
            elif isinstance(v, dict):
                result[k] = cls.scrub(v, replacement)
            elif isinstance(v, list):
                result[k] = [cls.scrub(item, replacement) if isinstance(item, dict) else item for item in v]
            else:
                result[k] = v
        return result
