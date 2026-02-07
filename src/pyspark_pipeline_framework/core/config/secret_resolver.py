"""Resolve secret references in configuration dictionaries.

Supports ``secret://PROVIDER/KEY`` syntax in string values within
component config dicts.  After HOCON parsing, call
:func:`resolve_config_secrets` to replace references with their
resolved values before component instantiation.

Examples::

    # Environment variable
    password = "secret://env/DB_PASSWORD"

    # AWS Secrets Manager
    api_key = "secret://aws/prod/api-key"

    # HashiCorp Vault
    token = "secret://vault/secret/data/myapp"
"""

from __future__ import annotations

import logging
import re
from typing import Any

from pyspark_pipeline_framework.core.secrets.base import (
    SecretResolutionStatus,
    SecretsReference,
)
from pyspark_pipeline_framework.core.secrets.resolver import SecretsResolver

logger = logging.getLogger(__name__)

SECRET_PATTERN = re.compile(r"^secret://([^/]+)/(.+)$")
"""Regex matching ``secret://PROVIDER/KEY`` references."""


class SecretResolutionError(Exception):
    """Raised when a secret reference cannot be resolved.

    Args:
        reference: The reference string that failed.
        reason: Human-readable failure description.
    """

    def __init__(self, reference: str, reason: str) -> None:
        self.reference = reference
        self.reason = reason
        super().__init__(f"Failed to resolve '{reference}': {reason}")


def parse_secret_reference(value: str) -> SecretsReference | None:
    """Parse a ``secret://PROVIDER/KEY`` string into a reference.

    Args:
        value: A string that may contain a secret reference.

    Returns:
        A :class:`SecretsReference` if the value matches, or ``None``.
    """
    match = SECRET_PATTERN.match(value)
    if match is None:
        return None
    return SecretsReference(provider=match.group(1), key=match.group(2))


def resolve_config_secrets(
    config: dict[str, Any],
    resolver: SecretsResolver,
) -> dict[str, Any]:
    """Recursively resolve secret references in a config dictionary.

    Walks the dictionary tree and replaces any string value matching
    ``secret://PROVIDER/KEY`` with the resolved secret value.

    Args:
        config: Configuration dictionary (typically
            ``ComponentConfig.config``).
        resolver: A configured :class:`SecretsResolver` with providers
            registered.

    Returns:
        A new dictionary with secret references replaced by their values.

    Raises:
        SecretResolutionError: If any secret cannot be resolved.
    """
    return _resolve_dict(config, resolver)


def _resolve_value(value: Any, resolver: SecretsResolver) -> Any:
    """Resolve a single value, recursing into dicts and lists."""
    if isinstance(value, str):
        ref = parse_secret_reference(value)
        if ref is None:
            return value
        result = resolver.resolve(ref)
        if result.status != SecretResolutionStatus.SUCCESS:
            raise SecretResolutionError(
                value,
                result.error or f"status={result.status.value}",
            )
        logger.debug("Resolved secret reference: %s/%s", ref.provider, ref.key)
        return result.value
    if isinstance(value, dict):
        return _resolve_dict(value, resolver)
    if isinstance(value, list):
        return [_resolve_value(item, resolver) for item in value]
    return value


def _resolve_dict(d: dict[str, Any], resolver: SecretsResolver) -> dict[str, Any]:
    """Resolve all secret references in a dictionary."""
    return {key: _resolve_value(val, resolver) for key, val in d.items()}
