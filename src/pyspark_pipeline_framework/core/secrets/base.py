"""Secrets management abstractions and models."""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum


class SecretResolutionStatus(str, Enum):
    """Outcome of a secret resolution attempt."""

    SUCCESS = "success"
    NOT_FOUND = "not_found"
    ERROR = "error"


@dataclass
class SecretsReference:
    """Reference to a secret in a specific provider.

    Args:
        provider: Provider name (e.g. ``"env"``, ``"aws"``, ``"vault"``).
        key: Secret key or path within the provider.
    """

    provider: str
    key: str


@dataclass
class SecretResolutionResult:
    """Result of resolving a secret reference.

    The ``value`` field is masked in ``__repr__`` to prevent accidental
    leakage in logs or tracebacks.

    Args:
        reference: The original reference that was resolved.
        status: Outcome of the resolution.
        value: The secret value (only set on success).
        error: Error description (only set on failure).
    """

    reference: SecretsReference
    status: SecretResolutionStatus
    value: str | None = None
    error: str | None = None

    def __repr__(self) -> str:
        masked = "***" if self.value is not None else "None"
        return (
            f"SecretResolutionResult("
            f"reference={self.reference!r}, "
            f"status={self.status!r}, "
            f"value={masked}, "
            f"error={self.error!r})"
        )


class SecretsProvider(ABC):
    """Base class for secrets providers.

    Subclasses implement :meth:`resolve` to fetch secrets from a
    specific backend (environment variables, AWS Secrets Manager, etc.).
    """

    @property
    @abstractmethod
    def provider_name(self) -> str:
        """Unique name for this provider (e.g. ``"env"``, ``"aws"``)."""
        ...

    @abstractmethod
    def resolve(self, reference: SecretsReference) -> SecretResolutionResult:
        """Resolve a single secret reference."""
        ...

    def resolve_all(self, references: list[SecretsReference]) -> list[SecretResolutionResult]:
        """Resolve multiple secret references."""
        return [self.resolve(ref) for ref in references]
