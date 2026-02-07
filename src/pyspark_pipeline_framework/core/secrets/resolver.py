"""Secrets resolver and caching layer."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

from pyspark_pipeline_framework.core.secrets.base import (
    SecretResolutionResult,
    SecretResolutionStatus,
    SecretsProvider,
    SecretsReference,
)


class SecretsResolver:
    """Composite resolver that routes requests to registered providers.

    Each :class:`SecretsReference` is dispatched to the provider whose
    :attr:`~SecretsProvider.provider_name` matches
    :attr:`SecretsReference.provider`.
    """

    def __init__(self) -> None:
        self._providers: dict[str, SecretsProvider] = {}

    def register(self, provider: SecretsProvider) -> None:
        """Register a secrets provider."""
        self._providers[provider.provider_name] = provider

    def resolve(self, reference: SecretsReference) -> SecretResolutionResult:
        """Resolve a secret using the appropriate provider."""
        provider = self._providers.get(reference.provider)
        if provider is None:
            return SecretResolutionResult(
                reference=reference,
                status=SecretResolutionStatus.ERROR,
                error=f"Unknown provider: {reference.provider}",
            )
        return provider.resolve(reference)

    def resolve_all(self, references: list[SecretsReference]) -> list[SecretResolutionResult]:
        """Resolve multiple secret references."""
        return [self.resolve(ref) for ref in references]


class SecretsCache:
    """Thread-safe caching wrapper for secret resolution.

    Caches all resolution results (success, not-found, and error) with
    a configurable TTL. Use :meth:`clear` to manually invalidate.

    Args:
        resolver: The underlying resolver to delegate to on cache miss.
        ttl_seconds: Cache entry lifetime in seconds. Defaults to 300.
        clock: Injectable monotonic clock for testing.
            Defaults to ``time.monotonic``.
    """

    def __init__(
        self,
        resolver: SecretsResolver,
        ttl_seconds: int = 300,
        clock: Callable[[], float] | None = None,
    ) -> None:
        self._resolver = resolver
        self._ttl = ttl_seconds
        self._clock = clock or time.monotonic
        self._cache: dict[str, tuple[SecretResolutionResult, float]] = {}
        self._lock = threading.Lock()

    def resolve(self, reference: SecretsReference) -> SecretResolutionResult:
        """Resolve a secret, returning a cached result if available."""
        cache_key = f"{reference.provider}:{reference.key}"
        now = self._clock()

        with self._lock:
            if cache_key in self._cache:
                result, timestamp = self._cache[cache_key]
                if now - timestamp < self._ttl:
                    return result

        result = self._resolver.resolve(reference)

        with self._lock:
            self._cache[cache_key] = (result, self._clock())

        return result

    def resolve_all(self, references: list[SecretsReference]) -> list[SecretResolutionResult]:
        """Resolve multiple secret references with caching."""
        return [self.resolve(ref) for ref in references]

    def clear(self) -> None:
        """Clear all cached entries."""
        with self._lock:
            self._cache.clear()
