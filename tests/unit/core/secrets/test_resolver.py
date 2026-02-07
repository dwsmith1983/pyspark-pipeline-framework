"""Tests for SecretsResolver and SecretsCache."""

from __future__ import annotations

from unittest.mock import MagicMock

from pyspark_pipeline_framework.core.secrets.base import (
    SecretResolutionResult,
    SecretResolutionStatus,
    SecretsProvider,
    SecretsReference,
)
from pyspark_pipeline_framework.core.secrets.resolver import (
    SecretsCache,
    SecretsResolver,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_provider(name: str, value: str = "secret") -> SecretsProvider:
    provider = MagicMock(spec=SecretsProvider)
    provider.provider_name = name
    provider.resolve.return_value = SecretResolutionResult(
        reference=SecretsReference(provider=name, key="k"),
        status=SecretResolutionStatus.SUCCESS,
        value=value,
    )
    return provider


# ---------------------------------------------------------------------------
# SecretsResolver
# ---------------------------------------------------------------------------


class TestSecretsResolver:
    def test_routes_to_correct_provider(self) -> None:
        resolver = SecretsResolver()
        env_provider = _mock_provider("env", "env-val")
        aws_provider = _mock_provider("aws", "aws-val")
        resolver.register(env_provider)
        resolver.register(aws_provider)

        ref = SecretsReference(provider="env", key="MY_VAR")
        result = resolver.resolve(ref)

        env_provider.resolve.assert_called_once_with(ref)
        aws_provider.resolve.assert_not_called()
        assert result.status == SecretResolutionStatus.SUCCESS

    def test_unknown_provider_returns_error(self) -> None:
        resolver = SecretsResolver()

        ref = SecretsReference(provider="unknown", key="k")
        result = resolver.resolve(ref)

        assert result.status == SecretResolutionStatus.ERROR
        assert "Unknown provider" in (result.error or "")

    def test_resolve_all(self) -> None:
        resolver = SecretsResolver()
        resolver.register(_mock_provider("env"))

        refs = [
            SecretsReference(provider="env", key="A"),
            SecretsReference(provider="env", key="B"),
        ]
        results = resolver.resolve_all(refs)

        assert len(results) == 2

    def test_register_replaces_existing(self) -> None:
        resolver = SecretsResolver()
        p1 = _mock_provider("env", "old")
        p2 = _mock_provider("env", "new")
        resolver.register(p1)
        resolver.register(p2)

        ref = SecretsReference(provider="env", key="k")
        resolver.resolve(ref)

        p1.resolve.assert_not_called()
        p2.resolve.assert_called_once()


# ---------------------------------------------------------------------------
# SecretsCache
# ---------------------------------------------------------------------------


class TestSecretsCache:
    def test_caches_result(self) -> None:
        resolver = MagicMock(spec=SecretsResolver)
        resolver.resolve.return_value = SecretResolutionResult(
            reference=SecretsReference(provider="env", key="K"),
            status=SecretResolutionStatus.SUCCESS,
            value="cached",
        )

        t = 100.0
        cache = SecretsCache(resolver, ttl_seconds=60, clock=lambda: t)

        ref = SecretsReference(provider="env", key="K")
        result1 = cache.resolve(ref)
        result2 = cache.resolve(ref)

        assert result1.value == "cached"
        assert result2.value == "cached"
        assert resolver.resolve.call_count == 1

    def test_expires_after_ttl(self) -> None:
        resolver = MagicMock(spec=SecretsResolver)
        resolver.resolve.return_value = SecretResolutionResult(
            reference=SecretsReference(provider="env", key="K"),
            status=SecretResolutionStatus.SUCCESS,
            value="val",
        )

        times = [100.0, 100.0, 200.0, 200.0]
        time_iter = iter(times)
        cache = SecretsCache(
            resolver, ttl_seconds=60, clock=lambda: next(time_iter)
        )

        ref = SecretsReference(provider="env", key="K")
        cache.resolve(ref)
        cache.resolve(ref)

        assert resolver.resolve.call_count == 2

    def test_clear_invalidates_cache(self) -> None:
        resolver = MagicMock(spec=SecretsResolver)
        resolver.resolve.return_value = SecretResolutionResult(
            reference=SecretsReference(provider="env", key="K"),
            status=SecretResolutionStatus.SUCCESS,
            value="val",
        )

        t = 100.0
        cache = SecretsCache(resolver, ttl_seconds=300, clock=lambda: t)

        ref = SecretsReference(provider="env", key="K")
        cache.resolve(ref)
        cache.clear()
        cache.resolve(ref)

        assert resolver.resolve.call_count == 2

    def test_caches_errors_too(self) -> None:
        resolver = MagicMock(spec=SecretsResolver)
        resolver.resolve.return_value = SecretResolutionResult(
            reference=SecretsReference(provider="env", key="K"),
            status=SecretResolutionStatus.ERROR,
            error="boom",
        )

        t = 100.0
        cache = SecretsCache(resolver, ttl_seconds=60, clock=lambda: t)

        ref = SecretsReference(provider="env", key="K")
        r1 = cache.resolve(ref)
        r2 = cache.resolve(ref)

        assert r1.status == SecretResolutionStatus.ERROR
        assert r2.status == SecretResolutionStatus.ERROR
        assert resolver.resolve.call_count == 1

    def test_resolve_all(self) -> None:
        resolver = MagicMock(spec=SecretsResolver)
        resolver.resolve.return_value = SecretResolutionResult(
            reference=SecretsReference(provider="env", key="K"),
            status=SecretResolutionStatus.SUCCESS,
            value="v",
        )

        t = 100.0
        cache = SecretsCache(resolver, ttl_seconds=60, clock=lambda: t)

        refs = [
            SecretsReference(provider="env", key="A"),
            SecretsReference(provider="env", key="B"),
        ]
        results = cache.resolve_all(refs)

        assert len(results) == 2

    def test_different_keys_cached_separately(self) -> None:
        resolver = MagicMock(spec=SecretsResolver)
        call_count = 0

        def side_effect(ref: SecretsReference) -> SecretResolutionResult:
            nonlocal call_count
            call_count += 1
            return SecretResolutionResult(
                reference=ref,
                status=SecretResolutionStatus.SUCCESS,
                value=f"val-{call_count}",
            )

        resolver.resolve.side_effect = side_effect

        t = 100.0
        cache = SecretsCache(resolver, ttl_seconds=60, clock=lambda: t)

        r1 = cache.resolve(SecretsReference(provider="env", key="A"))
        r2 = cache.resolve(SecretsReference(provider="env", key="B"))
        r3 = cache.resolve(SecretsReference(provider="env", key="A"))

        assert r1.value == "val-1"
        assert r2.value == "val-2"
        assert r3.value == "val-1"
        assert resolver.resolve.call_count == 2
