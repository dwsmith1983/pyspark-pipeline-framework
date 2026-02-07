"""Tests for secret reference resolution in config dicts."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from pyspark_pipeline_framework.core.config.secret_resolver import (
    SecretResolutionError,
    parse_secret_reference,
    resolve_config_secrets,
)
from pyspark_pipeline_framework.core.secrets.base import (
    SecretResolutionResult,
    SecretResolutionStatus,
    SecretsReference,
)
from pyspark_pipeline_framework.core.secrets.resolver import SecretsResolver


# ---------------------------------------------------------------------------
# parse_secret_reference
# ---------------------------------------------------------------------------


class TestParseSecretReference:
    def test_env_reference(self) -> None:
        ref = parse_secret_reference("secret://env/DB_PASSWORD")
        assert ref is not None
        assert ref.provider == "env"
        assert ref.key == "DB_PASSWORD"

    def test_aws_reference(self) -> None:
        ref = parse_secret_reference("secret://aws/prod/api-key")
        assert ref is not None
        assert ref.provider == "aws"
        assert ref.key == "prod/api-key"

    def test_vault_reference(self) -> None:
        ref = parse_secret_reference("secret://vault/secret/data/myapp")
        assert ref is not None
        assert ref.provider == "vault"
        assert ref.key == "secret/data/myapp"

    def test_plain_string_returns_none(self) -> None:
        assert parse_secret_reference("just a normal value") is None

    def test_empty_string_returns_none(self) -> None:
        assert parse_secret_reference("") is None

    def test_partial_match_returns_none(self) -> None:
        assert parse_secret_reference("secret://") is None

    def test_no_key_returns_none(self) -> None:
        assert parse_secret_reference("secret://env/") is None

    def test_missing_provider_returns_none(self) -> None:
        assert parse_secret_reference("secret:///key") is None

    def test_not_at_start_returns_none(self) -> None:
        assert parse_secret_reference("prefix secret://env/KEY") is None

    def test_url_like_string_not_matched(self) -> None:
        assert parse_secret_reference("https://example.com") is None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_resolver(secrets: dict[tuple[str, str], str]) -> SecretsResolver:
    """Build a resolver that returns predefined values."""
    resolver = MagicMock(spec=SecretsResolver)

    def mock_resolve(ref: SecretsReference) -> SecretResolutionResult:
        key = (ref.provider, ref.key)
        if key in secrets:
            return SecretResolutionResult(
                reference=ref,
                status=SecretResolutionStatus.SUCCESS,
                value=secrets[key],
            )
        return SecretResolutionResult(
            reference=ref,
            status=SecretResolutionStatus.NOT_FOUND,
            error=f"Secret not found: {ref.provider}/{ref.key}",
        )

    resolver.resolve.side_effect = mock_resolve
    return resolver


# ---------------------------------------------------------------------------
# resolve_config_secrets
# ---------------------------------------------------------------------------


class TestResolveConfigSecrets:
    def test_flat_dict(self) -> None:
        resolver = _make_resolver({("env", "DB_PASS"): "s3cret"})
        config = {"password": "secret://env/DB_PASS", "host": "localhost"}

        result = resolve_config_secrets(config, resolver)

        assert result == {"password": "s3cret", "host": "localhost"}

    def test_nested_dict(self) -> None:
        resolver = _make_resolver({("aws", "api-key"): "abc123"})
        config = {
            "database": {
                "connection": {
                    "api_key": "secret://aws/api-key",
                    "port": 5432,
                }
            }
        }

        result = resolve_config_secrets(config, resolver)

        assert result["database"]["connection"]["api_key"] == "abc123"
        assert result["database"]["connection"]["port"] == 5432

    def test_list_values(self) -> None:
        resolver = _make_resolver({
            ("env", "TOKEN_A"): "aaa",
            ("env", "TOKEN_B"): "bbb",
        })
        config = {
            "tokens": ["secret://env/TOKEN_A", "secret://env/TOKEN_B", "plain"]
        }

        result = resolve_config_secrets(config, resolver)

        assert result["tokens"] == ["aaa", "bbb", "plain"]

    def test_non_string_values_unchanged(self) -> None:
        resolver = _make_resolver({})
        config = {"count": 42, "enabled": True, "ratio": 3.14, "empty": None}

        result = resolve_config_secrets(config, resolver)

        assert result == config

    def test_mixed_secrets_and_plain(self) -> None:
        resolver = _make_resolver({("vault", "db/creds"): "hunter2"})
        config = {
            "db_password": "secret://vault/db/creds",
            "db_host": "postgres.internal",
            "db_port": 5432,
        }

        result = resolve_config_secrets(config, resolver)

        assert result["db_password"] == "hunter2"
        assert result["db_host"] == "postgres.internal"
        assert result["db_port"] == 5432

    def test_empty_dict(self) -> None:
        resolver = _make_resolver({})
        assert resolve_config_secrets({}, resolver) == {}

    def test_original_dict_not_mutated(self) -> None:
        resolver = _make_resolver({("env", "KEY"): "val"})
        config = {"secret": "secret://env/KEY"}

        resolve_config_secrets(config, resolver)

        assert config["secret"] == "secret://env/KEY"


class TestResolveConfigSecretsErrors:
    def test_not_found_raises(self) -> None:
        resolver = _make_resolver({})
        config = {"token": "secret://env/MISSING"}

        with pytest.raises(SecretResolutionError, match="MISSING"):
            resolve_config_secrets(config, resolver)

    def test_error_status_raises(self) -> None:
        resolver = MagicMock(spec=SecretsResolver)
        resolver.resolve.return_value = SecretResolutionResult(
            reference=SecretsReference(provider="aws", key="broken"),
            status=SecretResolutionStatus.ERROR,
            error="Connection refused",
        )
        config = {"key": "secret://aws/broken"}

        with pytest.raises(SecretResolutionError, match="Connection refused"):
            resolve_config_secrets(config, resolver)

    def test_error_contains_reference_string(self) -> None:
        resolver = _make_resolver({})
        config = {"x": "secret://env/NOPE"}

        with pytest.raises(SecretResolutionError) as exc_info:
            resolve_config_secrets(config, resolver)

        assert exc_info.value.reference == "secret://env/NOPE"

    def test_nested_error_propagates(self) -> None:
        resolver = _make_resolver({})
        config = {"outer": {"inner": "secret://env/MISSING"}}

        with pytest.raises(SecretResolutionError, match="MISSING"):
            resolve_config_secrets(config, resolver)

    def test_list_error_propagates(self) -> None:
        resolver = _make_resolver({("env", "A"): "ok"})
        config = {"items": ["secret://env/A", "secret://env/MISSING"]}

        with pytest.raises(SecretResolutionError, match="MISSING"):
            resolve_config_secrets(config, resolver)
