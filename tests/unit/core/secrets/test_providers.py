"""Tests for secrets providers."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from pyspark_pipeline_framework.core.secrets.base import (
    SecretResolutionStatus,
    SecretsReference,
)
from pyspark_pipeline_framework.core.secrets.providers import (
    AwsSecretsProvider,
    EnvSecretsProvider,
    VaultSecretsProvider,
)


# ---------------------------------------------------------------------------
# EnvSecretsProvider
# ---------------------------------------------------------------------------


class TestEnvSecretsProvider:
    def test_provider_name(self) -> None:
        provider = EnvSecretsProvider()
        assert provider.provider_name == "env"

    def test_resolves_existing_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("MY_SECRET", "secret-value")
        provider = EnvSecretsProvider()
        ref = SecretsReference(provider="env", key="MY_SECRET")

        result = provider.resolve(ref)

        assert result.status == SecretResolutionStatus.SUCCESS
        assert result.value == "secret-value"

    def test_not_found_for_missing_var(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("NONEXISTENT_VAR", raising=False)
        provider = EnvSecretsProvider()
        ref = SecretsReference(provider="env", key="NONEXISTENT_VAR")

        result = provider.resolve(ref)

        assert result.status == SecretResolutionStatus.NOT_FOUND
        assert result.value is None
        assert "NONEXISTENT_VAR" in (result.error or "")

    def test_resolve_all(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("A", "1")
        monkeypatch.setenv("B", "2")
        provider = EnvSecretsProvider()

        results = provider.resolve_all([
            SecretsReference(provider="env", key="A"),
            SecretsReference(provider="env", key="B"),
        ])

        assert len(results) == 2
        assert results[0].value == "1"
        assert results[1].value == "2"


# ---------------------------------------------------------------------------
# AwsSecretsProvider
# ---------------------------------------------------------------------------


class TestAwsSecretsProvider:
    def test_provider_name(self) -> None:
        provider = AwsSecretsProvider(region_name="us-east-1")
        assert provider.provider_name == "aws"

    def test_resolves_secret(self) -> None:
        provider = AwsSecretsProvider(region_name="us-east-1")
        mock_client = MagicMock()
        mock_client.get_secret_value.return_value = {
            "SecretString": "my-db-password"
        }
        provider._client = mock_client

        ref = SecretsReference(provider="aws", key="prod/db/password")
        result = provider.resolve(ref)

        assert result.status == SecretResolutionStatus.SUCCESS
        assert result.value == "my-db-password"
        mock_client.get_secret_value.assert_called_once_with(
            SecretId="prod/db/password"
        )

    def test_error_on_exception(self) -> None:
        provider = AwsSecretsProvider()
        mock_client = MagicMock()
        mock_client.get_secret_value.side_effect = RuntimeError("access denied")
        provider._client = mock_client

        ref = SecretsReference(provider="aws", key="secret")
        result = provider.resolve(ref)

        assert result.status == SecretResolutionStatus.ERROR
        assert "access denied" in (result.error or "")

    def test_lazy_client_init(self) -> None:
        provider = AwsSecretsProvider(region_name="eu-west-1")
        assert provider._client is None

        mock_boto3 = MagicMock()
        with patch.dict("sys.modules", {"boto3": mock_boto3}):
            client = provider._get_client()

        mock_boto3.client.assert_called_once_with(
            "secretsmanager", region_name="eu-west-1"
        )
        assert client is not None


# ---------------------------------------------------------------------------
# VaultSecretsProvider
# ---------------------------------------------------------------------------


class TestVaultSecretsProvider:
    def test_provider_name(self) -> None:
        provider = VaultSecretsProvider(url="http://vault:8200")
        assert provider.provider_name == "vault"

    def test_empty_url_raises(self) -> None:
        with pytest.raises(ValueError, match="url is required"):
            VaultSecretsProvider(url="")

    def test_resolves_with_default_field(self) -> None:
        provider = VaultSecretsProvider(url="http://vault:8200", token="tok")
        mock_client = MagicMock()
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"value": "my-secret"}}
        }
        provider._client = mock_client

        ref = SecretsReference(provider="vault", key="app/database")
        result = provider.resolve(ref)

        assert result.status == SecretResolutionStatus.SUCCESS
        assert result.value == "my-secret"
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            path="app/database", mount_point="secret"
        )

    def test_resolves_with_explicit_field(self) -> None:
        provider = VaultSecretsProvider(url="http://vault:8200", token="tok")
        mock_client = MagicMock()
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"password": "p@ss"}}
        }
        provider._client = mock_client

        ref = SecretsReference(provider="vault", key="app/database:password")
        result = provider.resolve(ref)

        assert result.status == SecretResolutionStatus.SUCCESS
        assert result.value == "p@ss"
        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            path="app/database", mount_point="secret"
        )

    def test_not_found_when_field_missing(self) -> None:
        provider = VaultSecretsProvider(url="http://vault:8200", token="tok")
        mock_client = MagicMock()
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"other_field": "val"}}
        }
        provider._client = mock_client

        ref = SecretsReference(provider="vault", key="app/database:password")
        result = provider.resolve(ref)

        assert result.status == SecretResolutionStatus.NOT_FOUND
        assert "password" in (result.error or "")

    def test_error_on_exception(self) -> None:
        provider = VaultSecretsProvider(url="http://vault:8200", token="tok")
        mock_client = MagicMock()
        mock_client.secrets.kv.v2.read_secret_version.side_effect = (
            RuntimeError("connection refused")
        )
        provider._client = mock_client

        ref = SecretsReference(provider="vault", key="secret/path")
        result = provider.resolve(ref)

        assert result.status == SecretResolutionStatus.ERROR
        assert "connection refused" in (result.error or "")

    def test_token_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("VAULT_TOKEN", "env-token")
        provider = VaultSecretsProvider(url="http://vault:8200")
        assert provider._token == "env-token"

    def test_custom_mount_point(self) -> None:
        provider = VaultSecretsProvider(
            url="http://vault:8200",
            token="tok",
            mount_point="kv",
        )
        mock_client = MagicMock()
        mock_client.secrets.kv.v2.read_secret_version.return_value = {
            "data": {"data": {"value": "v"}}
        }
        provider._client = mock_client

        ref = SecretsReference(provider="vault", key="path")
        provider.resolve(ref)

        mock_client.secrets.kv.v2.read_secret_version.assert_called_once_with(
            path="path", mount_point="kv"
        )
