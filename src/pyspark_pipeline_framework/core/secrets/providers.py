"""Built-in secrets provider implementations."""

from __future__ import annotations

import os
from typing import Any

from pyspark_pipeline_framework.core.secrets.base import (
    SecretResolutionResult,
    SecretResolutionStatus,
    SecretsProvider,
    SecretsReference,
)


class EnvSecretsProvider(SecretsProvider):
    """Resolve secrets from environment variables.

    No external dependencies required.
    """

    @property
    def provider_name(self) -> str:
        return "env"

    def resolve(self, reference: SecretsReference) -> SecretResolutionResult:
        value = os.environ.get(reference.key)
        if value is None:
            return SecretResolutionResult(
                reference=reference,
                status=SecretResolutionStatus.NOT_FOUND,
                error=f"Environment variable '{reference.key}' not set",
            )
        return SecretResolutionResult(
            reference=reference,
            status=SecretResolutionStatus.SUCCESS,
            value=value,
        )


class AwsSecretsProvider(SecretsProvider):
    """Resolve secrets from AWS Secrets Manager.

    Requires ``boto3`` to be installed. The client is created lazily
    on the first call to :meth:`resolve`.

    Args:
        region_name: AWS region. Defaults to boto3's default region.
    """

    def __init__(self, region_name: str | None = None) -> None:
        self._region = region_name
        self._client: Any = None

    @property
    def provider_name(self) -> str:
        return "aws"

    def _get_client(self) -> Any:
        if self._client is None:
            import boto3  # type: ignore[import-untyped]

            self._client = boto3.client(
                "secretsmanager", region_name=self._region
            )
        return self._client

    def resolve(self, reference: SecretsReference) -> SecretResolutionResult:
        try:
            response = self._get_client().get_secret_value(
                SecretId=reference.key
            )
            return SecretResolutionResult(
                reference=reference,
                status=SecretResolutionStatus.SUCCESS,
                value=response.get("SecretString"),
            )
        except Exception as exc:
            return SecretResolutionResult(
                reference=reference,
                status=SecretResolutionStatus.ERROR,
                error=str(exc),
            )


class VaultSecretsProvider(SecretsProvider):
    """Resolve secrets from HashiCorp Vault (KV v2 engine).

    Requires ``hvac`` to be installed. The client is created lazily
    on the first call to :meth:`resolve`.

    Key format: ``"path/to/secret"`` returns the ``"value"`` field, or
    ``"path/to/secret:field"`` returns a specific field.

    Args:
        url: Vault server URL.
        token: Vault token. Defaults to ``VAULT_TOKEN`` environment variable.
        mount_point: KV v2 mount point. Defaults to ``"secret"``.
    """

    def __init__(
        self,
        url: str,
        token: str | None = None,
        mount_point: str = "secret",
    ) -> None:
        if not url:
            raise ValueError("url is required")
        self._url = url
        self._token = token or os.environ.get("VAULT_TOKEN")
        self._mount_point = mount_point
        self._client: Any = None

    @property
    def provider_name(self) -> str:
        return "vault"

    def _get_client(self) -> Any:
        if self._client is None:
            import hvac  # type: ignore[import-untyped]

            self._client = hvac.Client(url=self._url, token=self._token)
        return self._client

    def resolve(self, reference: SecretsReference) -> SecretResolutionResult:
        try:
            if ":" in reference.key:
                path, field = reference.key.rsplit(":", 1)
            else:
                path, field = reference.key, "value"

            response = self._get_client().secrets.kv.v2.read_secret_version(
                path=path, mount_point=self._mount_point
            )
            data = response.get("data", {}).get("data", {})
            value = data.get(field)

            if value is None:
                return SecretResolutionResult(
                    reference=reference,
                    status=SecretResolutionStatus.NOT_FOUND,
                    error=f"Field '{field}' not found in secret '{path}'",
                )

            return SecretResolutionResult(
                reference=reference,
                status=SecretResolutionStatus.SUCCESS,
                value=str(value),
            )
        except Exception as exc:
            return SecretResolutionResult(
                reference=reference,
                status=SecretResolutionStatus.ERROR,
                error=str(exc),
            )
