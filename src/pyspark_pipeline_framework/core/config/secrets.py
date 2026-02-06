"""Secrets management configuration models."""

from dataclasses import dataclass

from pyspark_pipeline_framework.core.config.base import SecretsProvider


@dataclass
class SecretsConfig:
    """Configuration for secrets management integration."""

    provider: SecretsProvider = SecretsProvider.ENV
    """Secrets provider (default: env)"""

    vault_url: str | None = None
    """HashiCorp Vault URL (required for vault provider)"""

    vault_token: str | None = None
    """Vault authentication token (optional, can use env var VAULT_TOKEN)"""

    vault_namespace: str | None = None
    """Vault namespace (optional)"""

    aws_region: str | None = None
    """AWS region for Secrets Manager (required for aws_secrets_manager provider)"""

    secret_prefix: str | None = None
    """Prefix for secret keys (optional)"""

    cache_ttl_seconds: int = 300
    """TTL for secrets cache in seconds (default: 300)"""

    def __post_init__(self) -> None:
        """Validate configuration after initialization."""
        if self.cache_ttl_seconds < 0:
            raise ValueError("cache_ttl_seconds must be non-negative")

        if self.provider == SecretsProvider.VAULT and not self.vault_url:
            raise ValueError("vault_url is required when provider is vault")

        if self.provider == SecretsProvider.AWS_SECRETS_MANAGER and not self.aws_region:
            raise ValueError("aws_region is required when provider is aws_secrets_manager")
