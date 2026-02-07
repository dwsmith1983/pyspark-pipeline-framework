"""Secrets management: providers, resolver, and caching."""

from pyspark_pipeline_framework.core.secrets.audit import SecretsAuditLogger
from pyspark_pipeline_framework.core.secrets.base import (
    SecretResolutionResult,
    SecretResolutionStatus,
    SecretsProvider,
    SecretsReference,
)
from pyspark_pipeline_framework.core.secrets.providers import (
    AwsSecretsProvider,
    EnvSecretsProvider,
    VaultSecretsProvider,
)
from pyspark_pipeline_framework.core.secrets.resolver import SecretsCache, SecretsResolver

__all__ = [
    "AwsSecretsProvider",
    "EnvSecretsProvider",
    "SecretResolutionResult",
    "SecretResolutionStatus",
    "SecretsAuditLogger",
    "SecretsCache",
    "SecretsProvider",
    "SecretsReference",
    "SecretsResolver",
    "VaultSecretsProvider",
]
