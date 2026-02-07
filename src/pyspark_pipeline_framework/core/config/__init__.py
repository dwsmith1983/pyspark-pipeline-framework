"""Configuration models for pyspark-pipeline-framework.

This package provides dataconf-based configuration models for defining
PySpark pipelines in a type-safe, declarative manner using HOCON format.
"""

from pyspark_pipeline_framework.core.config.base import (
    ComponentType,
    Environment,
    LogFormat,
    LogLevel,
    MetricsBackend,
    PipelineMode,
    SecretsProvider,
    SparkDeployMode,
)
from pyspark_pipeline_framework.core.config.component import ComponentConfig
from pyspark_pipeline_framework.core.config.hooks import AuditConfig, HooksConfig, LoggingConfig, MetricsConfig
from pyspark_pipeline_framework.core.config.loader import load_from_env, load_from_file, load_from_string
from pyspark_pipeline_framework.core.config.pipeline import PipelineConfig
from pyspark_pipeline_framework.core.config.presets import CircuitBreakerConfigs, ResiliencePolicies, RetryPolicies
from pyspark_pipeline_framework.core.config.retry import CircuitBreakerConfig, ResiliencePolicy, RetryConfig
from pyspark_pipeline_framework.core.config.secret_resolver import (
    SecretResolutionError,
    parse_secret_reference,
    resolve_config_secrets,
)
from pyspark_pipeline_framework.core.config.secrets import SecretsConfig
from pyspark_pipeline_framework.core.config.spark import SparkConfig
from pyspark_pipeline_framework.core.config.validator import (
    DryRunResult,
    ValidationError,
    ValidationPhase,
    ValidationResult,
    dry_run,
    validate_pipeline,
)

__all__ = [
    "DryRunResult",
    "AuditConfig",
    "CircuitBreakerConfig",
    "CircuitBreakerConfigs",
    "ComponentConfig",
    "ComponentType",
    "Environment",
    "HooksConfig",
    "LogFormat",
    "LogLevel",
    "LoggingConfig",
    "MetricsBackend",
    "MetricsConfig",
    "PipelineConfig",
    "PipelineMode",
    "ResiliencePolicies",
    "ResiliencePolicy",
    "RetryConfig",
    "RetryPolicies",
    "SecretResolutionError",
    "SecretsConfig",
    "SecretsProvider",
    "SparkConfig",
    "SparkDeployMode",
    "ValidationError",
    "ValidationPhase",
    "ValidationResult",
    "dry_run",
    "load_from_env",
    "load_from_file",
    "load_from_string",
    "parse_secret_reference",
    "resolve_config_secrets",
    "validate_pipeline",
]
